use super::ExecutedBlock;
use alloy_consensus::BlockHeader;
use alloy_primitives::{keccak256, Address, BlockNumber, Bytes, StorageKey, StorageValue, B256};
use reth_errors::ProviderResult;
use reth_primitives_traits::{Account, Bytecode, NodePrimitives};
use reth_storage_api::{
    AccountReader, BlockHashReader, BytecodeReader, HashedPostStateProvider, StateProofProvider,
    StateProvider, StateProviderBox, StateRootProvider, StorageRootProvider,
};
use reth_trie::{
    updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof,
    MultiProofTargets, StorageMultiProof, TrieInput,
};
use revm_database::BundleState;
use std::{
    borrow::Cow,
    sync::{Arc, OnceLock},
};

/// A state provider that stores references to in-memory blocks along with their state as well as a
/// reference of the historical state provider for fallback lookups.
#[expect(missing_debug_implementations)]
pub struct MemoryOverlayStateProviderRef<
    'a,
    N: NodePrimitives = reth_ethereum_primitives::EthPrimitives,
> {
    /// Historical state provider for state lookups that are not found in memory blocks.
    pub(crate) historical: Box<dyn StateProvider + 'a>,
    /// The collection of executed parent blocks. Expected order is newest to oldest.
    pub(crate) in_memory: Cow<'a, [ExecutedBlock<N>]>,
    /// Lazy-loaded in-memory trie data.
    pub(crate) trie_input: OnceLock<TrieInput>,
}

impl<'a, N: NodePrimitives> MemoryOverlayStateProviderRef<'a, N> {
    /// Create new memory overlay state provider.
    ///
    /// ## Arguments
    ///
    /// - `in_memory` - the collection of executed ancestor blocks in reverse.
    /// - `historical` - a historical state provider for the latest ancestor block stored in the
    ///   database.
    pub fn new(historical: Box<dyn StateProvider + 'a>, in_memory: Vec<ExecutedBlock<N>>) -> Self {
        Self { historical, in_memory: Cow::Owned(in_memory), trie_input: OnceLock::new() }
    }

    /// Turn this state provider into a state provider
    pub fn boxed(self) -> Box<dyn StateProvider + 'a> {
        Box::new(self)
    }

    /// Return lazy-loaded trie state aggregated from in-memory blocks.
    fn trie_input(&self) -> &TrieInput {
        self.trie_input.get_or_init(|| {
            let mut input = TrieInput::default();
            // Iterate from oldest to newest
            for block in self.in_memory.iter().rev() {
                let data = block.trie_data();
                input.nodes.extend_from_sorted(&data.trie_updates);
                input.state.extend_from_sorted(&data.hashed_state);
            }
            input
        })
    }

    fn merged_hashed_storage(&self, address: Address, storage: HashedStorage) -> HashedStorage {
        let state = &self.trie_input().state;
        let mut hashed = state.storages.get(&keccak256(address)).cloned().unwrap_or_default();
        hashed.extend(&storage);
        hashed
    }

    /// Returns the cumulative flattened [`BundleState`] from the newest
    /// in-memory block, but only when its anchor matches the current
    /// persistence boundary.
    ///
    /// The anchor check compares the newest block's flattened-state anchor
    /// against the oldest in-memory block's parent hash (the persisted base
    /// this overlay sits on). A mismatch indicates a persistence event
    /// occurred after the flattened state was computed; in that case the
    /// overlay must not be reused, and callers fall back to per-block
    /// iteration.
    ///
    /// Returns `None` when:
    /// - `in_memory` is empty
    /// - the oldest block's parent hash is `B256::ZERO` (no real anchor)
    /// - the newest block carries a default/unpopulated flattened state
    /// - persistence has shifted the anchor since the overlay was built
    fn flattened_overlay(&self) -> Option<Arc<BundleState>> {
        let newest = self.in_memory.first()?;
        let oldest = self.in_memory.last()?;
        let expected_anchor = oldest.recovered_block.parent_hash();
        if expected_anchor == B256::ZERO {
            return None;
        }
        let computed = newest.flattened_state.wait_cloned();
        (computed.anchor_hash == expected_anchor).then_some(computed.bundle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComputedBundleState, DeferredBundleState, DeferredTrieData, ExecutedBlock};
    use alloy_consensus::Header;
    use alloy_primitives::U256;
    use reth_chainspec::ChainSpec;
    use reth_ethereum_primitives::{Block, BlockBody, EthPrimitives, Receipt};
    use reth_execution_types::BlockExecutionOutput;
    use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
    use reth_storage_api::noop::NoopProvider;
    use revm_state::AccountInfo;

    fn make_recovered(number: u64, parent_hash: B256) -> Arc<RecoveredBlock<Block>> {
        let header = Header { parent_hash, number, ..Default::default() };
        let sealed =
            SealedBlock::from_sealed_parts(SealedHeader::seal_slow(header), BlockBody::default());
        Arc::new(RecoveredBlock::new_sealed(sealed, Vec::new()))
    }

    fn executed_block_with(
        parent_hash: B256,
        block_number: u64,
        account_addr: Address,
        balance: u64,
        flattened: Option<ComputedBundleState>,
    ) -> ExecutedBlock<EthPrimitives> {
        let info =
            AccountInfo { balance: U256::from(balance), nonce: block_number, ..Default::default() };
        let bundle = BundleState::builder(block_number..=block_number)
            .state_present_account_info(account_addr, info)
            .build();

        let execution_output =
            Arc::new(BlockExecutionOutput::<Receipt> { result: Default::default(), state: bundle });

        let flattened_state = match flattened {
            Some(c) => DeferredBundleState::ready(c),
            None => DeferredBundleState::default(),
        };

        ExecutedBlock {
            recovered_block: make_recovered(block_number, parent_hash),
            execution_output,
            trie_data: DeferredTrieData::ready(Default::default()),
            flattened_state,
        }
    }

    fn make_ref<'a>(
        blocks: &'a [ExecutedBlock<EthPrimitives>],
    ) -> MemoryOverlayStateProviderRef<'a, EthPrimitives> {
        let noop: NoopProvider<ChainSpec, EthPrimitives> = NoopProvider::default();
        MemoryOverlayStateProviderRef {
            historical: Box::new(noop),
            in_memory: Cow::Borrowed(blocks),
            trie_input: OnceLock::new(),
        }
    }

    #[test]
    fn flattened_overlay_available_when_anchor_matches() {
        let anchor = B256::repeat_byte(0xAA);
        let addr = Address::repeat_byte(0x01);

        // Build chain: block1 parent = anchor, block2 parent = block1.hash()
        let block1 = executed_block_with(anchor, 1, addr, 10, None);
        let block1_hash = block1.recovered_block.hash();

        // Flattened bundle covering block1 + block2, anchored to `anchor`.
        let info2 = AccountInfo { balance: U256::from(20), nonce: 2, ..Default::default() };
        let mut cumulative = block1.execution_output.state.clone();
        cumulative
            .extend(BundleState::builder(2..=2).state_present_account_info(addr, info2).build());
        let flat = ComputedBundleState::new(Arc::new(cumulative), anchor);

        let block2 = executed_block_with(block1_hash, 2, addr, 20, Some(flat));

        // newest-to-oldest
        let blocks = vec![block2, block1];
        let provider = make_ref(&blocks);

        // Sanity: anchor check succeeds.
        assert!(provider.flattened_overlay().is_some());

        // Lookup via fast path returns the *newest* value.
        let acc = provider.basic_account(&addr).unwrap().expect("account present");
        assert_eq!(acc.balance, U256::from(20));
    }

    #[test]
    fn flattened_overlay_skipped_on_anchor_mismatch() {
        let addr = Address::repeat_byte(0x02);
        let real_anchor = B256::repeat_byte(0xBB);
        let stale_anchor = B256::repeat_byte(0xCC);

        let block1 = executed_block_with(real_anchor, 1, addr, 100, None);
        let block1_hash = block1.recovered_block.hash();

        // Flattened anchored to a DIFFERENT, stale anchor.
        let cumulative = block1.execution_output.state.clone();
        let flat = ComputedBundleState::new(Arc::new(cumulative), stale_anchor);
        let block2 = executed_block_with(block1_hash, 2, addr, 200, Some(flat));

        let blocks = vec![block2, block1];
        let provider = make_ref(&blocks);

        // Anchor mismatch: fast path must not activate.
        assert!(provider.flattened_overlay().is_none());

        // Correctness preserved via iteration fallback (newest wins).
        let acc = provider.basic_account(&addr).unwrap().expect("account present");
        assert_eq!(acc.balance, U256::from(200));
    }

    #[test]
    fn flattened_overlay_skipped_when_anchor_is_zero() {
        let addr = Address::repeat_byte(0x03);
        // Oldest block's parent_hash is B256::ZERO (genesis-like edge case).
        let block1 = executed_block_with(B256::ZERO, 1, addr, 5, None);
        let block1_hash = block1.recovered_block.hash();
        let flat =
            ComputedBundleState::new(Arc::new(block1.execution_output.state.clone()), B256::ZERO);
        let block2 = executed_block_with(block1_hash, 2, addr, 7, Some(flat));

        let blocks = vec![block2, block1];
        let provider = make_ref(&blocks);

        assert!(provider.flattened_overlay().is_none());
    }
}

impl<N: NodePrimitives> BlockHashReader for MemoryOverlayStateProviderRef<'_, N> {
    fn block_hash(&self, number: BlockNumber) -> ProviderResult<Option<B256>> {
        for block in self.in_memory.iter() {
            if block.recovered_block().number() == number {
                return Ok(Some(block.recovered_block().hash()));
            }
        }

        self.historical.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        let range = start..end;
        let mut earliest_block_number = None;
        let mut in_memory_hashes = Vec::with_capacity(range.size_hint().0);

        // iterate in ascending order (oldest to newest = low to high)
        for block in self.in_memory.iter() {
            let block_num = block.recovered_block().number();
            if range.contains(&block_num) {
                in_memory_hashes.push(block.recovered_block().hash());
                earliest_block_number = Some(block_num);
            }
        }

        // `self.in_memory` stores executed blocks in ascending order (oldest to newest).
        // However, `in_memory_hashes` should be constructed in descending order (newest to oldest),
        // so we reverse the vector after collecting the hashes.
        in_memory_hashes.reverse();

        let mut hashes =
            self.historical.canonical_hashes_range(start, earliest_block_number.unwrap_or(end))?;
        hashes.append(&mut in_memory_hashes);
        Ok(hashes)
    }
}

impl<N: NodePrimitives> AccountReader for MemoryOverlayStateProviderRef<'_, N> {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        // Fast path: single HashMap probe against the cumulative flattened overlay.
        if let Some(bundle) = self.flattened_overlay() {
            return match bundle.account(address) {
                Some(bundle_account) => Ok(bundle_account.info.as_ref().map(Into::into)),
                None => self.historical.basic_account(address),
            };
        }

        // Fallback: iterate newest-to-oldest when the flattened overlay is
        // unavailable (no anchor, persistence boundary, legacy construction).
        for block in self.in_memory.iter() {
            if let Some(account) = block.execution_output.account(address) {
                return Ok(account);
            }
        }

        self.historical.basic_account(address)
    }
}

impl<N: NodePrimitives> StateRootProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn state_root(&self, state: HashedPostState) -> ProviderResult<B256> {
        self.state_root_from_nodes(TrieInput::from_state(state))
    }

    fn state_root_from_nodes(&self, mut input: TrieInput) -> ProviderResult<B256> {
        input.prepend_self(self.trie_input().clone());
        self.historical.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.state_root_from_nodes_with_updates(TrieInput::from_state(state))
    }

    fn state_root_from_nodes_with_updates(
        &self,
        mut input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        input.prepend_self(self.trie_input().clone());
        self.historical.state_root_from_nodes_with_updates(input)
    }
}

impl<N: NodePrimitives> StorageRootProvider for MemoryOverlayStateProviderRef<'_, N> {
    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_root(&self, address: Address, storage: HashedStorage) -> ProviderResult<B256> {
        let merged = self.merged_hashed_storage(address, storage);
        self.historical.storage_root(address, merged)
    }

    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        let merged = self.merged_hashed_storage(address, storage);
        self.historical.storage_proof(address, slot, merged)
    }

    // TODO: Currently this does not reuse available in-memory trie nodes.
    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        let merged = self.merged_hashed_storage(address, storage);
        self.historical.storage_multiproof(address, slots, merged)
    }
}

impl<N: NodePrimitives> StateProofProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn proof(
        &self,
        mut input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        input.prepend_self(self.trie_input().clone());
        self.historical.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        mut input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        input.prepend_self(self.trie_input().clone());
        self.historical.multiproof(input, targets)
    }

    fn witness(
        &self,
        mut input: TrieInput,
        target: HashedPostState,
        mode: reth_trie::ExecutionWitnessMode,
    ) -> ProviderResult<Vec<Bytes>> {
        input.prepend_self(self.trie_input().clone());
        self.historical.witness(input, target, mode)
    }
}

impl<N: NodePrimitives> HashedPostStateProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        self.historical.hashed_post_state(bundle_state)
    }
}

impl<N: NodePrimitives> StateProvider for MemoryOverlayStateProviderRef<'_, N> {
    fn storage(
        &self,
        address: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        if let Some(bundle) = self.flattened_overlay() {
            if let Some(account) = bundle.account(&address) &&
                let Some(value) = account.storage_slot(storage_key.into())
            {
                return Ok(Some(value));
            }
            return self.historical.storage(address, storage_key);
        }

        for block in self.in_memory.iter() {
            if let Some(value) = block.execution_output.storage(&address, storage_key.into()) {
                return Ok(Some(value));
            }
        }

        self.historical.storage(address, storage_key)
    }
}

impl<N: NodePrimitives> BytecodeReader for MemoryOverlayStateProviderRef<'_, N> {
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        if let Some(bundle) = self.flattened_overlay() {
            if let Some(contract) = bundle.bytecode(code_hash) {
                return Ok(Some(Bytecode(contract)));
            }
            return self.historical.bytecode_by_hash(code_hash);
        }

        for block in self.in_memory.iter() {
            if let Some(contract) = block.execution_output.bytecode(code_hash) {
                return Ok(Some(contract));
            }
        }

        self.historical.bytecode_by_hash(code_hash)
    }
}

/// An owned state provider that stores references to in-memory blocks along with their state as
/// well as a reference of the historical state provider for fallback lookups.
#[expect(missing_debug_implementations)]
pub struct MemoryOverlayStateProvider<N: NodePrimitives = reth_ethereum_primitives::EthPrimitives> {
    /// Historical state provider for state lookups that are not found in memory blocks.
    pub(crate) historical: StateProviderBox,
    /// The collection of executed parent blocks. Expected order is newest to oldest.
    pub(crate) in_memory: Vec<ExecutedBlock<N>>,
    /// Lazy-loaded in-memory trie data.
    pub(crate) trie_input: OnceLock<TrieInput>,
}

impl<N: NodePrimitives> MemoryOverlayStateProvider<N> {
    /// Create new memory overlay state provider.
    ///
    /// ## Arguments
    ///
    /// - `in_memory` - the collection of executed ancestor blocks in reverse.
    /// - `historical` - a historical state provider for the latest ancestor block stored in the
    ///   database.
    pub fn new(historical: StateProviderBox, in_memory: Vec<ExecutedBlock<N>>) -> Self {
        Self { historical, in_memory, trie_input: OnceLock::new() }
    }

    /// Returns a new provider that takes the `TX` as reference
    #[inline(always)]
    fn as_ref(&self) -> MemoryOverlayStateProviderRef<'_, N> {
        MemoryOverlayStateProviderRef {
            historical: Box::new(self.historical.as_ref()),
            in_memory: Cow::Borrowed(&self.in_memory),
            trie_input: self.trie_input.clone(),
        }
    }

    /// Wraps the [`Self`] in a `Box`.
    pub fn boxed(self) -> StateProviderBox {
        Box::new(self)
    }
}

// Delegates all provider impls to [`MemoryOverlayStateProviderRef`]
reth_storage_api::macros::delegate_provider_impls!(MemoryOverlayStateProvider<N> where [N: NodePrimitives]);
