//! Benchmarks for `MemoryOverlayStateProvider` state-read hot paths.
//!
//! Two modes are compared:
//! - **legacy**: every in-memory block carries a default/empty `DeferredBundleState`, forcing the
//!   provider onto the historical O(N) iteration path for every lookup.
//! - **flattened**: the newest in-memory block carries a pre-computed cumulative
//!   `DeferredBundleState` anchored to the oldest block's parent hash, enabling the O(1) fast path
//!   introduced for #20612.
#![allow(missing_docs, unreachable_pub)]

use alloy_consensus::{BlockHeader, Header};
use alloy_primitives::{keccak256, map::U256Map, Address, B256, U256};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use reth_chain_state::{
    ComputedBundleState, ComputedTrieData, DeferredBundleState, DeferredTrieData, ExecutedBlock,
};
use reth_chainspec::ChainSpec;
use reth_ethereum_primitives::{Block, BlockBody, EthPrimitives, Receipt};
use reth_execution_types::BlockExecutionOutput;
use reth_primitives_traits::{RecoveredBlock, SealedBlock, SealedHeader};
use reth_storage_api::{noop::NoopProvider, AccountReader, BytecodeReader, StateProvider};
use revm_database::BundleState;
use revm_state::AccountInfo;
use std::{hint::black_box, sync::Arc};

const ACCOUNTS_PER_BLOCK: usize = 100;

/// Deterministic, well-distributed address derived from `(block_idx, acct_idx)`.
///
/// Hashed through keccak so the resulting addresses don't cluster in a way
/// that would defeat `FbBuildHasher` (alloy's `FxHasher`-based hasher is
/// sensitive to low-entropy inputs, e.g. addresses with many zero bytes).
fn make_address(block_idx: usize, acct_idx: usize) -> Address {
    let mut seed = [0u8; 16];
    seed[0..8].copy_from_slice(&(block_idx as u64).to_be_bytes());
    seed[8..16].copy_from_slice(&(acct_idx as u64).to_be_bytes());
    let hash = keccak256(seed);
    Address::from_slice(&hash[12..32])
}

/// Deterministic, well-distributed bytecode hash.
fn make_code_hash(block_idx: usize, acct_idx: usize) -> B256 {
    let mut seed = [0u8; 24];
    seed[0..8].copy_from_slice(&(block_idx as u64).to_be_bytes());
    seed[8..16].copy_from_slice(&(acct_idx as u64).to_be_bytes());
    seed[16..24].copy_from_slice(b"codehash");
    keccak256(seed)
}

/// Build a single block's `BundleState` with `ACCOUNTS_PER_BLOCK` unique touched accounts.
fn make_bundle(block_idx: usize, block_number: u64) -> BundleState {
    let mut builder = BundleState::builder(block_number..=block_number);
    for acct_idx in 0..ACCOUNTS_PER_BLOCK {
        let addr = make_address(block_idx, acct_idx);
        let info = AccountInfo {
            balance: U256::from(acct_idx as u64 + 1),
            nonce: block_number,
            code_hash: make_code_hash(block_idx, acct_idx),
            ..Default::default()
        };
        builder = builder.state_present_account_info(addr, info);

        let slot_key = U256::from(acct_idx as u64);
        let slot_value = U256::from(block_number);
        let mut slots: U256Map<(U256, U256)> = U256Map::default();
        slots.insert(slot_key, (U256::ZERO, slot_value));
        builder = builder.state_storage(addr, slots);
    }
    builder.build()
}

/// Construct a minimal `RecoveredBlock` whose header carries the given
/// `parent_hash` and `number`. All other fields are defaults.
fn make_recovered(number: u64, parent_hash: B256) -> Arc<RecoveredBlock<Block>> {
    let header = Header { parent_hash, number, ..Default::default() };
    let sealed_block =
        SealedBlock::from_sealed_parts(SealedHeader::seal_slow(header), BlockBody::default());
    Arc::new(RecoveredBlock::new_sealed(sealed_block, Vec::new()))
}

/// Build `n` in-memory blocks, newest-to-oldest, with a real anchor + chain of
/// `parent_hash` links so the flattened fast path's anchor validation can succeed.
///
/// If `flattened` is true, the newest block's `flattened_state` is populated
/// with the cumulative bundle from all `n` blocks, anchored to `anchor`. All
/// older blocks carry default flattened states.
fn make_blocks(n: usize, anchor: B256, flattened: bool) -> Vec<ExecutedBlock<EthPrimitives>> {
    let mut blocks: Vec<ExecutedBlock<EthPrimitives>> = Vec::with_capacity(n);
    let mut parent_hash = anchor;
    let mut cumulative: BundleState = BundleState::default();

    for i in 0..n {
        let block_number = i as u64 + 1;
        let bundle = make_bundle(i, block_number);
        // Maintain the cumulative flattened state for the newest block.
        cumulative.extend(bundle.clone());

        let recovered = make_recovered(block_number, parent_hash);
        let next_parent_hash = recovered.hash();

        let execution_output =
            Arc::new(BlockExecutionOutput::<Receipt> { result: Default::default(), state: bundle });

        // Only the last-constructed block (newest) gets the populated overlay.
        let flattened_state = if flattened && i == n - 1 {
            DeferredBundleState::ready(ComputedBundleState::new(
                Arc::new(cumulative.clone()),
                anchor,
            ))
        } else {
            DeferredBundleState::default()
        };

        blocks.push(ExecutedBlock {
            recovered_block: recovered,
            execution_output,
            trie_data: DeferredTrieData::ready(ComputedTrieData::default()),
            flattened_state,
        });

        parent_hash = next_parent_hash;
    }

    // Provider expects newest-to-oldest ordering.
    blocks.reverse();
    blocks
}

fn make_provider(
    blocks: Vec<ExecutedBlock<EthPrimitives>>,
) -> reth_chain_state::MemoryOverlayStateProvider<EthPrimitives> {
    let noop: NoopProvider<ChainSpec, EthPrimitives> = NoopProvider::default();
    reth_chain_state::MemoryOverlayStateProvider::new(Box::new(noop), blocks)
}

fn bench_basic_account(c: &mut Criterion) {
    let anchor = B256::repeat_byte(0xEE);
    let mut group = c.benchmark_group("MemoryOverlay/basic_account");

    for &num_blocks in &[8usize, 32, 64] {
        let newest_block_idx = num_blocks - 1;
        let oldest_block_idx = 0;

        let addr_newest = make_address(newest_block_idx, ACCOUNTS_PER_BLOCK / 2);
        let addr_oldest = make_address(oldest_block_idx, ACCOUNTS_PER_BLOCK / 2);
        let addr_miss = make_address(num_blocks + 1, 0);

        for &flattened in &[false, true] {
            let variant = if flattened { "flattened" } else { "legacy" };
            let blocks = make_blocks(num_blocks, anchor, flattened);

            // Sanity check: verify the flattened bundle resolves both newest
            // and oldest addresses before benchmarking. A miss here would mean
            // the bench setup is wrong and numbers would be meaningless.
            if flattened {
                let newest_block = &blocks[0];
                let oldest_block = &blocks[num_blocks - 1];
                let flat = newest_block.flattened_state.wait_cloned();
                assert_eq!(
                    oldest_block.recovered_block.parent_hash(),
                    flat.anchor_hash,
                    "bench invariant: oldest.parent_hash must equal newest.flattened.anchor"
                );
                assert!(
                    flat.bundle.account(&addr_newest).is_some(),
                    "addr_newest must be in cumulative bundle"
                );
                assert!(
                    flat.bundle.account(&addr_oldest).is_some(),
                    "addr_oldest must be in cumulative bundle"
                );
            }

            let provider = make_provider(blocks);

            group.bench_function(
                BenchmarkId::new(format!("{variant}/hit_newest"), num_blocks),
                |b| {
                    b.iter(|| {
                        let _ = black_box(provider.basic_account(&black_box(addr_newest)).unwrap());
                    });
                },
            );

            group.bench_function(
                BenchmarkId::new(format!("{variant}/hit_oldest"), num_blocks),
                |b| {
                    b.iter(|| {
                        let _ = black_box(provider.basic_account(&black_box(addr_oldest)).unwrap());
                    });
                },
            );

            group.bench_function(BenchmarkId::new(format!("{variant}/miss"), num_blocks), |b| {
                b.iter(|| {
                    let _ = black_box(provider.basic_account(&black_box(addr_miss)).unwrap());
                });
            });
        }
    }

    group.finish();
}

fn bench_storage(c: &mut Criterion) {
    let anchor = B256::repeat_byte(0xEE);
    let mut group = c.benchmark_group("MemoryOverlay/storage");

    for &num_blocks in &[8usize, 32, 64] {
        let newest_block_idx = num_blocks - 1;
        let oldest_block_idx = 0;

        let slot = B256::from(U256::from((ACCOUNTS_PER_BLOCK / 2) as u64));
        let addr_newest = make_address(newest_block_idx, ACCOUNTS_PER_BLOCK / 2);
        let addr_oldest = make_address(oldest_block_idx, ACCOUNTS_PER_BLOCK / 2);
        let addr_miss = make_address(num_blocks + 1, 0);

        for &flattened in &[false, true] {
            let variant = if flattened { "flattened" } else { "legacy" };
            let provider = make_provider(make_blocks(num_blocks, anchor, flattened));

            group.bench_function(
                BenchmarkId::new(format!("{variant}/hit_newest"), num_blocks),
                |b| {
                    b.iter(|| {
                        let _ = black_box(
                            provider.storage(black_box(addr_newest), black_box(slot)).unwrap(),
                        );
                    });
                },
            );

            group.bench_function(
                BenchmarkId::new(format!("{variant}/hit_oldest"), num_blocks),
                |b| {
                    b.iter(|| {
                        let _ = black_box(
                            provider.storage(black_box(addr_oldest), black_box(slot)).unwrap(),
                        );
                    });
                },
            );

            group.bench_function(BenchmarkId::new(format!("{variant}/miss"), num_blocks), |b| {
                b.iter(|| {
                    let _ =
                        black_box(provider.storage(black_box(addr_miss), black_box(slot)).unwrap());
                });
            });
        }
    }

    group.finish();
}

fn bench_bytecode_miss(c: &mut Criterion) {
    let anchor = B256::repeat_byte(0xEE);
    let mut group = c.benchmark_group("MemoryOverlay/bytecode_by_hash");

    for &num_blocks in &[8usize, 32, 64] {
        let hash = B256::repeat_byte(0xAB);
        for &flattened in &[false, true] {
            let variant = if flattened { "flattened" } else { "legacy" };
            let provider = make_provider(make_blocks(num_blocks, anchor, flattened));

            group.bench_function(BenchmarkId::new(format!("{variant}/miss"), num_blocks), |b| {
                b.iter(|| {
                    let _ = black_box(provider.bytecode_by_hash(&black_box(hash)).unwrap());
                });
            });
        }
    }

    group.finish();
}

criterion_group!(memory_overlay_benches, bench_basic_account, bench_storage, bench_bytecode_miss);
criterion_main!(memory_overlay_benches);
