//! Deferred asynchronous flattening of in-memory [`BundleState`] overlays.
//!
//! Mirrors [`DeferredTrieData`](crate::DeferredTrieData) in shape and invariants:
//! each in-memory block produces a handle that can be computed asynchronously
//! in the background and accessed with a synchronous fallback. The flattened
//! bundle aggregates all in-memory ancestor state changes into a single
//! [`BundleState`], enabling O(1) account/storage/bytecode lookups in
//! [`MemoryOverlayStateProvider`](crate::MemoryOverlayStateProvider) instead of
//! O(N) iteration across every in-memory block.

use alloy_primitives::B256;
use parking_lot::Mutex;
use reth_metrics::{metrics::Counter, Metrics};
use revm_database::BundleState;
use std::{
    fmt,
    sync::{Arc, LazyLock},
};
use tracing::{debug_span, instrument};

/// Shared handle to an asynchronously-flattened [`BundleState`] overlay.
///
/// Uses a try-lock + fallback computation approach for deadlock-free access.
/// If the deferred task hasn't completed, computes the flattened bundle
/// synchronously from stored pending inputs rather than blocking.
#[derive(Clone)]
pub struct DeferredBundleState {
    /// Shared deferred state holding either raw inputs (pending) or the computed result (ready).
    state: Arc<Mutex<DeferredBundleStateInner>>,
}

/// Flattened bundle covering all in-memory ancestor blocks plus the owning block.
///
/// The `bundle` is an `Arc`-wrapped [`BundleState`] so that child blocks can
/// reuse a parent's flattened overlay via structural sharing (`Arc::make_mut`).
/// The `anchor_hash` identifies the persisted base state this overlay sits on
/// top of; overlays anchored to different base states are not interchangeable.
#[derive(Clone, Debug, Default)]
pub struct ComputedBundleState {
    /// Cumulative flattened bundle for all in-memory ancestors + this block.
    pub bundle: Arc<BundleState>,
    /// The persisted ancestor hash this overlay is anchored to.
    pub anchor_hash: B256,
}

impl ComputedBundleState {
    /// Construct a new [`ComputedBundleState`] anchored to the given persisted ancestor.
    pub const fn new(bundle: Arc<BundleState>, anchor_hash: B256) -> Self {
        Self { bundle, anchor_hash }
    }
}

/// Metrics for deferred bundle flattening.
#[derive(Metrics)]
#[metrics(scope = "sync.block_validation")]
struct DeferredBundleMetrics {
    /// Number of times the flattened bundle was ready when a consumer asked
    /// (the async background task finished first).
    deferred_bundle_async_ready: Counter,
    /// Number of times a consumer had to fall back to the synchronous path.
    deferred_bundle_sync_fallback: Counter,
}

static DEFERRED_BUNDLE_METRICS: LazyLock<DeferredBundleMetrics> =
    LazyLock::new(DeferredBundleMetrics::default);

/// Internal state machine for [`DeferredBundleState`].
enum DeferredBundleStateInner {
    /// Flattened bundle has not been computed yet. Pending inputs are wrapped
    /// in [`Option`] so they can be taken by a synchronous-fallback caller
    /// without requiring the inputs to be [`Clone`].
    Pending(Option<PendingBundleInputs>),
    /// Flattened bundle has been computed and is ready to return.
    Ready(ComputedBundleState),
}

/// Inputs held while a deferred bundle flattening is pending.
struct PendingBundleInputs {
    /// This block's own post-execution bundle state (from
    /// [`BlockExecutionOutput::state`](reth_execution_types::BlockExecutionOutput)).
    this_block_state: Arc<BundleState>,
    /// Persisted ancestor hash this overlay is anchored to.
    anchor_hash: B256,
    /// Parent block's deferred bundle handle. When present and anchored to the
    /// same base state, the parent's flattened bundle is reused rather than
    /// recomputed from scratch.
    parent: Option<DeferredBundleState>,
}

impl fmt::Debug for DeferredBundleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        let tag = match &*state {
            DeferredBundleStateInner::Pending(_) => "pending",
            DeferredBundleStateInner::Ready(_) => "ready",
        };
        f.debug_struct("DeferredBundleState").field("state", &tag).finish()
    }
}

impl Default for DeferredBundleState {
    fn default() -> Self {
        Self::ready(ComputedBundleState::default())
    }
}

impl DeferredBundleState {
    /// Create a new pending handle with fallback inputs for synchronous computation.
    ///
    /// The flattened bundle will be computed lazily the first time
    /// [`Self::wait_cloned`] is called.
    ///
    /// # Arguments
    /// * `this_block_state` - This block's post-execution [`BundleState`].
    /// * `anchor_hash` - The persisted ancestor hash this overlay is anchored to.
    /// * `parent` - Parent block's deferred bundle handle, if any.
    pub fn pending(
        this_block_state: Arc<BundleState>,
        anchor_hash: B256,
        parent: Option<Self>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(DeferredBundleStateInner::Pending(Some(
                PendingBundleInputs { this_block_state, anchor_hash, parent },
            )))),
        }
    }

    /// Create a handle that is already populated with a pre-computed flattened bundle.
    pub fn ready(computed: ComputedBundleState) -> Self {
        Self { state: Arc::new(Mutex::new(DeferredBundleStateInner::Ready(computed))) }
    }

    /// Returns the flattened bundle, computing synchronously if the async task
    /// hasn't completed.
    ///
    /// - If the async task has completed (`Ready`), returns the cached result.
    /// - If still pending, computes from stored inputs and caches the result so subsequent calls
    ///   return immediately.
    ///
    /// Deadlock is avoided as long as the provided ancestor chain forms a DAG
    /// (each block only holds handles to its ancestors). Sibling blocks are
    /// never in each other's ancestor chains.
    #[instrument(level = "debug", target = "engine::tree::deferred_bundle", skip_all)]
    pub fn wait_cloned(&self) -> ComputedBundleState {
        let mut state = self.state.lock();
        match &mut *state {
            DeferredBundleStateInner::Ready(computed) => {
                DEFERRED_BUNDLE_METRICS.deferred_bundle_async_ready.increment(1);
                computed.clone()
            }
            DeferredBundleStateInner::Pending(maybe_inputs) => {
                DEFERRED_BUNDLE_METRICS.deferred_bundle_sync_fallback.increment(1);

                let inputs = maybe_inputs.take().expect("inputs must be present in Pending state");

                let computed = Self::flatten_inputs(
                    inputs.this_block_state,
                    inputs.anchor_hash,
                    inputs.parent,
                );
                *state = DeferredBundleStateInner::Ready(computed.clone());

                // Release the lock before the old `inputs` (and its potential
                // last parent Arc) are dropped, to avoid holding this lock while
                // destructors run on related handles.
                drop(state);

                computed
            }
        }
    }

    /// Flatten the current block's bundle onto the parent's cumulative overlay.
    ///
    /// # Reuse strategy
    /// - **Parent present + anchor match**: clone the parent's `Arc<BundleState>` (cheap) and call
    ///   `Arc::make_mut` to get exclusive access for extension. If the parent's Arc refcount is 1,
    ///   no allocation happens; otherwise the backing state is cloned once (typical on reorg
    ///   boundaries or when multiple consumers share the parent's handle).
    /// - **Parent present + anchor mismatch**: discard the parent overlay — it is anchored to a
    ///   different base state and cannot be reused safely. Fall through to using just this block's
    ///   own state.
    /// - **No parent**: first block after the persisted anchor; this block's own state IS the
    ///   cumulative overlay.
    ///
    /// In all cases, the resulting [`ComputedBundleState`] is anchored to
    /// `anchor_hash`.
    fn flatten_inputs(
        this_block_state: Arc<BundleState>,
        anchor_hash: B256,
        parent: Option<Self>,
    ) -> ComputedBundleState {
        let _span = debug_span!(target: "engine::tree::deferred_bundle", "flatten").entered();

        let bundle = match parent {
            Some(parent_handle) => {
                let parent_computed = parent_handle.wait_cloned();
                if parent_computed.anchor_hash == anchor_hash {
                    // Reuse parent's cumulative bundle, extend with this block's diff.
                    // `other` wins on conflicts per `BundleState::extend` semantics,
                    // which matches the "newest overrides older" rule used by the
                    // existing O(N) MemoryOverlayStateProvider iteration.
                    let mut overlay = parent_computed.bundle;
                    let bundle_mut = Arc::make_mut(&mut overlay);
                    let this_owned = match Arc::try_unwrap(this_block_state) {
                        Ok(state) => state,
                        Err(arc) => (*arc).clone(),
                    };
                    bundle_mut.extend(this_owned);
                    overlay
                } else {
                    // Anchor mismatch: parent's overlay was built against a
                    // different persisted base (e.g. after a persistence event
                    // changed what's on disk). Using it would yield incorrect
                    // lookups, so fall back to this block's own state and let
                    // the historical provider serve older state.
                    this_block_state
                }
            }
            None => this_block_state,
        };

        ComputedBundleState { bundle, anchor_hash }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};
    use revm_state::AccountInfo;
    use std::{thread, time::Duration};

    fn addr(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    fn anchor(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    fn bundle_with_account(block_number: u64, address: Address, balance: u64) -> Arc<BundleState> {
        let info =
            AccountInfo { balance: U256::from(balance), nonce: block_number, ..Default::default() };
        let bundle = BundleState::builder(block_number..=block_number)
            .state_present_account_info(address, info)
            .build();
        Arc::new(bundle)
    }

    fn balance_of(bundle: &BundleState, address: Address) -> Option<U256> {
        bundle.account(&address).and_then(|acc| acc.info.as_ref().map(|i| i.balance))
    }

    #[test]
    fn ready_returns_cached_result() {
        let computed =
            ComputedBundleState::new(bundle_with_account(1, addr(0x11), 100), anchor(0xAA));
        let handle = DeferredBundleState::ready(computed);

        let got = handle.wait_cloned();
        assert_eq!(got.anchor_hash, anchor(0xAA));
        assert_eq!(balance_of(&got.bundle, addr(0x11)), Some(U256::from(100)));
    }

    #[test]
    fn pending_without_parent_returns_own_state() {
        let handle = DeferredBundleState::pending(
            bundle_with_account(1, addr(0x22), 42),
            anchor(0xBB),
            None,
        );

        let got = handle.wait_cloned();
        assert_eq!(got.anchor_hash, anchor(0xBB));
        assert_eq!(balance_of(&got.bundle, addr(0x22)), Some(U256::from(42)));
    }

    #[test]
    fn fallback_result_is_cached_on_second_call() {
        let handle =
            DeferredBundleState::pending(bundle_with_account(1, addr(0x33), 7), anchor(0xCC), None);

        let first = handle.wait_cloned();
        let second = handle.wait_cloned();

        // Both calls observe the same Arc (second call hits the Ready branch).
        assert!(Arc::ptr_eq(&first.bundle, &second.bundle));
    }

    #[test]
    fn parent_same_anchor_extends_and_overrides() {
        // Parent block sets addr(0xAA) balance = 100.
        let parent = DeferredBundleState::pending(
            bundle_with_account(1, addr(0xAA), 100),
            anchor(0x01),
            None,
        );

        // Child block overrides addr(0xAA) balance = 200 and introduces addr(0xBB).
        let child_state = {
            let info_aa = AccountInfo { balance: U256::from(200), nonce: 2, ..Default::default() };
            let info_bb = AccountInfo { balance: U256::from(50), nonce: 0, ..Default::default() };
            let bundle = BundleState::builder(2..=2)
                .state_present_account_info(addr(0xAA), info_aa)
                .state_present_account_info(addr(0xBB), info_bb)
                .build();
            Arc::new(bundle)
        };
        let child = DeferredBundleState::pending(child_state, anchor(0x01), Some(parent));

        let computed = child.wait_cloned();
        // Child wins on addr(0xAA).
        assert_eq!(balance_of(&computed.bundle, addr(0xAA)), Some(U256::from(200)));
        // Parent-only account survives.
        assert_eq!(balance_of(&computed.bundle, addr(0xBB)), Some(U256::from(50)));
        assert_eq!(computed.anchor_hash, anchor(0x01));
    }

    #[test]
    fn parent_mismatched_anchor_is_ignored() {
        // Parent is anchored to anchor(0x01), contains addr(0xCC).
        let parent = DeferredBundleState::pending(
            bundle_with_account(1, addr(0xCC), 999),
            anchor(0x01),
            None,
        );

        // Child claims anchor(0x02) — different base. Parent's overlay must be dropped.
        let child = DeferredBundleState::pending(
            bundle_with_account(2, addr(0xDD), 1),
            anchor(0x02),
            Some(parent),
        );

        let computed = child.wait_cloned();
        assert_eq!(computed.anchor_hash, anchor(0x02));
        // Parent-only account should NOT appear — anchor mismatch caused fallback.
        assert_eq!(balance_of(&computed.bundle, addr(0xCC)), None);
        // Child's own state is present.
        assert_eq!(balance_of(&computed.bundle, addr(0xDD)), Some(U256::from(1)));
    }

    #[test]
    fn siblings_off_shared_parent_do_not_corrupt_each_other() {
        // Shared parent with addr(0xEE) = 10.
        let parent = DeferredBundleState::pending(
            bundle_with_account(1, addr(0xEE), 10),
            anchor(0x01),
            None,
        );

        // Pre-compute the parent so both siblings reuse the cached Arc.
        let _ = parent.wait_cloned();

        // Sibling A sets addr(0xEE) = 100.
        let sibling_a_state = bundle_with_account(2, addr(0xEE), 100);
        let sibling_a =
            DeferredBundleState::pending(sibling_a_state, anchor(0x01), Some(parent.clone()));

        // Sibling B sets addr(0xEE) = 200.
        let sibling_b_state = bundle_with_account(2, addr(0xEE), 200);
        let sibling_b =
            DeferredBundleState::pending(sibling_b_state, anchor(0x01), Some(parent.clone()));

        let a_computed = sibling_a.wait_cloned();
        let b_computed = sibling_b.wait_cloned();

        assert_eq!(balance_of(&a_computed.bundle, addr(0xEE)), Some(U256::from(100)));
        assert_eq!(balance_of(&b_computed.bundle, addr(0xEE)), Some(U256::from(200)));

        // Parent must still observe its original value; its cached bundle
        // must not have been mutated by either child's `make_mut`.
        let parent_again = parent.wait_cloned();
        assert_eq!(balance_of(&parent_again.bundle, addr(0xEE)), Some(U256::from(10)));
    }

    #[test]
    fn ready_handle_returns_quickly() {
        let handle = DeferredBundleState::ready(ComputedBundleState::default());
        let start = std::time::Instant::now();
        let _ = handle.wait_cloned();
        assert!(start.elapsed() < Duration::from_millis(20));
    }

    #[test]
    fn concurrent_wait_cloned_is_consistent() {
        let parent =
            DeferredBundleState::pending(bundle_with_account(1, addr(0xFF), 1), anchor(0x01), None);
        let child = DeferredBundleState::pending(
            bundle_with_account(2, addr(0xFF), 2),
            anchor(0x01),
            Some(parent),
        );

        let c1 = child.clone();
        let c2 = child;
        let t1 = thread::spawn(move || c1.wait_cloned());
        let t2 = thread::spawn(move || c2.wait_cloned());

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        assert_eq!(balance_of(&r1.bundle, addr(0xFF)), Some(U256::from(2)));
        assert_eq!(balance_of(&r2.bundle, addr(0xFF)), Some(U256::from(2)));
        assert!(Arc::ptr_eq(&r1.bundle, &r2.bundle));
    }
}
