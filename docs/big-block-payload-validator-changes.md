# Big Block Payload Validator Changes

This document catalogues every change made to `crates/engine/tree/src/tree/payload_validator.rs` on the `mediocregopher/2k-big-blocks` branch, relative to `origin/main`. Each change includes the motivation and how it fits into the overall big block execution architecture.

## Background

"Big blocks" are synthetic payloads that merge transactions from multiple consecutive real blocks into a single execution payload. They are used for benchmarking reth under high gas loads. Each big block carries `env_switches` — a list of `(tx_index, ExecutionData)` pairs that tell the executor to swap the EVM environment (gas limit, base fee, withdrawals, etc.) at specific transaction boundaries, simulating the original block boundaries within the merged payload.

## Summary of Changes

### 1. `AdjustCumulativeGas` trait

**What:** New public trait and blanket impl for `EthereumReceipt`:
```rust
pub trait AdjustCumulativeGas: Clone {
    fn with_gas_offset(&self, gas_offset: u64) -> Self;
}
```

**Why:** When executing with env_switches, `executor.finish()` is called at each segment boundary, which resets cumulative gas counters in receipts. The trait provides a way to offset `cumulative_gas_used` on receipt types to produce globally-correct values. It's needed because the generic `Receipt` trait only exposes a getter (`cumulative_gas_used()`) with no setter.

---

### 2. `validate_block_with_state` accepts `BigBlockData`

**What:** The method signature gained a new parameter:
```rust
fn validate_block_with_state<T>(
    &mut self,
    input: BlockOrPayload<T>,
    big_block_data: Option<BigBlockData<T::ExecutionData>>,  // NEW
    mut ctx: TreeCtx<'_, N>,
) -> InsertPayloadResult<N>
```

**Why:** `BigBlockData` carries `env_switches` (the list of EVM environment swaps) and `prior_block_hashes` (real block hashes from earlier big blocks for BLOCKHASH lookups). These are extracted from the `reth_newPayload` RPC call and threaded into execution.

---

### 3. Initial EVM environment from env_switch at tx_index 0

**What:** If the first env_switch is at tx_index 0, its `ExecutionData` is used to derive the initial EVM environment instead of the merged payload.

```rust
let initial_env_data = if env_switches.first().is_some_and(|(idx, _)| *idx == 0) {
    Some(env_switches.remove(0).1)
} else {
    None
};
```

**Why:** The merged payload has an inflated `gas_limit` (sum of all constituent blocks) and a synthetic `base_fee_per_gas` (derived for chaining). The original base block's real values are needed for correct EVM execution in the first segment.

---

### 4. `switch_envs` carries full `ExecutionData`

**What:** Each env_switch now preserves the complete `ExecutionData` alongside the derived EVM environment:
```rust
Vec<(usize, EvmEnvFor<Evm>, T::ExecutionData)>
```

**Why:** The execution context (parent_beacon_block_root, withdrawals, etc.) must come from each constituent block's data, not the merged payload. Without this, system calls like the beacon root update and withdrawal processing would use incorrect values.

---

### 5. Multi-segment execution loop in `execute_block`

**What:** The `execute_block` method was refactored from a single pass to a segmented execution loop that processes env_switches:

1. Sorts and splits `switch_envs` into `(idx, env)` pairs and a separate `ExecutionData` vec (for lifetime management)
2. For each segment boundary: executes transactions up to the switch index, calls `executor.finish()`, reclaims the EVM/DB, creates a new executor with the switched environment
3. Accumulates receipts, gas, blob gas, and requests across all segments
4. Applies gas offset corrections to receipts as they're accumulated

**Why:** Each env_switch represents a real block boundary. The executor must be finished (applying post-execution changes like withdrawals and system calls) and recreated with new environment parameters at each boundary.

---

### 6. Receipt cumulative gas correction at source

**What:** When accumulating receipts from each segment, the gas offset is applied immediately:
```rust
if accumulated_gas_used > 0 {
    accumulated_receipts.extend(
        segment_result.receipts.into_iter()
            .map(|r| r.with_gas_offset(accumulated_gas_used)),
    );
} else {
    accumulated_receipts.extend(segment_result.receipts);
}
```

**Why:** `executor.finish()` resets cumulative gas counters, so each segment's receipts have cumulative gas relative to that segment only. Offsetting at the source means the `BlockExecutionOutput` contains globally-correct receipts, allowing the normal `validate_post_execution` path to work without a separate env_switches validation code path.

---

### 7. Gas offset applied when streaming receipts to background root task

**What:** `execute_transactions` gained a `gas_offset: u64` parameter. Before sending each receipt to the background receipt root task, the offset is applied:
```rust
let corrected = if gas_offset > 0 {
    receipt.with_gas_offset(gas_offset)
} else {
    receipt.clone()
};
let _ = receipt_tx.send(IndexedReceipt::new(tx_index, corrected));
```

**Why:** The background receipt root task computes the receipt trie root incrementally as receipts are streamed. Without correction, it would compute the root from receipts with per-segment cumulative gas, producing an incorrect root for env_switches blocks. Correcting at the stream point means the pre-computed receipt root is correct for all blocks, avoiding redundant recomputation.

---

### 8. Global tx index for receipt streaming

**What:** The receipt index sent to the background task uses `senders.len() - 1` (global tx count across segments) instead of `executor.receipts().len() - 1` (per-segment count).

**Why:** `executor.receipts()` resets to 0 after each `executor.finish()`, but the background trie builder expects monotonically increasing indices across the entire block.

---

### 9. `last_sent_len` reset at segment boundaries

**What:** `last_sent_len` is reset to 0 after each env_switch.

**Why:** When a new executor is created for the next segment, its receipts count starts at 0. Without resetting, the `current_len > last_sent_len` check would never be true, and no receipts from subsequent segments would be streamed to the background task.

---

### 10. State hook management for multi-segment execution

**What:** The initial segment uses `state_hook_no_finish()` (streams state updates without signaling completion on drop). Pre-created hooks for each env_switch segment use `state_hook_no_finish()` for intermediates and `state_hook()` (with completion signal) for the last segment only.

```rust
let mut executor = if has_switches {
    executor.with_state_hook(handle.state_hook_no_finish()...)
} else {
    executor.with_state_hook(handle.state_hook()...)
};
```

**Why:** The standard `StateHookSender` sends `FinishedStateUpdates` on drop. When intermediate executors are dropped at segment boundaries, this would cause the sparse trie task to finalize prematurely — computing an incomplete state root from only the first segment's changes.

---

### 11. State hook re-attachment at segment boundaries

**What:** After creating a new executor at each env_switch, the pre-created state hook is attached:
```rust
executor.set_state_hook(segment_state_hooks[i].take());
```

**Why:** Without re-attaching, only segment 0's state changes would be visible to the state root task. Subsequent segments' storage and account changes would be silently dropped.

---

### 12. Precompile cache re-attachment at segment boundaries

**What:** After creating a new executor, the precompile cache is re-mapped onto the new EVM's precompiles.

**Why:** Creating a new EVM/executor at each env_switch boundary discards the precompile cache bindings. Without re-attachment, precompile results from earlier segments wouldn't be cached for reuse.

---

### 13. Prior block hash seeding for BLOCKHASH opcode

**What:** Before execution starts, `prior_block_hashes` from earlier big blocks are seeded into the State's `block_hashes` cache:
```rust
for (block_number, block_hash) in prior_block_hashes {
    db.block_hashes.insert(block_number, block_hash);
}
```

**Why:** When replaying sequential big blocks, BLOCKHASH lookups for blocks merged into earlier big blocks would return zero (they were never individually persisted to the database). Seeding the cache ensures correct results.

---

### 14. Intra-block hash seeding between segments

**What:** After finishing each segment, the just-completed block's hash is seeded into the State cache:
```rust
let finished_block_number = ExecutionPayload::block_number(&switch_data_vec[i]) - 1;
let finished_block_hash = ExecutionPayload::parent_hash(&switch_data_vec[i]);
reclaimed_db.block_hashes.insert(finished_block_number, finished_block_hash);
```

**Why:** Within a single big block, subsequent segments may reference the block number of a just-finished segment via BLOCKHASH. Since these virtual blocks aren't persisted, their hashes must be injected into the cache.

---

### 15. Initial execution context from env_switch data

**What:** When `initial_env_data_storage` is set (from the env_switch at tx_index 0), the executor's context is derived from `context_for_payload(data)` instead of `execution_ctx_for(input)`.

**Why:** The merged payload's `parent_hash` and `withdrawals` are mutated for chaining purposes. The original base block's `ExecutionData` has the correct values for the first segment's system calls (beacon root update, withdrawal processing).

---

### 16. `execute_transactions` refactored for reuse across segments

**What:** Changed from owning the executor and returning it, to taking `&mut` references:
```rust
fn execute_transactions(
    &self,
    executor: &mut E,           // was: E (owned)
    transactions: &mut impl Iterator,  // was: impl Iterator (owned)
    senders: &mut Vec<Address>, // was: returned
    last_sent_len: &mut usize,  // was: local
    stop_before: Option<usize>, // NEW: segment boundary
    gas_offset: u64,            // NEW: for receipt correction
) -> Result<(), BlockExecutionError>  // was: Result<(E, Vec<Address>)>
```

**Why:** The multi-segment loop calls `execute_transactions` once per segment. The function must be callable multiple times with the same mutable state (senders, last_sent_len) accumulating across calls.

---

### 17. `EngineValidator` trait updated for `BigBlockData`

**What:** The `validate_payload` method now accepts `big_block_data`:
```rust
fn validate_payload(
    &mut self,
    payload: Types::ExecutionData,
    big_block_data: Option<BigBlockData<Types::ExecutionData>>,  // NEW
    ctx: TreeCtx<'_, N>,
) -> ValidationOutcome<N>;
```

**Why:** The RPC layer extracts `BigBlockData` from the `reth_newPayload` call and must pass it through to the validator for env_switch execution.

---

### 18. `AdjustCumulativeGas` bound on `NodePrimitives::Receipt`

**What:** The `EngineValidator` impl for `BasicEngineValidator` requires `N: NodePrimitives<Receipt: AdjustCumulativeGas>`.

**Why:** The receipt gas correction logic in `execute_block` and `execute_transactions` calls `with_gas_offset()` on receipts, which requires the trait bound.

---

## What Was NOT Changed

- **`validate_post_execution`**: The normal validation path is unchanged and handles both regular blocks and env_switches blocks identically. No separate env_switches validation code path exists.
- **Receipt root computation in `validation.rs`**: The consensus-level `validate_block_post_execution` function is unmodified (aside from the separate `skip_requests_hash_check` parameter). It sees correct receipts because they are fixed at the source.
- **State root computation**: The parallel/async state root strategy is unchanged. Multi-segment support is handled entirely through state hook management.
