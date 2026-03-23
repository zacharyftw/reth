# Big Block Validation Analysis

This documents every consensus check for big block payloads with env_switches and its current status. Big blocks use the **normal `validate_post_execution` path** — there is no separate env_switches validation code path.

## Root Cause (historical)

Receipt cumulative gas counters reset at each segment boundary when `executor.finish()` is called. This is now fixed at the source: `execute_block` applies gas offsets to each segment's receipts as they are accumulated, so the `BlockExecutionOutput` contains globally-correct receipts. The normal validation path sees correct data.

## Checks

### 1. `validate_block_pre_execution_with_tx_root` — RESTORED

`validate_cancun_gas` passes because the merged header's `blob_gas_used` is the sum across all constituent blocks, matching the concatenated body transactions. The `max_blob_count` limit (which would reject the summed blob gas) is handled by `--testing.max-blob-count` in `validate_header` (standalone validation), not in `validate_cancun_gas`.

### 2. `validate_header_against_parent` — RESTORED

The big block generator derives correct `parent_hash`, `block_number`, `base_fee_per_gas`, and `excess_blob_gas` for chained big blocks. The gas limit ramp check is skipped via `--testing.skip-gas-limit-ramp-check`.

### 3. `validate_block_post_execution` — `gas_used` — RESTORED

Works via the normal path. The accumulated `gas_used` from all segments matches the merged header, and the receipts have globally-correct `cumulative_gas_used` (offsets applied during execution), so `receipts.last().cumulative_gas_used()` returns the correct total.

### 4. `validate_block_post_execution` — `receipts_root` + `logs_bloom` — RESTORED

Works via the normal path. Receipt cumulative gas is corrected during `execute_block` using the `AdjustCumulativeGas` trait, so receipt root and logs bloom computation produces correct results. The pre-computed receipt root from the background task is skipped for env_switches blocks (it received per-segment receipts before correction), so validation recomputes it from the corrected receipts.

### 5. `validate_block_post_execution` — `requests_hash` (EIP-7685) — SKIPPED (via flag)

**Why skipped:** Execution layer requests (EIP-7002/7251 system calls) are applied at each `executor.finish()` boundary, so the merged block accumulates requests from all segments. The correct merged `requests_hash` cannot be computed by the generator because raw execution layer requests are not exposed via `eth_getBlockByNumber` (only `requestsHash` appears in the header). The generator sets an empty `requests_hash` in the Prague sidecar, and the `--testing.skip-requests-hash-check` flag disables the check at validation time.

**What would restore it fully:** Fetch raw execution requests via the Beacon API (`/eth/v2/beacon/blocks/{slot}`) which includes `execution_requests` in the payload body. The generator could then concatenate all constituent blocks' requests and compute the correct merged `requests_hash`. This would remove the need for the skip flag.
