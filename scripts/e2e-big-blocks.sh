#!/usr/bin/env bash
#
# End-to-end test for the big block workflow.
#
# Generates sequential big blocks from a public testnet RPC, starts a local
# reth node, replays the blocks against it, and tears everything down.
#
# Prerequisites:
#   - A synced reth datadir for the chosen chain (default: hoodi)
#   - cargo / rustup toolchain with nightly for fmt
#
# Usage:
#   ./scripts/e2e-big-blocks.sh [OPTIONS]
#
# Options:
#   --datadir DIR        Path to reth datadir (required)
#   --chain CHAIN        Chain name (default: hoodi)
#   --rpc-url URL        Source RPC for fetching blocks
#                        (default: https://rpc.hoodi.ethpandaops.io)
#   --from-block N       First block number to merge (required)
#   --target-gas N       Target gas per big block (default: 100000000)
#   --num-big-blocks N   Number of sequential big blocks (default: 10)
#   --profile PROFILE    Cargo build profile (default: profiling)
#   --keep-temp          Don't delete temp dir on exit
#
# Example:
#   ./scripts/e2e-big-blocks.sh \
#       --datadir /data/reth/hoodi \
#       --from-block 910020 \
#       --target-gas 200000000 \
#       --num-big-blocks 3

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────

DATADIR=""
CHAIN="hoodi"
RPC_URL="https://rpc.hoodi.ethpandaops.io"
FROM_BLOCK=""
TARGET_GAS=100000000
NUM_BIG_BLOCKS=10
PROFILE="profiling"
KEEP_TEMP=false

# ── Parse args ────────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datadir)       DATADIR="$2";        shift 2 ;;
        --chain)         CHAIN="$2";          shift 2 ;;
        --rpc-url)       RPC_URL="$2";        shift 2 ;;
        --from-block)    FROM_BLOCK="$2";     shift 2 ;;
        --target-gas)    TARGET_GAS="$2";     shift 2 ;;
        --num-big-blocks) NUM_BIG_BLOCKS="$2"; shift 2 ;;
        --profile)       PROFILE="$2";        shift 2 ;;
        --keep-temp)     KEEP_TEMP=true;      shift ;;
        -h|--help)
            sed -n '2,/^$/{ s/^# \?//; p }' "$0"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$DATADIR" ]]; then
    echo "Error: --datadir is required" >&2
    exit 1
fi

if [[ -z "$FROM_BLOCK" ]]; then
    echo "Error: --from-block is required" >&2
    exit 1
fi

# ── Helpers ───────────────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }

RETH_PID=""
TMPDIR_PATH=""

cleanup() {
    local exit_code=$?
    log "Cleaning up..."
    if [[ -n "$RETH_PID" ]] && kill -0 "$RETH_PID" 2>/dev/null; then
        log "Sending SIGTERM to reth (PID $RETH_PID)"
        kill "$RETH_PID" 2>/dev/null || true
        # Wait up to 30s for graceful shutdown
        local waited=0
        while kill -0 "$RETH_PID" 2>/dev/null && [[ $waited -lt 30 ]]; do
            sleep 1
            waited=$((waited + 1))
        done
        if kill -0 "$RETH_PID" 2>/dev/null; then
            log "reth did not exit after 30s, sending SIGKILL"
            kill -9 "$RETH_PID" 2>/dev/null || true
        fi
        log "reth stopped"
    fi
    if [[ "$KEEP_TEMP" == false && -n "$TMPDIR_PATH" && -d "$TMPDIR_PATH" ]]; then
        log "Removing temp dir: $TMPDIR_PATH"
        rm -rf "$TMPDIR_PATH"
    fi
    exit $exit_code
}
trap cleanup EXIT INT TERM

# ── Paths ─────────────────────────────────────────────────────────────────────

TMPDIR_PATH="$(mktemp -d /tmp/e2e-big-blocks.XXXXXX)"
PAYLOAD_DIR="$TMPDIR_PATH/payloads"
JWT_FILE="$TMPDIR_PATH/jwt.hex"
RETH_LOG="$TMPDIR_PATH/reth.log"

mkdir -p "$PAYLOAD_DIR"
openssl rand -hex 32 > "$JWT_FILE"

log "Temp dir: $TMPDIR_PATH"
log "Chain:    $CHAIN"
log "Datadir:  $DATADIR"
log "Profile:  $PROFILE"

# ── Step 0: Build ─────────────────────────────────────────────────────────────

log "Building reth, reth-bb, and reth-bench (--profile=$PROFILE)..."
cargo build --profile="$PROFILE" -p reth -p reth-bb -p reth-bench

TARGET_DIR="$(cargo metadata --format-version=1 --no-deps 2>/dev/null \
    | jq -r '.target_directory')/$(
    case "$PROFILE" in
        dev)     echo debug ;;
        release) echo release ;;
        *)       echo "$PROFILE" ;;
    esac
)"

RETH_BIN="$TARGET_DIR/reth"
BB_BIN="$TARGET_DIR/reth-bb"
BENCH_BIN="$TARGET_DIR/reth-bench"

if [[ ! -x "$RETH_BIN" ]]; then
    echo "Error: reth binary not found at $RETH_BIN" >&2
    exit 1
fi
if [[ ! -x "$BB_BIN" ]]; then
    echo "Error: reth-bb binary not found at $BB_BIN" >&2
    exit 1
fi
if [[ ! -x "$BENCH_BIN" ]]; then
    echo "Error: reth-bench binary not found at $BENCH_BIN" >&2
    exit 1
fi
log "reth:       $RETH_BIN"
log "reth-bb:    $BB_BIN"
log "reth-bench: $BENCH_BIN"

# The node needs to be unwound to from_block - 1 so the replay has a clean
# starting state.
UNWIND_TO=$((FROM_BLOCK - 1))

# ── Step 1: Generate big blocks ──────────────────────────────────────────────

log "Generating $NUM_BIG_BLOCKS big blocks (target_gas=$TARGET_GAS each) from block $FROM_BLOCK..."
"$BENCH_BIN" generate-big-block \
    --rpc-url "$RPC_URL" \
    --chain "$CHAIN" \
    --from-block "$FROM_BLOCK" \
    --target-gas "$TARGET_GAS" \
    --num-big-blocks "$NUM_BIG_BLOCKS" \
    --output-dir "$PAYLOAD_DIR"

PAYLOAD_COUNT=$(find "$PAYLOAD_DIR" -name '*.json' | wc -l)
log "Generated $PAYLOAD_COUNT payload files"

if [[ "$PAYLOAD_COUNT" -eq 0 ]]; then
    echo "Error: No payloads generated" >&2
    exit 1
fi

# ── Step 2: Unwind the node ──────────────────────────────────────────────────

log "Unwinding node to block $UNWIND_TO..."
# Remove stale lock files that may remain from a previous unclean shutdown
rm -f "$DATADIR/db/mdbx.lck" "$DATADIR/db/lock" "$DATADIR/static_files/lock"

"$RETH_BIN" stage unwind \
    --datadir "$DATADIR" \
    --chain "$CHAIN" \
    to-block "$UNWIND_TO"

log "Unwind complete"

# ── Step 3: Start reth-bb ─────────────────────────────────────────────────────

log "Starting reth-bb node..."
rm -f "$DATADIR/db/mdbx.lck" "$DATADIR/db/lock" "$DATADIR/static_files/lock"

"$BB_BIN" node \
    --datadir "$DATADIR" \
    --chain "$CHAIN" \
    --http --http.api debug,eth \
    --authrpc.jwtsecret "$JWT_FILE" \
    -d \
    > "$RETH_LOG" 2>&1 &

RETH_PID=$!
log "reth-bb started (PID $RETH_PID), log: $RETH_LOG"

# Wait for the HTTP RPC to become ready (port 8545). The engine RPC (8551)
# requires JWT auth so we can't easily health-check it with curl.
log "Waiting for RPC to become ready..."
MAX_WAIT=120
waited=0
while ! curl -sf -X POST -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    http://localhost:8545 >/dev/null 2>&1; do
    if ! kill -0 "$RETH_PID" 2>/dev/null; then
        echo "Error: reth exited prematurely. Check $RETH_LOG" >&2
        tail -30 "$RETH_LOG" >&2
        exit 1
    fi
    sleep 1
    waited=$((waited + 1))
    if [[ $waited -ge $MAX_WAIT ]]; then
        echo "Error: RPC not ready after ${MAX_WAIT}s. Check $RETH_LOG" >&2
        tail -30 "$RETH_LOG" >&2
        exit 1
    fi
done
log "RPC is ready (waited ${waited}s)"

# ── Step 4: Replay payloads ─────────────────────────────────────────────────

log "Replaying $PAYLOAD_COUNT payloads..."
"$BENCH_BIN" replay-payloads \
    --engine-rpc-url http://localhost:8551 \
    --jwt-secret "$JWT_FILE" \
    --payload-dir "$PAYLOAD_DIR" \
    --reth-new-payload

log "Replay complete"

# ── Step 5: Shutdown ─────────────────────────────────────────────────────────

# The cleanup trap handles reth shutdown and temp dir removal.
log "E2E big block test passed ✓"
