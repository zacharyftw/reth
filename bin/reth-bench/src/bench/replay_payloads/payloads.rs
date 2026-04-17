//! Payload discovery, loading, and streaming for `replay-payloads`.

use crate::bench::generate_big_block::BigBlockPayload;
use alloy_primitives::B256;
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayloadEnvelopeV4, ExecutionPayloadSidecar,
    PraguePayloadFields,
};
use eyre::Context;
use reth_engine_primitives::BigBlockData;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Size of the prefetch buffer (in payloads) used in streaming mode.
const PREFETCH_BUFFER: usize = 8;

/// A loaded payload ready for execution.
pub(super) struct LoadedPayload {
    /// The index (from filename).
    pub(super) index: u64,
    /// The execution data for the block.
    pub(super) execution_data: ExecutionData,
    /// The block hash.
    pub(super) block_hash: B256,
    /// Big block data containing environment switches and prior block hashes.
    pub(super) big_block_data: BigBlockData<ExecutionData>,
}

/// A source of [`LoadedPayload`]s, either eagerly loaded into memory or streamed from disk
/// via a background prefetch task.
pub(super) struct PayloadStream {
    total: usize,
    source: Source,
}

impl PayloadStream {
    /// Discover payload files in `dir`, apply `skip`/`count`, and open a stream.
    ///
    /// If `streaming` is true, payloads are prefetched on a background task into a bounded
    /// channel so the hot loop doesn't stall on disk I/O. Otherwise all payloads are loaded
    /// into memory upfront.
    ///
    /// Returns an error if no payload files are found in `dir`.
    pub(super) fn open(
        dir: &Path,
        skip: usize,
        count: Option<usize>,
        streaming: bool,
    ) -> eyre::Result<Self> {
        let paths = discover_payload_paths(dir, skip, count)?;
        if paths.is_empty() {
            return Err(eyre::eyre!("No payload files found in {:?}", dir));
        }
        let total = paths.len();

        let source = if streaming {
            info!(
                target: "reth-bench",
                count = total,
                prefetch = PREFETCH_BUFFER,
                "Streaming payloads from disk"
            );
            Source::Streaming(spawn_prefetch(paths))
        } else {
            let loaded: Vec<LoadedPayload> = paths
                .into_iter()
                .map(|(idx, p)| load_single_payload(idx, p))
                .collect::<eyre::Result<_>>()?;
            info!(target: "reth-bench", count = loaded.len(), "Loaded main payloads from disk");
            Source::Eager(loaded.into_iter())
        };

        Ok(Self { total, source })
    }

    /// Total number of payloads that will be yielded.
    pub(super) const fn total(&self) -> usize {
        self.total
    }

    /// Pull the next payload, or `None` when the stream is exhausted.
    pub(super) async fn next(&mut self) -> Option<eyre::Result<LoadedPayload>> {
        match &mut self.source {
            Source::Eager(it) => it.next().map(Ok),
            Source::Streaming(rx) => rx.recv().await,
        }
    }
}

enum Source {
    /// All payloads have been loaded into memory.
    Eager(std::vec::IntoIter<LoadedPayload>),
    /// Payloads are prefetched on a background task via a bounded channel.
    Streaming(tokio::sync::mpsc::Receiver<eyre::Result<LoadedPayload>>),
}

/// Discover payload files in `dir`, returning `(index, path)` pairs sorted by index,
/// after applying `skip` and `count`.
///
/// Matches both legacy `payload_block_N.json` and `big_block_FROM_to_TO.json` filenames.
fn discover_payload_paths(
    dir: &Path,
    skip: usize,
    count: Option<usize>,
) -> eyre::Result<Vec<(u64, PathBuf)>> {
    let entries: Vec<_> = std::fs::read_dir(dir)
        .wrap_err_with(|| format!("Failed to read directory {:?}", dir))?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            e.path().extension().and_then(|s| s.to_str()) == Some("json") &&
                (name_str.starts_with("payload_block_") || name_str.starts_with("big_block_"))
        })
        .collect();

    // Parse filenames to get indices and sort.
    // Supports "payload_block_N.json" and "big_block_FROM_to_TO.json" naming.
    let mut indexed_paths: Vec<(u64, PathBuf)> = entries
        .into_iter()
        .filter_map(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            let index = if let Some(rest) = name_str.strip_prefix("payload_block_") {
                rest.strip_suffix(".json")?.parse::<u64>().ok()?
            } else if let Some(rest) = name_str.strip_prefix("big_block_") {
                // "big_block_FROM_to_TO.json" — use FROM as the index
                let rest = rest.strip_suffix(".json")?;
                rest.split("_to_").next()?.parse::<u64>().ok()?
            } else {
                return None;
            };
            Some((index, e.path()))
        })
        .collect();

    indexed_paths.sort_by_key(|(idx, _)| *idx);

    let indexed_paths: Vec<_> = indexed_paths.into_iter().skip(skip).collect();
    Ok(match count {
        Some(count) => indexed_paths.into_iter().take(count).collect(),
        None => indexed_paths,
    })
}

/// Read and parse a single payload file.
///
/// Tries [`BigBlockPayload`] first (which includes `env_switches`), falling back to
/// [`ExecutionPayloadEnvelopeV4`] for backwards compatibility.
fn load_single_payload(index: u64, path: PathBuf) -> eyre::Result<LoadedPayload> {
    let content =
        std::fs::read_to_string(&path).wrap_err_with(|| format!("Failed to read {:?}", path))?;

    let (execution_data, big_block_data) =
        if let Ok(big_block) = serde_json::from_str::<BigBlockPayload>(&content) {
            (big_block.execution_data, big_block.big_block_data)
        } else {
            let envelope: ExecutionPayloadEnvelopeV4 = serde_json::from_str(&content)
                .wrap_err_with(|| format!("Failed to parse {:?}", path))?;
            let execution_data = ExecutionData {
                payload: envelope.envelope_inner.execution_payload.clone().into(),
                sidecar: ExecutionPayloadSidecar::v4(
                    CancunPayloadFields {
                        versioned_hashes: Vec::new(),
                        parent_beacon_block_root: B256::ZERO,
                    },
                    PraguePayloadFields { requests: envelope.execution_requests.into() },
                ),
            };
            (execution_data, BigBlockData::default())
        };

    let block_hash = execution_data.payload.as_v1().block_hash;

    debug!(
        target: "reth-bench",
        index = index,
        block_hash = %block_hash,
        env_switches = big_block_data.env_switches.len(),
        prior_block_hashes = big_block_data.prior_block_hashes.len(),
        path = %path.display(),
        "Loaded payload"
    );

    Ok(LoadedPayload { index, execution_data, block_hash, big_block_data })
}

/// Spawn a background task that reads and parses payloads ahead of the consumer,
/// pushing them into a bounded tokio channel.
///
/// Stops on the first error (delivered as the last element) or when the receiver is dropped.
fn spawn_prefetch(
    paths: Vec<(u64, PathBuf)>,
) -> tokio::sync::mpsc::Receiver<eyre::Result<LoadedPayload>> {
    let (tx, rx) = tokio::sync::mpsc::channel(PREFETCH_BUFFER);
    tokio::task::spawn_blocking(move || {
        for (idx, path) in paths {
            let res = load_single_payload(idx, path);
            let is_err = res.is_err();
            if tx.blocking_send(res).is_err() {
                return; // consumer dropped
            }
            if is_err {
                return;
            }
        }
    });
    rx
}
