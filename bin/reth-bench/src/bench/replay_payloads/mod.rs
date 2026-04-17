//! Command for replaying pre-generated payloads from disk.

mod payloads;

use crate::{
    authenticated_transport::AuthenticatedTransportConnect,
    bench::{
        helpers::parse_duration,
        metrics_scraper::MetricsScraper,
        output::{
            write_benchmark_results, CombinedResult, NewPayloadResult, TotalGasOutput, TotalGasRow,
        },
        replay_payloads::payloads::PayloadStream,
    },
    valid_payload::{call_forkchoice_updated_with_reth, call_new_payload_with_reth},
};
use alloy_primitives::B256;
use alloy_provider::{network::AnyNetwork, Provider, RootProvider};
use alloy_rpc_client::ClientBuilder;
use alloy_rpc_types_engine::{ForkchoiceState, JwtSecret};
use clap::Parser;
use eyre::Context;
use reth_cli_runner::CliContext;
use reth_node_api::EngineApiMessageVersion;
use reth_node_core::args::WaitForPersistence;
use reth_rpc_api::RethNewPayloadInput;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};
use url::Url;

/// `reth bench replay-payloads` command
///
/// Replays pre-generated payloads from a directory by calling `newPayload` followed by
/// `forkchoiceUpdated` for each payload in sequence.
#[derive(Debug, Parser)]
pub struct Command {
    /// The engine RPC URL (with JWT authentication).
    #[arg(long, value_name = "ENGINE_RPC_URL", default_value = "http://localhost:8551")]
    engine_rpc_url: String,

    /// Path to the JWT secret file for engine API authentication.
    #[arg(long, value_name = "JWT_SECRET")]
    jwt_secret: PathBuf,

    /// Directory containing payload files (`payload_block_N.json`).
    #[arg(long, value_name = "PAYLOAD_DIR")]
    payload_dir: PathBuf,

    /// Optional limit on the number of payloads to replay.
    /// If not specified, replays all payloads in the directory.
    #[arg(long, value_name = "COUNT")]
    count: Option<usize>,

    /// Skip the first N payloads.
    #[arg(long, value_name = "SKIP", default_value = "0")]
    skip: usize,

    /// Deprecated: gas ramp is no longer needed. This flag is accepted but ignored.
    #[arg(long, value_name = "GAS_RAMP_DIR", hide = true)]
    gas_ramp_dir: Option<PathBuf>,

    /// Optional output directory for benchmark results (CSV files).
    #[arg(long, value_name = "OUTPUT")]
    output: Option<PathBuf>,

    /// How long to wait after a forkchoice update before sending the next payload.
    ///
    /// Accepts a duration string (e.g. `100ms`, `2s`) or a bare integer treated as
    /// milliseconds (e.g. `400`).
    #[arg(long, value_name = "WAIT_TIME", value_parser = parse_duration, verbatim_doc_comment)]
    wait_time: Option<Duration>,

    /// Use `reth_newPayload` endpoint instead of `engine_newPayload*`.
    ///
    /// The `reth_newPayload` endpoint is a reth-specific extension that takes `ExecutionData`
    /// directly, waits for persistence and cache updates to complete before processing,
    /// and returns server-side timing breakdowns (latency, persistence wait, cache wait).
    #[arg(long, default_value = "false", verbatim_doc_comment)]
    reth_new_payload: bool,

    /// Control when `reth_newPayload` waits for in-flight persistence.
    ///
    /// Accepts `always` (default — wait on every block), `never`, or a number N
    /// to wait every N blocks and skip the rest.
    ///
    /// Requires `--reth-new-payload`.
    #[arg(
        long = "wait-for-persistence",
        value_name = "MODE",
        num_args = 0..=1,
        default_missing_value = "always",
        value_parser = clap::value_parser!(WaitForPersistence),
        requires = "reth_new_payload",
        verbatim_doc_comment
    )]
    wait_for_persistence: Option<WaitForPersistence>,

    /// Skip waiting for execution cache and sparse trie locks before processing.
    ///
    /// Only works with `--reth-new-payload`. When set, passes `wait_for_caches: false`
    /// to the `reth_newPayload` endpoint.
    #[arg(long, default_value = "false", verbatim_doc_comment, requires = "reth_new_payload")]
    no_wait_for_caches: bool,

    /// Optional Prometheus metrics endpoint to scrape after each block.
    ///
    /// When provided, reth-bench will fetch metrics from this URL after each
    /// payload, recording per-block execution and state root durations.
    /// Results are written to `metrics.csv` in the output directory.
    #[arg(long = "metrics-url", value_name = "URL", verbatim_doc_comment)]
    metrics_url: Option<String>,

    /// Stream payloads from disk during replay instead of loading all upfront.
    ///
    /// By default, all payload files are loaded into memory before replay starts
    /// so the hot loop is free of I/O. For long runs with many payloads, set this
    /// flag to load each payload just before it's replayed, keeping memory bounded.
    /// Reads are prefetched on a background task to minimize interference with replay.
    ///
    /// Note: in streaming mode, parse errors surface mid-run (after the blocks
    /// preceding them have already been replayed), rather than upfront.
    #[arg(long, default_value = "false", verbatim_doc_comment)]
    stream_payloads: bool,
}

impl Command {
    /// Execute the `replay-payloads` command.
    pub async fn execute(self, _ctx: CliContext) -> eyre::Result<()> {
        info!(target: "reth-bench", payload_dir = %self.payload_dir.display(), "Replaying payloads");

        // Log mode configuration
        if let Some(duration) = self.wait_time {
            info!(target: "reth-bench", "Using wait-time mode with {}ms minimum interval between blocks", duration.as_millis());
        }
        if self.reth_new_payload {
            info!("Using reth_newPayload and reth_forkchoiceUpdated endpoints");
        }

        let mut metrics_scraper = MetricsScraper::maybe_new(self.metrics_url.clone());

        // Set up authenticated engine provider
        let jwt =
            std::fs::read_to_string(&self.jwt_secret).wrap_err("Failed to read JWT secret file")?;
        let jwt = JwtSecret::from_hex(jwt.trim())?;
        let auth_url = Url::parse(&self.engine_rpc_url)?;

        info!(target: "reth-bench", "Connecting to Engine RPC at {}", auth_url);
        let auth_transport = AuthenticatedTransportConnect::new(auth_url.clone(), jwt);
        let auth_client = ClientBuilder::default().connect_with(auth_transport).await?;
        let auth_provider = RootProvider::<AnyNetwork>::new(auth_client);

        // Get parent block (latest canonical block) - we need this for the first FCU
        let parent_block = auth_provider
            .get_block_by_number(alloy_eips::BlockNumberOrTag::Latest)
            .await?
            .ok_or_else(|| eyre::eyre!("Failed to fetch latest block"))?;

        let initial_parent_hash = parent_block.header.hash;
        let initial_parent_number = parent_block.header.number;

        info!(
            target: "reth-bench",
            parent_hash = %initial_parent_hash,
            parent_number = initial_parent_number,
            "Using initial parent block"
        );

        // Warn if deprecated --gas-ramp-dir is passed
        if self.gas_ramp_dir.is_some() {
            warn!(
                target: "reth-bench",
                "--gas-ramp-dir is deprecated and ignored."
            );
        }

        let mut payloads =
            PayloadStream::open(&self.payload_dir, self.skip, self.count, self.stream_payloads)?;
        let total = payloads.total();

        let mut parent_hash = initial_parent_hash;

        let mut results = Vec::new();
        let total_benchmark_duration = Instant::now();
        let mut warned_env_switches = false;
        let mut i: usize = 0;

        while let Some(payload_res) = payloads.next().await {
            let payload = payload_res?;

            if !self.reth_new_payload &&
                !warned_env_switches &&
                !payload.big_block_data.env_switches.is_empty()
            {
                warn!(
                    target: "reth-bench",
                    "Payloads contain env_switches but --reth-new-payload is not set. \
                     env_switches are only supported with reth_newPayload and will be ignored."
                );
                warned_env_switches = true;
            }

            let execution_data = &payload.execution_data;
            let block_hash = payload.block_hash;
            let v1 = execution_data.payload.as_v1();

            let gas_used = v1.gas_used;
            let gas_limit = v1.gas_limit;
            let block_number = v1.block_number;
            let transaction_count = v1.transactions.len() as u64;

            debug!(
                target: "reth-bench",
                payload = i + 1,
                total = total,
                index = payload.index,
                block_hash = %block_hash,
                "Executing payload (newPayload + FCU)"
            );

            let start = Instant::now();

            debug!(
                target: "reth-bench",
                method = "engine_newPayloadV4",
                block_hash = %block_hash,
                "Sending newPayload"
            );

            let (version, params) = if self.reth_new_payload {
                let big_block_data_param = if payload.big_block_data.env_switches.is_empty() &&
                    payload.big_block_data.prior_block_hashes.is_empty()
                {
                    None
                } else {
                    Some(payload.big_block_data.clone())
                };
                let wait_for_persistence = self
                    .wait_for_persistence
                    .unwrap_or(WaitForPersistence::Never)
                    .rpc_value(block_number);
                (
                    None,
                    serde_json::to_value((
                        RethNewPayloadInput::ExecutionData(execution_data.clone()),
                        wait_for_persistence,
                        self.no_wait_for_caches.then_some(false),
                        big_block_data_param,
                    ))?,
                )
            } else {
                let requests =
                    execution_data.sidecar.requests().cloned().unwrap_or_default().to_vec();
                (
                    Some(EngineApiMessageVersion::V4),
                    serde_json::to_value((
                        execution_data.payload.clone(),
                        Vec::<B256>::new(),
                        B256::ZERO,
                        requests,
                    ))?,
                )
            };

            let server_timings =
                call_new_payload_with_reth(&auth_provider, version, params).await?;

            let np_latency =
                server_timings.as_ref().map(|t| t.latency).unwrap_or_else(|| start.elapsed());
            let new_payload_result = NewPayloadResult {
                gas_used,
                latency: np_latency,
                persistence_wait: server_timings
                    .as_ref()
                    .map(|t| t.persistence_wait)
                    .unwrap_or_default(),
                execution_cache_wait: server_timings
                    .as_ref()
                    .map(|t| t.execution_cache_wait)
                    .unwrap_or_default(),
                sparse_trie_wait: server_timings
                    .as_ref()
                    .map(|t| t.sparse_trie_wait)
                    .unwrap_or_default(),
            };

            let fcu_state = ForkchoiceState {
                head_block_hash: block_hash,
                safe_block_hash: parent_hash,
                finalized_block_hash: parent_hash,
            };

            let fcu_start = Instant::now();
            call_forkchoice_updated_with_reth(&auth_provider, version, fcu_state).await?;
            let fcu_latency = fcu_start.elapsed();

            let total_latency =
                if server_timings.is_some() { np_latency + fcu_latency } else { start.elapsed() };

            let combined_result = CombinedResult {
                block_number,
                gas_limit,
                transaction_count,
                new_payload_result,
                fcu_latency,
                total_latency,
            };

            let current_duration = total_benchmark_duration.elapsed();
            let progress = format!("{}/{}", i + 1, total);
            info!(target: "reth-bench", progress, %combined_result);

            if let Some(scraper) = metrics_scraper.as_mut() &&
                let Err(err) = scraper.scrape_after_block(block_number).await
            {
                tracing::warn!(target: "reth-bench", %err, block_number, "Failed to scrape metrics");
            }

            if let Some(wait_time) = self.wait_time {
                let remaining = wait_time.saturating_sub(start.elapsed());
                if !remaining.is_zero() {
                    tokio::time::sleep(remaining).await;
                }
            }

            let gas_row =
                TotalGasRow { block_number, transaction_count, gas_used, time: current_duration };
            results.push((gas_row, combined_result));

            parent_hash = block_hash;
            i += 1;
        }

        let (gas_output_results, combined_results): (Vec<TotalGasRow>, Vec<CombinedResult>) =
            results.into_iter().unzip();

        if let Some(ref path) = self.output {
            write_benchmark_results(path, &gas_output_results, &combined_results)?;
        }

        if let (Some(path), Some(scraper)) = (&self.output, &metrics_scraper) {
            scraper.write_csv(path)?;
        }

        let gas_output =
            TotalGasOutput::with_combined_results(gas_output_results, &combined_results)?;
        info!(
            target: "reth-bench",
            total_gas_used = gas_output.total_gas_used,
            total_duration = ?gas_output.total_duration,
            execution_duration = ?gas_output.execution_duration,
            blocks_processed = gas_output.blocks_processed,
            wall_clock_ggas_per_second = format_args!("{:.4}", gas_output.total_gigagas_per_second()),
            execution_ggas_per_second = format_args!("{:.4}", gas_output.execution_gigagas_per_second()),
            "Benchmark complete"
        );

        Ok(())
    }
}
