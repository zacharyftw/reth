use clap::Parser;
use itertools::Itertools;
use reth_db::static_file::iter_static_files;
use reth_nippy_jar::{NippyJar, NippyJarWriter};
use reth_provider::{providers::ProviderNodeTypes, StaticFileProviderFactory};
use reth_static_file_types::{
    ChangesetOffset, ChangesetOffsetWriter, SegmentHeader, SegmentRangeInclusive, StaticFileSegment,
};
use std::{fs, path::PathBuf};
use tracing::{info, warn};

/// The arguments for the `reth db extract` command
#[derive(Parser, Debug)]
pub struct Command {
    /// Number of most recent blocks to extract from each segment.
    #[arg(long, short)]
    num_blocks: u64,

    /// Output directory for the extracted static files.
    #[arg(long, short)]
    output: PathBuf,

    /// Only extract specific segments (default: all).
    #[arg(long, value_enum)]
    segments: Vec<StaticFileSegment>,
}

impl Command {
    /// Execute `db extract` command
    pub fn execute<N: ProviderNodeTypes>(
        self,
        tool: &reth_db_common::DbTool<N>,
    ) -> eyre::Result<()> {
        eyre::ensure!(self.num_blocks > 0, "num_blocks must be greater than 0");

        let static_file_provider = tool.provider_factory.static_file_provider();

        // Ensure the output directory is not the same as the source static files directory
        let source_dir = fs::canonicalize(static_file_provider.directory())?;
        reth_fs_util::create_dir_all(&self.output)?;
        let output_dir = fs::canonicalize(&self.output)?;
        eyre::ensure!(
            source_dir != output_dir,
            "Output directory must differ from the source static files directory"
        );

        if let Err(err) = static_file_provider.check_consistency(&tool.provider_factory.provider()?)
        {
            warn!("Error checking consistency of static files: {err}");
        }

        let all_static_files = iter_static_files(static_file_provider.directory())?;

        let segments: Vec<StaticFileSegment> = if self.segments.is_empty() {
            StaticFileSegment::iter().collect()
        } else {
            self.segments.clone()
        };

        for segment in segments {
            let ranges = match all_static_files.get(segment) {
                Some(ranges) if !ranges.is_empty() => ranges,
                _ => {
                    info!("No static files found for segment: {segment}, skipping");
                    continue;
                }
            };

            // Find the highest block across all jars for this segment
            let highest_block = ranges.iter().map(|(range, _)| range.end()).max().unwrap();

            let start_block = highest_block.saturating_sub(self.num_blocks - 1);

            info!(
                "Extracting {segment} blocks {start_block}..={highest_block} to {}",
                self.output.display()
            );

            let output_range = SegmentRangeInclusive::new(start_block, highest_block);
            let output_path = self.output.join(segment.filename(&output_range));

            // Create a new jar matching the source segment's configuration
            let header = SegmentHeader::new(output_range, None, None, segment);
            let mut jar = NippyJar::new(segment.columns(), &output_path, header);

            // Match compression from the source
            if segment.is_headers() {
                jar = jar.with_lz4();
            }

            let mut writer = NippyJarWriter::new(jar)?;

            // Changeset offsets to write for change-based segments
            let mut changeset_offsets: Vec<ChangesetOffset> = Vec::new();

            // Track block/tx ranges for the output header
            let mut first_block = None;
            let mut last_block = None;
            let mut first_tx = None;
            let mut last_tx = None;
            let mut total_rows: u64 = 0;

            // Iterate source jars in block order, extracting rows in range
            let sorted_ranges = ranges.iter().sorted_by_key(|(range, _)| range.start());

            for (block_range, _source_header) in sorted_ranges {
                // Skip jars entirely before our range
                if block_range.end() < start_block {
                    continue;
                }
                // Stop if jar starts after our range
                if block_range.start() > highest_block {
                    break;
                }

                let fixed_block_range =
                    static_file_provider.find_fixed_range(segment, block_range.start());

                let jar_provider = static_file_provider
                    .get_segment_provider_for_range(segment, || Some(fixed_block_range), None)?
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "Failed to get segment provider for {segment} at range {block_range}"
                        )
                    })?;

                let source_header = jar_provider.user_header().clone();
                let mut cursor = jar_provider.cursor()?;

                if segment.is_change_based() {
                    // For change-based segments, we need the offsets to know which rows
                    // belong to which block
                    let offsets = jar_provider.read_changeset_offsets()?.ok_or_else(|| {
                        eyre::eyre!(
                            "Missing changeset offsets for {segment} at range {block_range}"
                        )
                    })?;

                    for (offset_index, offset) in offsets.iter().enumerate() {
                        let block_number = block_range.start() + offset_index as u64;
                        let in_range = block_number >= start_block && block_number <= highest_block;

                        if in_range {
                            // Track the row offset in the output jar
                            let new_offset = ChangesetOffset::new(total_rows, offset.num_changes());
                            changeset_offsets.push(new_offset);

                            if first_block.is_none() {
                                first_block = Some(block_number);
                            }
                            last_block = Some(block_number);
                        }

                        for _ in 0..offset.num_changes() {
                            let row = cursor.next_row()?.ok_or_else(|| {
                                eyre::eyre!("Unexpected EOF in {segment} at {block_range}")
                            })?;

                            if in_range {
                                for col in &row {
                                    writer.append_column(Some(Ok(col)))?;
                                }
                                total_rows += 1;
                            }
                        }
                    }
                } else if segment.is_tx_based() {
                    // For tx-based segments (Transactions, Receipts, TransactionSenders),
                    // rows correspond to transaction numbers. Since we can't map blocks to
                    // tx rows without body indices, we include all rows from overlapping jars.
                    // The output block range is set to the full coverage of overlapping jars.
                    let source_block_range = source_header.block_range().ok_or_else(|| {
                        eyre::eyre!("Missing block range in {segment} header at {block_range}")
                    })?;
                    let source_tx_range = source_header.tx_range().ok_or_else(|| {
                        eyre::eyre!("Missing tx range in {segment} header at {block_range}")
                    })?;

                    let mut row_index: u64 = 0;
                    while let Some(row) = cursor.next_row()? {
                        let tx_num = source_tx_range.start() + row_index;

                        for col in &row {
                            writer.append_column(Some(Ok(col)))?;
                        }

                        if first_tx.is_none() {
                            first_tx = Some(tx_num);
                        }
                        last_tx = Some(tx_num);
                        total_rows += 1;
                        row_index += 1;
                    }

                    // Use the full source jar's block range (not clamped), since we copy
                    // all rows from overlapping jars
                    if first_block.is_none() {
                        first_block = Some(source_block_range.start());
                    }
                    last_block = Some(source_block_range.end());
                } else {
                    // Block-based segments (Headers) - one row per block
                    let mut block_number = block_range.start();
                    while let Some(row) = cursor.next_row()? {
                        let actual_block = match source_header.block_range() {
                            Some(br) => br.start() + (block_number - block_range.start()),
                            None => block_number,
                        };

                        if actual_block >= start_block && actual_block <= highest_block {
                            for col in &row {
                                writer.append_column(Some(Ok(col)))?;
                            }
                            total_rows += 1;

                            if first_block.is_none() {
                                first_block = Some(actual_block);
                            }
                            last_block = Some(actual_block);
                        }

                        block_number += 1;
                    }
                }

                // Drop provider before removing from cache
                drop(jar_provider);
                static_file_provider.remove_cached_provider(segment, fixed_block_range.end());
            }

            // Update the output header with actual ranges
            {
                let header = writer.user_header_mut();
                if let (Some(first), Some(last)) = (first_block, last_block) {
                    header.set_block_range(first, last);
                }
                if let (Some(first), Some(last)) = (first_tx, last_tx) {
                    header.set_tx_range(first, last);
                }
                if segment.is_change_based() {
                    header.set_changeset_offsets_len(changeset_offsets.len() as u64);
                }
            }

            // For change-based segments, write the sidecar *before* finalizing the header.
            // This matches the durability contract: sidecar first, header last.
            writer.sync_all()?;

            if segment.is_change_based() && !changeset_offsets.is_empty() {
                let csoff_path = output_path.with_extension("csoff");
                let mut csoff_writer = ChangesetOffsetWriter::new(&csoff_path, 0)?;
                csoff_writer.append_many(&changeset_offsets)?;
                csoff_writer.sync()?;
            }

            writer.finalize()?;

            info!(
                "Extracted {total_rows} rows for {segment} (blocks {}..={})",
                first_block.unwrap_or(0),
                last_block.unwrap_or(0)
            );
        }

        info!("Extraction complete. Output: {}", self.output.display());

        Ok(())
    }
}
