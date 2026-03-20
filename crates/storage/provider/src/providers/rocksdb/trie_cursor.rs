//! RocksDB-backed trie cursor implementations.
//!
//! Provides [`RocksDBTrieCursorFactory`] which implements [`TrieCursorFactory`] using
//! RocksDB column families. Account trie uses `PackedStoredNibbles` (33-byte) keys,
//! while storage trie uses compound keys (`B256 || PackedStoredNibblesSubKey` = 65 bytes)
//! to simulate MDBX's DupSort semantics.

use super::provider::{RocksDBProvider, RocksDBRawIterEnum};
use alloy_primitives::B256;
use reth_db_api::{
    table::{Decode, Decompress, Encode, Table},
    tables, DatabaseError,
};
use reth_trie::{
    trie_cursor::{TrieCursor, TrieCursorFactory, TrieStorageCursor},
    BranchNodeCompact, Nibbles, PackedStoredNibbles, PackedStoredNibblesSubKey,
};
use rocksdb::perf::{PerfContext, PerfMetric, PerfStatsLevel};
use std::{cell::RefCell, time::Instant};

thread_local! {
    /// Thread-local PerfContext for RocksDB seek/next profiling.
    /// Initialized lazily on first use per worker thread.
    static PERF_CTX: RefCell<Option<PerfContext>> = const { RefCell::new(None) };
}

/// Ensures PerfContext is initialized for this thread and resets it.
#[inline(always)]
fn reset_thread_perf_ctx() {
    PERF_CTX.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            rocksdb::perf::set_perf_stats(PerfStatsLevel::EnableCount);
            *borrow = Some(PerfContext::default());
        }
        borrow.as_mut().unwrap().reset();
    });
}

/// Reads PerfContext counters accumulated since last reset and adds them to the stats.
#[inline(always)]
fn collect_thread_perf_ctx(stats: &mut CursorStats) {
    PERF_CTX.with(|cell| {
        let borrow = cell.borrow();
        if let Some(ctx) = borrow.as_ref() {
            stats.block_cache_hit += ctx.metric(PerfMetric::BlockCacheHitCount);
            stats.block_read_count += ctx.metric(PerfMetric::BlockReadCount);
            stats.block_read_nanos += ctx.metric(PerfMetric::BlockReadTime);
            stats.block_decompress_nanos += ctx.metric(PerfMetric::BlockDecompressTime);
            stats.index_block_nanos += ctx.metric(PerfMetric::ReadIndexBlockNanos);
            stats.filter_block_nanos += ctx.metric(PerfMetric::ReadFilterBlockNanos);
            stats.block_seek_nanos += ctx.metric(PerfMetric::BlockSeekNanos);
            stats.find_table_nanos += ctx.metric(PerfMetric::FindTableNanos);
            stats.bloom_sst_hit += ctx.metric(PerfMetric::BloomSstHitCount);
            stats.bloom_sst_miss += ctx.metric(PerfMetric::BloomSstMissCount);
            stats.key_comparisons += ctx.metric(PerfMetric::UserKeyComparisonCount);
            stats.internal_seek_nanos += ctx.metric(PerfMetric::SeekInternalSeekTime);
        }
    });
}

/// Per-cursor seek/next statistics. Accumulated locally per cursor instance
/// and flushed to global `metrics` counters on Drop.
struct CursorStats {
    seek_count: u64,
    seek_exact_count: u64,
    seek_nanos: u64,
    next_count: u64,
    next_nanos: u64,
    block_cache_hit: u64,
    block_read_count: u64,
    block_read_nanos: u64,
    block_decompress_nanos: u64,
    index_block_nanos: u64,
    filter_block_nanos: u64,
    block_seek_nanos: u64,
    find_table_nanos: u64,
    bloom_sst_hit: u64,
    bloom_sst_miss: u64,
    key_comparisons: u64,
    internal_seek_nanos: u64,
}

impl CursorStats {
    const fn new() -> Self {
        Self {
            seek_count: 0,
            seek_exact_count: 0,
            seek_nanos: 0,
            next_count: 0,
            next_nanos: 0,
            block_cache_hit: 0,
            block_read_count: 0,
            block_read_nanos: 0,
            block_decompress_nanos: 0,
            index_block_nanos: 0,
            filter_block_nanos: 0,
            block_seek_nanos: 0,
            find_table_nanos: 0,
            bloom_sst_hit: 0,
            bloom_sst_miss: 0,
            key_comparisons: 0,
            internal_seek_nanos: 0,
        }
    }

    #[inline(always)]
    fn record_seek(&mut self, nanos: u64) {
        self.seek_count += 1;
        self.seek_nanos += nanos;
        collect_thread_perf_ctx(self);
    }

    #[inline(always)]
    fn record_seek_exact(&mut self, nanos: u64) {
        self.seek_exact_count += 1;
        self.seek_count += 1;
        self.seek_nanos += nanos;
        collect_thread_perf_ctx(self);
    }

    #[inline(always)]
    fn record_next(&mut self, nanos: u64) {
        self.next_count += 1;
        self.next_nanos += nanos;
        collect_thread_perf_ctx(self);
    }

    fn flush(&self, kind: &'static str) {
        if self.seek_count > 0 {
            metrics::counter!("rocksdb.trie_cursor.seek_count", "kind" => kind)
                .increment(self.seek_count);
            metrics::counter!("rocksdb.trie_cursor.seek_exact_count", "kind" => kind)
                .increment(self.seek_exact_count);
            metrics::counter!("rocksdb.trie_cursor.seek_nanos", "kind" => kind)
                .increment(self.seek_nanos);
        }
        if self.next_count > 0 {
            metrics::counter!("rocksdb.trie_cursor.next_count", "kind" => kind)
                .increment(self.next_count);
            metrics::counter!("rocksdb.trie_cursor.next_nanos", "kind" => kind)
                .increment(self.next_nanos);
        }
        metrics::counter!("rocksdb.trie_cursor.block_cache_hit", "kind" => kind)
            .increment(self.block_cache_hit);
        metrics::counter!("rocksdb.trie_cursor.block_read_count", "kind" => kind)
            .increment(self.block_read_count);
        metrics::counter!("rocksdb.trie_cursor.block_read_nanos", "kind" => kind)
            .increment(self.block_read_nanos);
        metrics::counter!("rocksdb.trie_cursor.block_decompress_nanos", "kind" => kind)
            .increment(self.block_decompress_nanos);
        metrics::counter!("rocksdb.trie_cursor.index_block_nanos", "kind" => kind)
            .increment(self.index_block_nanos);
        metrics::counter!("rocksdb.trie_cursor.filter_block_nanos", "kind" => kind)
            .increment(self.filter_block_nanos);
        metrics::counter!("rocksdb.trie_cursor.block_seek_nanos", "kind" => kind)
            .increment(self.block_seek_nanos);
        metrics::counter!("rocksdb.trie_cursor.find_table_nanos", "kind" => kind)
            .increment(self.find_table_nanos);
        metrics::counter!("rocksdb.trie_cursor.bloom_sst_hit", "kind" => kind)
            .increment(self.bloom_sst_hit);
        metrics::counter!("rocksdb.trie_cursor.bloom_sst_miss", "kind" => kind)
            .increment(self.bloom_sst_miss);
        metrics::counter!("rocksdb.trie_cursor.key_comparisons", "kind" => kind)
            .increment(self.key_comparisons);
        metrics::counter!("rocksdb.trie_cursor.internal_seek_nanos", "kind" => kind)
            .increment(self.internal_seek_nanos);
    }
}

/// RocksDB-backed trie cursor factory.
///
/// Creates cursors that read trie data from RocksDB column families using packed
/// nibble encoding (storage v2). Account trie entries are stored as simple key-value
/// pairs, while storage trie entries use compound keys to flatten MDBX's DupSort layout.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct RocksDBTrieCursorFactory<'db> {
    provider: &'db RocksDBProvider,
}

impl<'db> RocksDBTrieCursorFactory<'db> {
    /// Creates a new [`RocksDBTrieCursorFactory`].
    #[allow(dead_code)]
    pub(crate) const fn new(provider: &'db RocksDBProvider) -> Self {
        Self { provider }
    }
}

impl<'db> TrieCursorFactory for RocksDBTrieCursorFactory<'db> {
    type AccountTrieCursor<'a>
        = RocksDBAccountTrieCursor<'a>
    where
        Self: 'a;

    type StorageTrieCursor<'a>
        = RocksDBStorageTrieCursor<'a>
    where
        Self: 'a;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor<'_>, DatabaseError> {
        let iter = self.provider.raw_iterator_for_cf(tables::AccountsTrie::NAME)?;
        Ok(RocksDBAccountTrieCursor { iter, stats: CursorStats::new() })
    }

    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor<'_>, DatabaseError> {
        // Use bounded iterator scoped to the address prefix.
        // Lower bound: address || 0x00..00  (inclusive)
        // Upper bound: (address + 1) || 0x00..00  (exclusive)
        let mut lower = [0u8; STORAGE_TRIE_ADDRESS_LEN];
        lower.copy_from_slice(hashed_address.as_ref());
        let upper = next_prefix(&lower);
        let iter = self.provider.raw_iterator_for_cf_bounded(
            tables::StoragesTrie::NAME,
            lower.to_vec(),
            upper,
        )?;
        Ok(RocksDBStorageTrieCursor { iter, hashed_address, stats: CursorStats::new() })
    }
}

/// RocksDB-backed account trie cursor.
///
/// Iterates over `AccountsTrie` column family entries with `PackedStoredNibbles` keys
/// and `BranchNodeCompact` values.
pub(crate) struct RocksDBAccountTrieCursor<'db> {
    iter: RocksDBRawIterEnum<'db>,
    stats: CursorStats,
}

impl Drop for RocksDBAccountTrieCursor<'_> {
    fn drop(&mut self) {
        self.stats.flush("account");
    }
}

impl<'db> RocksDBAccountTrieCursor<'db> {
    /// Creates a new account trie cursor from a `RocksDBProvider`.
    pub(crate) fn new(provider: &'db RocksDBProvider) -> Result<Self, DatabaseError> {
        let iter = provider.raw_iterator_for_cf(tables::AccountsTrie::NAME)?;
        Ok(Self { iter, stats: CursorStats::new() })
    }
}

impl TrieCursor for RocksDBAccountTrieCursor<'_> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let encoded = PackedStoredNibbles::from(key).encode();
        reset_thread_perf_ctx();
        let t = Instant::now();
        self.iter.seek(&encoded);
        self.stats.record_seek_exact(t.elapsed().as_nanos() as u64);
        check_iter_status(&self.iter)?;

        if !self.iter.valid() {
            return Ok(None);
        }

        let Some(key_bytes) = self.iter.key() else { return Ok(None) };
        if key_bytes != encoded.as_ref() {
            return Ok(None);
        }

        decode_account_entry(&self.iter)
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let encoded = PackedStoredNibbles::from(key).encode();
        reset_thread_perf_ctx();
        let t = Instant::now();
        self.iter.seek(&encoded);
        self.stats.record_seek(t.elapsed().as_nanos() as u64);
        check_iter_status(&self.iter)?;

        if !self.iter.valid() {
            return Ok(None);
        }

        decode_account_entry(&self.iter)
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        reset_thread_perf_ctx();
        let t = Instant::now();
        self.iter.next();
        self.stats.record_next(t.elapsed().as_nanos() as u64);
        check_iter_status(&self.iter)?;

        if !self.iter.valid() {
            return Ok(None);
        }

        decode_account_entry(&self.iter)
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        if !self.iter.valid() {
            return Ok(None);
        }

        let Some(key_bytes) = self.iter.key() else { return Ok(None) };
        let key = PackedStoredNibbles::decode(key_bytes)?;
        Ok(Some(key.0))
    }

    fn reset(&mut self) {
        // Seek to the beginning to invalidate current position.
        // Next operation must be a seek.
    }
}

/// RocksDB-backed storage trie cursor.
///
/// Iterates over `StoragesTrie` column family entries using compound keys
/// (`B256 || PackedStoredNibblesSubKey`). Only returns entries matching the
/// current `hashed_address` prefix. Uses bounded iterators to constrain
/// RocksDB to the address prefix range, skipping irrelevant SSTs.
pub(crate) struct RocksDBStorageTrieCursor<'db> {
    iter: RocksDBRawIterEnum<'db>,
    hashed_address: B256,
    stats: CursorStats,
}

impl Drop for RocksDBStorageTrieCursor<'_> {
    fn drop(&mut self) {
        self.stats.flush("storage");
    }
}

/// Length of the address prefix in a StoragesTrie compound key.
const STORAGE_TRIE_ADDRESS_LEN: usize = 32;
/// Length of the subkey portion in a StoragesTrie compound key.
const STORAGE_TRIE_SUBKEY_LEN: usize = 33;
/// Total length of a StoragesTrie compound key.
const STORAGE_TRIE_KEY_LEN: usize = STORAGE_TRIE_ADDRESS_LEN + STORAGE_TRIE_SUBKEY_LEN;

impl<'db> RocksDBStorageTrieCursor<'db> {
    /// Creates a new storage trie cursor from a `RocksDBProvider` scoped to an address.
    ///
    /// Uses prefix-bounded iteration: the 32-byte prefix extractor on StoragesTrie
    /// combined with `prefix_same_as_start` tells RocksDB that iteration stays
    /// within one address prefix. This lets RocksDB prune internal seek work
    /// (fewer levels/files to check). The `is_current_address()` check provides
    /// an additional safety guard at the application level.
    pub(crate) fn new(
        provider: &'db RocksDBProvider,
        hashed_address: B256,
    ) -> Result<Self, DatabaseError> {
        let iter =
            provider.raw_iterator_for_cf_prefix_same_as_start(tables::StoragesTrie::NAME)?;
        Ok(Self { iter, hashed_address, stats: CursorStats::new() })
    }

    /// Builds a compound key from the current hashed address and a nibbles subkey.
    fn compound_key(&self, nibbles: PackedStoredNibblesSubKey) -> [u8; STORAGE_TRIE_KEY_LEN] {
        let mut key = [0u8; STORAGE_TRIE_KEY_LEN];
        key[..STORAGE_TRIE_ADDRESS_LEN].copy_from_slice(self.hashed_address.as_ref());
        key[STORAGE_TRIE_ADDRESS_LEN..].copy_from_slice(&nibbles.encode());
        key
    }

    /// Checks if the iterator is positioned at an entry belonging to the current hashed address.
    fn is_current_address(&self) -> bool {
        self.iter.valid() &&
            self.iter.key().is_some_and(|k| {
                k.get(..STORAGE_TRIE_ADDRESS_LEN) == Some(self.hashed_address.as_ref())
            })
    }

    /// Decodes the current iterator entry into `(Nibbles, BranchNodeCompact)`.
    ///
    /// Returns `None` if the iterator is not positioned at the current address.
    fn decode_current(&self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        if !self.is_current_address() {
            return Ok(None);
        }

        let key_bytes = self.iter.key().ok_or(DatabaseError::Decode)?;
        let subkey_bytes =
            key_bytes.get(STORAGE_TRIE_ADDRESS_LEN..).ok_or(DatabaseError::Decode)?;
        let subkey = PackedStoredNibblesSubKey::decode(subkey_bytes)?;

        let value_bytes = self.iter.value().ok_or(DatabaseError::Decode)?;
        let node = BranchNodeCompact::decompress(value_bytes)?;

        Ok(Some((subkey.0, node)))
    }
}

impl TrieCursor for RocksDBStorageTrieCursor<'_> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let subkey = PackedStoredNibblesSubKey::from(key);
        let encoded_subkey = subkey.clone().encode();
        let compound = self.compound_key(subkey);
        reset_thread_perf_ctx();
        let t = Instant::now();
        self.iter.seek(&compound);
        self.stats.record_seek_exact(t.elapsed().as_nanos() as u64);
        check_iter_status(&self.iter)?;

        if !self.is_current_address() {
            return Ok(None);
        }

        // Check for exact match on the subkey portion
        let key_bytes = self.iter.key().ok_or(DatabaseError::Decode)?;
        let current_subkey_bytes =
            key_bytes.get(STORAGE_TRIE_ADDRESS_LEN..).ok_or(DatabaseError::Decode)?;
        if current_subkey_bytes != encoded_subkey.as_ref() {
            return Ok(None);
        }

        self.decode_current()
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let subkey = PackedStoredNibblesSubKey::from(key);
        let compound = self.compound_key(subkey);
        reset_thread_perf_ctx();
        let t = Instant::now();
        self.iter.seek(&compound);
        self.stats.record_seek(t.elapsed().as_nanos() as u64);
        check_iter_status(&self.iter)?;

        self.decode_current()
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        reset_thread_perf_ctx();
        let t = Instant::now();
        self.iter.next();
        self.stats.record_next(t.elapsed().as_nanos() as u64);
        check_iter_status(&self.iter)?;

        self.decode_current()
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        if !self.is_current_address() {
            return Ok(None);
        }

        let key_bytes = self.iter.key().ok_or(DatabaseError::Decode)?;
        let subkey_bytes =
            key_bytes.get(STORAGE_TRIE_ADDRESS_LEN..).ok_or(DatabaseError::Decode)?;
        let subkey = PackedStoredNibblesSubKey::decode(subkey_bytes)?;
        Ok(Some(subkey.0))
    }

    fn reset(&mut self) {
        // No-op; next operation must be a seek.
    }
}

impl TrieStorageCursor for RocksDBStorageTrieCursor<'_> {
    fn set_hashed_address(&mut self, hashed_address: B256) {
        self.hashed_address = hashed_address;
        // No need to recreate the iterator — the seek in `cursor_seek` will
        // reposition it to the new address, and `is_current_address()` filters
        // entries that don't match.
    }
}

/// Checks the raw iterator status and converts RocksDB errors to [`DatabaseError`].
fn check_iter_status(iter: &RocksDBRawIterEnum<'_>) -> Result<(), DatabaseError> {
    iter.status().map_err(|e| {
        DatabaseError::Read(reth_storage_errors::db::DatabaseErrorInfo {
            message: e.to_string().into(),
            code: -1,
        })
    })
}

/// Decodes the current account trie entry from the iterator.
fn decode_account_entry(
    iter: &RocksDBRawIterEnum<'_>,
) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
    let key_bytes = iter.key().ok_or(DatabaseError::Decode)?;
    let key = PackedStoredNibbles::decode(key_bytes)?;

    let value_bytes = iter.value().ok_or(DatabaseError::Decode)?;
    let node = BranchNodeCompact::decompress(value_bytes)?;

    Ok(Some((key.0, node)))
}

/// Computes the exclusive upper bound for a prefix by incrementing the last byte.
///
/// For a prefix like `[0x12, 0x34, 0xff]`, increments from the rightmost non-0xff byte
/// to produce `[0x12, 0x35]`. If all bytes are 0xff (e.g., the maximum address),
/// returns a single `[0xff, ..., 0xff, 0xff]` with one extra byte as a safe upper bound.
#[allow(dead_code)]
fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    // Find the rightmost byte that can be incremented
    for i in (0..prefix.len()).rev() {
        if prefix[i] < 0xff {
            let mut upper = prefix[..=i].to_vec();
            upper[i] += 1;
            return upper;
        }
    }
    // All bytes are 0xff — extend with an extra byte to create a bound past the prefix
    let mut upper = prefix.to_vec();
    upper.push(0x00);
    upper
}

