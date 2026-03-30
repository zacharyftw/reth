use super::{TrieCursor, TrieStorageCursor};
use crate::{BranchNodeCompact, Nibbles};
use alloy_primitives::B256;
use reth_primitives_traits::FastInstant as Instant;
use reth_storage_errors::db::DatabaseError;
use std::time::Duration;
use tracing::trace_span;

#[cfg(feature = "metrics")]
use crate::TrieType;
#[cfg(feature = "metrics")]
use reth_metrics::metrics::{self, Histogram};

/// Prometheus metrics for trie cursor operations.
///
/// Tracks the number of cursor operations for monitoring and performance analysis.
#[cfg(feature = "metrics")]
#[derive(Clone, Debug)]
pub struct TrieCursorMetrics {
    /// Histogram tracking overall time spent in database operations
    overall_duration: Histogram,
    /// Histogram for `next()` operations
    next_histogram: Histogram,
    /// Histogram for `seek()` operations
    seek_histogram: Histogram,
    /// Histogram for `seek_exact()` operations
    seek_exact_histogram: Histogram,
}

#[cfg(feature = "metrics")]
impl TrieCursorMetrics {
    /// Create a new metrics instance with the specified trie type label.
    pub fn new(trie_type: TrieType) -> Self {
        let trie_type_str = trie_type.as_str();

        Self {
            overall_duration: metrics::histogram!(
                "trie.cursor.overall_duration",
                "type" => trie_type_str
            ),
            next_histogram: metrics::histogram!(
                "trie.cursor.operations",
                "type" => trie_type_str,
                "operation" => "next"
            ),
            seek_histogram: metrics::histogram!(
                "trie.cursor.operations",
                "type" => trie_type_str,
                "operation" => "seek"
            ),
            seek_exact_histogram: metrics::histogram!(
                "trie.cursor.operations",
                "type" => trie_type_str,
                "operation" => "seek_exact"
            ),
        }
    }

    /// Record the cached metrics from the provided cache and reset the cache counters.
    ///
    /// This method adds the current counter values from the cache to the Prometheus metrics
    /// and then resets all cache counters to zero.
    pub fn record(&mut self, cache: &mut TrieCursorMetricsCache) {
        self.next_histogram.record(cache.next_count as f64);
        self.seek_histogram.record(cache.seek_count as f64);
        self.seek_exact_histogram.record(cache.seek_exact_count as f64);
        if cache.timed_operations > 0 {
            self.overall_duration.record(cache.total_duration.as_secs_f64());
        }
        cache.reset();
    }
}

/// Cached metrics counters for trie cursor operations.
#[derive(Debug, Copy, Clone)]
pub struct TrieCursorMetricsCache {
    /// Counter for `next()` calls
    pub next_count: usize,
    /// Counter for `seek()` calls
    pub seek_count: usize,
    /// Counter for `seek_exact()` calls
    pub seek_exact_count: usize,
    /// Total duration spent in database operations
    pub total_duration: Duration,
    /// Number of operations that recorded duration samples
    pub timed_operations: usize,
}

impl Default for TrieCursorMetricsCache {
    fn default() -> Self {
        Self {
            next_count: 0,
            seek_count: 0,
            seek_exact_count: 0,
            total_duration: Duration::ZERO,
            timed_operations: 0,
        }
    }
}

impl TrieCursorMetricsCache {
    /// Reset all counters to zero.
    pub const fn reset(&mut self) {
        self.next_count = 0;
        self.seek_count = 0;
        self.seek_exact_count = 0;
        self.total_duration = Duration::ZERO;
        self.timed_operations = 0;
    }

    /// Extend this cache by adding the counts from another cache.
    ///
    /// This accumulates the counter values from `other` into this cache.
    pub fn extend(&mut self, other: &Self) {
        self.next_count += other.next_count;
        self.seek_count += other.seek_count;
        self.seek_exact_count += other.seek_exact_count;
        self.total_duration += other.total_duration;
        self.timed_operations += other.timed_operations;
    }

    /// Record the span for metrics.
    pub fn record_span(&self, name: &'static str) {
        let _span = trace_span!(
            target: "trie::trie_cursor",
            "Trie cursor metrics",
            name,
            next_count = self.next_count,
            seek_count = self.seek_count,
            seek_exact_count = self.seek_exact_count,
            total_duration = self.total_duration.as_secs_f64(),
            timed_operations = self.timed_operations,
        )
        .entered();
    }
}

/// A wrapper around a [`TrieCursor`] that tracks metrics for cursor operations.
///
/// This implementation counts the number of times each cursor operation is called:
/// - `next()` - Move to the next entry
/// - `seek()` - Seek to a key or the next greater key
/// - `seek_exact()` - Seek to an exact key match
#[derive(Debug)]
pub struct InstrumentedTrieCursor<'metrics, C> {
    /// The underlying cursor being wrapped
    cursor: C,
    /// Cached metrics counters
    metrics: &'metrics mut TrieCursorMetricsCache,
    /// Whether every operation should record elapsed time
    measure_duration: bool,
}

impl<'metrics, C> InstrumentedTrieCursor<'metrics, C> {
    /// Create a new metrics cursor wrapping the given cursor.
    pub const fn new(cursor: C, metrics: &'metrics mut TrieCursorMetricsCache) -> Self {
        Self { cursor, metrics, measure_duration: true }
    }

    /// Create a cursor wrapper that only tracks operation counts.
    pub const fn count_only(cursor: C, metrics: &'metrics mut TrieCursorMetricsCache) -> Self {
        Self { cursor, metrics, measure_duration: false }
    }
}

impl<'metrics, C: TrieCursor> TrieCursor for InstrumentedTrieCursor<'metrics, C> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let start = self.measure_duration.then(Instant::now);
        self.metrics.seek_exact_count += 1;
        let result = self.cursor.seek_exact(key);
        if let Some(start) = start {
            self.metrics.total_duration += start.elapsed();
            self.metrics.timed_operations += 1;
        }
        result
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let start = self.measure_duration.then(Instant::now);
        self.metrics.seek_count += 1;
        let result = self.cursor.seek(key);
        if let Some(start) = start {
            self.metrics.total_duration += start.elapsed();
            self.metrics.timed_operations += 1;
        }
        result
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let start = self.measure_duration.then(Instant::now);
        self.metrics.next_count += 1;
        let result = self.cursor.next();
        if let Some(start) = start {
            self.metrics.total_duration += start.elapsed();
            self.metrics.timed_operations += 1;
        }
        result
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        self.cursor.current()
    }

    fn reset(&mut self) {
        self.cursor.reset()
    }
}

impl<'metrics, C: TrieStorageCursor> TrieStorageCursor for InstrumentedTrieCursor<'metrics, C> {
    fn set_hashed_address(&mut self, hashed_address: B256) {
        self.cursor.set_hashed_address(hashed_address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trie_cursor::mock::MockTrieCursor;
    use parking_lot::Mutex;
    use std::{collections::BTreeMap, sync::Arc};

    fn mock_trie_cursor() -> MockTrieCursor {
        MockTrieCursor::new(
            Arc::new(BTreeMap::from([
                (Nibbles::from_nibbles([0x1]), BranchNodeCompact::new(0, 0, 0, vec![], None)),
                (Nibbles::from_nibbles([0x2]), BranchNodeCompact::new(0, 0, 0, vec![], None)),
            ])),
            Arc::new(Mutex::new(Vec::new())),
        )
    }

    #[test]
    fn count_only_cursor_tracks_operation_counts_without_timing() {
        let mut metrics = TrieCursorMetricsCache::default();
        let mut cursor = InstrumentedTrieCursor::count_only(mock_trie_cursor(), &mut metrics);

        assert!(cursor.seek(Nibbles::from_nibbles([0x1])).unwrap().is_some());
        assert!(cursor.seek_exact(Nibbles::from_nibbles([0x2])).unwrap().is_some());
        assert!(cursor.next().unwrap().is_none());

        drop(cursor);

        assert_eq!(metrics.seek_count, 1);
        assert_eq!(metrics.seek_exact_count, 1);
        assert_eq!(metrics.next_count, 1);
        assert_eq!(metrics.total_duration, Duration::ZERO);
        assert_eq!(metrics.timed_operations, 0);
    }
}
