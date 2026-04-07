//! `TableStatistics` — per-column statistics for an entire table.
//!
//! Mirrors `duckdb/storage/table/table_statistics.hpp`.

use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

use super::types::LogicalType;
use crate::common::serializer::BinarySerializer;
use crate::common::serializer::{
    BinaryMetadataDeserializer, MESSAGE_TERMINATOR_FIELD_ID, skip_optional_blocking_sample,
};
use std::io;

// 使用统一的统计信息模块
pub use crate::storage::statistics::ColumnStatistics;

// ─────────────────────────────────────────────────────────────────────────────
// TableStatisticsLock
// ─────────────────────────────────────────────────────────────────────────────

/// RAII guard over `TableStatistics::stats_lock`.
///
/// Mirrors `class TableStatisticsLock`.  Must be held for any access to
/// `ColumnStatistics` references returned by `TableStatistics::get_stats`.
pub struct TableStatisticsLock<'a>(MutexGuard<'a, TableStatisticsInner>);

// ─────────────────────────────────────────────────────────────────────────────
// TableStatistics
// ─────────────────────────────────────────────────────────────────────────────

/// Table-wide statistics (one `ColumnStatistics` per column).
///
/// Mirrors `class TableStatistics`.
pub struct TableStatistics {
    inner: Arc<Mutex<TableStatisticsInner>>,
}

struct TableStatisticsInner {
    column_stats: Vec<Arc<ColumnStatistics>>,
    // TODO: replace with a proper reservoir / blocking sample type
    table_sample: Option<Vec<u8>>,
}

impl TableStatistics {
    // ── Constructors ─────────────────────────────────────────

    pub fn new() -> Self {
        TableStatistics {
            inner: Arc::new(Mutex::new(TableStatisticsInner {
                column_stats: Vec::new(),
                table_sample: None,
            })),
        }
    }

    /// Initialise statistics for a fresh (empty) table.
    pub fn initialize_empty(&self, types: &[LogicalType]) {
        let mut inner = self.inner.lock();
        inner.column_stats = types
            .iter()
            .map(|t| ColumnStatistics::create_empty(t.clone()))
            .collect();
    }

    /// Initialise with an extra column added.
    pub fn initialize_add_column(&self, parent: &TableStatistics, new_type: &LogicalType) {
        let parent_inner = parent.inner.lock();
        let mut inner = self.inner.lock();

        // Copy parent stats
        inner.column_stats = parent_inner.column_stats.clone();
        // Add new column
        inner
            .column_stats
            .push(ColumnStatistics::create_empty(new_type.clone()));
    }

    /// Initialise with one column removed.
    pub fn initialize_remove_column(&self, parent: &TableStatistics, removed_col: usize) {
        let parent_inner = parent.inner.lock();
        let mut inner = self.inner.lock();

        inner.column_stats = parent_inner
            .column_stats
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != removed_col)
            .map(|(_, s)| Arc::clone(s))
            .collect();
    }

    // ── Locking ──────────────────────────────────────────────

    /// Acquire the statistics lock.
    pub fn lock(&self) -> TableStatisticsLock<'_> {
        TableStatisticsLock(self.inner.lock())
    }

    // ── Accessors ────────────────────────────────────────────

    /// Returns a cloned Arc to column `i`'s statistics.
    pub fn get_stats(&self, i: usize) -> Option<Arc<ColumnStatistics>> {
        self.inner.lock().column_stats.get(i).map(Arc::clone)
    }

    /// Applies a mutation to column `i`'s statistics while holding the lock.
    pub fn with_stats_mut(&self, i: usize, f: impl FnOnce(&mut ColumnStatistics)) {
        let mut inner = self.inner.lock();
        if let Some(stats) = inner.column_stats.get_mut(i) {
            // Get mutable access through Arc
            if let Some(stats_mut) = Arc::get_mut(stats) {
                f(stats_mut);
            } else {
                // If Arc has multiple references, clone and modify
                let mut new_stats = (**stats).clone();
                f(&mut new_stats);
                *stats = Arc::new(new_stats);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().column_stats.is_empty()
    }

    // ── Merge ────────────────────────────────────────────────

    /// Merge statistics from `other` into `self` (expand min/max ranges, etc.).
    pub fn merge_stats(&self, other: &TableStatistics) {
        let other_inner = other.inner.lock();
        let mut inner = self.inner.lock();

        for (i, other_stats) in other_inner.column_stats.iter().enumerate() {
            if let Some(self_stats) = inner.column_stats.get_mut(i) {
                if let Some(self_stats_mut) = Arc::get_mut(self_stats) {
                    self_stats_mut.merge(other_stats);
                } else {
                    let mut new_stats = (**self_stats).clone();
                    new_stats.merge(other_stats);
                    *self_stats = Arc::new(new_stats);
                }
            }
        }
    }

    /// Merge a single column's statistics into column `i`.
    pub fn merge_column_stats(&self, i: usize, stats: &ColumnStatistics) {
        let mut inner = self.inner.lock();
        if let Some(self_stats) = inner.column_stats.get_mut(i) {
            if let Some(self_stats_mut) = Arc::get_mut(self_stats) {
                self_stats_mut.merge(stats);
            } else {
                let mut new_stats = (**self_stats).clone();
                new_stats.merge(stats);
                *self_stats = Arc::new(new_stats);
            }
        }
    }

    /// Replace all statistics with a copy from `other`.
    pub fn set_stats(&self, other: &TableStatistics) {
        let src = other.inner.lock();
        let mut dst = self.inner.lock();
        dst.column_stats = src.column_stats.clone();
    }

    /// Clone all statistics into `other`.
    pub fn copy_stats_into(&self, other: &TableStatistics) {
        let src = self.inner.lock();
        let mut dst = other.inner.lock();
        dst.column_stats = src.column_stats.clone();
    }

    /// Returns a cloned Arc to column `i`'s statistics.
    pub fn copy_column_stats(&self, i: usize) -> Option<Arc<ColumnStatistics>> {
        self.inner.lock().column_stats.get(i).map(Arc::clone)
    }

    pub fn serialize_checkpoint(&self, serializer: &mut BinarySerializer<'_>) {
        let inner = self.inner.lock();
        serializer.begin_list(100, inner.column_stats.len());
        for stats in &inner.column_stats {
            serializer.list_write_nullable_object(true, |s| stats.serialize_checkpoint(s));
        }
        serializer.end_list();
        serializer.write_nullable_field(101, inner.table_sample.is_some());
        if inner.table_sample.is_some() {
            serializer.end_object();
            serializer.end_nullable_object();
        }
    }

    pub fn deserialize_checkpoint(
        de: &mut BinaryMetadataDeserializer<'_>,
        types: &[LogicalType],
    ) -> io::Result<Self> {
        let result = TableStatistics::new();
        let mut inner = result.inner.lock();
        loop {
            let field = de.next_field()?;
            match field {
                100 => {
                    let count = de.read_list_len()?;
                    inner.column_stats.clear();
                    for idx in 0..count {
                        let logical_type =
                            types.get(idx).cloned().unwrap_or_else(LogicalType::integer);
                        let stats =
                            ColumnStatistics::deserialize_checkpoint(de, logical_type.clone())
                                .map_err(|e| {
                                    io::Error::new(
                                        e.kind(),
                                        format!("column_stats[{idx}] ({:?}): {e}", logical_type.id),
                                    )
                                })?
                                .map(Arc::new)
                                .unwrap_or_else(|| ColumnStatistics::create_empty(logical_type));
                        inner.column_stats.push(stats);
                    }
                }
                101 => {
                    if skip_optional_blocking_sample(de).is_ok() {
                        inner.table_sample = Some(Vec::new());
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "failed to skip table_sample",
                        ));
                    }
                }
                MESSAGE_TERMINATOR_FIELD_ID => break,
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected TableStatistics field {other}"),
                    ));
                }
            }
        }
        drop(inner);
        Ok(result)
    }
}

impl Default for TableStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TableStatistics {
    fn clone(&self) -> Self {
        let src = self.inner.lock();
        TableStatistics {
            inner: Arc::new(Mutex::new(TableStatisticsInner {
                column_stats: src
                    .column_stats
                    .iter()
                    .map(|stats| Arc::new((**stats).clone()))
                    .collect(),
                table_sample: src.table_sample.clone(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::serializer::{BinaryMetadataDeserializer, BinarySerializer};
    use crate::common::types::LogicalType;
    use crate::storage::metadata::{ReadStream, WriteStream};
    use crate::storage::statistics::{NumericStats, StatsData, StringStats};

    #[derive(Default)]
    struct VecStream {
        data: Vec<u8>,
        offset: usize,
    }

    impl WriteStream for VecStream {
        fn write_data(&mut self, buf: &[u8]) {
            self.data.extend_from_slice(buf);
        }
    }

    impl ReadStream for VecStream {
        fn read_data(&mut self, buf: &mut [u8]) {
            let end = self.offset + buf.len();
            if end <= self.data.len() {
                buf.copy_from_slice(&self.data[self.offset..end]);
            } else {
                let available = self.data.len().saturating_sub(self.offset);
                if available > 0 {
                    buf[..available].copy_from_slice(&self.data[self.offset..self.data.len()]);
                }
                buf[available..].fill(0xFF);
            }
            self.offset = end;
        }
    }

    #[test]
    fn checkpoint_roundtrip_numeric_and_string_stats() {
        let types = vec![LogicalType::integer(), LogicalType::varchar()];
        let stats = TableStatistics::new();
        stats.initialize_empty(&types);

        stats.with_stats_mut(0, |column| {
            column.statistics_mut().set_has_no_null();
            column.statistics_mut().set_distinct_count(42);
            if let StatsData::Numeric(data) = column.statistics_mut().get_stats_data_mut() {
                NumericStats::set_min(data, 10i32, &LogicalType::integer());
                NumericStats::set_max(data, 99i32, &LogicalType::integer());
            }
        });
        stats.with_stats_mut(1, |column| {
            column.statistics_mut().set_has_null();
            column.statistics_mut().set_has_no_null();
            if let StatsData::String(data) = column.statistics_mut().get_stats_data_mut() {
                StringStats::set_min(data, "alpha");
                StringStats::set_max(data, "omega");
                StringStats::set_max_string_length(data, 12);
                StringStats::set_contains_unicode(data);
            }
        });

        let mut stream = VecStream::default();
        {
            let mut serializer = BinarySerializer::new(&mut stream);
            serializer.begin_root_object();
            stats.serialize_checkpoint(&mut serializer);
            serializer.end_object();
        }
        stream.offset = 0;

        let decoded = {
            let mut deserializer = BinaryMetadataDeserializer::new(&mut stream);
            let decoded = TableStatistics::deserialize_checkpoint(&mut deserializer, &types)
                .expect("table statistics should deserialize");
            let terminator = deserializer
                .next_field()
                .expect("terminator should be readable");
            assert_eq!(
                terminator,
                crate::common::serializer::MESSAGE_TERMINATOR_FIELD_ID
            );
            decoded
        };

        let int_stats = decoded.get_stats(0).expect("integer stats");
        assert_eq!(int_stats.statistics().get_distinct_count(), 42);
        let numeric = match int_stats.statistics().get_stats_data() {
            StatsData::Numeric(data) => data,
            other => panic!("expected numeric stats, got {:?}", other),
        };
        unsafe {
            assert_eq!(numeric.min.integer, 10);
            assert_eq!(numeric.max.integer, 99);
        }

        let str_stats = decoded.get_stats(1).expect("string stats");
        let string = match str_stats.statistics().get_stats_data() {
            StatsData::String(data) => data,
            other => panic!("expected string stats, got {:?}", other),
        };
        assert_eq!(StringStats::min(string), "alpha");
        assert_eq!(StringStats::max(string), "omega");
        assert_eq!(StringStats::max_string_length(string), 12);
        assert!(StringStats::can_contain_unicode(string));
    }
}
