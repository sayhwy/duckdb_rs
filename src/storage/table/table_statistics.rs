//! `TableStatistics` — per-column statistics for an entire table.
//!
//! Mirrors `duckdb/storage/table/table_statistics.hpp`.

use std::sync::Arc;
use parking_lot::{Mutex, MutexGuard};

use super::types::LogicalType;

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
    pub fn initialize_empty(&mut self, types: &[LogicalType]) {
        let mut inner = self.inner.lock();
        inner.column_stats = types
            .iter()
            .map(|t| ColumnStatistics::create_empty(t.clone()))
            .collect();
    }

    /// Initialise with an extra column added.
    pub fn initialize_add_column(&mut self, parent: &TableStatistics, new_type: &LogicalType) {
        let parent_inner = parent.inner.lock();
        let mut inner = self.inner.lock();

        // Copy parent stats
        inner.column_stats = parent_inner.column_stats.clone();
        // Add new column
        inner.column_stats.push(ColumnStatistics::create_empty(new_type.clone()));
    }

    /// Initialise with one column removed.
    pub fn initialize_remove_column(&mut self, parent: &TableStatistics, removed_col: usize) {
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
    pub fn merge_stats(&mut self, other: &TableStatistics) {
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
    pub fn merge_column_stats(&mut self, i: usize, stats: &ColumnStatistics) {
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
    pub fn set_stats(&mut self, other: &TableStatistics) {
        let src = other.inner.lock();
        let mut dst = self.inner.lock();
        dst.column_stats = src.column_stats.clone();
    }

    /// Clone all statistics into `other`.
    pub fn copy_stats_into(&self, other: &mut TableStatistics) {
        let src = self.inner.lock();
        let mut dst = other.inner.lock();
        dst.column_stats = src.column_stats.clone();
    }

    /// Returns a cloned Arc to column `i`'s statistics.
    pub fn copy_column_stats(&self, i: usize) -> Option<Arc<ColumnStatistics>> {
        self.inner.lock().column_stats.get(i).map(Arc::clone)
    }
}

impl Default for TableStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TableStatistics {
    fn clone(&self) -> Self {
        TableStatistics { inner: Arc::clone(&self.inner) }
    }
}
