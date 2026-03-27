//! `SegmentBase<T>` — base trait for all segment types.
//!
//! Mirrors `duckdb/storage/table/segment_base.hpp`.
//! In C++, `SegmentBase<T>` is a CRTP base that adds an `atomic<idx_t> count`
//! field.  In Rust we model this as a trait so both `ColumnSegment` and
//! `RowGroup` can implement it without inheriting from a common struct.

use std::sync::atomic::{AtomicU64, Ordering};

use super::types::Idx;

/// Any storage segment that tracks the number of rows/entries it holds.
///
/// Implemented by: `ColumnSegment`, `RowGroup`.
pub trait SegmentBase {
    /// Returns a reference to the atomic row counter.
    fn count_atomic(&self) -> &AtomicU64;

    /// Snapshot read of the current count (Relaxed ordering — caller must
    /// establish happens-before via locks when accuracy is required).
    fn count(&self) -> Idx {
        self.count_atomic().load(Ordering::Relaxed)
    }

    /// Atomically set the count.
    fn set_count(&self, new_count: Idx) {
        self.count_atomic().store(new_count, Ordering::Relaxed);
    }
}
