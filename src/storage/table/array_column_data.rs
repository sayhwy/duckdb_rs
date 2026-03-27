//! `ArrayColumnData` — fixed-length array column storage.
//!
//! Mirrors `duckdb/storage/table/array_column_data.hpp`.
//!
//! A `ARRAY(T, N)` column stores:
//! - A flat child `ColumnData` holding `row_count * N` values.
//! - A validity bitmask (for NULL arrays).

use std::sync::Arc;

use super::column_data::{ ColumnData};
use super::validity_column_data::ValidityColumnData;

/// Fixed-length array column.
///
/// Mirrors `class ArrayColumnData : public ColumnData`.
pub struct ArrayColumnData {
    /// Flat element storage (length = `row_count * array_size`).
    pub child_column: Arc<ColumnData>,

    /// Validity bitmask for top-level ARRAY NULLs.
    pub validity: Arc<ValidityColumnData>,

    /// Fixed element count per row (the `N` in `ARRAY(T, N)`).
    pub array_size: u32,
}

impl ArrayColumnData {
    pub fn new(
        child_column: Arc<ColumnData>,
        validity: Arc<ValidityColumnData>,
        array_size: u32,
    ) -> Self {
        ArrayColumnData { child_column, validity, array_size }
    }
}
