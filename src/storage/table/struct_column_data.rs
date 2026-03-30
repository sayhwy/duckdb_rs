//! `StructColumnData` — nested struct column storage.
//!
//! Mirrors `duckdb/storage/table/struct_column_data.hpp`.
//!
//! A `STRUCT(a T1, b T2, …)` column stores:
//! - One child `ColumnData` per named field.
//! - A top-level validity bitmask.

use std::sync::Arc;

use super::column_data::{ColumnData, ColumnDataContext};
use super::validity_column_data::ValidityColumnData;

/// Nested struct column.
///
/// Mirrors `class StructColumnData : public ColumnData`.
pub struct StructColumnData {
    /// One sub-column per struct field (same order as the `LogicalType`).
    pub sub_columns: Vec<Arc<ColumnData>>,

    /// Validity bitmask for top-level STRUCT NULLs.
    pub validity: Arc<ValidityColumnData>,
}

impl StructColumnData {
    pub fn new(sub_columns: Vec<Arc<ColumnData>>, validity: Arc<ValidityColumnData>) -> Self {
        StructColumnData {
            sub_columns,
            validity,
        }
    }
}
