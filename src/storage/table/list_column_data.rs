//! `ListColumnData` — variable-length list column storage.
//!
//! Mirrors `duckdb/storage/table/list_column_data.hpp`.
//!
//! A `LIST(T)` column stores:
//! - An **offsets** segment (one `u64` per row — index into `child`).
//! - A **child** `ColumnData` holding the flat element values.
//! - A **validity** bitmask.

use std::sync::Arc;

use super::column_data::ColumnData;
use super::validity_column_data::ValidityColumnData;

/// Variable-length list column.
///
/// Mirrors `class ListColumnData : public ColumnData`.
pub struct ListColumnData {
    /// Offset array: `offsets[i]` is the start index in `child` for row `i`.
    /// Stored as a `StandardColumnData` of type `UBIGINT`.
    pub child_column: Arc<ColumnData>,

    /// Validity bitmask for top-level LIST nulls.
    pub validity: Arc<ValidityColumnData>,
}

impl ListColumnData {
    pub fn new(child_column: Arc<ColumnData>, validity: Arc<ValidityColumnData>) -> Self {
        ListColumnData {
            child_column,
            validity,
        }
    }
}
