//! `ValidityColumnData` — NULL bitmask column.
//!
//! Mirrors `duckdb/storage/table/validity_column_data.hpp`.
//!
//! Stores one bit per row: `1` = value present, `0` = NULL.
//! Always a *child* of another `ColumnData` (never stands alone).

use super::column_data::ColumnDataBase;

/// NULL bitmask sub-column.
///
/// Mirrors `class ValidityColumnData : public ColumnData`.
pub struct ValidityColumnData {
    pub base: ColumnDataBase,
}

impl ValidityColumnData {
    pub fn new(base: ColumnDataBase) -> Self {
        ValidityColumnData { base }
    }
}
