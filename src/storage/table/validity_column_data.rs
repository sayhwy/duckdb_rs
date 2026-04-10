//! `ValidityColumnData` — NULL bitmask column.
//!
//! Mirrors `duckdb/storage/table/validity_column_data.hpp`.
//!
//! Stores one bit per row: `1` = value present, `0` = NULL.
//! Always a *child* of another `ColumnData` (never stands alone).

use super::scan_state::ColumnScanState;
use crate::storage::statistics::FilterPropagateResult;

/// NULL bitmask sub-column.
///
/// Mirrors `class ValidityColumnData : public ColumnData`.
pub struct ValidityColumnData;

impl ValidityColumnData {
    pub fn new() -> Self {
        ValidityColumnData
    }

    pub fn check_zonemap(&self, _state: &mut ColumnScanState) -> FilterPropagateResult {
        FilterPropagateResult::NoPruningPossible
    }
}
