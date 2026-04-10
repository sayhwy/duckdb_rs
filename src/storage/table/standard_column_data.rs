//! `StandardColumnData` — flat (scalar) column storage.
//!
//! Mirrors `duckdb/storage/table/standard_column_data.hpp`.
//!
//! Handles all fixed-width and simple variable-width types:
//! `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`, `TIMESTAMP`, `VARCHAR`, …
//!
//! A `StandardColumnData` always has:
//! - An optional `ValidityColumnData` child (bit-mask for NULLs).
//! - A flat `ColumnSegmentTree` of compressed segments.

use std::sync::Arc;

use super::column_data::ColumnDataBase;
use super::validity_column_data::ValidityColumnData;

/// Standard (flat) column — the most common column variant.
///
/// Mirrors `class StandardColumnData : public ColumnData`.
pub struct StandardColumnData {
    /// Shared column metadata and segment tree.
    pub base: ColumnDataBase,

    /// NULL bitmask for this column.
    /// Present unless the column is declared `NOT NULL`.
    pub validity: Option<Arc<ValidityColumnData>>,
}

impl StandardColumnData {
    pub fn new(base: ColumnDataBase) -> Self {
        StandardColumnData {
            base,
            validity: None,
        }
    }

    pub fn with_validity(base: ColumnDataBase, validity: Arc<ValidityColumnData>) -> Self {
        StandardColumnData {
            base,
            validity: Some(validity),
        }
    }

    /// Returns `true` if there are any NULL values.
    pub fn has_validity(&self) -> bool {
        self.validity.is_some()
    }
}
