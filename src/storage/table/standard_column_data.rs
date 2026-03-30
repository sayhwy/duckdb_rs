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

use super::column_data::ColumnDataContext;
use super::validity_column_data::ValidityColumnData;

/// Standard (flat) column — the most common column variant.
///
/// Mirrors `class StandardColumnData : public ColumnData`.
pub struct StandardColumnData {
    /// Shared column metadata and segment tree.
    pub ctx: ColumnDataContext,

    /// NULL bitmask for this column.
    /// Present unless the column is declared `NOT NULL`.
    pub validity: Option<Arc<ValidityColumnData>>,
}

impl StandardColumnData {
    pub fn new(ctx: ColumnDataContext) -> Self {
        StandardColumnData {
            ctx,
            validity: None,
        }
    }

    pub fn with_validity(ctx: ColumnDataContext, validity: Arc<ValidityColumnData>) -> Self {
        StandardColumnData {
            ctx,
            validity: Some(validity),
        }
    }

    /// Returns `true` if there are any NULL values.
    pub fn has_validity(&self) -> bool {
        self.validity.is_some()
    }
}
