//! `VariantColumnData` — variant / semi-structured column storage.
//!
//! Mirrors `duckdb/storage/table/variant_column_data.hpp`.
//!
//! The current Rust implementation only keeps the child-column ownership
//! structure aligned with DuckDB. Behavior is still implemented elsewhere or
//! remains `unimplemented!()` where not yet required.

use std::sync::Arc;

use super::column_data::ColumnData;
use super::append_state::ColumnAppendState;

/// Variant / semi-structured column.
///
/// Mirrors `class VariantColumnData : public ColumnData`.
pub struct VariantColumnData {
    /// Child columns: `[unshredded]` or `[unshredded, shredded]`.
    pub sub_columns: Vec<Arc<ColumnData>>,

    /// Top-level validity bitmask, if present.
    pub validity: Option<Arc<ColumnData>>,
}

impl VariantColumnData {
    pub fn new(sub_columns: Vec<Arc<ColumnData>>, validity: Option<Arc<ColumnData>>) -> Self {
        Self {
            sub_columns,
            validity,
        }
    }

    pub fn initialize_append(&self, state: &mut ColumnAppendState) {
        let required = self.sub_columns.len() + usize::from(self.validity.is_some());
        while state.child_appends.len() < required {
            state.child_appends.push(ColumnAppendState::default());
        }
        let mut idx = 0usize;
        if let Some(validity) = &self.validity {
            validity.initialize_append(&mut state.child_appends[idx]);
            idx += 1;
        }
        for child in &self.sub_columns {
            child.initialize_append(&mut state.child_appends[idx]);
            idx += 1;
        }
    }
}
