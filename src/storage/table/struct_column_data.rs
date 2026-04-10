//! `StructColumnData` — nested struct column storage.
//!
//! Mirrors `duckdb/storage/table/struct_column_data.hpp`.
//!
//! A `STRUCT(a T1, b T2, …)` column stores:
//! - One child `ColumnData` per named field.
//! - A top-level validity bitmask.

use std::sync::Arc;

use super::append_state::ColumnAppendState;
use super::column_data::{ColumnData, ColumnDataBase, ColumnDataType};
use super::data_table_info::DataTableInfo;
use super::scan_state::ColumnScanState;
use super::types::Idx;
use super::types::LogicalType;
/// Nested struct column.
///
/// Mirrors `class StructColumnData : public ColumnData`.
pub struct StructColumnData {
    /// One sub-column per struct field (same order as the `LogicalType`).
    pub sub_columns: Vec<Arc<ColumnData>>,

    /// Validity bitmask for top-level STRUCT NULLs.
    pub validity: Arc<ColumnData>,
}

impl StructColumnData {
    pub fn create(
        info: Arc<DataTableInfo>,
        logical_type: &LogicalType,
        data_type: ColumnDataType,
    ) -> Self {
        let validity = ColumnData::validity(Arc::clone(&info), 0, data_type);
        let mut sub_columns = Vec::with_capacity(logical_type.get_struct_child_count());
        for (idx, (_, child_type)) in logical_type.get_struct_fields().iter().enumerate() {
            sub_columns.push(ColumnData::create(
                Arc::clone(&info),
                (idx + 1) as u64,
                child_type.clone(),
                data_type,
                true,
            ));
        }
        Self::new(sub_columns, validity)
    }

    pub fn new(sub_columns: Vec<Arc<ColumnData>>, validity: Arc<ColumnData>) -> Self {
        StructColumnData {
            sub_columns,
            validity,
        }
    }

    pub fn initialize_append(&self, state: &mut super::append_state::ColumnAppendState) {
        let required = self.sub_columns.len() + 1;
        while state.child_appends.len() < required {
            state.child_appends.push(ColumnAppendState::default());
        }
        self.validity.initialize_append(&mut state.child_appends[0]);
        for (idx, sub_column) in self.sub_columns.iter().enumerate() {
            sub_column.initialize_append(&mut state.child_appends[idx + 1]);
        }
    }

    pub fn initialize_scan(&self, state: &mut ColumnScanState) {
        let required = self.sub_columns.len() + 1;
        while state.child_states.len() < required {
            state.child_states.push(ColumnScanState::new());
        }
        state.offset_in_column = 0;
        state.current_segment_index = None;
        state.initialized = false;
        self.validity.initialize_scan(&mut state.child_states[0]);
        for (idx, sub_column) in self.sub_columns.iter().enumerate() {
            sub_column.initialize_scan(&mut state.child_states[idx + 1]);
        }
    }

    pub fn initialize_scan_with_offset(&self, state: &mut ColumnScanState, row_idx: Idx) {
        let required = self.sub_columns.len() + 1;
        while state.child_states.len() < required {
            state.child_states.push(ColumnScanState::new());
        }
        state.offset_in_column = row_idx;
        state.current_segment_index = None;
        state.initialized = false;
        self.validity
            .initialize_scan_with_offset(&mut state.child_states[0], row_idx);
        for (idx, sub_column) in self.sub_columns.iter().enumerate() {
            sub_column.initialize_scan_with_offset(&mut state.child_states[idx + 1], row_idx);
        }
    }

    pub fn revert_append(&self, base: &ColumnDataBase, new_count: Idx) {
        self.validity.revert_append(new_count);
        for sub_column in &self.sub_columns {
            sub_column.revert_append(new_count);
        }
        base.count.store(new_count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn skip(&self, state: &mut ColumnScanState, count: Idx) {
        self.validity.skip_n(&mut state.child_states[0], count);
        for (idx, sub_column) in self.sub_columns.iter().enumerate() {
            sub_column.skip_n(&mut state.child_states[idx + 1], count);
        }
    }
}
