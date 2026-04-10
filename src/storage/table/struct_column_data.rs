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
use crate::common::types::Vector;
use crate::storage::statistics::{BaseStatistics, StructStats};
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

    pub fn scan(
        &self,
        base: &ColumnDataBase,
        vector_index: Idx,
        state: &mut ColumnScanState,
        result: &mut Vector,
    ) -> Idx {
        self.scan_count(state, result, base.get_vector_count(vector_index), 0)
    }

    pub fn scan_count(
        &self,
        state: &mut ColumnScanState,
        result: &mut Vector,
        count: Idx,
        result_offset: Idx,
    ) -> Idx {
        let scan_count = self
            .validity
            .scan_count(&mut state.child_states[0], result, count, result_offset);
        if result.get_children().len() != self.sub_columns.len() {
            let children = result
                .logical_type
                .get_struct_fields()
                .iter()
                .map(|(_, child_type)| Vector::with_capacity(child_type.clone(), (result_offset + count) as usize))
                .collect();
            result.set_children(children);
        }
        for (idx, sub_column) in self.sub_columns.iter().enumerate() {
            let child_result = &mut result.get_children_mut()[idx];
            let _ = sub_column.scan_count(
                &mut state.child_states[idx + 1],
                child_result,
                count,
                result_offset,
            );
        }
        scan_count
    }

    pub fn append(
        &self,
        base: &ColumnDataBase,
        stats: &mut BaseStatistics,
        state: &mut ColumnAppendState,
        vector: &Vector,
        count: Idx,
    ) {
        if vector.get_vector_type() != crate::common::types::VectorType::Flat {
            unimplemented!("StructColumnData::append currently requires a flat vector");
        }
        if stats.child_stats.is_empty() {
            StructStats::construct(stats);
        }

        self.validity
            .append(stats, &mut state.child_appends[0], vector, count);
        let child_entries = vector.get_children();
        assert_eq!(
            child_entries.len(),
            self.sub_columns.len(),
            "StructColumnData::append child count mismatch"
        );
        for (idx, child_entry) in child_entries.iter().enumerate() {
            self.sub_columns[idx].append(
                StructStats::get_child_stats_mut(stats, idx),
                &mut state.child_appends[idx + 1],
                child_entry,
                count,
            );
        }
        base.count
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn skip(&self, state: &mut ColumnScanState, count: Idx) {
        self.validity.skip_n(&mut state.child_states[0], count);
        for (idx, sub_column) in self.sub_columns.iter().enumerate() {
            sub_column.skip_n(&mut state.child_states[idx + 1], count);
        }
    }
}
