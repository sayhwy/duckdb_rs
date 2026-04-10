//! `ArrayColumnData` — fixed-length array column storage.
//!
//! Mirrors `duckdb/storage/table/array_column_data.hpp`.
//!
//! A `ARRAY(T, N)` column stores:
//! - A flat child `ColumnData` holding `row_count * N` values.
//! - A validity bitmask (for NULL arrays).

use std::sync::Arc;

use super::append_state::ColumnAppendState;
use super::column_data::{ColumnData, ColumnDataBase, ColumnDataType};
use super::data_table_info::DataTableInfo;
use super::scan_state::ColumnScanState;
use super::types::{Idx, LogicalType, TransactionData};
use crate::common::types::{LogicalTypeId, SelectionVector, Vector};
use crate::storage::statistics::FilterPropagateResult;
/// Fixed-length array column.
///
/// Mirrors `class ArrayColumnData : public ColumnData`.
pub struct ArrayColumnData {
    /// Flat element storage (length = `row_count * array_size`).
    pub child_column: Arc<ColumnData>,

    /// Validity bitmask for top-level ARRAY NULLs.
    pub validity: Arc<ColumnData>,

    /// Fixed element count per row (the `N` in `ARRAY(T, N)`).
    pub array_size: u32,
}

impl ArrayColumnData {
    pub fn check_zonemap(&self, _state: &mut ColumnScanState) -> FilterPropagateResult {
        FilterPropagateResult::NoPruningPossible
    }

    pub fn create(
        info: Arc<DataTableInfo>,
        logical_type: &LogicalType,
        data_type: ColumnDataType,
    ) -> Self {
        let child_type = logical_type
            .get_child_type()
            .expect("ARRAY logical type missing child type")
            .clone();
        let validity = ColumnData::validity(Arc::clone(&info), 0, data_type);
        let child_column = ColumnData::create(Arc::clone(&info), 1, child_type, data_type, true);
        Self::new(child_column, validity, logical_type.get_array_size() as u32)
    }

    pub fn new(
        child_column: Arc<ColumnData>,
        validity: Arc<ColumnData>,
        array_size: u32,
    ) -> Self {
        ArrayColumnData {
            child_column,
            validity,
            array_size,
        }
    }

    pub fn initialize_append(&self, state: &mut super::append_state::ColumnAppendState) {
        while state.child_appends.len() < 2 {
            state.child_appends.push(ColumnAppendState::default());
        }
        self.validity.initialize_append(&mut state.child_appends[0]);
        self.child_column.initialize_append(&mut state.child_appends[1]);
    }

    pub fn initialize_scan(&self, state: &mut ColumnScanState) {
        while state.child_states.len() < 2 {
            state.child_states.push(ColumnScanState::new());
        }
        state.offset_in_column = 0;
        state.current_segment_index = None;
        state.initialized = false;
        self.validity.initialize_scan(&mut state.child_states[0]);
        self.child_column.initialize_scan(&mut state.child_states[1]);
    }

    pub fn initialize_scan_with_offset(&self, state: &mut ColumnScanState, row_idx: Idx) {
        while state.child_states.len() < 2 {
            state.child_states.push(ColumnScanState::new());
        }
        if row_idx == 0 {
            self.initialize_scan(state);
            return;
        }
        state.offset_in_column = row_idx;
        state.current_segment_index = None;
        state.initialized = false;
        self.validity
            .initialize_scan_with_offset(&mut state.child_states[0], row_idx);
        let child_offset = row_idx * u64::from(self.array_size);
        if child_offset < self.child_column.base.count() {
            self.child_column
                .initialize_scan_with_offset(&mut state.child_states[1], child_offset);
        }
    }

    pub fn revert_append(&self, base: &ColumnDataBase, new_count: Idx) {
        self.validity.revert_append(new_count);
        self.child_column
            .revert_append(new_count * u64::from(self.array_size));
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
        let child_type = result
            .logical_type
            .get_child_type()
            .expect("ARRAY result missing child type")
            .clone();
        let total_child_count = (result_offset + count) * u64::from(self.array_size);
        let child_offset = result_offset * u64::from(self.array_size);

        let needs_new_child = result
            .get_child()
            .map(|child| {
                child.get_type() != &child_type || child.raw_data().len() < total_child_count as usize * child_type.physical_size()
            })
            .unwrap_or(true);
        if needs_new_child {
            result.set_child(Vector::with_capacity(child_type.clone(), total_child_count as usize));
        }
        let child_vector = result
            .get_child_mut()
            .expect("ARRAY result child vector missing after initialization");
        let _ = self.child_column.scan_count(
            &mut state.child_states[1],
            child_vector,
            count * u64::from(self.array_size),
            child_offset,
        );
        scan_count
    }

    pub fn select(
        &self,
        base: &ColumnDataBase,
        transaction: TransactionData,
        vector_index: Idx,
        state: &mut ColumnScanState,
        result: &mut Vector,
        sel: &SelectionVector,
        sel_count: Idx,
    ) {
        let child_type = self.child_column.base.logical_type.clone();
        let is_supported = !matches!(
            child_type.id,
            LogicalTypeId::Array | LogicalTypeId::List | LogicalTypeId::Struct | LogicalTypeId::Variant
        ) && child_type.id != LogicalTypeId::Varchar;
        if !is_supported {
            let _ = self.scan(base, vector_index, state, result);
            result.slice(sel, sel_count as usize);
            return;
        }

        let mut consecutive_ranges = 0u64;
        let mut i = 0usize;
        while i < sel_count as usize {
            let start_idx = sel.get_index(i) as u64;
            let mut end_idx = start_idx + 1;
            while i + 1 < sel_count as usize {
                let next_idx = sel.get_index(i + 1) as u64;
                if next_idx > end_idx {
                    break;
                }
                end_idx = next_idx + 1;
                i += 1;
            }
            let _ = (start_idx, end_idx);
            consecutive_ranges += 1;
            i += 1;
        }

        let target_count = base.get_vector_count(vector_index);
        let allowed_ranges = u64::from(self.array_size / 2);
        if allowed_ranges < consecutive_ranges {
            let _ = transaction;
            let _ = self.scan(base, vector_index, state, result);
            result.slice(sel, sel_count as usize);
            return;
        }

        let mut current_offset = 0u64;
        let mut current_position = 0u64;
        let total_child_count = sel_count * u64::from(self.array_size);
        result.set_child(Vector::with_capacity(child_type, total_child_count as usize));

        let mut i = 0usize;
        while i < sel_count as usize {
            let start_idx = sel.get_index(i) as u64;
            let mut end_idx = start_idx + 1;
            while i + 1 < sel_count as usize {
                let next_idx = sel.get_index(i + 1) as u64;
                if next_idx > end_idx {
                    break;
                }
                end_idx = next_idx + 1;
                i += 1;
            }

            if start_idx > current_position {
                let skip_amount = start_idx - current_position;
                self.validity
                    .skip_n(&mut state.child_states[0], skip_amount);
                self.child_column.skip_n(
                    &mut state.child_states[1],
                    skip_amount * u64::from(self.array_size),
                );
            }

            let scan_count = end_idx - start_idx;
            let _ = self
                .validity
                .scan_count(&mut state.child_states[0], result, scan_count, current_offset);
            let child_vec = result
                .get_child_mut()
                .expect("ARRAY result child vector missing after initialization");
            let _ = self.child_column.scan_count(
                &mut state.child_states[1],
                child_vec,
                scan_count * u64::from(self.array_size),
                current_offset * u64::from(self.array_size),
            );

            current_offset += scan_count;
            current_position = end_idx;
            i += 1;
        }

        if current_position < target_count {
            let skip_amount = target_count - current_position;
            self.validity
                .skip_n(&mut state.child_states[0], skip_amount);
            self.child_column.skip_n(
                &mut state.child_states[1],
                skip_amount * u64::from(self.array_size),
            );
        }
    }

    pub fn skip(&self, state: &mut ColumnScanState, count: Idx) {
        self.validity.skip_n(&mut state.child_states[0], count);
        self.child_column
            .skip_n(&mut state.child_states[1], count * u64::from(self.array_size));
    }
}
