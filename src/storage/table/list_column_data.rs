//! `ListColumnData` — variable-length list column storage.
//!
//! Mirrors `duckdb/storage/table/list_column_data.hpp`.
//!
//! A `LIST(T)` column stores:
//! - An **offsets** segment (one `u64` per row — index into `child`).
//! - A **child** `ColumnData` holding the flat element values.
//! - A **validity** bitmask.

use std::sync::Arc;

use super::append_state::ColumnAppendState;
use super::column_data::{ColumnData, ColumnDataBase, ColumnDataType, ScanVectorType};
use super::data_table_info::DataTableInfo;
use super::segment_lock::SegmentLock;
use super::scan_state::ColumnScanState;
use super::types::{Idx, LogicalType};
use crate::common::types::Vector;
use crate::storage::statistics::{BaseStatistics, ListStats};
use crate::storage::statistics::FilterPropagateResult;
/// Variable-length list column.
///
/// Mirrors `class ListColumnData : public ColumnData`.
pub struct ListColumnData {
    /// Offset array: `offsets[i]` is the start index in `child` for row `i`.
    /// Stored as a `StandardColumnData` of type `UBIGINT`.
    pub child_column: Arc<ColumnData>,

    /// Validity bitmask for top-level LIST nulls.
    pub validity: Arc<ColumnData>,
}

impl ListColumnData {
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
            .expect("LIST logical type missing child type")
            .clone();
        let validity = ColumnData::validity(Arc::clone(&info), 0, data_type);
        let child_column = ColumnData::create(info, 1, child_type, data_type, true);
        Self::new(child_column, validity)
    }

    pub fn new(child_column: Arc<ColumnData>, validity: Arc<ColumnData>) -> Self {
        ListColumnData {
            child_column,
            validity,
        }
    }

    pub fn initialize_append(&self, state: &mut super::append_state::ColumnAppendState) {
        while state.child_appends.len() < 2 {
            state.child_appends.push(ColumnAppendState::default());
        }
        self.validity.initialize_append(&mut state.child_appends[0]);
        self.child_column.initialize_append(&mut state.child_appends[1]);
    }

    pub fn initialize_scan(&self, base: &ColumnDataBase, state: &mut ColumnScanState) {
        base.initialize_scan(state);
        while state.child_states.len() < 2 {
            state.child_states.push(ColumnScanState::new());
        }
        self.validity.initialize_scan(&mut state.child_states[0]);
        self.child_column.initialize_scan(&mut state.child_states[1]);
    }

    fn fetch_list_offset(&self, base: &ColumnDataBase, row_idx: Idx) -> Idx {
        let mut lock: SegmentLock<'_, _> = base.data.lock();
        let segment = base
            .data
            .get_segment(&mut lock, row_idx)
            .expect("ListColumnData::fetch_list_offset row_idx out of range");
        let index_in_segment = row_idx - segment.row_start();
        let mut result = Vector::with_capacity(LogicalType::ubigint(), 1);
        segment.node().fetch_row(index_in_segment, &mut result, 0);
        let width = std::mem::size_of::<u64>();
        u64::from_le_bytes(
            result.raw_data()[..width]
                .try_into()
                .expect("invalid list offset row"),
        )
    }

    pub fn initialize_scan_with_offset(
        &self,
        base: &ColumnDataBase,
        state: &mut ColumnScanState,
        row_idx: Idx,
    ) {
        if row_idx == 0 {
            self.initialize_scan(base, state);
            return;
        }
        base.initialize_scan_with_offset(state, row_idx);
        while state.child_states.len() < 2 {
            state.child_states.push(ColumnScanState::new());
        }
        self.validity
            .initialize_scan_with_offset(&mut state.child_states[0], row_idx);
        let child_offset = self.fetch_list_offset(base, row_idx - 1);
        debug_assert!(child_offset <= self.child_column.base.count());
        if child_offset < self.child_column.base.count() {
            self.child_column
                .initialize_scan_with_offset(&mut state.child_states[1], child_offset);
        }
        state.last_offset = child_offset;
    }

    pub fn revert_append(&self, base: &ColumnDataBase, new_count: Idx) {
        self.validity.revert_append(new_count);
        let column_count = base.count();
        if column_count == 0 {
            return;
        }
        let list_offset = self.fetch_list_offset(base, column_count - 1);
        self.child_column.revert_append(list_offset);
    }

    pub fn scan_count(
        &self,
        base: &ColumnDataBase,
        state: &mut ColumnScanState,
        result: &mut Vector,
        count: Idx,
        result_offset: Idx,
    ) -> Idx {
        if result_offset > 0 {
            panic!("ListColumnData::ScanCount not supported with result_offset > 0");
        }
        if count == 0 {
            return 0;
        }

        let mut offset_vector = Vector::with_capacity(LogicalType::ubigint(), count as usize);
        let scan_count = base.scan_vector(state, &mut offset_vector, count, ScanVectorType::ScanFlatVector, 0);
        debug_assert!(scan_count > 0);

        let _ = self
            .validity
            .scan_count(&mut state.child_states[0], result, count, 0);

        let base_offset = state.last_offset;
        let mut current_offset = 0u64;
        let entry_width = 8usize;
        let child_entry_type = result
            .logical_type
            .get_child_type()
            .expect("LIST result missing child type")
            .clone();

        let width = std::mem::size_of::<u64>();
        let mut last_entry = base_offset;
        for i in 0..scan_count as usize {
            let source_offset = i * width;
            last_entry = u64::from_le_bytes(
                offset_vector.raw_data()[source_offset..source_offset + width]
                    .try_into()
                    .expect("invalid list offset vector"),
            );
            let length = last_entry
                .checked_sub(base_offset + current_offset)
                .expect("invalid list offset ordering");
            let target_offset = i * entry_width;
            result.raw_data_mut()[target_offset..target_offset + 4]
                .copy_from_slice(&(current_offset as u32).to_le_bytes());
            result.raw_data_mut()[target_offset + 4..target_offset + 8]
                .copy_from_slice(&(length as u32).to_le_bytes());
            current_offset += length;
        }

        let child_scan_count = last_entry
            .checked_sub(base_offset)
            .expect("invalid child list scan count");
        let mut child_vector = Vector::with_capacity(child_entry_type, child_scan_count as usize);
        if child_scan_count > 0 {
            let _ = self
                .child_column
                .scan_count(&mut state.child_states[1], &mut child_vector, child_scan_count, 0);
        }
        result.set_child(child_vector);
        state.last_offset = last_entry;
        scan_count
    }

    pub fn scan(
        &self,
        base: &ColumnDataBase,
        vector_index: Idx,
        state: &mut ColumnScanState,
        result: &mut Vector,
    ) -> Idx {
        self.scan_count(base, state, result, base.get_vector_count(vector_index), 0)
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
            unimplemented!("ListColumnData::append currently requires a flat vector");
        }
        if stats.child_stats.is_empty() {
            ListStats::construct(stats);
        }

        let list_child = vector
            .get_child()
            .expect("ListColumnData::append requires list child vector");
        let mut append_mask = crate::common::types::ValidityMask::new(count as usize);
        append_mask.reset(count as usize);
        let mut append_offsets = vec![0u8; count as usize * std::mem::size_of::<u64>()];

        let start_offset = self.child_column.base.count();
        let mut child_count = 0u64;
        let mut child_contiguous = true;
        let mut input_entries = Vec::with_capacity(count as usize);
        for i in 0..count as usize {
            let byte_offset = i * 8;
            let entry = &vector.raw_data()[byte_offset..byte_offset + 8];
            let offset = u32::from_le_bytes(entry[0..4].try_into().expect("invalid list entry")) as u64;
            let length = u32::from_le_bytes(entry[4..8].try_into().expect("invalid list entry")) as u64;
            input_entries.push((offset, length));
            if !vector.validity.row_is_valid(i) {
                append_mask.set_invalid(i);
                append_offsets[byte_offset..byte_offset + 8]
                    .copy_from_slice(&(start_offset + child_count).to_le_bytes());
                continue;
            }
            if offset != child_count {
                child_contiguous = false;
            }
            child_count += length;
            append_offsets[byte_offset..byte_offset + 8]
                .copy_from_slice(&(start_offset + child_count).to_le_bytes());
        }

        let child_vector = if child_contiguous {
            list_child.shallow_clone()
        } else {
            match list_child.get_type().id {
                crate::common::types::LogicalTypeId::Array
                | crate::common::types::LogicalTypeId::List
                | crate::common::types::LogicalTypeId::Struct
                | crate::common::types::LogicalTypeId::Variant => {
                    unimplemented!("ListColumnData::append non-contiguous nested list child");
                }
                _ => {
                    let mut contiguous_child =
                        Vector::with_capacity(list_child.get_type().clone(), child_count as usize);
                    let mut current_count = 0usize;
                    for (i, (offset, length)) in input_entries.iter().enumerate() {
                        if !vector.validity.row_is_valid(i) {
                            continue;
                        }
                        for list_idx in 0..*length as usize {
                            contiguous_child.copy_row_from(
                                list_child,
                                *offset as usize + list_idx,
                                current_count,
                            );
                            current_count += 1;
                        }
                    }
                    debug_assert_eq!(current_count, child_count as usize);
                    contiguous_child
                }
            }
        };

        if child_count > 0 {
            self.child_column.append(
                ListStats::get_child_stats_mut(stats),
                &mut state.child_appends[1],
                &child_vector,
                child_count,
            );
        }

        let offset_vector = Vector::with_capacity(LogicalType::ubigint(), count as usize);
        let mut offset_vector = offset_vector;
        offset_vector.raw_data_mut().copy_from_slice(&append_offsets);
        let offset_vdata = super::column_segment::UnifiedVectorFormat::from_flat_vector(&offset_vector);
        base.append_data(
            &mut crate::storage::statistics::SegmentStatistics::from_stats(stats.clone()),
            state,
            &offset_vdata,
            count,
        );

        let mut validity_vector = Vector::with_capacity(LogicalType::validity(), count as usize);
        validity_vector.validity = append_mask;
        let validity_vdata =
            super::column_segment::UnifiedVectorFormat::from_flat_vector(&validity_vector);
        self.validity.base.append_data(
            &mut crate::storage::statistics::SegmentStatistics::from_stats(stats.clone()),
            &mut state.child_appends[0],
            &validity_vdata,
            count,
        );
    }

    pub fn skip(&self, base: &ColumnDataBase, state: &mut ColumnScanState, count: Idx) {
        self.validity.skip_n(&mut state.child_states[0], count);

        let mut offset_vector = Vector::with_capacity(LogicalType::ubigint(), count as usize);
        let scan_count = base.scan_vector(
            state,
            &mut offset_vector,
            count,
            super::column_data::ScanVectorType::ScanFlatVector,
            0,
        );
        if scan_count == 0 {
            return;
        }
        let width = std::mem::size_of::<u64>();
        let last_idx = (scan_count - 1) as usize;
        let offset = last_idx * width;
        let last_entry = u64::from_le_bytes(
            offset_vector.raw_data()[offset..offset + width]
                .try_into()
                .expect("invalid ubigint offset vector"),
        );
        let child_scan_count = last_entry.saturating_sub(state.last_offset);
        if child_scan_count == 0 {
            return;
        }
        state.last_offset = last_entry;
        self.child_column
            .skip_n(&mut state.child_states[1], child_scan_count);
    }
}
