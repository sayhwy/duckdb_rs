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

use super::column_data::{ColumnData, ColumnDataBase, ScanVectorType, logical_type_to_physical};
use super::column_segment::ColumnSegment;
use super::scan_state::ColumnScanState;
use super::types::{Idx, TransactionData};
use crate::common::types::{SelectionVector, Vector};
use crate::function::compression_config::get_compression_function;
use crate::planner::{TableFilter, TableFilterState};

/// Standard (flat) column — the most common column variant.
///
/// Mirrors `class StandardColumnData : public ColumnData`.
pub struct StandardColumnData {
    /// NULL bitmask for this column.
    /// Present unless the column is declared `NOT NULL`.
    pub validity: Option<Arc<ColumnData>>,
}

impl StandardColumnData {
    pub fn new() -> Self {
        StandardColumnData { validity: None }
    }

    pub fn with_validity(validity: Arc<ColumnData>) -> Self {
        StandardColumnData {
            validity: Some(validity),
        }
    }

    /// Returns `true` if there are any NULL values.
    pub fn has_validity(&self) -> bool {
        self.validity.is_some()
    }

    pub fn initialize_scan(&self, base: &ColumnDataBase, state: &mut ColumnScanState) {
        base.initialize_scan(state);
        if let Some(validity) = &self.validity {
            if state.child_states.is_empty() {
                state.child_states.push(ColumnScanState::new());
            }
            validity.initialize_scan(&mut state.child_states[0]);
        }
    }

    pub fn initialize_scan_with_offset(
        &self,
        base: &ColumnDataBase,
        state: &mut ColumnScanState,
        row_idx: Idx,
    ) {
        base.initialize_scan_with_offset(state, row_idx);
        if let Some(validity) = &self.validity {
            if state.child_states.is_empty() {
                state.child_states.push(ColumnScanState::new());
            }
            validity.initialize_scan_with_offset(&mut state.child_states[0], row_idx);
        }
    }

    pub fn scan(
        &self,
        base: &ColumnDataBase,
        transaction: TransactionData,
        vector_index: Idx,
        state: &mut ColumnScanState,
        result: &mut Vector,
    ) -> Idx {
        let Some(validity) = &self.validity else {
            let target_count = base.get_vector_count(vector_index);
            let scan_type = base.get_vector_scan_type(state, target_count);
            let scanned = base.scan_vector(state, result, target_count, scan_type, 0);
            if scanned > 0 {
                if let Some(update_segment) = base.updates.lock().as_ref() {
                    update_segment.fetch_updates(
                        transaction,
                        vector_index,
                        &mut result.raw_data_mut()
                            [..(scanned as usize * base.logical_type.physical_size())],
                    );
                }
            }
            return scanned;
        };

        if state.child_states.is_empty() {
            state.child_states.push(ColumnScanState::new());
            validity.initialize_scan(&mut state.child_states[0]);
        }
        let target_count = base.get_vector_count(vector_index);
        let scan_type = base.get_vector_scan_type(state, target_count);
        let scanned = base.scan_vector(state, result, target_count, scan_type, 0);
        validity
            .base()
            .scan_vector(&mut state.child_states[0], result, target_count, scan_type, 0);
        if scanned > 0 {
            if let Some(update_segment) = base.updates.lock().as_ref() {
                update_segment.fetch_updates(
                    transaction,
                    vector_index,
                    &mut result.raw_data_mut()[..(scanned as usize * base.logical_type.physical_size())],
                );
            }
        }
        scanned
    }

    pub fn filter(
        &self,
        base: &ColumnDataBase,
        transaction: TransactionData,
        vector_index: Idx,
        state: &mut ColumnScanState,
        result: &mut Vector,
        sel: &mut SelectionVector,
        sel_count: &mut Idx,
        filter: &dyn TableFilter,
        filter_state: &dyn TableFilterState,
    ) {
        let scan_count = self.scan(base, transaction, vector_index, state, result);
        result.flatten(scan_count as usize);
        ColumnSegment::filter_selection(sel, result, filter, filter_state, scan_count, sel_count);
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
        let Some(validity) = &self.validity else {
            let scan_count = self.scan(base, transaction, vector_index, state, result);
            result.slice(sel, sel_count as usize);
            if scan_count == 0 {
                result.slice(sel, 0);
            }
            return;
        };

        if state.child_states.is_empty() {
            state.child_states.push(ColumnScanState::new());
            validity.initialize_scan(&mut state.child_states[0]);
        }
        let target_count = base.get_vector_count(vector_index);
        let scan_type = base.get_vector_scan_type(state, target_count);
        let scan_entire_vector = scan_type == ScanVectorType::ScanEntireVector;
        let compression = base.compression.lock();
        let validity_compression = validity.base().compression.lock();
        let has_select = compression
            .as_ref()
            .and_then(|compression_type| {
                let function = get_compression_function(
                    *compression_type,
                    logical_type_to_physical(&base.logical_type),
                );
                function.select
            })
            .is_some();
        let validity_has_select = validity_compression
            .as_ref()
            .and_then(|compression_type| {
                let function = get_compression_function(
                    *compression_type,
                    logical_type_to_physical(&validity.base().logical_type),
                );
                function.select
            })
            .is_some();
        drop(validity_compression);
        drop(compression);
        if has_select && validity_has_select && scan_entire_vector {
            base.begin_scan_vector_internal(state);
            validity.base().begin_scan_vector_internal(&mut state.child_states[0]);
            let current_idx = state
                .current_segment_index
                .expect("standard select missing current segment");
            let validity_idx = state.child_states[0]
                .current_segment_index
                .expect("standard select missing validity segment");
            let (segment_arc, validity_arc) = {
                let data_lock = base.data.lock();
                let validity_lock = validity.base().data.lock();
                (
                    data_lock.0[current_idx].arc(),
                    validity_lock.0[validity_idx].arc(),
                )
            };
            segment_arc.select(state, result, sel, sel_count);
            validity_arc.select(&state.child_states[0], result, sel, sel_count);
            state.offset_in_column += target_count;
            state.internal_index = state.offset_in_column;
            state.child_states[0].offset_in_column += target_count;
            state.child_states[0].internal_index = state.child_states[0].offset_in_column;
            return;
        }
        let _ = self.scan(base, transaction, vector_index, state, result);
        result.slice(sel, sel_count as usize);
    }
}
