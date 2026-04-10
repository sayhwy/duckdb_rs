use crate::common::types::{SelectionVector, Vector};
use crate::function::compression_function::CompressionFunction;
use crate::storage::table::column_segment::ColumnSegment;
use crate::storage::table::scan_state::{ColumnScanState, SegmentScanState};
use crate::storage::table::types::{CompressionType, Idx, PhysicalType};

#[derive(Debug, Default)]
struct EmptyValiditySegmentScanState;

impl SegmentScanState for EmptyValiditySegmentScanState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn init_scan(_segment: &ColumnSegment, _state: &mut ColumnScanState) -> Option<Box<dyn SegmentScanState>> {
    Some(Box::new(EmptyValiditySegmentScanState))
}

fn scan_vector(_segment: &ColumnSegment, _state: &ColumnScanState, _scan_count: Idx, _result: &mut Vector) {}

fn scan_partial(
    _segment: &ColumnSegment,
    _state: &ColumnScanState,
    _scan_count: Idx,
    _result: &mut Vector,
    _result_offset: Idx,
) {
}

fn fetch_row(_segment: &ColumnSegment, _row_id: Idx, _result: &mut Vector, _result_idx: Idx) {}

fn skip(_segment: &ColumnSegment, _state: &mut ColumnScanState, _skip_count: Idx) {}

fn select(
    _segment: &ColumnSegment,
    _state: &ColumnScanState,
    _vector_count: Idx,
    _result: &mut Vector,
    _sel: &SelectionVector,
    _sel_count: Idx,
) {
}

pub struct EmptyValidityCompressionFun;

impl EmptyValidityCompressionFun {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        CompressionFunction::new(
            CompressionType::Empty,
            data_type,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(init_scan),
            Some(scan_vector),
            Some(scan_partial),
            Some(fetch_row),
            Some(skip),
            None,
            None,
            None,
            None,
            Some(select),
        )
    }

    pub fn type_is_supported(physical_type: PhysicalType) -> bool {
        physical_type == PhysicalType::Bit
    }
}
