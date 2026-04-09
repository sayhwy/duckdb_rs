use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::types::{SelectionVector, Vector, VectorType};
use crate::function::compression_function::{
    AnalyzeState, CompressionFunction, CompressionInfo, CompressionState,
};
use crate::storage::statistics::BaseStatistics;
use crate::storage::table::column_checkpoint_state::ColumnCheckpointState;
use crate::storage::table::column_segment::ColumnSegment;
use crate::storage::table::scan_state::{ColumnScanState, SegmentScanState};
use crate::storage::table::types::{CompressionType, Idx, PhysicalType};

#[derive(Debug)]
struct ConstantAnalyzeState;
impl AnalyzeState for ConstantAnalyzeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
struct ConstantCompressionState;
impl CompressionState for ConstantCompressionState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn init_analyze(_info: &CompressionInfo) -> Box<dyn AnalyzeState> {
    Box::new(ConstantAnalyzeState)
}

fn analyze(_state: &mut dyn AnalyzeState, _input: &Vector, _count: Idx) -> bool {
    true
}

fn final_analyze(_state: &mut dyn AnalyzeState) -> Idx {
    Idx::MAX
}

fn init_compression(
    _info: &CompressionInfo,
    _checkpoint_state: Arc<Mutex<ColumnCheckpointState>>,
    _analyze_state: Box<dyn AnalyzeState>,
) -> Box<dyn CompressionState> {
    Box::new(ConstantCompressionState)
}

fn compress(_state: &mut dyn CompressionState, _scan_vector: &Vector, _count: Idx) {}

fn compress_finalize(_state: &mut dyn CompressionState) {}

fn init_scan(_segment: &ColumnSegment, _state: &mut ColumnScanState) -> Option<Box<dyn SegmentScanState>> {
    None
}

trait ConstantValue: Copy {
    fn get(stats: &BaseStatistics) -> Self;
    fn write(self, dst: &mut [u8]);
}

macro_rules! impl_constant_value {
    ($t:ty, $field:ident) => {
        impl ConstantValue for $t {
            fn get(stats: &BaseStatistics) -> Self {
                let data = stats
                    .get_numeric_data()
                    .expect("constant compression requires numeric statistics");
                unsafe { data.min.$field as $t }
            }

            fn write(self, dst: &mut [u8]) {
                dst[..std::mem::size_of::<$t>()].copy_from_slice(&self.to_le_bytes());
            }
        }
    };
}

impl ConstantValue for bool {
    fn get(stats: &BaseStatistics) -> Self {
        let data = stats
            .get_numeric_data()
            .expect("constant compression requires numeric statistics");
        unsafe { data.min.boolean }
    }

    fn write(self, dst: &mut [u8]) {
        dst[0] = u8::from(self);
    }
}

impl_constant_value!(i8, tinyint);
impl_constant_value!(i16, smallint);
impl_constant_value!(i32, integer);
impl_constant_value!(i64, bigint);
impl_constant_value!(i128, hugeint);
impl_constant_value!(u8, tinyint);
impl_constant_value!(u16, smallint);
impl_constant_value!(u32, integer);
impl_constant_value!(u64, bigint);

impl ConstantValue for f32 {
    fn get(stats: &BaseStatistics) -> Self {
        let data = stats
            .get_numeric_data()
            .expect("constant compression requires numeric statistics");
        unsafe { data.min.float }
    }

    fn write(self, dst: &mut [u8]) {
        dst[..4].copy_from_slice(&self.to_le_bytes());
    }
}

impl ConstantValue for f64 {
    fn get(stats: &BaseStatistics) -> Self {
        let data = stats
            .get_numeric_data()
            .expect("constant compression requires numeric statistics");
        unsafe { data.min.double }
    }

    fn write(self, dst: &mut [u8]) {
        dst[..8].copy_from_slice(&self.to_le_bytes());
    }
}

fn all_null(segment: &ColumnSegment) -> bool {
    let stats = segment.stats.lock();
    stats.statistics().can_have_null() && !stats.statistics().can_have_no_null()
}

fn fill_validity(result: &mut Vector, start_idx: usize, count: usize, valid: bool) {
    for i in 0..count {
        if valid {
            result.validity.set_valid(start_idx + i);
        } else {
            result.validity.set_invalid(start_idx + i);
        }
    }
}

fn scan_partial_t<T: ConstantValue>(
    segment: &ColumnSegment,
    _state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
    result_offset: Idx,
) {
    result.vector_type = VectorType::Flat;
    if all_null(segment) {
        fill_validity(result, result_offset as usize, scan_count as usize, false);
        return;
    }
    let value = {
        let stats = segment.stats.lock();
        T::get(stats.statistics())
    };
    let elem_size = std::mem::size_of::<T>();
    let dst = result.raw_data_mut();
    for i in 0..scan_count as usize {
        let offset = (result_offset as usize + i) * elem_size;
        value.write(&mut dst[offset..offset + elem_size]);
    }
    fill_validity(result, result_offset as usize, scan_count as usize, true);
}

fn scan_vector_t<T: ConstantValue>(
    segment: &ColumnSegment,
    _state: &ColumnScanState,
    _scan_count: Idx,
    result: &mut Vector,
) {
    result.vector_type = VectorType::Constant;
    if all_null(segment) {
        result.validity.set_invalid(0);
        return;
    }
    let value = {
        let stats = segment.stats.lock();
        T::get(stats.statistics())
    };
    value.write(result.raw_data_mut());
    result.validity.set_valid(0);
}

fn fetch_row_t<T: ConstantValue>(segment: &ColumnSegment, _row_id: Idx, result: &mut Vector, result_idx: Idx) {
    scan_partial_t::<T>(segment, &ColumnScanState::default(), 1, result, result_idx);
}

fn select_t<T: ConstantValue>(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    vector_count: Idx,
    result: &mut Vector,
    _sel: &SelectionVector,
    _sel_count: Idx,
) {
    scan_vector_t::<T>(segment, state, vector_count, result);
}

fn empty_skip(_segment: &ColumnSegment, _state: &mut ColumnScanState, _skip_count: Idx) {}

pub struct ConstantFun;

impl ConstantFun {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        CompressionFunction::new(
            CompressionType::Constant,
            data_type,
            Some(init_analyze),
            Some(analyze),
            Some(final_analyze),
            Some(init_compression),
            Some(compress),
            Some(compress_finalize),
            Some(init_scan),
            Some(match data_type {
                PhysicalType::Bool => scan_vector_t::<bool>,
                PhysicalType::Int8 => scan_vector_t::<i8>,
                PhysicalType::Int16 => scan_vector_t::<i16>,
                PhysicalType::Int32 => scan_vector_t::<i32>,
                PhysicalType::Int64 => scan_vector_t::<i64>,
                PhysicalType::Int128 => scan_vector_t::<i128>,
                PhysicalType::Uint8 => scan_vector_t::<u8>,
                PhysicalType::Uint16 => scan_vector_t::<u16>,
                PhysicalType::Uint32 => scan_vector_t::<u32>,
                PhysicalType::Uint64 => scan_vector_t::<u64>,
                PhysicalType::Float => scan_vector_t::<f32>,
                PhysicalType::Double => scan_vector_t::<f64>,
                _ => unreachable!("unsupported constant physical type"),
            }),
            Some(match data_type {
                PhysicalType::Bool => scan_partial_t::<bool>,
                PhysicalType::Int8 => scan_partial_t::<i8>,
                PhysicalType::Int16 => scan_partial_t::<i16>,
                PhysicalType::Int32 => scan_partial_t::<i32>,
                PhysicalType::Int64 => scan_partial_t::<i64>,
                PhysicalType::Int128 => scan_partial_t::<i128>,
                PhysicalType::Uint8 => scan_partial_t::<u8>,
                PhysicalType::Uint16 => scan_partial_t::<u16>,
                PhysicalType::Uint32 => scan_partial_t::<u32>,
                PhysicalType::Uint64 => scan_partial_t::<u64>,
                PhysicalType::Float => scan_partial_t::<f32>,
                PhysicalType::Double => scan_partial_t::<f64>,
                _ => unreachable!("unsupported constant physical type"),
            }),
            Some(match data_type {
                PhysicalType::Bool => fetch_row_t::<bool>,
                PhysicalType::Int8 => fetch_row_t::<i8>,
                PhysicalType::Int16 => fetch_row_t::<i16>,
                PhysicalType::Int32 => fetch_row_t::<i32>,
                PhysicalType::Int64 => fetch_row_t::<i64>,
                PhysicalType::Int128 => fetch_row_t::<i128>,
                PhysicalType::Uint8 => fetch_row_t::<u8>,
                PhysicalType::Uint16 => fetch_row_t::<u16>,
                PhysicalType::Uint32 => fetch_row_t::<u32>,
                PhysicalType::Uint64 => fetch_row_t::<u64>,
                PhysicalType::Float => fetch_row_t::<f32>,
                PhysicalType::Double => fetch_row_t::<f64>,
                _ => unreachable!("unsupported constant physical type"),
            }),
            Some(empty_skip),
            None,
            None,
            None,
            None,
            Some(match data_type {
                PhysicalType::Bool => select_t::<bool>,
                PhysicalType::Int8 => select_t::<i8>,
                PhysicalType::Int16 => select_t::<i16>,
                PhysicalType::Int32 => select_t::<i32>,
                PhysicalType::Int64 => select_t::<i64>,
                PhysicalType::Int128 => select_t::<i128>,
                PhysicalType::Uint8 => select_t::<u8>,
                PhysicalType::Uint16 => select_t::<u16>,
                PhysicalType::Uint32 => select_t::<u32>,
                PhysicalType::Uint64 => select_t::<u64>,
                PhysicalType::Float => select_t::<f32>,
                PhysicalType::Double => select_t::<f64>,
                _ => unreachable!("unsupported constant physical type"),
            }),
        )
    }

    pub fn type_is_supported(physical_type: PhysicalType) -> bool {
        matches!(
            physical_type,
            PhysicalType::Bool
                | PhysicalType::Int8
                | PhysicalType::Int16
                | PhysicalType::Int32
                | PhysicalType::Int64
                | PhysicalType::Int128
                | PhysicalType::Uint8
                | PhysicalType::Uint16
                | PhysicalType::Uint32
                | PhysicalType::Uint64
                | PhysicalType::Float
                | PhysicalType::Double
        )
    }
}
