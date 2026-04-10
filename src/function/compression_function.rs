use std::any::Any;
use std::fmt;

use crate::common::types::{SelectionVector, Vector};
use crate::storage::table::append_state::ColumnAppendState;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::storage::table::column_checkpoint_state::ColumnCheckpointState;
use crate::storage::table::column_segment::ColumnSegment;
use crate::storage::table::column_segment::UnifiedVectorFormat;
use crate::storage::table::scan_state::{ColumnScanState, SegmentScanState};
use crate::storage::table::types::{CompressionType, Idx, PhysicalType};

#[derive(Debug)]
pub struct CompressionInfo {
    block_size: Idx,
    block_header_size: Idx,
}

impl CompressionInfo {
    pub fn new(block_size: Idx, block_header_size: Idx) -> Self {
        Self {
            block_size,
            block_header_size,
        }
    }

    pub fn get_compaction_flush_limit(&self) -> Idx {
        self.block_size / 5 * 4
    }

    pub fn get_block_size(&self) -> Idx {
        self.block_size
    }

    pub fn get_block_header_size(&self) -> Idx {
        self.block_header_size
    }
}

pub trait AnalyzeState: Send + Sync + fmt::Debug + Any {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
pub trait CompressionState: Send + Sync + fmt::Debug + Any {
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
pub trait CompressedSegmentState: Send + Sync + fmt::Debug {
    fn get_segment_info(&self) -> String {
        String::new()
    }
}

pub type CompressionInitAnalyze = fn(&CompressionInfo) -> Box<dyn AnalyzeState>;
pub type CompressionAnalyze = fn(&mut dyn AnalyzeState, &Vector, Idx) -> bool;
pub type CompressionFinalAnalyze = fn(&mut dyn AnalyzeState) -> Idx;

pub type CompressionInitCompression = fn(
    &CompressionInfo,
    Arc<Mutex<ColumnCheckpointState>>,
    Box<dyn AnalyzeState>,
) -> Box<dyn CompressionState>;
pub type CompressionCompress = fn(&mut dyn CompressionState, &Vector, Idx);
pub type CompressionCompressFinalize = fn(&mut dyn CompressionState);

pub type CompressionInitSegmentScan =
    fn(&ColumnSegment, &mut ColumnScanState) -> Option<Box<dyn SegmentScanState>>;
pub type CompressionScanVector = fn(&ColumnSegment, &ColumnScanState, Idx, &mut Vector);
pub type CompressionScanPartial = fn(&ColumnSegment, &ColumnScanState, Idx, &mut Vector, Idx);
pub type CompressionSelect =
    fn(&ColumnSegment, &ColumnScanState, Idx, &mut Vector, &SelectionVector, Idx);
pub type CompressionFetchRow = fn(&ColumnSegment, Idx, &mut Vector, Idx);
pub type CompressionSkip = fn(&ColumnSegment, &mut ColumnScanState, Idx);

pub type CompressionInitAppend = fn(&ColumnSegment, &mut ColumnAppendState);
pub type CompressionAppend =
    fn(&ColumnSegment, &mut ColumnAppendState, &UnifiedVectorFormat<'_>, Idx, Idx) -> Idx;
pub type CompressionFinalizeAppend = fn(&ColumnSegment, &mut ColumnAppendState) -> Idx;
pub type CompressionRevertAppend = fn(&ColumnSegment, Idx);

#[derive(Clone, Copy)]
pub struct CompressionFunction {
    pub type_: CompressionType,
    pub data_type: PhysicalType,
    pub init_analyze: Option<CompressionInitAnalyze>,
    pub analyze: Option<CompressionAnalyze>,
    pub final_analyze: Option<CompressionFinalAnalyze>,
    pub init_compression: Option<CompressionInitCompression>,
    pub compress: Option<CompressionCompress>,
    pub compress_finalize: Option<CompressionCompressFinalize>,
    pub init_scan: Option<CompressionInitSegmentScan>,
    pub scan_vector: Option<CompressionScanVector>,
    pub scan_partial: Option<CompressionScanPartial>,
    pub select: Option<CompressionSelect>,
    pub fetch_row: Option<CompressionFetchRow>,
    pub skip: Option<CompressionSkip>,
    pub init_append: Option<CompressionInitAppend>,
    pub append: Option<CompressionAppend>,
    pub finalize_append: Option<CompressionFinalizeAppend>,
    pub revert_append: Option<CompressionRevertAppend>,
}

impl fmt::Debug for CompressionFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompressionFunction")
            .field("type_", &self.type_)
            .field("data_type", &self.data_type)
            .finish()
    }
}

impl CompressionFunction {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        type_: CompressionType,
        data_type: PhysicalType,
        init_analyze: Option<CompressionInitAnalyze>,
        analyze: Option<CompressionAnalyze>,
        final_analyze: Option<CompressionFinalAnalyze>,
        init_compression: Option<CompressionInitCompression>,
        compress: Option<CompressionCompress>,
        compress_finalize: Option<CompressionCompressFinalize>,
        init_scan: Option<CompressionInitSegmentScan>,
        scan_vector: Option<CompressionScanVector>,
        scan_partial: Option<CompressionScanPartial>,
        fetch_row: Option<CompressionFetchRow>,
        skip: Option<CompressionSkip>,
        init_append: Option<CompressionInitAppend>,
        append: Option<CompressionAppend>,
        finalize_append: Option<CompressionFinalizeAppend>,
        revert_append: Option<CompressionRevertAppend>,
        select: Option<CompressionSelect>,
    ) -> Self {
        Self {
            type_,
            data_type,
            init_analyze,
            analyze,
            final_analyze,
            init_compression,
            compress,
            compress_finalize,
            init_scan,
            scan_vector,
            scan_partial,
            select,
            fetch_row,
            skip,
            init_append,
            append,
            finalize_append,
            revert_append,
        }
    }
}

pub struct CompressionFunctionSet {
    functions: Vec<Vec<CompressionFunction>>,
}

impl CompressionFunctionSet {
    pub const PHYSICAL_TYPE_COUNT: usize = 17;

    pub fn new() -> Self {
        Self {
            functions: vec![Vec::new(); Self::PHYSICAL_TYPE_COUNT],
        }
    }

    pub fn register_function(&mut self, function: CompressionFunction) {
        let index = Self::get_physical_index(function.data_type);
        self.functions[index].push(function);
    }

    pub fn get_compression_functions(&self, physical_type: PhysicalType) -> &[CompressionFunction] {
        let index = Self::get_physical_index(physical_type);
        &self.functions[index]
    }

    pub fn get_compression_function(
        &self,
        type_: CompressionType,
        physical_type: PhysicalType,
    ) -> Option<&CompressionFunction> {
        self.get_compression_functions(physical_type)
            .iter()
            .find(|function| function.type_ == type_)
    }

    fn get_physical_index(physical_type: PhysicalType) -> usize {
        match physical_type {
            PhysicalType::Bool => 0,
            PhysicalType::Bit => 1,
            PhysicalType::Int8 => 2,
            PhysicalType::Int16 => 3,
            PhysicalType::Int32 => 4,
            PhysicalType::Int64 => 5,
            PhysicalType::Int128 => 6,
            PhysicalType::Uint8 => 7,
            PhysicalType::Uint16 => 8,
            PhysicalType::Uint32 => 9,
            PhysicalType::Uint64 => 10,
            PhysicalType::Float => 11,
            PhysicalType::Double => 12,
            PhysicalType::VarChar => 13,
            PhysicalType::List => 14,
            PhysicalType::Struct => 15,
            PhysicalType::Array | PhysicalType::Invalid => 16,
        }
    }
}
