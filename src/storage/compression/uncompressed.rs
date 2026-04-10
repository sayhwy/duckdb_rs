// ============================================================
// uncompressed.rs
// 对应 C++:
//   duckdb/storage/compression/uncompressed.cpp
//   duckdb/storage/compression/fixed_size_uncompressed.cpp
// ============================================================

use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::types::{SelectionVector, Vector};
use crate::function::compression_function::{
    AnalyzeState, CompressedSegmentState, CompressionFunction, CompressionInfo, CompressionState,
};
use crate::storage::table::append_state::ColumnAppendState;
use crate::storage::table::column_checkpoint_state::ColumnCheckpointState;
use crate::storage::table::column_segment::{ColumnSegment, UnifiedVectorFormat};
use crate::storage::table::scan_state::{ColumnScanState, SegmentScanState};
use crate::storage::table::segment_base::SegmentBase;
use crate::storage::table::types::{CompressionType, Idx, PhysicalType};

/// 未压缩数据读取器（直接从字节切片中读取原始数据）
pub struct UncompressedReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> UncompressedReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn read_bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return None;
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Some(slice)
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        let b = self.data.get(self.pos).copied();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    pub fn read_u16_le(&mut self) -> Option<u16> {
        let bytes = self.read_bytes(2)?;
        Some(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn read_u32_le(&mut self) -> Option<u32> {
        let bytes = self.read_bytes(4)?;
        Some(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn read_u64_le(&mut self) -> Option<u64> {
        let bytes = self.read_bytes(8)?;
        Some(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn position(&self) -> usize {
        self.pos
    }
}

#[derive(Debug, Default)]
pub struct FixedSizeScanState;

impl SegmentScanState for FixedSizeScanState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
impl CompressedSegmentState for FixedSizeScanState {}

#[derive(Debug)]
struct UncompressedAnalyzeState {
    type_size: Idx,
    count: Idx,
}

struct UncompressedCompressState {
    checkpoint_state: Arc<Mutex<ColumnCheckpointState>>,
    current_segment: Arc<ColumnSegment>,
    append_state: ColumnAppendState,
    logical_type: crate::common::types::LogicalType,
    block_size: Idx,
}

impl CompressionState for UncompressedCompressState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl std::fmt::Debug for UncompressedCompressState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UncompressedCompressState")
            .field("logical_type", &self.logical_type)
            .field("block_size", &self.block_size)
            .field("current_segment_count", &self.current_segment.count())
            .finish()
    }
}

impl AnalyzeState for UncompressedAnalyzeState {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn init_analyze(info: &CompressionInfo) -> Box<dyn AnalyzeState> {
    let _ = info;
    Box::new(UncompressedAnalyzeState {
        type_size: 0,
        count: 0,
    })
}

fn analyze(state: &mut dyn AnalyzeState, input: &Vector, count: Idx) -> bool {
    let state = state
        .as_any_mut()
        .downcast_mut::<UncompressedAnalyzeState>()
        .expect("invalid analyze state for uncompressed");
    if state.type_size == 0 {
        state.type_size = input.get_type().physical_size() as Idx;
    }
    state.count += count;
    true
}

fn final_analyze(state: &mut dyn AnalyzeState) -> Idx {
    let state = state
        .as_any_mut()
        .downcast_mut::<UncompressedAnalyzeState>()
        .expect("invalid analyze state for uncompressed");
    state.type_size.saturating_mul(state.count)
}

fn init_compression(
    info: &CompressionInfo,
    checkpoint_state: Arc<Mutex<ColumnCheckpointState>>,
    _analyze_state: Box<dyn AnalyzeState>,
) -> Box<dyn CompressionState> {
    let logical_type = checkpoint_state.lock().get_original_column().base.logical_type.clone();
    let current_segment = Arc::new(ColumnSegment::create_transient(
        logical_type.clone(),
        info.get_block_size(),
        CompressionType::Uncompressed,
    ));
    let mut append_state = ColumnAppendState::default();
    current_segment.initialize_append(&mut append_state);
    Box::new(UncompressedCompressState {
        checkpoint_state,
        current_segment,
        append_state,
        logical_type,
        block_size: info.get_block_size(),
    })
}

fn compress(state: &mut dyn CompressionState, scan_vector: &Vector, count: Idx) {
    let state = state
        .as_any_mut()
        .downcast_mut::<UncompressedCompressState>()
        .expect("invalid uncompressed compression state");
    let vdata = UnifiedVectorFormat::from_flat_vector(scan_vector);
    let mut offset = 0;
    let mut remaining = count;
    while remaining > 0 {
        let appended = state
            .current_segment
            .append(&mut state.append_state, &vdata, offset, remaining);
        if appended == remaining {
            return;
        }
        flush_segment(state);
        state.current_segment = Arc::new(ColumnSegment::create_transient(
            state.logical_type.clone(),
            state.block_size,
            CompressionType::Uncompressed,
        ));
        state.current_segment.initialize_append(&mut state.append_state);
        offset += appended;
        remaining -= appended;
    }
}

fn compress_finalize(state: &mut dyn CompressionState) {
    let state = state
        .as_any_mut()
        .downcast_mut::<UncompressedCompressState>()
        .expect("invalid uncompressed compression state");
    flush_segment(state);
}

fn flush_segment(state: &mut UncompressedCompressState) {
    if state.current_segment.count() == 0 {
        return;
    }
    let segment_size = state.current_segment.finalize_append(&mut state.append_state);
    state
        .checkpoint_state
        .lock()
        .flush_segment(Arc::clone(&state.current_segment), segment_size);
}

fn fixed_size_init_scan(
    segment: &ColumnSegment,
    state: &mut ColumnScanState,
) -> Option<Box<dyn SegmentScanState>> {
    segment.initialize_scan_buffer(state);
    Some(Box::new(FixedSizeScanState))
}

fn fixed_size_scan_vector(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
) {
    segment.fixed_size_scan_vector(state, scan_count, result);
}

fn fixed_size_scan_partial(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
    result_offset: Idx,
) {
    segment.fixed_size_scan_partial(state, scan_count, result, result_offset);
}

fn fixed_size_select(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    _vector_count: Idx,
    result: &mut Vector,
    sel: &SelectionVector,
    sel_count: Idx,
) {
    segment.fixed_size_select(state, result, sel, sel_count);
}

fn fixed_size_fetch_row(segment: &ColumnSegment, row_id: Idx, result: &mut Vector, result_idx: Idx) {
    segment.fixed_size_fetch_row(row_id, result, result_idx);
}

fn validity_row_is_valid(buffer: &[u8], row_idx: usize) -> bool {
    let byte_idx = row_idx / 8;
    let bit_idx = row_idx % 8;
    buffer
        .get(byte_idx)
        .map(|byte| (byte >> bit_idx) & 1 == 1)
        .unwrap_or(true)
}

fn validity_set_invalid(buffer: &mut [u8], row_idx: usize) {
    let byte_idx = row_idx / 8;
    let bit_idx = row_idx % 8;
    if let Some(byte) = buffer.get_mut(byte_idx) {
        *byte &= !(1 << bit_idx);
    }
}

fn validity_set_valid(buffer: &mut [u8], row_idx: usize) {
    let byte_idx = row_idx / 8;
    let bit_idx = row_idx % 8;
    if let Some(byte) = buffer.get_mut(byte_idx) {
        *byte |= 1 << bit_idx;
    }
}

fn validity_init_scan(
    segment: &ColumnSegment,
    state: &mut ColumnScanState,
) -> Option<Box<dyn SegmentScanState>> {
    fixed_size_init_scan(segment, state)
}

fn validity_scan_partial(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
    result_offset: Idx,
) {
    let start = state.position_in_segment() as usize;
    let source = match segment.segment_type {
        crate::storage::table::column_segment::ColumnSegmentType::Transient => {
            let buf = segment.buffer.lock();
            buf.clone()
        }
        crate::storage::table::column_segment::ColumnSegmentType::Persistent => {
            let mut data = Vec::new();
            if let Some(handle) = state.pinned_buffer.as_ref() {
                let _ = handle.with_data(|block_data| {
                    let begin = segment.block_offset() as usize;
                    let end = begin + segment.segment_size() as usize;
                    data.extend_from_slice(&block_data[begin..end]);
                });
            }
            data
        }
    };
    for i in 0..scan_count as usize {
        if !validity_row_is_valid(&source, start + i) {
            result.validity.set_invalid(result_offset as usize + i);
        }
    }
}

fn validity_scan(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
) {
    validity_scan_partial(segment, state, scan_count, result, 0);
}

fn validity_select(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    _vector_count: Idx,
    result: &mut Vector,
    sel: &SelectionVector,
    sel_count: Idx,
) {
    let start = state.position_in_segment() as usize;
    let source = match segment.segment_type {
        crate::storage::table::column_segment::ColumnSegmentType::Transient => {
            let buf = segment.buffer.lock();
            buf.clone()
        }
        crate::storage::table::column_segment::ColumnSegmentType::Persistent => {
            let mut data = Vec::new();
            if let Some(handle) = state.pinned_buffer.as_ref() {
                let _ = handle.with_data(|block_data| {
                    let begin = segment.block_offset() as usize;
                    let end = begin + segment.segment_size() as usize;
                    data.extend_from_slice(&block_data[begin..end]);
                });
            }
            data
        }
    };
    for i in 0..sel_count as usize {
        let source_idx = start + sel.get_index(i);
        if !validity_row_is_valid(&source, source_idx) {
            result.validity.set_invalid(i);
        }
    }
}

fn validity_fetch_row(segment: &ColumnSegment, row_id: Idx, result: &mut Vector, result_idx: Idx) {
    let row_idx = row_id as usize;
    let is_valid = match segment.segment_type {
        crate::storage::table::column_segment::ColumnSegmentType::Transient => {
            let buf = segment.buffer.lock();
            validity_row_is_valid(&buf, row_idx)
        }
        crate::storage::table::column_segment::ColumnSegmentType::Persistent => {
            let handle = segment
                .block_handle
                .as_ref()
                .expect("persistent validity segment missing block handle");
            let pinned = handle.block_manager.buffer_manager().pin(handle.clone());
            pinned
                .with_data(|block_data| {
                    let begin = segment.block_offset() as usize;
                    let end = begin + segment.segment_size() as usize;
                    validity_row_is_valid(&block_data[begin..end], row_idx)
                })
                .unwrap_or(true)
        }
    };
    if !is_valid {
        result.validity.set_invalid(result_idx as usize);
    }
}

fn empty_skip(_segment: &ColumnSegment, _state: &mut ColumnScanState, _skip_count: Idx) {}

fn fixed_size_init_append(segment: &ColumnSegment, state: &mut ColumnAppendState) {
    segment.fixed_size_initialize_append(state);
}

fn fixed_size_append(
    segment: &ColumnSegment,
    state: &mut ColumnAppendState,
    vdata: &UnifiedVectorFormat<'_>,
    offset: Idx,
    count: Idx,
) -> Idx {
    segment.fixed_size_append(vdata, offset, count, state)
}

fn fixed_size_finalize_append(segment: &ColumnSegment, state: &mut ColumnAppendState) -> Idx {
    segment.fixed_size_finalize_append(state)
}

fn fixed_size_revert_append(segment: &ColumnSegment, new_count: Idx) {
    segment.fixed_size_revert_append(new_count);
}

fn validity_append(
    segment: &ColumnSegment,
    _state: &mut ColumnAppendState,
    vdata: &UnifiedVectorFormat<'_>,
    offset: Idx,
    count: Idx,
) -> Idx {
    let current_count = segment.count() as usize;
    let max_tuples = segment.segment_size() as usize * 8;
    let append_count = (count as usize).min(max_tuples.saturating_sub(current_count));
    if append_count == 0 {
        return 0;
    }

    if vdata.validity.is_all_valid() {
        segment.set_count((current_count + append_count) as Idx);
        segment.stats.lock().set_has_no_null();
        return append_count as Idx;
    }

    let mut stats = segment.stats.lock();
    let mut buf = segment.buffer.lock();
    for i in 0..append_count {
        let source_idx = vdata
            .sel
            .map(|sel| sel.get_index(offset as usize + i))
            .unwrap_or(offset as usize + i);
        let target_idx = current_count + i;
        if !vdata.validity.row_is_valid(source_idx) {
            validity_set_invalid(&mut buf, target_idx);
            stats.set_has_null();
        } else {
            stats.set_has_no_null();
        }
    }
    segment.set_count((current_count + append_count) as Idx);
    append_count as Idx
}

fn validity_finalize_append(segment: &ColumnSegment, _state: &mut ColumnAppendState) -> Idx {
    let count = segment.count() as usize;
    (((count + 2048 - 1) / 2048) * 256) as Idx
}

fn validity_revert_append(segment: &ColumnSegment, new_count: Idx) {
    let old_count = segment.count();
    if new_count >= old_count {
        return;
    }
    let mut buf = segment.buffer.lock();
    for row_idx in new_count as usize..old_count as usize {
        validity_set_valid(&mut buf, row_idx);
    }
    segment.set_count(new_count);
}

pub struct FixedSizeUncompressed;

impl FixedSizeUncompressed {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        CompressionFunction::new(
            CompressionType::Uncompressed,
            data_type,
            Some(init_analyze),
            Some(analyze),
            Some(final_analyze),
            Some(init_compression),
            Some(compress),
            Some(compress_finalize),
            Some(fixed_size_init_scan),
            Some(fixed_size_scan_vector),
            Some(fixed_size_scan_partial),
            Some(fixed_size_fetch_row),
            Some(empty_skip),
            Some(fixed_size_init_append),
            Some(fixed_size_append),
            Some(fixed_size_finalize_append),
            Some(fixed_size_revert_append),
            Some(fixed_size_select),
        )
    }
}

pub struct StringUncompressed;

impl StringUncompressed {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        FixedSizeUncompressed::get_function(data_type)
    }
}

pub struct ValidityUncompressed;

impl ValidityUncompressed {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        CompressionFunction::new(
            CompressionType::Uncompressed,
            data_type,
            Some(init_analyze),
            Some(analyze),
            Some(final_analyze),
            Some(init_compression),
            Some(compress),
            Some(compress_finalize),
            Some(validity_init_scan),
            Some(validity_scan),
            Some(validity_scan_partial),
            Some(validity_fetch_row),
            Some(empty_skip),
            Some(fixed_size_init_append),
            Some(validity_append),
            Some(validity_finalize_append),
            Some(validity_revert_append),
            Some(validity_select),
        )
    }
}

pub struct UncompressedFun;

impl UncompressedFun {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        match data_type {
            PhysicalType::Bit => ValidityUncompressed::get_function(data_type),
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
            | PhysicalType::List
            | PhysicalType::Array => FixedSizeUncompressed::get_function(data_type),
            PhysicalType::VarChar => StringUncompressed::get_function(data_type),
            PhysicalType::Struct | PhysicalType::Invalid => FixedSizeUncompressed::get_function(data_type),
        }
    }

    pub fn type_is_supported(data_type: PhysicalType) -> bool {
        matches!(
            data_type,
            PhysicalType::Bool | PhysicalType::Bit
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
                | PhysicalType::VarChar
                | PhysicalType::List
                | PhysicalType::Array
        )
    }
}
