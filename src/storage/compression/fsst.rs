use parking_lot::Mutex;

use crate::common::bitpacking::{BitpackingPrimitives, BitpackingWidth};
use crate::common::fsst::{decompress_value, import_symbol_table, DuckdbFsstDecoder};
use crate::common::types::{SelectionVector, Vector, VectorType};
use crate::function::compression_function::CompressionFunction;
use crate::storage::table::column_segment::ColumnSegment;
use crate::storage::table::scan_state::{ColumnScanState, SegmentScanState};
use crate::storage::table::types::{CompressionType, Idx, PhysicalType};

#[derive(Debug, Clone, Copy)]
struct FsstCompressionHeader {
    dict_size: u32,
    dict_end: u32,
    bitpacking_width: u32,
    fsst_symbol_table_offset: u32,
}

#[derive(Debug, Clone, Copy)]
struct StringDictionaryContainer {
    end: u32,
}

#[derive(Debug, Clone, Copy)]
struct BpDeltaDecodeOffsets {
    bitunpack_alignment_offset: Idx,
    bitunpack_start_row: Idx,
    unused_delta_decoded_values: Idx,
    scan_offset: Idx,
    total_delta_decode_count: Idx,
    total_bitunpack_count: Idx,
}

#[derive(Debug)]
struct FsstScanFields {
    decoder: Option<DuckdbFsstDecoder>,
    current_width: BitpackingWidth,
    last_known_index: u32,
    last_known_row: i64,
    bitunpack_buffer: Vec<u32>,
    delta_decode_buffer: Vec<u32>,
}

#[derive(Debug)]
struct FsstScanState {
    inner: Mutex<FsstScanFields>,
}

impl SegmentScanState for FsstScanState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn read_scan_state(state: &ColumnScanState) -> &FsstScanState {
    state
        .scan_state
        .as_ref()
        .and_then(|scan| scan.as_any().downcast_ref::<FsstScanState>())
        .expect("invalid fsst scan state")
}

fn with_segment_bytes<R, F: FnOnce(&[u8]) -> R>(state: &ColumnScanState, f: F) -> R {
    let handle = state
        .pinned_buffer
        .as_ref()
        .expect("fsst scan requires pinned buffer");
    handle
        .with_data(f)
        .expect("failed to access pinned block for fsst")
}

fn read_header(base_ptr: &[u8]) -> FsstCompressionHeader {
    FsstCompressionHeader {
        dict_size: u32::from_le_bytes(base_ptr[0..4].try_into().unwrap()),
        dict_end: u32::from_le_bytes(base_ptr[4..8].try_into().unwrap()),
        bitpacking_width: u32::from_le_bytes(base_ptr[8..12].try_into().unwrap()),
        fsst_symbol_table_offset: u32::from_le_bytes(base_ptr[12..16].try_into().unwrap()),
    }
}

fn parse_fsst_segment_header(
    base_ptr: &[u8],
    block_size: usize,
) -> (Option<DuckdbFsstDecoder>, BitpackingWidth) {
    let header = read_header(base_ptr);
    if header.fsst_symbol_table_offset as usize > block_size {
        panic!("invalid fsst_symbol_table_offset in fsst segment header");
    }

    let mut decoder = DuckdbFsstDecoder::default();
    let mut symbol_bytes = base_ptr[header.fsst_symbol_table_offset as usize..].to_vec();
    let imported = import_symbol_table(&mut decoder, &mut symbol_bytes);
    let decoder = if imported == 0 { None } else { Some(decoder) };
    (decoder, header.bitpacking_width as BitpackingWidth)
}

fn delta_decode_indices(buffer_in: &[u32], buffer_out: &mut [u32], decode_count: usize, last_known_value: u32) {
    if decode_count == 0 {
        return;
    }
    buffer_out[0] = buffer_in[0].wrapping_add(last_known_value);
    for i in 1..decode_count {
        buffer_out[i] = buffer_in[i].wrapping_add(buffer_out[i - 1]);
    }
}

fn calculate_bp_delta_offsets(last_known_row: i64, start: Idx, scan_count: Idx) -> BpDeltaDecodeOffsets {
    let delta_decode_start_row = (last_known_row + 1) as Idx;
    let bitunpack_alignment_offset =
        delta_decode_start_row % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE as Idx;
    let bitunpack_start_row = delta_decode_start_row - bitunpack_alignment_offset;
    let unused_delta_decoded_values = start - delta_decode_start_row;
    let scan_offset = bitunpack_alignment_offset + unused_delta_decoded_values;
    let total_delta_decode_count = scan_count + unused_delta_decoded_values;
    let total_bitunpack_count =
        BitpackingPrimitives::round_up_to_algorithm_group_size((scan_count + scan_offset) as usize) as Idx;

    BpDeltaDecodeOffsets {
        bitunpack_alignment_offset,
        bitunpack_start_row,
        unused_delta_decoded_values,
        scan_offset,
        total_delta_decode_count,
        total_bitunpack_count,
    }
}

fn start_scan(
    scan_state: &FsstScanState,
    base_data: &[u8],
    start: Idx,
    scan_count: Idx,
) -> BpDeltaDecodeOffsets {
    let mut inner = scan_state.inner.lock();
    if start == 0 || inner.last_known_row >= start as i64 {
        inner.last_known_index = 0;
        inner.last_known_row = -1;
    }

    let offsets = calculate_bp_delta_offsets(inner.last_known_row, start, scan_count);

    if inner.bitunpack_buffer.len() < offsets.total_bitunpack_count as usize {
        inner.bitunpack_buffer.resize(offsets.total_bitunpack_count as usize, 0);
    }
    let current_width = inner.current_width;
    let src = &base_data[((offsets.bitunpack_start_row * inner.current_width as Idx) / 8) as usize..];
    BitpackingPrimitives::unpack_buffer(
        &mut inner.bitunpack_buffer[..offsets.total_bitunpack_count as usize],
        src,
        offsets.total_bitunpack_count as usize,
        current_width,
        true,
    );

    if inner.delta_decode_buffer.len() < offsets.total_delta_decode_count as usize {
        inner.delta_decode_buffer
            .resize(offsets.total_delta_decode_count as usize, 0);
    }
    let alignment = offsets.bitunpack_alignment_offset as usize;
    let last_known_index = inner.last_known_index;
    let bitunpack_tmp = inner.bitunpack_buffer[alignment..alignment + offsets.total_delta_decode_count as usize].to_vec();
    delta_decode_indices(
        &bitunpack_tmp,
        &mut inner.delta_decode_buffer[..offsets.total_delta_decode_count as usize],
        offsets.total_delta_decode_count as usize,
        last_known_index,
    );
    offsets
}

fn end_scan(scan_state: &FsstScanState, offsets: &BpDeltaDecodeOffsets, start: Idx, scan_count: Idx) {
    let mut inner = scan_state.inner.lock();
    inner.last_known_index =
        inner.delta_decode_buffer[(scan_count + offsets.unused_delta_decoded_values - 1) as usize];
    inner.last_known_row = (start + scan_count - 1) as i64;
}

fn decode_string(
    scan_state: &FsstScanState,
    dict: StringDictionaryContainer,
    base_ptr: &[u8],
    string_len: usize,
    dict_offset: i32,
) -> Vec<u8> {
    if string_len == 0 || dict_offset == 0 {
        return Vec::new();
    }
    let dict_pos = dict.end as usize - dict_offset as usize;
    let compressed = &base_ptr[dict_pos..dict_pos + string_len];

    let mut inner = scan_state.inner.lock();
    match inner.decoder.as_mut() {
        None => Vec::new(),
        Some(decoder) => {
            let mut output = vec![0u8; string_len * 8];
            let size = decompress_value(decoder, compressed, &mut output);
            output.truncate(size);
            output
        }
    }
}

fn fsst_init_scan(segment: &ColumnSegment, state: &mut ColumnScanState) -> Option<Box<dyn SegmentScanState>> {
    segment.initialize_scan_buffer(state);
    let (decoder, width) = with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        parse_fsst_segment_header(base_ptr, segment.segment_size as usize)
    });
    Some(Box::new(FsstScanState {
        inner: Mutex::new(FsstScanFields {
            decoder,
            current_width: width,
            last_known_index: 0,
            last_known_row: -1,
            bitunpack_buffer: Vec::new(),
            delta_decode_buffer: Vec::new(),
        }),
    }))
}

fn fsst_scan_partial(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
    result_offset: Idx,
) {
    let start = state.position_in_segment();
    let scan_state = read_scan_state(state);
    result.vector_type = VectorType::Flat;
    if result_offset == 0 {
        result.reset_varchar_storage();
    }

    with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        let header = read_header(base_ptr);
        let _ = header.dict_size;
        let dict = StringDictionaryContainer { end: header.dict_end };
        let base_data = &base_ptr[std::mem::size_of::<FsstCompressionHeader>()..];
        let offsets = start_scan(scan_state, base_data, start, scan_count);

        let (lengths, positions) = {
            let inner = scan_state.inner.lock();
            let lengths = inner.bitunpack_buffer
                [offsets.scan_offset as usize..offsets.scan_offset as usize + scan_count as usize]
                .to_vec();
            let positions = inner.delta_decode_buffer[offsets.unused_delta_decoded_values as usize
                ..offsets.unused_delta_decoded_values as usize + scan_count as usize]
                .to_vec();
            (lengths, positions)
        };

        for i in 0..scan_count as usize {
            let value = decode_string(scan_state, dict, base_ptr, lengths[i] as usize, positions[i] as i32);
            result.write_varchar_bytes(result_offset as usize + i, &value);
        }
        end_scan(scan_state, &offsets, start, scan_count);
    });
}

fn fsst_scan_vector(segment: &ColumnSegment, state: &ColumnScanState, scan_count: Idx, result: &mut Vector) {
    fsst_scan_partial(segment, state, scan_count, result, 0);
}

fn fsst_select(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    vector_count: Idx,
    result: &mut Vector,
    sel: &SelectionVector,
    sel_count: Idx,
) {
    let start = state.position_in_segment();
    let scan_state = read_scan_state(state);
    result.vector_type = VectorType::Flat;
    result.reset_varchar_storage();

    with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        let header = read_header(base_ptr);
        let _ = header.dict_size;
        let dict = StringDictionaryContainer { end: header.dict_end };
        let base_data = &base_ptr[std::mem::size_of::<FsstCompressionHeader>()..];
        let offsets = start_scan(scan_state, base_data, start, vector_count);

        let (lengths, positions) = {
            let inner = scan_state.inner.lock();
            let lengths = inner.bitunpack_buffer.clone();
            let positions = inner.delta_decode_buffer.clone();
            (lengths, positions)
        };

        for i in 0..sel_count as usize {
            let index = sel.get_index(i);
            let value = decode_string(
                scan_state,
                dict,
                base_ptr,
                lengths[offsets.scan_offset as usize + index] as usize,
                positions[offsets.unused_delta_decoded_values as usize + index] as i32,
            );
            result.write_varchar_bytes(i, &value);
        }
        end_scan(scan_state, &offsets, start, vector_count);
    });
}

fn fsst_fetch_row(segment: &ColumnSegment, row_id: Idx, result: &mut Vector, result_idx: Idx) {
    let mut state = ColumnScanState::new();
    state.scan_state = fsst_init_scan(segment, &mut state);
    let scan_state = read_scan_state(&state);

    with_segment_bytes(&state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        let header = read_header(base_ptr);
        let _ = header.dict_size;
        let dict = StringDictionaryContainer { end: header.dict_end };
        let base_data = &base_ptr[std::mem::size_of::<FsstCompressionHeader>()..];
        let offsets = start_scan(scan_state, base_data, row_id, 1);

        let (string_len, dict_offset) = {
            let inner = scan_state.inner.lock();
            (
                inner.bitunpack_buffer[offsets.scan_offset as usize] as usize,
                inner.delta_decode_buffer[offsets.unused_delta_decoded_values as usize] as i32,
            )
        };

        let value = decode_string(scan_state, dict, base_ptr, string_len, dict_offset);
        result.write_varchar_bytes(result_idx as usize, &value);
    });
}

fn fsst_skip(segment: &ColumnSegment, state: &mut ColumnScanState, skip_count: Idx) {
    let start = state.position_in_segment();
    let scan_state = read_scan_state(state);
    with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        let base_data = &base_ptr[std::mem::size_of::<FsstCompressionHeader>()..];
        let offsets = start_scan(scan_state, base_data, start, skip_count);
        end_scan(scan_state, &offsets, start, skip_count);
    });
}

fn get_fsst_function() -> CompressionFunction {
    CompressionFunction::new(
        CompressionType::Fsst,
        PhysicalType::VarChar,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(fsst_init_scan),
        Some(fsst_scan_vector),
        Some(fsst_scan_partial),
        Some(fsst_fetch_row),
        Some(fsst_skip),
        None,
        None,
        None,
        None,
        Some(fsst_select),
    )
}

pub struct FsstFun;

impl FsstFun {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        match data_type {
            PhysicalType::VarChar => get_fsst_function(),
            _ => panic!("fsst not supported for physical type {:?}", data_type),
        }
    }

    pub fn type_is_supported(data_type: PhysicalType) -> bool {
        matches!(data_type, PhysicalType::VarChar)
    }
}
