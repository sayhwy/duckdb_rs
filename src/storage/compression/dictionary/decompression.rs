use parking_lot::Mutex;

use crate::common::bitpacking::{BitpackingPrimitives, BitpackingWidth};
use crate::common::types::{LogicalType, LogicalTypeId, SelectionVector, Vector, VectorType, STANDARD_VECTOR_SIZE};
use crate::storage::table::column_segment::ColumnSegment;
use crate::storage::table::scan_state::{ColumnScanState, SegmentScanState};
use crate::storage::table::types::Idx;

const DICTIONARY_HEADER_SIZE: usize = std::mem::size_of::<DictionaryCompressionHeader>();

#[derive(Debug, Clone, Copy)]
struct DictionaryCompressionHeader {
    dict_size: u32,
    dict_end: u32,
    index_buffer_offset: u32,
    index_buffer_count: u32,
    bitpacking_width: u32,
}

#[derive(Debug)]
struct CompressedStringScanFields {
    current_width: BitpackingWidth,
    selection_buffer: Vec<u32>,
    selection_buffer_size: usize,
}

#[derive(Debug)]
pub struct CompressedStringScanState {
    inner: Mutex<CompressedStringScanFields>,
    dictionary: Vector,
}

impl SegmentScanState for CompressedStringScanState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn with_segment_bytes<R, F: FnOnce(&[u8]) -> R>(state: &ColumnScanState, f: F) -> R {
    let handle = state
        .pinned_buffer
        .as_ref()
        .expect("dictionary scan requires pinned buffer");
    handle
        .with_data(f)
        .expect("failed to access pinned block for dictionary")
}

fn read_scan_state(state: &ColumnScanState) -> &CompressedStringScanState {
    state
        .scan_state
        .as_ref()
        .and_then(|scan| scan.as_any().downcast_ref::<CompressedStringScanState>())
        .expect("invalid dictionary scan state")
}

fn read_header(base_ptr: &[u8]) -> DictionaryCompressionHeader {
    DictionaryCompressionHeader {
        dict_size: u32::from_le_bytes(base_ptr[0..4].try_into().unwrap()),
        dict_end: u32::from_le_bytes(base_ptr[4..8].try_into().unwrap()),
        index_buffer_offset: u32::from_le_bytes(base_ptr[8..12].try_into().unwrap()),
        index_buffer_count: u32::from_le_bytes(base_ptr[12..16].try_into().unwrap()),
        bitpacking_width: u32::from_le_bytes(base_ptr[16..20].try_into().unwrap()),
    }
}

fn get_string_length(index_buffer: &[u32], index: usize) -> u16 {
    if index == 0 {
        0
    } else {
        (index_buffer[index] - index_buffer[index - 1]) as u16
    }
}

fn initialize_dictionary_vector(
    base_ptr: &[u8],
    header: DictionaryCompressionHeader,
    index_buffer: &[u32],
) -> Vector {
    let mut dictionary = Vector::with_capacity(LogicalType::varchar(), index_buffer.len());
    dictionary.validity.reset(index_buffer.len());
    if !index_buffer.is_empty() {
        dictionary.validity.set_invalid(0);
    }

    for i in 1..index_buffer.len() {
        let dict_offset = index_buffer[i] as usize;
        let string_len = get_string_length(index_buffer, i) as usize;
        let dict_pos = header.dict_end as usize - dict_offset;
        dictionary.write_varchar_bytes(i, &base_ptr[dict_pos..dict_pos + string_len]);
    }
    dictionary
}

fn prepare_selection_buffer(
    scan_state: &CompressedStringScanState,
    base_data: &[u8],
    start: Idx,
    scan_count: Idx,
) -> (usize, usize) {
    let start_offset = start as usize % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
    let decompress_count =
        BitpackingPrimitives::round_up_to_algorithm_group_size(scan_count as usize + start_offset);

    let mut inner = scan_state.inner.lock();
    if inner.selection_buffer_size < decompress_count {
        inner.selection_buffer.resize(decompress_count, 0);
        inner.selection_buffer_size = decompress_count;
    }

    let current_width = inner.current_width;
    let src = &base_data[((start as usize - start_offset) * current_width as usize) / 8..];
    BitpackingPrimitives::unpack_buffer(
        &mut inner.selection_buffer[..decompress_count],
        src,
        decompress_count,
        current_width,
        true,
    );
    (start_offset, decompress_count)
}

fn scan_to_flat_vector(
    segment: &ColumnSegment,
    scan_state: &CompressedStringScanState,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
    result_offset: Idx,
) {
    let start = state.position_in_segment();
    result.vector_type = VectorType::Flat;
    if result.logical_type.id == LogicalTypeId::Varchar && result_offset == 0 {
        result.reset_varchar_storage();
    }

    with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        let header = read_header(base_ptr);
        let base_data = &base_ptr[DICTIONARY_HEADER_SIZE..];
        let (start_offset, _) = prepare_selection_buffer(scan_state, base_data, start, scan_count);
        let inner = scan_state.inner.lock();

        for i in 0..scan_count as usize {
            let string_number = inner.selection_buffer[i + start_offset] as usize;
            if string_number == 0 {
                result.validity.set_invalid(result_offset as usize + i);
                continue;
            }
            let value = scan_state.dictionary.read_varchar_bytes(string_number);
            let _ = header.dict_size;
            result.write_varchar_bytes(result_offset as usize + i, &value);
        }
    });
}

fn scan_to_dictionary_vector(
    segment: &ColumnSegment,
    scan_state: &CompressedStringScanState,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
) {
    let start = state.position_in_segment();
    let selection = with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        let base_data = &base_ptr[DICTIONARY_HEADER_SIZE..];
        let (start_offset, _) = prepare_selection_buffer(scan_state, base_data, start, scan_count);
        let inner = scan_state.inner.lock();
        let mut indices = Vec::with_capacity(scan_count as usize);
        for i in 0..scan_count as usize {
            indices.push(inner.selection_buffer[i + start_offset]);
        }
        SelectionVector { indices }
    });
    result.set_dictionary(&scan_state.dictionary, selection);
}

pub fn string_init_scan(
    segment: &ColumnSegment,
    state: &mut ColumnScanState,
) -> Option<Box<dyn SegmentScanState>> {
    segment.initialize_scan_buffer(state);
    let (header, dictionary) = with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[segment.block_offset as usize..];
        let header = read_header(base_ptr);
        let index_buffer_offset = header.index_buffer_offset as usize;
        let index_buffer_count = header.index_buffer_count as usize;
        let index_end = index_buffer_offset + index_buffer_count * std::mem::size_of::<u32>();
        if index_end > segment.segment_size as usize {
            panic!("dictionary index buffer out of range");
        }

        let index_buffer = base_ptr[index_buffer_offset..index_end]
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        let dictionary = initialize_dictionary_vector(base_ptr, header, &index_buffer);
        (header, dictionary)
    });

    Some(Box::new(CompressedStringScanState {
        inner: Mutex::new(CompressedStringScanFields {
            current_width: header.bitpacking_width as BitpackingWidth,
            selection_buffer: Vec::new(),
            selection_buffer_size: 0,
        }),
        dictionary,
    }))
}

pub fn string_scan_partial(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
    result_offset: Idx,
) {
    let scan_state = read_scan_state(state);
    scan_to_flat_vector(segment, scan_state, state, scan_count, result, result_offset);
}

pub fn string_scan(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
) {
    let scan_state = read_scan_state(state);
    if scan_count == STANDARD_VECTOR_SIZE as Idx {
        scan_to_dictionary_vector(segment, scan_state, state, scan_count, result);
    } else {
        scan_to_flat_vector(segment, scan_state, state, scan_count, result, 0);
    }
}

pub fn string_select(
    _segment: &ColumnSegment,
    state: &ColumnScanState,
    vector_count: Idx,
    result: &mut Vector,
    sel: &SelectionVector,
    sel_count: Idx,
) {
    let scan_state = read_scan_state(state);
    let start = state.position_in_segment();
    result.vector_type = VectorType::Flat;
    result.reset_varchar_storage();

    with_segment_bytes(state, |block_data| {
        let base_ptr = &block_data[_segment.block_offset as usize..];
        let base_data = &base_ptr[DICTIONARY_HEADER_SIZE..];
        let (start_offset, _) = prepare_selection_buffer(scan_state, base_data, start, vector_count);
        let inner = scan_state.inner.lock();
        for i in 0..sel_count as usize {
            let index = sel.get_index(i) as usize;
            let string_number = inner.selection_buffer[start_offset + index] as usize;
            if string_number == 0 {
                result.validity.set_invalid(i);
                continue;
            }
            let value = scan_state.dictionary.read_varchar_bytes(string_number);
            result.write_varchar_bytes(i, &value);
        }
    });
}

pub fn string_fetch_row(segment: &ColumnSegment, row_id: Idx, result: &mut Vector, result_idx: Idx) {
    let mut state = ColumnScanState::new();
    state.scan_state = string_init_scan(segment, &mut state);
    state.offset_in_column = row_id;
    state.segment_row_start = 0;
    string_scan_partial(segment, &state, 1, result, result_idx);
}
