use parking_lot::Mutex;
use std::sync::Arc;

use crate::common::bitpacking::{BitpackingPrimitives, BitpackingValue, BitpackingWidth};
use crate::common::types::{Vector, VectorType};
use crate::function::compression_function::{
    AnalyzeState, CompressionFunction, CompressionInfo, CompressionState,
};
use crate::storage::statistics::NumericStats;
use crate::storage::table::column_checkpoint_state::ColumnCheckpointState;
use crate::storage::table::column_segment::{ColumnSegment, SegmentStatistics};
use crate::storage::table::scan_state::{ColumnScanState, SegmentScanState};
use crate::storage::table::types::{CompressionType, Idx, PhysicalType};

const BITPACKING_METADATA_GROUP_SIZE: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitpackingMode {
    Invalid = 0,
    Auto = 1,
    Constant = 2,
    ConstantDelta = 3,
    DeltaFor = 4,
    For = 5,
}

#[derive(Debug, Clone, Copy)]
struct BitpackingMetadata {
    mode: BitpackingMode,
    offset: u32,
}

#[derive(Debug)]
struct BitpackingFields<T: BitpackingValue + std::fmt::Debug> {
    decompression_buffer: [T; BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE],
    current_group: BitpackingMetadata,
    current_width: BitpackingWidth,
    current_frame_of_reference: T,
    current_constant: T,
    current_delta_offset: T,
    current_group_offset: usize,
    current_group_ptr_offset: usize,
    bitpacking_metadata_offset: usize,
}

#[derive(Debug)]
struct BitpackingScanState<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> {
    inner: Mutex<BitpackingFields<T>>,
}

#[derive(Debug)]
struct BitpackingAnalyzeState<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> {
    info: CompressionInfo,
    state: BitpackingState<T>,
}

impl<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> AnalyzeState
    for BitpackingAnalyzeState<T>
{
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct BitpackingCompressionState<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> {
    checkpoint_state: Arc<Mutex<ColumnCheckpointState>>,
    logical_type: crate::common::types::LogicalType,
    info: CompressionInfo,
    state: BitpackingState<T>,
    data: Vec<u8>,
    data_ptr: usize,
    metadata_ptr: usize,
    current_segment_count: Idx,
    current_segment_stats: SegmentStatistics,
}

impl<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> CompressionState
    for BitpackingCompressionState<T>
{
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> std::fmt::Debug
    for BitpackingCompressionState<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitpackingCompressionState")
            .field("logical_type", &self.logical_type)
            .field("data_ptr", &self.data_ptr)
            .field("metadata_ptr", &self.metadata_ptr)
            .field("current_segment_count", &self.current_segment_count)
            .finish()
    }
}

#[derive(Debug)]
struct BitpackingState<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> {
    compression_buffer: [T; BITPACKING_METADATA_GROUP_SIZE],
    compression_buffer_validity: [bool; BITPACKING_METADATA_GROUP_SIZE],
    compression_buffer_idx: usize,
    total_size: Idx,
    minimum: T,
    maximum: T,
    min_max_diff: T,
    minimum_delta: i128,
    maximum_delta: i128,
    min_max_delta_diff: i128,
    delta_offset: i128,
    all_valid: bool,
    all_invalid: bool,
    has_valid: bool,
    has_invalid: bool,
    can_do_delta: bool,
    can_do_for: bool,
    mode: BitpackingMode,
}

impl<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> BitpackingState<T> {
    fn new() -> Self {
        let mut state = Self {
            compression_buffer: [T::default(); BITPACKING_METADATA_GROUP_SIZE],
            compression_buffer_validity: [false; BITPACKING_METADATA_GROUP_SIZE],
            compression_buffer_idx: 0,
            total_size: 0,
            minimum: T::default(),
            maximum: T::default(),
            min_max_diff: T::default(),
            minimum_delta: 0,
            maximum_delta: 0,
            min_max_delta_diff: 0,
            delta_offset: 0,
            all_valid: true,
            all_invalid: true,
            has_valid: false,
            has_invalid: false,
            can_do_delta: false,
            can_do_for: false,
            mode: BitpackingMode::Auto,
        };
        state.reset();
        state
    }

    fn reset(&mut self) {
        self.minimum = T::from_i128(T::MAX_I128);
        self.maximum = T::from_i128(T::MIN_I128);
        self.minimum_delta = i128::MAX;
        self.maximum_delta = i128::MIN;
        self.delta_offset = 0;
        self.all_valid = true;
        self.all_invalid = true;
        self.has_valid = false;
        self.has_invalid = false;
        self.can_do_delta = false;
        self.can_do_for = false;
        self.compression_buffer_idx = 0;
        self.min_max_diff = T::default();
        self.min_max_delta_diff = 0;
    }

    fn update(&mut self, value: T, is_valid: bool) -> bool {
        let had_valid = self.has_valid;
        self.compression_buffer_validity[self.compression_buffer_idx] = is_valid;
        self.has_valid |= is_valid;
        self.has_invalid |= !is_valid;
        self.all_valid &= is_valid;
        self.all_invalid &= !is_valid;

        if is_valid {
            self.compression_buffer[self.compression_buffer_idx] = value;
            if !had_valid {
                self.minimum = value;
                self.maximum = value;
            } else {
                if value.to_i128() < self.minimum.to_i128() {
                    self.minimum = value;
                }
                if value.to_i128() > self.maximum.to_i128() {
                    self.maximum = value;
                }
            }
        }

        self.compression_buffer_idx += 1;
        if self.compression_buffer_idx == BITPACKING_METADATA_GROUP_SIZE {
            let success = self.flush();
            self.reset();
            return success;
        }
        true
    }

    fn flush(&mut self) -> bool {
        if self.compression_buffer_idx == 0 {
            return true;
        }

        if (self.all_invalid || self.maximum.to_i128() == self.minimum.to_i128())
            && matches!(self.mode, BitpackingMode::Auto | BitpackingMode::Constant)
        {
            self.total_size += std::mem::size_of::<T>() as Idx + std::mem::size_of::<u32>() as Idx;
            return true;
        }

        self.calculate_for_stats();
        self.calculate_delta_stats();

        if self.can_do_delta {
            if self.maximum_delta == self.minimum_delta
                && !matches!(self.mode, BitpackingMode::For | BitpackingMode::DeltaFor)
            {
                self.total_size +=
                    (2 * std::mem::size_of::<T>() + std::mem::size_of::<u32>()) as Idx;
                return true;
            }

            let delta_required_bitwidth = minimum_bit_width_i128::<T>(self.min_max_delta_diff);
            let regular_required_bitwidth = minimum_bit_width_value::<T>(self.min_max_diff);
            let prefer_for = self.can_do_for && delta_required_bitwidth >= regular_required_bitwidth;

            if !prefer_for && self.mode != BitpackingMode::For {
                self.total_size += std::mem::size_of::<T>() as Idx;
                self.total_size += align_value(std::mem::size_of::<BitpackingWidth>() as Idx);
                self.total_size += std::mem::size_of::<T>() as Idx;
                self.total_size +=
                    BitpackingPrimitives::get_required_size(self.compression_buffer_idx as Idx, delta_required_bitwidth)
                        as Idx;
                return true;
            }
        }

        if self.can_do_for {
            let width = minimum_bit_width_value::<T>(self.min_max_diff);
            self.total_size +=
                BitpackingPrimitives::get_required_size(self.compression_buffer_idx as Idx, width) as Idx;
            self.total_size += std::mem::size_of::<T>() as Idx;
            self.total_size += align_value(std::mem::size_of::<BitpackingWidth>() as Idx);
            return true;
        }

        false
    }

    fn calculate_for_stats(&mut self) {
        self.can_do_for = false;
        if let Some(diff) = self.maximum.checked_sub(self.minimum) {
            self.min_max_diff = diff;
            self.can_do_for = true;
        }
    }

    fn calculate_delta_stats(&mut self) {
        self.can_do_delta = false;

        if !T::SIGNED && self.maximum.to_i128() > signed_max_for_type::<T>() {
            return;
        }
        if self.compression_buffer_idx < 2 || !self.all_valid {
            return;
        }

        if T::SIGNED
            && (self.minimum.checked_sub(self.maximum).is_none()
                || self.maximum.checked_sub(self.minimum).is_none())
        {
            return;
        }

        let mut deltas = [0i128; BITPACKING_METADATA_GROUP_SIZE];
        for idx in 0..self.compression_buffer_idx {
            let current = self.compression_buffer[idx].to_i128();
            let previous = if idx == 0 {
                0
            } else {
                self.compression_buffer[idx - 1].to_i128()
            };
            let delta = current - previous;
            if delta < signed_min_for_type::<T>() || delta > signed_max_for_type::<T>() {
                return;
            }
            deltas[idx] = delta;
        }

        self.can_do_delta = true;
        for delta in deltas.iter().take(self.compression_buffer_idx).skip(1) {
            self.maximum_delta = self.maximum_delta.max(*delta);
            self.minimum_delta = self.minimum_delta.min(*delta);
        }

        deltas[0] = self.minimum_delta;
        let diff = self.maximum_delta - self.minimum_delta;
        if diff < signed_min_for_type::<T>() || diff > signed_max_for_type::<T>() {
            self.can_do_delta = false;
            return;
        }
        self.min_max_delta_diff = diff;

        let delta_offset = self.compression_buffer[0].to_i128() - self.minimum_delta;
        if delta_offset < signed_min_for_type::<T>() || delta_offset > signed_max_for_type::<T>() {
            self.can_do_delta = false;
            return;
        }
        self.delta_offset = delta_offset;
    }
}

impl<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> BitpackingCompressionState<T> {
    fn new(
        info: &CompressionInfo,
        checkpoint_state: Arc<Mutex<ColumnCheckpointState>>,
        mut analyze_state: Box<dyn AnalyzeState>,
    ) -> Self {
        let logical_type = checkpoint_state.lock().get_original_column().ctx.logical_type.clone();
        let mut state = BitpackingState::new();
        if let Some(analyze) = analyze_state
            .as_any_mut()
            .downcast_mut::<BitpackingAnalyzeState<T>>()
        {
            state.mode = analyze.state.mode;
        }
        let mut result = Self {
            checkpoint_state,
            logical_type: logical_type.clone(),
            info: CompressionInfo::new(info.get_block_size(), info.get_block_header_size()),
            state,
            data: vec![0u8; info.get_block_size() as usize],
            data_ptr: BitpackingPrimitives::BITPACKING_HEADER_SIZE,
            metadata_ptr: info.get_block_size() as usize,
            current_segment_count: 0,
            current_segment_stats: SegmentStatistics::new(logical_type),
        };
        result.reset_segment();
        result
    }

    fn reset_segment(&mut self) {
        self.data.fill(0);
        self.data_ptr = BitpackingPrimitives::BITPACKING_HEADER_SIZE;
        self.metadata_ptr = self.info.get_block_size() as usize;
        self.current_segment_count = 0;
        self.current_segment_stats = SegmentStatistics::new(self.logical_type.clone());
    }

    fn can_store(&self, data_bytes: usize, meta_bytes: usize) -> bool {
        let required_data_bytes = align_value((self.data_ptr + data_bytes) as Idx) as usize;
        let required_meta_bytes =
            self.info.get_block_size() as usize - (self.metadata_ptr.saturating_sub(self.data_ptr)) + meta_bytes;
        required_data_bytes + required_meta_bytes
            <= self.info.get_block_size() as usize - BitpackingPrimitives::BITPACKING_HEADER_SIZE
    }

    fn flush_and_create_segment_if_full(&mut self, data_bytes: usize, meta_bytes: usize) {
        if !self.can_store(data_bytes, meta_bytes) {
            self.flush_segment();
            self.reset_segment();
        }
    }

    fn reserve_space(&mut self, data_bytes: usize) {
        self.flush_and_create_segment_if_full(data_bytes, std::mem::size_of::<u32>());
    }

    fn write_metadata(&mut self, mode: BitpackingMode) {
        let encoded = encode_metadata(BitpackingMetadata {
            mode,
            offset: self.data_ptr as u32,
        });
        self.metadata_ptr -= std::mem::size_of::<u32>();
        self.data[self.metadata_ptr..self.metadata_ptr + 4].copy_from_slice(&encoded.to_le_bytes());
    }

    fn update_segment_stats(&mut self, count: Idx) {
        self.current_segment_count += count;
        if self.state.has_valid {
            self.current_segment_stats.statistics_mut().set_has_no_null();
        }
        if self.state.has_invalid {
            self.current_segment_stats.statistics_mut().set_has_null();
        }
        if !self.state.all_invalid {
            let stats = self.current_segment_stats.statistics_mut();
            let data = stats
                .get_numeric_data()
                .cloned()
                .unwrap_or_default();
            let mut target = data;
            NumericStats::set_max(&mut target, self.state.maximum, stats.get_type());
            NumericStats::set_min(&mut target, self.state.minimum, stats.get_type());
            *stats.get_stats_data_mut() = crate::storage::statistics::StatsData::Numeric(target);
        }
    }

    fn write_value(&mut self, value: T) {
        let width = std::mem::size_of::<T>();
        value.write_le(&mut self.data[self.data_ptr..self.data_ptr + width]);
        self.data_ptr += width;
    }

    fn write_constant(&mut self, constant: T, count: Idx) {
        self.reserve_space(std::mem::size_of::<T>());
        self.write_metadata(BitpackingMode::Constant);
        self.write_value(constant);
        self.update_segment_stats(count);
    }

    fn write_constant_delta(&mut self, constant: T, frame_of_reference: T, count: Idx) {
        self.reserve_space(2 * std::mem::size_of::<T>());
        self.write_metadata(BitpackingMode::ConstantDelta);
        self.write_value(frame_of_reference);
        self.write_value(constant);
        self.update_segment_stats(count);
    }

    fn write_for(&mut self, values: &[T], width: BitpackingWidth, frame_of_reference: T, count: Idx) {
        let bp_size = BitpackingPrimitives::get_required_size(count, width);
        self.reserve_space(bp_size + 2 * std::mem::size_of::<T>());
        self.write_metadata(BitpackingMode::For);
        self.write_value(frame_of_reference);
        self.write_value(T::from_i128(width as i128));
        BitpackingPrimitives::pack_buffer(
            &mut self.data[self.data_ptr..self.data_ptr + bp_size],
            values,
            count as usize,
            width,
        );
        self.data_ptr += bp_size;
        self.update_segment_stats(count);
    }

    fn write_delta_for(
        &mut self,
        values: &[T],
        width: BitpackingWidth,
        frame_of_reference: T,
        delta_offset: T,
        count: Idx,
    ) {
        let bp_size = BitpackingPrimitives::get_required_size(count, width);
        self.reserve_space(bp_size + 3 * std::mem::size_of::<T>());
        self.write_metadata(BitpackingMode::DeltaFor);
        self.write_value(frame_of_reference);
        self.write_value(T::from_i128(width as i128));
        self.write_value(delta_offset);
        BitpackingPrimitives::pack_buffer(
            &mut self.data[self.data_ptr..self.data_ptr + bp_size],
            values,
            count as usize,
            width,
        );
        self.data_ptr += bp_size;
        self.update_segment_stats(count);
    }

    fn append_vector(&mut self, scan_vector: &Vector, count: Idx) {
        let data = scan_vector.raw_data();
        let width = std::mem::size_of::<T>();
        for idx in 0..count as usize {
            let value = T::read_le(&data[idx * width..idx * width + width]);
            let is_valid = scan_vector.validity.row_is_valid(idx);
            self.update(value, is_valid);
        }
    }

    fn update(&mut self, value: T, is_valid: bool) {
        let had_valid = self.state.has_valid;
        self.state.compression_buffer_validity[self.state.compression_buffer_idx] = is_valid;
        self.state.has_valid |= is_valid;
        self.state.has_invalid |= !is_valid;
        self.state.all_valid &= is_valid;
        self.state.all_invalid &= !is_valid;

        if is_valid {
            self.state.compression_buffer[self.state.compression_buffer_idx] = value;
            if !had_valid {
                self.state.minimum = value;
                self.state.maximum = value;
            } else {
                if value.to_i128() < self.state.minimum.to_i128() {
                    self.state.minimum = value;
                }
                if value.to_i128() > self.state.maximum.to_i128() {
                    self.state.maximum = value;
                }
            }
        }

        self.state.compression_buffer_idx += 1;
        if self.state.compression_buffer_idx == BITPACKING_METADATA_GROUP_SIZE {
            self.flush_group();
            self.state.reset();
        }
    }

    fn flush_group(&mut self) {
        if self.state.compression_buffer_idx == 0 {
            return;
        }

        if (self.state.all_invalid || self.state.maximum.to_i128() == self.state.minimum.to_i128())
            && matches!(self.state.mode, BitpackingMode::Auto | BitpackingMode::Constant)
        {
            self.write_constant(self.state.maximum, self.state.compression_buffer_idx as Idx);
            return;
        }

        self.state.calculate_for_stats();
        self.state.calculate_delta_stats();

        if self.state.can_do_delta {
            if self.state.maximum_delta == self.state.minimum_delta
                && !matches!(self.state.mode, BitpackingMode::For | BitpackingMode::DeltaFor)
            {
                self.write_constant_delta(
                    T::from_i128(self.state.maximum_delta),
                    self.state.compression_buffer[0],
                    self.state.compression_buffer_idx as Idx,
                );
                return;
            }

            let delta_required_bitwidth = minimum_bit_width_i128::<T>(self.state.min_max_delta_diff);
            let regular_required_bitwidth = minimum_bit_width_value::<T>(self.state.min_max_diff);
            let prefer_for =
                self.state.can_do_for && delta_required_bitwidth >= regular_required_bitwidth;

            if !prefer_for && self.state.mode != BitpackingMode::For {
                let mut delta_values = [T::default(); BITPACKING_METADATA_GROUP_SIZE];
                for (idx, slot) in delta_values
                    .iter_mut()
                    .enumerate()
                    .take(self.state.compression_buffer_idx)
                {
                    let current = self.state.compression_buffer[idx].to_i128();
                    let previous = if idx == 0 {
                        0
                    } else {
                        self.state.compression_buffer[idx - 1].to_i128()
                    };
                    *slot = T::from_i128(current - previous - self.state.minimum_delta);
                }
                self.write_delta_for(
                    &delta_values[..self.state.compression_buffer_idx],
                    delta_required_bitwidth,
                    T::from_i128(self.state.minimum_delta),
                    T::from_i128(self.state.delta_offset),
                    self.state.compression_buffer_idx as Idx,
                );
                return;
            }
        }

        if self.state.can_do_for {
            let width = minimum_bit_width_value::<T>(self.state.min_max_diff);
            let mut for_values = [T::default(); BITPACKING_METADATA_GROUP_SIZE];
            for (idx, slot) in for_values
                .iter_mut()
                .enumerate()
                .take(self.state.compression_buffer_idx)
            {
                *slot = T::from_i128(
                    self.state.compression_buffer[idx].to_i128() - self.state.minimum.to_i128(),
                );
            }
            self.write_for(
                &for_values[..self.state.compression_buffer_idx],
                width,
                self.state.minimum,
                self.state.compression_buffer_idx as Idx,
            );
            return;
        }

        panic!("bitpacking flush failed: no suitable encoding mode");
    }

    fn flush_segment(&mut self) {
        if self.current_segment_count == 0 {
            return;
        }
        let unaligned_offset = self.data_ptr;
        let metadata_offset = align_value(unaligned_offset as Idx) as usize;
        let metadata_size = self.info.get_block_size() as usize - self.metadata_ptr;
        let total_segment_size = metadata_offset + metadata_size;

        if unaligned_offset != metadata_offset {
            self.data[unaligned_offset..metadata_offset].fill(0);
        }
        self.data
            .copy_within(self.metadata_ptr..self.metadata_ptr + metadata_size, metadata_offset);
        let header_value = (metadata_offset + metadata_size) as u64;
        self.data[..8].copy_from_slice(&header_value.to_le_bytes());

        let segment = Arc::new(ColumnSegment::create_transient(
            self.logical_type.clone(),
            total_segment_size as Idx,
            CompressionType::BitPacking,
        ));
        {
            let mut buffer = segment.buffer.lock();
            buffer[..total_segment_size].copy_from_slice(&self.data[..total_segment_size]);
        }
        segment.set_count(self.current_segment_count);
        *segment.stats.lock() = self.current_segment_stats.clone();
        self.checkpoint_state.lock().flush_segment(segment);
    }

    fn finalize(&mut self) {
        self.flush_group();
        self.flush_segment();
        self.state.reset();
    }
}

fn init_compression_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    info: &CompressionInfo,
    checkpoint_state: Arc<Mutex<ColumnCheckpointState>>,
    analyze_state: Box<dyn AnalyzeState>,
) -> Box<dyn CompressionState> {
    Box::new(BitpackingCompressionState::<T>::new(
        info,
        checkpoint_state,
        analyze_state,
    ))
}

fn compress_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    state: &mut dyn CompressionState,
    scan_vector: &Vector,
    count: Idx,
) {
    let state = state
        .as_any_mut()
        .downcast_mut::<BitpackingCompressionState<T>>()
        .expect("invalid bitpacking compression state");
    state.append_vector(scan_vector, count);
}

fn compress_finalize_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    state: &mut dyn CompressionState,
) {
    let state = state
        .as_any_mut()
        .downcast_mut::<BitpackingCompressionState<T>>()
        .expect("invalid bitpacking compression state");
    state.finalize();
}

impl<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> BitpackingScanState<T> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(BitpackingFields {
                decompression_buffer: [T::default(); BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE],
                current_group: BitpackingMetadata {
                    mode: BitpackingMode::Invalid,
                    offset: 0,
                },
                current_width: 0,
                current_frame_of_reference: T::default(),
                current_constant: T::default(),
                current_delta_offset: T::default(),
                current_group_offset: 0,
                current_group_ptr_offset: 0,
                bitpacking_metadata_offset: 0,
            }),
        }
    }

    fn load_next_group(&self, segment: &ColumnSegment, block_data: &[u8]) {
        let mut inner = self.inner.lock();
        inner.current_group_offset = 0;

        let encoded = u32::from_le_bytes(
            block_data[inner.bitpacking_metadata_offset..inner.bitpacking_metadata_offset + 4]
                .try_into()
                .unwrap(),
        );
        let metadata = decode_metadata(encoded);
        inner.current_group = metadata;
        inner.bitpacking_metadata_offset -= std::mem::size_of::<u32>();
        inner.current_group_ptr_offset = segment.block_offset as usize + metadata.offset as usize;

        let mut ptr = inner.current_group_ptr_offset;
        match metadata.mode {
            BitpackingMode::Constant => {
                inner.current_constant = T::read_le(&block_data[ptr..ptr + segment.type_size as usize]);
                ptr += segment.type_size as usize;
            }
            BitpackingMode::For | BitpackingMode::ConstantDelta | BitpackingMode::DeltaFor => {
                inner.current_frame_of_reference =
                    T::read_le(&block_data[ptr..ptr + segment.type_size as usize]);
                ptr += segment.type_size as usize;
            }
            _ => panic!("invalid bitpacking mode"),
        }

        match metadata.mode {
            BitpackingMode::ConstantDelta => {
                inner.current_constant = T::read_le(&block_data[ptr..ptr + segment.type_size as usize]);
                ptr += segment.type_size as usize;
            }
            BitpackingMode::For | BitpackingMode::DeltaFor => {
                inner.current_width =
                    T::read_le(&block_data[ptr..ptr + segment.type_size as usize]).to_i128() as u8;
                ptr += segment.type_size as usize;
            }
            BitpackingMode::Constant => {}
            _ => panic!("invalid bitpacking mode"),
        }

        if metadata.mode == BitpackingMode::DeltaFor {
            inner.current_delta_offset =
                T::read_le(&block_data[ptr..ptr + segment.type_size as usize]);
            ptr += segment.type_size as usize;
        }

        inner.current_group_ptr_offset = ptr;
    }
}

fn align_value(value: Idx) -> Idx {
    let alignment = std::mem::size_of::<Idx>() as Idx;
    let remainder = value % alignment;
    if remainder == 0 {
        value
    } else {
        value + alignment - remainder
    }
}

fn signed_max_for_type<T: BitpackingValue>() -> i128 {
    (1i128 << (T::BITS - 1)) - 1
}

fn signed_min_for_type<T: BitpackingValue>() -> i128 {
    -(1i128 << (T::BITS - 1))
}

fn minimum_bit_width_i128<T: BitpackingValue>(value: i128) -> BitpackingWidth {
    if value == 0 {
        return if T::SIGNED { 1 } else { 0 };
    }
    let mut bitwidth: BitpackingWidth = if T::SIGNED { 1 } else { 0 };
    let mut current = value as u128;
    while current != 0 {
        bitwidth += 1;
        current >>= 1;
    }
    get_effective_width::<T>(bitwidth)
}

fn minimum_bit_width_value<T: BitpackingValue>(value: T) -> BitpackingWidth {
    if T::SIGNED {
        if value.to_i128() == T::MIN_I128 {
            return T::BITS as BitpackingWidth;
        }
        let magnitude = value.to_i128().abs();
        minimum_bit_width_i128::<T>(magnitude)
    } else {
        minimum_bit_width_i128::<T>(value.to_i128())
    }
}

fn get_effective_width<T: BitpackingValue>(width: BitpackingWidth) -> BitpackingWidth {
    let bits_of_type = T::BITS as BitpackingWidth;
    let type_size = (T::BITS / 8) as BitpackingWidth;
    if width.saturating_add(type_size) > bits_of_type {
        bits_of_type
    } else {
        width
    }
}

fn init_analyze_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    info: &CompressionInfo,
) -> Box<dyn AnalyzeState> {
    Box::new(BitpackingAnalyzeState::<T> {
        info: CompressionInfo::new(info.get_block_size(), info.get_block_header_size()),
        state: BitpackingState::new(),
    })
}

fn analyze_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    state: &mut dyn AnalyzeState,
    input: &Vector,
    count: Idx,
) -> bool {
    let state = state
        .as_any_mut()
        .downcast_mut::<BitpackingAnalyzeState<T>>()
        .expect("invalid analyze state for bitpacking");

    let type_size = input.get_type().physical_size() as Idx;
    if type_size * BITPACKING_METADATA_GROUP_SIZE as Idx * 2 > state.info.get_block_size() {
        return false;
    }

    let data = input.raw_data();
    let validity = &input.validity;
    let type_size = type_size as usize;
    for idx in 0..count as usize {
        let is_valid = validity.row_is_valid(idx);
        let value = if is_valid {
            T::read_le(&data[idx * type_size..(idx + 1) * type_size])
        } else {
            T::default()
        };
        if !state.state.update(value, is_valid) {
            return false;
        }
    }
    true
}

fn final_analyze_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    state: &mut dyn AnalyzeState,
) -> Idx {
    let state = state
        .as_any_mut()
        .downcast_mut::<BitpackingAnalyzeState<T>>()
        .expect("invalid analyze state for bitpacking");
    if !state.state.flush() {
        return Idx::MAX;
    }
    state.state.total_size
}

impl<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static> SegmentScanState
    for BitpackingScanState<T>
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn encode_metadata(metadata: BitpackingMetadata) -> u32 {
    debug_assert!(metadata.offset <= 0x00FF_FFFF);
    metadata.offset | ((metadata.mode as u32) << 24)
}

fn decode_metadata(encoded: u32) -> BitpackingMetadata {
    let mode = match ((encoded >> 24) & 0xFF) as u8 {
        1 => BitpackingMode::Auto,
        2 => BitpackingMode::Constant,
        3 => BitpackingMode::ConstantDelta,
        4 => BitpackingMode::DeltaFor,
        5 => BitpackingMode::For,
        _ => BitpackingMode::Invalid,
    };
    BitpackingMetadata {
        mode,
        offset: encoded & 0x00FF_FFFF,
    }
}

fn with_segment_bytes<R, F: FnOnce(&[u8]) -> R>(state: &ColumnScanState, f: F) -> R {
    let handle = state
        .pinned_buffer
        .as_ref()
        .expect("bitpacking scan requires pinned buffer");
    handle
        .with_data(f)
        .expect("failed to access pinned block for bitpacking")
}

fn apply_frame_of_reference<T: BitpackingValue>(values: &mut [T], frame_of_reference: T) {
    if frame_of_reference.to_i128() == 0 {
        return;
    }
    for value in values {
        *value = value.wrapping_add(frame_of_reference);
    }
}

fn delta_decode<T: BitpackingValue>(values: &mut [T], previous_value: T) -> T {
    if values.is_empty() {
        return previous_value;
    }
    values[0] = values[0].wrapping_add(previous_value);
    for idx in 1..values.len() {
        values[idx] = values[idx].wrapping_add(values[idx - 1]);
    }
    *values.last().unwrap()
}

fn write_value<T: BitpackingValue>(result: &mut Vector, index: usize, value: T, type_size: usize) {
    let offset = index * type_size;
    value.write_le(&mut result.raw_data_mut()[offset..offset + type_size]);
}

fn read_scan_state<'a, T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    state: &'a ColumnScanState,
) -> &'a BitpackingScanState<T> {
    let Some(scan_state) = state.scan_state.as_ref() else {
        panic!(
            "invalid bitpacking scan state: missing scan_state for {}",
            std::any::type_name::<T>()
        );
    };
    scan_state
        .as_any()
        .downcast_ref::<BitpackingScanState<T>>()
        .unwrap_or_else(|| {
            panic!(
                "invalid bitpacking scan state: downcast failed for {}",
                std::any::type_name::<T>()
            )
        })
}

fn bitpacking_init_scan_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    segment: &ColumnSegment,
    state: &mut ColumnScanState,
) -> Option<Box<dyn SegmentScanState>> {
    segment.initialize_scan_buffer(state);
    let scan_state = BitpackingScanState::<T>::new();
    with_segment_bytes(state, |block_data| {
        let metadata_offset = u64::from_le_bytes(
            block_data[segment.block_offset as usize
                ..segment.block_offset as usize + BitpackingPrimitives::BITPACKING_HEADER_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;
        scan_state.inner.lock().bitpacking_metadata_offset =
            segment.block_offset as usize + metadata_offset - std::mem::size_of::<u32>();
        scan_state.load_next_group(segment, block_data);
    });
    Some(Box::new(scan_state))
}

fn scan_partial_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
    result_offset: Idx,
) {
    let scan_state = read_scan_state::<T>(state);
    result.vector_type = VectorType::Flat;

    let type_size = segment.type_size as usize;
    let mut scanned = 0usize;

    while scanned < scan_count as usize {
        if scan_state.inner.lock().current_group_offset == BITPACKING_METADATA_GROUP_SIZE {
            with_segment_bytes(state, |block_data| scan_state.load_next_group(segment, block_data));
        }

        let (current_group, current_group_offset, current_constant, current_frame_of_reference) = {
            let inner = scan_state.inner.lock();
            (
                inner.current_group,
                inner.current_group_offset,
                inner.current_constant,
                inner.current_frame_of_reference,
            )
        };
        let offset_in_compression_group =
            current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

        match current_group.mode {
            BitpackingMode::Constant => {
                let to_scan =
                    (scan_count as usize - scanned).min(BITPACKING_METADATA_GROUP_SIZE - current_group_offset);
                for i in 0..to_scan {
                    write_value(
                        result,
                        result_offset as usize + scanned + i,
                        current_constant,
                        type_size,
                    );
                }
                scanned += to_scan;
                scan_state.inner.lock().current_group_offset += to_scan;
            }
            BitpackingMode::ConstantDelta => {
                let to_scan =
                    (scan_count as usize - scanned).min(BITPACKING_METADATA_GROUP_SIZE - current_group_offset);
                for i in 0..to_scan {
                    let multiplier = T::from_u128((current_group_offset + i) as u128);
                    let value = current_constant
                        .wrapping_mul(multiplier)
                        .wrapping_add(current_frame_of_reference);
                    write_value(result, result_offset as usize + scanned + i, value, type_size);
                }
                scanned += to_scan;
                scan_state.inner.lock().current_group_offset += to_scan;
            }
            BitpackingMode::For | BitpackingMode::DeltaFor => {
                let to_scan = (scan_count as usize - scanned).min(
                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group,
                );

                with_segment_bytes(state, |block_data| {
                    let mut inner = scan_state.inner.lock();
                    let current_width = inner.current_width;
                    let current_position_ptr = inner.current_group_ptr_offset
                        + (inner.current_group_offset * current_width as usize) / 8;
                    let decompression_group_start_pointer = current_position_ptr
                        - (offset_in_compression_group * current_width as usize) / 8;
                    BitpackingPrimitives::unpack_block(
                        &mut inner.decompression_buffer,
                        &block_data[decompression_group_start_pointer..],
                        current_width,
                        true,
                    );
                });

                let mut inner = scan_state.inner.lock();
                let current_frame_of_reference = inner.current_frame_of_reference;
                let current_delta_offset = inner.current_delta_offset;
                let output: Vec<T>;
                if current_group.mode == BitpackingMode::DeltaFor {
                    let values = &mut inner.decompression_buffer
                        [offset_in_compression_group..offset_in_compression_group + to_scan];
                    apply_frame_of_reference(values, current_frame_of_reference);
                    let new_delta_offset = delta_decode(values, current_delta_offset);
                    output = values.to_vec();
                    inner.current_delta_offset = new_delta_offset;
                } else {
                    let values = &mut inner.decompression_buffer
                        [offset_in_compression_group..offset_in_compression_group + to_scan];
                    apply_frame_of_reference(values, current_frame_of_reference);
                    output = values.to_vec();
                }
                for (idx, value) in output.iter().enumerate() {
                    write_value(result, result_offset as usize + scanned + idx, *value, type_size);
                }

                scanned += to_scan;
                inner.current_group_offset += to_scan;
            }
            _ => panic!("invalid bitpacking mode"),
        }
    }
}

fn scan_vector_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    segment: &ColumnSegment,
    state: &ColumnScanState,
    scan_count: Idx,
    result: &mut Vector,
) {
    scan_partial_t::<T>(segment, state, scan_count, result, 0);
}

fn skip_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    segment: &ColumnSegment,
    state: &mut ColumnScanState,
    skip_count: Idx,
) {
    let scan_state = read_scan_state::<T>(state);

    let (initial_group_offset, current_group_mode) = {
        let inner = scan_state.inner.lock();
        (inner.current_group_offset, inner.current_group.mode)
    };
    let meta_groups_to_skip = (skip_count as usize + initial_group_offset) / BITPACKING_METADATA_GROUP_SIZE;
    let mut skipped = 0usize;

    if meta_groups_to_skip > 0 {
        scan_state.inner.lock().bitpacking_metadata_offset -=
            (meta_groups_to_skip - 1) * std::mem::size_of::<u32>();
        with_segment_bytes(state, |block_data| scan_state.load_next_group(segment, block_data));
        skipped += BITPACKING_METADATA_GROUP_SIZE - initial_group_offset;
        skipped += (meta_groups_to_skip - 1) * BITPACKING_METADATA_GROUP_SIZE;
    }

    let mut remaining_to_skip = skip_count as usize - skipped;
    if matches!(
        current_group_mode,
        BitpackingMode::Constant | BitpackingMode::ConstantDelta | BitpackingMode::For
    ) {
        scan_state.inner.lock().current_group_offset += remaining_to_skip;
        return;
    }

    while remaining_to_skip > 0 {
        let (current_group_offset, current_width, current_group_ptr_offset, current_frame_of_reference, current_delta_offset) = {
            let inner = scan_state.inner.lock();
            (
                inner.current_group_offset,
                inner.current_width,
                inner.current_group_ptr_offset,
                inner.current_frame_of_reference,
                inner.current_delta_offset,
            )
        };
        let offset_in_compression_group =
            current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
        let skipping_this_group = remaining_to_skip.min(
            BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group,
        );

        with_segment_bytes(state, |block_data| {
            let mut inner = scan_state.inner.lock();
            let current_delta_offset = inner.current_delta_offset;
            let current_position_ptr =
                current_group_ptr_offset + (current_group_offset * current_width as usize) / 8;
            let decompression_group_start_pointer =
                current_position_ptr - (offset_in_compression_group * current_width as usize) / 8;
            BitpackingPrimitives::unpack_block(
                &mut inner.decompression_buffer,
                &block_data[decompression_group_start_pointer..],
                current_width,
                true,
            );
            let values = &mut inner.decompression_buffer
                [offset_in_compression_group..offset_in_compression_group + skipping_this_group];
            apply_frame_of_reference(values, current_frame_of_reference);
            let new_delta_offset = delta_decode(values, current_delta_offset);
            inner.current_delta_offset = new_delta_offset;
            inner.current_group_offset += skipping_this_group;
        });

        remaining_to_skip -= skipping_this_group;
        if scan_state.inner.lock().current_group_offset == BITPACKING_METADATA_GROUP_SIZE
            && remaining_to_skip > 0
        {
            with_segment_bytes(state, |block_data| scan_state.load_next_group(segment, block_data));
        }
    }
}

fn fetch_row_t<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    segment: &ColumnSegment,
    row_id: Idx,
    result: &mut Vector,
    result_idx: Idx,
) {
    let mut state = ColumnScanState::new();
    state.scan_state = bitpacking_init_scan_t::<T>(segment, &mut state);
    if row_id > 0 {
        skip_t::<T>(segment, &mut state, row_id);
    }

    let mut tmp = Vector::with_capacity(result.logical_type.clone(), 1);
    scan_vector_t::<T>(segment, &state, 1, &mut tmp);
    let type_size = segment.type_size as usize;
    result.raw_data_mut()[result_idx as usize * type_size..(result_idx as usize + 1) * type_size]
        .copy_from_slice(&tmp.raw_data()[..type_size]);
}

fn get_bitpacking_function<T: BitpackingValue + std::fmt::Debug + Default + Send + Sync + 'static>(
    data_type: PhysicalType,
) -> CompressionFunction {
    CompressionFunction::new(
        CompressionType::BitPacking,
        data_type,
        Some(init_analyze_t::<T>),
        Some(analyze_t::<T>),
        Some(final_analyze_t::<T>),
        Some(init_compression_t::<T>),
        Some(compress_t::<T>),
        Some(compress_finalize_t::<T>),
        Some(bitpacking_init_scan_t::<T>),
        Some(scan_vector_t::<T>),
        Some(scan_partial_t::<T>),
        Some(fetch_row_t::<T>),
        Some(skip_t::<T>),
        None,
        None,
        None,
        None,
        None,
    )
}

pub struct BitpackingFun;

impl BitpackingFun {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        match data_type {
            PhysicalType::Bool | PhysicalType::Int8 => get_bitpacking_function::<i8>(data_type),
            PhysicalType::Int16 => get_bitpacking_function::<i16>(data_type),
            PhysicalType::Int32 => get_bitpacking_function::<i32>(data_type),
            PhysicalType::Int64 => get_bitpacking_function::<i64>(data_type),
            PhysicalType::Uint8 => get_bitpacking_function::<u8>(data_type),
            PhysicalType::Uint16 => get_bitpacking_function::<u16>(data_type),
            PhysicalType::Uint32 => get_bitpacking_function::<u32>(data_type),
            PhysicalType::Uint64 => get_bitpacking_function::<u64>(data_type),
            _ => panic!("bitpacking not supported for physical type {:?}", data_type),
        }
    }

    pub fn type_is_supported(data_type: PhysicalType) -> bool {
        matches!(
            data_type,
            PhysicalType::Bool
                | PhysicalType::Int8
                | PhysicalType::Int16
                | PhysicalType::Int32
                | PhysicalType::Int64
                | PhysicalType::Uint8
                | PhysicalType::Uint16
                | PhysicalType::Uint32
                | PhysicalType::Uint64
        )
    }
}
