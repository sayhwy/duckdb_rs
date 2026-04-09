//! 列数据 Checkpoint 状态。
//!
//! 对应 C++: `duckdb/storage/table/column_checkpoint_state.hpp`
//!
//! # 职责
//!
//! `ColumnCheckpointState` 在 checkpoint 期间持有一列的压缩/写盘状态：
//! - 收集本次 checkpoint 生成的 `DataPointer` 列表。
//! - 持有全局统计信息（`global_stats`）。
//! - 管理 `result_column`（新创建的持久化列数据）。
//!
//! `PartialBlockForCheckpoint` 实现 C++ `PartialBlock` 接口，
//! 支持将多个 `ColumnSegment` 共享同一个物理 block 以节省空间。

use parking_lot::Mutex;
use std::sync::Arc;

use crate::common::types::LogicalTypeId;
use crate::storage::buffer::BlockManager as StorageBlockManager;

use super::column_data::{ColumnData, ColumnDataKind, ColumnDataType};
use super::column_segment::{ColumnSegment, ColumnSegmentType, SegmentStatistics};
use super::segment_base::SegmentBase;
use super::types::{DataPointer, Idx};

// 使用统一的统计信息模块
pub use crate::storage::statistics::BaseStatistics;

// ─── 外部类型占位 ──────────────────────────────────────────────────────────────

/// 部分块状态（C++: `PartialBlockState`）。
pub struct PartialBlockState;

/// 部分块管理器（C++: `PartialBlockManager`）。
pub struct PartialBlockManager {
    block_manager: Option<Arc<dyn StorageBlockManager>>,
}

impl PartialBlockManager {
    pub fn new(block_manager: Arc<dyn StorageBlockManager>) -> Self {
        Self {
            block_manager: Some(block_manager),
        }
    }

    pub fn empty() -> Self {
        Self { block_manager: None }
    }

    pub fn get_block_manager(&self) -> Arc<dyn StorageBlockManager> {
        self.block_manager
            .as_ref()
            .cloned()
            .expect("PartialBlockManager requires block manager")
    }

    pub fn clear_blocks(&mut self) {}

    pub fn flush_partial_blocks(&mut self) {}

    pub fn merge(&mut self, _other: &mut PartialBlockManager) {}

    pub fn rollback(&mut self) {}
}

/// 压缩函数（C++: `CompressionFunction`）。
pub struct CompressionFunction;

// ─── PartialBlock trait ────────────────────────────────────────────────────────

/// 可与其他列段共享的物理块接口（C++: `class PartialBlock`，纯虚）。
pub trait PartialBlock: Send + Sync {
    /// 将块写入存储（C++: `Flush(QueryContext, idx_t free_space_left)`）。
    fn flush(&mut self, free_space_left: Idx);
    /// 与另一块合并（C++: `Merge(PartialBlock&, idx_t offset, idx_t other_size)`）。
    fn merge(&mut self, other: &mut dyn PartialBlock, offset: Idx, other_size: Idx);
    /// 将列段追加到块尾（C++: `AddSegmentToTail()`）。
    fn add_segment_to_tail(&mut self, segment_offset: u32);
    /// 清除（C++: `Clear()`）。
    fn clear(&mut self);
}

// ─── PartialColumnSegment ──────────────────────────────────────────────────────

/// 指向一个列段在共享块中的偏移（C++: `PartialColumnSegment`）。
pub struct PartialColumnSegment {
    /// 所属列数据 ID（C++: `ColumnData &data`，用 ID 代替引用）。
    pub column_data_id: u64,
    /// 所属列段 ID（C++: `ColumnSegment &segment`）。
    pub column_segment_id: u64,
    /// 在共享块中的起始字节偏移（C++: `uint32_t offset_in_block`）。
    pub offset_in_block: u32,
}

// ─── PartialBlockForCheckpoint ────────────────────────────────────────────────

/// Checkpoint 期间的共享物理块（C++: `struct PartialBlockForCheckpoint`）。
///
/// 多个 `ColumnSegment` 的压缩数据写入同一个块以提高空间利用率。
/// 块写满或 checkpoint 完成时调用 `flush()`，触发实际落盘。
pub struct PartialBlockForCheckpoint {
    /// 状态（C++: `PartialBlockState state`）。
    pub state: PartialBlockState,
    /// 块管理器（C++: `BlockManager &block_manager`）。
    pub block_manager: Arc<dyn StorageBlockManager>,
    /// 部分块管理器（C++: `PartialBlockManager &partial_block_manager`）。
    pub partial_block_manager: Arc<Mutex<PartialBlockManager>>,
    /// 该块包含的列段列表（C++: `vector<PartialColumnSegment> segments`）。
    pub segments: Vec<PartialColumnSegment>,
}

impl PartialBlock for PartialBlockForCheckpoint {
    fn flush(&mut self, _free_space_left: Idx) {
        todo!("flush partial block to disk")
    }

    fn merge(&mut self, _other: &mut dyn PartialBlock, _offset: Idx, _other_size: Idx) {
        todo!("merge two partial blocks")
    }

    fn add_segment_to_tail(&mut self, _segment_offset: u32) {
        todo!("add segment to tail of partial block")
    }

    fn clear(&mut self) {
        self.segments.clear();
    }
}

// ─── ColumnCheckpointState ───────────────────────────────────────────────

/// Checkpoint 期间单列的状态（C++: `class ColumnCheckpointState`）。
pub struct ColumnCheckpointState {
    /// 原始列数据（C++: `ColumnData &original_column`）。
    pub original_column: Option<Arc<ColumnData>>,
    /// 全局统计信息（C++: `unique_ptr<BaseStatistics> global_stats`）。
    pub global_stats: BaseStatistics,
    /// 本次 checkpoint 生成的数据指针列表（C++: `vector<DataPointer> data_pointers`）。
    pub data_pointers: Vec<DataPointer>,
    /// 结果列数据（C++: `shared_ptr<ColumnData> result_column`）。
    pub result_column: Option<Arc<ColumnData>>,
    /// 当前 checkpoint 使用的 partial block manager。
    pub partial_block_manager: Option<Arc<PartialBlockManager>>,
}

impl ColumnCheckpointState {
    /// 创建新的 checkpoint 状态
    pub fn new(row_group_id: u64, column_id: u64, partial_block_manager_id: u64) -> Self {
        // 使用默认的 integer 类型创建统计信息
        let _ = (row_group_id, column_id, partial_block_manager_id);
        Self {
            original_column: None,
            global_stats: BaseStatistics::create_empty(crate::common::types::LogicalType::integer()),
            data_pointers: Vec::new(),
            result_column: None,
            partial_block_manager: None,
        }
    }

    /// 创建带指定类型的 checkpoint 状态
    pub fn with_type(logical_type: crate::common::types::LogicalType) -> Self {
        Self {
            original_column: None,
            global_stats: BaseStatistics::create_empty(logical_type),
            data_pointers: Vec::new(),
            result_column: None,
            partial_block_manager: None,
        }
    }

    /// 绑定原始列数据。
    pub fn set_original_column(&mut self, original_column: Arc<ColumnData>) {
        self.global_stats = BaseStatistics::create_empty(original_column.ctx.logical_type.clone());
        self.original_column = Some(original_column);
    }

    pub fn set_partial_block_manager(
        &mut self,
        partial_block_manager: Arc<PartialBlockManager>,
    ) {
        self.partial_block_manager = Some(partial_block_manager);
    }

    /// 获取原始列。
    pub fn get_original_column(&self) -> Arc<ColumnData> {
        self.original_column
            .as_ref()
            .cloned()
            .expect("original_column not set")
    }

    /// 获取 checkpoint 结果列；若尚未创建则按 DuckDB 语义创建空列。
    pub fn get_result_column(&mut self) -> Arc<ColumnData> {
        if self.result_column.is_none() {
            let original = self.get_original_column();
            self.result_column = Some(ColumnDataKind::create(
                Arc::clone(&original.ctx.info),
                original.ctx.column_index,
                original.ctx.logical_type.clone(),
                ColumnDataType::CheckpointTarget,
                false,
            ));
        }
        self.result_column.as_ref().cloned().unwrap()
    }

    /// 获取最终结果列。
    pub fn get_final_result(&mut self) -> Arc<ColumnData> {
        if self.result_column.is_none() {
            return self.get_original_column();
        }
        let original_count = self
            .original_column
            .as_ref()
            .map(|column| column.count())
            .unwrap_or_default();
        let result = self.result_column.as_ref().cloned().unwrap();
        result
            .ctx
            .count
            .store(original_count, std::sync::atomic::Ordering::Relaxed);
        result
    }

    /// Attach a checkpoint-produced segment to the result column.
    ///
    /// This restores DuckDB's `ColumnCheckpointState::FlushSegment` role at the
    /// storage-tree level: compressed transient segments are materialized into
    /// the checkpoint target column, while persistent metadata is collected
    /// later from that column.
    pub fn flush_segment(&mut self, segment: Arc<ColumnSegment>, segment_size: Idx) {
        let segment = match segment.segment_type {
            ColumnSegmentType::Persistent => segment,
            ColumnSegmentType::Transient => self.flush_segment_internal(segment, segment_size),
        };
        let result = self.get_result_column();
        let start_row = result.count();
        result.ctx.append_existing_segment(Arc::clone(&segment), start_row);
        self.data_pointers.push(DataPointer {
            block_id: segment.block_id,
            offset: segment.block_offset as u32,
            row_start: start_row,
            tuple_count: segment.count(),
            compression_type: segment.compression,
            statistics: segment.stats.lock().statistics().clone(),
        });
    }

    /// 获取全局统计信息
    pub fn get_statistics(&self) -> &BaseStatistics {
        &self.global_stats
    }

    /// 获取可变全局统计信息
    pub fn get_statistics_mut(&mut self) -> &mut BaseStatistics {
        &mut self.global_stats
    }

    fn flush_segment_internal(
        &self,
        segment: Arc<ColumnSegment>,
        segment_size: Idx,
    ) -> Arc<ColumnSegment> {
        let block_manager = self.get_block_manager();
        let block_id = block_manager.get_free_block_id_for_checkpoint();
        let mut block = block_manager.create_block(block_id, None);
        let payload_size = self.write_transient_segment_payload(
            &segment,
            segment_size as usize,
            block.payload_mut(),
        );
        block_manager.write_block(&block, block_id);
        let handle = block_manager.register_block(block_id);

        Arc::new(ColumnSegment::create_persistent_with_handle(
            segment.logical_type.clone(),
            block_id,
            0,
            segment.count(),
            payload_size as Idx,
            segment.compression,
            SegmentStatistics::from_stats(segment.stats.lock().statistics().clone()),
            handle,
        ))
    }

    fn get_block_manager(&self) -> Arc<dyn StorageBlockManager> {
        self.partial_block_manager
            .as_ref()
            .expect("ColumnCheckpointState::flush_segment requires partial block manager")
            .get_block_manager()
    }

    fn write_transient_segment_payload(
        &self,
        segment: &ColumnSegment,
        segment_size: usize,
        payload: &mut [u8],
    ) -> usize {
        if segment.compression != super::types::CompressionType::Uncompressed {
            let used_bytes = segment.segment_size() as usize;
            let buffer = segment.buffer.lock();
            payload[..used_bytes].copy_from_slice(&buffer[..used_bytes]);
            return used_bytes;
        }

        if segment.logical_type.id == LogicalTypeId::Varchar {
            return self.write_transient_varchar_payload(segment, payload);
        }

        let used_bytes = segment_size;
        let buffer = segment.buffer.lock();
        payload[..used_bytes].copy_from_slice(&buffer[..used_bytes]);
        used_bytes
    }

    fn write_transient_varchar_payload(&self, segment: &ColumnSegment, payload: &mut [u8]) -> usize {
        let tuple_count = segment.count() as usize;
        let buffer = segment.buffer.lock();
        let mut strings = Vec::with_capacity(tuple_count);
        for row_idx in 0..tuple_count {
            let offset = row_idx * 16;
            let len = u32::from_le_bytes(buffer[offset..offset + 4].try_into().unwrap()) as usize;
            if len <= 12 {
                strings.push(buffer[offset + 4..offset + 4 + len].to_vec());
            } else {
                let prefix = &buffer[offset + 4..offset + 8];
                let ptr = u64::from_le_bytes(buffer[offset + 8..offset + 16].try_into().unwrap())
                    as *const u8;
                let source = unsafe { std::slice::from_raw_parts(ptr, len) };
                debug_assert_eq!(&source[..4.min(source.len())], prefix);
                strings.push(source.to_vec());
            }
        }
        drop(buffer);

        let dict_size: usize = strings.iter().map(|value| value.len()).sum();
        let offsets_size = tuple_count * std::mem::size_of::<i32>();
        let dict_start = 8 + offsets_size;
        let dict_end = dict_start + dict_size;
        assert!(
            dict_end <= payload.len(),
            "VARCHAR checkpoint segment too large for one block"
        );

        payload[..dict_end].fill(0);
        payload[0..4].copy_from_slice(&(dict_size as u32).to_le_bytes());
        payload[4..8].copy_from_slice(&(dict_end as u32).to_le_bytes());

        let mut cumulative = 0u32;
        for (idx, string) in strings.iter().enumerate() {
            cumulative += string.len() as u32;
            let entry_offset = 8 + idx * std::mem::size_of::<i32>();
            payload[entry_offset..entry_offset + 4]
                .copy_from_slice(&(cumulative as i32).to_le_bytes());
            if !string.is_empty() {
                let dict_pos = dict_end - cumulative as usize;
                payload[dict_pos..dict_pos + string.len()].copy_from_slice(string);
            }
        }
        dict_end
    }
}
