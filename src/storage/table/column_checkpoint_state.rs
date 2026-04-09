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

use super::column_data::{ColumnData, ColumnDataKind, ColumnDataType};
use super::column_segment::{ColumnSegment, ColumnSegmentType};
use super::segment_base::SegmentBase;
use super::types::{DataPointer, Idx};

// 使用统一的统计信息模块
pub use crate::storage::statistics::BaseStatistics;

// ─── 外部类型占位 ──────────────────────────────────────────────────────────────

/// 块管理器存根（C++: `BlockManager`）。
pub struct BlockManager;

/// 部分块状态（C++: `PartialBlockState`）。
pub struct PartialBlockState;

/// 部分块管理器（C++: `PartialBlockManager`）。
pub struct PartialBlockManager;

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
    pub block_manager: Arc<BlockManager>,
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
        }
    }

    /// 创建带指定类型的 checkpoint 状态
    pub fn with_type(logical_type: crate::common::types::LogicalType) -> Self {
        Self {
            original_column: None,
            global_stats: BaseStatistics::create_empty(logical_type),
            data_pointers: Vec::new(),
            result_column: None,
        }
    }

    /// 绑定原始列数据。
    pub fn set_original_column(&mut self, original_column: Arc<ColumnData>) {
        self.global_stats = BaseStatistics::create_empty(original_column.ctx.logical_type.clone());
        self.original_column = Some(original_column);
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
    pub fn flush_segment(&mut self, segment: Arc<ColumnSegment>) {
        let result = self.get_result_column();
        let start_row = result.count();
        result.ctx.append_existing_segment(Arc::clone(&segment), start_row);
        if segment.segment_type == ColumnSegmentType::Persistent {
            self.data_pointers.push(DataPointer {
                block_id: segment.block_id,
                offset: segment.block_offset as u32,
                row_start: start_row,
                tuple_count: segment.count(),
                compression_type: segment.compression,
                statistics: segment.stats.lock().statistics().clone(),
            });
        }
    }

    /// 获取全局统计信息
    pub fn get_statistics(&self) -> &BaseStatistics {
        &self.global_stats
    }

    /// 获取可变全局统计信息
    pub fn get_statistics_mut(&mut self) -> &mut BaseStatistics {
        &mut self.global_stats
    }
}
