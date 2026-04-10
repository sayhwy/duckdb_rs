//! Row ID 虚列数据。
//!
//! 对应 C++: `duckdb/storage/table/row_id_column_data.hpp`
//!
//! # 职责
//!
//! `RowIdColumnData` 实现 `ColumnData` 接口，但**不存储真实数据**。
//! 它在扫描时动态计算每行的 row_id（= `row_group_start + vector_index * VECTOR_SIZE + i`）。
//!
//! 用途：`SELECT rowid FROM t` 或作为 UPDATE/DELETE 的内部列。
//!
//! # C++ → Rust 映射
//!
//! C++ 继承：`class RowIdColumnData : public ColumnData`。
//! Rust：`RowIdColumnData` 作为独立列类型，复用 `ColumnDataBase` 的基础状态。

use super::column_data::{ColumnDataBase, ColumnDataType};
use super::data_table_info::DataTableInfo;
use super::types::{Idx, LogicalType, RowId, TransactionData};
use std::sync::Arc;

// ─── RowIdColumnData ───────────────────────────────────────────────────────────

/// Row ID 虚列（C++: `class RowIdColumnData : public ColumnData`）。
///
/// 扫描时不读磁盘，动态生成行号向量。
pub struct RowIdColumnData {
    /// 基础上下文（C++: `ColumnData` 基类字段）。
    pub base: ColumnDataBase,
}

impl RowIdColumnData {
    /// 构造（C++: `RowIdColumnData(BlockManager&, DataTableInfo&)`）。
    pub fn new(info: Arc<DataTableInfo>, column_index: Idx) -> Self {
        Self {
            base: ColumnDataBase::new(
                info,
                column_index,
                LogicalType::bigint(),
                ColumnDataType::MainTable,
                false,
            ),
        }
    }

    /// 扫描：填充 row_id 向量（C++: `Scan(TransactionData, vector_index, state, result, count)`）。
    ///
    /// 生成值：`row_group_start + vector_index * STANDARD_VECTOR_SIZE + [0..count)`
    pub fn scan(
        &self,
        _transaction: TransactionData,
        vector_index: Idx,
        row_group_start: RowId,
        count: Idx,
        result: &mut Vec<RowId>,
    ) -> Idx {
        let base =
            row_group_start + (vector_index as i64) * (super::types::STANDARD_VECTOR_SIZE as i64);
        result.clear();
        for i in 0..count as i64 {
            result.push(base + i);
        }
        count
    }

    /// Scan committed（C++: `ScanCommitted()`）— 与 `scan` 相同，无 MVCC 可见性过滤。
    pub fn scan_committed(
        &self,
        vector_index: Idx,
        row_group_start: RowId,
        count: Idx,
        result: &mut Vec<RowId>,
    ) -> Idx {
        self.scan(
            TransactionData {
                start_time: 0,
                transaction_id: 0,
            },
            vector_index,
            row_group_start,
            count,
            result,
        )
    }

    /// 按 row_id fetch 单行（C++: `Fetch(state, row_id, result)`）。
    pub fn fetch(&self, row_id: RowId, result: &mut Vec<RowId>) -> Idx {
        result.push(row_id);
        1
    }

    /// Skip（C++: `Skip(state, count)`）— row_id 列无段需要跳过。
    pub fn skip(&self, _count: Idx) {}

    /// 初始化 Append（C++: `InitializeAppend()`）— row_id 列只读，无需实现。
    pub fn initialize_append(&self) {}

    /// Append（C++: `Append()`）— row_id 列由引擎自动维护，不手动追加。
    pub fn append(&mut self, _count: Idx) {
        let old = self.base.count.load(std::sync::atomic::Ordering::Relaxed);
        self.base
            .count
            .store(old + _count, std::sync::atomic::Ordering::Relaxed);
    }

    /// RevertAppend（C++: `RevertAppend(row_t new_count)`）。
    pub fn revert_append(&mut self, new_count: RowId) {
        self.base
            .count
            .store(new_count as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// 创建 Checkpoint 状态（C++: `CreateCheckpointState()`）。
    /// Row ID 列不需要持久化，返回空实现。
    pub fn create_checkpoint_state(&self) -> super::column_checkpoint_state::ColumnCheckpointState {
        super::column_checkpoint_state::ColumnCheckpointState::new(0, 0, 0)
    }

    /// 是否持久化（C++: `IsPersistent()`）— row_id 列永远返回 false。
    pub fn is_persistent(&self) -> bool {
        false
    }
}
