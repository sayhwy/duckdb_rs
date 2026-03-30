//! Variant 列数据（JSON 半结构化）。
//!
//! 对应 C++: `duckdb/storage/table/variant_column_data.hpp`
//!
//! # 职责
//!
//! `VariantColumnData` 存储 JSON/Variant 类型的列数据。支持两种模式：
//!
//! - **非 Shredded（`sub_columns.len() != 2`）**：整体存储为一个列（类似 VARCHAR）。
//! - **Shredded（`sub_columns.len() == 2`）**：拆分为类型列 + 值列两个子列，
//!   提升过滤性能（类似 Parquet Variant shredding）。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class VariantColumnData : public ColumnData` | `VariantColumnData` impl `ColumnData` trait |
//! | `vector<shared_ptr<ColumnData>> sub_columns` | `sub_columns: Vec<Arc<Mutex<ColumnDataKind>>>` |
//! | `shared_ptr<ValidityColumnData> validity` | `validity: Option<Arc<Mutex<ValidityColumnData>>>` |
//! | 静态方法 `ShredVariantData/UnshredVariantData` | 迁移到 `variant/` 子模块 |

use parking_lot::Mutex;
use std::sync::Arc;

use super::column_checkpoint_state::ColumnCheckpointState;
use super::column_data::{ColumnData, ColumnDataContext, ColumnDataType, PersistentColumnData};
use super::data_table_info::DataTableInfo;
use super::types::{Idx, LogicalType, RowId, TransactionData};
use super::validity_column_data::ValidityColumnData;

// ─── VariantColumnData ────────────────────────────────────────────────────────

/// Variant（JSON）类型列数据（C++: `class VariantColumnData`）。
pub struct VariantColumnData {
    /// 基础上下文（C++: `ColumnData` 基类字段）。
    pub ctx: ColumnDataContext,

    /// 子列（Shredded 时有 2 个：类型列 + 值列）（C++: `vector<shared_ptr<ColumnData>>`）。
    pub sub_columns: Vec<Arc<Mutex<ColumnData>>>,

    /// 有效性列（C++: `shared_ptr<ValidityColumnData> validity`）。
    pub validity: Option<Arc<Mutex<ValidityColumnData>>>,
}

impl VariantColumnData {
    /// 构造（C++: `VariantColumnData(BlockManager&, DataTableInfo&, idx_t, LogicalType, ColumnDataType, parent)`）。
    pub fn new(
        info: std::sync::Arc<DataTableInfo>,
        column_index: Idx,
        logical_type: LogicalType,
        data_type: ColumnDataType,
    ) -> Self {
        Self {
            ctx: ColumnDataContext::new(info, column_index, logical_type, data_type, false),
            sub_columns: Vec::new(),
            validity: None,
        }
    }

    /// 是否为 Shredded 模式（C++: `IsShredded()`）。
    ///
    /// Shredded = 正好有 2 个子列（类型列 + 值列）。
    pub fn is_shredded(&self) -> bool {
        self.sub_columns.len() == 2
    }

    /// 最大条目数（C++: `GetMaxEntry()`）。
    pub fn get_max_entry(&self) -> Idx {
        self.ctx.count.load(std::sync::atomic::Ordering::Relaxed)
    }

    // ── 扫描 ──────────────────────────────────────────────────────────────────

    /// 初始化扫描（C++: `InitializeScan()`）。
    pub fn initialize_scan(&self) {
        todo!("初始化有效性列和所有子列的扫描状态")
    }

    /// 初始化带偏移的扫描（C++: `InitializeScanWithOffset()`）。
    pub fn initialize_scan_with_offset(&self, row_idx: Idx) {
        todo!("从 row_idx 开始初始化子列扫描")
    }

    /// 扫描（C++: `Scan(TransactionData, vector_index, state, result, count)`）。
    pub fn scan(&self, _transaction: TransactionData, _vector_index: Idx, _count: Idx) -> Idx {
        todo!(
            "若 is_shredded: 扫描两个子列，调用 UnshredVariantData 重组；\
             否则: 扫描单个基础列"
        )
    }

    // ── 写入 ──────────────────────────────────────────────────────────────────

    /// 初始化 Append（C++: `InitializeAppend()`）。
    pub fn initialize_append(&self) {
        todo!("初始化有效性列和所有子列的 ColumnAppendState")
    }

    /// Append（C++: `Append(stats, state, vector, count)`）。
    pub fn append(&mut self, count: Idx) {
        todo!(
            "若 is_shredded: 调用 ShredVariantData 拆分为类型+值，\
             分别追加到两个子列；\
             否则: 直接追加原始数据"
        )
    }

    /// RevertAppend（C++: `RevertAppend(row_t)`）。
    pub fn revert_append(&mut self, new_count: RowId) {
        todo!("逆向恢复所有子列的 count")
    }

    // ── 索引 / 统计 ────────────────────────────────────────────────────────────

    /// 是否有任何未刷新变更（C++: `HasAnyChanges()`）。
    pub fn has_any_changes(&self) -> bool {
        todo!("检查有效性列和所有子列是否有 updates 或未刷新的段")
    }

    // ── Checkpoint ────────────────────────────────────────────────────────────

    /// 创建 checkpoint 状态（C++: `CreateCheckpointState()`）。
    pub fn create_checkpoint_state(
        &self,
        row_group_id: u64,
        partial_block_manager_id: u64,
    ) -> ColumnCheckpointState {
        ColumnCheckpointState::new(row_group_id, 0, partial_block_manager_id)
    }

    /// 序列化为持久化格式（C++: `Serialize()`）。
    pub fn serialize(&self) -> PersistentColumnData {
        todo!(
            "序列化有效性列和每个子列（递归）；\
             返回含子列 PersistentColumnData 的结构"
        )
    }

    /// 从持久化数据初始化（C++: `InitializeColumn(PersistentColumnData&, BaseStatistics&)`）。
    pub fn initialize_column(&mut self, data: &PersistentColumnData) {
        todo!("按 data.child_columns 顺序初始化有效性列和子列")
    }

    // ── 静态工具方法（委托 variant/ 子模块）──────────────────────────────────

    /// Shred：将 Variant 向量拆分为 (type_id_vector, value_vector)（C++: `ShredVariantData()`）。
    pub fn shred_variant_data(input: &[u8], count: Idx) -> (Vec<u8>, Vec<u8>) {
        todo!("委托 variant::variant_shredding::shred()")
    }

    /// Unshred：将 (type_id_vector, value_vector) 合并为 Variant 向量（C++: `UnshredVariantData()`）。
    pub fn unshred_variant_data(type_vec: &[u8], value_vec: &[u8], count: Idx) -> Vec<u8> {
        todo!("委托 variant::variant_unshredding::unshred()")
    }

    // ── 子列设置 ──────────────────────────────────────────────────────────────

    /// 设置有效性列（C++: `SetValidityData()`）。
    pub fn set_validity_data(&mut self, validity: Arc<Mutex<ValidityColumnData>>) {
        self.validity = Some(validity);
    }

    /// 设置子列（C++: `SetChildData()`）。
    pub fn set_child_data(&mut self, child_data: Vec<Arc<Mutex<ColumnData>>>) {
        self.sub_columns = child_data;
    }
}
