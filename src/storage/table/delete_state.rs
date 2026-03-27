//! Delete 操作的约束检查状态。
//!
//! 对应 C++: `duckdb/storage/table/delete_state.hpp`
//!
//! # 职责
//!
//! `TableDeleteState` 在 `DataTable::Delete()` 期间持有：
//! - 约束验证状态（`ConstraintState`）。
//! - 是否存在 DELETE 约束标志。
//! - 用于约束校验的临时 DataChunk。
//! - 需要验证的列 ID 列表。

use super::types::Idx;

// ─── ConstraintState 占位 ──────────────────────────────────────────────────────

/// 约束验证状态（C++: `ConstraintState`）。
pub struct ConstraintState;

/// 存储列索引（C++: `StorageIndex`）。
#[derive(Debug, Clone, Copy)]
pub struct StorageIndex(pub u64);

// ─── TableDeleteState ──────────────────────────────────────────────────────────

/// DELETE 操作的约束状态（C++: `struct TableDeleteState`）。
pub struct TableDeleteState {
    /// 约束验证状态（C++: `unique_ptr<ConstraintState> constraint_state`）。
    pub constraint_state: Option<Box<ConstraintState>>,

    /// 是否有 DELETE 约束需要校验（C++: `bool has_delete_constraints`）。
    pub has_delete_constraints: bool,

    /// 用于约束校验的临时行数据（C++: `DataChunk verify_chunk`）。
    /// 实际类型为 DataChunk，此处用 Vec<u8> 占位。
    pub verify_chunk: Vec<u8>,

    /// 需要读取的列 ID（C++: `vector<StorageIndex> col_ids`）。
    pub col_ids: Vec<StorageIndex>,
}

impl TableDeleteState {
    pub fn new() -> Self {
        Self {
            constraint_state: None,
            has_delete_constraints: false,
            verify_chunk: Vec::new(),
            col_ids: Vec::new(),
        }
    }
}
