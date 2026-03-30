//! Update 操作的约束检查状态。
//!
//! 对应 C++: `duckdb/storage/table/update_state.hpp`
//!
//! # 职责
//!
//! `TableUpdateState` 在 `DataTable::Update()` 期间持有约束验证状态。
//! 目前仅含 `ConstraintState`，结构较简单。

use super::delete_state::ConstraintState;

// ─── TableUpdateState ──────────────────────────────────────────────────────────

/// UPDATE 操作的约束状态（C++: `struct TableUpdateState`）。
pub struct TableUpdateState {
    /// 约束验证状态（C++: `unique_ptr<ConstraintState> constraint_state`）。
    pub constraint_state: Option<Box<ConstraintState>>,
}

impl TableUpdateState {
    pub fn new() -> Self {
        Self {
            constraint_state: None,
        }
    }
}
