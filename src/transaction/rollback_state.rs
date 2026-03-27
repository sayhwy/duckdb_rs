//! 回滚状态机。
//!
//! 对应 C++:
//!   `duckdb/transaction/rollback_state.hpp`
//!   `src/transaction/rollback_state.cpp`
//!
//! # 职责
//!
//! `RollbackState` 在 `UndoBuffer::Rollback()` 中使用，
//! **反向**遍历所有 Undo 条目，将每个修改还原到修改前状态：
//!
//! - **UpdateTuple**：将 UpdateInfo.version_number 改为 `NOT_DELETED_ID`，
//!   并从版本链中解链该节点。
//! - **DeleteTuple**：将 ChunkVectorInfo 中对应行的版本号从 `transaction_id` 改为 `NOT_DELETED_ID`。
//! - **CatalogEntry**：撤销 Catalog 条目变更（恢复旧版本）。
//! - **Append**：回滚追加的行。

use super::types::{UndoFlags, NOT_DELETED_ID};
use super::append_info::AppendInfo;
use super::delete_info::DeleteInfo;

// ─── RollbackState ─────────────────────────────────────────────────────────────

/// 回滚阶段 Undo 遍历状态机。
pub struct RollbackState;

impl RollbackState {
    /// 构造。
    pub fn new() -> Self {
        Self
    }

    /// 处理一条 Undo 条目（反向遍历）。
    pub fn rollback_entry(&mut self, flags: UndoFlags, payload: &[u8]) {
        match flags {
            UndoFlags::UpdateTuple  => self.rollback_update(payload),
            UndoFlags::DeleteTuple  => self.rollback_delete(payload),
            UndoFlags::CatalogEntry => self.rollback_catalog_entry(payload),
            UndoFlags::Append       => self.rollback_append(payload),
            UndoFlags::SequenceValue | UndoFlags::Attach | UndoFlags::Empty => {}
            _ => {}
        }
    }

    // ── 私有：各类型回滚处理 ───────────────────────────────────────────────────

    fn rollback_update(&mut self, _payload: &[u8]) {
        // 在完整实现中：
        // 1. 反序列化 UpdateInfo
        // 2. 将 version_number 原子设为 NOT_DELETED_ID
        // 3. 从版本链中解链：将 prev.next 指向 self.next
        // 当前简化实现跳过
    }

    fn rollback_delete(&mut self, payload: &[u8]) {
        // 反序列化 DeleteInfo
        let info = DeleteInfo::deserialize(payload);

        // 在完整实现中：
        // 将 ChunkVectorInfo 中对应行的 version_number 从 transaction_id 改为 NOT_DELETED_ID
        // 当前简化实现仅记录日志
        let _ = info;
    }

    fn rollback_catalog_entry(&mut self, _payload: &[u8]) {
        // 在完整实现中：
        // 读取 CatalogEntry 指针和 extra_data
        // 调用 Catalog::Undo(entry) 恢复旧版本
        // 当前简化实现跳过
    }

    fn rollback_append(&mut self, payload: &[u8]) {
        // Append 回滚：撤销追加的行
        let info = AppendInfo::deserialize(payload);

        // 在完整实现中：
        // 调用 table.revert_append(transaction, start_row, count)
        // 这会减少 RowGroup 的行数并标记这些行为无效
        // 当前简化实现仅记录日志
        let _ = info;
    }
}

impl Default for RollbackState {
    fn default() -> Self {
        Self::new()
    }
}