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

use super::append_info::AppendInfo;
use super::delete_info::DeleteInfo;
use super::duck_transaction::DuckTransaction;
use super::types::{NOT_DELETED_ID, UndoFlags};
use super::update_info::UpdateInfo;
use crate::catalog::{CatalogEntryKind, CatalogEntryNode, CatalogType};

// ─── RollbackState ─────────────────────────────────────────────────────────────

/// 回滚阶段 Undo 遍历状态机。
pub struct RollbackState<'a> {
    transaction: &'a DuckTransaction,
}

impl<'a> RollbackState<'a> {
    pub fn new(transaction: &'a DuckTransaction) -> Self {
        Self { transaction }
    }

    /// 处理一条 Undo 条目（反向遍历）。
    pub fn rollback_entry(&mut self, flags: UndoFlags, payload: &[u8]) {
        match flags {
            UndoFlags::UpdateTuple => self.rollback_update(payload),
            UndoFlags::DeleteTuple => self.rollback_delete(payload),
            UndoFlags::CatalogEntry => self.rollback_catalog_entry(payload),
            UndoFlags::Append => self.rollback_append(payload),
            UndoFlags::SequenceValue | UndoFlags::Attach | UndoFlags::Empty => {}
            _ => {}
        }
    }

    // ── 私有：各类型回滚处理 ───────────────────────────────────────────────────

    fn rollback_update(&mut self, payload: &[u8]) {
        let info = UpdateInfo::deserialize_auto(payload);
        if let Some(segment) =
            crate::storage::table::update_segment::UpdateSegment::lookup(info.segment_id)
        {
            segment.rollback_update(&info);
        }
    }

    fn rollback_delete(&mut self, payload: &[u8]) {
        let info = DeleteInfo::deserialize(payload);
        if let Some(version_info) =
            crate::storage::table::row_version_manager::RowVersionManager::lookup(
                info.version_info_id,
            )
        {
            version_info.rollback_delete(&info);
        }
    }

    fn rollback_catalog_entry(&mut self, payload: &[u8]) {
        if payload.len() < 8 {
            return;
        }
        let entry_ptr = u64::from_le_bytes(payload[0..8].try_into().unwrap()) as usize;
        if entry_ptr == 0 {
            return;
        }
        let old_entry = unsafe { &*(entry_ptr as *const CatalogEntryNode) };
        let Some(set) = old_entry.catalog_set() else {
            return;
        };
        let was_drop = old_entry.parent().map(|entry| entry.base.deleted).unwrap_or(false);
        set.undo(old_entry);
        if old_entry.base.entry_type == CatalogType::TableEntry {
            match (&old_entry.kind, was_drop) {
                (CatalogEntryKind::Table(_), false) => {
                    self.transaction.db.tables.lock().remove(&(
                        old_entry.base.schema_name.to_ascii_lowercase(),
                        old_entry.base.name.to_ascii_lowercase(),
                    ));
                }
                (CatalogEntryKind::Table(table), true) => {
                    self.transaction.db.tables.lock().insert(
                        (
                            old_entry.base.schema_name.to_ascii_lowercase(),
                            old_entry.base.name.to_ascii_lowercase(),
                        ),
                        crate::db::conn::TableHandle {
                            entry: table.clone(),
                        },
                    );
                }
                _ => {}
            }
        }
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
