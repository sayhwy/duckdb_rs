//! Cleanup 状态机。
//!
//! 对应 C++:
//!   `duckdb/transaction/cleanup_state.hpp`
//!   `src/transaction/cleanup_state.cpp`
//!
//! # 职责
//!
//! 当一个事务提交后，若所有可能看到其旧版本的事务都已结束
//! （即 `lowest_active_transaction > commit_id`），则可以"清理"：
//!
//! - **UpdateTuple**：若旧版本对所有事务不可见，从版本链中移除并回收 UndoBuffer 空间。
//! - **DeleteTuple**：若已删除行对所有事务不可见，从 ChunkVectorInfo 中彻底移除版本记录。
//! - **CatalogEntry**：若旧 Catalog 版本不再可见，调用 `CatalogEntry::Cleanup()` 释放资源。
//!
//! Cleanup 在 `DuckTransactionManager::RemoveTransaction()` 后异步执行（通过 cleanup_queue）。

use super::append_info::AppendInfo;
use super::commit_state::{IndexDataRemover, IndexRemovalType};
use super::delete_info::DeleteInfo;
use super::duck_transaction::DuckTransaction;
use super::types::{ActiveTransactionState, TransactionId, UndoFlags};
use super::update_info::UpdateInfo;

// ─── CleanupState ──────────────────────────────────────────────────────────────

/// Cleanup 阶段 Undo 遍历状态机。
pub struct CleanupState<'a> {
    /// 所属事务（对应 DuckDB `CleanupState(transaction, ...)`）。
    transaction: &'a DuckTransaction,
    /// 当前系统中最低的活跃事务 start_time。
    lowest_active_transaction: TransactionId,
    /// 提交时的活跃事务状态。
    transaction_state: ActiveTransactionState,
    /// 用于从 Delta 索引中移除已删除行。
    index_data_remover: IndexDataRemover,
}

impl<'a> CleanupState<'a> {
    /// 构造。
    pub fn new(
        transaction: &'a DuckTransaction,
        lowest_active_transaction: TransactionId,
        transaction_state: ActiveTransactionState,
    ) -> Self {
        Self {
            transaction,
            lowest_active_transaction,
            transaction_state,
            index_data_remover: IndexDataRemover::new(IndexRemovalType::PermanentDelete),
        }
    }

    /// 处理一条 Undo 条目。
    pub fn cleanup_entry(&mut self, flags: UndoFlags, payload: &[u8]) {
        match flags {
            UndoFlags::CatalogEntry => self.cleanup_catalog_entry(payload),
            UndoFlags::DeleteTuple => self.cleanup_delete(payload),
            UndoFlags::UpdateTuple => self.cleanup_update(payload),
            UndoFlags::Append => self.cleanup_append(payload),
            UndoFlags::SequenceValue | UndoFlags::Attach | UndoFlags::Empty => {}
            _ => {}
        }
    }

    /// Flush 待处理的删除。
    pub fn flush(&mut self) {
        self.index_data_remover.flush();
    }

    // ── 私有：各类型 cleanup 处理 ─────────────────────────────────────────────

    fn cleanup_catalog_entry(&mut self, _payload: &[u8]) {
        // 在完整实现中：
        // 1. 反序列化 CatalogEntry 指针
        // 2. 若 parent 为 DELETED_ENTRY，调用 CatalogEntry::Cleanup() 释放资源
        // 当前简化实现跳过
    }

    fn cleanup_delete(&mut self, payload: &[u8]) {
        let info = DeleteInfo::deserialize(payload);
        if let Some(version_info) =
            crate::storage::table::row_version_manager::RowVersionManager::lookup(
                info.version_info_id,
            )
        {
            version_info.cleanup_delete(&info, self.lowest_active_transaction);
        }

        // 若没有其他活跃事务，不需要特殊清理
        if self.transaction_state == ActiveTransactionState::NoActiveTransactions {
            return;
        }

        // 在完整实现中：
        // 1. 若 commit_id < lowest_active_transaction，从 ChunkVectorInfo 中彻底移除版本记录
        // 2. 若表有 delta 索引，调用 index_data_remover.push_delete()
        // 当前简化实现仅记录日志
        let _ = info;
    }

    fn cleanup_update(&mut self, payload: &[u8]) {
        let info = UpdateInfo::deserialize_auto(payload);
        if let Some(segment) =
            crate::storage::table::update_segment::UpdateSegment::lookup(info.segment_id)
        {
            segment.cleanup_update(&info);
        }
    }

    fn cleanup_append(&mut self, payload: &[u8]) {
        let info = AppendInfo::deserialize(payload);

        // 在完整实现中：
        // 调用 table.cleanup_append(lowest_active_transaction, start_row, count)
        // 清理追加操作的元数据
        // 当前简化实现仅记录日志
        let _ = info;
    }
}
