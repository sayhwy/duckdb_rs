//! 提交状态机。
//!
//! 对应 C++:
//!   `duckdb/transaction/commit_state.hpp`
//!   `src/transaction/commit_state.cpp`
//!
//! # 职责
//!
//! `CommitState` 在 `UndoBuffer::Commit()` 和 `UndoBuffer::RevertCommit()` 中使用，
//! 正向遍历所有 Undo 条目，对每条执行：
//!
//! - **Commit**：将版本号从 `transaction_id` 改写为 `commit_id`，并清理索引数据。
//! - **RevertCommit**：将版本号从 `commit_id` 改回 `transaction_id`（commit 失败时）。

use super::append_info::AppendInfo;
use super::delete_info::DeleteInfo;
use super::types::{ActiveTransactionState, CommitMode, NOT_DELETED_ID, TransactionId, UndoFlags};

// ─── IndexDataRemover ──────────────────────────────────────────────────────────

/// 批量从索引中删除行。
pub struct IndexDataRemover {
    /// 待删除的行 ID 缓冲。
    pending_deletes: Vec<i64>,
    /// 当前批次对应的表 ID。
    current_table_id: Option<u64>,
    /// 删除类型。
    removal_type: IndexRemovalType,
}

/// 索引行删除类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRemovalType {
    /// 提交时立即从索引中删除。
    PermanentDelete,
    /// 仅标记，cleanup 时才删除。
    TemporaryDelete,
}

impl IndexDataRemover {
    /// 构造。
    pub fn new(removal_type: IndexRemovalType) -> Self {
        Self {
            pending_deletes: Vec::new(),
            current_table_id: None,
            removal_type,
        }
    }

    /// 加入一条 Delete 信息。
    pub fn push_delete(&mut self, table_id: u64, row_ids: &[i64]) {
        // 若表切换，flush 上一批
        if self.current_table_id.is_some() && self.current_table_id != Some(table_id) {
            self.flush();
        }

        self.current_table_id = Some(table_id);
        self.pending_deletes.extend_from_slice(row_ids);

        // 满批次时 flush
        if self.pending_deletes.len() >= 2048 {
            self.flush();
        }
    }

    /// 强制刷新剩余删除。
    pub fn flush(&mut self) {
        if self.pending_deletes.is_empty() {
            return;
        }

        // 在完整实现中，这里需要实际从索引中删除行
        // 当前简化实现仅清空缓冲
        self.pending_deletes.clear();
    }

    /// 选择删除类型。
    pub fn index_removal_type(
        active_transactions: ActiveTransactionState,
        commit_mode: CommitMode,
    ) -> IndexRemovalType {
        match (active_transactions, commit_mode) {
            (ActiveTransactionState::NoActiveTransactions, CommitMode::Commit) => {
                IndexRemovalType::PermanentDelete
            }
            _ => IndexRemovalType::TemporaryDelete,
        }
    }
}

// ─── CommitState ───────────────────────────────────────────────────────────────

/// 提交阶段 Undo 遍历状态机。
pub struct CommitState {
    /// 提交 / 回退 ID。
    pub commit_id: TransactionId,
    /// 提交模式。
    pub commit_mode: CommitMode,
    /// 索引删除批处理器。
    index_data_remover: IndexDataRemover,
    /// 存储管理器引用（用于提交时的存储操作）。
    tables: Option<
        std::collections::HashMap<u64, std::sync::Arc<crate::storage::data_table::DataTable>>,
    >,
}

impl CommitState {
    /// 构造。
    pub fn new(
        commit_id: TransactionId,
        active_transaction_state: ActiveTransactionState,
        commit_mode: CommitMode,
    ) -> Self {
        let removal_type =
            IndexDataRemover::index_removal_type(active_transaction_state, commit_mode);
        Self {
            commit_id,
            commit_mode,
            index_data_remover: IndexDataRemover::new(removal_type),
            tables: None,
        }
    }

    /// 设置表映射（用于提交时的存储操作）。
    pub fn set_tables(
        &mut self,
        tables: std::collections::HashMap<
            u64,
            std::sync::Arc<crate::storage::data_table::DataTable>,
        >,
    ) {
        self.tables = Some(tables);
    }

    /// 处理一条 Undo 条目（Commit 路径）。
    pub fn commit_entry(&mut self, flags: UndoFlags, payload: &[u8]) {
        match flags {
            UndoFlags::CatalogEntry => self.commit_catalog_entry(payload),
            UndoFlags::DeleteTuple => self.commit_delete(payload),
            UndoFlags::UpdateTuple => self.commit_update(payload),
            UndoFlags::Append => self.commit_append(payload),
            UndoFlags::SequenceValue | UndoFlags::Attach | UndoFlags::Empty => {}
            _ => {}
        }
    }

    /// 处理一条 Undo 条目（RevertCommit 路径）。
    pub fn revert_commit(&mut self, flags: UndoFlags, payload: &[u8]) {
        match flags {
            UndoFlags::UpdateTuple => self.revert_update(payload),
            UndoFlags::DeleteTuple => self.revert_delete(payload),
            UndoFlags::CatalogEntry => self.revert_catalog_entry(payload),
            UndoFlags::Append => self.revert_append(payload),
            _ => {}
        }
    }

    /// 确保所有 pending 删除已 flush。
    pub fn flush(&mut self) {
        self.index_data_remover.flush();
    }

    // ── 私有：各类型提交处理 ───────────────────────────────────────────────────

    fn commit_catalog_entry(&mut self, _payload: &[u8]) {
        // 在完整实现中，需要将 Catalog 条目的 timestamp 改为 commit_id
        // 并标记 parent 不可见
        // 当前简化实现跳过
    }

    fn commit_delete(&mut self, payload: &[u8]) {
        // 反序列化 DeleteInfo
        let info = DeleteInfo::deserialize(payload);

        // 在完整实现中：
        // 1. 将 ChunkVectorInfo 中对应行的版本号从 transaction_id 改为 commit_id
        // 2. 若表有索引，push_delete 到 index_data_remover

        // 当前简化实现：记录删除操作
        if info.is_consecutive {
            let row_ids: Vec<i64> = (0..info.count as i64)
                .map(|i| (info.base_row + i as u64) as i64)
                .collect();
            self.index_data_remover.push_delete(info.table_id, &row_ids);
        } else if let Some(ref rows) = info.rows {
            let row_ids: Vec<i64> = rows
                .iter()
                .map(|r| (info.base_row + *r as u64) as i64)
                .collect();
            self.index_data_remover.push_delete(info.table_id, &row_ids);
        }
    }

    fn commit_update(&mut self, _payload: &[u8]) {
        // 在完整实现中：
        // 反序列化 UpdateInfo
        // 将 version_number 从 transaction_id 原子改写为 commit_id
        // 当前简化实现跳过
    }

    fn commit_append(&mut self, payload: &[u8]) {
        // Append 提交：标记行已提交
        let info = AppendInfo::deserialize(payload);

        // 在完整实现中，调用 table.commit_append(commit_id, start_row, count)
        // 当前简化实现，我们直接记录日志
        let _ = info; // 避免未使用警告
    }

    fn revert_catalog_entry(&mut self, _payload: &[u8]) {
        // 将 Catalog 条目恢复为 transaction_id（撤销提交）
    }

    fn revert_delete(&mut self, _payload: &[u8]) {
        // 将 ChunkVectorInfo 中行版本号从 commit_id 改回 transaction_id
    }

    fn revert_update(&mut self, _payload: &[u8]) {
        // 将 UpdateInfo.version_number 从 commit_id 改回 transaction_id
    }

    fn revert_append(&mut self, payload: &[u8]) {
        // Append 回退：回滚追加的行
        let info = AppendInfo::deserialize(payload);

        // 在完整实现中，调用 table.revert_append(transaction, start_row, count)
        // 当前简化实现
        let _ = info;
    }
}
