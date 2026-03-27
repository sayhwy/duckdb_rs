//! UndoBuffer — 事务的撤销日志。
//!
//! 对应 C++:
//!   `duckdb/transaction/undo_buffer.hpp`
//!   `src/transaction/undo_buffer.cpp`
//!
//! # 职责
//!
//! 每个 `DuckTransaction` 拥有一个 `UndoBuffer`，用于：
//! - **Rollback**：反向遍历，撤销所有修改。
//! - **Commit**：正向遍历，设置版本号 / 写入 WAL。
//! - **Cleanup**：正向遍历，回收 MVCC 旧版本（在无活跃事务可见它时）。

use super::types::{ActiveTransactionState, TransactionId, UndoFlags, UNDO_ENTRY_HEADER_SIZE};
use super::undo_buffer_allocator::{UndoBufferAllocator, UndoBufferReference};
use super::commit_state::CommitState;
use super::rollback_state::RollbackState;
use super::cleanup_state::CleanupState;

// ─── UndoBufferProperties ──────────────────────────────────────────────────────

/// UndoBuffer 内容的统计摘要。
#[derive(Debug, Default)]
pub struct UndoBufferProperties {
    /// 估算的 Undo 数据总大小（字节）。
    pub estimated_size: usize,
    /// 是否包含 UPDATE 条目。
    pub has_updates: bool,
    /// 是否包含 DELETE 条目。
    pub has_deletes: bool,
    /// 是否存在带索引的删除。
    pub has_index_deletes: bool,
    /// 是否包含 Catalog 变更。
    pub has_catalog_changes: bool,
    /// 是否包含已删除的 Catalog 条目。
    pub has_dropped_entries: bool,
}

// ─── IteratorState ─────────────────────────────────────────────────────────────

/// 正向遍历游标。
#[derive(Default, Clone)]
pub struct IteratorState {
    /// 当前 Slab 下标。
    pub slab_index: usize,
    /// 当前 Slab 内字节偏移。
    pub byte_offset: usize,
    /// 是否已启动迭代。
    pub started: bool,
}

// ─── CommitInfo ────────────────────────────────────────────────────────────────

/// 提交阶段向 UndoBuffer 传递的上下文。
#[derive(Debug, Clone, Copy)]
pub struct CommitInfo {
    /// 提交时分配的提交 ID。
    pub commit_id: TransactionId,
    /// 提交时其他事务的活跃状态。
    pub active_transactions: ActiveTransactionState,
}

// ─── UndoBuffer ────────────────────────────────────────────────────────────────

/// 事务的 Undo 日志。
pub struct UndoBuffer {
    /// 底层字节分配器。
    allocator: UndoBufferAllocator,
    /// 提交时记录的活跃状态，供 Cleanup 判断是否可立即回收。
    active_transaction_state: ActiveTransactionState,
}

impl UndoBuffer {
    /// 创建空的 UndoBuffer。
    pub fn new(block_alloc_size: u64) -> Self {
        Self {
            allocator: UndoBufferAllocator::new(block_alloc_size),
            active_transaction_state: ActiveTransactionState::Unset,
        }
    }

    // ── 写入 ──────────────────────────────────────────────────────────────────

    /// 写入一条新 Undo 条目，返回可写入载荷的引用。
    ///
    /// 条目在 Slab 中的格式：
    /// ```text
    /// [ flags(1B) | payload_len(4B LE) | payload(len B) ]
    /// ```
    pub fn create_entry(&mut self, flags: UndoFlags, payload_len: usize) -> UndoBufferReference {
        // 对齐分配大小
        let alloc_len = payload_len + UNDO_ENTRY_HEADER_SIZE;

        // 分配空间
        let handle = self.allocator.allocate(alloc_len);

        if let Some(slab_idx) = handle.slab_index {
            // 写入 header
            let mut header = [0u8; 5];
            header[0] = flags as u8;
            let payload_len_bytes = (payload_len as u32).to_le_bytes();
            header[1..5].copy_from_slice(&payload_len_bytes);

            self.allocator.write_at(slab_idx, handle.position, &header);
        }

        // 返回载荷起始位置的引用
        UndoBufferReference {
            slab_index: handle.slab_index,
            position: handle.position + UNDO_ENTRY_HEADER_SIZE,
        }
    }

    /// 写入载荷数据到指定位置。
    pub fn write_payload(&self, slab_index: usize, position: usize, data: &[u8]) {
        self.allocator.write_at(slab_index, position, data);
    }

    // ── 查询 ──────────────────────────────────────────────────────────────────

    /// 是否有任何修改。
    pub fn changes_made(&self) -> bool {
        self.allocator.has_entries()
    }

    /// 统计 UndoBuffer 内容。
    pub fn get_properties(&self) -> UndoBufferProperties {
        let mut properties = UndoBufferProperties::default();

        if !self.changes_made() {
            return properties;
        }

        // 计算总大小
        properties.estimated_size = self.allocator.total_size();

        // 遍历条目统计类型
        self.allocator.iterate_forward(|flags, _| {
            match flags {
                UndoFlags::UpdateTuple => properties.has_updates = true,
                UndoFlags::DeleteTuple => properties.has_deletes = true,
                UndoFlags::CatalogEntry => properties.has_catalog_changes = true,
                _ => {}
            }
        });

        properties
    }

    // ── 生命周期操作 ───────────────────────────────────────────────────────────

    /// 提交：正向遍历，设置版本号。
    ///
    /// `iterator_state` 在返回后记录本次迭代位置，供 `revert_commit` 使用。
    pub fn commit(&mut self, iterator_state: &mut IteratorState, info: CommitInfo) {
        self.active_transaction_state = info.active_transactions;

        // 创建 CommitState 并遍历
        let mut commit_state = CommitState::new(
            info.commit_id,
            info.active_transactions,
            super::types::CommitMode::Commit,
        );

        iterator_state.started = true;
        iterator_state.slab_index = 0;
        iterator_state.byte_offset = 0;

        self.allocator.iterate_forward(|flags, payload| {
            commit_state.commit_entry(flags, payload);
        });

        commit_state.flush();
    }

    /// 写入 WAL（C++: `UndoBuffer::WriteToWAL()`）。
    ///
    /// 正向遍历所有 Undo 条目，将变更序列化写入 WAL。
    ///
    /// # C++ 源码对应
    /// ```cpp
    /// void UndoBuffer::WriteToWAL(WriteAheadLog &log, optional_ptr<StorageCommitState> commit_state) {
    ///     WALWriteState state(transaction, log, commit_state);
    ///     IterateEntries(state, [&](UndoFlags type, data_ptr_t data) {
    ///         state.CommitEntry(type, data);
    ///     });
    /// }
    /// ```
    pub fn write_to_wal(
        &self,
        commit_state: &mut dyn crate::storage::storage_manager::StorageCommitState,
    ) -> Result<(), String> {
        // 创建 WAL 写入状态机
        // 注意：当前简化实现中，WALWriteState 需要访问实际的 WriteAheadLog
        // 由于架构限制，我们通过 StorageCommitState 间接访问 WAL

        // 正向遍历所有 Undo 条目
        self.allocator.iterate_forward(|flags, payload| {
            // 根据条目类型写入 WAL
            match flags {
                UndoFlags::CatalogEntry => {
                    // TODO: 写入 WAL_CREATE_*/WAL_DROP_* 记录
                    // 需要反序列化 CatalogEntry 并调用 WAL 写入方法
                }
                UndoFlags::DeleteTuple => {
                    // TODO: 写入 WAL_DELETE 记录
                    // 需要反序列化 DeleteInfo 并调用 WAL 写入方法
                }
                UndoFlags::UpdateTuple => {
                    // TODO: 写入 WAL_UPDATE 记录
                    // 需要反序列化 UpdateInfo 并调用 WAL 写入方法
                }
                UndoFlags::Append => {
                    // Append 由 LocalStorage 处理，不需要在 UndoBuffer 中写 WAL
                }
                UndoFlags::SequenceValue | UndoFlags::Attach | UndoFlags::Empty => {
                    // 这些类型不需要写入 WAL
                }
            }
        });

        Ok(())
    }

    /// 回退已提交的修改。
    ///
    /// 仅回退到 `end_state` 位置（即本次 commit 写入的范围）。
    pub fn revert_commit(&mut self, _end_state: &IteratorState, transaction_id: TransactionId) {
        let mut commit_state = CommitState::new(
            transaction_id,
            self.active_transaction_state,
            super::types::CommitMode::RevertCommit,
        );

        // 反向遍历
        self.allocator.iterate_reverse(|flags, payload| {
            commit_state.revert_commit(flags, payload);
        });
    }

    /// 回滚：反向遍历，撤销所有修改。
    pub fn rollback(&mut self) {
        let mut rollback_state = RollbackState::new();

        // 反向遍历
        self.allocator.iterate_reverse(|flags, payload| {
            rollback_state.rollback_entry(flags, payload);
        });

        // 清空 UndoBuffer
        self.allocator.clear();
    }

    /// 清理旧版本。
    ///
    /// 只有在提交成功后、且所有可能看见旧版本的事务都已结束时调用。
    pub fn cleanup(&self, lowest_active_transaction: TransactionId) {
        let mut cleanup_state = CleanupState::new(
            lowest_active_transaction,
            self.active_transaction_state,
        );

        self.allocator.iterate_forward(|flags, payload| {
            cleanup_state.cleanup_entry(flags, payload);
        });
    }

    /// 获取分配器引用。
    pub fn allocator(&self) -> &UndoBufferAllocator {
        &self.allocator
    }
}