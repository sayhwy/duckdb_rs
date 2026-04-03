//! DuckDB 原生事务实现。
//!
//! 对应 C++:
//!   `duckdb/transaction/duck_transaction.hpp`
//!   `src/transaction/duck_transaction.cpp`
//!
//! # 结构
//!
//! ```text
//! DuckTransaction
//!   ├── UndoBuffer          — MVCC 撤销日志
//!   ├── LocalStorage        — 未提交 Append 的本地缓冲
//!   ├── write_lock          — CheckpointLock（共享写锁）
//!   ├── sequence_usage      — 本事务使用的序列快照（序列ID → SequenceValue）
//!   ├── modified_tables     — 被本事务修改的表 ID 集合
//!   └── active_locks        — 每张表的 CheckpointLock 弱引用缓存
//! ```
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class DuckTransaction : public Transaction` | `struct DuckTransaction` impl `Transaction` |
//! | `UndoBuffer undo_buffer` | `undo_buffer: UndoBuffer` |
//! | `unique_ptr<LocalStorage> storage` | `storage: Box<LocalStorage>` |
//! | `unique_ptr<StorageLockKey> write_lock` | `write_lock: Option<Box<StorageLockKey>>` |
//! | `reference_map_t<SequenceCatalogEntry, SequenceValue&>` | `sequence_usage: Mutex<HashMap<u64, SequenceValue>>` |
//! | `reference_map_t<DataTable, shared_ptr<DataTable>>` | `modified_tables: Mutex<HashSet<u64>>` |
//! | `atomic<idx_t> catalog_version` | `catalog_version: AtomicU64` |

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::append_info::AppendInfo;
use super::delete_info::DeleteInfo;
use super::transaction::Transaction;
use super::types::{
    ActiveTransactionState, Idx, MAXIMUM_QUERY_ID, NOT_DELETED_ID, TransactionId, UndoFlags,
};
use super::update_info::UpdateInfo;
use super::undo_buffer::{CommitInfo, IteratorState, UndoBuffer, UndoBufferProperties};
use crate::storage::local_storage::LocalStorage;
use crate::storage::storage_lock::{StorageLock, StorageLockKey};
use crate::storage::storage_manager::StorageManager;

// ─── TransactionData ───────────────────────────────────────────────────────────

/// 向 Storage/UndoBuffer 传递的轻量事务上下文（C++: `struct TransactionData`）。
///
/// 包含完成 MVCC 判断所需的最小信息，避免传入完整 `DuckTransaction&`。
#[derive(Debug, Clone, Copy)]
pub struct TransactionData {
    /// 写入时的事务 ID（C++: `transaction_t transaction_id`）。
    pub transaction_id: TransactionId,
    /// 事务开始时间（用于可见性判断）（C++: `transaction_t start_time`）。
    pub start_time: TransactionId,
}

impl TransactionData {
    /// 从 `DuckTransaction` 提取事务上下文（C++: `TransactionData(DuckTransaction&)`）。
    pub fn from_transaction(txn: &DuckTransaction) -> Self {
        Self {
            transaction_id: txn.transaction_id,
            start_time: txn.start_time,
        }
    }

    /// 直接指定 ID 构造（C++: `TransactionData(transaction_t, transaction_t)`）。
    pub fn new(transaction_id: TransactionId, start_time: TransactionId) -> Self {
        Self {
            transaction_id,
            start_time,
        }
    }
}

// ─── SequenceValue ─────────────────────────────────────────────────────────────

/// 事务中序列的当前值快照（C++: `SequenceValue`）。
///
/// 每次 `PushSequenceUsage` 时，将序列的最新 counter/usage_count 写入 Undo 日志，
/// 以便 Rollback 时恢复序列状态。
#[derive(Debug, Clone, Copy)]
pub struct SequenceValue {
    /// 累计使用次数（C++: `int64_t usage_count`）。
    pub usage_count: i64,
    /// 当前计数器值（C++: `int64_t counter`）。
    pub counter: i64,
}

// ─── DatabaseModificationType ──────────────────────────────────────────────────

/// 数据库修改类型标志位集合（C++: `enum class DatabaseModificationType`）。
///
/// `SetModifications` 依据这些标志决定是否需要在事务内持有共享 checkpoint 锁，
/// 防止 checkpoint 与正在进行的修改操作并发执行。
#[derive(Debug, Clone, Copy, Default)]
pub struct DatabaseModificationType(u32);

impl DatabaseModificationType {
    // ── 标志位常量 ───────────────────────────────────────────────────────────

    pub const INSERT_DATA_WITH_INDEX: u32 = 1 << 0;
    pub const DELETE_DATA: u32 = 1 << 1;
    pub const UPDATE_DATA: u32 = 1 << 2;
    pub const ALTER_TABLE: u32 = 1 << 3;
    pub const CREATE_CATALOG_ENTRY: u32 = 1 << 4;
    pub const DROP_CATALOG_ENTRY: u32 = 1 << 5;
    pub const SEQUENCE: u32 = 1 << 6;
    pub const CREATE_INDEX: u32 = 1 << 7;

    // ── 构造 ─────────────────────────────────────────────────────────────────

    pub fn new(flags: u32) -> Self {
        Self(flags)
    }

    // ── 判断 ─────────────────────────────────────────────────────────────────

    /// 是否包含带索引的插入（C++: `InsertDataWithIndex()`）。
    pub fn insert_data_with_index(self) -> bool {
        self.0 & Self::INSERT_DATA_WITH_INDEX != 0
    }
    /// 是否包含删除（C++: `DeleteData()`）。
    pub fn delete_data(self) -> bool {
        self.0 & Self::DELETE_DATA != 0
    }
    /// 是否包含更新（C++: `UpdateData()`）。
    pub fn update_data(self) -> bool {
        self.0 & Self::UPDATE_DATA != 0
    }
    /// 是否包含 ALTER TABLE（C++: `AlterTable()`）。
    pub fn alter_table(self) -> bool {
        self.0 & Self::ALTER_TABLE != 0
    }
    /// 是否包含创建 Catalog 条目（C++: `CreateCatalogEntry()`）。
    pub fn create_catalog_entry(self) -> bool {
        self.0 & Self::CREATE_CATALOG_ENTRY != 0
    }
    /// 是否包含删除 Catalog 条目（C++: `DropCatalogEntry()`）。
    pub fn drop_catalog_entry(self) -> bool {
        self.0 & Self::DROP_CATALOG_ENTRY != 0
    }
    /// 是否包含序列操作（C++: `Sequence()`）。
    pub fn sequence(self) -> bool {
        self.0 & Self::SEQUENCE != 0
    }
    /// 是否包含创建索引（C++: `CreateIndex()`）。
    pub fn create_index(self) -> bool {
        self.0 & Self::CREATE_INDEX != 0
    }

    /// 是否需要持有写锁（任一写操作标志为真）。
    pub fn requires_write_lock(self) -> bool {
        self.insert_data_with_index()
            || self.delete_data()
            || self.update_data()
            || self.alter_table()
            || self.create_catalog_entry()
            || self.drop_catalog_entry()
            || self.sequence()
            || self.create_index()
    }
}

// ─── CheckpointLock ────────────────────────────────────────────────────────────

/// 按表缓存的 Checkpoint 共享锁（C++: `shared_ptr<CheckpointLock>`）。
///
/// `SharedLockTable` 返回 `Arc<CheckpointLock>` 给调用方持有；
/// 若事务内对同一张表再次调用 `SharedLockTable`，则返回已有的同一个 `Arc`，
/// 无需重复申请锁，保证每个事务对每张表只持有一把锁。
pub struct CheckpointLock {
    /// 内部的 Storage 共享锁（C++: `StorageLockKey` with `Shared` type）。
    _key: Box<StorageLockKey>,
}

impl CheckpointLock {
    fn new(key: Box<StorageLockKey>) -> Self {
        Self { _key: key }
    }
}

// ─── ActiveTableLock ───────────────────────────────────────────────────────────

/// 每张表的锁缓存条目（C++: `struct ActiveTableLock`）。
///
/// C++ 中：`weak_ptr<CheckpointLock> checkpoint_lock` + `mutex checkpoint_lock_mutex`。
/// Rust 中：`Weak<CheckpointLock>` + `Mutex<()>` 保护访问。
struct ActiveTableLock {
    /// 对已分配 `CheckpointLock` 的弱引用（过期 = 锁已被释放）。
    checkpoint_lock: std::sync::Weak<CheckpointLock>,
    /// 防止并发 acquire（C++: `mutex checkpoint_lock_mutex`）。
    checkpoint_lock_mutex: parking_lot::Mutex<()>,
}

impl ActiveTableLock {
    fn new() -> Self {
        Self {
            checkpoint_lock: std::sync::Weak::new(),
            checkpoint_lock_mutex: parking_lot::Mutex::new(()),
        }
    }
}

// ─── DuckTransaction ───────────────────────────────────────────────────────────

/// DuckDB 原生事务（C++: `class DuckTransaction`）。
pub struct DuckTransaction {
    // ── 事务标识 ──────────────────────────────────────────────────────────────
    /// 事务的 start_time（用于 MVCC 可见性判断）（C++: `transaction_t start_time`）。
    pub start_time: TransactionId,

    /// 事务的写入 ID（C++: `transaction_t transaction_id`）。
    pub transaction_id: TransactionId,

    /// 提交后设置的 commit_id（提交前为 NOT_DELETED_ID）（C++: `transaction_t commit_id`）。
    pub commit_id: TransactionId,

    /// 事务开始时的 catalog 版本号（C++: `atomic<idx_t> catalog_version`）。
    pub catalog_version: AtomicU64,

    /// 是否等待 cleanup 调度（C++: `bool awaiting_cleanup`）。
    pub awaiting_cleanup: bool,

    // ── 可见性 / 读写状态 ──────────────────────────────────────────────────────
    /// 是否只读（C++: `bool is_read_only` in Transaction base）。
    ///
    /// 使用 `AtomicBool` 以支持通过 `Arc<dyn Transaction>` 调用 `set_read_write(&self)`。
    is_read_only: AtomicBool,

    /// 当前活跃查询 ID（C++: `atomic<transaction_t> active_query` in Transaction base）。
    active_query: AtomicU64,

    // ── 撤销日志 ──────────────────────────────────────────────────────────────
    /// MVCC 撤销日志（C++: `UndoBuffer undo_buffer`）。
    pub undo_buffer: UndoBuffer,

    // ── 本地未提交数据 ─────────────────────────────────────────────────────────
    /// 本地 Append 缓冲（C++: `unique_ptr<LocalStorage> storage`）。
    pub storage: Box<LocalStorage>,

    // ── 锁 ────────────────────────────────────────────────────────────────────
    /// Checkpoint 写锁（Shared 类型）（C++: `unique_ptr<StorageLockKey> write_lock`）。
    ///
    /// 非 None 时，表示事务已持有 checkpoint 锁，防止并发 checkpoint。
    write_lock: Option<Box<StorageLockKey>>,

    // ── 序列使用快照 ───────────────────────────────────────────────────────────
    /// 序列 ID → 本事务快照值（C++: `reference_map_t<SequenceCatalogEntry, SequenceValue>`）。
    ///
    /// 受 `sequence_lock` 保护，以支持并发 PushSequenceUsage。
    sequence_usage: Mutex<HashMap<u64, SequenceValue>>,

    // ── 修改的表（持有 ID 集合防止错误调度）──────────────────────────────────
    /// 被本事务修改的表 ID 集合（C++: `reference_map_t<DataTable, shared_ptr<DataTable>>`）。
    ///
    /// C++ 持有 `shared_ptr<DataTable>` 防止表在事务提交前被析构；
    /// Rust 暂以 `u64` 表 ID 替代，待 DataTable 完整实现后再持有 `Arc<DataTable>`。
    modified_tables: Mutex<HashSet<u64>>,

    // ── 按表的 Checkpoint 锁缓存 ───────────────────────────────────────────────
    /// 表 ID → 活跃 Checkpoint 锁缓存（C++: `reference_map_t<DataTableInfo, ActiveTableLock>`）。
    ///
    /// 受 `active_locks_lock` 保护。
    active_locks: Mutex<HashMap<u64, Box<ActiveTableLock>>>,
}

impl DuckTransaction {
    /// 构造新事务（C++: `DuckTransaction::DuckTransaction(…)`）。
    ///
    /// C++ 还接受 `DuckTransactionManager&` 和 `ClientContext&`；
    /// 此处简化为仅接受所需的值参数，管理器在调用方持有。
    pub fn new(
        start_time: TransactionId,
        transaction_id: TransactionId,
        catalog_version: Idx,
        block_alloc_size: u64,
    ) -> Self {
        Self {
            start_time,
            transaction_id,
            commit_id: NOT_DELETED_ID,
            catalog_version: AtomicU64::new(catalog_version),
            awaiting_cleanup: false,
            is_read_only: AtomicBool::new(true),
            active_query: AtomicU64::new(MAXIMUM_QUERY_ID),
            undo_buffer: UndoBuffer::new(block_alloc_size),
            storage: Box::new(LocalStorage::new()),
            write_lock: None,
            sequence_usage: Mutex::new(HashMap::new()),
            modified_tables: Mutex::new(HashSet::new()),
            active_locks: Mutex::new(HashMap::new()),
        }
    }

    // ── 内部辅助 ──────────────────────────────────────────────────────────────

    /// 记录本事务修改了某张表（C++: `DuckTransaction::ModifyTable()`）。
    ///
    /// 若该表已在 `modified_tables` 中则忽略（幂等）。
    /// C++ 同时持有 `shared_ptr<DataTable>` 防止表被提前析构；
    /// Rust 暂存 `u64` 表 ID，待 DataTable 完整实现后替换。
    pub fn modify_table(&self, table_id: u64) {
        let mut tables = self.modified_tables.lock();
        tables.insert(table_id);
    }

    // ── UndoBuffer 写入接口（供 Storage 层调用）────────────────────────────────

    /// 记录 Catalog 条目变更（C++: `DuckTransaction::PushCatalogEntry()`）。
    ///
    /// 布局（载荷）：`catalog_entry_id(8B) | extra_data_size(8B) | extra_data(N B)`
    ///
    /// C++ 实际存储的是 `CatalogEntry*` 指针（8 字节），Rust 用 catalog_entry_id 代替。
    pub fn push_catalog_entry(&mut self, catalog_entry_id: u64, extra_data: &[u8]) {
        // alloc_size = sizeof(CatalogEntry*) + (extra_data_size > 0 ? sizeof(idx_t) + extra_data_size : 0)
        let alloc_size = 8 + if extra_data.is_empty() {
            0
        } else {
            8 + extra_data.len()
        };

        let entry_ref = self
            .undo_buffer
            .create_entry(UndoFlags::CatalogEntry, alloc_size);
        // 载荷格式：catalog_entry_id(8B) [+ extra_data_size(8B) + extra_data(N B)]
        // 注：实际写入操作待 UndoBufferReference 提供写接口后填充。
        let _ = (entry_ref, catalog_entry_id, extra_data);
    }

    /// 记录 ATTACH DATABASE 操作（C++: `DuckTransaction::PushAttach()`）。
    ///
    /// 载荷：`database_id(8B)`（C++ 存 `AttachedDatabase*` 指针，Rust 用 ID 代替）。
    pub fn push_attach(&mut self, database_id: u64) {
        let entry_ref = self.undo_buffer.create_entry(UndoFlags::Attach, 8);
        let _ = (entry_ref, database_id);
    }

    /// 记录一次 Delete 操作到 Undo 日志（C++: `DuckTransaction::PushDelete()`）。
    ///
    /// 同时调用 `modify_table` 记录被修改的表。
    ///
    /// 载荷格式见 [`DeleteInfo::serialized_size`]。
    pub fn push_delete(&mut self, info: DeleteInfo) {
        self.modify_table(info.table_id);

        let alloc_size = info.serialized_size();
        let entry_ref = self
            .undo_buffer
            .create_entry(UndoFlags::DeleteTuple, alloc_size);
        // TODO: 将 info 序列化写入 entry_ref.payload_mut()
        let _ = (entry_ref, info);
    }

    /// 记录一次 Append 操作到 Undo 日志（C++: `DuckTransaction::PushAppend()`）。
    ///
    /// 同时调用 `modify_table` 记录被修改的表。
    ///
    /// 载荷格式：`table_id(8B) | start_row(8B) | count(8B)` == [`AppendInfo::serialized_size`]。
    pub fn push_append(&mut self, table_id: u64, start_row: Idx, row_count: Idx) {
        self.modify_table(table_id);

        let alloc_size = AppendInfo::serialized_size();
        let entry_ref = self.undo_buffer.create_entry(UndoFlags::Append, alloc_size);
        // TODO: 将 AppendInfo { table_id, start_row, count: row_count } 序列化写入 entry_ref.payload_mut()
        let info = AppendInfo {
            table_id,
            start_row,
            count: row_count,
        };
        let mut payload = vec![0u8; alloc_size];
        info.serialize(&mut payload);
        self.undo_buffer.write_payload(
            entry_ref
                .slab_index
                .expect("Append undo entry must have a slab index"),
            entry_ref.position,
            &payload,
        );
    }

    /// 在 Undo 日志中为 UpdateInfo 预留空间，返回可写引用
    /// （C++: `DuckTransaction::CreateUpdateInfo()`）。
    ///
    /// 分配大小由 `UpdateInfo::GetAllocSize(type_size)` 确定（固定头 + `max * (sizeof(sel_t) + type_size)`）。
    pub fn create_update_info(
        &mut self,
        type_size: usize,
        table_id: u64,
        entries: usize,
        row_group_start: Idx,
    ) -> super::undo_buffer_allocator::UndoBufferReference {
        // alloc_size = UpdateInfo 固定头 + entries * (sizeof(sel_t=4) + type_size)
        let alloc_size = std::mem::size_of::<u64>() * 6 // 固定字段（segment_id, table_id, column_index, row_group_start, vector_index, version_number）
            + std::mem::size_of::<u64>() * 2             // prev + next (UndoBufferPointer)
            + std::mem::size_of::<u16>() * 2             // n, max
            + entries * (4 + type_size); // tuples(sel_t) + values

        let entry_ref = self
            .undo_buffer
            .create_entry(UndoFlags::UpdateTuple, alloc_size);
        // TODO: 在 entry_ref 所指载荷中初始化 UpdateInfo 固定字段：
        //   table_id, transaction_id=self.transaction_id, row_group_start
        let _ = (table_id, entries, row_group_start);
        entry_ref
    }

    pub fn push_update_payload(&mut self, payload: Vec<u8>) {
        self.modify_table(u64::from_le_bytes(
            payload[8..16]
                .try_into()
                .expect("UpdateInfo payload must include table_id"),
        ));
        let entry_ref = self
            .undo_buffer
            .create_entry(UndoFlags::UpdateTuple, payload.len());
        self.undo_buffer.write_payload(
            entry_ref
                .slab_index
                .expect("Update undo entry must have a slab index"),
            entry_ref.position,
            &payload,
        );
    }

    /// 记录序列使用（C++: `DuckTransaction::PushSequenceUsage()`）。
    ///
    /// 若本事务首次使用该序列，则在 Undo 日志中写入 `SequenceValue` 条目；
    /// 若已有记录，则更新 `sequence_usage` map 中的值（原地更新，无需新条目）。
    ///
    /// 载荷格式：`sequence_id(8B) | usage_count(8B) | counter(8B)`
    pub fn push_sequence_usage(&mut self, sequence_id: u64, value: SequenceValue) {
        let mut usage = self.sequence_usage.lock();
        if let Some(existing) = usage.get_mut(&sequence_id) {
            // 序列已在本事务中使用过：只更新 map，Undo 日志条目不变（回滚时用原始值）
            // C++: sequence_info.usage_count = data.usage_count; sequence_info.counter = data.counter;
            existing.usage_count = value.usage_count;
            existing.counter = value.counter;
        } else {
            // 首次使用：写入 Undo 日志并记录到 map
            // 载荷：sequence_id(8B) | usage_count(8B) | counter(8B) = 24 字节
            let entry_ref = self.undo_buffer.create_entry(UndoFlags::SequenceValue, 24);
            // TODO: 序列化 sequence_id, value.usage_count, value.counter 到 entry_ref.payload_mut()
            let _ = (entry_ref, sequence_id);
            usage.insert(sequence_id, value);
        }
    }

    // ── 查询接口 ──────────────────────────────────────────────────────────────

    /// 是否有任何写入（C++: `DuckTransaction::ChangesMade()`）。
    ///
    /// 包含 undo_buffer 和 local_storage 两个维度。
    pub fn changes_made(&self) -> bool {
        self.undo_buffer.changes_made() || self.storage.changes_made()
    }

    /// 统计 Undo 日志属性（C++: `DuckTransaction::GetUndoProperties()`）。
    ///
    /// `estimated_size` 还需叠加 `LocalStorage::EstimatedSize()`。
    pub fn get_undo_properties(&self) -> UndoBufferProperties {
        let mut properties = self.undo_buffer.get_properties();
        properties.estimated_size += self.storage.estimated_size();
        properties
    }

    // ── Checkpoint 相关 ────────────────────────────────────────────────────────

    /// 当前事务是否需要在数据库上触发自动 Checkpoint（C++: `DuckTransaction::AutomaticCheckpoint()`）。
    ///
    /// 参数：
    /// 判断是否应触发自动 checkpoint（C++: `DuckTransaction::AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties)`）。
    ///
    /// # 参数
    /// - `db`: 数据库实例引用（C++: `AttachedDatabase &db`）
    /// - `properties`: Undo 缓冲区属性（C++: `const UndoBufferProperties &properties`）
    ///
    /// # 返回
    /// 如果应该触发自动 checkpoint 返回 `true`，否则返回 `false`
    ///
    /// # C++ 实现
    /// ```cpp
    /// bool DuckTransaction::AutomaticCheckpoint(AttachedDatabase &db, const UndoBufferProperties &properties) {
    ///     if (!ChangesMade()) {
    ///         // read-only transactions cannot trigger an automated checkpoint
    ///         return false;
    ///     }
    ///     if (db.IsReadOnly()) {
    ///         // when attaching a database in read-only mode we cannot checkpoint
    ///         return false;
    ///     }
    ///     auto &storage_manager = db.GetStorageManager();
    ///     return storage_manager.AutomaticCheckpoint(properties.estimated_size);
    /// }
    /// ```
    pub fn automatic_checkpoint(
        &self,
        db: &Arc<crate::db::connection::DatabaseInstance>,
        properties: &UndoBufferProperties,
    ) -> bool {
        // C++: if (!ChangesMade()) { return false; }
        if !self.changes_made() {
            // 只读事务不触发自动 checkpoint
            return false;
        }

        // C++: if (db.IsReadOnly()) { return false; }
        // TODO: 从 db 获取 is_read_only 标志
        // if db.is_read_only { return false; }

        // C++: auto &storage_manager = db.GetStorageManager();
        //      return storage_manager.AutomaticCheckpoint(properties.estimated_size);
        db.storage_manager
            .automatic_checkpoint(properties.estimated_size as u64)
    }

    /// 是否需要将本次提交写入 WAL（C++: `DuckTransaction::ShouldWriteToWAL()`）。
    ///
    /// 参数：
    /// - `is_system_db`：数据库是否为系统数据库（C++: `db.IsSystem()`）。
    /// - `has_wal`：存储管理器是否存在 WAL（C++: `storage_manager.HasWAL()`）。
    pub fn should_write_to_wal(&self, is_system_db: bool, has_wal: bool) -> bool {
        if !self.changes_made() {
            return false;
        }
        if is_system_db {
            return false;
        }
        has_wal
    }

    /// 设置修改类型，按需获取 Checkpoint 共享锁（C++: `DuckTransaction::SetModifications()`）。
    ///
    /// 若已持有写锁则直接返回（幂等）。
    /// 若需要写锁，则通过 `checkpoint_lock.get_shared_lock()` 获取共享 Checkpoint 锁，
    /// 防止事务运行期间发生并发 checkpoint。
    ///
    /// C++ 通过 `GetTransactionManager().SharedCheckpointLock()` 获取锁；
    /// Rust 改为传入 `&StorageLock` 参数，避免循环引用。
    pub fn set_modifications(
        &mut self,
        modification_type: DatabaseModificationType,
        checkpoint_lock: &StorageLock,
    ) {
        if self.write_lock.is_some() {
            // 已有写锁，无需重复获取
            return;
        }
        if modification_type.requires_write_lock() {
            // 获取 Checkpoint 共享锁，防止 checkpoint 与本事务并发
            self.write_lock = Some(checkpoint_lock.get_shared_lock());
        }
    }

    /// 尝试获取 Checkpoint 独占锁（C++: `DuckTransaction::TryGetCheckpointLock()`）。
    ///
    /// - 若事务没有 write_lock：尝试直接获取独占锁。
    /// - 若事务已有 write_lock（Shared）：尝试将其升级为独占锁。
    ///
    /// C++ 通过 `GetTransactionManager().TryGetCheckpointLock()` /
    /// `TryUpgradeCheckpointLock(*write_lock)` 实现。
    pub fn try_get_checkpoint_lock(
        &mut self,
        checkpoint_lock: &StorageLock,
    ) -> Option<Box<StorageLockKey>> {
        match &self.write_lock {
            None => checkpoint_lock.try_get_exclusive_lock(),
            Some(existing) => checkpoint_lock.try_upgrade_checkpoint_lock(existing.as_ref()),
        }
    }

    /// 是否持有写锁（C++: `HasWriteLock()`）。
    pub fn has_write_lock(&self) -> bool {
        self.write_lock.is_some()
    }

    /// 为指定表获取（或复用）Checkpoint 共享锁（C++: `DuckTransaction::SharedLockTable()`）。
    ///
    /// 每个事务对同一张表只持有一把 `CheckpointLock`（Weak 缓存）。
    /// 若缓存已过期（锁被外部释放）则重新获取。
    ///
    /// C++ 原型：
    /// ```cpp
    /// shared_ptr<CheckpointLock> SharedLockTable(DataTableInfo& info);
    /// ```
    ///
    /// Rust 以 `table_info_id: u64` 替代 `DataTableInfo&`，
    /// `data_table_shared_lock` 代替 `info.GetSharedLock()`。
    pub fn shared_lock_table(
        &mut self,
        table_info_id: u64,
        data_table_shared_lock: Box<StorageLockKey>,
    ) -> Arc<CheckpointLock> {
        // ── 步骤 1：从事务级 map 查找或插入 ActiveTableLock ──────────────────
        // C++: unique_lock<mutex> transaction_lock(active_locks_lock);
        //      auto entry = active_locks.find(info);
        //      if (entry == active_locks.end()) {
        //          entry = active_locks.insert(entry, {ref(info), make_uniq<ActiveTableLock>()});
        //      }
        let mut active_locks = self.active_locks.lock();
        let active_table_lock = active_locks
            .entry(table_info_id)
            .or_insert_with(|| Box::new(ActiveTableLock::new()));

        // ── 步骤 2：在 table 级锁保护下获取 CheckpointLock ──────────────────
        // C++: transaction_lock.unlock(); // 先释放事务锁
        //      lock_guard<mutex> table_lock(active_table_lock.checkpoint_lock_mutex);
        //      auto checkpoint_lock = active_table_lock.checkpoint_lock.lock(); // weak_ptr::lock()
        //      if (checkpoint_lock) { return checkpoint_lock; }    // 未过期，直接返回
        //      checkpoint_lock = make_shared_ptr<CheckpointLock>(info.GetSharedLock());
        //      active_table_lock.checkpoint_lock = checkpoint_lock;
        //      return checkpoint_lock;
        let _table_guard = active_table_lock.checkpoint_lock_mutex.lock();

        // 尝试升级弱引用
        if let Some(existing) = active_table_lock.checkpoint_lock.upgrade() {
            return existing;
        }

        // 弱引用已过期：用调用方传入的 key 创建新的 CheckpointLock
        let new_lock = Arc::new(CheckpointLock::new(data_table_shared_lock));
        active_table_lock.checkpoint_lock = Arc::downgrade(&new_lock);
        new_lock
    }

    // ── 写入 WAL ──────────────────────────────────────────────────────────────

    /// 将本事务的修改写入 WAL（C++: `DuckTransaction::WriteToWAL()`）。
    ///
    /// 流程（noexcept，错误通过返回值传递）：
    /// 1. `storage.Commit(commit_state)` — 将本地 append flush 到 WAL。
    /// 2. `undo_buffer.WriteToWAL(*wal, commit_state)` — 写 catalog/delete/update 变更。
    /// 3. 若 commit_state 有 RowGroup 数据，需 `block_manager.FileSync()`。
    ///
    /// 若任一步骤出错，自动调用 `commit_state.RevertCommit()` 撤销已写 WAL。
    ///
    /// # 参数
    /// - `storage_manager`: 存储管理器引用，用于获取 WAL 和 BlockManager
    /// - `tables`: 表 ID → DataTable 的映射，用于 LocalStorage 提交
    ///
    /// # 返回
    /// - `Ok(commit_state)`: 成功，返回提交状态供后续 FlushCommit 使用
    /// - `Err(msg)`: 失败，commit_state 已自动回滚
    ///
    /// # C++ 源码对应
    /// ```cpp
    /// ErrorData DuckTransaction::WriteToWAL(ClientContext &context, AttachedDatabase &db,
    ///                                       unique_ptr<StorageCommitState> &commit_state) noexcept {
    ///     ErrorData error_data;
    ///     try {
    ///         D_ASSERT(ShouldWriteToWAL(db));
    ///         auto &storage_manager = db.GetStorageManager();
    ///         auto wal = storage_manager.GetWAL();
    ///         commit_state = storage_manager.GenStorageCommitState(*wal);
    ///
    ///         auto &profiler = *context.client_data->profiler;
    ///         auto commit_timer = profiler.StartTimer(MetricType::COMMIT_LOCAL_STORAGE_LATENCY);
    ///         storage->Commit(commit_state.get());
    ///
    ///         auto wal_timer = profiler.StartTimer(MetricType::WRITE_TO_WAL_LATENCY);
    ///         undo_buffer.WriteToWAL(*wal, commit_state.get());
    ///         if (commit_state->HasRowGroupData()) {
    ///             storage_manager.GetBlockManager().FileSync();
    ///         }
    ///     } catch (std::exception &ex) {
    ///         error_data = ErrorData(ex);
    ///     }
    ///
    ///     if (commit_state && error_data.HasError()) {
    ///         try {
    ///             commit_state->RevertCommit();
    ///             commit_state.reset();
    ///         } catch (std::exception &) {
    ///             // Ignore this error. If we fail to RevertCommit(), just return the original exception
    ///         }
    ///     }
    ///
    ///     return error_data;
    /// }
    /// ```
    pub fn write_to_wal(
        &mut self,
        storage_manager: &dyn StorageManager,
        tables: &HashMap<u64, Arc<crate::storage::data_table::DataTable>>,
    ) -> Result<Box<dyn crate::storage::storage_manager::StorageCommitState>, String> {
        // C++: D_ASSERT(ShouldWriteToWAL(db));
        // 调用方应在调用前检查 should_write_to_wal()

        // C++: auto &storage_manager = db.GetStorageManager();
        //      auto wal = storage_manager.GetWAL();
        //      commit_state = storage_manager.GenStorageCommitState(*wal);
        let mut commit_state = storage_manager.gen_storage_commit_state().ok_or_else(|| {
            "Failed to generate StorageCommitState (WAL not available)".to_string()
        })?;

        // 使用 catch_unwind 捕获 panic，模拟 C++ 的 try-catch
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // C++: storage->Commit(commit_state.get());
            // 将本地 Append 数据 flush 到 WAL
            let append_entries = self
                .storage
                .commit(tables)
                .map_err(|e| format!("LocalStorage commit failed: {:?}", e))?;
            for (table_id, row_start, row_count) in append_entries {
                self.push_append(table_id, row_start, row_count);
            }

            // C++: undo_buffer.WriteToWAL(*wal, commit_state.get());
            // 将 Undo 日志（catalog/delete/update）写入 WAL
            self.undo_buffer
                .write_to_wal(commit_state.as_mut(), tables)
                .map_err(|e| format!("UndoBuffer write_to_wal failed: {:?}", e))?;

            // C++: if (commit_state->HasRowGroupData()) {
            //          storage_manager.GetBlockManager().FileSync();
            //      }
            // 若有乐观写入的 RowGroup 数据，需要 fsync 块文件
            if commit_state.has_row_group_data() {
                // 注意：BlockManager::FileSync() 需要在 StorageManager trait 中暴露
                // 当前简化实现：跳过 FileSync（假设 WAL flush 时会同步）
                // TODO: 添加 storage_manager.file_sync() 方法
            }

            Ok::<_, String>(())
        }));

        // C++: if (commit_state && error_data.HasError()) {
        //          try { commit_state->RevertCommit(); commit_state.reset(); }
        //          catch (std::exception &) { /* Ignore */ }
        //      }
        match result {
            Ok(Ok(())) => {
                // 成功：返回 commit_state 供调用方 FlushCommit
                Ok(commit_state)
            }
            Ok(Err(e)) => {
                // 失败：回滚 commit_state（截断 WAL）
                commit_state.revert_commit();
                Err(e)
            }
            Err(_) => {
                // Panic：回滚 commit_state（截断 WAL）
                commit_state.revert_commit();
                Err("WriteToWAL panicked".to_string())
            }
        }
    }

    // ── 生命周期 ──────────────────────────────────────────────────────────────

    /// 提交（C++: `DuckTransaction::Commit()`）。
    ///
    /// 流程（noexcept）：
    /// 1. 设置 `commit_id`。
    /// 2. 若无修改直接返回 Ok。
    /// 3. `storage.Commit(commit_state)` — flush local storage。
    /// 4. `undo_buffer.Commit(iterator_state, commit_info)` — 设置版本号。
    /// 5. 若有 commit_state：`commit_state.FlushCommit()`。
    ///
    /// 出错时：`undo_buffer.RevertCommit(iterator_state, transaction_id)`，
    ///          以及 `commit_state.RevertCommit()`。
    ///
    /// # 参数
    /// - `commit_info`: 包含 commit_id 和 active_transactions 的提交信息
    /// - `commit_state`: 可选的存储提交状态，用于 WAL 写入
    /// - `tables`: 表 ID → DataTable 的映射，用于将本地数据合并到全局存储
    ///
    /// # C++ 源码对应
    /// ```cpp
    /// ErrorData DuckTransaction::Commit(AttachedDatabase &db, CommitInfo &commit_info,
    ///                                   unique_ptr<StorageCommitState> commit_state) noexcept {
    ///     this->commit_id = commit_info.commit_id;
    ///     if (!ChangesMade()) {
    ///         return ErrorData();
    ///     }
    ///     D_ASSERT(db.IsSystem() || db.IsTemporary() || !IsReadOnly());
    ///
    ///     UndoBuffer::IteratorState iterator_state;
    ///     try {
    ///         storage->Commit(commit_state.get());
    ///         undo_buffer.Commit(iterator_state, commit_info);
    ///         if (commit_state) {
    ///             commit_state->FlushCommit();
    ///         }
    ///         return ErrorData();
    ///     } catch (std::exception &ex) {
    ///         undo_buffer.RevertCommit(iterator_state, this->transaction_id);
    ///         if (commit_state) {
    ///             commit_state->RevertCommit();
    ///         }
    ///         return ErrorData(ex);
    ///     }
    /// }
    /// ```
    pub fn commit(
        &mut self,
        commit_info: CommitInfo,
        commit_state: Option<&mut Box<dyn crate::storage::storage_manager::StorageCommitState>>,
        tables: &HashMap<u64, Arc<crate::storage::data_table::DataTable>>,
    ) -> Result<(), String> {
        // C++: this->commit_id = commit_info.commit_id;
        self.commit_id = commit_info.commit_id;

        // C++: if (!ChangesMade()) { return ErrorData(); }
        if !self.changes_made() {
            // 只读事务：无需 flush 任何内容
            return Ok(());
        }

        // C++: D_ASSERT(db.IsSystem() || db.IsTemporary() || !IsReadOnly());
        // 注意：Rust 版本中这个断言在调用方进行

        // C++: UndoBuffer::IteratorState iterator_state;
        let mut iterator_state = IteratorState::default();

        // C++: try { ... } catch (std::exception &ex) { ... }
        // Rust 中使用 match 处理可能的错误

        // 步骤 1: 提交本地存储
        // C++: storage->Commit(commit_state.get());
        let append_entries = match self.storage.commit(tables) {
            Ok(entries) => entries,
            Err(e) => {
                // 提交失败，回滚 UndoBuffer
                // C++: undo_buffer.RevertCommit(iterator_state, this->transaction_id);
                self.undo_buffer
                    .revert_commit(&iterator_state, self.transaction_id);

                // C++: if (commit_state) { commit_state->RevertCommit(); }
                if let Some(cs) = commit_state {
                    cs.revert_commit();
                }

                return Err(format!("LocalStorage commit failed: {:?}", e));
            }
        };
        for (table_id, row_start, row_count) in append_entries {
            self.push_append(table_id, row_start, row_count);
        }

        // 步骤 2: 提交 UndoBuffer（设置版本号）
        // C++: undo_buffer.Commit(iterator_state, commit_info);
        // 注意：undo_buffer.commit 可能失败，需要捕获异常
        // 当前 Rust 实现中 commit 不会返回错误，但我们需要支持 revert_commit
        let undo_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.undo_buffer.commit(&mut iterator_state, commit_info);
        }));

        if let Err(_e) = undo_result {
            // Undo 提交失败（panic），需要回滚
            self.undo_buffer
                .revert_commit(&iterator_state, self.transaction_id);

            if let Some(cs) = commit_state {
                cs.revert_commit();
            }

            return Err("UndoBuffer commit panicked".to_string());
        }

        // 步骤 3: 刷新 WAL
        // C++: if (commit_state) { commit_state->FlushCommit(); }
        if let Some(cs) = commit_state {
            if let Err(e) = cs.flush_commit() {
                // Flush 失败，需要回滚
                self.undo_buffer
                    .revert_commit(&iterator_state, self.transaction_id);
                cs.revert_commit();

                return Err(format!("FlushCommit failed: {:?}", e));
            }
        }

        // C++: return ErrorData();
        Ok(())
    }

    /// 简化版提交（无 WAL 支持，无表合并）
    ///
    /// 用于事务管理器级别的提交，此时 tables 由调用方单独处理。
    /// 仅执行 UndoBuffer 提交，不执行 LocalStorage 合并。
    ///
    /// 注意：调用方需要确保 LocalStorage 已在调用前单独提交。
    pub fn commit_undo_only(&mut self, commit_info: CommitInfo) -> Result<(), String> {
        // C++: this->commit_id = commit_info.commit_id;
        self.commit_id = commit_info.commit_id;

        // C++: if (!ChangesMade()) { return ErrorData(); }
        if !self.changes_made() {
            return Ok(());
        }

        // C++: UndoBuffer::IteratorState iterator_state;
        let mut iterator_state = IteratorState::default();

        // 仅提交 UndoBuffer
        // C++: undo_buffer.Commit(iterator_state, commit_info);
        self.undo_buffer.commit(&mut iterator_state, commit_info);

        Ok(())
    }

    /// 回滚（C++: `DuckTransaction::Rollback()`）。
    ///
    /// 顺序：先回滚 local_storage（释放乐观写块），再回滚 undo_buffer（恢复 MVCC 版本号）。
    pub fn rollback(&mut self) -> Result<(), String> {
        // C++: storage->Rollback();
        self.storage.rollback();
        // C++: undo_buffer.Rollback();
        self.undo_buffer.rollback();
        Ok(())
    }

    /// 清理旧 MVCC 版本（C++: `DuckTransaction::Cleanup(transaction_t lowest_active_transaction)`）。
    ///
    /// 在所有可能看到旧版本的事务都已结束后，由 `DuckTransactionManager` 调度。
    pub fn cleanup(&self, lowest_active_transaction: TransactionId) {
        self.undo_buffer.cleanup(lowest_active_transaction)
    }
}

// ─── Transaction impl ──────────────────────────────────────────────────────────

impl Transaction for DuckTransaction {
    fn is_read_only(&self) -> bool {
        self.is_read_only.load(Ordering::Relaxed)
    }

    /// 通过 `AtomicBool` 实现，可从 `&self` 调用（支持通过 `Arc<dyn Transaction>` 使用）。
    fn set_read_write(&self) {
        self.is_read_only.store(false, Ordering::Relaxed);
    }

    fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    fn is_duck_transaction(&self) -> bool {
        true
    }

    fn active_query(&self) -> TransactionId {
        self.active_query.load(Ordering::Relaxed)
    }

    fn set_active_query(&self, query_id: TransactionId) {
        self.active_query.store(query_id, Ordering::Relaxed);
    }
}
