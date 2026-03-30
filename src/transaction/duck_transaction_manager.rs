//! DuckDB 原生事务管理器。
//!
//! 对应 C++:
//!   `duckdb/transaction/duck_transaction_manager.hpp`
//!   `src/transaction/duck_transaction_manager.cpp`
//!
//! # 职责
//!
//! - 分配单调递增的 `start_time` 和 `transaction_id`。
//! - 维护活跃事务列表（`active_transactions`）和已提交但待清理的列表（`recently_committed`）。
//! - 管理 `lowest_active_id` / `lowest_active_start`（供 MVCC Cleanup 使用）。
//! - 管理 `checkpoint_lock`，控制 checkpoint 与写事务的互斥。
//! - 提交时决策是否触发自动 checkpoint。
//! - 维护 Catalog 版本号（`last_committed_version` / `last_uncommitted_catalog_version`）。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `vector<unique_ptr<DuckTransaction>> active_transactions` | `Vec<Arc<DuckTxnHandle>>` |
//! | `vector<unique_ptr<DuckTransaction>> recently_committed_transactions` | `Vec<Arc<DuckTxnHandle>>` |
//! | `atomic<transaction_t> lowest_active_id` | `AtomicU64` |
//! | `mutex transaction_lock` | `Mutex<TransactionLockInner>` |
//! | `StorageLock checkpoint_lock` | `StorageLock` |
//! | `queue<unique_ptr<DuckCleanupInfo>> cleanup_queue` | `Mutex<CleanupQueueInner>` |
//! | `mutex cleanup_lock` / `cleanup_queue_lock` | 合并为两层 `Mutex`：执行锁 + 队列锁 |
//!
//! # 关键设计决策
//!
//! **DuckTxnHandle**：将 `DuckTransaction` 包裹在 `parking_lot::Mutex` 中，
//! 使 `Arc<DuckTxnHandle>` 可以同时被事务管理器（活跃列表）和调用方（TransactionRef）持有，
//! 且支持通过 Mutex 获取 `&mut DuckTransaction` 进行提交/回滚操作。
//!
//! **TransactionRef 定位**：`Transaction::transaction_id()` 方法让管理器在收到 `&TransactionRef`
//! 时，通过 transaction_id 在 `active_transactions` 中找到对应 `Arc<DuckTxnHandle>`。

use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::duck_transaction::{DuckTransaction, SequenceValue};
use super::transaction::{Transaction, TransactionRef};
use super::transaction_manager::{ErrorData, TransactionManager};
use super::types::{
    ActiveTransactionState, Idx, MAX_TRANSACTION_ID, TRANSACTION_ID_START, TransactionId,
};
use super::undo_buffer::CommitInfo;
use crate::storage::data_table::DataTable;
use crate::storage::storage_lock::{StorageLock, StorageLockKey};
use crate::storage::storage_manager::StorageManager;

// ─── CheckpointType ────────────────────────────────────────────────────────────

/// Checkpoint 的类型（C++: `enum class CheckpointType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointType {
    /// 完整 checkpoint：锁定所有写事务（C++: `FULL_CHECKPOINT`）。
    #[default]
    FullCheckpoint,
    /// 并发 checkpoint：允许其他活跃读事务（C++: `CONCURRENT_CHECKPOINT`）。
    ConcurrentCheckpoint,
    /// 仅对内存数据库做 vacuum（C++: `VACUUM_ONLY`）。
    VacuumOnly,
}

// ─── CheckpointDecision ────────────────────────────────────────────────────────

/// 提交时对是否触发自动 Checkpoint 的决策（C++: `struct CheckpointDecision`）。
#[derive(Debug)]
pub struct CheckpointDecision {
    /// 可以 checkpoint（C++: `bool can_checkpoint`）。
    pub can_checkpoint: bool,
    /// 不能 checkpoint 的原因（C++: `string reason`）。
    pub reason: String,
    /// checkpoint 类型（C++: `CheckpointType type`，仅 can_checkpoint=true 时有效）。
    pub checkpoint_type: CheckpointType,
}

impl CheckpointDecision {
    /// 可以做指定类型的 checkpoint。
    pub fn yes(checkpoint_type: CheckpointType) -> Self {
        Self {
            can_checkpoint: true,
            reason: String::new(),
            checkpoint_type,
        }
    }

    /// 不能做 checkpoint，附原因。
    pub fn no(reason: impl Into<String>) -> Self {
        Self {
            can_checkpoint: false,
            reason: reason.into(),
            checkpoint_type: CheckpointType::FullCheckpoint,
        }
    }
}

// ─── DbContext ─────────────────────────────────────────────────────────────────

/// 管理器在提交/回滚时所需的数据库上下文参数（代替 C++ `AttachedDatabase &db`）。
///
/// 由于 Rust 无法直接在 `TransactionManager` trait 中传递数据库引用，
/// 需要调用方将这些布尔属性传入。
#[derive(Clone, Default)]
pub struct DbContext {
    /// 是否为系统数据库（C++: `db.IsSystem()`）。
    pub is_system: bool,
    /// 是否为临时数据库（C++: `db.IsTemporary()`）。
    pub is_temporary: bool,
    /// 是否以只读模式挂载（C++: `db.IsReadOnly()`）。
    pub is_read_only: bool,
    /// Storage Manager 是否已完成加载（C++: `storage_manager.IsLoaded()`）。
    pub storage_loaded: bool,
    /// Storage Manager 是否为内存模式（C++: `storage_manager.InMemory()`）。
    pub storage_in_memory: bool,
    /// 是否启用了压缩（C++: `!storage_manager.CompressionIsEnabled()`）。
    pub compression_enabled: bool,
    /// 是否跳过提交时 checkpoint（调试开关）（C++: `DebugSkipCheckpointOnCommitSetting`）。
    pub debug_skip_checkpoint_on_commit: bool,
    /// 是否有 WAL（C++: `storage_manager.HasWAL()`）。
    pub has_wal: bool,
    /// 恢复模式是否为默认（C++: `db.GetRecoveryMode() == RecoveryMode::DEFAULT`）。
    pub recovery_mode_default: bool,
    /// Storage Manager 引用（用于 WAL 写入和 checkpoint）。
    pub storage_manager: Option<Arc<dyn crate::storage::storage_manager::StorageManager>>,
}

// ─── DuckTxnHandle ─────────────────────────────────────────────────────────────

/// `DuckTransaction` 的共享句柄（C++: `unique_ptr<DuckTransaction>` + 所有权共享）。
///
/// 将 `DuckTransaction` 包裹在 `Mutex` 中，使得：
/// - 事务管理器可在 `active_transactions` 中持有 `Arc<DuckTxnHandle>`；
/// - 调用方可持有 `Arc<DuckTxnHandle>` 作为 `TransactionRef`（`Arc<dyn Transaction>`）；
/// - 两者均可通过 `Mutex::lock()` 获得独占写访问。
pub struct DuckTxnHandle {
    inner: Mutex<DuckTransaction>,
}

impl DuckTxnHandle {
    fn wrap(txn: DuckTransaction) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(txn),
        })
    }

    /// 获取内部 DuckTransaction 的 MutexGuard。
    pub fn lock_inner(&self) -> parking_lot::MutexGuard<'_, DuckTransaction> {
        self.inner.lock()
    }

    /// 获取事务 ID（不进行完整锁定的快速路径）。
    ///
    /// `transaction_id` 在创建后不再修改，因此可以通过锁读取。
    fn transaction_id(&self) -> TransactionId {
        self.inner.lock().transaction_id
    }

    /// 获取 start_time。
    fn start_time(&self) -> TransactionId {
        self.inner.lock().start_time
    }

    /// 获取 commit_id（提交前为 NOT_DELETED_ID，提交后为实际 commit_id）。
    fn commit_id(&self) -> TransactionId {
        self.inner.lock().commit_id
    }

    /// 是否有任何修改（委托给 `DuckTransaction::changes_made()`）。
    fn changes_made(&self) -> bool {
        self.inner.lock().changes_made()
    }

    /// 是否正在等待 cleanup。
    fn awaiting_cleanup(&self) -> bool {
        self.inner.lock().awaiting_cleanup
    }

    /// 设置 awaiting_cleanup 标志。
    fn set_awaiting_cleanup(&self, val: bool) {
        self.inner.lock().awaiting_cleanup = val;
    }
}

impl Transaction for DuckTxnHandle {
    fn is_read_only(&self) -> bool {
        self.inner.lock().is_read_only()
    }

    fn set_read_write(&self) {
        self.inner.lock().set_read_write();
    }

    fn transaction_id(&self) -> TransactionId {
        self.transaction_id()
    }

    fn is_duck_transaction(&self) -> bool {
        true
    }

    fn active_query(&self) -> TransactionId {
        self.inner.lock().active_query()
    }

    fn set_active_query(&self, query_id: TransactionId) {
        self.inner.lock().set_active_query(query_id);
    }
}

// ─── DuckCleanupInfo ───────────────────────────────────────────────────────────

/// 一批待 cleanup 的事务（C++: `struct DuckCleanupInfo`）。
///
/// Cleanup 操作在释放 `transaction_lock` 后执行，保证同一批事务共享同一 `lowest_start_time`。
/// 同时确保 cleanup 顺序正确（按提交顺序清理）。
pub struct DuckCleanupInfo {
    /// 该批事务提交时最低的活跃 start_time（C++: `transaction_t lowest_start_time`）。
    pub lowest_start_time: TransactionId,
    /// 待 cleanup 的事务句柄列表（C++: `vector<unique_ptr<DuckTransaction>>`）。
    pub transactions: Vec<Arc<DuckTxnHandle>>,
}

impl DuckCleanupInfo {
    fn new() -> Self {
        Self {
            lowest_start_time: TRANSACTION_ID_START,
            transactions: Vec::new(),
        }
    }

    /// 执行 cleanup（C++: `DuckCleanupInfo::Cleanup() noexcept`）。
    ///
    /// 对每个设置了 `awaiting_cleanup` 的事务，调用 `DuckTransaction::cleanup()`。
    pub fn cleanup(&self) {
        for handle in &self.transactions {
            if handle.awaiting_cleanup() {
                handle.inner.lock().cleanup(self.lowest_start_time);
            }
        }
    }

    /// 是否有需要调度的 cleanup（C++: `DuckCleanupInfo::ScheduleCleanup() noexcept`）。
    pub fn schedule_cleanup(&self) -> bool {
        !self.transactions.is_empty()
    }
}

// ─── TransactionLockInner ──────────────────────────────────────────────────────

/// `transaction_lock` 保护的可变状态。
///
/// C++ 中这些字段分散在 `DuckTransactionManager` 中，均在 `lock_guard<mutex> lock(transaction_lock)` 下访问。
/// Rust 统一放入此结构以避免死锁风险和误用。
struct TransactionLockInner {
    /// 下一个分配的 start_time（C++: `transaction_t current_start_timestamp`，初始为 2）。
    current_start_timestamp: TransactionId,
    /// 下一个分配的 transaction_id（C++: `transaction_t current_transaction_id`）。
    current_transaction_id: TransactionId,
    /// 当前活跃事务列表（C++: `vector<unique_ptr<DuckTransaction>> active_transactions`）。
    active_transactions: Vec<Arc<DuckTxnHandle>>,
    /// 已提交但可能被其他事务读取旧版本的事务列表（C++: `recently_committed_transactions`）。
    ///
    /// 按 commit_id 升序排列。当 `commit_id < lowest_start_time` 时，可移入 cleanup。
    recently_committed_transactions: Vec<Arc<DuckTxnHandle>>,
    /// 最新已提交的 catalog 版本号（C++: `idx_t last_committed_version`）。
    last_committed_version: Idx,
}

impl TransactionLockInner {
    /// 分配新的 commit timestamp（C++: `DuckTransactionManager::GetCommitTimestamp()`）。
    ///
    /// C++: `return current_start_timestamp++` —— 复用 start_timestamp 序列。
    fn get_commit_timestamp(&mut self) -> TransactionId {
        let ts = self.current_start_timestamp;
        self.current_start_timestamp += 1;
        ts
    }

    /// 检查是否存在除 `exclude_id` 之外的其他活跃事务（C++: `HasOtherTransactions()`）。
    fn has_other_transactions(&self, exclude_id: TransactionId) -> bool {
        self.active_transactions
            .iter()
            .any(|h| h.transaction_id() != exclude_id)
    }

    /// 构建除 `exclude_id` 之外的其他活跃事务 ID 列表（调试信息用）。
    fn other_transaction_ids(&self, exclude_id: TransactionId) -> String {
        self.active_transactions
            .iter()
            .filter(|h| h.transaction_id() != exclude_id)
            .map(|h| format!("[{}]", h.transaction_id()))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

// ─── CleanupQueueInner ─────────────────────────────────────────────────────────

/// cleanup 队列保护的状态（C++: `cleanup_queue_lock` + `cleanup_queue`）。
struct CleanupQueueInner {
    /// 待执行的 cleanup 批次队列（C++: `queue<unique_ptr<DuckCleanupInfo>>`）。
    queue: VecDeque<DuckCleanupInfo>,
}

// ─── DuckTransactionManager ────────────────────────────────────────────────────

/// DuckDB 原生事务管理器（C++: `class DuckTransactionManager : public TransactionManager`）。
pub struct DuckTransactionManager {
    // ── 原子计数器（无锁快速读）──────────────────────────────────────────────
    /// 全局最低活跃 transaction_id（C++: `atomic<transaction_t> lowest_active_id`）。
    ///
    /// 初始值 = `TRANSACTION_ID_START`（无活跃事务时）。
    lowest_active_id: AtomicU64,

    /// 全局最低活跃 start_time（C++: `atomic<transaction_t> lowest_active_start`）。
    ///
    /// 初始值 = `MAX_TRANSACTION_ID`（无活跃事务时）。
    lowest_active_start: AtomicU64,

    /// 最后一次成功提交的 commit_id（C++: `atomic<transaction_t> last_commit`）。
    last_commit: AtomicU64,

    /// 当前活跃 checkpoint 的 commit_id，无则为 `MAX_TRANSACTION_ID`
    /// （C++: `atomic<transaction_t> active_checkpoint`）。
    active_checkpoint: AtomicU64,

    /// 最新未提交 catalog 版本（C++: `atomic<idx_t> last_uncommitted_catalog_version`）。
    ///
    /// 初始值 = `TRANSACTION_ID_START`（确保大于所有合法时间戳）。
    last_uncommitted_catalog_version: AtomicU64,

    // ── Mutex 保护的可变状态 ───────────────────────────────────────────────────
    /// 核心事务状态（C++: 多字段在 `transaction_lock` 下）。
    inner: Mutex<TransactionLockInner>,

    /// cleanup 队列（C++: `cleanup_queue_lock` + `cleanup_queue`）。
    cleanup_queue: Mutex<CleanupQueueInner>,

    /// 独占 cleanup 执行锁，确保每次只有一个 cleanup 在运行
    /// （C++: `mutex cleanup_lock`）。
    cleanup_exec_lock: Mutex<()>,

    /// 阻止新事务启动的锁，FORCE CHECKPOINT 时持有
    /// （C++: `mutex start_transaction_lock`）。
    start_transaction_lock: Mutex<()>,

    // ── Checkpoint 锁 ─────────────────────────────────────────────────────────
    /// Checkpoint 读写锁（C++: `StorageLock checkpoint_lock`）。
    ///
    /// 写事务持有共享锁（阻止并发 checkpoint），checkpoint 持有独占锁（等待所有写事务完成）。
    pub checkpoint_lock: StorageLock,
}

impl DuckTransactionManager {
    /// 构造新的事务管理器（C++: `DuckTransactionManager(AttachedDatabase &db)`）。
    ///
    /// C++ 要求 `db.GetCatalog().IsDuckCatalog()` 为真，否则抛出异常。
    /// Rust 版本不做此检查（调用方负责保证）。
    pub fn new() -> Self {
        Self {
            lowest_active_id: AtomicU64::new(TRANSACTION_ID_START),
            lowest_active_start: AtomicU64::new(MAX_TRANSACTION_ID),
            last_commit: AtomicU64::new(0),
            active_checkpoint: AtomicU64::new(MAX_TRANSACTION_ID),
            last_uncommitted_catalog_version: AtomicU64::new(TRANSACTION_ID_START),
            inner: Mutex::new(TransactionLockInner {
                current_start_timestamp: 2, // C++: start at 2
                current_transaction_id: TRANSACTION_ID_START,
                active_transactions: Vec::new(),
                recently_committed_transactions: Vec::new(),
                last_committed_version: 0,
            }),
            cleanup_queue: Mutex::new(CleanupQueueInner {
                queue: VecDeque::new(),
            }),
            cleanup_exec_lock: Mutex::new(()),
            start_transaction_lock: Mutex::new(()),
            checkpoint_lock: StorageLock::new(),
        }
    }

    // ── 原子计数器只读访问 ─────────────────────────────────────────────────────

    /// 全局最低活跃 transaction_id（C++: `LowestActiveId()`）。
    pub fn lowest_active_id(&self) -> TransactionId {
        self.lowest_active_id.load(Ordering::Acquire)
    }

    /// 全局最低活跃 start_time（C++: `LowestActiveStart()`）。
    pub fn lowest_active_start(&self) -> TransactionId {
        self.lowest_active_start.load(Ordering::Acquire)
    }

    /// 最后提交 ID（C++: `GetLastCommit()`）。
    pub fn last_commit(&self) -> TransactionId {
        self.last_commit.load(Ordering::Acquire)
    }

    /// 当前活跃 checkpoint ID（C++: `GetActiveCheckpoint()`）。
    pub fn active_checkpoint(&self) -> TransactionId {
        self.active_checkpoint.load(Ordering::Acquire)
    }

    // ── Checkpoint ID 管理 ────────────────────────────────────────────────────

    /// 分配新的 checkpoint ID，并记录为当前活跃 checkpoint
    /// （C++: `DuckTransactionManager::GetNewCheckpointId()`）。
    ///
    /// # Panics
    /// 若 `active_checkpoint != MAX_TRANSACTION_ID`（另一个 checkpoint 正在进行）。
    pub fn get_new_checkpoint_id(&self) -> TransactionId {
        let current = self.active_checkpoint.load(Ordering::Acquire);
        assert_eq!(
            current, MAX_TRANSACTION_ID,
            "DuckTransactionManager::GetNewCheckpointId: active_checkpoint already set to {}",
            current
        );
        let id = self.last_commit.load(Ordering::Acquire);
        self.active_checkpoint.store(id, Ordering::Release);
        id
    }

    /// 重置活跃 checkpoint ID 为 MAX（C++: `DuckTransactionManager::ResetCheckpointId()`）。
    pub fn reset_checkpoint_id(&self) {
        self.active_checkpoint
            .store(MAX_TRANSACTION_ID, Ordering::Release);
    }

    // ── Checkpoint 锁公共接口 ──────────────────────────────────────────────────

    /// 获取 Checkpoint 共享锁（写事务持有，阻止并发 checkpoint）
    /// （C++: `DuckTransactionManager::SharedCheckpointLock()`）。
    pub fn shared_checkpoint_lock(&self) -> Box<StorageLockKey> {
        self.checkpoint_lock.get_shared_lock()
    }

    /// 尝试将现有共享锁升级为独占锁
    /// （C++: `DuckTransactionManager::TryUpgradeCheckpointLock()`）。
    pub fn try_upgrade_checkpoint_lock(
        &self,
        lock: &StorageLockKey,
    ) -> Option<Box<StorageLockKey>> {
        self.checkpoint_lock.try_upgrade_checkpoint_lock(lock)
    }

    /// 尝试获取 Checkpoint 独占锁（非阻塞）
    /// （C++: `DuckTransactionManager::TryGetCheckpointLock()`）。
    pub fn try_get_checkpoint_lock(&self) -> Option<Box<StorageLockKey>> {
        self.checkpoint_lock.try_get_exclusive_lock()
    }

    // ── Catalog 版本 ──────────────────────────────────────────────────────────

    /// 返回当前已提交的 catalog 版本（C++: `GetCatalogVersion(Transaction&)`）。
    ///
    /// C++ 返回 `transaction.catalog_version`，但在这里我们返回管理器的全局版本。
    pub fn get_catalog_version(&self) -> Idx {
        self.inner.lock().last_committed_version
    }

    /// 返回当前活跃事务数量。
    pub fn active_transaction_count(&self) -> usize {
        self.inner.lock().active_transactions.len()
    }

    /// 检查是否存在除指定事务之外的其他活跃事务。
    pub fn has_other_transactions(&self, exclude_id: TransactionId) -> bool {
        self.inner.lock().has_other_transactions(exclude_id)
    }

    /// 获取提交时间戳。
    pub fn get_commit_timestamp(&self) -> TransactionId {
        self.inner.lock().get_commit_timestamp()
    }

    // ── Catalog 操作转发 ──────────────────────────────────────────────────────

    /// 向事务的 Undo 日志推入 Catalog 条目变更（C++: `DuckTransactionManager::PushCatalogEntry()`）。
    ///
    /// 仅系统/临时数据库之外的只读事务会触发错误。
    /// 每次调用都会递增 `last_uncommitted_catalog_version`，标记事务的 catalog 变更。
    pub fn push_catalog_entry(
        &self,
        transaction: &TransactionRef,
        db_ctx: DbContext,
        catalog_entry_id: u64,
        extra_data: &[u8],
    ) -> Result<(), ErrorData> {
        if !db_ctx.is_system && !db_ctx.is_temporary && transaction.is_read_only() {
            return Err(ErrorData::new(
                "Attempting to do catalog changes on a transaction that is read-only",
            ));
        }
        // 找到对应事务句柄
        let txn_id = transaction.transaction_id();
        let inner = self.inner.lock();
        let handle = inner
            .active_transactions
            .iter()
            .find(|h| h.transaction_id() == txn_id)
            .cloned()
            .ok_or_else(|| ErrorData::new("Transaction not found in active list"))?;
        drop(inner); // 释放 transaction_lock，避免与 sequence_lock 死锁

        // C++: transaction.catalog_version = ++last_uncommitted_catalog_version;
        let new_version = self
            .last_uncommitted_catalog_version
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        {
            let mut txn = handle.inner.lock();
            txn.catalog_version.store(new_version, Ordering::Relaxed);
            txn.push_catalog_entry(catalog_entry_id, extra_data);
        }
        Ok(())
    }

    /// 向事务的 Undo 日志推入 ATTACH DATABASE 操作（C++: `DuckTransactionManager::PushAttach()`）。
    ///
    /// 仅系统数据库允许 ATTACH。
    pub fn push_attach(
        &self,
        transaction: &TransactionRef,
        db_ctx: DbContext,
        database_id: u64,
    ) -> Result<(), ErrorData> {
        if !db_ctx.is_system {
            return Err(ErrorData::new("Can only ATTACH in the system catalog"));
        }
        let txn_id = transaction.transaction_id();
        let inner = self.inner.lock();
        let handle = inner
            .active_transactions
            .iter()
            .find(|h| h.transaction_id() == txn_id)
            .cloned()
            .ok_or_else(|| ErrorData::new("Transaction not found in active list"))?;
        drop(inner);

        let new_version = self
            .last_uncommitted_catalog_version
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        {
            let mut txn = handle.inner.lock();
            txn.catalog_version.store(new_version, Ordering::Relaxed);
            txn.push_attach(database_id);
        }
        Ok(())
    }

    // ── 内部实现 ──────────────────────────────────────────────────────────────

    /// 判断是否可以在本次提交后触发自动 checkpoint（C++: `CanCheckpoint()`）。
    ///
    /// 调用时必须持有 `transaction_lock`（通过 `&TransactionLockInner`）。
    ///
    /// C++ 签名：
    /// ```cpp
    /// CheckpointDecision CanCheckpoint(DuckTransaction&, unique_ptr<StorageLockKey>&,
    ///                                  const UndoBufferProperties&);
    /// ```
    fn can_checkpoint(
        &self,
        inner: &TransactionLockInner,
        txn_id: TransactionId,
        handle: &DuckTxnHandle,
        checkpoint_lock_out: &mut Option<Box<StorageLockKey>>,
        db: &Arc<crate::connection::connection::DatabaseInstance>,
        db_ctx: &DbContext,
    ) -> CheckpointDecision {
        // C++: if (db.IsSystem()) return "system transaction";
        if db_ctx.is_system {
            return CheckpointDecision::no("system transaction");
        }
        // C++: if (transaction.IsReadOnly()) return "read-only";
        if handle.is_read_only() {
            return CheckpointDecision::no("transaction is read-only");
        }
        // C++: if (!storage_manager.IsLoaded()) return "loading";
        if !db_ctx.storage_loaded {
            return CheckpointDecision::no("cannot checkpoint while loading");
        }

        // C++: if (!transaction.AutomaticCheckpoint(db, undo_properties)) return "no reason";
        let undo_properties = handle.inner.lock().get_undo_properties();
        let should_auto_checkpoint = handle
            .inner
            .lock()
            .automatic_checkpoint(db, &undo_properties);
        if !should_auto_checkpoint {
            return CheckpointDecision::no("no reason to automatically checkpoint");
        }

        // C++: if (DebugSkipCheckpointOnCommitSetting) return "disabled through config";
        if db_ctx.debug_skip_checkpoint_on_commit {
            return CheckpointDecision::no(
                "checkpointing on commit disabled through configuration",
            );
        }

        // C++: lock = transaction.TryGetCheckpointLock(); if (!lock) return "failed to obtain lock";
        let lock = handle
            .inner
            .lock()
            .try_get_checkpoint_lock(&self.checkpoint_lock);
        if lock.is_none() {
            return CheckpointDecision::no(
                "Failed to obtain checkpoint lock - another thread is writing/checkpointing \
                 or another read transaction relies on data that is not yet committed",
            );
        }
        *checkpoint_lock_out = lock;

        // 默认 FULL_CHECKPOINT；若有其他活跃事务，可能降级为 CONCURRENT
        let mut checkpoint_type = CheckpointType::FullCheckpoint;
        let has_others = inner.has_other_transactions(txn_id);
        if has_others {
            if undo_properties.has_updates || undo_properties.has_dropped_entries {
                // C++: 有更新/删除 catalog 条目时，不能在有其他事务时 checkpoint
                let other_ids = inner.other_transaction_ids(txn_id);
                if undo_properties.has_dropped_entries {
                    return CheckpointDecision::no(format!(
                        "Transaction has dropped catalog entries and there are other transactions active\n\
                         Active transactions: {}",
                        other_ids
                    ));
                }
                return CheckpointDecision::no(format!(
                    "Transaction has performed updates and there are other transactions active\n\
                     Active transactions: {}",
                    other_ids
                ));
            }
            checkpoint_type = CheckpointType::ConcurrentCheckpoint;
        }

        // C++: if (storage_manager.InMemory() && !storage_manager.CompressionIsEnabled()) { ... }
        if db_ctx.storage_in_memory && !db_ctx.compression_enabled {
            if checkpoint_type == CheckpointType::ConcurrentCheckpoint {
                return CheckpointDecision::no(
                    "Cannot vacuum, and compression is disabled for in-memory table",
                );
            }
            return CheckpointDecision::yes(CheckpointType::VacuumOnly);
        }

        CheckpointDecision::yes(checkpoint_type)
    }

    /// 从活跃列表中移除事务，并构建 cleanup 批次（C++: `RemoveTransaction(bool)` noexcept）。
    ///
    /// 调用时必须持有 `transaction_lock`（通过 `&mut TransactionLockInner`）。
    ///
    /// 流程：
    /// 1. 在 `active_transactions` 中找到 `txn_id`，同时计算新的 lowest_start_time 和 lowest_transaction_id。
    /// 2. 更新原子计数器 `lowest_active_start` / `lowest_active_id`。
    /// 3. 按 `store_transaction` 决策：
    ///    - store=true + committed → 放入 `recently_committed_transactions`
    ///    - store=true + aborted → 放入 cleanup 批次
    ///    - store=false + changes_made → 设置 awaiting_cleanup, 放入 cleanup 批次
    /// 4. 遍历 `recently_committed_transactions`：commit_id < new lowest_start_time 的可移入 cleanup。
    fn remove_transaction(
        &self,
        inner: &mut TransactionLockInner,
        txn_id: TransactionId,
        store_transaction: bool,
    ) -> DuckCleanupInfo {
        let mut cleanup_info = DuckCleanupInfo::new();

        // ── 步骤 1：遍历活跃事务，找目标事务，计算新 lowest ─────────────────
        let mut t_index = inner.active_transactions.len(); // sentinel = not found
        let mut lowest_start_time = TRANSACTION_ID_START;
        let mut lowest_transaction_id = MAX_TRANSACTION_ID;
        let active_checkpoint_id = self.active_checkpoint.load(Ordering::Acquire);

        for (i, h) in inner.active_transactions.iter().enumerate() {
            if h.transaction_id() == txn_id {
                t_index = i;
                continue; // 跳过被移除的事务本身
            }
            let st = h.start_time();
            let ti = h.transaction_id();
            if st < lowest_start_time {
                lowest_start_time = st;
            }
            if ti < lowest_transaction_id {
                lowest_transaction_id = ti;
            }
        }

        // C++: if (active_checkpoint_id != MAX_TRANSACTION_ID && active_checkpoint_id < lowest_start_time)
        //          lowest_start_time = active_checkpoint_id;
        if active_checkpoint_id != MAX_TRANSACTION_ID && active_checkpoint_id < lowest_start_time {
            lowest_start_time = active_checkpoint_id;
        }

        // ── 步骤 2：更新原子计数器 ─────────────────────────────────────────
        self.lowest_active_start
            .store(lowest_start_time, Ordering::Release);
        self.lowest_active_id
            .store(lowest_transaction_id, Ordering::Release);

        debug_assert!(
            t_index < inner.active_transactions.len(),
            "Transaction not found in active list"
        );

        // ── 步骤 3：取出目标事务 ───────────────────────────────────────────
        let current_transaction = inner.active_transactions.remove(t_index);
        cleanup_info.lowest_start_time = lowest_start_time;

        if store_transaction {
            let commit_id = current_transaction.commit_id();
            if commit_id != 0 {
                // 已提交：暂存到 recently_committed（可能被其他活跃事务读旧版本）
                // C++: recently_committed_transactions.push_back(std::move(current_transaction));
                inner
                    .recently_committed_transactions
                    .push(current_transaction);
            } else {
                // 已中止（回滚）：直接进 cleanup
                // C++: cleanup_info->transactions.push_back(std::move(current_transaction));
                cleanup_info.transactions.push(current_transaction);
            }
        } else if current_transaction.changes_made() {
            // 无需保存事务，但有修改时标记为 awaiting_cleanup
            // C++: current_transaction->awaiting_cleanup = true;
            current_transaction.set_awaiting_cleanup(true);
            cleanup_info.transactions.push(current_transaction);
        }

        // ── 步骤 4：将可清理的 recently_committed 事务移入 cleanup ─────────
        //
        // C++: for (i < recently_committed_transactions.size())
        //          if (recently_committed_transactions[i]->commit_id >= lowest_start_time) break;
        //          recently_committed_transactions[i]->awaiting_cleanup = true;
        //          cleanup_info->transactions.push_back(...)
        let mut drain_count = 0;
        for h in &inner.recently_committed_transactions {
            if h.commit_id() >= lowest_start_time {
                // recently_committed 按 commit_id 升序，后续也不满足条件
                break;
            }
            h.set_awaiting_cleanup(true);
            drain_count += 1;
        }
        if drain_count > 0 {
            let drained: Vec<Arc<DuckTxnHandle>> = inner
                .recently_committed_transactions
                .drain(..drain_count)
                .collect();
            cleanup_info.transactions.extend(drained);
        }

        cleanup_info
    }

    /// 将 cleanup 批次加入队列，然后尝试执行队首批次（C++: commit/rollback 后的 cleanup 逻辑）。
    ///
    /// C++ 使用两把锁：
    /// - `cleanup_queue_lock`：保护队列的读写（入队/出队）。
    /// - `cleanup_lock`：保证同一时刻只有一个 cleanup 执行（try_lock 语义）。
    fn schedule_and_run_cleanup(&self, cleanup_info: DuckCleanupInfo) {
        // ── 入队 ─────────────────────────────────────────────────────────────
        // C++: lock_guard<mutex> q_lock(cleanup_queue_lock);
        //      cleanup_queue.emplace(std::move(cleanup_info));
        if cleanup_info.schedule_cleanup() {
            self.cleanup_queue.lock().queue.push_back(cleanup_info);
        }

        // ── 尝试执行队首 ──────────────────────────────────────────────────────
        // C++: lock_guard<mutex> c_lock(cleanup_lock);  // 独占执行权
        //      { q_lock; top = queue.front(); queue.pop(); }
        //      if (top) { top->Cleanup(); }
        let _exec_guard = self.cleanup_exec_lock.lock();

        let top = {
            let mut q = self.cleanup_queue.lock();
            q.queue.pop_front()
        };
        if let Some(batch) = top {
            batch.cleanup();
        }
    }

    /// 在提交成功时，根据 `CheckpointDecision` 执行额外逻辑的钩子
    /// （C++: `virtual OnCommitCheckpointDecision()`，默认空实现）。
    fn on_commit_checkpoint_decision(
        &self,
        _decision: &CheckpointDecision,
        _txn_handle: &DuckTxnHandle,
    ) {
        // 默认空实现，子类可覆盖
    }

    // ── 完整提交流程（含 WAL 和 checkpoint 逻辑）─────────────────────────────

    /// 完整提交流程（C++: `DuckTransactionManager::CommitTransaction()`）。
    ///
    /// 接受 `DbContext` 替代 C++ 中的 `AttachedDatabase &db`，
    /// 接受 `tables` 用于 `LocalStorage::commit()`（C++: 通过 `StorageCommitState` 传递）。
    ///
    /// 流程：
    /// 1. 持有 `transaction_lock`。
    /// 2. 检查 CanCheckpoint。
    /// 3. 若需要写 WAL：释放 transaction_lock，写 WAL，重新获取 transaction_lock。
    /// 4. 分配 commit_id。
    /// 5. 提交事务（LocalStorage 合并 + UndoBuffer 设置版本号）。
    /// 6. 提交出错：Rollback；成功：更新 last_commit 和 catalog_version。
    /// 7. RemoveTransaction → 入 cleanup 队列。
    /// 8. 释放 transaction_lock。
    /// 9. 执行 cleanup。
    /// 10. 若 can_checkpoint：CreateCheckpoint（此处 todo!() 接入存储层）。
    /// 提交事务（C++: `DuckTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction)`）。
    ///
    /// # 参数
    /// - `db`: 数据库实例引用（C++: `AttachedDatabase &db`，通过 `context` 访问）
    /// - `transaction`: 要提交的事务引用
    ///
    /// # 返回
    /// 成功返回 `None`，失败返回 `Some(ErrorData)`
    pub fn commit_transaction(
        &self,
        db: &Arc<crate::connection::connection::DatabaseInstance>,
        transaction: &TransactionRef,
    ) -> Option<ErrorData> {
        let txn_id = transaction.transaction_id();

        // 构建 DbContext（C++: 从 AttachedDatabase 获取）
        let db_ctx = DbContext {
            is_system: false,    // TODO: 从 db 获取
            is_temporary: false, // TODO: 从 db 获取
            is_read_only: false,
            storage_loaded: true,
            storage_in_memory: false,
            compression_enabled: false,
            debug_skip_checkpoint_on_commit: false,
            has_wal: true,
            recovery_mode_default: true,
            storage_manager: Some(db.storage_manager.clone()),
        };

        // 获取 tables（C++: 通过 AttachedDatabase 访问）
        let tables_guard = db.tables.lock();
        let tables: HashMap<u64, Arc<DataTable>> = tables_guard
            .values()
            .map(|h| (h.storage.info.table_id(), h.storage.clone()))
            .collect();
        drop(tables_guard);

        // ── 步骤 1：获取 transaction_lock ─────────────────────────────────────
        // C++: unique_lock<mutex> t_lock(transaction_lock);
        let mut inner = self.inner.lock();

        // 找到事务句柄（持有 transaction_lock 期间）
        let handle = match inner
            .active_transactions
            .iter()
            .find(|h| h.transaction_id() == txn_id)
            .cloned()
        {
            Some(h) => h,
            None => return Some(ErrorData::new("Transaction not found in active list")),
        };

        // C++: if (!db.IsSystem() && !db.IsTemporary()) { check read-only + changes }
        if !db_ctx.is_system && !db_ctx.is_temporary {
            if handle.changes_made() && handle.is_read_only() {
                return Some(ErrorData::new(
                    "Attempting to commit a transaction that is read-only but has made changes",
                ));
            }
        }

        // ── 步骤 2：决策是否 checkpoint ────────────────────────────────────────
        // C++: unique_ptr<StorageLockKey> lock;
        //      auto undo_properties = transaction.GetUndoProperties();
        //      auto checkpoint_decision = CanCheckpoint(transaction, lock, undo_properties);
        let mut checkpoint_lock_key: Option<Box<StorageLockKey>> = None;
        let mut checkpoint_decision = self.can_checkpoint(
            &inner,
            txn_id,
            &handle,
            &mut checkpoint_lock_key,
            db,
            &db_ctx,
        );

        // ── 步骤 3：写 WAL（若需要）──────────────────────────────────────────
        // C++: if (!checkpoint_decision.can_checkpoint && transaction.ShouldWriteToWAL(db)) {
        //          t_lock.unlock(); // grab WAL lock, write WAL, t_lock.lock();
        //      }
        let mut commit_error: Option<ErrorData> = None;
        let mut commit_state: Option<Box<dyn crate::storage::storage_manager::StorageCommitState>> =
            None;
        let should_write_wal = handle
            .inner
            .lock()
            .should_write_to_wal(db_ctx.is_system, db_ctx.has_wal);
        if !checkpoint_decision.can_checkpoint && should_write_wal {
            // 释放 transaction_lock，避免长时间 WAL 写阻塞其他事务开始
            drop(inner);
            // C++: held_wal_lock = storage_manager.GetWALLock();
            //      if (db.GetRecoveryMode() == DEFAULT) { error = transaction.WriteToWAL(...);}
            if db_ctx.recovery_mode_default {
                // 调用 write_to_wal，传入 storage_manager 和 tables
                if let Some(storage_mgr) = db_ctx.storage_manager.as_ref() {
                    match handle
                        .inner
                        .lock()
                        .write_to_wal(storage_mgr.as_ref(), &tables)
                    {
                        Ok(state) => {
                            commit_state = Some(state);
                        }
                        Err(e) => {
                            commit_error = Some(ErrorData::new(e));
                        }
                    }
                } else {
                    commit_error = Some(ErrorData::new(
                        "StorageManager not available for WAL write".to_string(),
                    ));
                }
            }
            // 重新获取 transaction_lock
            inner = self.inner.lock();
        }

        // ── 步骤 4：分配 commit_id ─────────────────────────────────────────────
        // C++: CommitInfo info; info.commit_id = GetCommitTimestamp();
        let commit_id = inner.get_commit_timestamp();
        let has_others = inner.has_other_transactions(txn_id);

        // ── 步骤 5：提交事务（LocalStorage 合并 + UndoBuffer 设置版本号）────────
        // C++: if (!error.HasError()) { transaction.Commit(db, info, commit_state); }
        if commit_error.is_none() {
            let active_transactions_state = if has_others {
                ActiveTransactionState::HasActiveTransactions
            } else {
                ActiveTransactionState::NoActiveTransactions
            };
            let commit_info = CommitInfo {
                commit_id,
                active_transactions: active_transactions_state,
            };
            // 完整提交：LocalStorage 合并到主存储 + UndoBuffer 版本号更新
            // C++: transaction.Commit(db, info, commit_state) 内部先 storage->Commit() 再 undo_buffer.Commit()
            if let Err(e) = handle.inner.lock().commit(commit_info, None, &tables) {
                commit_error = Some(ErrorData::new(e));
            }

            // C++: if (commit_state) { commit_state->FlushCommit(); }
            // 提交成功后，flush WAL 到磁盘
            if commit_error.is_none() {
                if let Some(ref mut state) = commit_state {
                    if let Err(e) = state.flush_commit() {
                        commit_error = Some(ErrorData::new(format!("FlushCommit failed: {:?}", e)));
                    }
                }
            }
        }

        // ── 步骤 6：处理提交结果 ───────────────────────────────────────────────
        if commit_error.is_some() {
            // C++: commit 失败 → Rollback
            // checkpoint_decision = CheckpointDecision(error.Message());
            checkpoint_decision =
                CheckpointDecision::no(commit_error.as_ref().unwrap().message.clone());
            let mut txn = handle.inner.lock();
            txn.commit_id = 0;
            if let Err(rollback_err) = txn.rollback() {
                // C++: throw FatalException("Failed to rollback...");
                panic!(
                    "Failed to rollback transaction after failed commit.\n\
                     Original: {:?}\nRollback: {:?}",
                    commit_error, rollback_err
                );
            }
        } else {
            // C++: last_commit = info.commit_id;
            self.last_commit.store(commit_id, Ordering::Release);

            // C++: if (transaction.catalog_version >= TRANSACTION_ID_START)
            //          transaction.catalog_version = ++last_committed_version;
            let catalog_version = handle
                .inner
                .lock()
                .catalog_version
                .load(std::sync::atomic::Ordering::Relaxed);
            if catalog_version >= TRANSACTION_ID_START {
                inner.last_committed_version += 1;
                handle.inner.lock().catalog_version.store(
                    inner.last_committed_version,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }

        // C++: OnCommitCheckpointDecision(checkpoint_decision, transaction);
        self.on_commit_checkpoint_decision(&checkpoint_decision, &handle);

        // C++: if (!checkpoint_decision.can_checkpoint && lock) { lock.reset(); }
        if !checkpoint_decision.can_checkpoint {
            checkpoint_lock_key = None; // 释放 checkpoint lock
        }

        // ── 步骤 7：RemoveTransaction ──────────────────────────────────────────
        // C++: bool store_transaction = undo_properties.has_updates || has_index_deletes ||
        //          has_catalog_changes || error;
        //      auto cleanup_info = RemoveTransaction(transaction, store_transaction);
        let undo_properties = handle.inner.lock().get_undo_properties();
        let store_transaction = undo_properties.has_updates
            || undo_properties.has_index_deletes
            || undo_properties.has_catalog_changes
            || commit_error.is_some();

        let cleanup_info = self.remove_transaction(&mut inner, txn_id, store_transaction);

        // ── 步骤 8：释放 transaction_lock ─────────────────────────────────────
        // C++: t_lock.unlock(); held_wal_lock.reset();
        drop(inner);

        // ── 步骤 9：执行 cleanup ────────────────────────────────────────────────
        // C++: { c_lock; top = queue.front(); top->Cleanup(); }
        self.schedule_and_run_cleanup(cleanup_info);

        // ── 步骤 10：若可 checkpoint，执行之 ───────────────────────────────────
        // C++: if (checkpoint_decision.can_checkpoint) {
        //          CheckpointOptions options; options.action = ALWAYS_CHECKPOINT;
        //          storage_manager.CreateCheckpoint(context, options);
        //      }
        if checkpoint_decision.can_checkpoint {
            debug_assert!(checkpoint_lock_key.is_some());

            // 触发自动 checkpoint
            if let Some(storage_mgr) = db_ctx.storage_manager.as_ref() {
                let checkpoint_options = crate::storage::storage_manager::CheckpointOptions {
                    action: crate::storage::storage_manager::CheckpointAction::AlwaysCheckpoint,
                    transaction_id: Some(txn_id),
                };

                // 执行 checkpoint（忽略错误，checkpoint 失败不影响事务提交）
                // C++: storage_manager.CreateCheckpoint(context, options);
                match storage_mgr.create_checkpoint(checkpoint_options) {
                    Ok(_) => println!("   ✅ 自动 checkpoint 执行成功"),
                    Err(e) => println!("   ⚠️  自动 checkpoint 执行失败: {:?}", e),
                }
            } else {
                println!("   ⚠️  StorageManager 不可用，跳过 checkpoint");
            }

            // checkpoint_lock_key 会在此处 drop，自动释放锁
        }
        commit_error.map(|e| e)
    }

    /// 完整回滚流程（C++: `DuckTransactionManager::RollbackTransaction()`）。
    pub fn rollback_transaction_with_context(
        &self,
        transaction: &TransactionRef,
    ) -> Option<ErrorData> {
        let txn_id = transaction.transaction_id();

        // C++: lock_guard<mutex> t_lock(transaction_lock);
        let mut inner = self.inner.lock();

        let handle = match inner
            .active_transactions
            .iter()
            .find(|h| h.transaction_id() == txn_id)
            .cloned()
        {
            Some(h) => h,
            None => return Some(ErrorData::new("Transaction not found in active list")),
        };

        // C++: error = transaction.Rollback();
        let rollback_error = handle
            .inner
            .lock()
            .rollback()
            .err()
            .map(|e| ErrorData::new(e));

        // C++: auto cleanup_info = RemoveTransaction(transaction);
        //      (store_transaction = transaction.ChangesMade())
        let store_transaction = false; // rollback 时不 store（已在 remove_transaction 内判断 changes_made）
        let cleanup_info = self.remove_transaction(&mut inner, txn_id, store_transaction);

        // C++: t_lock.unlock();
        drop(inner);

        // C++: { c_lock; top = queue.front(); top->Cleanup(); }
        self.schedule_and_run_cleanup(cleanup_info);

        // C++: if (error.HasError()) throw FatalException(...);
        if let Some(ref err) = rollback_error {
            panic!(
                "Failed to rollback transaction. Cannot continue operation.\nError: {}",
                err.message
            );
        }

        rollback_error
    }

    /// 手动触发 checkpoint（C++: `DuckTransactionManager::Checkpoint()`）。
    ///
    /// - `force = false`：非阻塞，若无法获取独占锁则报错。
    /// - `force = true`：阻塞等待所有写事务结束后再 checkpoint。
    ///
    /// 由于 Rust 版本没有 `ClientContext&` 的 `interrupted` 信号，force 模式使用自旋等待。
    pub fn checkpoint_with_context(
        &self,
        current_txn_id: Option<TransactionId>,
        current_txn_has_changes: bool,
        force: bool,
    ) -> Result<(), ErrorData> {
        // C++: auto current = Transaction::TryGet(context, db);
        //      if (current) {
        //          if (force) throw TransactionException("cannot FORCE CHECKPOINT with active txn");
        //          else if (duck_transaction.ChangesMade()) throw "cannot CHECKPOINT with local changes";
        //      }
        if current_txn_id.is_some() {
            if force {
                return Err(ErrorData::new(
                    "Cannot FORCE CHECKPOINT: the current transaction has been started for this database",
                ));
            } else if current_txn_has_changes {
                return Err(ErrorData::new(
                    "Cannot CHECKPOINT: the current transaction has transaction local changes",
                ));
            }
        }

        let lock: Box<StorageLockKey>;
        if !force {
            // C++: lock = checkpoint_lock.TryGetExclusiveLock(); if (!lock) throw ...;
            lock = self
                .checkpoint_lock
                .try_get_exclusive_lock()
                .ok_or_else(|| {
                    ErrorData::new(
                        "Cannot CHECKPOINT: there are other write transactions active. \
                     Try using FORCE CHECKPOINT to wait until all active transactions are finished",
                    )
                })?;
        } else {
            // C++: lock_guard<mutex> start_lock(start_transaction_lock);
            //      while (!lock) { lock = checkpoint_lock.TryGetExclusiveLock(); }
            let _start_lock = self.start_transaction_lock.lock();
            loop {
                if let Some(exclusive) = self.checkpoint_lock.try_get_exclusive_lock() {
                    lock = exclusive;
                    break;
                }
                std::hint::spin_loop();
            }
        }

        // C++: if (GetLastCommit() > LowestActiveStart()) options.type = CONCURRENT_CHECKPOINT;
        let _checkpoint_type = if self.last_commit.load(Ordering::Acquire)
            > self.lowest_active_start.load(Ordering::Acquire)
        {
            CheckpointType::ConcurrentCheckpoint
        } else {
            CheckpointType::FullCheckpoint
        };

        // C++: storage_manager.CreateCheckpoint(context, options);
        // TODO: 调用 storage_manager.create_checkpoint(_checkpoint_type)

        drop(lock); // 释放独占锁
        Ok(())
    }
}

impl Default for DuckTransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager for DuckTransactionManager {
    /// 开始新事务（C++: `DuckTransactionManager::StartTransaction(ClientContext &context)`）。
    ///
    /// 流程：
    /// 1. 若是写事务，持有 `start_transaction_lock`（防止与 FORCE CHECKPOINT 竞争）。
    /// 2. 持有 `transaction_lock`。
    /// 3. 检查 `current_start_timestamp < TRANSACTION_ID_START`（overflow guard）。
    /// 4. 分配 `start_time = current_start_timestamp++`，`transaction_id = current_transaction_id++`。
    /// 5. 若 `active_transactions.empty()`：初始化 `lowest_active_start/id`。
    /// 6. 构造 `DuckTransaction`，推入 `active_transactions`，返回 `TransactionRef`。
    ///
    /// # 与 C++ 的差异
    ///
    /// C++ 接受 `ClientContext &` 用于判断 meta_transaction.IsReadOnly()。
    /// Rust 改为接受 `is_read_only: bool` 参数，以避免引入 ClientContext 依赖。
    fn start_transaction(&self) -> TransactionRef {
        self.start_transaction_with_read_only(true)
    }

    /// 提交事务（C++: `DuckTransactionManager::CommitTransaction()`）。
    ///
    /// 这是 TransactionManager trait 的实现，用于向后兼容。
    /// 新代码应使用接收 DatabaseInstance 的版本。
    fn commit_transaction(
        &self,
        transaction: &TransactionRef,
        tables: &HashMap<u64, Arc<DataTable>>,
    ) -> Option<ErrorData> {
        // 注意：这个方法缺少 DatabaseInstance 引用，无法完全实现
        // 应该通过新的 commit_transaction(db, transaction) 方法调用
        Some(ErrorData::new(
            "commit_transaction requires DatabaseInstance reference - use the new API",
        ))
    }

    /// 回滚事务（C++: `DuckTransactionManager::RollbackTransaction()`）。
    fn rollback_transaction(&self, transaction: &TransactionRef) {
        self.rollback_transaction_with_context(transaction);
    }

    /// 手动触发 checkpoint（C++: `DuckTransactionManager::Checkpoint()`）。
    fn checkpoint(&self, force: bool) {
        let _ = self.checkpoint_with_context(None, false, force);
    }

    fn is_duck_transaction_manager(&self) -> bool {
        true
    }
}

impl DuckTransactionManager {
    /// 开始新事务，直接返回 `Arc<DuckTxnHandle>`（供 `MetaTransaction` 使用）。
    ///
    /// C++ 中通过 `MetaTransaction::Get(context).IsReadOnly()` 判断是否为只读事务。
    /// 只读事务不获取 `start_transaction_lock`，允许在 FORCE CHECKPOINT 等待期间继续。
    pub fn start_duck_transaction(&self, is_read_only: bool) -> Arc<DuckTxnHandle> {
        // C++: if (!meta_transaction.IsReadOnly()) {
        //          start_lock = make_uniq<lock_guard<mutex>>(start_transaction_lock);
        //      }
        let _start_lock = if !is_read_only {
            Some(self.start_transaction_lock.lock())
        } else {
            None
        };

        // C++: lock_guard<mutex> lock(transaction_lock);
        let mut inner = self.inner.lock();

        // C++: if (current_start_timestamp >= TRANSACTION_ID_START) throw "ran out of identifiers";
        if inner.current_start_timestamp >= TRANSACTION_ID_START {
            panic!("Cannot start more transactions, ran out of transaction identifiers!");
        }

        // C++: transaction_t start_time = current_start_timestamp++;
        //      transaction_t transaction_id = current_transaction_id++;
        let start_time = inner.current_start_timestamp;
        inner.current_start_timestamp += 1;
        let transaction_id = inner.current_transaction_id;
        inner.current_transaction_id += 1;

        // C++: if (active_transactions.empty()) {
        //          lowest_active_start = start_time;
        //          lowest_active_id = transaction_id;
        //      }
        if inner.active_transactions.is_empty() {
            self.lowest_active_start
                .store(start_time, Ordering::Release);
            self.lowest_active_id
                .store(transaction_id, Ordering::Release);
        }

        // C++: auto transaction = make_uniq<DuckTransaction>(*this, context, start_time, transaction_id,
        //                                                   last_committed_version);
        let catalog_version = inner.last_committed_version;
        let txn = DuckTransaction::new(
            start_time,
            transaction_id,
            catalog_version,
            256 * 1024, // default block_alloc_size = 256 KB
        );
        if !is_read_only {
            txn.set_read_write(); // 写事务
        }

        // C++: active_transactions.push_back(std::move(transaction));
        //      return transaction_ref;
        let handle = DuckTxnHandle::wrap(txn);
        let ret = Arc::clone(&handle);
        inner.active_transactions.push(handle);
        ret
    }

    /// 开始新事务的完整版本，接受 `is_read_only` 参数，返回 `TransactionRef`。
    pub fn start_transaction_with_read_only(&self, is_read_only: bool) -> TransactionRef {
        self.start_duck_transaction(is_read_only) as TransactionRef
    }

    /// 回滚指定 `Arc<DuckTxnHandle>` 持有的事务（供 `MetaTransaction` 使用）。
    pub fn rollback_duck_transaction(&self, handle: &Arc<DuckTxnHandle>) {
        let txn_ref: TransactionRef = Arc::clone(handle) as TransactionRef;
        self.rollback_transaction_with_context(&txn_ref);
    }
}
