//! 跨数据库元事务。
//!
//! 对应 C++: `duckdb/transaction/meta_transaction.hpp`
//!
//! # 职责
//!
//! 一个客户端连接（`ClientContext`）对应一个 `MetaTransaction`。
//! 当查询涉及多个附加数据库时，`MetaTransaction` 为每个库
//! 分别维护一个具体 `Transaction`，统一提交/回滚。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `reference_map_t<AttachedDatabase, TransactionReference> transactions` | `HashMap<u64, TransactionReference>` |
//! | `vector<reference<AttachedDatabase>> all_transactions` | `Vec<u64>` (db_id 有序列表) |
//! | `optional_ptr<AttachedDatabase> modified_database` | `Option<u64>` |
//! | `mutex lock` | `Mutex<MetaTransactionInner>` |
//! | `atomic<transaction_t> active_query` | `AtomicU64` |
//! | `DuckTransactionManager &manager` | `Arc<DuckTransactionManager>` |

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use super::duck_transaction_manager::{DuckTransactionManager, DuckTxnHandle};
use super::transaction_manager::ErrorData;
use super::types::{MAXIMUM_QUERY_ID, Timestamp, TransactionId, TransactionState};
use crate::storage::data_table::DataTable;

// ─── TransactionReference ──────────────────────────────────────────────────────

/// 附加到某个数据库的事务状态记录（C++: `struct TransactionReference`）。
pub struct TransactionReference {
    /// 当前状态（C++: `TransactionState state`）。
    pub state: TransactionState,
    /// 具体事务句柄（C++: `Transaction &transaction`）。
    pub transaction: Arc<DuckTxnHandle>,
}

impl TransactionReference {
    pub fn new(transaction: Arc<DuckTxnHandle>) -> Self {
        Self {
            state: TransactionState::Uncommitted,
            transaction,
        }
    }
}

// ─── MetaTransactionInner ──────────────────────────────────────────────────────

/// `lock` 保护的可变状态（C++: 多字段在 `mutex lock` 下访问）。
struct MetaTransactionInner {
    /// db_id → 该库的事务引用（C++: `reference_map_t<AttachedDatabase, TransactionReference>`）。
    transactions: HashMap<u64, TransactionReference>,

    /// 按开启顺序记录的 db_id 列表（C++: `vector<reference<AttachedDatabase>> all_transactions`）。
    all_transactions: Vec<u64>,

    /// 本次元事务修改的唯一数据库（C++: `optional_ptr<AttachedDatabase> modified_database`）。
    /// DuckDB 单事务只允许修改一个数据库。
    modified_database: Option<u64>,

    /// 是否只读（C++: `bool is_read_only`）。
    is_read_only: bool,
}

// ─── MetaTransaction ───────────────────────────────────────────────────────────

/// 跨数据库元事务（C++: `class MetaTransaction`）。
pub struct MetaTransaction {
    /// 事务开始时间戳（C++: `timestamp_t start_timestamp`）。
    pub start_timestamp: Timestamp,

    /// 全局事务 ID（C++: `transaction_t global_transaction_id`）。
    pub global_transaction_id: TransactionId,

    /// 当前活跃查询 ID（C++: `atomic<transaction_t> active_query`）。
    pub active_query: AtomicU64,

    /// 锁保护的内部状态（C++: `mutex lock` + 受保护字段）。
    inner: Mutex<MetaTransactionInner>,

    /// 数据库的事务管理器（C++: `DuckTransactionManager &manager`）。
    transaction_manager: Arc<DuckTransactionManager>,

    /// 数据库实例的弱引用（C++: `ClientContext &context` 中包含）。
    db_instance: Weak<crate::connection::connection::DatabaseInstance>,
}

impl MetaTransaction {
    /// 构造（C++: `MetaTransaction(ClientContext&, timestamp_t, transaction_t)`）。
    pub fn new(
        transaction_manager: Arc<DuckTransactionManager>,
        start_timestamp: Timestamp,
        global_transaction_id: TransactionId,
        db_instance: Weak<crate::connection::connection::DatabaseInstance>,
    ) -> Self {
        Self {
            start_timestamp,
            global_transaction_id,
            active_query: AtomicU64::new(MAXIMUM_QUERY_ID),
            inner: Mutex::new(MetaTransactionInner {
                transactions: HashMap::new(),
                all_transactions: Vec::new(),
                modified_database: None,
                is_read_only: true,
            }),
            transaction_manager,
            db_instance,
        }
    }

    // ── 事务管理 ──────────────────────────────────────────────────────────────

    /// 获取或创建指定数据库的事务（C++: `MetaTransaction::GetTransaction(AttachedDatabase&)`）。
    ///
    /// 首次访问时向该库的 `TransactionManager::start_duck_transaction()` 申请新事务。
    /// `is_read_only` 取决于元事务当前的只读标志。
    pub fn get_transaction(&self, db_id: u64) -> Arc<DuckTxnHandle> {
        let mut inner = self.inner.lock();
        if let Some(txn_ref) = inner.transactions.get(&db_id) {
            return Arc::clone(&txn_ref.transaction);
        }
        // 首次访问：创建 DuckTransaction（C++: manager.StartTransaction(context)）
        let is_read_only = inner.is_read_only;
        let handle = self
            .transaction_manager
            .start_duck_transaction(is_read_only);
        inner.all_transactions.push(db_id);
        inner
            .transactions
            .insert(db_id, TransactionReference::new(Arc::clone(&handle)));
        handle
    }

    /// 尝试获取已有事务，不存在返回 `None`（C++: `TryGetTransaction()`）。
    pub fn try_get_transaction(&self, db_id: u64) -> Option<Arc<DuckTxnHandle>> {
        let inner = self.inner.lock();
        inner
            .transactions
            .get(&db_id)
            .map(|r| Arc::clone(&r.transaction))
    }

    /// 移除指定数据库的事务记录（C++: `RemoveTransaction()`）。
    pub fn remove_transaction(&self, db_id: u64) {
        let mut inner = self.inner.lock();
        inner.transactions.remove(&db_id);
        inner.all_transactions.retain(|id| *id != db_id);
    }

    // ── 提交 / 回滚 ───────────────────────────────────────────────────────────

    /// 提交所有子事务（C++: `MetaTransaction::Commit()`）。
    ///
    /// 按 `all_transactions` 顺序逐个提交，任意一个失败则回滚后续并返回错误。
    pub fn commit(&self) -> Option<ErrorData> {
        // 获取数据库实例引用（C++: 通过 context 引用访问）
        let db = match self.db_instance.upgrade() {
            Some(db) => db,
            None => return Some(ErrorData::new("Database instance has been dropped")),
        };

        let inner = self.inner.lock();
        let all_dbs: Vec<u64> = inner.all_transactions.clone();
        drop(inner); // 释放锁，允许 commit_transaction 内部操作

        for db_id in &all_dbs {
            let handle = {
                let inner = self.inner.lock();
                match inner.transactions.get(db_id) {
                    Some(r) => Arc::clone(&r.transaction),
                    None => continue,
                }
            };
            // C++: auto error = manager.CommitTransaction(context, transaction);
            use super::transaction::Transaction;
            let txn_ref = Arc::clone(&handle) as Arc<dyn Transaction>;
            let result = self.transaction_manager.commit_transaction(&db, &txn_ref);
            if let Some(err) = result {
                // 提交失败：回滚所有尚未提交的子事务
                // C++: for (auto &[db, txn_ref] : transactions) { if txn_ref.state == Uncommitted rollback }
                let inner = self.inner.lock();
                for &remaining_db in &inner.all_transactions {
                    if remaining_db == *db_id {
                        continue; // 当前失败的事务跳过
                    }
                    if let Some(r) = inner.transactions.get(&remaining_db) {
                        if r.state == TransactionState::Uncommitted {
                            self.transaction_manager
                                .rollback_duck_transaction(&r.transaction);
                        }
                    }
                }
                return Some(err);
            }
        }
        None
    }

    /// 回滚所有子事务（C++: `MetaTransaction::Rollback()`）。
    ///
    /// 按 `all_transactions` 逆序逐个回滚（C++: 也是逆序）。
    pub fn rollback(&self) {
        let inner = self.inner.lock();
        // C++: for (idx_t i = all_transactions.size(); i > 0; i--)
        //          auto &db = all_transactions[i - 1]; auto &txn = transactions[db];
        //          manager.RollbackTransaction(txn.transaction);
        for &db_id in inner.all_transactions.iter().rev() {
            if let Some(r) = inner.transactions.get(&db_id) {
                self.transaction_manager
                    .rollback_duck_transaction(&r.transaction);
            }
        }
    }

    // ── 查询状态 ──────────────────────────────────────────────────────────────

    /// 当前活跃查询 ID（C++: `GetActiveQuery()`）。
    pub fn get_active_query(&self) -> TransactionId {
        self.active_query.load(Ordering::Relaxed)
    }

    /// 设置活跃查询 ID（C++: `SetActiveQuery()`）。
    pub fn set_active_query(&self, query_id: TransactionId) {
        self.active_query.store(query_id, Ordering::Relaxed);
    }

    // ── 读写状态 ──────────────────────────────────────────────────────────────

    /// 设置为只读（C++: `SetReadOnly()`）。
    pub fn set_read_only(&self) {
        self.inner.lock().is_read_only = true;
    }

    /// 是否只读（C++: `IsReadOnly()`）。
    pub fn is_read_only(&self) -> bool {
        self.inner.lock().is_read_only
    }

    /// 记录修改的数据库，并将元事务标记为写事务（C++: `ModifyDatabase()`）。
    ///
    /// 同一事务只允许修改一个数据库，否则 panic（与 C++ 行为一致）。
    pub fn modify_database(&self, db_id: u64) {
        let mut inner = self.inner.lock();
        match inner.modified_database {
            None => {
                inner.modified_database = Some(db_id);
                inner.is_read_only = false; // 写事务
            }
            Some(existing) if existing == db_id => {}
            Some(_) => {
                panic!("MetaTransaction: cannot modify more than one database per transaction")
            }
        }
    }

    /// 已修改的数据库 ID（C++: `ModifiedDatabase()`）。
    pub fn modified_database(&self) -> Option<u64> {
        self.inner.lock().modified_database
    }
}
