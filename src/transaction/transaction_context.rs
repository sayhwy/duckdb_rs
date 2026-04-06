//! 客户端事务上下文。
//!
//! 对应 C++: `duckdb/transaction/transaction_context.hpp`
//!
//! # 职责
//!
//! 每个 `ClientContext` 持有一个 `TransactionContext`，负责：
//! - 跟踪是否有活跃元事务（`current_transaction`）。
//! - 管理自动提交（`auto_commit`）模式。
//! - 提供 `BeginTransaction / Commit / Rollback` 的统一入口。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `ClientContext &context` | `client_id: u64`（ClientContext 实现后替换） |
//! | `unique_ptr<MetaTransaction> current_transaction` | `Option<Box<MetaTransaction>>` |
//! | `bool auto_commit` | `auto_commit: bool` |
//! | `DuckTransactionManager &manager` | `transaction_manager: Arc<DuckTransactionManager>` |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use super::duck_transaction_manager::{DuckTransactionManager, DuckTxnHandle};
use super::meta_transaction::MetaTransaction;
use super::transaction_manager::ErrorData;
use super::types::TransactionId;

// ─── TransactionError ──────────────────────────────────────────────────────────

/// 事务错误类型。
#[derive(Debug, Clone)]
pub struct TransactionError {
    pub message: String,
}

impl TransactionError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TransactionError {}

impl From<ErrorData> for TransactionError {
    fn from(err: ErrorData) -> Self {
        Self {
            message: err.message,
        }
    }
}

fn current_timestamp_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

// ─── TransactionContext ────────────────────────────────────────────────────────

/// 客户端级别的事务状态（C++: `class TransactionContext`）。
///
/// 非 `Send + Sync`：设计上由单个客户端线程独占访问，
/// 多线程并发通过 `ClientContext` 的锁保护。
pub struct TransactionContext {
    /// 客户端 ID（C++: `ClientContext &context`，暂用 ID 替代）。
    _client_id: u64,

    /// 是否自动提交（C++: `bool auto_commit`）。
    auto_commit: AtomicBool,

    /// 当前活跃元事务（C++: `unique_ptr<MetaTransaction> current_transaction`）。
    ///
    /// 使用 `Arc` 而非 `Box`，以便 `begin_transaction_arc()` 可以同时向调用方返回引用。
    current_transaction: Mutex<Option<Arc<MetaTransaction>>>,

    /// 事务管理器（C++: 通过 `AttachedDatabase` 获取）。
    transaction_manager: Arc<DuckTransactionManager>,

    /// 数据库实例的弱引用（用于获取 tables）。
    db_instance: Weak<crate::db::conn::DatabaseInstance>,
}

impl TransactionContext {
    /// 构造（C++: `TransactionContext(ClientContext&)`）。
    pub fn new(
        client_id: u64,
        transaction_manager: Arc<DuckTransactionManager>,
        db_instance: Weak<crate::db::conn::DatabaseInstance>,
    ) -> Self {
        Self {
            _client_id: client_id,
            auto_commit: AtomicBool::new(true),
            current_transaction: Mutex::new(None),
            transaction_manager,
            db_instance,
        }
    }

    // ── 状态查询 ──────────────────────────────────────────────────────────────

    /// 是否有活跃事务（C++: `HasActiveTransaction()`）。
    pub fn has_active_transaction(&self) -> bool {
        self.current_transaction.lock().is_some()
    }

    /// 尝试获取当前事务句柄。
    pub fn try_get_transaction(&self) -> Option<Arc<DuckTxnHandle>> {
        let current = self.current_transaction.lock();
        let db_id = self.default_db_id()?;
        current
            .as_ref()
            .and_then(|txn| txn.try_get_transaction(db_id))
    }

    /// 获取或创建事务（只读）。
    pub fn get_or_create_transaction(&self) -> Arc<DuckTxnHandle> {
        let mut current = self.current_transaction.lock();
        if current.is_none() {
            *current = Some(Arc::new(self.create_meta_transaction()));
        }
        let db_id = self
            .default_db_id()
            .expect("TransactionContext::get_or_create_transaction: database instance dropped");
        current.as_ref().unwrap().get_transaction(db_id)
    }

    /// 获取或创建写事务。
    pub fn get_or_create_write_transaction(&self) -> Arc<DuckTxnHandle> {
        let txn = {
            let mut current = self.current_transaction.lock();
            if current.is_none() {
                *current = Some(Arc::new(self.create_meta_transaction()));
            }
            Arc::clone(current.as_ref().unwrap())
        };
        let db_id = self
            .default_db_id()
            .expect("TransactionContext::get_or_create_write_transaction: database instance dropped");
        txn.modify_database(db_id);
        txn.get_transaction(db_id)
    }

    /// 获取当前活跃元事务的引用（C++: `ActiveTransaction()`）。
    ///
    /// # Panics
    /// 若无活跃事务（与 C++ `InternalException` 行为对应）。
    pub fn active_transaction(&self) -> parking_lot::MutexGuard<'_, Option<Arc<MetaTransaction>>> {
        self.current_transaction.lock()
    }

    // ── 自动提交 ──────────────────────────────────────────────────────────────

    /// 是否自动提交（C++: `IsAutoCommit()`）。
    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit.load(Ordering::Relaxed)
    }

    /// 设置自动提交模式（C++: `SetAutoCommit(bool)`）。
    pub fn set_auto_commit(&self, value: bool) {
        self.auto_commit.store(value, Ordering::Relaxed);
        if !value && !self.has_active_transaction() {
            self.begin_transaction()
                .expect("TransactionContext::set_auto_commit(false) failed to start transaction");
        }
    }


    /// 开始新事务（C++: `TransactionContext::BeginTransaction()`）。
    ///
    /// 内部调用 [`begin_transaction_arc`] 并丢弃返回的 Arc。
    ///
    /// # Errors
    /// 若已有活跃事务返回错误。
    pub fn begin_transaction(&self) -> Result<(), TransactionError> {
        let mut current = self.current_transaction.lock();
        if current.is_some() {
            return Err(TransactionError::new(
                "cannot begin transaction: already in transaction",
            ));
        }
        let txn = Arc::new(self.create_meta_transaction());
        *current = Some(Arc::clone(&txn));
        self.auto_commit.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// 提交当前事务（C++: `TransactionContext::Commit()`）。
    ///
    /// # Errors
    /// 提交失败时返回错误（C++: `ThrowException(error)`）。
    pub fn commit(&self) -> Result<(), TransactionError> {
        let txn = self.current_transaction.lock().take().ok_or_else(|| {
            TransactionError::new("TransactionContext::commit called without active transaction")
        })?;
        self.auto_commit.store(true, Ordering::Relaxed);

        // C++: auto error = transaction->Commit();
        let result = txn.commit();
        match result {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    /// 回滚当前事务（C++: `TransactionContext::Rollback(optional_ptr<ErrorData>)`）。
    pub fn rollback(&self) -> Result<(), TransactionError> {
        if let Some(txn) = self.current_transaction.lock().take() {
            self.auto_commit.store(true, Ordering::Relaxed);
            txn.rollback();
        }
        Ok(())
    }

    /// 清除事务记录（C++: `TransactionContext::ClearTransaction()`）。
    pub fn clear_transaction(&self) {
        *self.current_transaction.lock() = None;
    }

    // ── 查询 ID 管理 ──────────────────────────────────────────────────────────

    /// 获取活跃查询 ID（C++: `GetActiveQuery()`）。
    pub fn get_active_query(&self) -> TransactionId {
        self.current_transaction
            .lock()
            .as_ref()
            .map(|txn| txn.get_active_query())
            .unwrap_or(super::types::MAXIMUM_QUERY_ID)
    }

    /// 设置活跃查询 ID（C++: `SetActiveQuery()`）。
    pub fn set_active_query(&self, query_id: TransactionId) {
        if let Some(txn) = self.current_transaction.lock().as_mut() {
            txn.set_active_query(query_id);
        }
    }

    /// 重置为无活跃查询（C++: `ResetActiveQuery()`）。
    pub fn reset_active_query(&self) {
        self.set_active_query(super::types::MAXIMUM_QUERY_ID);
    }

    fn create_meta_transaction(&self) -> MetaTransaction {
        let db = self
            .db_instance
            .upgrade()
            .expect("TransactionContext::create_meta_transaction: database instance dropped");
        MetaTransaction::new(
            Arc::clone(&self.transaction_manager),
            current_timestamp_micros(),
            db.get_new_transaction_number(),
            self.db_instance.clone(),
        )
    }

    fn default_db_id(&self) -> Option<u64> {
        self.db_instance.upgrade().map(|db| db.db_id)
    }
}
