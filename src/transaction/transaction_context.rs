//! 客户端事务上下文（单数据库版本）。
//!
//! # 简化说明
//!
//! 原设计通过 `MetaTransaction` 支持跨多个附加数据库的协调提交。
//! 本版本只支持单数据库场景，直接持有 `Arc<DuckTxnHandle>`，
//! 去掉了 MetaTransaction / db_id 间接层和 Weak<DatabaseInstance>。
//!
//! # 职责
//!
//! - 跟踪当前活跃事务（`current_transaction`）。
//! - 管理自动提交（`auto_commit`）模式。
//! - 提供 `begin_transaction / commit / rollback` 统一入口。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use super::duck_transaction_manager::{DuckTransactionManager, DuckTxnHandle};
use super::transaction::Transaction;
use super::transaction_manager::ErrorData;
use super::types::{TransactionId, MAXIMUM_QUERY_ID};

// ─── TransactionError ──────────────────────────────────────────────────────────

/// 事务操作错误。
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
        Self { message: err.message }
    }
}

// ─── TransactionContext ────────────────────────────────────────────────────────

/// 每个连接独立的事务状态。
///
/// 直接持有 `Arc<DuckTxnHandle>`（原先通过 `MetaTransaction` 间接访问）。
/// `db` 以强引用持有，因为 `TransactionContext` 的生命周期严格短于
/// `DatabaseInstance`（Connection 被 drop 后 TransactionContext 随之消亡）。
pub struct TransactionContext {
    /// 是否自动提交。
    auto_commit: AtomicBool,

    /// 当前活跃事务句柄（None 表示无活跃事务）。
    current_transaction: Mutex<Option<Arc<DuckTxnHandle>>>,

    /// 事务管理器（从 DatabaseInstance 借用的 Arc）。
    transaction_manager: Arc<DuckTransactionManager>,

    /// 数据库实例（用于 commit_transaction 调用）。
    db: Arc<crate::db::conn::DatabaseInstance>,
}

impl TransactionContext {
    /// 构造。
    pub fn new(
        transaction_manager: Arc<DuckTransactionManager>,
        db: Arc<crate::db::conn::DatabaseInstance>,
    ) -> Self {
        Self {
            auto_commit: AtomicBool::new(true),
            current_transaction: Mutex::new(None),
            transaction_manager,
            db,
        }
    }

    // ── 状态查询 ──────────────────────────────────────────────────────────────

    /// 是否有活跃事务。
    pub fn has_active_transaction(&self) -> bool {
        self.current_transaction.lock().is_some()
    }

    /// 尝试获取当前活跃事务句柄（不创建新事务）。
    pub fn try_get_transaction(&self) -> Option<Arc<DuckTxnHandle>> {
        self.current_transaction.lock().as_ref().map(Arc::clone)
    }

    // ── 事务获取 ──────────────────────────────────────────────────────────────

    /// 获取或创建只读事务（用于 SELECT 类操作）。
    ///
    /// 若无活跃事务，以 `is_read_only=true` 开启新事务。
    pub fn get_or_create_transaction(&self) -> Arc<DuckTxnHandle> {
        let mut current = self.current_transaction.lock();
        if current.is_none() {
            *current = Some(self.transaction_manager.start_duck_transaction(true));
        }
        Arc::clone(current.as_ref().unwrap())
    }

    /// 获取或创建写事务（用于 INSERT / UPDATE / DELETE）。
    ///
    /// 若无活跃事务，以 `is_read_only=false` 开启新事务；
    /// 若已有事务（只读），将其升级为读写。
    pub fn get_or_create_write_transaction(&self) -> Arc<DuckTxnHandle> {
        let mut current = self.current_transaction.lock();
        if current.is_none() {
            *current = Some(self.transaction_manager.start_duck_transaction(false));
        }
        let txn = Arc::clone(current.as_ref().unwrap());
        // 确保事务为读写模式（幂等）
        txn.set_read_write();
        txn
    }

    // ── 事务生命周期 ──────────────────────────────────────────────────────────

    /// 开始新事务（对应 `BEGIN TRANSACTION`）。
    ///
    /// # Errors
    /// 若已有活跃事务，返回错误。
    pub fn begin_transaction(&self) -> Result<(), TransactionError> {
        let mut current = self.current_transaction.lock();
        if current.is_some() {
            return Err(TransactionError::new(
                "cannot begin transaction: already in transaction",
            ));
        }
        *current = Some(self.transaction_manager.start_duck_transaction(false));
        self.auto_commit.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// 提交当前事务。
    ///
    /// # Errors
    /// 若无活跃事务或提交失败，返回错误。
    pub fn commit(&self) -> Result<(), TransactionError> {
        let txn = self
            .current_transaction
            .lock()
            .take()
            .ok_or_else(|| TransactionError::new("commit called without active transaction"))?;
        self.auto_commit.store(true, Ordering::Relaxed);

        let txn_ref = Arc::clone(&txn) as Arc<dyn Transaction>;
        match self.transaction_manager.commit_transaction(&self.db, &txn_ref) {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    /// 回滚当前事务。
    ///
    /// 若无活跃事务，静默成功（幂等）。
    pub fn rollback(&self) -> Result<(), TransactionError> {
        if let Some(txn) = self.current_transaction.lock().take() {
            self.auto_commit.store(true, Ordering::Relaxed);
            self.transaction_manager.rollback_duck_transaction(&txn);
        }
        Ok(())
    }

    /// 清除事务记录，不执行提交或回滚（内部使用）。
    pub fn clear_transaction(&self) {
        *self.current_transaction.lock() = None;
    }

    // ── 自动提交 ──────────────────────────────────────────────────────────────

    /// 是否自动提交模式。
    pub fn is_auto_commit(&self) -> bool {
        self.auto_commit.load(Ordering::Relaxed)
    }

    /// 设置自动提交模式。
    ///
    /// 设置为 `false` 时若无活跃事务则立即开启一个。
    pub fn set_auto_commit(&self, value: bool) {
        self.auto_commit.store(value, Ordering::Relaxed);
        if !value && !self.has_active_transaction() {
            self.begin_transaction()
                .expect("set_auto_commit(false): failed to start transaction");
        }
    }

    // ── 查询 ID 管理 ──────────────────────────────────────────────────────────

    /// 获取当前活跃查询 ID。
    pub fn get_active_query(&self) -> TransactionId {
        self.current_transaction
            .lock()
            .as_ref()
            .map(|txn| txn.active_query())
            .unwrap_or(MAXIMUM_QUERY_ID)
    }

    /// 设置当前活跃查询 ID。
    pub fn set_active_query(&self, query_id: TransactionId) {
        if let Some(txn) = self.current_transaction.lock().as_ref() {
            txn.set_active_query(query_id);
        }
    }

    /// 重置为无活跃查询。
    pub fn reset_active_query(&self) {
        self.set_active_query(MAXIMUM_QUERY_ID);
    }
}
