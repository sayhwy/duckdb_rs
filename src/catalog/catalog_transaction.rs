//! Catalog 事务包装。
//!
//! 对应 C++: `duckdb/catalog/catalog_transaction.hpp`
//!
//! # 设计说明
//!
//! | C++ | Rust |
//! |-----|------|
//! | `optional_ptr<DatabaseInstance> db` | `db_oid: u64`（数据库 OID） |
//! | `optional_ptr<Transaction> transaction` | `transaction_id: TransactionId` |
//! | `transaction_t transaction_id` | `transaction_id: TransactionId` |
//! | `transaction_t start_time` | `start_time: TransactionId` |

use crate::transaction::types::{TRANSACTION_ID_START, TransactionId};
use crate::transaction::duck_transaction_manager::{DuckTransactionManager, DuckTxnHandle};
use std::sync::Arc;

// ─── CatalogTransaction ────────────────────────────────────────────────────────

/// Catalog 操作的事务上下文（C++: `struct CatalogTransaction`）。
///
/// 包含进行 catalog 操作所需的所有事务元数据。
/// 在 Rust 中去除了直接指针，改用 ID 值传递。
#[derive(Clone)]
pub struct CatalogTransaction {
    /// 数据库实例 OID（用于区分不同的 AttachedDatabase）。
    pub db_oid: u64,
    /// 当前事务的写入 ID（C++: `transaction_t transaction_id`）。
    pub transaction_id: TransactionId,
    /// 事务开始时间（用于 MVCC 可见性判断）（C++: `transaction_t start_time`）。
    pub start_time: TransactionId,
    /// 是否拥有客户端上下文（用于区分系统事务和用户事务）。
    pub has_context: bool,
    /// 当前事务句柄（对应 DuckDB `CatalogTransaction::transaction`）。
    pub transaction: Option<Arc<DuckTxnHandle>>,
    /// 当前事务所属事务管理器。
    pub transaction_manager: Option<Arc<DuckTransactionManager>>,
}

impl CatalogTransaction {
    /// 创建用户事务上下文（C++: `CatalogTransaction(Catalog&, ClientContext&)`）。
    pub fn new(
        db_oid: u64,
        transaction_id: TransactionId,
        start_time: TransactionId,
        transaction: Option<Arc<DuckTxnHandle>>,
        transaction_manager: Option<Arc<DuckTransactionManager>>,
    ) -> Self {
        Self {
            db_oid,
            transaction_id,
            start_time,
            has_context: true,
            transaction,
            transaction_manager,
        }
    }

    /// 创建系统事务上下文（C++: `CatalogTransaction::GetSystemTransaction(DatabaseInstance&)`）。
    ///
    /// 系统事务的 start_time = 0，transaction_id = 0，可以看到所有已提交数据。
    pub fn system(db_oid: u64) -> Self {
        Self {
            db_oid,
            transaction_id: 1,
            start_time: 1,
            has_context: false,
            transaction: None,
            transaction_manager: None,
        }
    }

    /// 是否为系统事务（无客户端上下文）。
    pub fn is_system(&self) -> bool {
        !self.has_context
    }

    /// 是否为活跃未提交事务（事务 ID 在活跃范围内）。
    pub fn is_active_transaction(&self) -> bool {
        self.transaction_id >= TRANSACTION_ID_START
    }

    /// 最大可见时间戳（系统事务可以看到所有提交的数据）。
    pub fn max_visible_timestamp(&self) -> TransactionId {
        if self.is_system() {
            u64::MAX
        } else {
            self.start_time
        }
    }
}

// ─── MVCC 可见性辅助函数 ────────────────────────────────────────────────────────

/// 判断给定时间戳对事务是否可见（C++: `CatalogSet::UseTimestamp`）。
///
/// 可见条件：
/// 1. 时间戳是已提交的事务（< TRANSACTION_ID_START），且在本事务开始之前提交
/// 2. 时间戳就是本事务自身
pub fn is_visible(txn: &CatalogTransaction, timestamp: TransactionId) -> bool {
    if timestamp == txn.transaction_id {
        // 本事务自身的修改总是可见
        return true;
    }
    if timestamp >= TRANSACTION_ID_START {
        // 其他活跃事务的修改不可见
        return false;
    }
    // 已提交事务：检查是否在本事务开始之前提交
    timestamp < txn.start_time || txn.is_system()
}

/// 判断时间戳是否对应一个已提交事务（C++: `CatalogSet::IsCommitted`）。
pub fn is_committed(timestamp: TransactionId) -> bool {
    timestamp < TRANSACTION_ID_START
}

/// 判断是否由其他活跃事务创建（C++: `CatalogSet::CreatedByOtherActiveTransaction`）。
pub fn created_by_other_active_transaction(
    txn: &CatalogTransaction,
    timestamp: TransactionId,
) -> bool {
    timestamp != txn.transaction_id && timestamp >= TRANSACTION_ID_START
}

/// 判断是否在本事务开始后提交（C++: `CatalogSet::CommittedAfterStarting`）。
pub fn committed_after_starting(txn: &CatalogTransaction, timestamp: TransactionId) -> bool {
    is_committed(timestamp) && timestamp >= txn.start_time
}

/// 判断是否存在写-写冲突（C++: `CatalogSet::HasConflict`）。
pub fn has_conflict(txn: &CatalogTransaction, timestamp: TransactionId) -> bool {
    timestamp != txn.transaction_id && timestamp >= TRANSACTION_ID_START
}
