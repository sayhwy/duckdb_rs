//! 事务管理器抽象接口。
//!
//! 对应 C++: `duckdb/transaction/transaction_manager.hpp`
//!
//! # 设计说明
//!
//! C++ 用虚基类 `TransactionManager`，每个附加数据库（`AttachedDatabase`）
//! 持有一个具体实现（DuckDB 使用 `DuckTransactionManager`）。
//!
//! Rust 用 `trait TransactionManager`，具体实现为 `DuckTransactionManager`。
//! 外部持有 `Arc<dyn TransactionManager>` 或直接 `Arc<DuckTransactionManager>`。

use std::collections::HashMap;
use std::sync::Arc;

use super::transaction::TransactionRef;
use crate::storage::data_table::DataTable;

// ─── ErrorData ─────────────────────────────────────────────────────────────────

/// 操作错误（C++: `ErrorData`）。
/// 实际实现后替换为完整的错误类型。
#[derive(Debug, Clone)]
pub struct ErrorData {
    pub message: String,
}

impl ErrorData {
    pub fn ok() -> Option<Self> {
        None
    }
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            message: msg.into(),
        }
    }
}

// ─── TransactionManager trait ──────────────────────────────────────────────────

/// 事务管理器接口（C++: `class TransactionManager`，纯虚）。
///
/// 每个附加数据库持有一个实现此 trait 的对象，负责：
/// - 分配 start_time / transaction_id。
/// - 维护活跃事务列表。
/// - 触发 checkpoint。
pub trait TransactionManager: Send + Sync {
    /// 开启新事务，返回事务引用（C++: `virtual Transaction &StartTransaction()`）。
    fn start_transaction(&self) -> TransactionRef;

    /// 提交事务。失败时返回 `Some(ErrorData)`（C++: `virtual ErrorData CommitTransaction()`）。
    ///
    /// `tables` 用于 `LocalStorage::commit()`（C++: 通过 `AttachedDatabase` 访问存储层）。
    fn commit_transaction(
        &self,
        transaction: &TransactionRef,
        tables: &HashMap<u64, Arc<DataTable>>,
    ) -> Option<ErrorData>;

    /// 回滚事务（C++: `virtual void RollbackTransaction()`）。
    fn rollback_transaction(&self, transaction: &TransactionRef);

    /// 触发检查点（C++: `virtual void Checkpoint(ClientContext&, bool force)`）。
    fn checkpoint(&self, force: bool);

    /// 是否为 DuckDB 原生实现（C++: `virtual bool IsDuckTransactionManager()`）。
    fn is_duck_transaction_manager(&self) -> bool {
        false
    }
}
