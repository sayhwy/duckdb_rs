//! 事务基类 trait。
//!
//! 对应 C++: `duckdb/transaction/transaction.hpp`
//!
//! # 设计说明
//!
//! C++ 用虚继承：`Transaction` → `DuckTransaction`。
//! Rust 用 trait 对象：`dyn Transaction`。
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class Transaction` | `trait Transaction` |
//! | `virtual ~Transaction()` | `Drop` on concrete type |
//! | `virtual bool IsDuckTransaction()` | `fn is_duck_transaction() -> bool` |
//! | `bool is_read_only` | `fn is_read_only() / set_read_write()` |
//! | `atomic<transaction_t> active_query` | 各实现自行持有 |

use super::types::TransactionId;

// ─── Transaction trait ─────────────────────────────────────────────────────────

/// 所有事务实现必须满足的接口（C++: `class Transaction`）。
pub trait Transaction: Send + Sync {
    /// 是否为只读事务（C++: `Transaction::IsReadOnly()`）。
    fn is_read_only(&self) -> bool;

    /// 将事务提升为读写事务（C++: `Transaction::SetReadWrite()`）。
    ///
    /// 实现须使用内部可变性（如 `AtomicBool`），使得通过 `Arc<dyn Transaction>` 可调用。
    fn set_read_write(&self);

    /// 返回该事务的唯一事务 ID（C++: `Transaction::transaction_id` 字段）。
    ///
    /// 用于 `DuckTransactionManager` 在提交/回滚时定位事务。
    fn transaction_id(&self) -> TransactionId;

    /// 是否为 DuckDB 原生事务（C++: `virtual bool IsDuckTransaction()`）。
    fn is_duck_transaction(&self) -> bool {
        false
    }

    /// 当前活跃查询 ID，无活跃查询时返回 `MAXIMUM_QUERY_ID`
    /// （C++: `atomic<transaction_t> active_query`）。
    fn active_query(&self) -> TransactionId;

    /// 设置活跃查询 ID（C++: `active_query.store()`）。
    fn set_active_query(&self, query_id: TransactionId);
}

// ─── TransactionRef ────────────────────────────────────────────────────────────

/// 对某个具体事务的引用（C++: `Transaction &` 或 `optional_ptr<Transaction>`）。
///
/// 在 Rust 中，`TransactionManager::start_transaction()` 返回 `Arc<dyn Transaction>`，
/// 调用方持有此引用直到 commit/rollback。
pub type TransactionRef = std::sync::Arc<dyn Transaction>;
