//! MetaTransaction — 已移除。
//!
//! 原设计用于协调跨多个附加数据库（multi-attach）的分布式提交。
//! 当前项目只支持单数据库场景，该层已被删除：
//!
//! - `TransactionContext` 直接持有 `Arc<DuckTxnHandle>`
//! - 无需 `db_id` 路由，无需 `Weak<DatabaseInstance>`
//! - 提交 / 回滚直接调用 `DuckTransactionManager::commit_transaction`
//!
//! 若将来需要多库支持，可在此文件重新引入 MetaTransaction，
//! 但需同时更新 `transaction_context.rs` 和 `conn.rs`。
