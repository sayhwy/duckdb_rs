//! 事务子系统。
//!
//! 对应 C++: `src/transaction/` + `src/include/duckdb/transaction/`
//!
//! # 模块结构
//!
//! ```text
//! transaction/
//! ├── types.rs                    — 基础类型（TransactionId 常量、UndoFlags、状态枚举）
//! ├── undo_buffer_allocator.rs    — UndoBuffer 底层分配器（Slab 链表）
//! ├── undo_buffer.rs              — 事务撤销日志（commit / rollback / cleanup 入口）
//! ├── append_info.rs              — Append 操作 Undo 载荷
//! ├── delete_info.rs              — Delete 操作 Undo 载荷
//! ├── update_info.rs              — Update 操作 Undo 载荷 + MVCC 版本链
//! ├── transaction.rs              — Transaction trait（抽象接口）
//! ├── duck_transaction.rs         — DuckTransaction（具体实现）
//! ├── transaction_manager.rs      — TransactionManager trait
//! ├── duck_transaction_manager.rs — DuckTransactionManager（具体实现）
//! ├── meta_transaction.rs         — MetaTransaction（跨数据库元事务）
//! ├── transaction_context.rs      — TransactionContext（客户端事务状态）
//! ├── (local_storage 已移至 crate::storage::local_storage)
//! ├── commit_state.rs             — Commit / RevertCommit 状态机
//! ├── rollback_state.rs           — Rollback 状态机
//! ├── cleanup_state.rs            — Cleanup 状态机（MVCC 旧版本回收）
//! └── wal_write_state.rs          — WAL 写入状态机
//! ```

pub mod types;
pub mod undo_buffer_allocator;
pub mod undo_buffer;
pub mod append_info;
pub mod delete_info;
pub mod update_info;
pub mod transaction;
pub mod duck_transaction;
pub mod transaction_manager;
pub mod duck_transaction_manager;
pub mod meta_transaction;
pub mod transaction_context;
// local_storage 已移至 crate::storage::local_storage
pub mod commit_state;
pub mod rollback_state;
pub mod cleanup_state;
pub mod wal_write_state;
