//! Connection 和 ClientContext 实现。
//!
//! 对应 DuckDB C++:
//!   `duckdb/main/connection.hpp` / `connection.cpp`
//!   `duckdb/main/client_context.hpp` / `client_context.cpp`
//!
//! # 架构
//!
//! ```text
//! DB (DatabaseInstance)
//!   ├── StorageManager
//!   ├── TransactionManager (shared, global)
//!   └── Tables
//!
//! Connection
//!   └── ClientContext
//!         ├── TransactionContext
//!         │     └── MetaTransaction (optional)
//!         │           └── DuckTransaction (per-database)
//!         ├── LocalStorage
//!         └── Config
//! ```
//!
//! # 事务模型
//!
//! - **Auto-commit 模式**（默认）：每个语句自动开始并提交事务
//! - **Manual 模式**：调用 `begin_transaction()` 开始事务，多个语句共享同一事务，
//!   调用 `commit()` 或 `rollback()` 结束事务
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class Connection` | `struct Connection` |
//! | `shared_ptr<ClientContext> context` | `Arc<Mutex<ClientContext>>` |
//! | `class ClientContext` | `struct ClientContext` |
//! | `TransactionContext transaction` | `transaction: TransactionContext` |
//! | `bool auto_commit` | `auto_commit: AtomicBool` |

pub mod connection;

pub use connection::{Connection, DatabaseInstance, TableHandle};
