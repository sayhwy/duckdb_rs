//! 数据库引擎接口。
//!
//! # 层次设计
//!
//! ```text
//! Engine (trait)           — 数据库管理层：打开/附加库、checkpoint、元信息查询
//!   └── connect()          — 每次调用创建一个独立连接
//!         └── DuckConnection — 连接层：事务、DML、DDL，所有数据操作入口
//! ```
//!
//! 与 DuckDB C++ 对应关系：
//!
//! | Rust | DuckDB C++ |
//! |------|-----------|
//! | `DuckEngine` | `DuckDB` / `DatabaseManager` |
//! | `DuckConnection` | `Connection` / `ClientContext` |
//! | `Arc<MetaTransaction>` | `MetaTransaction` |
//!
//! ## 使用示例
//!
//! ```rust
//! // 打开数据库
//! let engine = DuckEngine::open("mydb.db")?;
//!
//! // 创建连接（可创建多个，每个独立持有事务）
//! let mut conn = engine.connect();
//! conn.create_schema("main")?;
//! conn.create_table("main", "students", vec![
//!     ("id".to_string(), LogicalType::integer()),
//!     ("name".to_string(), LogicalType::varchar()),
//! ])?;
//!
//! // 事务操作
//! let txn = conn.begin_transaction()?;
//! conn.insert(&txn, "students", &mut chunk)?;
//! conn.commit(txn)?;
//!
//! // 可并发创建第二个连接
//! let conn2 = engine.connect();
//! let txn2 = conn2.begin_transaction()?;
//! // ...
//! ```

use crate::common::types::LogicalType;

// ─── Error Type ───────────────────────────────────────────────────────────────

/// 引擎/连接操作的错误类型。
pub type EngineError = String;

// ─── Schema Types ─────────────────────────────────────────────────────────────

/// Schema 下单张表的信息。
pub struct SchemaTableInfo {
    /// 表名。
    pub name: String,
    /// 列定义列表：`(列名, 逻辑类型)`。
    pub columns: Vec<(String, LogicalType)>,
}

/// Schema 信息（命名空间 + 其下所有表的定义）。
pub struct SchemaInfo {
    /// Schema 名称（如 `"main"`）。
    pub name: String,
    /// 该 Schema 下的所有表。
    pub tables: Vec<SchemaTableInfo>,
}

// ─── Engine Trait ─────────────────────────────────────────────────────────────

/// 数据库引擎接口（数据库管理层）。
///
/// 负责打开/附加数据库实例，以及数据库级别的管理操作。
/// 所有数据操作（DML/DDL/事务）需先通过 [`Engine::connect`] 获取连接。
pub trait Engine {
    /// 创建一个新的连接（C++: `Connection(DuckDB &database)`）。
    ///
    /// 每个连接有独立的 `ClientContext` 和 `TransactionContext`，
    /// 可以并发持有多个活跃事务，互不干扰。
    fn connect(&self) -> crate::db::DuckConnection;

    /// 附加额外数据库（对应 DuckDB 的 `ATTACH DATABASE`）。
    fn attach(&mut self, name: &str, path: &str) -> Result<(), EngineError>;

    /// 执行 Checkpoint，将内存/WAL 数据持久化到磁盘。
    fn checkpoint(&self) -> Result<(), EngineError>;

    /// 获取默认数据库中的所有表名。
    fn tables(&self) -> Vec<String>;

    /// 获取默认数据库路径。
    fn path(&self) -> &str;
}
