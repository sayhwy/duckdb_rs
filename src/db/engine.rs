//! 数据库引擎接口。
//!
//! # 对外类型
//!
//! ```text
//! DuckdbEngine            — 数据库管理层：打开/附加库、checkpoint、元信息查询
//!   └── connect()         — 每次调用创建一个独立连接
//!         └── DuckConnection — 连接层：事务、DML、DDL，所有数据操作入口
//! ```
//!
//! 与 DuckDB C++ 对应关系：
//!
//! | Rust | DuckDB C++ |
//! |------|-----------|
//! | `DuckdbEngine` | `DuckDB` / `DatabaseManager` |
//! | `DuckConnection` | `Connection` / `ClientContext` |
//!
//! ## 使用示例
//!
//! ```rust
//! // 打开数据库
//! let engine = DuckdbEngine::open("mydb.db")?;
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
//! conn.begin_transaction()?;
//! conn.insert("students", &mut chunk)?;
//! conn.commit()?;
//!
//! // 可并发创建第二个连接
//! let conn2 = engine.connect();
//! conn2.begin_transaction()?;
//! // ...
//! ```

use crate::common::types::LogicalType;
use crate::storage::data_table::{StorageIndex, TableFilterSet};

// ─── Error Type ───────────────────────────────────────────────────────────────

/// 引擎/连接操作的错误类型。
pub type EngineError = crate::common::errors::Error;

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

/// 存储层可识别的表扫描请求。
///
/// 对齐 DuckDB 的 `column_ids + table_filters` 入口，
/// 不直接承载执行器表达式树。
#[derive(Clone, Default)]
pub struct TableScanRequest {
    pub column_ids: Vec<StorageIndex>,
    pub filters: Option<TableFilterSet>,
}

impl TableScanRequest {
    pub fn new(column_ids: Vec<StorageIndex>) -> Self {
        Self {
            column_ids,
            filters: None,
        }
    }
}

/// Table scan 绑定数据。
///
/// 对齐 DuckDB 的 `TableScanBindData`：
/// 保存绑定阶段解析好的列列表和输出类型。
pub struct TableScanBindData {
    pub request: TableScanRequest,
    pub result_types: Vec<LogicalType>,
}
