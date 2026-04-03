//! `DuckdbEngine` 和 `DuckConnection`：数据库引擎与连接的具体实现。
//!
//! # 架构
//!
//! ```text
//! DuckdbEngine
//!   └── databases: HashMap<String, Arc<Mutex<InnerDatabase>>>   — 已注册的数据库实例
//!         └── connect() → DuckConnection             — 每次调用创建独立连接
//!                           ├── conn: Connection     — 底层连接（ClientContext）
//!                           ├── db: Arc<Mutex<InnerDatabase>>   — 数据库引用（用于 DDL）
//!                           └── schemas: HashSet     — 已注册 Schema
//! ```
//!
//! # 多连接并发示例
//!
//! ```rust
//! let engine = DuckdbEngine::open("mydb.db")?;
//!
//! // 连接 1：写入
//! let mut conn1 = engine.connect();
//! conn1.create_table("main", "items", columns)?;
//! conn1.begin_transaction()?;
//! conn1.insert("items", &mut chunk)?;
//!
//! // 连接 2：独立读取（并发）
//! let conn2 = engine.connect();
//! conn2.begin_transaction()?;
//! let data = conn2.scan("items", None)?;
//! conn2.commit()?;
//!
//! conn1.commit()?;
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::types::{DataChunk, LogicalType};
use crate::db::conn::Connection;
use super::InnerDatabase;
use super::engine::{EngineError, SchemaInfo, SchemaTableInfo};

// ─── DuckdbEngine ──────────────────────────────────────────────────────────────

/// DuckDB 引擎（数据库管理层）。
///
/// 负责打开/附加数据库实例。所有数据操作通过 [`DuckdbEngine::connect`] 获取的
/// [`DuckConnection`] 进行，每个连接独立持有 `ClientContext` 和事务状态。
pub struct DuckdbEngine {
    /// 已注册的数据库实例（名称 → InnerDatabase）。
    ///
    /// 用 `Arc<Mutex<InnerDatabase>>` 封装，使多个 `DuckConnection` 可共享同一数据库句柄以执行 DDL。
    databases: HashMap<String, Arc<Mutex<InnerDatabase>>>,

    /// 默认数据库名称。
    default_db: String,

    /// 默认数据库路径，避免从锁保护的内部状态借出 `&str`。
    default_path: String,
}

impl DuckdbEngine {
    /// 打开（或创建）数据库，返回引擎实例。
    ///
    /// 打开的数据库注册为默认库，之后可通过 [`DuckdbEngine::connect`] 获取连接操作。
    ///
    /// # 参数
    /// - `path`：数据库文件路径；使用 `":memory:"` 创建内存数据库。
    pub fn open(path: impl Into<String>) -> Result<Self, EngineError> {
        let path = path.into();
        let db_name = derive_db_name(&path);
        let db = InnerDatabase::open(path.clone()).map_err(|e| format!("open db failed: {:?}", e))?;
        let mut databases = HashMap::new();
        databases.insert(db_name.clone(), Arc::new(Mutex::new(db)));
        Ok(Self { databases, default_db: db_name, default_path: path })
    }

    /// 获取默认数据库路径。
    pub fn path(&self) -> &str {
        &self.default_path
    }

    /// 执行 Checkpoint，将内存/WAL 数据持久化到磁盘。
    pub fn checkpoint(&self) -> Result<(), EngineError> {
        let db = self.default_db_arc()?;
        db.lock()
            .checkpoint()
            .map_err(|e| format!("checkpoint failed: {:?}", e))
    }

    /// 获取默认数据库中的所有表名。
    pub fn tables(&self) -> Vec<String> {
        self.default_db_arc()
            .map(|db| db.lock().tables())
            .unwrap_or_default()
    }

    /// 创建一个新的独立连接，连接到默认数据库。
    ///
    /// 每次调用返回一个全新的 [`DuckConnection`]，拥有独立的 `ClientContext`
    /// 和 `TransactionContext`。多个连接可以并发持有各自的事务。
    ///
    /// # 示例
    ///
    /// ```rust
    /// let engine = DuckdbEngine::open("mydb.db")?;
    /// let conn1 = engine.connect();   // 连接 1
    /// let conn2 = engine.connect();   // 连接 2（独立，并发安全）
    /// ```
    pub fn connect(&self) -> DuckConnection {
        let db = self.default_db_arc().expect("default database not found");
        let conn = db.lock().connect();
        let mut schemas = HashSet::new();
        schemas.insert("main".to_string());
        DuckConnection { conn, db, schemas }
    }

    // ── 内部辅助 ──────────────────────────────────────────────────────────────

    pub fn attach(&mut self, name: &str, path: &str) -> Result<(), EngineError> {
        let db = InnerDatabase::open(path).map_err(|e| format!("open db failed: {:?}", e))?;
        self.databases.insert(name.to_string(), Arc::new(Mutex::new(db)));
        Ok(())
    }

    fn default_db_arc(&self) -> Result<Arc<Mutex<InnerDatabase>>, EngineError> {
        self.databases
            .get(&self.default_db)
            .cloned()
            .ok_or_else(|| format!("default database '{}' not found", self.default_db))
    }
}

/// 向后兼容的旧类型名。
pub type DuckEngine = DuckdbEngine;

// ─── DuckConnection ────────────────────────────────────────────────────────────

/// 数据库连接（操作层）。
///
/// 通过 [`DuckdbEngine::connect`] 创建，每个 `DuckConnection` 拥有：
/// - 独立的 [`Connection`]（含 `ClientContext` 和 `TransactionContext`）
/// - 数据库引用（用于 DDL 操作）
/// - 本连接已注册的 Schema 集合
///
pub struct DuckConnection {
    /// 底层连接（C++: `Connection`）。
    conn: Connection,

    /// 数据库引用（用于 DDL：create_table 等需要修改元数据）。
    db: Arc<Mutex<InnerDatabase>>,

    /// 本连接已注册的 Schema 集合（C++: catalog schema entries）。
    schemas: HashSet<String>,
}

impl DuckConnection {
    // ── 事务管理 ──────────────────────────────────────────────────────────────

    /// 开启当前连接上的事务。
    pub fn begin_transaction(&self) -> Result<(), EngineError> {
        self.conn.begin_transaction().map_err(|e| e.to_string())
    }

    /// 提交当前连接上的事务。
    pub fn commit(&self) -> Result<(), EngineError> {
        self.conn.commit().map_err(|e| e.to_string())
    }

    /// 回滚当前连接上的事务，丢弃所有未提交修改。
    pub fn rollback(&self) -> Result<(), EngineError> {
        self.conn.rollback().map_err(|e| e.to_string())
    }

    // ── CRUD ──────────────────────────────────────────────────────────────────

    /// 在事务内向表插入一批数据。
    pub fn insert(
        &self,
        table_name: &str,
        chunk: &mut DataChunk,
    ) -> Result<(), EngineError> {
        self.conn.insert_chunk(table_name, chunk)
    }

    /// 在事务内按 Row ID 更新指定列。
    pub fn update(
        &self,
        table_name: &str,
        row_ids: &[i64],
        column_ids: &[u64],
        updates: &mut DataChunk,
    ) -> Result<(), EngineError> {
        self.conn.update_chunk(table_name, row_ids, column_ids, updates)
    }

    /// 在事务内按 Row ID 删除行，返回实际删除行数。
    pub fn delete(
        &self,
        table_name: &str,
        row_ids: &[i64],
    ) -> Result<usize, EngineError> {
        self.conn.delete_chunk(table_name, row_ids)
    }

    /// 在事务内扫描表数据，可见当前事务内的未提交写入。
    pub fn scan(
        &self,
        table_name: &str,
        column_ids: Option<Vec<u64>>,
    ) -> Result<Vec<DataChunk>, EngineError> {
        self.conn.scan_chunks(table_name, column_ids)
    }

    // ── Schema / DDL 管理 ────────────────────────────────────────────────────

    /// 注册 Schema 名称。若已存在则幂等成功。
    pub fn create_schema(&mut self, schema_name: &str) -> Result<(), EngineError> {
        self.schemas.insert(schema_name.to_string());
        Ok(())
    }

    /// 获取 Schema 信息，包含该 Schema 下所有表的列定义。
    pub fn get_schema(&self, schema_name: &str) -> Result<SchemaInfo, EngineError> {
        if !self.schemas.contains(schema_name) {
            return Err(format!("Schema '{}' not found", schema_name));
        }
        let db = self.db.lock();
        let tables_guard = db.instance().tables.lock();
        let schema_tables: Vec<SchemaTableInfo> = tables_guard
            .values()
            .filter(|t| t.catalog_entry.base.schema_name == schema_name)
            .map(|t| SchemaTableInfo {
                name: t.catalog_entry.base.fields().name.clone(),
                columns: t
                    .storage
                    .column_definitions
                    .iter()
                    .map(|col| (col.name.clone(), col.logical_type.clone()))
                    .collect(),
            })
            .collect();
        Ok(SchemaInfo { name: schema_name.to_string(), tables: schema_tables })
    }

    /// 在指定 Schema 下创建表。
    ///
    /// # 错误
    /// 若 `schema` 未通过 [`create_schema`] 注册，返回错误。
    pub fn create_table(
        &mut self,
        schema: &str,
        table: &str,
        columns: Vec<(String, LogicalType)>,
    ) -> Result<(), EngineError> {
        if !self.schemas.contains(schema) {
            return Err(format!(
                "Schema '{}' not found. Call create_schema(\"{}\") first.",
                schema, schema
            ));
        }
        self.db.lock().create_table(schema, table, columns);
        Ok(())
    }
}

// ─── 内部工具函数 ─────────────────────────────────────────────────────────────

/// 从路径推导数据库注册名称。
///
/// - `":memory:"` → `"memory"`
/// - `"path/to/mydb.db"` → `"mydb"`
fn derive_db_name(path: &str) -> String {
    if path == ":memory:" {
        return "memory".to_string();
    }
    std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("duckdb")
        .to_string()
}
