//! `DuckdbEngine` 和 `DuckConnection`：数据库引擎与连接的具体实现。
//!
//! # 架构
//!
//! ```text
//! DuckdbEngine
//!   └── db: Arc<Mutex<InnerDatabase>>   — 单一数据库实例
//!         └── connect() → DuckConnection
//!                           ├── conn: Connection        — 底层连接（ClientContext + TransactionContext）
//!                           ├── db: Arc<Mutex<InnerDatabase>>  — DDL 操作句柄（与 engine.db 同一实例）
//!                           └── schemas: HashSet<String>       — 已注册的 schema 名称
//! ```
//!
//! # 设计说明
//!
//! `DuckdbEngine` 管理**单个**数据库文件（或内存库）。
//! 原先用 `HashMap<String, Arc<Mutex<InnerDatabase>>>` 表示多库，但：
//! - `connect()` 只能连默认库，附加库永远无法操作；
//! - 已移除 `MetaTransaction` 后不再支持跨库事务。
//!
//! 简化为持有单个 `Arc<Mutex<InnerDatabase>>`，所有歧义消除。
//!
//! # 多连接并发示例
//!
//! ```rust
//! let engine = DuckdbEngine::open("mydb.db")?;
//!
//! let mut conn1 = engine.connect();
//! conn1.create_schema("main")?;
//! conn1.create_table("main", "items", columns)?;
//! conn1.begin_transaction()?;
//! conn1.insert("items", &mut chunk)?;
//!
//! // conn2 与 conn1 共享同一个 DatabaseInstance（事务独立）
//! let conn2 = engine.connect();
//! conn2.begin_transaction()?;
//! let data = conn2.scan("items", None)?;
//! conn2.commit()?;
//!
//! conn1.commit()?;
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use super::InnerDatabase;
use super::engine::{EngineError, SchemaInfo, SchemaTableInfo};
use crate::common::types::{DataChunk, LogicalType};
use crate::db::conn::Connection;

// ─── DuckdbEngine ──────────────────────────────────────────────────────────────

/// DuckDB 引擎：管理单个数据库实例，提供连接工厂。
pub struct DuckdbEngine {
    /// 底层数据库实例（Arc 允许多个 DuckConnection 共享）。
    db: Arc<Mutex<InnerDatabase>>,
}

impl DuckdbEngine {
    /// 打开（或创建）数据库文件，返回引擎实例。
    ///
    /// - `path = ":memory:"` → 纯内存数据库，进程退出后数据丢失。
    /// - `path = "mydb.db"` → 持久化文件，自动加载已有数据并回放 WAL。
    pub fn open(path: impl Into<String>) -> Result<Self, EngineError> {
        let db = InnerDatabase::open(path).map_err(|e| format!("open db failed: {:?}", e))?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// 返回数据库文件路径。
    pub fn path(&self) -> String {
        self.db.lock().path().to_string()
    }

    /// 将内存/WAL 数据持久化到磁盘（显式 checkpoint）。
    pub fn checkpoint(&self) -> Result<(), EngineError> {
        self.db
            .lock()
            .checkpoint()
            .map_err(|e| format!("checkpoint failed: {:?}", e))
    }

    /// 返回默认 schema（`"main"`）下的所有表名。
    pub fn tables(&self) -> Vec<String> {
        self.db.lock().tables()
    }

    /// 创建一个新的独立连接。
    ///
    /// 每个 `DuckConnection` 拥有独立的 `ClientContext` 和 `TransactionContext`，
    /// 但与同一引擎上的其他连接共享同一个 `DatabaseInstance`（存储 + 事务管理器）。
    pub fn connect(&self) -> DuckConnection {
        let conn = self.db.lock().connect();
        let mut schemas = HashSet::new();
        schemas.insert("main".to_string());
        DuckConnection {
            conn,
            db: Arc::clone(&self.db),
            schemas,
        }
    }
}

/// 向后兼容的旧类型名。
pub type DuckEngine = DuckdbEngine;

// ─── DuckConnection ────────────────────────────────────────────────────────────

/// 数据库连接（操作层）。
///
/// 通过 [`DuckdbEngine::connect`] 创建，持有：
/// - `conn`：独立的底层连接（事务状态隔离）。
/// - `db`：与引擎共享的数据库实例引用（DDL 修改元数据用）。
/// - `schemas`：本连接可见的 schema 名称集合。
pub struct DuckConnection {
    /// 底层连接（含独立 ClientContext / TransactionContext）。
    conn: Connection,

    /// 数据库实例引用（用于 DDL：`create_table` 需修改全局 tables 映射）。
    ///
    /// 与创建本连接的 `DuckdbEngine.db` 指向同一个对象。
    db: Arc<Mutex<InnerDatabase>>,

    /// 本连接已注册的 schema 名称（默认包含 `"main"`）。
    schemas: HashSet<String>,
}

impl DuckConnection {
    // ── 事务管理 ──────────────────────────────────────────────────────────────

    /// 开启事务（`BEGIN TRANSACTION`）。
    pub fn begin_transaction(&self) -> Result<(), EngineError> {
        self.conn.begin_transaction().map_err(|e| e.to_string())
    }

    /// 提交当前事务。
    pub fn commit(&self) -> Result<(), EngineError> {
        self.conn.commit().map_err(|e| e.to_string())
    }

    /// 回滚当前事务，丢弃所有未提交修改。
    pub fn rollback(&self) -> Result<(), EngineError> {
        self.conn.rollback().map_err(|e| e.to_string())
    }

    // ── DML ───────────────────────────────────────────────────────────────────

    /// 向表插入一批数据。
    ///
    /// `table_name` 支持非限定（`"users"` → 默认 `"main"` schema）
    /// 和限定格式（`"hr.users"`）。
    pub fn insert(&self, table_name: &str, chunk: &mut DataChunk) -> Result<(), EngineError> {
        self.conn.insert_chunk(table_name, chunk)
    }

    /// 按 Row ID 更新指定列。
    pub fn update(
        &self,
        table_name: &str,
        row_ids: &[i64],
        column_ids: &[u64],
        updates: &mut DataChunk,
    ) -> Result<(), EngineError> {
        self.conn
            .update_chunk(table_name, row_ids, column_ids, updates)
    }

    /// 按 Row ID 删除行，返回实际删除行数。
    pub fn delete(&self, table_name: &str, row_ids: &[i64]) -> Result<usize, EngineError> {
        self.conn.delete_chunk(table_name, row_ids)
    }

    /// 扫描表数据，可见当前事务内的未提交写入。
    pub fn scan(
        &self,
        table_name: &str,
        column_ids: Option<Vec<u64>>,
    ) -> Result<Vec<DataChunk>, EngineError> {
        self.conn.scan_chunks(table_name, column_ids)
    }

    // ── Schema / DDL ──────────────────────────────────────────────────────────

    /// 注册 schema 名称（幂等）。
    ///
    /// 注册后方可在该 schema 下执行 `create_table`。
    /// `"main"` 默认已注册，无需显式调用。
    pub fn create_schema(&mut self, schema_name: &str) -> Result<(), EngineError> {
        self.schemas.insert(schema_name.to_string());
        Ok(())
    }

    /// 获取 schema 下所有表的定义。
    ///
    /// # Errors
    /// 若 schema 未通过 [`create_schema`] 注册，返回错误。
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
        Ok(SchemaInfo {
            name: schema_name.to_string(),
            tables: schema_tables,
        })
    }

    /// 在指定 schema 下创建表。
    ///
    /// # Errors
    /// 若 schema 未通过 [`create_schema`] 注册，返回错误。
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

// derive_db_name 已移除：引擎不再需要为数据库注册名称（单库无需 HashMap key）。
