//! Connection 实现。
//!
//! 对应 DuckDB C++: `duckdb/main/connection.hpp` / `connection.cpp`
//!
//! # 层次结构
//!
//! ```text
//! Connection
//!   └── Arc<ClientContext>
//!         ├── db: Arc<DatabaseInstance>     — 共享数据库状态
//!         └── transaction: TransactionContext
//!               ├── Arc<DuckTransactionManager>
//!               ├── Arc<DatabaseInstance>   — commit/rollback 时使用
//!               └── Mutex<Option<Arc<DuckTxnHandle>>>   — 当前活跃事务
//! ```
//!
//! # 设计说明
//!
//! - `Connection` 不再持有独立的 `Arc<DatabaseInstance>`；
//!   所有数据库访问均通过 `context.db` 进行。
//!   这消除了原有的双重引用（`Connection.db` 与 `ClientContext.db` 指向同一对象）。
//!
//! - DML 操作（insert/update/delete/scan）不持有 `ClientContext` 锁跨越 I/O；
//!   获取事务句柄后立即释放锁，避免与 auto_commit 路径的死锁。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::catalog::PhysicalIndex;
use crate::catalog::TableCatalogEntry;
use crate::common::errors::{Result, anyhow};
use crate::common::types::{DataChunk, LogicalType, STANDARD_VECTOR_SIZE};
use crate::storage::data_table::{DataTable, StorageIndex};
use crate::storage::storage_manager::StorageManager;
use crate::transaction::duck_transaction_manager::{DuckTransactionManager, DuckTxnHandle};
use crate::transaction::transaction_context::TransactionContext;

// ─── ConnectionId ──────────────────────────────────────────────────────────────

/// 连接 ID 类型。
pub type ConnectionId = u64;

static CONNECTION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_connection_id() -> ConnectionId {
    CONNECTION_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ─── DatabaseInstance ──────────────────────────────────────────────────────────

/// 数据库实例（共享状态，多连接共享同一实例）。
///
/// 对应 C++: `class DatabaseInstance`
pub struct DatabaseInstance {
    /// 数据库唯一 ID。
    pub db_id: u64,

    /// 数据库文件路径。
    pub path: String,

    /// 存储管理器。
    pub storage_manager: Arc<crate::storage::storage_manager::SingleFileStorageManager>,

    /// 事务管理器（全局单例，所有连接共享）。
    pub transaction_manager: Arc<DuckTransactionManager>,

    /// 表集合（(schema, table) → 句柄，均小写）。
    ///
    /// key 为 `(schema_name, table_name)` 的小写元组，确保：
    /// - 不同 schema 下同名表不会相互覆盖；
    /// - WAL 回放可以精确路由到正确表。
    pub tables: Mutex<HashMap<(String, String), TableHandle>>,

    /// 全局事务 ID 计数器。
    transaction_counter: AtomicU64,

    /// Block 分配大小。
    pub block_alloc_size: u64,
}

static DB_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl DatabaseInstance {
    pub fn new(
        path: String,
        storage_manager: Arc<crate::storage::storage_manager::SingleFileStorageManager>,
        transaction_manager: Arc<DuckTransactionManager>,
        block_alloc_size: u64,
    ) -> Self {
        Self {
            db_id: DB_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            path,
            storage_manager,
            transaction_manager,
            tables: Mutex::new(HashMap::new()),
            transaction_counter: std::sync::atomic::AtomicU64::new(0),
            block_alloc_size,
        }
    }

    /// 获取新的事务 ID（单调递增）。
    pub fn get_new_transaction_number(&self) -> u64 {
        self.transaction_counter.fetch_add(1, Ordering::Relaxed)
    }
}

// ─── TableHandle ───────────────────────────────────────────────────────────────

/// 表句柄：目录条目 + 存储引用。
#[derive(Clone)]
pub struct TableHandle {
    pub catalog_entry: TableCatalogEntry,
    pub storage: Arc<DataTable>,
}

// ─── ClientContext ─────────────────────────────────────────────────────────────

/// 每个连接的运行时上下文。
///
/// 对应 C++: `class ClientContext`
pub struct ClientContext {
    /// 数据库实例引用。
    pub db: Arc<DatabaseInstance>,

    /// 事务上下文（begin / commit / rollback 入口）。
    pub transaction: TransactionContext,

    /// 连接 ID。
    pub connection_id: ConnectionId,

    /// 是否被中断。
    interrupted: std::sync::atomic::AtomicBool,
}

impl ClientContext {
    pub fn new(db: Arc<DatabaseInstance>) -> Self {
        let transaction_manager = db.transaction_manager.clone();
        Self {
            transaction: TransactionContext::new(transaction_manager, db.clone()),
            db,
            connection_id: next_connection_id(),
            interrupted: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Relaxed);
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::Relaxed)
    }

    pub fn reset_interrupt(&self) {
        self.interrupted.store(false, Ordering::Relaxed);
    }

    pub fn begin_transaction(&self) -> Result<()> {
        self.transaction.begin_transaction()
    }

    pub fn commit(&self) -> Result<()> {
        self.transaction.commit()
    }

    pub fn rollback(&self) -> Result<()> {
        self.transaction.rollback()
    }

    pub fn set_auto_commit(&self, value: bool) {
        self.transaction.set_auto_commit(value);
    }

    pub fn is_auto_commit(&self) -> bool {
        self.transaction.is_auto_commit()
    }

    pub fn has_active_transaction(&self) -> bool {
        self.transaction.has_active_transaction()
    }

    /// 获取当前事务句柄（不创建新事务）。
    pub fn active_transaction(&self) -> Option<Arc<DuckTxnHandle>> {
        self.transaction.try_get_transaction()
    }
}

// ─── Connection ────────────────────────────────────────────────────────────────

/// 数据库连接（高层 API）。
///
/// 对应 C++: `class Connection`
///
/// # 设计
///
/// `Connection` 不再持有独立的 `Arc<DatabaseInstance>`。
/// 所有数据库访问通过 `context.db` 进行。
/// DML 操作先短暂锁 context 取得所需句柄，再释放锁执行 I/O，
/// 避免在 I/O 路径上持有粗粒度锁。
pub struct Connection {
    /// 客户端上下文（含 db 引用和事务状态）。
    pub(crate) context: Arc<ClientContext>,

    /// 连接 ID（缓存，避免每次锁 context）。
    connection_id: ConnectionId,
}

impl Connection {
    pub fn new(db: Arc<DatabaseInstance>) -> Self {
        let context = Arc::new(ClientContext::new(db));
        let connection_id = context.connection_id;
        Self {
            context,
            connection_id,
        }
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    // ── 事务控制 ──────────────────────────────────────────────────────────────

    pub fn begin_transaction(&self) -> Result<()> {
        self.context.begin_transaction()
    }

    pub fn commit(&self) -> Result<()> {
        self.context.commit()
    }

    pub fn rollback(&self) -> Result<()> {
        self.context.rollback()
    }

    pub fn set_auto_commit(&self, value: bool) {
        self.context.set_auto_commit(value);
    }

    pub fn is_auto_commit(&self) -> bool {
        self.context.is_auto_commit()
    }

    pub fn has_active_transaction(&self) -> bool {
        self.context.has_active_transaction()
    }

    pub fn get_transaction(&self) -> Option<Arc<DuckTxnHandle>> {
        self.context.transaction.try_get_transaction()
    }

    pub fn interrupt(&self) {
        self.context.interrupt();
    }

    // ── 数据库访问辅助 ────────────────────────────────────────────────────────

    /// 获取数据库实例引用（克隆 Arc，短暂锁 context）。
    pub fn database(&self) -> Arc<DatabaseInstance> {
        self.context.db.clone()
    }

    /// 获取存储管理器引用。
    pub fn storage_manager(
        &self,
    ) -> Arc<crate::storage::storage_manager::SingleFileStorageManager> {
        self.context.db.storage_manager.clone()
    }

    /// 查找表句柄（短暂锁 context + tables）。
    ///
    /// `table_name` 支持两种格式：
    /// - 非限定：`"users"`  → 在默认 schema `"main"` 下查找
    /// - 限定：  `"main.users"` → 在指定 schema 下查找
    pub(crate) fn get_table(&self, table_name: &str) -> Result<TableHandle> {
        let key = parse_table_name(table_name);
        self.context
            .db
            .tables
            .lock()
            .get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("table '{}' not found", table_name))
    }

    // ── 事务获取辅助（短暂锁，立即释放）────────────────────────────────────

    /// 在 auto_commit 模式下开始事务，返回"是否需要 auto-commit"标志。
    ///
    /// 调用方在操作完成后应调用 `commit_if_auto_commit`。
    fn ensure_write_transaction(&self) -> Result<bool> {
        let auto_commit = self.is_auto_commit();
        if auto_commit && !self.has_active_transaction() {
            self.begin_transaction()?;
        }
        Ok(auto_commit)
    }

    /// 获取写事务句柄（短暂锁 context 后释放）。
    fn acquire_write_transaction(&self) -> Arc<DuckTxnHandle> {
        self.context.transaction.get_or_create_write_transaction()
    }

    /// 获取读事务句柄（短暂锁 context 后释放）。
    pub(crate) fn acquire_read_transaction(&self) -> Arc<DuckTxnHandle> {
        if let Some(txn) = self.get_transaction() {
            txn
        } else {
            self.context.transaction.get_or_create_transaction()
        }
    }

    /// 若 `auto_commit` 为 true 则提交。
    fn commit_if_auto_commit(&self, auto_commit: bool) -> Result<()> {
        if auto_commit {
            self.context.commit()?;
        }
        Ok(())
    }

    fn build_row_id_vector(row_ids: &[i64]) -> crate::common::types::Vector {
        let mut v = crate::common::types::Vector::with_capacity(
            crate::common::types::LogicalType::bigint(),
            row_ids.len(),
        );
        for (idx, &row_id) in row_ids.iter().enumerate() {
            let base = idx * 8;
            v.raw_data_mut()[base..base + 8].copy_from_slice(&row_id.to_le_bytes());
        }
        v
    }

    fn build_physical_column_ids(column_ids: &[u64]) -> Vec<PhysicalIndex> {
        column_ids
            .iter()
            .copied()
            .map(|idx| PhysicalIndex(idx as usize))
            .collect()
    }

    // ── DML ───────────────────────────────────────────────────────────────────

    /// 向表插入数据块。
    ///
    /// 在 auto_commit 模式下自动包裹事务。
    /// `context` 锁在获取事务句柄后立即释放，不跨越 I/O 操作持有。
    pub fn insert_chunk(&self, table_name: &str, chunk: &mut DataChunk) -> Result<()> {
        let table = self.get_table(table_name)?;
        let auto_commit = self.ensure_write_transaction()?;

        // 获取事务句柄后立即释放 context 锁，避免后续 commit 时死锁
        let _txn = self.acquire_write_transaction();

        table
            .storage
            .local_append(&table.catalog_entry, &self.context, chunk, &[])
            .map_err(|e| anyhow!("append failed: {e:?}"))?;

        self.commit_if_auto_commit(auto_commit)
    }

    /// 扫描表数据（可见当前事务内的未提交写入）。
    pub fn scan_chunks(
        &self,
        table_name: &str,
        column_ids: Option<Vec<u64>>,
    ) -> Result<Vec<DataChunk>> {
        let table = self.get_table(table_name)?;

        let column_ids = column_ids.unwrap_or_else(|| {
            (0..table.storage.column_count())
                .map(|idx| idx as u64)
                .collect()
        });

        let result_types: Vec<LogicalType> = column_ids
            .iter()
            .map(|idx| {
                table.storage.column_definitions[*idx as usize]
                    .logical_type
                    .clone()
            })
            .collect();

        // 获取事务句柄（不持有 context 锁）
        let txn = self.acquire_read_transaction();
        let storage_column_ids: Vec<StorageIndex> =
            column_ids.iter().copied().map(StorageIndex).collect();

        let mut state = crate::storage::table::scan_state::TableScanState::new();
        {
            let txn_guard = txn.lock_inner();
            table.storage.initialize_scan(
                self.context.as_ref(),
                &*txn_guard,
                &mut state,
                &storage_column_ids,
                None,
            );
        }

        let mut chunks = Vec::new();
        loop {
            let mut chunk = DataChunk::new();
            chunk.initialize(&result_types, STANDARD_VECTOR_SIZE);
            table.storage.scan(self.context.as_ref(), &mut chunk, &mut state);
            if chunk.size() == 0 {
                break;
            }
            chunk.flatten();
            chunks.push(chunk);
        }

        Ok(chunks)
    }

    /// 按 Row ID 更新指定列。
    pub fn update_chunk(
        &self,
        table_name: &str,
        row_ids: &[i64],
        column_ids: &[u64],
        updates: &mut DataChunk,
    ) -> Result<()> {
        let table = self.get_table(table_name)?;
        let auto_commit = self.ensure_write_transaction()?;

        let _txn = self.acquire_write_transaction();

        let mut row_id_vector = Self::build_row_id_vector(row_ids);
        let physical_column_ids = Self::build_physical_column_ids(column_ids);

        let mut state = table
            .storage
            .initialize_update()
            .map_err(|e| anyhow!("initialize update failed: {e:?}"))?;
        table
            .storage
            .update(
                state.as_mut(),
                self.context.as_ref(),
                &mut row_id_vector,
                &physical_column_ids,
                updates,
            )
            .map_err(|e| anyhow!("update failed: {e:?}"))?;

        self.commit_if_auto_commit(auto_commit)
    }

    /// 按 Row ID 删除行，返回实际删除行数。
    pub fn delete_chunk(&self, table_name: &str, row_ids: &[i64]) -> Result<usize> {
        let table = self.get_table(table_name)?;
        let auto_commit = self.ensure_write_transaction()?;

        let _txn = self.acquire_write_transaction();

        let mut row_id_vector = Self::build_row_id_vector(row_ids);

        let mut state = table.storage.initialize_delete();
        let deleted = table
            .storage
            .delete(
                state.as_mut(),
                self.context.as_ref(),
                &mut row_id_vector,
                row_ids.len() as u64,
            )
            .map_err(|e| anyhow!("delete failed: {e:?}"))?;

        self.commit_if_auto_commit(auto_commit)?;
        Ok(deleted as usize)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // 析构时回滚未完成的事务
        if self.has_active_transaction() {
            let _ = self.rollback();
        }
    }
}

// ─── 辅助函数 ──────────────────────────────────────────────────────────────────

/// 将表名解析为 `(schema, table)` 小写 key。
///
/// - `"users"`        → `("main", "users")`   — 非限定，默认 schema "main"
/// - `"main.users"`   → `("main", "users")`   — 限定格式
/// - `"hr.employees"` → `("hr", "employees")` — 非默认 schema
pub fn parse_table_name(table_name: &str) -> (String, String) {
    match table_name.split_once('.') {
        Some((schema, table)) => (schema.to_ascii_lowercase(), table.to_ascii_lowercase()),
        None => ("main".to_string(), table_name.to_ascii_lowercase()),
    }
}
