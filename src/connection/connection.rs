//! Connection 实现。
//!
//! 对应 DuckDB C++: `duckdb/main/connection.hpp` / `connection.cpp`

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::common::types::{DataChunk, LogicalType, STANDARD_VECTOR_SIZE};
use crate::storage::data_table::DataTable;
use crate::storage::storage_manager::StorageManager;
use crate::transaction::duck_transaction_manager::{DuckTransactionManager, DuckTxnHandle};
use crate::catalog::TableCatalogEntry;
use crate::transaction::transaction_context::TransactionContext;

// ─── ConnectionId ──────────────────────────────────────────────────────────────

/// 连接 ID 类型（C++: `typedef uint64_t connection_t`）。
pub type ConnectionId = u64;

/// 全局连接 ID 计数器。
static CONNECTION_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_connection_id() -> ConnectionId {
    CONNECTION_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

// ─── ClientContext ─────────────────────────────────────────────────────────────

/// 客户端上下文：每个连接的状态（C++: `class ClientContext`）。
///
/// # C++ 源码
/// ```cpp
/// class ClientContext : public enable_shared_from_this<ClientContext> {
/// public:
///     shared_ptr<DatabaseInstance> db;
///     atomic<bool> interrupted;
///     unique_ptr<RegisteredStateManager> registered_state;
///     shared_ptr<Logger> logger;
///     ClientConfig config;
///     unique_ptr<ClientData> client_data;
///     TransactionContext transaction;
///
///     MetaTransaction &ActiveTransaction() {
///         return transaction.ActiveTransaction();
///     }
/// };
/// ```
pub struct ClientContext {
    /// 数据库实例引用（C++: `shared_ptr<DatabaseInstance> db`）。
    pub db: Arc<DatabaseInstance>,

    /// 事务上下文（C++: `TransactionContext transaction`）。
    pub transaction: TransactionContext,

    /// 连接 ID（C++: `connection_t connection_id`）。
    pub connection_id: ConnectionId,

    /// 是否中断（C++: `atomic<bool> interrupted`）。
    interrupted: std::sync::atomic::AtomicBool,

    /// 每个表的 LocalAppendState，在事务期间复用。
    /// 类似 DuckDB C++ 的 BatchInsertLocalState::current_append_state。
    append_states: Mutex<HashMap<String, crate::storage::table::append_state::LocalAppendState>>,
}

impl ClientContext {
    /// 创建新的客户端上下文（C++: `ClientContext(shared_ptr<DatabaseInstance> db)`）。
    pub fn new(db: Arc<DatabaseInstance>) -> Self {
        let db_id = db.db_id;
        let transaction_manager = db.transaction_manager.clone();
        let db_weak = Arc::downgrade(&db);
        Self {
            transaction: TransactionContext::new(
                db_id,
                transaction_manager,
                db_weak,
            ),
            db,
            connection_id: next_connection_id(),
            interrupted: std::sync::atomic::AtomicBool::new(false),
            append_states: Mutex::new(HashMap::new()),
        }
    }

    /// 获取活跃事务句柄（如果存在）。
    pub fn active_transaction(&self) -> Option<Arc<DuckTxnHandle>> {
        self.transaction.try_get_transaction()
    }

    /// 中断执行（C++: `Interrupt()`）。
    pub fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Relaxed);
    }

    /// 是否被中断（C++: `IsInterrupted()`）。
    pub fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::Relaxed)
    }

    /// 重置中断状态。
    pub fn reset_interrupt(&self) {
        self.interrupted.store(false, Ordering::Relaxed);
    }

    /// 开始事务（C++: 通过 `Query("BEGIN TRANSACTION")` 实现）。
    pub fn begin_transaction(&self) -> Result<(), crate::transaction::transaction_context::TransactionError> {
        self.transaction.begin_transaction()
    }

    /// 提交事务。
    pub fn commit(&self) -> Result<(), crate::transaction::transaction_context::TransactionError> {
        // Finalize all pending local appends so collection.total_rows is up-to-date
        // before LocalStorage::commit() scans them in flush_one.
        {
            let mut states = self.append_states.lock();
            for (_, state) in states.iter_mut() {
                crate::storage::local_storage::LocalStorage::finalize_append(state);
            }
            states.clear();
        }
        self.transaction.commit()
    }

    /// 回滚事务。
    pub fn rollback(&self) -> Result<(), crate::transaction::transaction_context::TransactionError> {
        // Discard pending local append states on rollback.
        self.append_states.lock().clear();
        self.transaction.rollback()
    }

    /// 设置自动提交模式。
    pub fn set_auto_commit(&self, value: bool) {
        self.transaction.set_auto_commit(value);
    }

    /// 是否自动提交模式。
    pub fn is_auto_commit(&self) -> bool {
        self.transaction.is_auto_commit()
    }

    /// 是否有活跃事务。
    pub fn has_active_transaction(&self) -> bool {
        self.transaction.has_active_transaction()
    }
}

// ─── DatabaseInstance ──────────────────────────────────────────────────────────

/// 数据库实例（C++: `class DatabaseInstance`）。
///
/// 共享的数据库状态，多个连接可以共享同一个实例。
pub struct DatabaseInstance {
    /// 数据库 ID（用于 MetaTransaction 中区分多个附加数据库）。
    pub db_id: u64,

    /// 数据库路径。
    pub path: String,

    /// 存储管理器。
    pub storage_manager: Arc<crate::storage::storage_manager::SingleFileStorageManager>,

    /// 事务管理器（全局共享）。
    pub transaction_manager: Arc<DuckTransactionManager>,

    /// 表集合。
    pub tables: Mutex<HashMap<String, TableHandle>>,

    /// 全局事务计数器（C++: `atomic<transaction_t> transaction_id`）。
    transaction_counter: AtomicU64,

    /// 全局提交时间戳计数器。
    commit_counter: AtomicU64,

    /// Block 分配大小。
    pub block_alloc_size: u64,
}

/// 全局数据库 ID 计数器。
static DB_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl DatabaseInstance {
    /// 创建新的数据库实例。
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
            transaction_counter: AtomicU64::new(1),
            commit_counter: AtomicU64::new(1),
            block_alloc_size,
        }
    }

    /// 获取新的事务 ID（C++: `GetNewTransactionNumber()`）。
    pub fn get_new_transaction_number(&self) -> u64 {
        self.transaction_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// 获取新的提交时间戳（C++: `GetNewCommitTimestamp()`）。
    pub fn get_new_commit_timestamp(&self) -> u64 {
        // 提交时间戳从 TRANSACTION_ID_START 开始，确保比所有事务 ID 大
        let ts = self.commit_counter.fetch_add(1, Ordering::Relaxed);
        crate::transaction::types::TRANSACTION_ID_START + ts
    }
}

// ─── TableHandle ───────────────────────────────────────────────────────────────

/// 表句柄：包含目录条目和存储引用。
#[derive(Clone)]
pub struct TableHandle {
    /// 目录条目（C++: `TableCatalogEntry`）。
    pub catalog_entry: TableCatalogEntry,

    /// 数据表存储（C++: `DataTable`）。
    pub storage: Arc<DataTable>,
}

// ─── Connection ────────────────────────────────────────────────────────────────

/// 数据库连接（C++: `class Connection`）。
///
/// # C++ 源码
/// ```cpp
/// class Connection {
/// public:
///     explicit Connection(DuckDB &database);
///     explicit Connection(DatabaseInstance &database);
///
///     shared_ptr<ClientContext> context;
///
///     void BeginTransaction();
///     void Commit();
///     void Rollback();
///     void SetAutoCommit(bool auto_commit);
///     bool IsAutoCommit();
///     bool HasActiveTransaction();
///
///     unique_ptr<QueryResult> Query(const string &query);
///     void Append(TableDescription &description, DataChunk &chunk);
/// };
/// ```
///
/// # 使用示例
///
/// ```rust
/// // 创建数据库和连接
/// let db = DB::open(":memory:")?;
/// let conn = db.connect();
///
/// // 方式 1: 自动提交模式（默认）
/// conn.insert_chunk("my_table", &mut chunk)?;  // 自动开始并提交事务
///
/// // 方式 2: 手动事务控制
/// conn.begin_transaction()?;
/// conn.insert_chunk("my_table", &mut chunk)?;
/// conn.insert_chunk("my_table", &mut chunk2)?;
/// conn.commit()?;  // 或者 conn.rollback()?
/// ```
pub struct Connection {
    /// 客户端上下文（C++: `shared_ptr<ClientContext> context`）。
    pub(crate) context: Arc<Mutex<ClientContext>>,

    /// 连接 ID（C++: `connection_t connection_id`）。
    connection_id: ConnectionId,

    /// 数据库实例引用。
    db: Arc<DatabaseInstance>,
}

impl Connection {
    /// 创建新连接（C++: `Connection(DatabaseInstance &database)`）。
    pub fn new(db: Arc<DatabaseInstance>) -> Self {
        let context = Arc::new(Mutex::new(ClientContext::new(db.clone())));
        let connection_id = context.lock().connection_id;

        Self {
            context,
            connection_id,
            db,
        }
    }

    /// 获取连接 ID。
    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    /// 开始事务（C++: `Connection::BeginTransaction()`）。
    ///
    /// # C++ 源码
    /// ```cpp
    /// void Connection::BeginTransaction() {
    ///     auto result = Query("BEGIN TRANSACTION");
    ///     if (result->HasError()) {
    ///         result->ThrowError();
    ///     }
    /// }
    /// ```
    pub fn begin_transaction(&self) -> Result<(), crate::transaction::transaction_context::TransactionError> {
        let context = self.context.lock();
        context.begin_transaction()
    }

    /// 提交事务（C++: `Connection::Commit()`）。
    ///
    /// # C++ 源码
    /// ```cpp
    /// void Connection::Commit() {
    ///     auto result = Query("COMMIT");
    ///     if (result->HasError()) {
    ///         result->ThrowError();
    ///     }
    /// }
    /// ```
    pub fn commit(&self) -> Result<(), crate::transaction::transaction_context::TransactionError> {
        let context = self.context.lock();
        context.commit()
    }

    /// 回滚事务（C++: `Connection::Rollback()`）。
    ///
    /// # C++ 源码
    /// ```cpp
    /// void Connection::Rollback() {
    ///     auto result = Query("ROLLBACK");
    ///     if (result->HasError()) {
    ///         result->ThrowError();
    ///     }
    /// }
    /// ```
    pub fn rollback(&self) -> Result<(), crate::transaction::transaction_context::TransactionError> {
        let context = self.context.lock();
        context.rollback()
    }

    /// 设置自动提交模式（C++: `SetAutoCommit()`）。
    pub fn set_auto_commit(&self, value: bool) {
        self.context.lock().set_auto_commit(value);
    }

    /// 是否自动提交模式（C++: `IsAutoCommit()`）。
    pub fn is_auto_commit(&self) -> bool {
        self.context.lock().is_auto_commit()
    }

    /// 是否有活跃事务（C++: `HasActiveTransaction()`）。
    pub fn has_active_transaction(&self) -> bool {
        self.context.lock().has_active_transaction()
    }

    /// 获取当前事务句柄（如果存在）。
    pub fn get_transaction(&self) -> Option<Arc<DuckTxnHandle>> {
        self.context.lock().transaction.try_get_transaction()
    }

    /// 中断当前查询（C++: `Interrupt()`）。
    pub fn interrupt(&self) {
        self.context.lock().interrupt();
    }

    /// 插入数据到指定表（C++: `Append()`）。
    ///
    /// 在自动提交模式下，会自动开始并提交事务。
    /// 在手动模式下，会使用当前活跃事务。
    ///
    /// # 参数
    /// - `table_name`: 表名
    /// - `chunk`: 要插入的数据块
    ///
    /// # 返回
    /// 成功返回 `Ok(())`，失败返回错误信息。
    pub fn insert_chunk(
        &self,
        table_name: &str,
        chunk: &mut DataChunk,
    ) -> Result<(), String> {
        // 获取表
        let tables = self.db.tables.lock();
        let table = tables.get(&table_name.to_ascii_lowercase())
            .ok_or_else(|| format!("Table '{}' not found", table_name))?
            .clone();
        drop(tables);

        // 获取或创建事务
        let auto_commit = self.is_auto_commit();
        if auto_commit {
            // 自动提交模式：确保有事务
            if !self.has_active_transaction() {
                self.begin_transaction().map_err(|e| e.to_string())?;
            }
        }

        let table_id = table.storage.info.table_id();
        let table_info = Arc::clone(&table.storage.info);
        let table_types = table.storage.get_types();

        // 获取写事务句柄
        let context_guard = self.context.lock();
        let txn = context_guard.transaction.get_or_create_write_transaction();

        // 按照 DuckDB 的方式：复用 LocalAppendState
        // 类似 C++ 的 BatchInsertLocalState::current_append_state
        let table_key = table_name.to_ascii_lowercase();
        let mut append_states = context_guard.append_states.lock();

        // 获取或初始化该表的 LocalAppendState
        // 直接在事务的真实 LocalStorage 中创建 LocalTableStorage，
        // 而不是通过克隆的 StorageClientContext（克隆的存储在提交时不可见）。
        if !append_states.contains_key(&table_key) {
            let mut state = crate::storage::table::append_state::LocalAppendState::new();
            {
                let inner = txn.lock_inner();
                inner.storage.initialize_append_state(
                    &mut state,
                    table_id,
                    table_info,
                    table_types.clone(),
                );
            }
            append_states.insert(table_key.clone(), state);
        }

        let state = append_states.get_mut(&table_key).unwrap();

        // 执行插入
        crate::storage::local_storage::LocalStorage::append(state, chunk, &table.storage.info)
            .map_err(|e| format!("Append failed: {:?}", e))?;

        drop(append_states);
        drop(context_guard);

        // 如果是自动提交模式，立即提交
        if auto_commit {
            let context_guard = self.context.lock();
            context_guard.commit()
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    /// 扫描表数据。
    pub fn scan_chunks(
        &self,
        table_name: &str,
        column_ids: Option<Vec<u64>>,
    ) -> Result<Vec<DataChunk>, String> {
        let tables = self.db.tables.lock();
        let table = tables.get(&table_name.to_ascii_lowercase())
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;
        let table = table.clone();
        drop(tables);

        let column_ids = column_ids.unwrap_or_else(|| {
            (0..table.storage.column_count()).map(|idx| idx as u64).collect()
        });

        let result_types: Vec<LogicalType> = column_ids
            .iter()
            .map(|idx| table.storage.column_definitions[*idx as usize].logical_type.clone())
            .collect();

        let mut state = table.storage.begin_scan_state(column_ids.clone());

        // 获取事务句柄（优先使用已有事务，否则创建临时只读事务）
        let txn = if let Some(t) = self.get_transaction() {
            t
        } else {
            self.context.lock().transaction.get_or_create_transaction()
        };

        {
            let txn_guard = txn.lock_inner();
            txn_guard.storage.initialize_scan_state(
                table.storage.info.table_id(),
                &mut state.local_state,
                &column_ids,
            );
        }

        let mut chunks = Vec::new();
        loop {
            let mut chunk = DataChunk::new();
            chunk.initialize(&result_types, STANDARD_VECTOR_SIZE);
            {
                let txn_guard = txn.lock_inner();
                table.storage.scan(&*txn_guard, &mut chunk, &mut state);
            }
            if chunk.size() == 0 {
                break;
            }
            chunks.push(chunk);
        }

        Ok(chunks)
    }

    /// 获取数据库实例引用。
    pub fn database(&self) -> &Arc<DatabaseInstance> {
        &self.db
    }

    /// 获取存储管理器引用。
    pub fn storage_manager(&self) -> &Arc<crate::storage::storage_manager::SingleFileStorageManager> {
        &self.db.storage_manager
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // 析构时回滚未完成的事务
        // C++: ~Connection() 清理工作在 ConnectionManager 中处理
        if self.has_active_transaction() {
            let _ = self.rollback();
        }
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::storage_manager::SingleFileStorageManager;

    #[test]
    fn test_connection_basic() {
        // 创建内存数据库
        let storage_manager = Arc::new(SingleFileStorageManager::new(
            SingleFileStorageManager::IN_MEMORY_PATH.to_string(),
            false,
            Default::default(),
        ));
        let transaction_manager = Arc::new(DuckTransactionManager::new());
        let db = Arc::new(DatabaseInstance::new(
            SingleFileStorageManager::IN_MEMORY_PATH.to_string(),
            storage_manager,
            transaction_manager,
            256 * 1024,
        ));

        // 创建连接
        let conn = Connection::new(db.clone());
        assert!(!conn.has_active_transaction());
        assert!(conn.is_auto_commit());
    }

    #[test]
    fn test_transaction_control() {
        let storage_manager = Arc::new(SingleFileStorageManager::new(
            SingleFileStorageManager::IN_MEMORY_PATH.to_string(),
            false,
            Default::default(),
        ));
        let transaction_manager = Arc::new(DuckTransactionManager::new());
        let db = Arc::new(DatabaseInstance::new(
            SingleFileStorageManager::IN_MEMORY_PATH.to_string(),
            storage_manager,
            transaction_manager,
            256 * 1024,
        ));

        let conn = Connection::new(db.clone());

        // 开始事务
        conn.begin_transaction().unwrap();
        assert!(conn.has_active_transaction());
        assert!(!conn.is_auto_commit());

        // 不能嵌套开始事务
        assert!(conn.begin_transaction().is_err());

        // 回滚
        conn.rollback().unwrap();
        assert!(!conn.has_active_transaction());
        assert!(conn.is_auto_commit());
    }

    #[test]
    fn test_multiple_connections() {
        let storage_manager = Arc::new(SingleFileStorageManager::new(
            SingleFileStorageManager::IN_MEMORY_PATH.to_string(),
            false,
            Default::default(),
        ));
        let transaction_manager = Arc::new(DuckTransactionManager::new());
        let db = Arc::new(DatabaseInstance::new(
            SingleFileStorageManager::IN_MEMORY_PATH.to_string(),
            storage_manager,
            transaction_manager,
            256 * 1024,
        ));

        // 创建多个连接
        let conn1 = Connection::new(db.clone());
        let conn2 = Connection::new(db.clone());

        // 每个连接有独立的连接 ID
        assert_ne!(conn1.connection_id(), conn2.connection_id());

        // 每个连接有独立的事务
        conn1.begin_transaction().unwrap();
        assert!(conn1.has_active_transaction());
        assert!(!conn2.has_active_transaction());

        conn1.commit().unwrap();
        assert!(!conn1.has_active_transaction());
    }
}