use std::collections::HashSet;
use std::sync::Arc;

use super::engine::{
    EngineError, EngineParallelScanState, EngineScanGlobalState, EngineScanLocalState,
    SchemaInfo, SchemaTableInfo, TableScanRequest,
};
use crate::common::errors::{Result, anyhow};
use crate::common::types::{DataChunk, LogicalType};
use crate::db::conn::{Connection, DatabaseInstance, TableHandle};
use crate::storage::data_table::StorageIndex;
use crate::storage::table::scan_state::{ParallelTableScanState, TableScanState};
use crate::transaction::duck_transaction_manager::DuckTxnHandle;

// ─── DuckdbEngine ──────────────────────────────────────────────────────────────

/// DuckDB 引擎：管理单个数据库实例，提供连接工厂。
pub struct DuckEngine {
    /// 底层数据库实例（Arc 允许多个 DuckConnection 共享）。
    db: Arc<DatabaseInstance>,
}

impl DuckEngine {
    /// 打开（或创建）数据库文件，返回引擎实例。
    ///
    /// - `path = ":memory:"` → 纯内存数据库，进程退出后数据丢失。
    /// - `path = "mydb.db"` → 持久化文件，自动加载已有数据并回放 WAL。
    pub fn open(path: impl Into<String>) -> Result<Self, EngineError> {
        let db = DatabaseInstance::open(path).map_err(|e| anyhow!("open db failed: {e:?}"))?;
        Ok(Self { db })
    }

    /// 返回数据库文件路径。
    pub fn path(&self) -> String {
        self.db.path.clone()
    }

    /// 将内存/WAL 数据持久化到磁盘（显式 checkpoint）。
    pub fn checkpoint(&self) -> Result<(), EngineError> {
        self.db
            .checkpoint()
            .map_err(|e| anyhow!("checkpoint failed: {e:?}"))
    }

    /// 返回默认 schema（`"main"`）下的所有表名。
    pub fn tables(&self) -> Vec<String> {
        self.db.tables()
    }

    /// 创建一个新的独立连接。
    ///
    /// 每个 `DuckConnection` 拥有独立的 `ClientContext` 和 `TransactionContext`，
    /// 但与同一引擎上的其他连接共享同一个 `DatabaseInstance`（存储 + 事务管理器）。
    pub fn connect(&self) -> DuckConnection {
        let conn = self.db.connect();
        let mut schemas = HashSet::new();
        schemas.insert("main".to_string());
        DuckConnection {
            conn,
            db: Arc::clone(&self.db),
            schemas,
        }
    }
}

// ─── DuckConnection ────────────────────────────────────────────────────────────

/// 数据库连接（操作层）。
///
/// 通过 [`DuckEngine::connect`] 创建，持有：
/// - `conn`：独立的底层连接（事务状态隔离）。
/// - `db`：与引擎共享的数据库实例引用（DDL 修改元数据用）。
/// - `schemas`：本连接可见的 schema 名称集合。
pub struct DuckConnection {
    /// 底层连接（含独立 ClientContext / TransactionContext）。
    conn: Connection,

    /// 数据库实例引用（用于 DDL：`create_table` 需修改全局 tables 映射）。
    ///
    /// 与创建本连接的 `DuckdbEngine.db` 指向同一个对象。
    db: Arc<DatabaseInstance>,

    /// 本连接已注册的 schema 名称（默认包含 `"main"`）。
    schemas: HashSet<String>,
}

impl DuckConnection {
    fn resolve_scan(
        &self,
        table_name: &str,
        mut request: TableScanRequest,
    ) -> Result<ResolvedScan, EngineError> {
        let table = self.conn.get_table(table_name)?;
        if request.column_ids.is_empty() {
            request.column_ids = (0..table.storage.column_count())
                .map(|idx| StorageIndex(idx as u64))
                .collect();
        }

        let result_types: Vec<LogicalType> = request
            .column_ids
            .iter()
            .map(|idx| {
                table.storage.column_definitions[idx.0 as usize]
                    .logical_type
                    .clone()
            })
            .collect();

        let txn = self.conn.acquire_read_transaction();
        let storage_context = Connection::build_storage_context(&txn);

        Ok(ResolvedScan {
            table,
            txn,
            storage_context,
            global_state: EngineScanGlobalState {
                request,
                result_types,
            },
        })
    }

    // ── 事务管理 ──────────────────────────────────────────────────────────────

    /// 开启事务（`BEGIN TRANSACTION`）。
    pub fn begin_transaction(&self) -> Result<(), EngineError> {
        self.conn.begin_transaction()
    }

    /// 提交当前事务。
    pub fn commit(&self) -> Result<(), EngineError> {
        self.conn.commit()
    }

    /// 回滚当前事务，丢弃所有未提交修改。
    pub fn rollback(&self) -> Result<(), EngineError> {
        self.conn.rollback()
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

    /// 初始化顺序扫描。
    ///
    /// 返回值持有：
    /// - engine 级 global state
    /// - 线程私有 local scan state
    /// - 绑定当前事务的存储上下文
    pub fn begin_scan(
        &self,
        table_name: &str,
        request: TableScanRequest,
    ) -> Result<DuckTableScanHandle, EngineError> {
        let resolved = self.resolve_scan(table_name, request)?;
        let column_ids = resolved
            .global_state
            .request
            .column_ids
            .clone();
        let mut local_state = EngineScanLocalState {
            scan_state: TableScanState::new(),
        };
        {
            let txn_guard = resolved.txn.lock_inner();
            resolved.table.storage.initialize_scan(
                &resolved.storage_context,
                &*txn_guard,
                &mut local_state.scan_state,
                &column_ids,
                resolved.global_state.request.filters.as_ref(),
            );
        }

        Ok(DuckTableScanHandle {
            table: resolved.table,
            txn: resolved.txn,
            global_state: resolved.global_state,
            local_state,
        })
    }

    /// 初始化并行扫描共享状态。
    ///
    /// worker 线程应通过返回值反复领取 `DuckTableScanTask`，
    /// 然后在各自线程内调用 `next_chunk`。
    pub fn begin_parallel_scan(
        &self,
        table_name: &str,
        request: TableScanRequest,
    ) -> Result<DuckParallelScanHandle, EngineError> {
        let resolved = self.resolve_scan(table_name, request)?;
        let mut parallel_state = EngineParallelScanState {
            parallel_state: ParallelTableScanState {
                scan_state: crate::storage::table::scan_state::ParallelCollectionScanState {
                    row_groups: None,
                    current_row_group: None,
                    next_row_group_index: 0,
                    vector_index: 0,
                    max_row: 0,
                    batch_index: 0,
                    processed_rows: 0,
                    lock: parking_lot::Mutex::new(()),
                },
                local_state: crate::storage::table::scan_state::ParallelCollectionScanState {
                    row_groups: None,
                    current_row_group: None,
                    next_row_group_index: 0,
                    vector_index: 0,
                    max_row: 0,
                    batch_index: 0,
                    processed_rows: 0,
                    lock: parking_lot::Mutex::new(()),
                },
            },
        };
        resolved.table.storage.initialize_parallel_scan(
            &resolved.storage_context,
            &mut parallel_state.parallel_state,
            &[],
        );

        Ok(DuckParallelScanHandle {
            table: resolved.table,
            txn: resolved.txn,
            storage_context: resolved.storage_context,
            global_state: resolved.global_state,
            parallel_state,
        })
    }

    /// 扫描表数据，可见当前事务内的未提交写入。
    ///
    /// 这是兼容层，内部走 `begin_scan` + `next_chunk`。
    pub fn scan(
        &self,
        table_name: &str,
        column_ids: Option<Vec<u64>>,
    ) -> Result<Vec<DataChunk>, EngineError> {
        let request = TableScanRequest::new(
            column_ids
                .unwrap_or_default()
                .into_iter()
                .map(StorageIndex)
                .collect(),
        );
        let mut handle = self.begin_scan(table_name, request)?;
        let mut chunks = Vec::new();
        loop {
            let mut chunk = DataChunk::new();
            chunk.initialize(handle.result_types(), crate::common::types::STANDARD_VECTOR_SIZE);
            if !handle.next_chunk(&mut chunk)? {
                break;
            }
            chunk.flatten();
            chunks.push(chunk);
        }
        Ok(chunks)
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
            return Err(anyhow!("schema '{}' not found", schema_name));
        }
        let tables_guard = self.db.tables.lock();
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
            return Err(anyhow!(
                "Schema '{}' not found. Call create_schema(\"{}\") first.",
                schema, schema
            ));
        }
        self.db.create_table(schema, table, columns);
        Ok(())
    }
}

struct ResolvedScan {
    table: TableHandle,
    txn: Arc<DuckTxnHandle>,
    storage_context: crate::storage::data_table::ClientContext,
    global_state: EngineScanGlobalState,
}

/// 顺序扫描句柄。
///
/// 这层对应 engine 的 `global state + local state`。
pub struct DuckTableScanHandle {
    table: TableHandle,
    txn: Arc<DuckTxnHandle>,
    global_state: EngineScanGlobalState,
    local_state: EngineScanLocalState,
}

impl DuckTableScanHandle {
    pub fn global_state(&self) -> &EngineScanGlobalState {
        &self.global_state
    }

    pub fn local_state(&self) -> &EngineScanLocalState {
        &self.local_state
    }

    pub fn result_types(&self) -> &[LogicalType] {
        &self.global_state.result_types
    }

    pub fn next_chunk(&mut self, result: &mut DataChunk) -> Result<bool, EngineError> {
        let txn_guard = self.txn.lock_inner();
        self.table
            .storage
            .scan(&*txn_guard, result, &mut self.local_state.scan_state);
        Ok(result.size() > 0)
    }
}

/// 并行扫描共享句柄。
///
/// 这层对应 engine 的 global state 和 global_parallel_state。
pub struct DuckParallelScanHandle {
    table: TableHandle,
    txn: Arc<DuckTxnHandle>,
    storage_context: crate::storage::data_table::ClientContext,
    global_state: EngineScanGlobalState,
    parallel_state: EngineParallelScanState,
}

impl DuckParallelScanHandle {
    pub fn global_state(&self) -> &EngineScanGlobalState {
        &self.global_state
    }

    pub fn parallel_state(&self) -> &EngineParallelScanState {
        &self.parallel_state
    }

    pub fn result_types(&self) -> &[LogicalType] {
        &self.global_state.result_types
    }

    pub fn next_task(&mut self) -> Result<Option<DuckTableScanTask>, EngineError> {
        let mut local_state = EngineScanLocalState {
            scan_state: TableScanState::new(),
        };
        let column_ids: Vec<u64> = self
            .global_state
            .request
            .column_ids
            .iter()
            .map(|idx| idx.0)
            .collect();
        local_state.scan_state.initialize(column_ids);

        let rows = self.table.storage.next_parallel_scan(
            &self.storage_context,
            &mut self.parallel_state.parallel_state,
            &mut local_state.scan_state,
        );
        if rows == 0 {
            return Ok(None);
        }

        Ok(Some(DuckTableScanTask {
            table: self.table.clone(),
            txn: Arc::clone(&self.txn),
            global_state: EngineScanGlobalState {
                request: self.global_state.request.clone(),
                result_types: self.global_state.result_types.clone(),
            },
            local_state,
        }))
    }
}

/// 并行 worker 私有扫描任务。
///
/// 这层对应执行器线程拿到的 local state。
pub struct DuckTableScanTask {
    table: TableHandle,
    txn: Arc<DuckTxnHandle>,
    global_state: EngineScanGlobalState,
    local_state: EngineScanLocalState,
}

impl DuckTableScanTask {
    pub fn global_state(&self) -> &EngineScanGlobalState {
        &self.global_state
    }

    pub fn local_state(&self) -> &EngineScanLocalState {
        &self.local_state
    }

    pub fn result_types(&self) -> &[LogicalType] {
        &self.global_state.result_types
    }

    pub fn next_chunk(&mut self, result: &mut DataChunk) -> Result<bool, EngineError> {
        let txn_guard = self.txn.lock_inner();
        self.table
            .storage
            .scan(&*txn_guard, result, &mut self.local_state.scan_state);
        Ok(result.size() > 0)
    }
}
