use std::sync::Arc;

use crate::catalog::{Catalog, SchemaCatalogEntry};
use super::engine::{EngineError, SchemaInfo, SchemaTableInfo, TableScanBindData, TableScanRequest};
use crate::common::errors::{Result, anyhow};
use crate::common::types::{DataChunk, LogicalType};
use crate::db::conn::{ClientContext, Connection, DatabaseInstance, TableHandle};
use crate::planner::TableFilterSet;
use crate::storage::data_table::StorageIndex;
use crate::storage::table::scan_state::{ParallelCollectionScanState, ParallelTableScanState, TableScanState};
use crate::storage::table::segment_base::SegmentBase;
use parking_lot::Mutex;

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
        DuckConnection { conn, db: Arc::clone(&self.db) }
    }
}

// ─── DuckConnection ────────────────────────────────────────────────────────────

/// 数据库连接（操作层）。
///
/// 通过 [`DuckEngine::connect`] 创建，持有：
/// - `conn`：独立的底层连接（事务状态隔离）。
/// - `db`：与引擎共享的数据库实例引用（DDL 修改元数据用）。
pub struct DuckConnection {
    /// 底层连接（含独立 ClientContext / TransactionContext）。
    conn: Connection,

    /// 数据库实例引用（用于 DDL：`create_table` 需修改全局 tables 映射）。
    ///
    /// 与创建本连接的 `DuckdbEngine.db` 指向同一个对象。
    db: Arc<DatabaseInstance>,

}

impl DuckConnection {
    fn default_schema_name(&self) -> String {
        let catalog = self.db.catalog.lock();
        catalog.default_schema().to_string()
    }

    fn current_catalog_transaction(&self) -> crate::catalog::CatalogTransaction {
        self.conn.current_catalog_transaction()
    }

    fn parse_create_table_name(&self, table_name: &str) -> Result<(String, String), EngineError> {
        let mut parts = table_name.split('.');
        let first = parts.next().unwrap_or_default();
        let second = parts.next();
        let third = parts.next();

        if first.is_empty() {
            return Err(anyhow!("table name must not be empty"));
        }
        if third.is_some() {
            return Err(anyhow!(
                "table name '{}' is invalid: only 'table' or 'schema.table' is supported",
                table_name
            ));
        }

        match second {
            Some(table) if table.is_empty() => Err(anyhow!("table name '{}' is invalid", table_name)),
            Some(table) => Ok((first.to_string(), table.to_string())),
            None => Ok((self.default_schema_name(), first.to_string())),
        }
    }

    fn resolve_scan(
        &self,
        table_name: &str,
        mut request: TableScanRequest,
    ) -> Result<ResolvedScan, EngineError> {
        let table = self.conn.get_table(table_name)?;
        let requested_column_ids = if request.column_ids.is_empty() {
            (0..table.entry.storage.column_count())
                .map(|idx| StorageIndex(idx as u64))
                .collect()
        } else {
            request.column_ids.clone()
        };

        let mut scan_column_ids = requested_column_ids.clone();
        let projection_ids: Vec<usize> = (0..requested_column_ids.len()).collect();

        let mut filters = request.filters.take().unwrap_or_default();
        if !request.table_column_filters.is_empty() {
            for entry in &request.table_column_filters {
                let scan_idx = match scan_column_ids.iter().position(|col| *col == entry.column_id) {
                    Some(idx) => idx,
                    None => {
                        scan_column_ids.push(entry.column_id);
                        scan_column_ids.len() - 1
                    }
                };
                filters.push_filter(scan_idx, entry.filter.copy());
            }
        }
        request.column_ids = scan_column_ids.clone();
        request.filters = filters.has_filters().then_some(filters);

        let result_types: Vec<LogicalType> = request
            .column_ids
            .iter()
            .take(requested_column_ids.len())
            .map(|idx| {
                table.entry.storage.column_definitions[idx.0 as usize]
                    .logical_type
                    .clone()
            })
            .collect();
        let scanned_types: Vec<LogicalType> = request
            .column_ids
            .iter()
            .map(|idx| {
                table.entry.storage.column_definitions[idx.0 as usize]
                    .logical_type
                    .clone()
            })
            .collect();

        let _txn = self.conn.acquire_read_transaction();

        Ok(ResolvedScan {
            table,
            storage_context: Arc::clone(&self.conn.context),
            bind_data: TableScanBindData {
                request,
                result_types,
                scanned_types,
                projection_ids,
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

    /// 初始化 table scan 的 global state。
    ///
    /// 对齐 `DuckTableScanInitGlobal(...)`：
    /// - 使用绑定阶段的 `TableScanBindData`
    /// - 调用 `DataTable::InitializeParallelScan(...)`
    /// - 在构造 `DuckTableScanState` 时调用 `DataTable::MaxThreads()`
    pub fn duck_table_scan_init_global(
        &self,
        table_name: &str,
        request: TableScanRequest,
    ) -> Result<DuckTableScanState, EngineError> {
        let resolved = self.resolve_scan(table_name, request)?;
        let max_threads =
            resolved.table.entry.storage.max_threads(resolved.storage_context.as_ref()) as usize;
        let mut parallel_state = ParallelTableScanState {
            scan_state: ParallelCollectionScanState {
                row_groups: None,
                current_row_group: None,
                next_row_group_index: 0,
                vector_index: 0,
                max_row: 0,
                batch_index: 0,
                processed_rows: 0,
                lock: parking_lot::Mutex::new(()),
            },
            local_state: ParallelCollectionScanState {
                row_groups: None,
                current_row_group: None,
                next_row_group_index: 0,
                vector_index: 0,
                max_row: 0,
                batch_index: 0,
                processed_rows: 0,
                lock: parking_lot::Mutex::new(()),
            },
        };
        resolved.table.entry.storage.initialize_parallel_scan(
            resolved.storage_context.as_ref(),
            &mut parallel_state,
            &[],
        );

        Ok(DuckTableScanState {
            table: resolved.table,
            storage_context: resolved.storage_context,
            bind_data: resolved.bind_data,
            parallel_state: Mutex::new(parallel_state),
            max_threads,
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
        let global_state = self.duck_table_scan_init_global(table_name, request)?;
        let result_types = global_state.result_types().to_vec();
        let mut local_state = global_state.init_local_state();
        let mut chunks = Vec::new();
        loop {
            let mut chunk = DataChunk::new();
            chunk.initialize(&result_types, crate::common::types::STANDARD_VECTOR_SIZE);
            if !global_state.table_scan(&mut local_state, &mut chunk)? {
                break;
            }
            chunk.flatten();
            chunks.push(chunk);
        }
        Ok(chunks)
    }

    // ── Schema / DDL ──────────────────────────────────────────────────────────

    /// 创建 schema。
    ///
    /// 对齐 DuckDB: `CREATE SCHEMA` 直接修改全局 catalog，而非连接本地状态。
    pub fn create_schema(&self, schema_name: &str) -> Result<(), EngineError> {
        self.conn.create_schema(schema_name)
    }

    /// 获取 schema 下所有表的定义。
    ///
    /// # Errors
    /// 若 schema 在 catalog 中不存在，返回错误。
    pub fn get_schema(&self, schema_name: &str) -> Result<SchemaInfo, EngineError> {
        let txn = self.current_catalog_transaction();
        let catalog = self.db.catalog.lock();
        let schema = catalog
            .get_schema_for_operation(&txn, schema_name)
            .map_err(|e| anyhow!("lookup schema failed: {e}"))?;
        let mut schema_tables = Vec::new();
        schema.scan(&txn, crate::catalog::CatalogType::TableEntry, &mut |node| {
            if let crate::catalog::CatalogEntryKind::Table(entry) = &node.kind {
                schema_tables.push(SchemaTableInfo {
                    name: entry.name().to_string(),
                    columns: entry
                        .storage
                        .column_definitions
                        .iter()
                        .map(|col| (col.name.clone(), col.logical_type.clone()))
                        .collect(),
                });
            }
        });
        Ok(SchemaInfo {
            name: schema_name.to_string(),
            tables: schema_tables,
        })
    }

    /// 在指定 schema 下创建表。
    pub fn create_table_in_schema(
        &self,
        schema: &str,
        table: &str,
        columns: Vec<(String, LogicalType)>,
    ) -> Result<(), EngineError> {
        self.conn.create_table(schema, table, columns)
    }

    /// 创建表。
    ///
    /// `table_name` 支持：
    /// - `"tbl"`：在默认 schema 下建表
    /// - `"schema.tbl"`：在指定 schema 下建表
    pub fn create_table(
        &self,
        table_name: &str,
        columns: Vec<(String, LogicalType)>,
    ) -> Result<(), EngineError> {
        let (schema, table) = self.parse_create_table_name(table_name)?;
        self.create_table_in_schema(&schema, &table, columns)
    }

    pub fn drop_schema(&self, schema_name: &str) -> Result<(), EngineError> {
        self.conn.drop_schema(schema_name)
    }

    pub fn drop_table_in_schema(&self, schema: &str, table: &str) -> Result<(), EngineError> {
        self.conn.drop_table(schema, table)
    }

    pub fn drop_table(&self, table_name: &str) -> Result<(), EngineError> {
        let (schema, table) = self.parse_create_table_name(table_name)?;
        self.drop_table_in_schema(&schema, &table)
    }
}

struct ResolvedScan {
    table: TableHandle,
    storage_context: Arc<ClientContext>,
    bind_data: TableScanBindData,
}

/// 对齐 DuckDB 的 `class DuckTableScanState`。
pub struct DuckTableScanState {
    table: TableHandle,
    storage_context: Arc<ClientContext>,
    bind_data: TableScanBindData,
    parallel_state: Mutex<ParallelTableScanState>,
    max_threads: usize,
}

impl DuckTableScanState {
    pub fn bind_data(&self) -> &TableScanBindData {
        &self.bind_data
    }

    pub fn result_types(&self) -> &[LogicalType] {
        &self.bind_data.result_types
    }

    pub fn max_threads(&self) -> usize {
        self.max_threads
    }

    /// 对齐 `DuckTableScanState::InitLocalState(...)`。
    pub fn init_local_state(&self) -> TableScanLocalState {
        let mut local_state = TableScanLocalState {
            scan_state: TableScanState::new(),
            rows_scanned: 0,
            rows_in_current_row_group: 0,
            all_columns: None,
        };
        local_state.scan_state.initialize_with_context(
            self.bind_data
                .request
                .column_ids
                .iter()
                .map(|idx| idx.0)
                .collect(),
            self.storage_context.as_ref(),
            self.bind_data.request.filters.as_ref(),
        );
        if self.bind_data.projection_ids.len() != self.bind_data.request.column_ids.len() {
            let mut all_columns = DataChunk::new();
            all_columns.initialize(
                &self.bind_data.scanned_types,
                crate::common::types::STANDARD_VECTOR_SIZE,
            );
            local_state.all_columns = Some(all_columns);
        }

        let mut parallel_state = self.parallel_state.lock();
        local_state.rows_in_current_row_group = self.table.entry.storage.next_parallel_scan(
            self.storage_context.as_ref(),
            &mut parallel_state,
            &mut local_state.scan_state,
        );
        local_state
    }

    /// 对齐 `DuckTableScanState::TableScanFunc(...)`。
    pub fn table_scan(
        &self,
        local_state: &mut TableScanLocalState,
        result: &mut DataChunk,
    ) -> Result<bool, EngineError> {
        loop {
            if local_state.rows_in_current_row_group == 0 {
                return Ok(false);
            }

            if let Some(all_columns) = local_state.all_columns.as_mut() {
                all_columns.reset();
                self.table.entry.storage.scan(
                    self.storage_context.as_ref(),
                    all_columns,
                    &mut local_state.scan_state,
                );
                if all_columns.size() > 0 {
                    result.reference_columns(all_columns, &self.bind_data.projection_ids);
                    return Ok(true);
                }
            } else {
                self.table
                    .entry
                    .storage
                    .scan(self.storage_context.as_ref(), result, &mut local_state.scan_state);
                if result.size() > 0 {
                    return Ok(true);
                }
            }

            local_state.rows_scanned += local_state.rows_in_current_row_group;
            let mut parallel_state = self.parallel_state.lock();
            local_state.rows_in_current_row_group = self.table.entry.storage.next_parallel_scan(
                self.storage_context.as_ref(),
                &mut parallel_state,
                &mut local_state.scan_state,
            );
            if local_state.rows_in_current_row_group == 0 {
                return Ok(false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DuckEngine;
    use crate::common::types::LogicalType;

    #[test]
    fn ddl_facade_follows_catalog_path() {
        let engine = DuckEngine::open(":memory:").expect("open engine");
        let conn = engine.connect();

        conn.create_schema("s1").expect("create schema");
        conn.create_table(
            "s1.t1",
            vec![("id".to_string(), LogicalType::integer())],
        )
        .expect("create table");

        let schema = conn.get_schema("s1").expect("get schema");
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "t1");

        conn.drop_table("s1.t1").expect("drop table");
        let schema = conn.get_schema("s1").expect("get schema after drop table");
        assert!(schema.tables.is_empty());

        conn.drop_schema("s1").expect("drop schema");
        assert!(conn.get_schema("s1").is_err());
    }
}

/// table scan 的 local state。
pub struct TableScanLocalState {
    pub scan_state: TableScanState,
    pub rows_scanned: usize,
    pub rows_in_current_row_group: usize,
    pub all_columns: Option<DataChunk>,
}
