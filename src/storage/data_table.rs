//! 物理表存储。
//!
//! 对应 C++:
//!   `duckdb/storage/data_table.hpp`
//!   `src/storage/data_table.cpp`
//!
//! # 职责
//!
//! `DataTable` 是 DuckDB 中一张物理表的根对象，持有：
//! - 共享元数据（[`DataTableInfo`]）：索引、checkpoint 锁、表名。
//! - 列定义（`column_definitions`）。
//! - 行组集合（`RowGroupCollection`）：所有持久化数据。
//! - append 互斥锁（`append_lock`）。
//! - 表版本状态（MAIN / ALTERED / DROPPED）。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class DataTable : enable_shared_from_this<DataTable>` | `struct DataTable`（通过 `Arc<DataTable>` 共享） |
//! | `shared_ptr<DataTableInfo> info` | `info: Arc<DataTableInfo>` |
//! | `vector<ColumnDefinition> column_definitions` | `column_definitions: Vec<ColumnDefinition>` |
//! | `mutex append_lock` | `append_lock: Mutex<()>` |
//! | `shared_ptr<RowGroupCollection> row_groups` | `row_groups: Arc<RowGroupCollection>` |
//! | `atomic<DataTableVersion> version` | `version: AtomicU8` |
//!
//! # 设计说明
//!
//! 大量方法依赖 `ClientContext`、`Expression`、`ConstraintState` 等尚未在
//! Rust 层完全实现的类型，因此复杂逻辑体保留为 `todo!()` 存根。
//! 结构字段、公开 API 签名、版本控制逻辑和简单查询方法已完整实现。

use crate::catalog::{PhysicalIndex, TableCatalogEntry};
use crate::common::errors::StorageResult;
use crate::common::types::{DataChunk, LogicalType, LogicalTypeId, Vector};
use crate::db::conn::ClientContext;
use crate::planner::TableFilterSet;
use crate::storage::local_storage::LocalStorage;
use crate::storage::storage_info::StorageError;
use crate::storage::local_storage::MAX_ROW_ID;
use crate::storage::storage_lock::StorageLockKey;
use crate::storage::storage_manager::StorageCommitState;
use crate::storage::table::append_state::TableAppendState as PhysicalTableAppendState;
use crate::storage::table::append_state::{BoundConstraint, ConstraintState, LocalAppendState};
use crate::storage::table::data_table_info::{DataTableInfo, IndexStorageInfo};
use crate::storage::table::delete_state::TableDeleteState;
use crate::storage::table::persistent_table_data::PersistentTableData;
use crate::storage::table::row_group_collection::RowGroupCollection;
use crate::storage::table::scan_state::{ColumnFetchState, ParallelTableScanState, TableScanState};
use crate::storage::table::segment_base::SegmentBase;
use crate::storage::table::types::{Idx, RowId, TransactionData};
use crate::storage::write_ahead_log::WriteAheadLog;
use crate::transaction::duck_transaction::DuckTransaction;
use parking_lot::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
// ─── 外部类型占位 ────────────────────────────────────────────────────────────
// 以下占位类型在相应子系统实现后应替换为真实导入。

// ── 执行层占位 ──────────────────────────────────────────────────────────────

/// 列定义（C++: `ColumnDefinition`）。
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub logical_type: LogicalType,
    /// 物理存储序号（C++: `storage_oid`）。
    pub storage_oid: u64,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self {
            name: name.into(),
            logical_type,
            storage_oid: 0,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }
}

// ─── Scan 相关类型 ────────────────────────────────────────────────────────────

/// 物理列存储索引（C++: `StorageIndex`，区别于逻辑 `LogicalIndex`）。
///
/// 用于指定 Scan/Fetch 时需要读取的列。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageIndex(pub u64);

/// 逻辑列索引路径（C++: `ColumnIndex`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnIndex(pub u64);

/// 扫描模式（C++: `TableScanType`）。
///
/// 用于 `CreateIndexScan`，区分是否需要 MVCC 可见性过滤。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableScanType {
    /// 普通扫描，仅返回对当前事务可见的已提交行（C++: `TABLE_SCAN_COMMITTED_ROWS`）。
    CommittedRows,
    /// 扫描所有已提交行，忽略事务可见性（C++: `TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED`）。
    CommittedRowsOmitDeleted,
    /// 扫描全部行（含未提交）（C++: `TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES`）。
    CommittedRowsDisallowUpdates,
}

/// 并行扫描状态（C++: `ParallelTableScanState`）。
///
/// 由 `InitializeParallelScan` 填充，供各并行线程调用 `NextParallelScan` 推进。
// (已通过 scan_state 模块导入，此处仅为文档注释目的保留说明)

/// 旧的表追加状态占位，保留给早期单元测试结构检查使用。
pub struct LegacyTableAppendState {
    pub row_start: i64,
    pub current_row: i64,
}

pub use crate::storage::table::update_state::TableUpdateState;
// ConstraintState 现在从 storage::table::append_state 导入，此处不再重复定义。

/// 表数据写入器（C++: `TableDataWriter`）。
pub struct TableDataWriter;

// ─── DataTableVersion ────────────────────────────────────────────────────────

/// 表版本状态（C++: `enum class DataTableVersion`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataTableVersion {
    /// 最新版本，未被 ALTER 或 DROP（C++: `MAIN_TABLE`）。
    MainTable = 0,
    /// 已被 ALTER（C++: `ALTERED`）。
    Altered = 1,
    /// 已被 DROP（C++: `DROPPED`）。
    Dropped = 2,
}

impl DataTableVersion {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::MainTable,
            1 => Self::Altered,
            2 => Self::Dropped,
            _ => Self::MainTable,
        }
    }
}

// ─── DataTable ───────────────────────────────────────────────────────────────

/// 物理表的根存储对象（C++: `class DataTable`）。
///
/// 通过 `Arc<DataTable>` 在 catalog、事务、查询执行之间共享。
pub struct DataTable {
    // ── 元数据 ───────────────────────────────────────────────────────────────
    /// 表的共享元数据（C++: `shared_ptr<DataTableInfo> info`）。
    pub info: Arc<DataTableInfo>,

    // ── 列定义 ───────────────────────────────────────────────────────────────
    /// 表的物理列定义列表（C++: `vector<ColumnDefinition> column_definitions`）。
    pub column_definitions: Vec<ColumnDefinition>,

    // ── Append 互斥 ──────────────────────────────────────────────────────────
    /// Append 操作互斥锁（C++: `mutex append_lock`）。
    ///
    /// 通过 `Mutex<()>` 实现；持有期间由 `TableAppendState.append_lock` 拥有。
    append_lock: Mutex<()>,

    // ── 行组数据 ─────────────────────────────────────────────────────────────
    /// 所有行组的有序集合（C++: `shared_ptr<RowGroupCollection> row_groups`）。
    pub row_groups: Arc<RowGroupCollection>,

    // ── 版本 ─────────────────────────────────────────────────────────────────
    /// 表版本（C++: `atomic<DataTableVersion> version`）。
    version: AtomicU8,
}

impl DataTable {
    // ── 构造函数 ─────────────────────────────────────────────────────────────

    /// 从（可选）持久化数据构造新表（C++: 第一个构造函数）。
    ///
    /// 若 `data` 为 `None` 或 `row_group_count == 0`，则初始化为空表。
    pub fn new(
        db_id: u64,
        table_io_manager_id: u64,
        schema: impl Into<String>,
        table: impl Into<String>,
        column_definitions: Vec<ColumnDefinition>,
        data: Option<Box<PersistentTableData>>,
    ) -> Arc<Self> {
        let schema = schema.into();
        let table = table.into();
        let info = Arc::new(DataTableInfo::new(
            db_id,
            table_io_manager_id,
            schema,
            table,
        ));
        let types: Vec<LogicalType> = column_definitions
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();
        let row_groups = RowGroupCollection::new(Arc::clone(&info), types, 0);

        let table = Arc::new(Self {
            info,
            column_definitions,
            append_lock: Mutex::new(()),
            row_groups,
            version: AtomicU8::new(DataTableVersion::MainTable as u8),
        });
        if let Some(data) = data {
            if let Some(runtime) = data.runtime.clone() {
                table.info.set_persistent_storage(runtime);
            }
            if data.row_group_count > 0 {
                table.row_groups.initialize_from_table_data(&data);
                if table.info.indexes.is_empty() {
                    table.row_groups.set_append_requires_new_row_group();
                }
            } else {
                table.row_groups.initialize_empty();
            }
        } else {
            table.row_groups.initialize_empty();
        }
        table
    }

    /// 构造添加列后的新 DataTable（C++: 第二个构造函数）。
    ///
    /// 父表被标记为 `Altered`，此表接管其 `info`。
    pub fn with_added_column(parent: &DataTable, new_column: ColumnDefinition) -> Arc<Self> {
        let mut column_definitions: Vec<ColumnDefinition> =
            parent.column_definitions.iter().cloned().collect();
        column_definitions.push(new_column);

        // parent 被替换，标记为 Altered
        parent
            .version
            .store(DataTableVersion::Altered as u8, Ordering::Release);

        let types: Vec<LogicalType> = column_definitions
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();
        let table = Arc::new(Self {
            info: Arc::clone(&parent.info),
            column_definitions,
            append_lock: Mutex::new(()),
            row_groups: RowGroupCollection::new(Arc::clone(&parent.info), types, 0),
            version: AtomicU8::new(DataTableVersion::MainTable as u8),
        });
        table.row_groups.initialize_empty();
        table
    }

    /// 构造移除列后的新 DataTable（C++: 第三个构造函数）。
    pub fn with_removed_column(parent: &DataTable, removed_column: usize) -> Arc<Self> {
        assert!(
            removed_column < parent.column_definitions.len(),
            "removed_column out of range"
        );

        let mut column_definitions: Vec<ColumnDefinition> =
            parent.column_definitions.iter().cloned().collect();
        column_definitions.remove(removed_column);

        parent
            .version
            .store(DataTableVersion::Altered as u8, Ordering::Release);

        let types: Vec<LogicalType> = column_definitions
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();
        let table = Arc::new(Self {
            info: Arc::clone(&parent.info),
            column_definitions,
            append_lock: Mutex::new(()),
            row_groups: RowGroupCollection::new(Arc::clone(&parent.info), types, 0),
            version: AtomicU8::new(DataTableVersion::MainTable as u8),
        });
        table.row_groups.initialize_empty();
        table
    }

    // ── 版本控制 ─────────────────────────────────────────────────────────────

    /// 设置为主表（C++: `SetAsMainTable()`）。
    pub fn set_as_main_table(&self) {
        self.version
            .store(DataTableVersion::MainTable as u8, Ordering::Release);
    }

    /// 标记为已 DROP（C++: `SetAsDropped()`）。
    pub fn set_as_dropped(&self) {
        self.version
            .store(DataTableVersion::Dropped as u8, Ordering::Release);
    }

    /// 是否是当前主版本（C++: `IsMainTable() / IsRoot()`）。
    pub fn is_main_table(&self) -> bool {
        self.version() == DataTableVersion::MainTable
    }

    /// 是否是当前主版本（C++: `IsRoot()`，与 `IsMainTable()` 相同）。
    pub fn is_root(&self) -> bool {
        self.is_main_table()
    }

    /// 读取当前版本。
    fn version(&self) -> DataTableVersion {
        DataTableVersion::from_u8(self.version.load(Ordering::Acquire))
    }

    /// 表修改状态描述（C++: `TableModification()`）。
    pub fn table_modification(&self) -> &'static str {
        match self.version() {
            DataTableVersion::MainTable => "no changes",
            DataTableVersion::Altered => "altered",
            DataTableVersion::Dropped => "dropped",
        }
    }

    // ── 基本查询 ─────────────────────────────────────────────────────────────

    /// 表名（C++: `GetTableName()`）。
    pub fn get_table_name(&self) -> String {
        self.info.get_table_name()
    }

    /// 设置表名（C++: `SetTableName()`，用于 ALTER TABLE RENAME）。
    pub fn set_table_name(&self, new_name: String) {
        self.info.set_table_name(new_name);
    }

    /// 是否为临时表（C++: `IsTemporary()`）。
    pub fn is_temporary(&self) -> bool {
        self.info.is_temporary()
    }

    /// 列数（C++: `ColumnCount()`）。
    pub fn column_count(&self) -> usize {
        self.column_definitions.len()
    }

    /// 全表总行数（C++: `GetTotalRows()`）。
    pub fn get_total_rows(&self) -> Idx {
        self.row_groups.total_rows()
    }

    /// 列类型列表（C++: `GetTypes()`）。
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.column_definitions
            .iter()
            .map(|c| c.logical_type.clone())
            .collect()
    }

    /// 列定义只读视图（C++: `Columns()`）。
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.column_definitions
    }

    /// RowGroup 大小（行数）（C++: `GetRowGroupSize()`）。
    pub fn get_row_group_size(&self) -> Idx {
        self.row_groups.row_group_size
    }

    /// 是否有索引（C++: `HasIndexes()`）。
    pub fn has_indexes(&self) -> bool {
        !self.info.indexes.is_empty()
    }

    /// 是否有唯一索引（C++: `HasUniqueIndexes()`）。
    pub fn has_unique_indexes(&self) -> bool {
        false
    }

    /// 共享元数据（C++: `GetDataTableInfo()`）。
    pub fn get_data_table_info(&self) -> Arc<DataTableInfo> {
        Arc::clone(&self.info)
    }

    // ── Scan ─────────────────────────────────────────────────────────────────

    /// 初始化顺序扫描（C++: `DataTable::InitializeScan()`）。
    ///
    /// 流程：
    /// 1. 用 `column_ids` 初始化 `state`。
    /// 2. 将持久化行组状态写入 `state.table_state`（`row_groups.initialize_scan()`）。
    /// 3. 将事务本地存储状态写入 `state.local_state`（`local_storage.initialize_scan()`）。
    ///
    /// ```text
    /// C++:
    ///   state.Initialize(column_ids, context, table_filters);
    ///   row_groups->InitializeScan(context, state.table_state, column_ids, table_filters);
    ///   local_storage.InitializeScan(*this, state.local_state, table_filters);
    /// ```
    pub fn initialize_scan(
        &self,
        context: &ClientContext,
        _transaction: &DuckTransaction,
        state: &mut TableScanState,
        column_ids: &[StorageIndex],
        table_filters: Option<&TableFilterSet>,
    ) {
        let storage_column_ids: Vec<u64> = column_ids.iter().map(|column_id| column_id.0).collect();
        state.initialize(storage_column_ids.clone());
        self.row_groups
            .initialize_scan(&mut state.table_state, &storage_column_ids);
        LocalStorage::get(context).initialize_scan(self, &mut state.local_state, table_filters);
    }

    /// 推进顺序扫描，填充一批结果（C++: `DataTable::Scan()`）。
    ///
    /// 优先扫描持久化行组（`state.table_state`），耗尽后扫描事务本地数据（`state.local_state`）。
    ///
    /// ```text
    /// C++:
    ///   void DataTable::Scan(DuckTransaction &transaction, DataChunk &result, TableScanState &state) {
    ///       if (state.table_state.Scan(transaction, result)) { D_ASSERT(result.size() > 0); return; }
    ///       auto &local_storage = LocalStorage::Get(transaction);
    ///       local_storage.Scan(state.local_state, state.GetColumnIds(), result);
    ///   }
    /// ```
    ///
    /// # 设计说明
    /// - 这里直接接收由上层从事务中提取出的 `ClientContext`，其内容正对应
    ///   DuckDB 在 `DataTable::Scan` 内部实际使用的 `TransactionData(transaction)`
    ///   与 `LocalStorage::Get(transaction)`。
    /// - `LocalStorage::scan` 目前返回 `false`（存根），待 `RowGroupCollection` 完整实现后填充。
    pub fn scan(
        &self,
        context: &ClientContext,
        result: &mut DataChunk,
        state: &mut TableScanState,
    ) {
        let transaction = DuckTransaction::get(context);
        let transaction_data = transaction.storage_transaction_data();
        // 1. 扫描持久化行组
        //    C++: if (state.table_state.Scan(transaction, result)) { D_ASSERT(...); return; }
        if state.table_state.scan(transaction_data, result) {
            debug_assert!(result.size() > 0, "scan returned true but result is empty");
            return;
        }

        // 2. 扫描事务本地数据
        //    C++: auto &local_storage = LocalStorage::Get(transaction);
        //         local_storage.Scan(state.local_state, state.GetColumnIds(), result);
        {
            let local_column_ids: Vec<StorageIndex> = state
                .column_ids()
                .into_iter()
                .map(StorageIndex)
                .collect();
            LocalStorage::get_from_transaction(&transaction)
                .scan(&mut state.local_state, &local_column_ids, result);
        }
    }

    /// 按行 ID 精确读取（C++: `DataTable::Fetch()`）。
    ///
    /// 用于 UPDATE / DELETE 时先按行 ID 取出旧值做约束验证。
    ///
    /// ```text
    /// C++:
    ///   row_groups->Fetch(transaction, result, column_ids, row_identifiers, fetch_count, state);
    /// ```
    pub fn fetch(
        &self,
        _column_ids: &[StorageIndex],
        _row_ids: &[RowId],
        _fetch_count: usize,
        _fetch_state: &mut ColumnFetchState,
        result: &mut DataChunk,
    ) {
        // RowGroupCollection::fetch is not yet implemented; return empty result.
        result.set_cardinality(0);
    }

    /// 判断事务是否可见某行（C++: `DataTable::CanFetch()`）。
    ///
    /// 用于 UPDATE 时检查行的可见性，避免读到其他事务未提交的行。
    pub fn can_fetch(&self, _row_id: RowId) -> bool {
        // RowGroupCollection::can_fetch is not yet implemented.
        false
    }

    /// 初始化并行扫描（C++: `DataTable::InitializeParallelScan()`）。
    ///
    /// 将行组范围和本地存储范围分配到 `state` 中，
    /// 各并行线程随后各自调用 `next_parallel_scan()`。
    ///
    /// ```text
    /// C++:
    ///   row_groups->InitializeParallelScan(state.scan_state);
    ///   local_storage.InitializeParallelScan(*this, state.local_state);
    /// ```
    pub fn initialize_parallel_scan(
        &self,
        context: &ClientContext,
        state: &mut ParallelTableScanState,
        _column_indexes: &[ColumnIndex],
    ) {
        self.row_groups
            .initialize_parallel_scan(&mut state.scan_state);
        LocalStorage::get(context).initialize_parallel_scan(self, &mut state.local_state);
    }

    /// 推进并行扫描（C++: `DataTable::NextParallelScan()`）。
    ///
    /// 线程安全地从共享的 `ParallelTableScanState` 中领取下一个扫描块，
    /// 将结果写入线程私有的 `scan_state`。
    ///
    /// 返回当前领取到的 row group 行数；返回 `0` 表示扫描已完成。
    ///
    /// ```text
    /// C++:
    ///   if (row_groups->NextParallelScan(context, state.scan_state, scan_state.table_state)) return true;
    ///   if (local_storage.NextParallelScan(context, *this, state.local_state, scan_state.local_state)) return true;
    ///   return false;
    /// ```
    pub fn next_parallel_scan(
        &self,
        context: &ClientContext,
        parallel_state: &mut ParallelTableScanState,
        scan_state: &mut TableScanState,
    ) -> usize {
        if self
            .row_groups
            .next_parallel_scan(&mut parallel_state.scan_state, &mut scan_state.table_state)
        {
            return scan_state
                .table_state
                .current_row_group
                .as_ref()
                .map(|current_row_group| current_row_group.row_group.count() as usize)
                .unwrap_or(0);
        }
        if LocalStorage::get(context).next_parallel_scan(
            context,
            self,
            &mut parallel_state.local_state,
            &mut scan_state.local_state,
        ) {
            return scan_state
                .local_state
                .current_row_group
                .as_ref()
                .map(|current_row_group| current_row_group.row_group.count() as usize)
                .unwrap_or(0);
        }
        0
    }

    /// 用于 CREATE INDEX 的提交行扫描（C++: `DataTable::CreateIndexScan()`）。
    ///
    /// 与普通 Scan 不同：跳过 MVCC 可见性过滤，直接扫描所有已提交行，
    /// 用于建索引时读取全量数据。
    ///
    /// 返回 `true` 表示还有数据，`false` 表示扫描完毕。
    ///
    /// ```text
    /// C++:
    ///   return state.table_state.ScanCommitted(result, type);
    /// ```
    pub fn create_index_scan(
        &self,
        state: &mut TableScanState,
        result: &mut DataChunk,
        _scan_type: TableScanType,
    ) -> bool {
        // C++: return state.table_state.ScanCommitted(result, type);
        // Scan all committed rows (no MVCC visibility filtering).
        self.row_groups.scan(
            TransactionData {
                start_time: u64::MAX,
                transaction_id: 0,
            },
            &mut state.table_state,
            result,
        )
    }

    /// 按偏移范围初始化扫描（C++: `DataTable::InitializeScanWithOffset()`）。
    ///
    /// 用于 WAL 回放时从指定行开始扫描，不涉及事务本地存储。
    ///
    /// ```text
    /// C++:
    ///   state.Initialize(column_ids);
    ///   row_groups->InitializeScanWithOffset(state.table_state, column_ids, start_row, end_row);
    /// ```
    pub fn initialize_scan_with_offset(
        &self,
        state: &mut TableScanState,
        column_ids: Vec<u64>,
        start_row: Idx,
        end_row: Idx,
    ) {
        // C++: state.Initialize(column_ids);
        //      row_groups->InitializeScanWithOffset(context, state.table_state, column_ids, start_row, end_row);
        state.initialize(column_ids.clone());
        self.row_groups.initialize_scan_with_offset(
            &mut state.table_state,
            &column_ids,
            start_row,
            end_row,
        );
    }

    /// 扫描表中指定行范围并逐块回调（C++: `DataTable::ScanTableSegment()`）。
    ///
    /// 用于 WAL 写入（`write_to_log`）和 `revert_append`：
    /// 按 `[row_start, row_start + count)` 迭代，每次填充最多一个 SIMD 向量的行，
    /// 调用 `f(&chunk)` 处理。
    ///
    /// ```text
    /// C++:
    ///   InitializeScanWithOffset(transaction, state, column_ids, row_start, row_start + count);
    ///   while (state.table_state.ScanCommitted(chunk, TABLE_SCAN_COMMITTED_ROWS))
    ///       f(chunk);
    /// ```
    pub fn scan_table_segment(
        &self,
        transaction: TransactionData,
        row_start: Idx,
        count: Idx,
        mut f: impl FnMut(&DataChunk),
    ) {
        if count == 0 {
            return;
        }
        let end = row_start + count;

        let col_count = self.column_definitions.len();
        let column_ids: Vec<u64> = (0..col_count as u64).collect();
        let types = self.get_types();

        let mut state = TableScanState::new();
        self.initialize_scan_with_offset(&mut state, column_ids, row_start, end);

        let mut chunk = DataChunk::new();
        chunk.initialize(&types, crate::common::types::STANDARD_VECTOR_SIZE);

        let Some(current_row_group) = state.table_state.current_row_group.clone() else {
            return;
        };
        let mut current_row = current_row_group.row_start
            + state.table_state.vector_index * crate::storage::table::types::STANDARD_VECTOR_SIZE;
        while current_row < end {
            chunk.reset();
            if !self.row_groups.scan(transaction, &mut state.table_state, &mut chunk) {
                break;
            }
            if chunk.size() == 0 {
                break;
            }

            let scan_end = current_row + chunk.size() as Idx;
            if scan_end <= row_start {
                current_row = scan_end;
                continue;
            }
            if current_row >= end {
                break;
            }

            let slice_start = row_start.saturating_sub(current_row) as usize;
            let slice_end = (end.min(scan_end) - current_row) as usize;
            debug_assert!(slice_start < slice_end);
            if slice_start != 0 || slice_end != chunk.size() {
                chunk.slice_range(slice_start, slice_end - slice_start);
            }
            f(&chunk);
            current_row = scan_end;
        }
    }

    /// 最大并行扫描线程数（C++: `DataTable::MaxThreads()`）。
    ///
    /// 基于 RowGroup 大小和总行数计算。
    ///
    /// ```text
    /// C++:
    ///   idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;
    ///   return GetTotalRows() / parallel_scan_tuple_count + 1;
    /// ```
    pub fn max_threads(&self, _context: &ClientContext) -> Idx {
        use crate::storage::table::types::STANDARD_VECTOR_SIZE;
        let row_group_size = self.get_row_group_size();
        let parallel_scan_vector_count = row_group_size / STANDARD_VECTOR_SIZE;
        let parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;
        self.get_total_rows() / parallel_scan_tuple_count + 1
    }

    // ── 索引管理 ─────────────────────────────────────────────────────────────

    /// 设置索引磁盘存储元数据（C++: `SetIndexStorageInfo()`）。
    pub fn set_index_storage_info(&self, infos: Vec<IndexStorageInfo>) {
        // C++: info->index_storage_infos = std::move(index_storage_info);
        self.info.set_index_storage_infos(infos);
    }

    /// 索引名称是否唯一（C++: `IndexNameIsUnique()`）。
    pub fn index_name_is_unique(&self, name: &str) -> bool {
        // C++: return info->indexes.NameIsUnique(name);
        self.info.indexes.name_is_unique(name)
    }

    // ── Checkpoint 锁 ────────────────────────────────────────────────────────

    /// 获取 checkpoint 独占锁（C++: `GetCheckpointLock()`）。
    ///
    /// 调用方在 checkpoint 期间持有此锁，阻止其他线程读写该表。
    pub fn get_checkpoint_lock(&self) -> Box<StorageLockKey> {
        self.info.checkpoint_lock.get_exclusive_lock()
    }

    // ── Append ───────────────────────────────────────────────────────────────

    /// 获取 Append 锁并填写 AppendState 基础信息（C++: `AppendLock()`）。
    ///
    /// 必须在 `initialize_append` 之前调用。
    /// Acquire the append lock and return the guard.
    ///
    /// C++: `state.append_lock = unique_lock<mutex>(append_lock)`.
    /// In Rust the caller holds the `MutexGuard` for the duration of the append.
    pub fn acquire_append_lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.append_lock.lock()
    }

    /// 初始化 Append 操作（C++: `InitializeAppend()`）。
    pub fn initialize_append(&self, state: &mut PhysicalTableAppendState) -> StorageResult<()> {
        if !self.is_main_table() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Transaction conflict: attempting to insert into table \"{}\" but it has been {} by a different transaction",
                    self.get_table_name(),
                    self.table_modification(),
                ),
            });
        }
        self.row_groups.initialize_append(state);
        Ok(())
    }

    /// 追加一个数据块（C++: `Append(DataChunk &chunk, TableAppendState &state)`）。
    pub fn append(
        &self,
        chunk: &mut DataChunk,
        state: &mut PhysicalTableAppendState,
    ) -> StorageResult<()> {
        if !self.is_main_table() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Transaction conflict: attempting to insert into table \"{}\" but it has been {} by a different transaction",
                    self.get_table_name(),
                    self.table_modification(),
                ),
            });
        }
        // C++: D_ASSERT(IsMainTable()); row_groups->Append(chunk, state);
        self.row_groups.append(chunk, state);
        Ok(())
    }

    /// 完成 Append（C++: `FinalizeAppend(DuckTransaction &transaction, TableAppendState &state)`）。
    pub fn finalize_append(
        &self,
        transaction: TransactionData,
        state: &mut PhysicalTableAppendState,
    ) -> StorageResult<()> {
        // C++: row_groups->FinalizeAppend(transaction, state);
        self.row_groups.finalize_append(transaction, state);
        Ok(())
    }

    /// 将 LocalStorage 合并进主存储（C++: `MergeStorage()`）。
    pub fn merge_storage(
        &self,
        local_collection: Arc<RowGroupCollection>,
        _commit_state: Option<&mut dyn StorageCommitState>,
    ) -> StorageResult<()> {
        self.row_groups.merge_storage(local_collection)
    }

    /// 清理过期的 Append 记录（C++: `CleanupAppend()`）。
    pub fn cleanup_append(&self, _lowest_transaction: u64, _start: Idx, _count: Idx) {
        // TODO: Implement cleanup logic for MVCC
        // This would remove old version information that's no longer needed
    }

    /// 提交 Append（C++: `CommitAppend()`）。
    pub fn commit_append(&self, commit_id: u64, row_start: Idx, count: Idx) {
        // 需要持有 append_lock
        let _guard = self.append_lock.lock();
        self.row_groups.commit_append(commit_id, row_start, count);
    }

    /// 写入 WAL（C++: `WriteToLog()`）。
    pub fn write_to_log(
        &self,
        transaction: TransactionData,
        log: &WriteAheadLog,
        row_start: Idx,
        mut count: Idx,
        mut commit_state: Option<&mut dyn StorageCommitState>,
    ) -> StorageResult<()> {
        let schema = self.info.get_schema_name();
        let table = self.info.get_table_name();
        log.write_set_table(&schema, &table)
            .map_err(StorageError::Io)?;

        let mut row_start = row_start;
        if let Some(commit_state) = commit_state.as_deref_mut() {
            if let Some((row_group_data, optimistic_count)) =
                commit_state.get_row_group_data(self.info.table_id(), row_start)
            {
                if optimistic_count > count as u64 {
                    return Err(StorageError::Other(format!(
                        "Optimistically written count cannot exceed actual count (got {}, expected {})",
                        optimistic_count, count
                    )));
                }
                if !row_group_data.data.is_empty() {
                    let payload = serialize_row_group_data_payload(row_group_data);
                    log.write_row_group_data(&payload)
                        .map_err(StorageError::Io)?;
                }
                row_start += optimistic_count as Idx;
                count -= optimistic_count as Idx;
                if count == 0 {
                    return Ok(());
                }
            }
        }

        let mut write_result = Ok(());
        self.scan_table_segment(transaction, row_start, count, |chunk| {
            if write_result.is_err() {
                return;
            }
            let payload = serialize_insert_chunk_payload(chunk);
            write_result = log.write_insert(&payload).map_err(StorageError::Io);
        });
        write_result?;
        Ok(())
    }

    /// 回滚 Append（C++: `RevertAppend()`）。
    pub fn revert_append(&self, start_row: Idx, _count: Idx) {
        let _guard = self.append_lock.lock();
        self.revert_append_internal(start_row);
    }

    /// 回滚 Append（内部，不获取锁）（C++: `RevertAppendInternal()`）。
    pub fn revert_append_internal(&self, start_row: Idx) {
        debug_assert!(self.is_main_table());
        // C++: row_groups->RevertAppendInternal(start_row);
        self.row_groups.revert_append_internal(start_row);
    }

    // ── Delete ───────────────────────────────────────────────────────────────

    /// 初始化 Delete 操作（C++: `InitializeDelete()`）。
    ///
    /// Full implementation requires the constraint/expression engine; returns a
    /// minimal `TableDeleteState` without constraint binding.
    pub fn initialize_delete(&self) -> Box<TableDeleteState> {
        // C++: info->BindIndexes(context);
        //      result->has_delete_constraints = TableHasDeleteConstraints(table);
        //      if (has_delete_constraints) { /* initialise verify_chunk and constraint_state */ }
        Box::new(TableDeleteState::new())
    }

    /// 删除行（C++: `Delete()`）。
    ///
    /// Full implementation requires row_id vectors and the MVCC delete path;
    /// returns 0 until RowGroupCollection::delete is implemented.
    pub fn delete(
        &self,
        _state: &mut TableDeleteState,
        context: &ClientContext,
        row_identifiers: &mut Vector,
        count: Idx,
    ) -> StorageResult<Idx> {
        if count == 0 {
            return Ok(0);
        }
        if !self.is_main_table() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Transaction conflict: attempting to delete from table \"{}\" but it has been {} by a different transaction",
                    self.get_table_name(),
                    self.table_modification(),
                ),
            });
        }

        row_identifiers.flatten(count as usize);
        let flat_row_ids = extract_row_ids(row_identifiers, count as usize)?;

        let transaction = DuckTransaction::get(context);
        let transaction_data = transaction.storage_transaction_data();
        let mut pos = 0usize;
        let mut delete_count = 0;
        while pos < count as usize {
            let start = pos;
            let is_transaction_delete = flat_row_ids[pos] >= MAX_ROW_ID;
            pos += 1;
            while pos < count as usize {
                if (flat_row_ids[pos] >= MAX_ROW_ID) != is_transaction_delete {
                    break;
                }
                pos += 1;
            }

            let current_count = (pos - start) as Idx;
            let mut offset_ids = flat_row_ids[start..pos].to_vec();

            if is_transaction_delete {
                delete_count += LocalStorage::get_from_transaction(&transaction).delete(
                    self.info.table_id(),
                    &mut offset_ids,
                    current_count,
                    TransactionData {
                        start_time: 0,
                        transaction_id: 0,
                    },
                );
            } else {
                transaction.modify_table(self.info.table_id());
                delete_count += self.row_groups.delete(
                    Some(&transaction),
                    transaction_data,
                    Some(self),
                    &mut offset_ids,
                    current_count,
                );
            }
        }
        Ok(delete_count)
    }

    // ── Update ───────────────────────────────────────────────────────────────

    /// 初始化 Update 操作（C++: `InitializeUpdate()`）。
    ///
    /// Full implementation requires the constraint/expression engine.
    pub fn initialize_update(&self) -> StorageResult<Box<TableUpdateState>> {
        if !self.is_main_table() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Transaction conflict: attempting to update table \"{}\" but it has been {} by a different transaction",
                    self.get_table_name(),
                    self.table_modification(),
                ),
            });
        }
        // C++: info->BindIndexes(context);
        //      result->constraint_state = InitializeConstraintState(table, bound_constraints);
        Ok(Box::new(TableUpdateState::new()))
    }

    /// 更新行（C++: `Update()`）。
    ///
    /// Current implementation supports in-place updates on transient segments only.
    ///
    /// Parameters mirror DuckDB C++:
    /// - `row_ids`: row identifiers (we treat them as 0-based physical row numbers)
    /// - `column_ids`: physical column indexes to update
    /// - `updates`: a DataChunk with one column per `column_ids`, and `updates.size() == row_ids.len()`
    pub fn update(
        &self,
        _state: &mut TableUpdateState,
        context: &ClientContext,
        row_ids: &mut Vector,
        column_ids: &[PhysicalIndex],
        updates: &mut DataChunk,
    ) -> StorageResult<()> {
        if !self.is_main_table() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Transaction conflict: attempting to update table \"{}\" but it has been {} by a different transaction",
                    self.get_table_name(),
                    self.table_modification(),
                ),
            });
        }
        updates.flatten();
        row_ids.flatten(updates.size());

        let flat_row_ids = extract_row_ids(row_ids, updates.size())?;
        if flat_row_ids.is_empty() || updates.size() == 0 {
            return Ok(());
        }
        if updates.size() != flat_row_ids.len() {
            return Err(StorageError::Other(format!(
                "DataTable::update: updates.size()={} must equal row_ids.len()={}",
                updates.size(),
                flat_row_ids.len()
            )));
        }
        if updates.column_count() != column_ids.len() {
            return Err(StorageError::Other(format!(
                "DataTable::update: updates.column_count()={} must equal column_ids.len()={}",
                updates.column_count(),
                column_ids.len()
            )));
        }

        let transaction = DuckTransaction::get(context);
        let transaction_data = transaction.storage_transaction_data();
        let mut local_sel = crate::common::types::SelectionVector::default();
        let mut global_sel = crate::common::types::SelectionVector::default();
        for (idx, row_id) in flat_row_ids.iter().copied().enumerate() {
            if row_id >= MAX_ROW_ID {
                local_sel.indices.push(idx as u32);
            } else {
                global_sel.indices.push(idx as u32);
            }
        }

        if !local_sel.indices.is_empty() {
            let row_ids_slice = slice_row_ids(&flat_row_ids, &local_sel);
            let updates_slice = slice_updates(updates, &local_sel.indices)?;
            LocalStorage::get_from_transaction(&transaction).update(
                self.info.table_id(),
                &row_ids_slice,
                column_ids,
                &updates_slice,
                TransactionData {
                    start_time: 0,
                    transaction_id: 0,
                },
            );
        }

        if !global_sel.indices.is_empty() {
            transaction.modify_table(self.info.table_id());

            let row_ids_slice = slice_row_ids(&flat_row_ids, &global_sel);
            let updates_slice = slice_updates(updates, &global_sel.indices)?;
            self.row_groups.update(
                Some(&transaction),
                transaction_data,
                Some(self),
                &row_ids_slice,
                column_ids,
                &updates_slice,
            )?;
        }
        Ok(())
    }

    // ── Checkpoint ───────────────────────────────────────────────────────────

    /// Checkpoint 到 TableDataWriter（C++: `Checkpoint()`）。
    pub fn checkpoint(&self, _writer: &mut TableDataWriter) -> StorageResult<()> {
        // C++: TableStatistics global_stats;
        //      row_groups->Checkpoint(writer, global_stats);
        //      if (!HasIndexes()) row_groups->SetAppendRequiresNewRowGroup();
        //      writer.FinalizeTable(global_stats, *info, *row_groups, serializer);
        let mut global_stats = crate::storage::table::table_statistics::TableStatistics::new();
        self.row_groups.checkpoint(&mut global_stats);
        Ok(())
    }

    /// 提交 DROP TABLE（C++: `CommitDropTable()`）。
    pub fn commit_drop_table(&self) {
        // C++: row_groups->CommitDropTable();
        //      info->indexes.Scan([&](Index &index) { index.Cast<BoundIndex>().CommitDrop(); return false; });
        self.row_groups.commit_drop_table();
        // Index CommitDrop not yet implemented (BoundIndex stub has no CommitDrop).
    }

    /// 提交 DROP COLUMN（C++: `CommitDropColumn()`）。
    pub fn commit_drop_column(&self, column_index: usize) {
        // C++: row_groups->CommitDropColumn(column_index);
        self.row_groups.commit_drop_column(column_index);
    }

    /// 销毁（C++: `Destroy()`）。
    pub fn destroy(&self) {
        // C++: row_groups->Destroy();
        self.row_groups.destroy();
    }

    // ── 本地存储（LocalStorage）───────────────────────────────────────────────

    /// 初始化事务本地 Append（C++: `InitializeLocalAppend()`）。
    ///
    /// 对应 C++:
    /// ```cpp
    /// void DataTable::InitializeLocalAppend(
    ///     LocalAppendState &state, TableCatalogEntry &table,
    ///     ClientContext &context,
    ///     const vector<unique_ptr<BoundConstraint>> &bound_constraints)
    /// {
    ///     if (!IsMainTable()) { throw ...; }
    ///     auto &local_storage = LocalStorage::Get(context, db);
    ///     local_storage.InitializeAppend(state, *this);
    ///     state.constraint_state = InitializeConstraintState(table, bound_constraints);
    /// }
    /// ```
    ///
    /// # 参数
    /// - `state`: 调用方提供的空 [`LocalAppendState`]，本方法填充其所有字段。
    /// - `table`: 目标表的 catalog 条目（用于约束绑定）。
    /// - `context`: 客户端上下文（提供事务本地 `LocalStorage`）。
    /// - `bound_constraints`: 已绑定的表约束列表。
    ///
    /// # Rust 与 C++ 的差异
    /// - C++ 用异常报错；Rust 用 `StorageResult`。
    /// - C++ `LocalStorage::Get(context, db)` 通过全局注册表查找；
    ///   Rust 直接持有 `context.local_storage`。
    /// - `info.table_id` 代替 `*this` 作为表的唯一标识。
    pub fn initialize_local_append(
        &self,
        state: &mut LocalAppendState,
        table: &TableCatalogEntry,
        context: &Arc<ClientContext>,
        bound_constraints: &[BoundConstraint],
    ) -> StorageResult<()> {
        // 1. 事务冲突检查（C++: `if (!IsMainTable()) throw`）
        if !self.is_main_table() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Transaction conflict: attempting to insert into table \"{}\" \
                     but it has been {} by a different transaction",
                    self.get_table_name(),
                    self.table_modification(),
                ),
            });
        }

        // 2. 初始化 state.storage（C++: `local_storage.InitializeAppend(state, *this)`）
        //    table_id = db_id * 2^32 + table_io_manager_id 作为唯一键（临时方案）
        let table_id = self.info.table_id();
        LocalStorage::get(context.as_ref()).initialize_append_state(
            context,
            state,
            table_id,
            Arc::clone(&self.info),
            self.get_types(),
        );

        // 3. 绑定约束状态（C++: `state.constraint_state = InitializeConstraintState(...)`）
        state.constraint_state = Some(Box::new(ConstraintState::new(
            &table.base.fields().name,
            bound_constraints,
        )));

        Ok(())
    }

    /// 将一个 chunk 追加到事务本地存储（C++: `LocalAppend(state, context, chunk, unsafe)`）。
    ///
    /// 对应 C++:
    /// ```cpp
    /// void DataTable::LocalAppend(
    ///     LocalAppendState &state, ClientContext &context,
    ///     DataChunk &chunk, bool unsafe)
    /// {
    ///     if (chunk.size() == 0) return;
    ///     if (!IsMainTable()) { throw ...; }
    ///     chunk.Verify();
    ///     if (!unsafe)
    ///         VerifyAppendConstraints(*state.constraint_state, context, chunk, *state.storage, nullptr);
    ///     auto data_table_info = GetDataTableInfo();
    ///     LocalStorage::Append(state, chunk, *data_table_info);
    /// }
    /// ```
    ///
    /// # 参数
    /// - `state`: 已由 [`initialize_local_append`] 填充的状态。
    /// - `context`: 客户端上下文（仅在约束校验时使用，当前为占位）。
    /// - `chunk`: 要插入的数据批次。
    /// - `skip_constraints`: C++ `unsafe` 参数；`true` 跳过约束校验
    ///   （用于 WAL 回放，此时数据已被校验过）。
    ///
    /// # Rust 与 C++ 的差异
    /// - C++ `unsafe` 是关键字，Rust 改为 `skip_constraints`。
    /// - `chunk.Verify()` 尚未实现（需要执行层向量校验逻辑）。
    /// - `VerifyAppendConstraints` 是存根（约束校验依赖表达式求值引擎）。
    pub fn local_append_chunk(
        &self,
        state: &mut LocalAppendState,
        _context: &Arc<ClientContext>,
        chunk: &mut DataChunk,
        skip_constraints: bool,
    ) -> StorageResult<()> {
        // 1. 空 chunk 短路（C++: `if (chunk.size() == 0) return`）
        if chunk.size() == 0 {
            return Ok(());
        }

        // 2. 事务冲突检查（C++: `if (!IsMainTable()) throw`）
        if !self.is_main_table() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Transaction conflict: attempting to insert into table \"{}\" \
                     but it has been {} by a different transaction",
                    self.get_table_name(),
                    self.table_modification(),
                ),
            });
        }

        // 3. 约束校验（C++: `VerifyAppendConstraints(constraint_state, context, chunk, ...)`）
        //    skip_constraints == true 时跳过（WAL 回放路径，对应 C++ `unsafe == true`）
        if !skip_constraints {
            // TODO: self.verify_append_constraints(
            //           state.constraint_state.as_ref().expect("constraint_state not set"),
            //           _context, chunk, state.storage.as_ref().expect("storage not set"))
        }

        // 4. 写入本地存储（C++: `LocalStorage::Append(state, chunk, *data_table_info)`）
        LocalStorage::append(state, chunk, &self.info)?;

        Ok(())
    }

    /// 完成事务本地 Append（C++: `FinalizeLocalAppend(state)`）。
    ///
    /// 对应 C++:
    /// ```cpp
    /// void DataTable::FinalizeLocalAppend(LocalAppendState &state) {
    ///     LocalStorage::FinalizeAppend(state);
    /// }
    /// ```
    ///
    /// 调用此方法后 `state` 不应再被使用。
    pub fn finalize_local_append(&self, state: &mut LocalAppendState) {
        LocalStorage::finalize_append(state);
    }

    /// 将一个 chunk 原子性地追加到表（初始化→追加→完成的便捷包装）。
    ///
    /// 对应 C++ 的三步便捷重载：
    /// ```cpp
    /// void DataTable::LocalAppend(
    ///     TableCatalogEntry &table, ClientContext &context,
    ///     DataChunk &chunk,
    ///     const vector<unique_ptr<BoundConstraint>> &bound_constraints)
    /// {
    ///     LocalAppendState append_state;
    ///     InitializeLocalAppend(append_state, table, context, bound_constraints);
    ///     LocalAppend(append_state, context, chunk, false);
    ///     FinalizeLocalAppend(append_state);
    /// }
    /// ```
    ///
    /// # 参数
    /// - `table`: 目标表的 catalog 条目。
    /// - `context`: 客户端上下文。
    /// - `chunk`: 要插入的数据批次。
    /// - `bound_constraints`: 已绑定的约束列表。
    ///
    /// # 错误
    /// 若表已被 ALTER/DROP（非主版本），或约束校验失败，返回 `Err`。
    pub fn local_append(
        &self,
        table: &TableCatalogEntry,
        context: &Arc<ClientContext>,
        chunk: &mut DataChunk,
        bound_constraints: &[BoundConstraint],
    ) -> StorageResult<()> {
        let mut append_state = LocalAppendState::new();
        self.initialize_local_append(&mut append_state, table, context, bound_constraints)?;
        self.local_append_chunk(&mut append_state, context, chunk, false)?;
        self.finalize_local_append(&mut append_state);
        Ok(())
    }

    // ── 统计 ─────────────────────────────────────────────────────────────────

    /// 获取列统计信息（C++: `GetStatistics()`）。
    pub fn get_statistics(
        &self,
        column_id: u64,
    ) -> Option<crate::storage::data_pointer::BaseStatistics> {
        if column_id == u64::MAX {
            // C++: if (column_id == COLUMN_IDENTIFIER_ROW_ID) return nullptr;
            return None;
        }
        // C++: return row_groups->CopyStats(column_id);
        self.row_groups.copy_stats(column_id)
    }

    // ── 列段调试信息 ──────────────────────────────────────────────────────────

    /// 获取所有列段的调试信息（C++: `GetColumnSegmentInfo()`）。
    pub fn get_column_segment_info(&self) -> Vec<ColumnSegmentInfo> {
        // C++: return row_groups->GetColumnSegmentInfo(context);
        // Map the raw tuples returned by RGC into ColumnSegmentInfo structs.
        self.row_groups
            .get_column_segment_info()
            .into_iter()
            .map(
                |(column_id, row_group_index, segment_type)| ColumnSegmentInfo {
                    column_id,
                    row_group_index,
                    segment_type,
                },
            )
            .collect()
    }

    pub fn begin_scan_state(&self, column_ids: Vec<u64>) -> TableScanState {
        let mut state = TableScanState::new();
        state.initialize(column_ids.clone());
        self.row_groups
            .initialize_scan(&mut state.table_state, &column_ids);
        state
    }

    pub fn begin_scan_state_with_offset(
        &self,
        column_ids: Vec<u64>,
        start_row: Idx,
        end_row: Idx,
    ) -> TableScanState {
        let mut state = TableScanState::new();
        state.initialize(column_ids.clone());
        self.row_groups.initialize_scan_with_offset(
            &mut state.table_state,
            &column_ids,
            start_row,
            end_row,
        );
        state
    }

    pub fn scan_committed_chunk(&self, state: &mut TableScanState, result: &mut DataChunk) -> bool {
        let column_ids = state.column_ids();
        if result.column_count() != column_ids.len() {
            let types: Vec<LogicalType> = column_ids
                .iter()
                .map(|idx| self.column_definitions[*idx as usize].logical_type.clone())
                .collect();
            result.initialize(
                &types,
                crate::storage::table::types::STANDARD_VECTOR_SIZE as usize,
            );
        } else {
            result.reset();
        }

        self.row_groups.scan(
            TransactionData {
                start_time: u64::MAX,
                transaction_id: 0,
            },
            &mut state.table_state,
            result,
        )
    }

    pub fn append_chunk_for_testing(&self, chunk: &mut DataChunk) {
        let mut state = PhysicalTableAppendState::new();
        self.row_groups.initialize_append(&mut state);
        self.row_groups.append(chunk, &mut state);
        self.row_groups.finalize_append(
            TransactionData {
                start_time: 0,
                transaction_id: 0,
            },
            &mut state,
        );
    }
}

// ─── ColumnSegmentInfo ────────────────────────────────────────────────────────

/// 列段信息（C++: `ColumnSegmentInfo`）。
#[derive(Debug, Clone)]
pub struct ColumnSegmentInfo {
    pub column_id: u64,
    pub row_group_index: u64,
    pub segment_type: String,
}

// ─── TableStorageInfo ────────────────────────────────────────────────────────

/// 表存储摘要信息（C++: `TableStorageInfo`）。
#[derive(Debug, Default)]
pub struct TableStorageInfo {
    /// 表的当前行数（C++: `cardinality`）。
    pub cardinality: Idx,
    /// 各索引的元信息（C++: `vector<IndexInfo> index_info`）。
    pub index_info: Vec<IndexInfo>,
}

/// 单个索引的元信息（C++: `IndexInfo`）。
#[derive(Debug, Default)]
pub struct IndexInfo {
    pub is_primary: bool,
    pub is_unique: bool,
    pub is_foreign: bool,
    /// 索引覆盖的列 ID 集合（C++: `physical_index_set_t column_set`）。
    pub column_set: Vec<u64>,
}

impl DataTable {
    /// 获取表的存储摘要信息（C++: `GetStorageInfo()`）。
    pub fn get_storage_info(&self) -> TableStorageInfo {
        TableStorageInfo {
            cardinality: self.get_total_rows(),
            index_info: vec![], // 需索引层实现后填充
        }
    }
}

pub(crate) fn serialize_insert_chunk_payload(chunk: &DataChunk) -> Vec<u8> {
    let mut out = Vec::new();
    let row_count = chunk.size() as u32;
    let column_count = chunk.column_count() as u32;
    out.extend_from_slice(&row_count.to_le_bytes());
    out.extend_from_slice(&column_count.to_le_bytes());

    for column in &chunk.data {
        serialize_wal_vector(column, chunk.size(), &mut out);
    }
    out
}

fn serialize_wal_vector(vector: &Vector, count: usize, out: &mut Vec<u8>) {
    serialize_wal_validity(vector, count, out);

    match vector.logical_type.id {
        LogicalTypeId::Varchar => {
            for row_idx in 0..count {
                let is_valid = vector.row_is_valid(row_idx);
                let bytes = if is_valid {
                    vector.read_varchar_bytes(row_idx)
                } else {
                    Vec::new()
                };
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
        }
        LogicalTypeId::Struct | LogicalTypeId::Variant => {
            for child in vector.get_children() {
                serialize_wal_vector(child, count, out);
            }
        }
        LogicalTypeId::List => {
            for row_idx in 0..count {
                if !vector.row_is_valid(row_idx) {
                    out.extend_from_slice(&0u32.to_le_bytes());
                    out.extend_from_slice(&0u32.to_le_bytes());
                    continue;
                }
                let source_row = vector.resolved_row_index(row_idx);
                let base = source_row * 8;
                out.extend_from_slice(&vector.raw_data()[base..base + 8]);
            }
            let child = vector
                .get_child()
                .expect("serialize_insert_chunk_payload: LIST child vector missing");
            let list_size = compute_list_child_count(vector, count);
            out.extend_from_slice(&(list_size as u64).to_le_bytes());
            serialize_wal_vector(child, list_size, out);
        }
        LogicalTypeId::Array => {
            let child = vector
                .get_child()
                .expect("serialize_insert_chunk_payload: ARRAY child vector missing");
            let array_size = vector.logical_type.get_array_size();
            serialize_wal_vector(child, count * array_size, out);
        }
        _ => {
            serialize_wal_fixed_size(vector, count, out);
        }
    }
}

fn serialize_wal_fixed_size(vector: &Vector, count: usize, out: &mut Vec<u8>) {
    let type_size = vector.logical_type.physical_size();
    for row_idx in 0..count {
        serialize_wal_fixed_size_row(vector, row_idx, type_size, out);
    }
}

fn serialize_wal_fixed_size_row(vector: &Vector, row_idx: usize, type_size: usize, out: &mut Vec<u8>) {
    match vector.get_vector_type() {
        crate::common::types::VectorType::Sequence => {
            let value = vector
                .get_sequence()
                .map(|(start, increment)| start + (row_idx as i64) * increment)
                .expect("sequence vector missing parameters");
            match type_size {
                8 => out.extend_from_slice(&value.to_le_bytes()),
                4 => out.extend_from_slice(&(value as i32).to_le_bytes()),
                2 => out.extend_from_slice(&(value as i16).to_le_bytes()),
                1 => out.push(value as u8),
                _ => panic!(
                    "serialize_insert_chunk_payload: unsupported sequence type size {}",
                    type_size
                ),
            }
        }
        crate::common::types::VectorType::Dictionary => {
            let child = vector
                .get_child()
                .expect("serialize_insert_chunk_payload: dictionary child vector missing");
            let source_row = vector.resolved_row_index(row_idx);
            serialize_wal_fixed_size_row(child, source_row, type_size, out);
        }
        _ => {
            let source_row = vector.resolved_row_index(row_idx);
            let source_offset = source_row * type_size;
            let bytes = &vector.raw_data()[source_offset..source_offset + type_size];
            out.extend_from_slice(bytes);
        }
    }
}

fn serialize_wal_validity(vector: &Vector, count: usize, out: &mut Vec<u8>) {
    let has_validity = (0..count).any(|row_idx| !vector.row_is_valid(row_idx));
    out.push(has_validity as u8);
    if !has_validity {
        return;
    }

    let byte_len = count.div_ceil(8);
    out.extend_from_slice(&(byte_len as u32).to_le_bytes());
    for byte_idx in 0..byte_len {
        let mut byte = 0u8;
        for bit in 0..8 {
            let row_idx = byte_idx * 8 + bit;
            if row_idx < count && vector.row_is_valid(row_idx) {
                byte |= 1 << bit;
            }
        }
        out.push(byte);
    }
}

fn compute_list_child_count(vector: &Vector, count: usize) -> usize {
    let mut max_end = 0usize;
    for row_idx in 0..count {
        if !vector.validity.row_is_valid(row_idx) {
            continue;
        }
        let base = row_idx * 8;
        let offset =
            u32::from_le_bytes(vector.raw_data()[base..base + 4].try_into().expect("invalid list offset"))
                as usize;
        let len = u32::from_le_bytes(
            vector.raw_data()[base + 4..base + 8]
                .try_into()
                .expect("invalid list length"),
        ) as usize;
        max_end = max_end.max(offset + len);
    }
    max_end
}

fn extract_row_ids(row_ids: &Vector, count: usize) -> StorageResult<Vec<RowId>> {
    let raw = row_ids.raw_data();
    if raw.len() < count * 8 {
        return Err(StorageError::Other(format!(
            "DataTable::update: row_ids buffer too small ({} < {})",
            raw.len(),
            count * 8
        )));
    }
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let base = i * 8;
        result.push(i64::from_le_bytes(
            raw[base..base + 8]
                .try_into()
                .expect("row id bytes must be 8 bytes"),
        ));
    }
    Ok(result)
}

fn slice_updates(updates: &DataChunk, indices: &[u32]) -> StorageResult<DataChunk> {
    let mut out = DataChunk::new();
    let types: Vec<LogicalType> = updates
        .data
        .iter()
        .map(|v| v.logical_type.clone())
        .collect();
    out.initialize(&types, indices.len());
    let sel = crate::common::types::SelectionVector {
        indices: indices.to_vec(),
    };
    for (dst, src) in out.data.iter_mut().zip(updates.data.iter()) {
        dst.copy_from_sel(src, &sel, indices.len(), 0);
    }
    out.set_cardinality(indices.len());
    Ok(out)
}

fn slice_row_ids(row_ids: &[RowId], sel: &crate::common::types::SelectionVector) -> Vec<RowId> {
    sel.indices
        .iter()
        .map(|&idx| row_ids[idx as usize])
        .collect()
}

fn serialize_row_group_data_payload(
    data: &crate::storage::storage_manager::PersistentCollectionData,
) -> Vec<u8> {
    // Current WAL replay path only needs a stable opaque payload. The full
    // PersistentCollectionData serializer is not implemented yet in this port.
    let mut out = Vec::new();
    out.extend_from_slice(&(data.data.len() as u32).to_le_bytes());
    out.extend_from_slice(&data.data);
    out
}
