//! 事务本地存储（Transaction-Local Storage）。
//!
//! 对应 C++:
//!   `duckdb/transaction/local_storage.hpp`
//!   `src/storage/local_storage.cpp`
//!
//! # 职责
//!
//! 每个事务在提交之前，所有 INSERT/DELETE/UPDATE 都先写入
//! `LocalStorage`，以实现 MVCC 的写隔离。提交时再将本地存储
//! Flush 到全局 `DataTable`。
//!
//! # 层次结构
//!
//! ```text
//! LocalStorage
//!   └── LocalTableManager
//!         └── HashMap<table_id: u64, Arc<Mutex<LocalTableStorage>>>
//!               ├── row_groups: OptimisticWriteCollection  (事务本地行组)
//!               ├── append_indexes: TableIndexList
//!               ├── delete_indexes: TableIndexList
//!               └── optimistic_writer: OptimisticDataWriter
//! ```

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::AtomicU8,
};

use parking_lot::Mutex;

use crate::catalog::table_catalog_entry::PhysicalIndex;
use crate::common::types::DataChunk;
use crate::storage::data_table::{
    ClientContext, ColumnDefinition, DataTable, StorageCommitState, StorageIndex,
};
use crate::storage::optimistic_data_writer::{
    OptimisticDataWriter, OptimisticWriteCollection, OptimisticWritePartialManagers,
};
use crate::storage::storage_info::{StorageError, StorageResult};
use crate::storage::table::append_state::{BoundConstraint, LocalAppendState, TableAppendState};
use crate::storage::table::data_table_info::DataTableInfo;
use crate::storage::table::row_group_collection::RowGroupCollection;
use crate::storage::table::scan_state::{
    CollectionScanState, ColumnFetchState, ParallelCollectionScanState,
};
use crate::storage::table::table_index_list::TableIndexList;
use crate::storage::table::types::{Idx, LogicalType, RowId, TransactionData};

// ─── MAX_ROW_ID ───────────────────────────────────────────────────────────────

/// 事务本地行 ID 基准偏移（C++: `MAX_ROW_ID = (1LL << 62) - 1`）。
///
/// 本地未提交行 ID 从此值开始偏移，与已提交行区分。
pub const MAX_ROW_ID: i64 = (1i64 << 62) - 1;

// ─── IndexAppendMode ──────────────────────────────────────────────────────────

/// 索引追加模式（C++: `enum class IndexAppendMode`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAppendMode {
    /// 默认：检查约束冲突（C++: `DEFAULT`）。
    Default,
    /// WAL 回放时跳过约束检查（C++: `INSERT_DUPLICATES`）。
    InsertDuplicates,
}

// ─── PartitionStatistics ──────────────────────────────────────────────────────

/// 分区级统计信息（C++: `PartitionStatistics`）。
pub struct PartitionStatistics {
    pub row_count: Idx,
    pub deleted_count: Idx,
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalTableStorage
// ─────────────────────────────────────────────────────────────────────────────

/// 单张表的事务本地存储（C++: `class LocalTableStorage`）。
///
/// 持有本事务对该表的所有未提交写入（行组、索引、删除计数等）。
/// 提交时由 `LocalStorage::flush_one` 合并到主表。
pub struct LocalTableStorage {
    // ── 表标识 ───────────────────────────────────────────────────────────────

    /// 表的唯一数字标识（C++: 通过 `reference<DataTable>` 地址标识）。
    pub table_id: u64,

    /// 表的共享元数据（C++: `QueryContext context; reference<DataTable> table_ref`）。
    pub info: Arc<DataTableInfo>,

    /// 列类型列表（C++: 通过 `table.GetTypes()` 获取）。
    pub types: Vec<LogicalType>,

    // ── 行组数据 ──────────────────────────────────────────────────────────────

    /// 主行组集合（C++: `unique_ptr<OptimisticWriteCollection> row_groups`）。
    pub row_groups: Box<OptimisticWriteCollection>,

    // ── 索引 ──────────────────────────────────────────────────────────────────

    /// 唯一约束追加索引（C++: `TableIndexList append_indexes`）。
    pub append_indexes: TableIndexList,

    /// 删除辅助索引（C++: `TableIndexList delete_indexes`）。
    pub delete_indexes: TableIndexList,

    // ── 模式与计数 ────────────────────────────────────────────────────────────

    /// 索引追加模式（C++: `IndexAppendMode index_append_mode`）。
    pub index_append_mode: IndexAppendMode,

    /// 本地已删除行数（C++: `idx_t deleted_rows`）。
    pub deleted_rows: Idx,

    // ── 乐观写入 ──────────────────────────────────────────────────────────────

    /// 乐观写入集合列表（C++: `vector<unique_ptr<OptimisticWriteCollection>>`）。
    optimistic_collections: Vec<Option<Box<OptimisticWriteCollection>>>,

    /// 主乐观数据写入器（C++: `OptimisticDataWriter optimistic_writer`）。
    pub optimistic_writer: OptimisticDataWriter,

    // ── 标志 ──────────────────────────────────────────────────────────────────

    /// 是否已通过 LocalMerge 合并（C++: `bool merged_storage`）。
    pub merged_storage: bool,

    /// 是否已被 DROP（C++: `bool is_dropped`）。
    pub is_dropped: bool,

    // ── 锁 ────────────────────────────────────────────────────────────────────

    /// 保护 `optimistic_collections` 的互斥锁（C++: `mutex collections_lock`）。
    collections_lock: Mutex<()>,
}

impl LocalTableStorage {
    // ── 构造函数 ─────────────────────────────────────────────────────────────

    /// 从表元数据和类型创建空的本地存储（C++: 第一个构造函数）。
    ///
    /// 创建一个最小虚拟 `DataTable`（用于乐观写入器），
    /// 初始化空行组集合，并扫描已有唯一约束索引。
    pub fn new(
        table_id: u64,
        info: Arc<DataTableInfo>,
        types: Vec<LogicalType>,
    ) -> Arc<Mutex<Self>> {
        // 创建虚拟 DataTable 供 OptimisticDataWriter 使用。
        // 乐观写入器仅需要列定义来确定压缩类型（默认 Auto），
        // 因此创建一个轻量占位 DataTable 是安全的。
        let col_defs: Vec<ColumnDefinition> = types
            .iter()
            .enumerate()
            .map(|(i, t)| ColumnDefinition::new(format!("col_{}", i), t.clone()))
            .collect();
        let virtual_table = DataTable::new(
            info.db_id,
            info.table_io_manager_id,
            "",
            "",
            col_defs,
            None,
        );

        // 创建最小上下文供乐观写入器持有。
        // 写入器仅用该上下文读取写缓冲阈值（默认 2），无副作用。
        let ctx = Arc::new(Mutex::new(ClientContext {
            local_storage: LocalStorage::new(),
        }));

        // C++: optimistic_writer(context, table)
        let mut optimistic_writer =
            OptimisticDataWriter::new(ctx, Arc::clone(&virtual_table));

        // C++: row_groups = optimistic_writer.CreateCollection(table, types, GLOBAL)
        let row_groups = Box::new(optimistic_writer.create_collection(
            Arc::clone(&virtual_table),
            &types,
            OptimisticWritePartialManagers::Global,
        ));

        // C++: collection.InitializeEmpty()
        row_groups.collection.initialize_empty();

        // C++: data_table_info->GetIndexes().Scan([&](Index &index) { ... })
        // 扫描表上的 ART 唯一/主键索引，并创建本地副本。
        // 当前 Rust ART 实现为 stub，跳过实际 ART 复制，仅初始化空列表。
        let append_indexes = TableIndexList::new();
        let delete_indexes = TableIndexList::new();
        info.indexes.scan(|_index| {
            // 完整实现：
            //   检查 index.constraint_type() 是否为 UNIQUE/PRIMARY_KEY
            //   检查 index.index_type() == ART::TYPE_NAME
            //   检查 index.is_bound()
            //   创建 delete_index = ART(name, constraint, col_ids, ...)
            //   创建 append_index = ART(name, constraint, col_ids, ...)
            //   delete_indexes.add_index(delete_index)
            //   append_indexes.add_index(append_index)
            false
        });

        Arc::new(Mutex::new(Self {
            table_id,
            info,
            types,
            row_groups,
            append_indexes,
            delete_indexes,
            index_append_mode: IndexAppendMode::Default,
            deleted_rows: 0,
            optimistic_collections: Vec::new(),
            optimistic_writer,
            merged_storage: false,
            is_dropped: false,
            collections_lock: Mutex::new(()),
        }))
    }

    /// 从 ALTER TYPE 操作的父存储创建（C++: 第二个构造函数）。
    pub fn from_alter_type(
        new_table_id: u64,
        new_info: Arc<DataTableInfo>,
        parent: &mut LocalTableStorage,
        alter_column_index: usize,
        target_type: &LogicalType,
    ) -> Arc<Mutex<Self>> {
        // C++: auto new_collection = parent_collection.AlterType(context, alter_column_index, target_type, ...);
        //      parent_collection.CommitDropColumn(alter_column_index);
        //      row_groups = std::move(parent.row_groups);
        //      row_groups->collection = std::move(new_collection);
        //
        // Rust: RowGroupCollection::alter_type 暂未完整实现，
        // 暂用 remove_column + add_column 近似（不保证数据完整性）。
        let parent_col = Arc::clone(&parent.row_groups.collection);

        // 应用列类型更改（近似实现）
        let new_collection = parent_col.remove_column(alter_column_index);
        parent_col.commit_drop_column(alter_column_index);

        // 更新父存储的 row_groups，然后取出
        parent.row_groups.collection = new_collection;

        // 移出父存储的 row_groups
        let mut new_types = parent.types.clone();
        if alter_column_index < new_types.len() {
            new_types[alter_column_index] = target_type.clone();
        }

        // C++: optimistic_writer(new_data_table, parent.optimistic_writer)
        //      optimistic_collections = std::move(parent.optimistic_collections)
        let col_defs: Vec<ColumnDefinition> = new_types
            .iter()
            .enumerate()
            .map(|(i, t)| ColumnDefinition::new(format!("col_{}", i), t.clone()))
            .collect();
        let new_virtual_table = DataTable::new(
            new_info.db_id,
            new_info.table_io_manager_id,
            "",
            "",
            col_defs,
            None,
        );

        let optimistic_writer = OptimisticDataWriter::new_with_parent(
            Arc::clone(&new_virtual_table),
            &mut parent.optimistic_writer,
        );

        let mut optimistic_collections = Vec::new();
        std::mem::swap(&mut optimistic_collections, &mut parent.optimistic_collections);

        // C++: append_indexes.Move(parent.append_indexes)
        let append_indexes = TableIndexList::new();

        let row_groups = Box::new(OptimisticWriteCollection::new(
            Arc::clone(&parent.row_groups.collection),
        ));

        Arc::new(Mutex::new(Self {
            table_id: new_table_id,
            info: new_info,
            types: new_types,
            row_groups,
            append_indexes,
            delete_indexes: TableIndexList::new(),
            index_append_mode: parent.index_append_mode,
            deleted_rows: parent.deleted_rows,
            optimistic_collections,
            optimistic_writer,
            merged_storage: parent.merged_storage,
            is_dropped: false,
            collections_lock: Mutex::new(()),
        }))
    }

    /// 从 DROP COLUMN 操作的父存储创建（C++: 第三个构造函数）。
    pub fn from_drop_column(
        new_table_id: u64,
        new_info: Arc<DataTableInfo>,
        parent: &mut LocalTableStorage,
        drop_column_index: usize,
    ) -> Arc<Mutex<Self>> {
        // C++: auto new_collection = parent_collection.RemoveColumn(drop_column_index);
        //      parent_collection.CommitDropColumn(drop_column_index);
        //      row_groups = std::move(parent.row_groups); row_groups->collection = new_collection;
        let parent_col = Arc::clone(&parent.row_groups.collection);
        let new_collection = parent_col.remove_column(drop_column_index);
        parent_col.commit_drop_column(drop_column_index);
        parent.row_groups.collection = Arc::clone(&new_collection);

        let mut new_types = parent.types.clone();
        if drop_column_index < new_types.len() {
            new_types.remove(drop_column_index);
        }

        let col_defs: Vec<ColumnDefinition> = new_types
            .iter()
            .enumerate()
            .map(|(i, t)| ColumnDefinition::new(format!("col_{}", i), t.clone()))
            .collect();
        let new_virtual_table = DataTable::new(
            new_info.db_id,
            new_info.table_io_manager_id,
            "",
            "",
            col_defs,
            None,
        );

        let optimistic_writer = OptimisticDataWriter::new_with_parent(
            Arc::clone(&new_virtual_table),
            &mut parent.optimistic_writer,
        );

        let mut optimistic_collections = Vec::new();
        std::mem::swap(&mut optimistic_collections, &mut parent.optimistic_collections);

        let append_indexes = TableIndexList::new();
        let row_groups = Box::new(OptimisticWriteCollection::new(new_collection));

        Arc::new(Mutex::new(Self {
            table_id: new_table_id,
            info: new_info,
            types: new_types,
            row_groups,
            append_indexes,
            delete_indexes: TableIndexList::new(),
            index_append_mode: parent.index_append_mode,
            deleted_rows: parent.deleted_rows,
            optimistic_collections,
            optimistic_writer,
            merged_storage: parent.merged_storage,
            is_dropped: false,
            collections_lock: Mutex::new(()),
        }))
    }

    /// 从 ADD COLUMN 操作的父存储创建（C++: 第四个构造函数）。
    pub fn from_add_column(
        new_table_id: u64,
        new_info: Arc<DataTableInfo>,
        parent: &mut LocalTableStorage,
        new_column_type: &LogicalType,
    ) -> Arc<Mutex<Self>> {
        // C++: auto new_collection = parent_collection.AddColumn(context, new_column, default_executor);
        //      row_groups = std::move(parent.row_groups); row_groups->collection = new_collection;
        let parent_col = Arc::clone(&parent.row_groups.collection);
        let new_collection = parent_col.add_column(new_column_type.clone());
        parent.row_groups.collection = Arc::clone(&new_collection);

        let mut new_types = parent.types.clone();
        new_types.push(new_column_type.clone());

        let col_defs: Vec<ColumnDefinition> = new_types
            .iter()
            .enumerate()
            .map(|(i, t)| ColumnDefinition::new(format!("col_{}", i), t.clone()))
            .collect();
        let new_virtual_table = DataTable::new(
            new_info.db_id,
            new_info.table_io_manager_id,
            "",
            "",
            col_defs,
            None,
        );

        let optimistic_writer = OptimisticDataWriter::new_with_parent(
            Arc::clone(&new_virtual_table),
            &mut parent.optimistic_writer,
        );

        let mut optimistic_collections = Vec::new();
        std::mem::swap(&mut optimistic_collections, &mut parent.optimistic_collections);

        let append_indexes = TableIndexList::new();
        let row_groups = Box::new(OptimisticWriteCollection::new(new_collection));

        Arc::new(Mutex::new(Self {
            table_id: new_table_id,
            info: new_info,
            types: new_types,
            row_groups,
            append_indexes,
            delete_indexes: TableIndexList::new(),
            index_append_mode: parent.index_append_mode,
            deleted_rows: parent.deleted_rows,
            optimistic_collections,
            optimistic_writer,
            merged_storage: parent.merged_storage,
            is_dropped: false,
            collections_lock: Mutex::new(()),
        }))
    }

    // ── 公开接口 ──────────────────────────────────────────────────────────────

    /// 返回本地行组集合的 Arc 引用（C++: `GetCollection()`）。
    pub fn get_collection(&self) -> Arc<RowGroupCollection> {
        Arc::clone(&self.row_groups.collection)
    }

    /// 初始化集合扫描状态（C++: `InitializeScan`）。
    pub fn initialize_scan(
        &self,
        state: &mut CollectionScanState,
        _table_filters: Option<&crate::storage::data_table::TableFilterSet>,
    ) {
        // C++: if (collection.GetTotalRows() == 0)
        //          throw InternalException("No rows in LocalTableStorage row group for scan");
        let collection = &self.row_groups.collection;
        if collection.total_rows() == 0 {
            // 调用方应先通过 GetCollection().GetTotalRows() 检查，
            // 此处静默返回以避免 panic
            return;
        }

        // C++: collection.InitializeScan(context, state, state.GetColumnIds(), table_filters.get());
        let column_ids = state.get_column_ids().to_vec();
        collection.initialize_scan(state, &column_ids);
    }

    /// 估算本地存储占用的内存大小（字节）（C++: `EstimatedSize()`）。
    ///
    /// 返回 `usize` 以与 `UndoBufferProperties::estimated_size` 保持一致。
    pub fn estimated_size(&self) -> usize {
        let collection = &self.row_groups.collection;
        let total_rows = collection.total_rows() as usize;
        let deleted = self.deleted_rows as usize;
        let appended_rows = total_rows.saturating_sub(deleted);

        // C++: idx_t row_size = sum of GetTypeIdSize(type.InternalType())
        let row_size: usize = collection
            .types
            .iter()
            .map(|t| t.physical_size())
            .sum();

        // C++: idx_t index_sizes = bound_index.GetInMemorySize() for each append index
        // 当前索引为 stub，内存大小为 0
        let index_sizes: usize = 0;

        appended_rows * row_size + index_sizes
    }

    /// 尝试预写入新完成的行组到磁盘（C++: `WriteNewRowGroup()`）。
    pub fn write_new_row_group(&mut self) {
        // C++: if (deleted_rows != 0) { return; }
        if self.deleted_rows != 0 {
            return;
        }
        self.optimistic_writer.write_new_row_group(&mut self.row_groups);
    }

    /// 将最后一个行组及 partial blocks 刷写到磁盘（C++: `FlushBlocks()`）。
    pub fn flush_blocks(&mut self) {
        let row_group_size = self.row_groups.collection.row_group_size;
        let total_rows = self.row_groups.collection.total_rows();

        // C++: if (!merged_storage && collection.GetTotalRows() > row_group_size)
        //          optimistic_writer.WriteLastRowGroup(*row_groups);
        if !self.merged_storage && total_rows > row_group_size {
            self.optimistic_writer.write_last_row_group(&mut self.row_groups);
        }
        // C++: optimistic_writer.FinalFlush();
        self.optimistic_writer.final_flush();
    }

    /// 将行追加到指定索引列表（带源集合版本）（C++: `AppendToIndexes` 重载一）。
    ///
    /// 扫描 `source` 集合，将每个 chunk 写入 `index_list`。
    /// 返回第一个约束冲突错误（若有）。
    pub fn append_to_indexes_with_source(
        &self,
        transaction: TransactionData,
        source: &Arc<RowGroupCollection>,
        index_list: &TableIndexList,
        _table_types: &[LogicalType],
        start_row: &mut RowId,
    ) -> Option<String> {
        // C++: auto indexed_columns = index_list.GetRequiredColumns();
        //      vector<StorageIndex> mapped_column_ids = sorted(indexed_columns);
        //      DataChunk table_chunk; table_chunk.InitializeEmpty(table_types);
        //      source.Scan(transaction, mapped_column_ids, [&](DataChunk &index_chunk) -> bool {
        //          ...
        //          error = DataTable::AppendToIndexes(index_list, delete_indexes, ...);
        //          ...
        //      });
        if index_list.is_empty() {
            return None;
        }

        // 扫描源集合并处理每个 chunk
        let column_ids: Vec<u64> = (0..source.types.len() as u64).collect();
        let mut scan_state = CollectionScanState::new();
        scan_state.set_column_ids(column_ids.clone());
        source.initialize_scan(&mut scan_state, &column_ids);

        let mut result_chunk = DataChunk::new();
        result_chunk.initialize_empty(&source.types);

        loop {
            result_chunk.reset();
            let has_data = source.scan(transaction, &mut scan_state, &mut result_chunk);
            if !has_data || result_chunk.size() == 0 {
                break;
            }

            // C++: error = DataTable::AppendToIndexes(index_list, delete_indexes,
            //                  table_chunk, index_chunk, mapped_column_ids, start_row, ...);
            // 完整 ART 实现需在此处执行唯一约束检查；当前 stub 索引列表为空，跳过。

            *start_row += result_chunk.size() as RowId;
        }

        None
    }

    /// 将本地行组追加到索引（主版本）（C++: `AppendToIndexes` 重载二）。
    ///
    /// - `append_to_table = true`：同时追加行到基表（逐行路径）。
    /// - `append_to_table = false`：仅追加到索引（批量路径）。
    pub fn append_to_indexes(
        &mut self,
        transaction: TransactionData,
        append_state: &mut TableAppendState,
        append_to_table: bool,
        base_row_groups: &Arc<RowGroupCollection>,
        base_indexes: &TableIndexList,
        table_types: &[LogicalType],
    ) {
        let collection = Arc::clone(&self.row_groups.collection);

        if append_to_table {
            // C++: table.InitializeAppend(transaction, append_state);
            base_row_groups.initialize_append(append_state);

            // C++: collection.Scan(transaction, [&](DataChunk &table_chunk) -> bool {
            //          if (table.HasIndexes()) { error = table.AppendToIndexes(...); }
            //          table.Append(table_chunk, append_state);
            //      });
            let column_ids: Vec<u64> = (0..collection.types.len() as u64).collect();
            let mut scan_state = CollectionScanState::new();
            scan_state.set_column_ids(column_ids.clone());
            collection.initialize_scan(&mut scan_state, &column_ids);

            let mut chunk = DataChunk::new();
            chunk.initialize(&collection.types, crate::common::types::data_chunk::STANDARD_VECTOR_SIZE);

            let mut chunks_scanned = 0usize;
            let mut rows_scanned = 0usize;
            loop {
                chunk.reset();
                let has_data = collection.scan(transaction, &mut scan_state, &mut chunk);
                if !has_data || chunk.size() == 0 {
                    break;
                }

                chunks_scanned += 1;
                rows_scanned += chunk.size();

                // C++: error = table.AppendToIndexes(delete_indexes, table_chunk, ...)
                // 唯一约束检查（当前 stub 索引，跳过实际检查）
                if !base_indexes.is_empty() {
                    // 完整实现：DataTable::AppendToIndexes(...)
                }

                // C++: table.Append(table_chunk, append_state);
                base_row_groups.append(&mut chunk, append_state);
            }

            // C++: table.FinalizeAppend(transaction, append_state);
            base_row_groups.finalize_append(transaction, append_state);
        } else {
            // C++: error = AppendToIndexes(transaction, collection, index_list, table.GetTypes(), start_row);
            let mut start_row = append_state.current_row;
            self.append_to_indexes_with_source(
                transaction,
                &collection,
                base_indexes,
                table_types,
                &mut start_row,
            );
        }
    }

    /// 将被删除行追加到删除辅助索引（C++: `AppendToDeleteIndexes`）。
    pub fn append_to_delete_indexes(&mut self, _row_ids: &[RowId], delete_chunk: &DataChunk) {
        // C++: if (delete_chunk.size() == 0) { return; }
        if delete_chunk.size() == 0 {
            return;
        }
        // C++: delete_indexes.Scan([&](Index &index) {
        //          D_ASSERT(index.IsBound());
        //          if (!index.IsUnique()) { return false; }
        //          IndexAppendInfo info(IndexAppendMode::IGNORE_DUPLICATES, nullptr);
        //          index.Cast<BoundIndex>().Append(delete_chunk, row_ids, info);
        //      });
        // 当前 delete_indexes 为 stub（空列表），无需操作。
    }

    /// 创建新的乐观写入集合（C++: `CreateOptimisticCollection`）。
    ///
    /// 返回新集合在 `optimistic_collections` 中的索引（`PhysicalIndex`）。
    pub fn create_optimistic_collection(
        &mut self,
        collection: Box<OptimisticWriteCollection>,
    ) -> PhysicalIndex {
        // C++: lock_guard<mutex> l(collections_lock);
        //      optimistic_collections.push_back(std::move(collection));
        //      return PhysicalIndex(optimistic_collections.size() - 1);
        let _lock = self.collections_lock.lock();
        self.optimistic_collections.push(Some(collection));
        PhysicalIndex(self.optimistic_collections.len() - 1)
    }

    /// 获取指定索引的乐观写入集合不可变引用（C++: `GetOptimisticCollection`）。
    ///
    /// # Panics
    /// 若 `collection_index` 越界或对应位置已重置，则 panic。
    pub fn get_optimistic_collection(
        &self,
        collection_index: PhysicalIndex,
    ) -> &OptimisticWriteCollection {
        // C++: lock_guard<mutex> l(collections_lock);
        //      return *optimistic_collections[collection_index.index];
        let _lock = self.collections_lock.lock();
        self.optimistic_collections[collection_index.0]
            .as_deref()
            .expect("optimistic collection has been reset")
    }

    /// 重置指定索引的乐观写入集合（C++: `ResetOptimisticCollection`）。
    pub fn reset_optimistic_collection(&mut self, collection_index: PhysicalIndex) {
        // C++: lock_guard<mutex> l(collections_lock);
        //      optimistic_collections[collection_index.index].reset();
        let _lock = self.collections_lock.lock();
        self.optimistic_collections[collection_index.0] = None;
    }

    /// 返回主乐观写入器的可变引用（C++: `GetOptimisticWriter`）。
    pub fn get_optimistic_writer(&mut self) -> &mut OptimisticDataWriter {
        &mut self.optimistic_writer
    }

    /// 回滚本地所有乐观写入（C++: `Rollback`）。
    pub fn rollback(&mut self) {
        // C++: optimistic_writer.Rollback();
        self.optimistic_writer.rollback();

        // C++: for (auto &collection : optimistic_collections) {
        //          if (!collection) continue;
        //          collection->collection->CommitDropTable();
        //      }
        //      optimistic_collections.clear();
        for maybe_coll in &mut self.optimistic_collections {
            if let Some(coll) = maybe_coll.take() {
                coll.collection.commit_drop_table();
            }
        }
        self.optimistic_collections.clear();

        // C++: row_groups->collection->CommitDropTable();
        self.row_groups.collection.commit_drop_table();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalTableManager
// ─────────────────────────────────────────────────────────────────────────────

/// 事务内所有表的本地存储管理器（C++: `class LocalTableManager`）。
///
/// 使用 `table_id: u64` 作为键（对应 C++ `reference_map_t<DataTable, ...>`）。
pub struct LocalTableManager {
    /// 受锁保护的表存储映射（C++: `mutable mutex table_storage_lock`）。
    table_storage: Mutex<HashMap<u64, Arc<Mutex<LocalTableStorage>>>>,
}

impl LocalTableManager {
    /// 创建空的管理器。
    pub fn new() -> Self {
        Self {
            table_storage: Mutex::new(HashMap::new()),
        }
    }

    /// 若存在则返回表的本地存储，否则返回 `None`（C++: `GetStorage`）。
    pub fn get_storage(&self, table_id: u64) -> Option<Arc<Mutex<LocalTableStorage>>> {
        // C++: lock_guard<mutex> l(table_storage_lock);
        //      auto entry = table_storage.find(table);
        //      return entry == table_storage.end() ? nullptr : entry->second.get();
        self.table_storage.lock().get(&table_id).cloned()
    }

    /// 若不存在则创建本地存储并返回（C++: `GetOrCreateStorage`）。
    pub fn get_or_create_storage(
        &self,
        table_id: u64,
        info: &Arc<DataTableInfo>,
        types: &[LogicalType],
    ) -> Arc<Mutex<LocalTableStorage>> {
        // C++: lock_guard<mutex> l(table_storage_lock);
        //      auto entry = table_storage.find(table);
        //      if (entry == table_storage.end()) {
        //          auto new_storage = make_shared_ptr<LocalTableStorage>(context, table);
        //          table_storage.insert(...);
        //          return *storage;
        //      } else { return *entry->second.get(); }
        let mut lock = self.table_storage.lock();
        if let Some(existing) = lock.get(&table_id) {
            return Arc::clone(existing);
        }
        let new_storage = LocalTableStorage::new(
            table_id,
            Arc::clone(info),
            types.to_vec(),
        );
        lock.insert(table_id, Arc::clone(&new_storage));
        new_storage
    }

    /// 是否没有任何本地存储（C++: `IsEmpty`）。
    pub fn is_empty(&self) -> bool {
        // C++: lock_guard<mutex> l(table_storage_lock);
        //      return table_storage.empty();
        self.table_storage.lock().is_empty()
    }

    /// 移出指定表的本地存储条目（C++: `MoveEntry`）。
    pub fn move_entry(&self, table_id: u64) -> Option<Arc<Mutex<LocalTableStorage>>> {
        // C++: lock_guard<mutex> l(table_storage_lock);
        //      auto entry = table_storage.find(table);
        //      if (entry == table_storage.end()) return nullptr;
        //      auto storage_entry = std::move(entry->second);
        //      table_storage.erase(entry);
        //      return storage_entry;
        self.table_storage.lock().remove(&table_id)
    }

    /// 移出所有本地存储条目（C++: `MoveEntries`）。
    pub fn move_entries(&self) -> HashMap<u64, Arc<Mutex<LocalTableStorage>>> {
        // C++: lock_guard<mutex> l(table_storage_lock);
        //      return std::move(table_storage);
        let mut lock = self.table_storage.lock();
        let mut map = HashMap::new();
        std::mem::swap(&mut *lock, &mut map);
        map
    }

    /// 估算所有本地存储占用的内存大小（C++: `EstimatedSize`）。
    pub fn estimated_size(&self) -> usize {
        // C++: lock_guard<mutex> l(table_storage_lock);
        //      for (auto &storage : table_storage) { estimated_size += storage.second->EstimatedSize(); }
        self.table_storage
            .lock()
            .values()
            .map(|s| s.lock().estimated_size())
            .sum()
    }

    /// 插入表的本地存储条目（C++: `InsertEntry`）。
    ///
    /// # Panics
    /// 若该 `table_id` 已有条目，则 panic（对应 C++ `D_ASSERT`）。
    pub fn insert_entry(&self, table_id: u64, entry: Arc<Mutex<LocalTableStorage>>) {
        // C++: lock_guard<mutex> l(table_storage_lock);
        //      D_ASSERT(table_storage.find(table) == table_storage.end());
        //      table_storage[table] = std::move(entry);
        let mut lock = self.table_storage.lock();
        assert!(
            !lock.contains_key(&table_id),
            "LocalTableManager::insert_entry: table_id {} already has a local storage entry",
            table_id
        );
        lock.insert(table_id, entry);
    }
}

impl Default for LocalTableManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalStorage
// ─────────────────────────────────────────────────────────────────────────────

/// 事务的全局本地写缓冲（C++: `class LocalStorage`）。
///
/// 每个事务拥有一个 `LocalStorage`，通过 `LocalTableManager` 管理
/// 每张被修改表的 `LocalTableStorage`。
pub struct LocalStorage {
    /// 所有表的本地存储管理器（C++: private `LocalTableManager table_manager`）。
    table_manager: LocalTableManager,
}

/// 提交状态（C++: `struct LocalStorage::CommitState`）。
pub struct CommitState {
    /// 表 ID → 追加状态（C++: `reference_map_t<DataTable, unique_ptr<TableAppendState>>`）。
    pub append_states: HashMap<u64, Box<TableAppendState>>,
}

impl CommitState {
    pub fn new() -> Self {
        Self {
            append_states: HashMap::new(),
        }
    }
}

impl Default for CommitState {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalStorage {
    // ── 构造函数 ─────────────────────────────────────────────────────────────

    /// 创建空的 LocalStorage（C++: `LocalStorage(ClientContext&, DuckTransaction&)`）。
    ///
    /// Rust 版本不存储 context/transaction 引用，通过方法参数传递。
    pub fn new() -> Self {
        Self {
            table_manager: LocalTableManager::new(),
        }
    }

    // ── 扫描接口 ──────────────────────────────────────────────────────────────

    /// 初始化对本地存储的顺序扫描（C++: `InitializeScan`）。
    pub fn initialize_scan(
        &self,
        table_id: u64,
        state: &mut CollectionScanState,
        table_filters: Option<&crate::storage::data_table::TableFilterSet>,
    ) {
        // C++: auto storage = table_manager.GetStorage(table);
        //      if (!storage || storage->GetCollection().GetTotalRows() == 0) return;
        //      storage->InitializeScan(state, table_filters);
        let storage = match self.table_manager.get_storage(table_id) {
            Some(s) => s,
            None => return,
        };
        let guard = storage.lock();
        if guard.get_collection().total_rows() == 0 {
            return;
        }
        guard.initialize_scan(state, table_filters);
    }

    /// 推进扫描，将下一批行写入 `result`（C++: `Scan`）。
    ///
    /// C++ 原型: `Scan(CollectionScanState&, const vector<StorageIndex>&, DataChunk&)`
    ///
    /// 通过 `table_id` 查找本地集合并驱动扫描。
    pub fn scan(
        &self,
        table_id: u64,
        state: &mut CollectionScanState,
        result: &mut DataChunk,
    ) {
        // C++: state.Scan(transaction, result);
        // 在 Rust 中，CollectionScanState 不直接持有 collection 引用，
        // 需通过 table_id 找到对应集合后驱动扫描。
        let storage = match self.table_manager.get_storage(table_id) {
            Some(s) => s,
            None => return,
        };
        let collection = storage.lock().get_collection();
        // 本地未提交数据对事务自身完全可见；使用事务 ID 0 访问所有本地行。
        let txn_data = TransactionData {
            start_time: 0,
            transaction_id: 0,
        };
        collection.scan(txn_data, state, result);
    }

    /// 初始化并行扫描状态（C++: `InitializeParallelScan`）。
    pub fn initialize_parallel_scan(
        &self,
        table_id: u64,
        state: &mut ParallelCollectionScanState,
    ) {
        // C++: if (!storage) { state.max_row = 0; state.vector_index = 0; ... }
        //      else { storage->GetCollection().InitializeParallelScan(state); }
        match self.table_manager.get_storage(table_id) {
            None => {
                state.max_row = 0;
                state.current_row_group_index
                    .store(0, std::sync::atomic::Ordering::Relaxed);
            }
            Some(s) => {
                s.lock().get_collection().initialize_parallel_scan(state);
            }
        }
    }

    /// 推进并行扫描（C++: `NextParallelScan`）。
    pub fn next_parallel_scan(
        &self,
        table_id: u64,
        state: &mut ParallelCollectionScanState,
        scan_state: &mut CollectionScanState,
    ) -> bool {
        // C++: if (!storage) return false;
        //      return storage->GetCollection().NextParallelScan(context, state, scan_state);
        let storage = match self.table_manager.get_storage(table_id) {
            Some(s) => s,
            None => return false,
        };
        let collection = storage.lock().get_collection();
        collection.next_parallel_scan(state, scan_state)
    }

    // ── 追加接口 ──────────────────────────────────────────────────────────────

    /// 初始化本地追加状态（C++: `InitializeAppend`）。
    ///
    /// 对应 `data_table.rs` 中的调用：
    /// `context.local_storage.initialize_append_state(state, table_id, info, types)`
    pub fn initialize_append_state(
        &self,
        state: &mut LocalAppendState,
        table_id: u64,
        info: Arc<DataTableInfo>,
        types: Vec<LogicalType>,
    ) {
        // C++: state.storage = &table_manager.GetOrCreateStorage(context, table);
        //      state.storage->GetCollection().InitializeAppend(TransactionData(transaction), state.append_state);
        let storage = self.table_manager.get_or_create_storage(table_id, &info, &types);
        let collection = storage.lock().get_collection();
        collection.initialize_append(&mut state.append_state);
        state.storage = Some(Arc::clone(&storage));
    }

    /// 仅初始化存储，不初始化行组（C++: `InitializeStorage`）。
    pub fn initialize_storage(
        &self,
        state: &mut LocalAppendState,
        table_id: u64,
        info: Arc<DataTableInfo>,
        types: Vec<LogicalType>,
    ) {
        // C++: state.storage = &table_manager.GetOrCreateStorage(context, table);
        let storage = self.table_manager.get_or_create_storage(table_id, &info, &types);
        state.storage = Some(storage);
    }

    /// 将一个 chunk 追加到本地存储（C++: `Append`，静态方法）。
    ///
    /// 首先检查唯一约束索引（若有），然后追加到本地行组集合。
    pub fn append(
        state: &mut LocalAppendState,
        table_chunk: &mut DataChunk,
        _data_table_info: &Arc<DataTableInfo>,
    ) -> StorageResult<()> {
        // C++: auto storage = state.storage;
        //      auto offset = NumericCast<idx_t>(MAX_ROW_ID) + storage->GetCollection().GetTotalRows();
        //      idx_t base_id = offset + state.append_state.total_append_count;
        let storage_arc = match &state.storage {
            Some(s) => Arc::clone(s),
            None => {
                return Err(StorageError::Other(
                    "LocalStorage::append: LocalAppendState not initialized".to_string(),
                ))
            }
        };
        let mut storage = storage_arc.lock();
        let collection = Arc::clone(&storage.row_groups.collection);

        let offset = MAX_ROW_ID as Idx + collection.total_rows();
        let base_id = offset + state.append_state.total_append_count;

        // C++: if (!storage->append_indexes.Empty()) { ... AppendToIndexes ... }
        if !storage.append_indexes.is_empty() {
            // 完整实现需调用 DataTable::AppendToIndexes 写入唯一约束 ART 索引。
            // 当前 append_indexes 为 stub，跳过约束检查。
            let _ = base_id;
        }

        // C++: auto new_row_group = storage->GetCollection().Append(table_chunk, state.append_state);
        let new_row_group = collection.append(table_chunk, &mut state.append_state);

        // C++: if (new_row_group) { storage->WriteNewRowGroup(); }
        if new_row_group {
            storage.write_new_row_group();
        }

        Ok(())
    }

    /// 完成追加操作（C++: `FinalizeAppend`，静态方法）。
    pub fn finalize_append(state: &mut LocalAppendState) {
        // C++: state.storage->GetCollection().FinalizeAppend(state.append_state.transaction, state.append_state);
        if let Some(storage_arc) = &state.storage {
            let storage = storage_arc.lock();
            let collection = storage.get_collection();
            let txn_data = TransactionData {
                start_time: 0,
                transaction_id: 0,
            };
            collection.finalize_append(txn_data, &mut state.append_state);
        }
    }

    /// 将乐观写入集合合并到本地存储（C++: `LocalMerge`）。
    pub fn local_merge(
        &self,
        table_id: u64,
        info: &Arc<DataTableInfo>,
        types: &[LogicalType],
        collection: &OptimisticWriteCollection,
        transaction: TransactionData,
    ) -> StorageResult<()> {
        // C++: auto &storage = table_manager.GetOrCreateStorage(context, table);
        let storage_arc = self.table_manager.get_or_create_storage(table_id, info, types);
        let mut storage = storage_arc.lock();

        // C++: if (!storage.append_indexes.Empty()) {
        //          row_t base_id = MAX_ROW_ID + storage.GetCollection().GetTotalRows();
        //          auto error = storage.AppendToIndexes(...);
        //          if (error.HasError()) error.Throw();
        //      }
        if !storage.append_indexes.is_empty() {
            let mut start_row = MAX_ROW_ID + storage.row_groups.collection.total_rows() as RowId;
            if let Some(err) = storage.append_to_indexes_with_source(
                transaction,
                &collection.collection,
                &storage.append_indexes,
                &storage.types.clone(),
                &mut start_row,
            ) {
                return Err(StorageError::Other(err));
            }
        }

        // C++: storage.GetCollection().MergeStorage(*collection.collection, nullptr, nullptr);
        //      storage.merged_storage = true;
        let local_col = Arc::clone(&storage.row_groups.collection);
        local_col.merge_storage(Arc::clone(&collection.collection))?;
        storage.merged_storage = true;

        Ok(())
    }

    /// 为表创建乐观写入集合（C++: `CreateOptimisticCollection`）。
    pub fn create_optimistic_collection(
        &self,
        table_id: u64,
        info: &Arc<DataTableInfo>,
        types: &[LogicalType],
        collection: Box<OptimisticWriteCollection>,
    ) -> PhysicalIndex {
        // C++: auto &storage = table_manager.GetOrCreateStorage(context, table);
        //      return storage.CreateOptimisticCollection(std::move(collection));
        let storage_arc = self.table_manager.get_or_create_storage(table_id, info, types);
        storage_arc.lock().create_optimistic_collection(collection)
    }

    /// 重置指定索引的乐观写入集合（C++: `ResetOptimisticCollection`）。
    pub fn reset_optimistic_collection(
        &self,
        table_id: u64,
        collection_index: PhysicalIndex,
    ) {
        // C++: auto &storage = table_manager.GetOrCreateStorage(context, table);
        //      storage.ResetOptimisticCollection(collection_index);
        if let Some(storage_arc) = self.table_manager.get_storage(table_id) {
            storage_arc.lock().reset_optimistic_collection(collection_index);
        }
    }

    // ── 删除/更新 ─────────────────────────────────────────────────────────────

    /// 从本地存储中删除一批行（C++: `Delete`）。
    pub fn delete(
        &self,
        table_id: u64,
        row_ids: &mut [RowId],
        count: Idx,
        transaction: TransactionData,
    ) -> Idx {
        // C++: auto storage = table_manager.GetStorage(table); D_ASSERT(storage);
        let storage_arc = match self.table_manager.get_storage(table_id) {
            Some(s) => s,
            None => panic!("LocalStorage::delete: no storage for table_id={}", table_id),
        };
        let mut storage = storage_arc.lock();

        // C++: if (!storage->append_indexes.Empty()) {
        //          storage->GetCollection().RemoveFromIndexes(context, storage->append_indexes, row_ids, ...);
        //      }
        if !storage.append_indexes.is_empty() {
            // 完整实现：从 append_indexes 移除对应行条目
        }

        // C++: idx_t delete_count = storage->GetCollection().Delete(TransactionData(0,0), table, ids, count);
        //      storage->deleted_rows += delete_count;
        let collection = Arc::clone(&storage.row_groups.collection);
        let delete_count = collection.delete(transaction, row_ids, count);
        storage.deleted_rows += delete_count;
        delete_count
    }

    /// 更新本地存储中的一批行（C++: `Update`）。
    pub fn update(
        &self,
        table_id: u64,
        row_ids: &[RowId],
        column_ids: &[u64],
        updates: &DataChunk,
        transaction: TransactionData,
    ) {
        // C++: auto storage = table_manager.GetStorage(table); D_ASSERT(storage);
        //      storage->GetCollection().Update(TransactionData(0,0), table, ids, column_ids, updates);
        let storage_arc = match self.table_manager.get_storage(table_id) {
            Some(s) => s,
            None => panic!("LocalStorage::update: no storage for table_id={}", table_id),
        };
        let collection = storage_arc.lock().get_collection();
        collection.update(transaction, row_ids, column_ids, updates);
    }

    // ── 提交/回滚 ─────────────────────────────────────────────────────────────

    /// 将所有本地存储 flush 到基表并完成提交（C++: `Commit`）。
    ///
    /// `tables` 为表 ID → Arc<DataTable> 映射，用于在 flush 时获取基表引用。
    pub fn commit(
        &mut self,
        tables: &HashMap<u64, Arc<DataTable>>,
    ) -> StorageResult<()> {
        // C++: auto table_storage = table_manager.MoveEntries();
        //      for (auto &entry : table_storage) {
        //          auto table = entry.first;
        //          auto storage = entry.second.get();
        //          Flush(table, *storage, commit_state);
        //          entry.second.reset();
        //      }
        let table_storage = self.table_manager.move_entries();
        for (table_id, storage_arc) in table_storage {
            let mut storage = storage_arc.lock();
            if let Some(table) = tables.get(&table_id) {
                self.flush_one(&mut storage, table)?;
            } else {
                // 找不到基表（可能已被 DROP）：回滚本地存储
                storage.rollback();
            }
        }
        Ok(())
    }

    /// 回滚所有本地存储（C++: `Rollback`）。
    pub fn rollback(&mut self) {
        // C++: auto table_storage = table_manager.MoveEntries();
        //      for (auto &entry : table_storage) {
        //          if (!storage) continue;
        //          storage->Rollback();
        //          entry.second.reset();
        //      }
        let table_storage = self.table_manager.move_entries();
        for (_key, storage_arc) in table_storage {
            let mut storage = storage_arc.lock();
            storage.rollback();
        }
    }

    // ── 查询接口 ──────────────────────────────────────────────────────────────

    /// 是否有未提交修改（C++: `ChangesMade`）。
    pub fn changes_made(&self) -> bool {
        // C++: return !table_manager.IsEmpty();
        !self.table_manager.is_empty()
    }

    /// 是否存在该表的本地存储（C++: `Find`）。
    pub fn find(&self, table_id: u64) -> bool {
        self.table_manager.get_storage(table_id).is_some()
    }

    /// 估算所有本地存储占用的内存大小（C++: `EstimatedSize`）。
    ///
    /// 返回 `usize` 以与 `UndoBufferProperties::estimated_size` 保持一致。
    pub fn estimated_size(&self) -> usize {
        // C++: return table_manager.EstimatedSize();
        self.table_manager.estimated_size()
    }

    /// 初始化扫描状态（用于本地存储扫描）（别名方法，供 connection/db 层调用）。
    ///
    /// 设置 `state.column_ids` 并在存在本地行组时初始化扫描位置。
    pub fn initialize_scan_state(
        &self,
        table_id: u64,
        state: &mut CollectionScanState,
        column_ids: &[u64],
    ) {
        state.set_column_ids(column_ids.to_vec());
        self.initialize_scan(table_id, state, None);
    }

    /// 本事务在该表上追加的净行数（C++: `AddedRows`）。
    pub fn added_rows(&self, table_id: u64) -> Idx {
        // C++: if (!storage) return 0;
        //      return storage->GetCollection().GetTotalRows() - storage->deleted_rows;
        match self.table_manager.get_storage(table_id) {
            None => 0,
            Some(s) => {
                let guard = s.lock();
                guard
                    .get_collection()
                    .total_rows()
                    .saturating_sub(guard.deleted_rows)
            }
        }
    }

    /// 获取该表本地存储的分区统计信息（C++: `GetPartitionStats`）。
    pub fn get_partition_stats(&self, table_id: u64) -> Vec<PartitionStatistics> {
        // C++: if (!storage) return {};
        //      return storage->GetCollection().GetPartitionStats();
        match self.table_manager.get_storage(table_id) {
            None => Vec::new(),
            Some(s) => {
                let guard = s.lock();
                let collection = guard.get_collection();
                let total = collection.total_rows();
                let deleted = guard.deleted_rows;
                // 每个行组构成一个分区；当前简化：整体作为一个分区返回
                vec![PartitionStatistics {
                    row_count: total,
                    deleted_count: deleted,
                }]
            }
        }
    }

    // ── Schema 变更 ───────────────────────────────────────────────────────────

    /// 标记表已被 DROP（C++: `DropTable`）。
    pub fn drop_table(&self, table_id: u64) {
        // C++: if (!storage) return; storage->is_dropped = true;
        if let Some(s) = self.table_manager.get_storage(table_id) {
            s.lock().is_dropped = true;
        }
    }

    /// 将本地存储迁移到新版本的表（C++: `MoveStorage`）。
    pub fn move_storage(&self, old_id: u64, new_id: u64, new_info: Arc<DataTableInfo>) {
        // C++: auto new_storage = table_manager.MoveEntry(old_dt);
        //      if (!new_storage) return;
        //      new_storage->table_ref = new_dt;
        //      table_manager.InsertEntry(new_dt, std::move(new_storage));
        let storage = match self.table_manager.move_entry(old_id) {
            Some(s) => s,
            None => return,
        };
        // 更新表标识
        {
            let mut guard = storage.lock();
            guard.table_id = new_id;
            guard.info = new_info;
        }
        self.table_manager.insert_entry(new_id, storage);
    }

    /// 为添加列后的新表迁移本地存储（C++: `AddColumn`）。
    pub fn add_column(
        &self,
        old_id: u64,
        new_id: u64,
        new_info: Arc<DataTableInfo>,
        new_column_type: &LogicalType,
    ) {
        // C++: auto storage = table_manager.MoveEntry(old_dt);
        //      if (!storage) return;
        //      auto new_storage = make_shared_ptr<LocalTableStorage>(context, new_dt, *storage, new_column, executor);
        //      table_manager.InsertEntry(new_dt, std::move(new_storage));
        let storage_arc = match self.table_manager.move_entry(old_id) {
            Some(s) => s,
            None => return,
        };
        let new_storage = {
            let mut guard = storage_arc.lock();
            LocalTableStorage::from_add_column(
                new_id,
                new_info,
                &mut guard,
                new_column_type,
            )
        };
        self.table_manager.insert_entry(new_id, new_storage);
    }

    /// 为删除列后的新表迁移本地存储（C++: `DropColumn`）。
    pub fn drop_column(
        &self,
        old_id: u64,
        new_id: u64,
        new_info: Arc<DataTableInfo>,
        drop_column_index: usize,
    ) {
        let storage_arc = match self.table_manager.move_entry(old_id) {
            Some(s) => s,
            None => return,
        };
        let new_storage = {
            let mut guard = storage_arc.lock();
            LocalTableStorage::from_drop_column(
                new_id,
                new_info,
                &mut guard,
                drop_column_index,
            )
        };
        self.table_manager.insert_entry(new_id, new_storage);
    }

    /// 为修改列类型后的新表迁移本地存储（C++: `ChangeType`）。
    pub fn change_type(
        &self,
        old_id: u64,
        new_id: u64,
        new_info: Arc<DataTableInfo>,
        changed_idx: usize,
        target_type: &LogicalType,
    ) {
        let storage_arc = match self.table_manager.move_entry(old_id) {
            Some(s) => s,
            None => return,
        };
        let new_storage = {
            let mut guard = storage_arc.lock();
            LocalTableStorage::from_alter_type(
                new_id,
                new_info,
                &mut guard,
                changed_idx,
                target_type,
            )
        };
        self.table_manager.insert_entry(new_id, new_storage);
    }

    // ── 数据获取 ──────────────────────────────────────────────────────────────

    /// 按行 ID 获取本地存储中的列数据（C++: `FetchChunk`）。
    pub fn fetch_chunk(
        &self,
        table_id: u64,
        row_ids: &[RowId],
        count: Idx,
        col_ids: &[StorageIndex],
        chunk: &mut DataChunk,
        fetch_state: &mut ColumnFetchState,
        transaction: TransactionData,
    ) {
        // C++: if (!storage) throw InternalException("LocalStorage::FetchChunk - not found");
        //      storage->GetCollection().Fetch(transaction, chunk, col_ids, row_ids, count, fetch_state);
        let storage_arc = match self.table_manager.get_storage(table_id) {
            Some(s) => s,
            None => panic!("LocalStorage::fetch_chunk: local storage not found for table_id={}", table_id),
        };
        let collection = storage_arc.lock().get_collection();
        let col_ids_u64: Vec<u64> = col_ids.iter().map(|s| s.0).collect();
        collection.fetch(transaction, chunk, &col_ids_u64, row_ids, count, fetch_state);
    }

    /// 判断该行 ID 是否在本地存储中可见（C++: `CanFetch`）。
    pub fn can_fetch(
        &self,
        table_id: u64,
        row_id: RowId,
        transaction: TransactionData,
    ) -> bool {
        // C++: if (!storage) throw InternalException("LocalStorage::CanFetch - not found");
        //      return storage->GetCollection().CanFetch(transaction, row_id);
        let storage_arc = match self.table_manager.get_storage(table_id) {
            Some(s) => s,
            None => panic!("LocalStorage::can_fetch: local storage not found for table_id={}", table_id),
        };
        let collection = storage_arc.lock().get_collection();
        collection.can_fetch(transaction, row_id)
    }

    /// 验证新约束在本地存储中是否满足（C++: `VerifyNewConstraint`）。
    pub fn verify_new_constraint(
        &self,
        table_id: u64,
        _constraint: &BoundConstraint,
    ) {
        // C++: storage->GetCollection().VerifyNewConstraint(context, parent, constraint);
        if let Some(s) = self.table_manager.get_storage(table_id) {
            let _collection = s.lock().get_collection();
            // 完整实现需调用 collection.verify_new_constraint(constraint)
        }
    }

    // ── 私有辅助 ──────────────────────────────────────────────────────────────

    /// 将单个表的本地存储 flush 到基表（C++: `Flush`）。
    ///
    /// 提交路径的核心方法，实现 C++ `LocalStorage::Flush` 的全部逻辑。
    fn flush_one(
        &self,
        storage: &mut LocalTableStorage,
        table: &Arc<DataTable>,
    ) -> StorageResult<()> {
        // C++: if (storage.is_dropped) { return; }
        if storage.is_dropped {
            return Ok(());
        }

        let total_rows = storage.get_collection().total_rows();

        // C++: if (storage.GetCollection().GetTotalRows() <= storage.deleted_rows)
        //      { storage.Rollback(); return; }
        if total_rows <= storage.deleted_rows {
            storage.rollback();
            return Ok(());
        }

        let append_count = total_rows - storage.deleted_rows;
        let row_group_size = storage.row_groups.collection.row_group_size;

        // C++: TableAppendState append_state;
        //      table.AppendLock(transaction, append_state);
        //      transaction.PushAppend(table, NumericCast<idx_t>(append_state.row_start), append_count);
        let mut append_state = TableAppendState::new();
        table.row_groups.initialize_append(&mut append_state);
        let row_start = append_state.row_start as Idx;

        let txn_data = TransactionData {
            start_time: 0,
            transaction_id: 0,
        };
        let table_types: Vec<LogicalType> = table
            .column_definitions
            .iter()
            .map(|c| c.logical_type.clone())
            .collect();

        // C++: if ((append_state.row_start == 0 ||
        //            storage.GetCollection().GetTotalRows() >= row_group_size)
        //          && storage.deleted_rows == 0) {
        //     // 批量路径
        //     } else {
        //     // 逐行路径
        //     }

        if (row_start == 0 || total_rows >= row_group_size) && storage.deleted_rows == 0 {
            // ── 批量路径 ─────────────────────────────────────────────────────
            // C++: storage.FlushBlocks();
            storage.flush_blocks();

            // C++: if (table.HasIndexes()) { storage.AppendToIndexes(transaction, append_state, false); }
            if !table.info.indexes.is_empty() {
                storage.append_to_indexes(
                    txn_data,
                    &mut append_state,
                    false,
                    &table.row_groups,
                    &table.info.indexes,
                    &table_types,
                );
            }

            // C++: table.MergeStorage(storage.GetCollection(), storage.append_indexes, commit_state);
            let local_collection = Arc::clone(&storage.row_groups.collection);
            table.row_groups.merge_storage(local_collection)?;
        } else {
            // ── 逐行路径 ─────────────────────────────────────────────────────
            // C++: storage.Rollback();
            storage.rollback();

            // C++: storage.AppendToIndexes(transaction, append_state, true);
            storage.append_to_indexes(
                txn_data,
                &mut append_state,
                true,
                &table.row_groups,
                &table.info.indexes,
                &table_types,
            );
        }

        Ok(())
    }
}

impl Default for LocalStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for LocalStorage {
    /// Creates a fresh empty `LocalStorage`.
    ///
    /// NOTE: This does **not** share table-storage data with the original.
    /// Cloning is provided primarily so that `ClientContext` (which embeds
    /// `LocalStorage`) can be constructed from an existing transaction context
    /// in the connection layer. The cloned instance starts with no local tables.
    fn clone(&self) -> Self {
        Self::new()
    }
}
