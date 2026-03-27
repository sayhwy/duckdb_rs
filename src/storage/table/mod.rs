//! Table storage layer — column data, row groups, version management.
//!
//! Mirrors `src/storage/table/` in the DuckDB C++ source.
//!
//! # Module dependency order (bottom → top)
//!
//! ```text
//! types                         (primitive aliases, LogicalType placeholder)
//! segment_base                  (SegmentBase trait)
//! segment_lock                  (SegmentLock RAII guard)
//! segment_tree                  (SegmentNode, SegmentTree)
//! chunk_info                    (ChunkInfo, ChunkConstantInfo, ChunkVectorInfo)
//! column_segment                (ColumnSegment, ColumnSegmentType)
//! column_segment_tree           (type alias: ColumnSegmentTree = SegmentTree<ColumnSegment>)
//! update_segment                (UpdateSegment, UpdateNode)
//! column_data                   (ColumnDataContext, ColumnDataKind, Persistent* structs)
//!   ├── standard_column_data
//!   ├── validity_column_data
//!   ├── list_column_data
//!   ├── struct_column_data
//!   ├── array_column_data
//!   ├── row_id_column_data      (虚列，动态生成 row_id)
//!   └── variant_column_data     (JSON/Variant 半结构化列)
//! column_checkpoint_state       (ColumnCheckpointState, PartialBlock trait)
//! column_data_checkpointer      (ColumnDataCheckpointer, CheckpointAnalyzeResult)
//! persistent_table_data         (PersistentTableData, PersistentColumnData)
//! table_index_list              (TableIndexList, IndexEntry, IndexBindState)
//! data_table_info               (DataTableInfo)
//! row_version_manager           (RowVersionManager)
//! table_statistics              (TableStatistics, TableStatisticsLock)
//! delete_state                  (TableDeleteState)
//! update_state                  (TableUpdateState)
//! scan_state                    (ColumnScanState, CollectionScanState, TableScanState, …)
//! append_state                  (ColumnAppendState, RowGroupAppendState, TableAppendState, …)
//! row_group_segment_tree        (RowGroupSegmentTree，带懒加载)
//! row_group_reorderer           (RowGroupReorderer，ORDER BY 优化)
//! row_group                     (RowGroup, RowGroupWriteInfo, RowGroupWriteData)
//! row_group_collection          (RowGroupCollection)
//! in_memory_checkpoint          (InMemoryCheckpointer 及相关写入器)
//! variant/                      (variant_shredding, variant_unshredding)
//! ```

// ── 基础层 ────────────────────────────────────────────────────────────────────
pub mod types;
pub mod segment_base;
pub mod segment_lock;
pub mod segment_tree;

// ── 列段层 ────────────────────────────────────────────────────────────────────
pub mod chunk_info;
pub mod column_segment;
pub mod column_segment_tree;
pub mod update_segment;

// ── 列数据层 ──────────────────────────────────────────────────────────────────
pub mod column_data;
pub mod standard_column_data;
pub mod validity_column_data;
pub mod list_column_data;
pub mod struct_column_data;
pub mod array_column_data;
pub mod row_id_column_data;
pub mod variant_column_data;

// ── Checkpoint 层 ─────────────────────────────────────────────────────────────
pub mod column_checkpoint_state;
pub mod column_data_checkpointer;
pub mod persistent_table_data;

// ── 索引 / 元数据层 ───────────────────────────────────────────────────────────
pub mod table_index_list;
pub mod data_table_info;

// ── MVCC 层 ───────────────────────────────────────────────────────────────────
pub mod row_version_manager;
pub mod table_statistics;

// ── 操作状态层 ────────────────────────────────────────────────────────────────
pub mod delete_state;
pub mod update_state;
pub mod scan_state;
pub mod append_state;

// ── RowGroup 层 ───────────────────────────────────────────────────────────────
pub mod row_group_segment_tree;
pub mod row_group_reorderer;
pub mod row_group;
pub mod row_group_collection;

// ── Checkpoint 高层 ───────────────────────────────────────────────────────────
pub mod in_memory_checkpoint;

// ── Variant 子模块 ────────────────────────────────────────────────────────────
mod variant;
