//! DuckDB Catalog 子系统（Rust 实现）。
//!
//! 对应 C++: `src/catalog/` 及 `src/include/duckdb/catalog/`
//!
//! # 模块层次（由底向上）
//!
//! ```text
//! types.rs              ← 基础类型（CatalogType, Value, LogicalType, CreateInfo …）
//! error.rs              ← CatalogError
//! dependency.rs         ← 依赖关系类型（LogicalDependency, MangledEntryName …）
//! transaction.rs        ← CatalogTransaction + MVCC 可见性辅助函数
//! entry_lookup.rs       ← EntryLookupInfo, SimilarCatalogEntry
//! entry.rs              ← CatalogEntryBase, CatalogEntryKind, CatalogEntryNode
//! catalog_set.rs        ← CatalogEntryMap, CatalogSet（MVCC 核心）
//! default_generator.rs  ← DefaultGenerator trait, DefaultSchemaGenerator
//! search_path.rs        ← CatalogSearchEntry, CatalogSearchPath
//! column_dependency.rs  ← ColumnDependencyManager（表内生成列依赖）
//! dependency_manager.rs ← DependencyManager, DependencyCatalogSet
//! schema_entry.rs       ← SchemaCatalogEntry trait, DuckSchemaEntry
//! catalog.rs            ← Catalog trait, DuckCatalog
//! ```

pub mod types;
pub mod error;
pub mod dependency;
pub mod transaction;
pub mod entry_lookup;
pub mod entry;
pub mod catalog_set;
pub mod default_generator;
pub mod search_path;
pub mod column_dependency;
pub mod dependency_manager;
pub mod schema_entry;
pub mod catalog;
pub mod table_catalog_entry;
mod catalog_entry;
mod standard_entry;
// ─── 常用类型重新导出 ──────────────────────────────────────────────────────────

pub use types::{
    CatalogType, OnEntryNotFound, OnCreateConflict, CatalogLookupBehavior,
    IndexConstraintType, Value, LogicalTypeId, LogicalType,
    ColumnDefinition, ColumnList, ConstraintType,
    CreateInfo, CreateSchemaInfo, CreateTableInfo, CreateViewInfo,
    CreateSequenceInfo, CreateTypeInfo, CreateIndexInfo,
    CreateFunctionInfo, CreateCopyFunctionInfo, CreatePragmaFunctionInfo,
    CreateCollationInfo,
    AlterInfo, AlterKind, DropInfo,
    DatabaseSize, MetadataBlockInfo,
};

pub use error::CatalogError;

pub use dependency::{
    CatalogEntryInfo, LogicalDependency, LogicalDependencyList,
    DependencySubjectFlags, DependencyDependentFlags,
    DependencySubject, DependencyDependent, DependencyInfo,
    MangledEntryName, MangledDependencyName,
};

pub use transaction::{CatalogTransaction, is_visible, is_committed};

pub use entry_lookup::{EntryLookupInfo, CatalogEntryLookup, SimilarCatalogEntry};

pub use entry::{
    CatalogEntryBase, CatalogEntryKind, CatalogEntryNode, CatalogEntryRef,
    TableEntryData, ViewEntryData, SequenceEntryData,
    IndexEntryData, TypeEntryData, FunctionEntryData,
    DependencyRelationData,
    new_entry_ref,
};

pub use catalog_set::{CatalogSet, EntryLookupResult, LookupFailureReason};

pub use default_generator::{DefaultGenerator, DefaultSchemaGenerator};

pub use search_path::{CatalogSearchEntry, CatalogSearchPath, CatalogSetPathType};

pub use column_dependency::ColumnDependencyManager;

pub use dependency_manager::DependencyManager;

pub use schema_entry::{SchemaCatalogEntry, DuckSchemaEntry};

pub use catalog::{Catalog, DuckCatalog};

pub use table_catalog_entry::{
    TableCatalogEntry, TableCatalogEntryVirtual,
    LogicalIndex, PhysicalIndex,
    ColumnSegmentInfo, TableStorageInfo, IndexInfo,
    VirtualColumn, VirtualColumnMap,
    ColumnStatistics, ScanFunctionBinding,
    COLUMN_IDENTIFIER_ROW_ID,
};
