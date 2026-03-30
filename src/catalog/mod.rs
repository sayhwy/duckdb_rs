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

pub mod catalog;
mod catalog_entry;
pub mod catalog_set;
pub mod column_dependency;
pub mod default_generator;
pub mod dependency;
pub mod dependency_manager;
pub mod entry;
pub mod entry_lookup;
pub mod error;
pub mod schema_entry;
pub mod search_path;
mod standard_entry;
pub mod table_catalog_entry;
pub mod transaction;
pub mod types;
// ─── 常用类型重新导出 ──────────────────────────────────────────────────────────

pub use types::{
    AlterInfo, AlterKind, CatalogLookupBehavior, CatalogType, ColumnDefinition, ColumnList,
    ConstraintType, CreateCollationInfo, CreateCopyFunctionInfo, CreateFunctionInfo,
    CreateIndexInfo, CreateInfo, CreatePragmaFunctionInfo, CreateSchemaInfo, CreateSequenceInfo,
    CreateTableInfo, CreateTypeInfo, CreateViewInfo, DatabaseSize, DropInfo, IndexConstraintType,
    LogicalType, LogicalTypeId, MetadataBlockInfo, OnCreateConflict, OnEntryNotFound, Value,
};

pub use error::CatalogError;

pub use dependency::{
    CatalogEntryInfo, DependencyDependent, DependencyDependentFlags, DependencyInfo,
    DependencySubject, DependencySubjectFlags, LogicalDependency, LogicalDependencyList,
    MangledDependencyName, MangledEntryName,
};

pub use transaction::{CatalogTransaction, is_committed, is_visible};

pub use entry_lookup::{CatalogEntryLookup, EntryLookupInfo, SimilarCatalogEntry};

pub use entry::{
    CatalogEntryBase, CatalogEntryKind, CatalogEntryNode, CatalogEntryRef, DependencyRelationData,
    FunctionEntryData, IndexEntryData, SequenceEntryData, TableEntryData, TypeEntryData,
    ViewEntryData, new_entry_ref,
};

pub use catalog_set::{CatalogSet, EntryLookupResult, LookupFailureReason};

pub use default_generator::{DefaultGenerator, DefaultSchemaGenerator};

pub use search_path::{CatalogSearchEntry, CatalogSearchPath, CatalogSetPathType};

pub use column_dependency::ColumnDependencyManager;

pub use dependency_manager::DependencyManager;

pub use schema_entry::{DuckSchemaEntry, SchemaCatalogEntry};

pub use catalog::{Catalog, DuckCatalog};

pub use table_catalog_entry::{
    COLUMN_IDENTIFIER_ROW_ID, ColumnSegmentInfo, ColumnStatistics, IndexInfo, LogicalIndex,
    PhysicalIndex, ScanFunctionBinding, TableCatalogEntry, TableCatalogEntryVirtual,
    TableStorageInfo, VirtualColumn, VirtualColumnMap,
};
