//! Schema Catalog 条目。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog_entry/schema_catalog_entry.hpp`（`SchemaCatalogEntry` 抽象类）
//!   - `duckdb/catalog/catalog_entry/duck_schema_entry.hpp`（`DuckSchemaEntry`）
//!
//! # 设计说明
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class SchemaCatalogEntry : public InCatalogEntry` | `trait SchemaCatalogEntry` |
//! | `class DuckSchemaEntry : public SchemaCatalogEntry` | `struct DuckSchemaEntry` |
//! | 九个 `CatalogSet` 成员 | 九个 `CatalogSet` 字段 |

use parking_lot::Mutex;
use std::sync::Arc;

use super::catalog_set::{CatalogSet, LookupFailureReason};
use super::dependency::LogicalDependencyList;
use super::entry::{
    CatalogEntryBase, CatalogEntryKind, CatalogEntryNode, FunctionEntryData, IndexEntryData,
    SequenceEntryData, TableEntryData, TypeEntryData, ViewEntryData,
};
use super::entry_lookup::{EntryLookupInfo, SimilarCatalogEntry};
use super::error::CatalogError;
use super::transaction::CatalogTransaction;
use super::types::{
    AlterInfo, CatalogType, CreateCollationInfo, CreateCopyFunctionInfo, CreateFunctionInfo,
    CreateIndexInfo, CreatePragmaFunctionInfo, CreateSequenceInfo, CreateTableInfo, CreateTypeInfo,
    CreateViewInfo, DropInfo, OnCreateConflict, OnEntryNotFound,
};
use crate::common::errors::CatalogResult;

// ─── SchemaCatalogEntry trait ─────────────────────────────────────────────────

/// Schema 条目接口（C++: `class SchemaCatalogEntry`）。
pub trait SchemaCatalogEntry: Send + Sync {
    // ── 元数据 ────────────────────────────────────────────────────────────────
    fn name(&self) -> &str;
    fn catalog_name(&self) -> &str;
    fn is_internal(&self) -> bool;
    fn to_sql(&self) -> String;

    // ── 创建操作 ──────────────────────────────────────────────────────────────
    fn create_table(
        &self,
        txn: &CatalogTransaction,
        info: &CreateTableInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_view(
        &self,
        txn: &CatalogTransaction,
        info: &CreateViewInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_sequence(
        &self,
        txn: &CatalogTransaction,
        info: &CreateSequenceInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_type(
        &self,
        txn: &CatalogTransaction,
        info: &CreateTypeInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_index(
        &self,
        txn: &CatalogTransaction,
        info: &CreateIndexInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_table_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_copy_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCopyFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_pragma_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreatePragmaFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode>;
    fn create_collation(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCollationInfo,
    ) -> CatalogResult<CatalogEntryNode>;

    // ── 查找操作 ──────────────────────────────────────────────────────────────
    fn lookup_entry(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
    ) -> Option<CatalogEntryNode>;
    fn get_similar_entry(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
    ) -> SimilarCatalogEntry;

    // ── 修改与删除 ────────────────────────────────────────────────────────────
    fn alter(&self, txn: &CatalogTransaction, info: &AlterInfo) -> CatalogResult<()>;
    fn drop_entry(&self, txn: &CatalogTransaction, info: &DropInfo) -> CatalogResult<()>;

    // ── 遍历 ──────────────────────────────────────────────────────────────────
    fn scan(
        &self,
        txn: &CatalogTransaction,
        catalog_type: CatalogType,
        f: &mut dyn FnMut(&CatalogEntryNode),
    );
    fn scan_all(&self, catalog_type: CatalogType, f: &mut dyn FnMut(&CatalogEntryNode));
}

// ─── DuckSchemaEntry ──────────────────────────────────────────────────────────

/// DuckDB 原生 Schema 条目（C++: `DuckSchemaEntry`）。
///
/// 持有九个 CatalogSet，分别存储各类 catalog 对象。
pub struct DuckSchemaEntry {
    pub base: CatalogEntryBase,

    /// 表（C++: `CatalogSet tables`）。
    pub tables: CatalogSet,
    /// 索引（C++: `CatalogSet indexes`）。
    pub indexes: CatalogSet,
    /// 表函数（C++: `CatalogSet table_functions`）。
    pub table_functions: CatalogSet,
    /// 复制函数（C++: `CatalogSet copy_functions`）。
    pub copy_functions: CatalogSet,
    /// PRAGMA 函数（C++: `CatalogSet pragma_functions`）。
    pub pragma_functions: CatalogSet,
    /// 标量/聚合函数（C++: `CatalogSet functions`）。
    pub functions: CatalogSet,
    /// 序列（C++: `CatalogSet sequences`）。
    pub sequences: CatalogSet,
    /// Collation（C++: `CatalogSet collations`）。
    pub collations: CatalogSet,
    /// 类型（C++: `CatalogSet types`）。
    pub types: CatalogSet,
}

impl DuckSchemaEntry {
    pub fn new(
        oid: u64,
        name: String,
        catalog_name: String,
        catalog_oid: u64,
        internal: bool,
    ) -> Self {
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::SchemaEntry,
            name.clone(),
            catalog_name,
            String::new(),
        );
        base.internal = internal;
        base.set_timestamp(0);

        Self {
            base,
            tables: CatalogSet::new(catalog_oid),
            indexes: CatalogSet::new(catalog_oid),
            table_functions: CatalogSet::new(catalog_oid),
            copy_functions: CatalogSet::new(catalog_oid),
            pragma_functions: CatalogSet::new(catalog_oid),
            functions: CatalogSet::new(catalog_oid),
            sequences: CatalogSet::new(catalog_oid),
            collations: CatalogSet::new(catalog_oid),
            types: CatalogSet::new(catalog_oid),
        }
    }

    /// 根据 CatalogType 返回对应的 CatalogSet 引用（C++: `GetCatalogSet`）。
    pub fn get_catalog_set(&self, catalog_type: CatalogType) -> &CatalogSet {
        match catalog_type {
            CatalogType::TableEntry => &self.tables,
            CatalogType::IndexEntry => &self.indexes,
            CatalogType::TableFunctionEntry => &self.table_functions,
            CatalogType::CopyFunctionEntry => &self.copy_functions,
            CatalogType::PragmaFunctionEntry => &self.pragma_functions,
            CatalogType::ScalarFunctionEntry
            | CatalogType::AggregateFunctionEntry
            | CatalogType::MacroEntry
            | CatalogType::TableMacroEntry => &self.functions,
            CatalogType::SequenceEntry => &self.sequences,
            CatalogType::CollateCatalogEntry => &self.collations,
            CatalogType::TypeEntry => &self.types,
            CatalogType::ViewEntry => &self.tables, // 视图存在 tables set 中
            _ => &self.tables,
        }
    }

    /// 内部添加条目（C++: `DuckSchemaEntry::AddEntryInternal`）。
    fn add_entry_internal(
        &self,
        txn: &CatalogTransaction,
        node: Box<CatalogEntryNode>,
        on_conflict: OnCreateConflict,
        dependencies: &LogicalDependencyList,
    ) -> CatalogResult<CatalogEntryNode> {
        let name = node.base.name.clone();
        let entry_type = node.base.entry_type;
        let set = self.get_catalog_set(entry_type);

        let created = match on_conflict {
            OnCreateConflict::ErrorOnConflict => {
                set.create_entry(txn, &name, node, dependencies)?;
                true
            }
            OnCreateConflict::IgnoreOnConflict => {
                match set.create_entry(txn, &name, node, dependencies) {
                    Ok(v) => v,
                    Err(CatalogError::AlreadyExists { .. }) => false,
                    Err(e) => return Err(e),
                }
            }
            OnCreateConflict::ReplaceOnConflict | OnCreateConflict::AlterOnConflict => {
                set.create_or_replace_entry(txn, &name, node, dependencies)?;
                true
            }
        };

        if created {
            set.get_entry(txn, &name).ok_or_else(|| {
                CatalogError::other(format!("Entry \"{}\" disappeared after creation", name))
            })
        } else {
            set.get_entry(txn, &name)
                .ok_or_else(|| CatalogError::not_found(entry_type, &name))
        }
    }

    /// 生成新 OID（简单的计数器）。
    fn next_oid(name: &str, entry_type: CatalogType) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        name.hash(&mut h);
        (entry_type as u64).hash(&mut h);
        h.finish()
    }
}

impl SchemaCatalogEntry for DuckSchemaEntry {
    fn name(&self) -> &str {
        &self.base.name
    }
    fn catalog_name(&self) -> &str {
        &self.base.catalog_name
    }
    fn is_internal(&self) -> bool {
        self.base.internal
    }

    fn to_sql(&self) -> String {
        if self.base.internal {
            return String::new();
        }
        format!("CREATE SCHEMA {};", self.base.name)
    }

    // ── 创建操作 ──────────────────────────────────────────────────────────────

    fn create_table(
        &self,
        txn: &CatalogTransaction,
        info: &CreateTableInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.table, CatalogType::TableEntry);
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::TableEntry,
            info.table.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.temporary = info.base.temporary;
        base.comment = info.base.comment.clone();
        base.tags = info.base.tags.clone();
        let kind = CatalogEntryKind::Table(TableEntryData::new(
            info.columns.clone(),
            info.constraints.clone(),
        ));
        let node = Box::new(CatalogEntryNode::new(base, kind));
        self.add_entry_internal(
            txn,
            node,
            info.base.on_conflict,
            &LogicalDependencyList::new(),
        )
    }

    fn create_view(
        &self,
        txn: &CatalogTransaction,
        info: &CreateViewInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.view_name, CatalogType::ViewEntry);
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::ViewEntry,
            info.view_name.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.temporary = info.base.temporary;
        base.comment = info.base.comment.clone();
        let mut view_data = ViewEntryData::new(info.query.clone());
        view_data.aliases = info.aliases.clone();
        view_data.types = info.types.clone();
        view_data.column_names = info.column_names.clone();
        let node = Box::new(CatalogEntryNode::new(
            base,
            CatalogEntryKind::View(view_data),
        ));
        self.add_entry_internal(
            txn,
            node,
            info.base.on_conflict,
            &LogicalDependencyList::new(),
        )
    }

    fn create_sequence(
        &self,
        txn: &CatalogTransaction,
        info: &CreateSequenceInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.name, CatalogType::SequenceEntry);
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::SequenceEntry,
            info.name.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.temporary = info.base.temporary;
        base.comment = info.base.comment.clone();
        let seq_data = SequenceEntryData::from_create_info(info);
        let node = Box::new(CatalogEntryNode::new(
            base,
            CatalogEntryKind::Sequence(seq_data),
        ));
        self.add_entry_internal(
            txn,
            node,
            info.base.on_conflict,
            &LogicalDependencyList::new(),
        )
    }

    fn create_type(
        &self,
        txn: &CatalogTransaction,
        info: &CreateTypeInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.name, CatalogType::TypeEntry);
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::TypeEntry,
            info.name.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.comment = info.base.comment.clone();
        let node = Box::new(CatalogEntryNode::new(
            base,
            CatalogEntryKind::Type(TypeEntryData::new(info.logical_type.clone())),
        ));
        self.add_entry_internal(
            txn,
            node,
            info.base.on_conflict,
            &LogicalDependencyList::new(),
        )
    }

    fn create_index(
        &self,
        txn: &CatalogTransaction,
        info: &CreateIndexInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.index_name, CatalogType::IndexEntry);
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::IndexEntry,
            info.index_name.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.comment = info.base.comment.clone();
        let idx_data = IndexEntryData {
            sql: info.sql.clone(),
            index_type: info.index_type.clone(),
            constraint_type: info.constraint_type,
            column_ids: info.column_ids.clone(),
            expressions: info.expressions.clone(),
            options: info.options.clone(),
            table_name: info.table.clone(),
            schema_name: self.base.name.clone(),
        };
        let node = Box::new(CatalogEntryNode::new(
            base,
            CatalogEntryKind::Index(idx_data),
        ));
        self.add_entry_internal(
            txn,
            node,
            info.base.on_conflict,
            &LogicalDependencyList::new(),
        )
    }

    fn create_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.name, info.base.catalog_type);
        let mut base = CatalogEntryBase::new(
            oid,
            info.base.catalog_type,
            info.name.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.comment = info.base.comment.clone();
        let mut fn_data = FunctionEntryData::new(info.base.catalog_type);
        fn_data.descriptions = info.descriptions.clone();
        let node = Box::new(CatalogEntryNode::new(
            base,
            CatalogEntryKind::Function(fn_data),
        ));
        self.add_entry_internal(
            txn,
            node,
            info.base.on_conflict,
            &LogicalDependencyList::new(),
        )
    }

    fn create_table_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        self.create_function(txn, info)
    }

    fn create_copy_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCopyFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        self.create_function(txn, info)
    }

    fn create_pragma_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreatePragmaFunctionInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        self.create_function(txn, info)
    }

    fn create_collation(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCollationInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.name, CatalogType::CollateCatalogEntry);
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::CollateCatalogEntry,
            info.name.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.comment = info.base.comment.clone();
        let node = Box::new(CatalogEntryNode::new(
            base,
            CatalogEntryKind::Generic { sql: String::new() },
        ));
        self.add_entry_internal(
            txn,
            node,
            info.base.on_conflict,
            &LogicalDependencyList::new(),
        )
    }

    // ── 查找操作 ──────────────────────────────────────────────────────────────

    fn lookup_entry(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
    ) -> Option<CatalogEntryNode> {
        let set = self.get_catalog_set(lookup.catalog_type);
        set.get_entry(txn, &lookup.name)
    }

    fn get_similar_entry(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
    ) -> SimilarCatalogEntry {
        let set = self.get_catalog_set(lookup.catalog_type);
        set.similar_entry(txn, &lookup.name)
    }

    // ── 修改与删除 ────────────────────────────────────────────────────────────

    fn alter(&self, txn: &CatalogTransaction, info: &AlterInfo) -> CatalogResult<()> {
        let set = self.get_catalog_set(info.catalog_type);
        set.alter_entry(txn, &info.name, info)
    }

    fn drop_entry(&self, txn: &CatalogTransaction, info: &DropInfo) -> CatalogResult<()> {
        let set = self.get_catalog_set(info.catalog_type);
        match set.drop_entry(txn, &info.name, info.cascade, info.allow_drop_internal) {
            Ok(()) => Ok(()),
            Err(CatalogError::EntryNotFound { .. }) if info.if_exists => Ok(()),
            Err(e) => Err(e),
        }
    }

    // ── 遍历 ──────────────────────────────────────────────────────────────────

    fn scan(
        &self,
        txn: &CatalogTransaction,
        catalog_type: CatalogType,
        f: &mut dyn FnMut(&CatalogEntryNode),
    ) {
        self.get_catalog_set(catalog_type)
            .scan_with_txn(txn, |node| f(node));
    }

    fn scan_all(&self, catalog_type: CatalogType, f: &mut dyn FnMut(&CatalogEntryNode)) {
        self.get_catalog_set(catalog_type).scan(|node| f(node));
    }
}
