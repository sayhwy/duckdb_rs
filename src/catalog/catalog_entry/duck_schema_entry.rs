//! DuckSchemaEntry 实现。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog_entry/duck_schema_entry.hpp`
//!   - `duckdb/catalog/catalog_entry/duck_schema_entry.cpp`

use std::fmt;

use crate::common::errors::CatalogResult;

use crate::catalog::catalog_entry::schema_catalog_entry::SchemaCatalogEntry;
use crate::catalog::catalog_entry::duck_table_entry::DuckTableEntry;
use crate::catalog::catalog_set::CatalogSet;
use crate::catalog::dependency::LogicalDependencyList;
use crate::catalog::entry::{
    CatalogEntryBase, CatalogEntryKind, CatalogEntryNode, FunctionEntryData, IndexEntryData,
    SequenceEntryData, TypeEntryData, ViewEntryData,
};
use crate::catalog::entry_lookup::{EntryLookupInfo, SimilarCatalogEntry};
use crate::catalog::error::CatalogError;
use crate::catalog::transaction::CatalogTransaction;
use crate::catalog::types::{
    AlterInfo, CatalogType, CreateCollationInfo, CreateCopyFunctionInfo, CreateFunctionInfo,
    CreateIndexInfo, CreatePragmaFunctionInfo, CreateSequenceInfo, BoundCreateTableInfo,
    CreateTypeInfo, CreateViewInfo, DropInfo, OnCreateConflict,
};

/// DuckDB 原生 Schema 条目（C++: `DuckSchemaEntry`）。
pub struct DuckSchemaEntry {
    pub base: CatalogEntryBase,
    pub tables: CatalogSet,
    pub indexes: CatalogSet,
    pub table_functions: CatalogSet,
    pub copy_functions: CatalogSet,
    pub pragma_functions: CatalogSet,
    pub functions: CatalogSet,
    pub sequences: CatalogSet,
    pub collations: CatalogSet,
    pub types: CatalogSet,
}

impl fmt::Debug for DuckSchemaEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckSchemaEntry")
            .field("name", &self.base.name)
            .field("catalog_name", &self.base.catalog_name)
            .field("internal", &self.base.internal)
            .finish()
    }
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
            CatalogType::ViewEntry => &self.tables,
            _ => &self.tables,
        }
    }

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
            OnCreateConflict::IgnoreOnConflict => match set.create_entry(txn, &name, node, dependencies) {
                Ok(v) => v,
                Err(CatalogError::AlreadyExists { .. }) => false,
                Err(e) => return Err(e),
            },
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

    fn create_table(
        &self,
        txn: &CatalogTransaction,
        info: &BoundCreateTableInfo,
    ) -> CatalogResult<CatalogEntryNode> {
        let oid = Self::next_oid(&info.base.table, CatalogType::TableEntry);
        let table_entry = DuckTableEntry::from_bound_info(
            self.base.catalog_name.clone(),
            self.base.name.clone(),
            oid,
            info,
        )?;
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::TableEntry,
            info.base.table.clone(),
            self.base.catalog_name.clone(),
            self.base.name.clone(),
        );
        base.temporary = info.base.base.temporary;
        base.comment = info.base.base.comment.clone();
        base.tags = info.base.base.tags.clone();
        let kind = CatalogEntryKind::Table(std::sync::Arc::new(table_entry));
        let node = Box::new(CatalogEntryNode::new(base, kind));
        self.add_entry_internal(txn, node, info.base.base.on_conflict, &LogicalDependencyList::new())
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
        let node = Box::new(CatalogEntryNode::new(base, CatalogEntryKind::View(view_data)));
        self.add_entry_internal(txn, node, info.base.on_conflict, &LogicalDependencyList::new())
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
        let node = Box::new(CatalogEntryNode::new(
            base,
            CatalogEntryKind::Sequence(SequenceEntryData::from_create_info(info)),
        ));
        self.add_entry_internal(txn, node, info.base.on_conflict, &LogicalDependencyList::new())
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
        self.add_entry_internal(txn, node, info.base.on_conflict, &LogicalDependencyList::new())
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
        let node = Box::new(CatalogEntryNode::new(base, CatalogEntryKind::Index(idx_data)));
        self.add_entry_internal(txn, node, info.base.on_conflict, &LogicalDependencyList::new())
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
        let node = Box::new(CatalogEntryNode::new(base, CatalogEntryKind::Function(fn_data)));
        self.add_entry_internal(txn, node, info.base.on_conflict, &LogicalDependencyList::new())
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
        self.add_entry_internal(txn, node, info.base.on_conflict, &LogicalDependencyList::new())
    }

    fn lookup_entry(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
    ) -> Option<CatalogEntryNode> {
        self.get_catalog_set(lookup.catalog_type).get_entry(txn, &lookup.name)
    }

    fn get_similar_entry(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
    ) -> SimilarCatalogEntry {
        self.get_catalog_set(lookup.catalog_type)
            .similar_entry(txn, &lookup.name)
    }

    fn alter(&self, txn: &CatalogTransaction, info: &AlterInfo) -> CatalogResult<()> {
        self.get_catalog_set(info.catalog_type)
            .alter_entry(txn, &info.name, info)
    }

    fn drop_entry(&self, txn: &CatalogTransaction, info: &DropInfo) -> CatalogResult<()> {
        let set = self.get_catalog_set(info.catalog_type);
        match set.drop_entry(txn, &info.name, info.cascade, info.allow_drop_internal) {
            Ok(()) => Ok(()),
            Err(CatalogError::EntryNotFound { .. }) if info.if_exists => Ok(()),
            Err(e) => Err(e),
        }
    }

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

    fn catalog_set_for_type(&self, catalog_type: CatalogType) -> Option<&CatalogSet> {
        Some(self.get_catalog_set(catalog_type))
    }
}
