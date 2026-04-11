//! Schema Catalog 条目接口。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog_entry/schema_catalog_entry.hpp`

use crate::common::errors::CatalogResult;

use crate::catalog::catalog_set::CatalogSet;
use crate::catalog::entry::CatalogEntryNode;
use crate::catalog::entry_lookup::{EntryLookupInfo, SimilarCatalogEntry};
use crate::catalog::transaction::CatalogTransaction;
use crate::catalog::types::{
    AlterInfo, CatalogType, CreateCollationInfo, CreateCopyFunctionInfo, CreateFunctionInfo,
    CreateIndexInfo, CreatePragmaFunctionInfo, CreateSequenceInfo, BoundCreateTableInfo,
    CreateTypeInfo, CreateViewInfo, DropInfo,
};

/// Schema 条目接口（C++: `class SchemaCatalogEntry`）。
pub trait SchemaCatalogEntry: Send + Sync {
    fn name(&self) -> &str;
    fn catalog_name(&self) -> &str;
    fn is_internal(&self) -> bool;
    fn to_sql(&self) -> String;

    fn create_table(
        &self,
        txn: &CatalogTransaction,
        info: &BoundCreateTableInfo,
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

    fn alter(&self, txn: &CatalogTransaction, info: &AlterInfo) -> CatalogResult<()>;
    fn drop_entry(&self, txn: &CatalogTransaction, info: &DropInfo) -> CatalogResult<()>;

    fn scan(
        &self,
        txn: &CatalogTransaction,
        catalog_type: CatalogType,
        f: &mut dyn FnMut(&CatalogEntryNode),
    );
    fn scan_all(&self, catalog_type: CatalogType, f: &mut dyn FnMut(&CatalogEntryNode));

    fn lookup_entry_detailed(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
    ) -> crate::catalog::EntryLookupResult {
        match self.lookup_entry(txn, lookup) {
            Some(node) => crate::catalog::EntryLookupResult {
                node: Some(Box::new(node)),
                reason: crate::catalog::LookupFailureReason::Success,
            },
            None => crate::catalog::EntryLookupResult {
                node: None,
                reason: crate::catalog::LookupFailureReason::Deleted,
            },
        }
    }

    fn get_entry(
        &self,
        txn: &CatalogTransaction,
        catalog_type: CatalogType,
        name: &str,
    ) -> Option<CatalogEntryNode> {
        self.lookup_entry(txn, &EntryLookupInfo::new(catalog_type, name))
    }

    fn catalog_set_for_type(&self, _catalog_type: CatalogType) -> Option<&CatalogSet> {
        None
    }
}
