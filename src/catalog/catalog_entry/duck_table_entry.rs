//! DuckTableEntry 实现。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog_entry/duck_table_entry.hpp`
//!   - `duckdb/catalog/catalog_entry/duck_table_entry.cpp`
//!
//! 当前阶段只先对齐对象边界：
//! - `TableCatalogEntry` 保留 catalog 基类字段与通用表元数据；
//! - `DuckTableEntry` 组合 `TableCatalogEntry` 与 `DataTable`，作为运行时唯一表对象。

use std::fmt;
use std::sync::Arc;

use crate::catalog::{BoundCreateTableInfo, CreateTableInfo, TableCatalogEntry};
use crate::common::errors::CatalogResult;
use crate::catalog::error::CatalogError;
use crate::storage::data_table::DataTable;

/// DuckDB 原生表条目（C++: `DuckTableEntry`）。
#[derive(Clone)]
pub struct DuckTableEntry {
    pub base: TableCatalogEntry,
    pub storage: Arc<DataTable>,
}

impl fmt::Debug for DuckTableEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckTableEntry")
            .field("name", &self.base.base.fields().name)
            .field("schema", &self.base.base.parent_schema())
            .field("catalog", &self.base.base.parent_catalog())
            .finish()
    }
}

impl DuckTableEntry {
    pub fn new(base: TableCatalogEntry, storage: Arc<DataTable>) -> Self {
        Self { base, storage }
    }

    pub fn from_create_info(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        oid: u64,
        info: CreateTableInfo,
        storage: Arc<DataTable>,
    ) -> Self {
        Self {
            base: TableCatalogEntry::new(catalog_name, schema_name, oid, info),
            storage,
        }
    }

    pub fn from_bound_info(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        oid: u64,
        info: &BoundCreateTableInfo,
    ) -> CatalogResult<Self> {
        let storage = info.storage.clone().ok_or_else(|| {
            CatalogError::other("BoundCreateTableInfo is missing storage for DuckTableEntry")
        })?;
        Ok(Self {
            base: TableCatalogEntry::new(catalog_name, schema_name, oid, info.base.clone()),
            storage,
        })
    }

    pub fn get_info(&self) -> CreateTableInfo {
        self.base.get_info()
    }

    pub fn name(&self) -> &str {
        &self.base.base.fields().name
    }

    pub fn schema_name(&self) -> &str {
        &self.base.base.schema_name
    }

    pub fn catalog_entry(&self) -> &TableCatalogEntry {
        &self.base
    }
}
