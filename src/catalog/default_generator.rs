//! 默认条目生成器。
//!
//! 对应 C++:
//!   - `duckdb/catalog/default/default_generator.hpp`（`DefaultGenerator`）
//!   - `duckdb/catalog/default/default_schemas.hpp`（`DefaultSchemaGenerator`）
//!
//! # 设计说明
//!
//! | C++ | Rust |
//! |-----|------|
//! | `virtual unique_ptr<CatalogEntry> CreateDefaultEntry(...)` | `fn create_default_entry(...)` |
//! | `atomic<bool> created_all_entries` | `created_all_entries: AtomicBool` |

use std::sync::Arc;

use super::catalog_entry::DuckSchemaEntry;
use super::entry::{CatalogEntryBase, CatalogEntryKind, CatalogEntryNode};
use super::catalog_transaction::CatalogTransaction;
use super::types::CatalogType;
use std::sync::atomic::{AtomicBool, Ordering};

// ─── DefaultGenerator trait ────────────────────────────────────────────────────

/// 默认条目生成器接口（C++: `class DefaultGenerator`）。
///
/// 允许 CatalogSet 在条目不存在时懒加载默认条目（如内置函数、系统 schema 等）。
pub trait DefaultGenerator: Send + Sync {
    /// 为给定名称创建默认条目，如果名称没有对应默认条目则返回 None。
    ///
    /// （C++: `virtual unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext&, const string&)`）
    fn create_default_entry(
        &mut self,
        txn: &CatalogTransaction,
        name: &str,
    ) -> Option<CatalogEntryNode>;

    /// 返回所有默认条目的名称列表（C++: `virtual vector<string> GetDefaultEntries()`）。
    fn default_entry_names(&self) -> Vec<String>;

    /// 是否已经创建了所有默认条目（C++: `atomic<bool> created_all_entries`）。
    fn all_entries_created(&self) -> bool;

    /// 标记所有默认条目已创建。
    fn mark_all_created(&mut self);
}

// ─── DefaultSchemaGenerator ────────────────────────────────────────────────────

/// 内置 schema 生成器（C++: `class DefaultSchemaGenerator`）。
///
/// 负责生成 `main`、`pg_catalog`、`information_schema` 等系统 schema。
pub struct DefaultSchemaGenerator {
    catalog_oid: u64,
    catalog_name: String,
    created_all: AtomicBool,
}

impl DefaultSchemaGenerator {
    /// DuckDB 内置 schema 名称。
    pub const DEFAULT_SCHEMAS: &'static [&'static str] =
        &["main", "pg_catalog", "information_schema", "temp"];

    pub fn new(catalog_oid: u64, catalog_name: impl Into<String>) -> Self {
        Self {
            catalog_oid,
            catalog_name: catalog_name.into(),
            created_all: AtomicBool::new(false),
        }
    }

    /// 判断是否为内置 schema（C++: `DefaultSchemaGenerator::IsDefaultSchema`）。
    pub fn is_default_schema(name: &str) -> bool {
        let lower = name.to_lowercase();
        Self::DEFAULT_SCHEMAS
            .iter()
            .any(|s| s.eq_ignore_ascii_case(&lower))
    }

    fn make_schema_node(&self, name: &str, _txn: &CatalogTransaction) -> CatalogEntryNode {
        let oid = name.len() as u64 ^ self.catalog_oid; // 简单的 OID 生成
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::SchemaEntry,
            name.to_string(),
            self.catalog_name.clone(),
            String::new(),
        );
        base.internal = true;
        base.set_timestamp(0); // 系统条目时间戳为 0（始终可见）
        let schema = Arc::new(DuckSchemaEntry::new(
            oid,
            name.to_string(),
            self.catalog_name.clone(),
            self.catalog_oid,
            true,
        ));
        CatalogEntryNode::new(base, CatalogEntryKind::Schema(schema))
    }
}

impl DefaultGenerator for DefaultSchemaGenerator {
    fn create_default_entry(
        &mut self,
        txn: &CatalogTransaction,
        name: &str,
    ) -> Option<CatalogEntryNode> {
        if Self::is_default_schema(name) {
            Some(self.make_schema_node(name, txn))
        } else {
            None
        }
    }

    fn default_entry_names(&self) -> Vec<String> {
        Self::DEFAULT_SCHEMAS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn all_entries_created(&self) -> bool {
        self.created_all.load(Ordering::SeqCst)
    }

    fn mark_all_created(&mut self) {
        self.created_all.store(true, Ordering::SeqCst);
    }
}

// ─── EmptyGenerator ────────────────────────────────────────────────────────────

/// 空生成器（不提供任何默认条目）。
pub struct EmptyGenerator {
    created_all: bool,
}

impl EmptyGenerator {
    pub fn new() -> Self {
        Self { created_all: false }
    }
}

impl Default for EmptyGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultGenerator for EmptyGenerator {
    fn create_default_entry(
        &mut self,
        _txn: &CatalogTransaction,
        _name: &str,
    ) -> Option<CatalogEntryNode> {
        None
    }
    fn default_entry_names(&self) -> Vec<String> {
        Vec::new()
    }
    fn all_entries_created(&self) -> bool {
        self.created_all
    }
    fn mark_all_created(&mut self) {
        self.created_all = true;
    }
}
