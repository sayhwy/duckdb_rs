//! `StandardEntry` 的 Rust 实现。
//!
//! 对应 C++: `src/include/duckdb/catalog/standard_entry.hpp`
//!
//! # 设计说明
//!
//! `StandardEntry` 是 DuckDB 中所有"属于某个 Schema 的 Catalog 条目"的基类，
//! 继承自 `InCatalogEntry`，新增两个成员：
//! - `schema`：所属 schema 的引用（C++ 持有引用，Rust 以名称字符串代替）。
//! - `dependencies`：此条目的逻辑依赖集合（可为空）。
//!
//! | C++ 成员                              | Rust 实现                              |
//! |---------------------------------------|----------------------------------------|
//! | `SchemaCatalogEntry& schema`          | `schema_name: String`                  |
//! | `LogicalDependencyList dependencies`  | `dependencies: LogicalDependencyList`  |
//! | `SchemaCatalogEntry& ParentSchema()`  | `parent_schema() -> &str`              |
//! | `const SchemaCatalogEntry& ParentSchema() const` | `parent_schema() -> &str` （同一方法，Rust 不区分 const） |

use super::catalog_entry::{InCatalogEntry, CatalogEntryFields, CatalogEntryVirtual};
use super::dependency::LogicalDependencyList;
use super::types::{CatalogType, CreateInfo, AlterInfo};
use super::error::CatalogError;
use super::transaction::CatalogTransaction;

// ─── StandardEntry ─────────────────────────────────────────────────────────────

/// 属于某个 Schema 的标准 Catalog 条目（C++: `class StandardEntry : public InCatalogEntry`）。
///
/// 所有具体条目类型（表、视图、序列、索引、类型、函数等）均以此作为逻辑基类。
///
/// # 字段说明
///
/// - `base`：继承自 `InCatalogEntry`，包含 `CatalogEntryFields`（oid、name、
///   deleted、timestamp 等）以及 `catalog_name`。
/// - `schema_name`：所属 Schema 名称（C++ 中为 `SchemaCatalogEntry&` 引用）。
/// - `dependencies`：此条目依赖的其他条目列表，可为空。
#[derive(Debug, Clone)]
pub struct StandardEntry {
    /// 继承自 `InCatalogEntry` 的字段（C++: `InCatalogEntry` base class）。
    pub base: InCatalogEntry,
    /// 所属 Schema 名称（C++: `SchemaCatalogEntry& schema`）。
    pub schema_name: String,
    /// 此条目的逻辑依赖集合（C++: `LogicalDependencyList dependencies`）。
    pub dependencies: LogicalDependencyList,
}

impl StandardEntry {
    /// 构造标准条目。
    ///
    /// C++: `StandardEntry(CatalogType type, SchemaCatalogEntry& schema, Catalog& catalog, string name)`
    ///
    /// # 参数
    /// - `entry_type`：条目类型（`CatalogType`）。
    /// - `schema_name`：所属 schema 名称。
    /// - `catalog_name`：所属 catalog 名称。
    /// - `name`：条目名称。
    /// - `oid`：全局唯一对象 ID（由 `DatabaseManager::NextOid()` 分配）。
    pub fn new(
        entry_type: CatalogType,
        schema_name: impl Into<String>,
        catalog_name: impl Into<String>,
        name: impl Into<String>,
        oid: u64,
    ) -> Self {
        Self {
            base: InCatalogEntry::new(entry_type, catalog_name, name, oid),
            schema_name: schema_name.into(),
            dependencies: LogicalDependencyList::new(),
        }
    }

    // ── 字段访问 ─────────────────────────────────────────────────────────────

    /// 获取通用字段（`CatalogEntryFields`）的不可变引用。
    pub fn fields(&self) -> &CatalogEntryFields {
        &self.base.fields
    }

    /// 获取通用字段（`CatalogEntryFields`）的可变引用。
    pub fn fields_mut(&mut self) -> &mut CatalogEntryFields {
        &mut self.base.fields
    }

    // ── ParentCatalog / ParentSchema ─────────────────────────────────────────

    /// 返回所属 Catalog 名称。
    ///
    /// C++: 继承自 `InCatalogEntry::ParentCatalog() override`。
    pub fn parent_catalog(&self) -> &str {
        self.base.parent_catalog()
    }

    /// 返回所属 Schema 名称。
    ///
    /// C++: `SchemaCatalogEntry& ParentSchema() override`
    ///      + `const SchemaCatalogEntry& ParentSchema() const override`
    pub fn parent_schema(&self) -> &str {
        &self.schema_name
    }

    // ── Verify ───────────────────────────────────────────────────────────────

    /// 一致性校验（C++: 继承 `InCatalogEntry::Verify`）。
    ///
    /// 断言此条目的 `catalog_name` 与传入的 catalog 名称相符。
    pub fn verify(&self, catalog_name: &str) -> Result<(), CatalogError> {
        self.base.verify(catalog_name)
    }
}

// ─── StandardEntry 实现 CatalogEntryVirtual ───────────────────────────────────

/// 为 `StandardEntry` 提供 `CatalogEntryVirtual` 的默认实现。
///
/// 具体条目类型（如 `TableCatalogEntry`）应包装 `StandardEntry` 并为自身
/// 实现 `CatalogEntryVirtual`，覆盖 `alter_entry`、`copy`、`get_info`、`to_sql` 等。
impl CatalogEntryVirtual for StandardEntry {
    /// C++: `SchemaCatalogEntry& ParentSchema() override`
    fn parent_schema_name(&self) -> Result<&str, CatalogError> {
        Ok(&self.schema_name)
    }

    /// C++: `Catalog& ParentCatalog() override`（继承自 `InCatalogEntry`）
    fn parent_catalog_name(&self) -> Result<&str, CatalogError> {
        Ok(self.base.parent_catalog())
    }

    /// C++: `void Verify(Catalog& catalog_p) override`（继承自 `InCatalogEntry`）
    fn verify(&self, catalog_name: &str) -> Result<(), CatalogError> {
        self.base.verify(catalog_name)
    }

    // alter_entry、copy、get_info、to_sql 保留基类的"抛出 InternalException"语义，
    // 由具体子类型覆盖。
}

// ─── StandardEntryBuilder ─────────────────────────────────────────────────────

/// `StandardEntry` 的构建器，方便链式设置字段。
///
/// 对应 C++ 中通过构造函数参数 + 直接字段赋值来初始化条目的模式。
pub struct StandardEntryBuilder {
    entry: StandardEntry,
}

impl StandardEntryBuilder {
    /// 从基本参数创建构建器（等同于 `StandardEntry::new`）。
    pub fn new(
        entry_type: CatalogType,
        schema_name: impl Into<String>,
        catalog_name: impl Into<String>,
        name: impl Into<String>,
        oid: u64,
    ) -> Self {
        Self {
            entry: StandardEntry::new(entry_type, schema_name, catalog_name, name, oid),
        }
    }

    /// 设置为临时对象（C++: `entry->temporary = true`）。
    pub fn temporary(mut self) -> Self {
        self.entry.fields_mut().temporary = true;
        self
    }

    /// 设置为内部对象（C++: `entry->internal = true`）。
    pub fn internal(mut self) -> Self {
        self.entry.fields_mut().internal = true;
        self
    }

    /// 添加依赖（C++: `entry->dependencies.AddDependency(...)`）。
    pub fn with_dependency(mut self, dep: super::dependency::LogicalDependency) -> Self {
        self.entry.dependencies.add(dep);
        self
    }

    /// 完成构建，返回 `StandardEntry`。
    pub fn build(self) -> StandardEntry {
        self.entry
    }
}
