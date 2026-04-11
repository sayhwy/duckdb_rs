//! `CatalogEntry` 和 `InCatalogEntry` 的 Rust 实现。
//!
//! 对应 C++:
//!   - `src/include/duckdb/catalog/catalog_entry.hpp`（`CatalogEntry`, `InCatalogEntry`）
//!   - `src/catalog/catalog_entry.cpp`
//!
//! # 设计说明
//!
//! C++ 使用虚函数继承体系，Rust 采用：
//! - [`CatalogEntryFields`]：公共字段结构体（对应 `CatalogEntry` 所有数据成员）。
//! - [`CatalogEntryVirtual`] trait：虚方法接口（对应 C++ `virtual` 方法集合），
//!   提供与 C++ 基类相同的"抛出 InternalException"默认实现。
//! - [`InCatalogEntry`]：组合 `CatalogEntryFields` 并新增 `catalog_name`，
//!   覆盖 `parent_catalog_name` / `verify`，对应 C++ `InCatalogEntry`。
//! - [`CatalogEntryChain`]：MVCC 版本链（`child` / `parent` 指针语义）。
//!
//! | C++ 成员                              | Rust 实现                              |
//! |---------------------------------------|----------------------------------------|
//! | `idx_t oid`                           | `fields.oid: u64`                      |
//! | `CatalogType type`                    | `fields.entry_type: CatalogType`       |
//! | `optional_ptr<CatalogSet> set`        | 由 `CatalogSet` 管理，此处不持有       |
//! | `string name`                         | `fields.name: String`                  |
//! | `bool deleted`                        | `fields.deleted: bool`                 |
//! | `bool temporary`                      | `fields.temporary: bool`               |
//! | `bool internal`                       | `fields.internal: bool`                |
//! | `atomic<transaction_t> timestamp`     | `fields.timestamp: AtomicU64`          |
//! | `Value comment`                       | `fields.comment: Option<Value>`        |
//! | `InsertionOrderPreservingMap<string> tags` | `fields.tags: HashMap<String,String>` |
//! | `unique_ptr<CatalogEntry> child`      | `CatalogEntryChain::older`             |
//! | `optional_ptr<CatalogEntry> parent`   | 由 `CatalogSet` 管理，此处不持有       |

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::error::CatalogError;
use super::transaction::CatalogTransaction;
use super::types::{AlterInfo, CatalogType, CreateInfo, Value};
use crate::common::errors::CatalogResult;

pub mod duck_schema_entry;
pub mod duck_table_entry;
pub mod schema_catalog_entry;
pub mod table_catalog_entry;

pub use duck_schema_entry::DuckSchemaEntry;
pub use duck_table_entry::DuckTableEntry;
pub use schema_catalog_entry::SchemaCatalogEntry;
pub use table_catalog_entry::{
    COLUMN_IDENTIFIER_ROW_ID, ColumnSegmentInfo, ColumnStatistics, IndexInfo, LogicalIndex,
    PhysicalIndex, ScanFunctionBinding, TableCatalogEntry, TableCatalogEntryVirtual,
    TableStorageInfo, VirtualColumn, VirtualColumnMap,
};

// ─── CatalogEntryFields ────────────────────────────────────────────────────────

/// `CatalogEntry` 的公共数据字段（C++: `CatalogEntry` 所有 public 成员变量）。
#[derive(Debug)]
pub struct CatalogEntryFields {
    /// 对象标识符（C++: `idx_t oid`）。
    pub oid: u64,
    /// 条目类型（C++: `CatalogType type`）。
    pub entry_type: CatalogType,
    /// 条目名称（C++: `string name`）。
    pub name: String,
    /// 是否已删除（C++: `bool deleted`）。
    pub deleted: bool,
    /// 是否为临时对象，不写 WAL（C++: `bool temporary`）。
    pub temporary: bool,
    /// 是否为内部系统对象，不可删除、不导出（C++: `bool internal`）。
    pub internal: bool,
    /// MVCC 可见性时间戳（C++: `atomic<transaction_t> timestamp`）。
    pub timestamp: AtomicU64,
    /// 可选注释（C++: `Value comment`）。
    pub comment: Option<Value>,
    /// 额外键值标签（C++: `InsertionOrderPreservingMap<string> tags`）。
    pub tags: HashMap<String, String>,
}

impl CatalogEntryFields {
    /// 从 oid 和基本字段构造（C++: `CatalogEntry(CatalogType, string name_p, idx_t oid)`）。
    pub fn new(entry_type: CatalogType, name: impl Into<String>, oid: u64) -> Self {
        Self {
            oid,
            entry_type,
            name: name.into(),
            deleted: false,
            temporary: false,
            internal: false,
            timestamp: AtomicU64::new(0),
            comment: None,
            tags: HashMap::new(),
        }
    }

    /// 读取时间戳（C++: `timestamp.load()`）。
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::SeqCst)
    }

    /// 写入时间戳（C++: `timestamp.store()`）。
    pub fn set_timestamp(&self, ts: u64) {
        self.timestamp.store(ts, Ordering::SeqCst);
    }
}

impl Clone for CatalogEntryFields {
    fn clone(&self) -> Self {
        Self {
            oid: self.oid,
            entry_type: self.entry_type,
            name: self.name.clone(),
            deleted: self.deleted,
            temporary: self.temporary,
            internal: self.internal,
            timestamp: AtomicU64::new(self.get_timestamp()),
            comment: self.comment.clone(),
            tags: self.tags.clone(),
        }
    }
}

// ─── CatalogEntryVirtual ───────────────────────────────────────────────────────

/// `CatalogEntry` 的虚方法接口（C++: `CatalogEntry` 所有 `virtual` 方法）。
///
/// 每个方法的默认实现对应 C++ 基类中抛出 `InternalException` 的行为。
/// 子类型通过实现此 trait 覆盖所需方法，等同于 C++ 的虚函数覆盖。
///
/// # 线程安全
/// `Send + Sync` 约束对应 C++ 中跨线程共享 catalog 条目的惯例。
pub trait CatalogEntryVirtual: Send + Sync {
    // ── AlterEntry ────────────────────────────────────────────────────────────

    /// 使用客户端上下文执行 Alter 操作，返回新版本节点。
    ///
    /// C++: `virtual unique_ptr<CatalogEntry> AlterEntry(ClientContext&, AlterInfo&)`
    ///
    /// 默认实现：抛出"不支持"错误（对应 C++ 的 `throw InternalException`）。
    fn alter_entry(&self, _info: &AlterInfo) -> CatalogResult<Box<dyn CatalogEntryVirtual>> {
        Err(CatalogError::other(
            "Unsupported alter type for catalog entry!",
        ))
    }

    /// 使用事务上下文执行 Alter 操作。
    ///
    /// C++: `virtual unique_ptr<CatalogEntry> AlterEntry(CatalogTransaction, AlterInfo&)`
    ///
    /// 默认实现：检查 `has_context` 后委托给 `alter_entry`，
    /// 与 C++ 基类 `AlterEntry(CatalogTransaction, ...)` 的逻辑完全对应。
    fn alter_entry_txn(
        &self,
        transaction: &CatalogTransaction,
        info: &AlterInfo,
    ) -> CatalogResult<Box<dyn CatalogEntryVirtual>> {
        if !transaction.has_context {
            return Err(CatalogError::other(
                "Cannot AlterEntry without client context",
            ));
        }
        self.alter_entry(info)
    }

    // ── Undo / Rollback / OnDrop ──────────────────────────────────────────────

    /// 撤销 Alter 操作（C++: `virtual void UndoAlter(ClientContext&, AlterInfo&)`）。
    fn undo_alter(&self, _info: &AlterInfo) {}

    /// 回滚到旧版本（C++: `virtual void Rollback(CatalogEntry& prev_entry)`）。
    fn rollback(&self) {}

    /// 条目被删除时的清理回调（C++: `virtual void OnDrop()`）。
    fn on_drop(&mut self) {}

    // ── Copy / GetInfo ────────────────────────────────────────────────────────

    /// 深拷贝此条目（C++: `virtual unique_ptr<CatalogEntry> Copy(ClientContext&) const`）。
    fn copy(&self) -> CatalogResult<Box<dyn CatalogEntryVirtual>> {
        Err(CatalogError::other(
            "Unsupported copy type for catalog entry!",
        ))
    }

    /// 获取条目的 `CreateInfo`（C++: `virtual unique_ptr<CreateInfo> GetInfo() const`）。
    fn get_info(&self) -> CatalogResult<CreateInfo> {
        Err(CatalogError::other(
            "Unsupported type for CatalogEntry::GetInfo!",
        ))
    }

    // ── SetAsRoot / ToSQL ────────────────────────────────────────────────────

    /// 设为最新版本根节点时的回调（C++: `virtual void SetAsRoot()`）。
    ///
    /// 默认实现为空（对应 C++ 基类的空实现）。
    fn set_as_root(&mut self) {}

    /// 生成重建此条目的 SQL（C++: `virtual string ToSQL() const`）。
    fn to_sql(&self) -> CatalogResult<String> {
        Err(CatalogError::other("Unsupported catalog type for ToSQL()"))
    }

    // ── ParentCatalog / ParentSchema ─────────────────────────────────────────

    /// 返回所属 Catalog 名称（C++: `virtual Catalog& ParentCatalog()`）。
    ///
    /// 默认实现：抛出错误（基类无 catalog 引用）。
    fn parent_catalog_name(&self) -> CatalogResult<&str> {
        Err(CatalogError::other(
            "CatalogEntry::ParentCatalog called on catalog entry without catalog",
        ))
    }

    /// 返回所属 Schema 名称（C++: `virtual SchemaCatalogEntry& ParentSchema()`）。
    ///
    /// 默认实现：抛出错误（基类无 schema 引用）。
    fn parent_schema_name(&self) -> CatalogResult<&str> {
        Err(CatalogError::other(
            "CatalogEntry::ParentSchema called on catalog entry without schema",
        ))
    }

    // ── Verify / Serialize ───────────────────────────────────────────────────

    /// 一致性校验（C++: `virtual void Verify(Catalog&)`）。
    ///
    /// 默认实现为空（对应 C++ 基类的空实现）。
    fn verify(&self, _catalog_name: &str) -> CatalogResult<()> {
        Ok(())
    }

    /// 序列化条目（C++: `void Serialize(Serializer&) const`）。
    ///
    /// 通过 `get_info()` 获取 `CreateInfo` 再序列化，与 C++ 实现对应。
    fn serialize(&self) -> CatalogResult<Vec<u8>> {
        // C++: `const auto info = GetInfo(); info->Serialize(serializer);`
        let _info = self.get_info()?;
        // 实际序列化由 Serializer 子系统完成；此处返回占位字节流。
        Ok(Vec::new())
    }
}

// ─── CatalogEntryChain ────────────────────────────────────────────────────────

/// MVCC 版本链辅助类型。
///
/// 封装 C++ `CatalogEntry` 中 `child` / `parent` 指针语义：
/// - `older`：指向同名条目的旧版本（C++: `unique_ptr<CatalogEntry> child`）。
/// - `parent` 方向由 `CatalogSet` 统一持有，此处不存储。
///
/// 典型 MVCC 链：`(新版本) → older → (旧版本) → older → None`
#[derive(Debug)]
pub struct CatalogEntryChain {
    /// 同名条目的旧版本（C++: `unique_ptr<CatalogEntry> child`）。
    pub older: Option<Box<CatalogEntryChain>>,
    /// 此版本对应的字段快照。
    pub fields: CatalogEntryFields,
}

impl CatalogEntryChain {
    /// 构造链节点（C++: `CatalogEntry` 构造 + `child = nullptr`）。
    pub fn new(fields: CatalogEntryFields) -> Self {
        Self {
            older: None,
            fields,
        }
    }

    /// 是否存在旧版本（C++: `bool HasChild() const`）。
    pub fn has_older(&self) -> bool {
        self.older.is_some()
    }

    /// 获取旧版本共享引用（C++: `CatalogEntry& Child()`）。
    pub fn older_ref(&self) -> Option<&CatalogEntryChain> {
        self.older.as_deref()
    }

    /// 获取旧版本可变引用。
    pub fn older_mut(&mut self) -> Option<&mut CatalogEntryChain> {
        self.older.as_deref_mut()
    }

    /// 取出旧版本所有权（C++: `unique_ptr<CatalogEntry> TakeChild()`）。
    ///
    /// 取出后 `older` 变为 `None`，等同于 C++ 中 `child->parent = nullptr; return std::move(child);`
    pub fn take_older(&mut self) -> Option<Box<CatalogEntryChain>> {
        self.older.take()
    }

    /// 设置旧版本（C++: `void SetChild(unique_ptr<CatalogEntry> child_p)`）。
    ///
    /// 对应 C++ 的 `child = std::move(child_p); child->parent = this;`
    pub fn set_older(&mut self, older: Box<CatalogEntryChain>) {
        self.older = Some(older);
    }
}

// ─── InCatalogEntry ────────────────────────────────────────────────────────────

/// 属于某个 Catalog 的条目（C++: `class InCatalogEntry : public CatalogEntry`）。
///
/// 在 `CatalogEntryFields` 基础上新增 `catalog_name` 字段，
/// 并覆盖 `ParentCatalog` 和 `Verify`（C++: `override`）。
#[derive(Debug, Clone)]
pub struct InCatalogEntry {
    /// 继承自 `CatalogEntry` 的字段（C++: base class members）。
    pub fields: CatalogEntryFields,
    /// 所属 Catalog 名称（C++: `Catalog& catalog`，以名称代替直接引用）。
    pub catalog_name: String,
}

impl InCatalogEntry {
    /// 构造（C++: `InCatalogEntry(CatalogType type, Catalog& catalog, string name)`）。
    pub fn new(
        entry_type: CatalogType,
        catalog_name: impl Into<String>,
        name: impl Into<String>,
        oid: u64,
    ) -> Self {
        let catalog_name = catalog_name.into();
        let name = name.into();
        Self {
            fields: CatalogEntryFields::new(entry_type, name, oid),
            catalog_name,
        }
    }

    /// 返回所属 Catalog 名称（C++: `Catalog& ParentCatalog() override`）。
    pub fn parent_catalog(&self) -> &str {
        &self.catalog_name
    }

    /// 一致性校验（C++: `void Verify(Catalog& catalog_p) override`）。
    ///
    /// 对应 C++ 中的 `D_ASSERT(&catalog_p == &catalog)` —— 检查 catalog 名称相符。
    pub fn verify(&self, catalog_name: &str) -> CatalogResult<()> {
        if self.catalog_name != catalog_name {
            return Err(CatalogError::other(format!(
                "InCatalogEntry::Verify failed: entry '{}' belongs to catalog '{}', not '{}'",
                self.fields.name, self.catalog_name, catalog_name
            )));
        }
        Ok(())
    }
}
