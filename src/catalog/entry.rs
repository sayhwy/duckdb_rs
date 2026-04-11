//! Catalog 条目核心类型。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog_entry.hpp`（`CatalogEntry`, `InCatalogEntry`）
//!   - `duckdb/catalog/standard_entry.hpp`（`StandardEntry`）
//!
//! # 设计说明
//!
//! C++ 用虚继承链：`CatalogEntry` → `InCatalogEntry` → `StandardEntry` → 具体条目。
//! Rust 用：
//! - `CatalogEntryBase`：所有公共字段（对应 `CatalogEntry` + `InCatalogEntry` + `StandardEntry` 的字段合并）。
//! - `CatalogEntryKind`：enum，携带各类型专属数据。
//! - `CatalogEntryNode`：MVCC 版本链节点（含 `older` 指向旧版本）。
//! - `CatalogEntryRef`：`Arc<parking_lot::RwLock<CatalogEntryNode>>` 别名，用于跨线程共享。
//!
//! | C++ | Rust |
//! |-----|------|
//! | `unique_ptr<CatalogEntry> child` (older version) | `older: Option<Box<CatalogEntryNode>>` |
//! | `optional_ptr<CatalogEntry> parent` | 去除，由 CatalogSet 管理 |
//! | `optional_ptr<CatalogSet> set` | 去除，由 CatalogSet 管理 |
//! | `atomic<transaction_t> timestamp` | `timestamp: AtomicU64` |

use parking_lot::RwLock;
use std::ptr;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::catalog::catalog_entry::{DuckSchemaEntry, DuckTableEntry};
use crate::catalog::catalog_set::CatalogSet;

use super::dependency::LogicalDependencyList;
use super::error::CatalogError;
use crate::common::errors::CatalogResult;
use super::catalog_transaction::CatalogTransaction;
use super::types::{
    AlterInfo, AlterKind, CatalogType, ColumnList, ConstraintType, CreateCollationInfo,
    CreateFunctionInfo, CreateIndexInfo, CreateInfo, CreateSchemaInfo, CreateSequenceInfo,
    CreateTableInfo, CreateTypeInfo, CreateViewInfo, IndexConstraintType, LogicalType, Value,
};

// ─── CatalogEntryBase ──────────────────────────────────────────────────────────

/// Catalog 条目的公共字段（C++: `CatalogEntry` + `InCatalogEntry` + `StandardEntry`）。
#[derive(Debug)]
pub struct CatalogEntryBase {
    /// 对象标识符（C++: `idx_t oid`）。
    pub oid: u64,
    /// 条目类型（C++: `CatalogType type`）。
    pub entry_type: CatalogType,
    /// 条目名称（C++: `string name`）。
    pub name: String,
    /// 是否已删除（C++: `bool deleted`）。
    pub deleted: bool,
    /// 是否为临时对象（C++: `bool temporary`）。
    pub temporary: bool,
    /// 是否为内部系统对象（C++: `bool internal`）。
    pub internal: bool,
    /// 创建/修改时间戳（MVCC 可见性依据）（C++: `atomic<transaction_t> timestamp`）。
    pub timestamp: AtomicU64,
    /// 可选注释（C++: `Value comment`）。
    pub comment: Option<Value>,
    /// 额外标签（C++: `InsertionOrderPreservingMap<string> tags`）。
    pub tags: HashMap<String, String>,
    /// 所属 catalog 名称（C++: `Catalog& catalog` via `InCatalogEntry`）。
    pub catalog_name: String,
    /// 所属 schema 名称（C++: `SchemaCatalogEntry& schema` via `StandardEntry`）。
    pub schema_name: String,
    /// 逻辑依赖集合（C++: `LogicalDependencyList dependencies` in `StandardEntry`）。
    pub dependencies: LogicalDependencyList,
}

impl CatalogEntryBase {
    pub fn new(
        oid: u64,
        entry_type: CatalogType,
        name: String,
        catalog_name: String,
        schema_name: String,
    ) -> Self {
        Self {
            oid,
            entry_type,
            name,
            deleted: false,
            temporary: false,
            internal: false,
            timestamp: AtomicU64::new(0),
            comment: None,
            tags: HashMap::new(),
            catalog_name,
            schema_name,
            dependencies: LogicalDependencyList::new(),
        }
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::SeqCst)
    }
    pub fn set_timestamp(&self, ts: u64) {
        self.timestamp.store(ts, Ordering::SeqCst);
    }
}

impl Clone for CatalogEntryBase {
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
            catalog_name: self.catalog_name.clone(),
            schema_name: self.schema_name.clone(),
            dependencies: self.dependencies.clone(),
        }
    }
}

// ─── CatalogEntryKind ──────────────────────────────────────────────────────────

/// 各类 Catalog 条目的专属数据（C++: 各具体 CatalogEntry 子类的字段）。
#[derive(Debug, Clone)]
pub enum CatalogEntryKind {
    /// Schema 条目占位（实际数据在 DuckSchemaEntry 中，通过 CatalogSet 管理）。
    Schema(Arc<DuckSchemaEntry>),
    /// 表条目数据（C++: `DuckTableEntry` 核心字段）。
    Table(Arc<DuckTableEntry>),
    /// 视图条目数据（C++: `ViewCatalogEntry`）。
    View(ViewEntryData),
    /// 序列条目数据（C++: `SequenceCatalogEntry`）。
    Sequence(SequenceEntryData),
    /// 索引条目数据（C++: `IndexCatalogEntry`）。
    Index(IndexEntryData),
    /// 类型条目数据（C++: `TypeCatalogEntry`）。
    Type(TypeEntryData),
    /// 函数条目数据（C++: `ScalarFunctionCatalogEntry` / `TableFunctionCatalogEntry`）。
    Function(FunctionEntryData),
    /// 依赖关系条目（用于 DependencyManager 的内部 CatalogSet）。
    DependencyRelation(DependencyRelationData),
    /// 通用/占位（用于 Collation, CopyFunction 等暂未详细实现的类型）。
    Generic { sql: String },
}

impl CatalogEntryKind {
    pub fn to_sql(&self, base: &CatalogEntryBase) -> String {
        match self {
            CatalogEntryKind::Schema(_) => format!("CREATE SCHEMA {};", base.name),
            CatalogEntryKind::Table(entry) => entry.base.to_sql(),
            CatalogEntryKind::View(d) => d.to_sql(base),
            CatalogEntryKind::Sequence(d) => d.to_sql(base),
            CatalogEntryKind::Index(d) => d.to_sql(base),
            CatalogEntryKind::Type(d) => d.to_sql(base),
            CatalogEntryKind::Function(d) => d.to_sql(base),
            CatalogEntryKind::DependencyRelation(_) => String::new(),
            CatalogEntryKind::Generic { sql } => sql.clone(),
        }
    }

    pub fn get_create_info(&self, base: &CatalogEntryBase) -> CreateInfo {
        let mut info = CreateInfo::new(base.entry_type);
        info.catalog = base.catalog_name.clone();
        info.schema = base.schema_name.clone();
        info.temporary = base.temporary;
        info.internal = base.internal;
        info.comment = base.comment.clone();
        info.tags = base.tags.clone();
        if let CatalogEntryKind::Table(entry) = self {
            let table_info = entry.get_info();
            info.catalog = table_info.base.catalog;
            info.schema = table_info.base.schema;
            info.temporary = table_info.base.temporary;
            info.internal = table_info.base.internal;
            info.comment = table_info.base.comment;
            info.tags = table_info.base.tags;
        }
        info
    }
}

// ─── TableEntryData ────────────────────────────────────────────────────────────

/// 表条目专属数据（C++: `DuckTableEntry` / `TableCatalogEntry` 字段）。
#[derive(Debug, Clone)]
pub struct TableEntryData {
    pub columns: ColumnList,
    pub constraints: Vec<ConstraintType>,
    /// 存储层 DataTable 的引用 ID（避免直接循环引用，通过 OID 查找）。
    pub storage_oid: Option<u64>,
}

impl TableEntryData {
    pub fn new(columns: ColumnList, constraints: Vec<ConstraintType>) -> Self {
        Self {
            columns,
            constraints,
            storage_oid: None,
        }
    }

    pub fn to_sql(&self, base: &CatalogEntryBase) -> String {
        let schema = if base.schema_name.is_empty() {
            "main"
        } else {
            &base.schema_name
        };
        let mut parts: Vec<String> = self.columns.columns.iter().map(|c| c.to_sql()).collect();
        for c in &self.constraints {
            parts.push(c.to_sql());
        }
        format!(
            "CREATE TABLE {}.{} (\n    {}\n);",
            schema,
            base.name,
            parts.join(",\n    ")
        )
    }

    pub fn apply_alter(
        &self,
        base: &CatalogEntryBase,
        info: &AlterInfo,
    ) -> CatalogResult<(CatalogEntryBase, CatalogEntryKind)> {
        let mut new_base = base.clone();
        let mut new_data = self.clone();

        match &info.kind {
            AlterKind::RenameTable { new_name } => {
                new_base.name = new_name.clone();
            }
            AlterKind::RenameColumn { old_name, new_name } => {
                let col = new_data
                    .columns
                    .get_by_name_mut(old_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, old_name))?;
                col.name = new_name.clone();
            }
            AlterKind::AddColumn {
                column,
                if_not_exists,
            } => {
                if new_data.columns.column_exists(&column.name) {
                    if *if_not_exists {
                        return Ok((
                            new_base,
                            CatalogEntryKind::Generic {
                                sql: new_data.to_sql(&base.clone()),
                            },
                        ));
                    }
                    return Err(CatalogError::already_exists(
                        CatalogType::Invalid,
                        &column.name,
                    ));
                }
                new_data.columns.add_column(column.clone());
            }
            AlterKind::RemoveColumn {
                column_name,
                if_exists,
                cascade: _,
            } => {
                let idx = new_data.columns.logical_index_of(column_name);
                match idx {
                    None if *if_exists => {}
                    None => return Err(CatalogError::not_found(CatalogType::Invalid, column_name)),
                    Some(i) => {
                        new_data.columns.columns.remove(i);
                    }
                }
            }
            AlterKind::AlterColumnType {
                column_name,
                new_type,
                ..
            } => {
                let col = new_data
                    .columns
                    .get_by_name_mut(column_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, column_name))?;
                col.logical_type = new_type.clone();
            }
            AlterKind::SetDefault {
                column_name,
                default_value,
            } => {
                let col = new_data
                    .columns
                    .get_by_name_mut(column_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, column_name))?;
                col.default_value = default_value.clone();
            }
            AlterKind::SetNotNull { column_name } => {
                // 添加 NOT NULL 约束
                if !new_data.constraints.iter().any(
                    |c| matches!(c, ConstraintType::NotNull { column } if column == column_name),
                ) {
                    new_data.constraints.push(ConstraintType::NotNull {
                        column: column_name.clone(),
                    });
                }
            }
            AlterKind::DropNotNull { column_name } => {
                new_data.constraints.retain(
                    |c| !matches!(c, ConstraintType::NotNull { column } if column == column_name),
                );
            }
            AlterKind::SetComment { new_comment } => {
                new_base.comment = new_comment.clone();
            }
            AlterKind::SetColumnComment {
                column_name,
                comment,
            } => {
                let col = new_data
                    .columns
                    .get_by_name_mut(column_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, column_name))?;
                col.comment = comment.clone();
            }
            AlterKind::SetTags { tags } => {
                new_base.tags = tags.clone();
            }
            AlterKind::AddForeignKey { constraint } => {
                new_data.constraints.push(constraint.clone());
            }
            AlterKind::DropConstraint {
                constraint_name, ..
            } => {
                // 简化：按名称匹配约束（C++ 中有更精细的匹配逻辑）
                new_data
                    .constraints
                    .retain(|c| c.to_sql() != *constraint_name);
            }
            _ => {
                return Err(CatalogError::invalid(format!(
                    "Alter kind {:?} is not applicable to tables",
                    info.kind
                )));
            }
        }
        Ok((
            new_base.clone(),
            CatalogEntryKind::Generic {
                sql: new_data.to_sql(&new_base),
            },
        ))
    }
}

// ─── ViewEntryData ─────────────────────────────────────────────────────────────

/// 视图条目专属数据（C++: `ViewCatalogEntry`）。
#[derive(Debug, Clone)]
pub struct ViewEntryData {
    pub query: String,
    pub aliases: Vec<String>,
    pub types: Vec<LogicalType>,
    pub column_names: Vec<String>,
    pub column_comments: Vec<Option<Value>>,
}

impl ViewEntryData {
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            aliases: Vec::new(),
            types: Vec::new(),
            column_names: Vec::new(),
            column_comments: Vec::new(),
        }
    }

    pub fn to_sql(&self, base: &CatalogEntryBase) -> String {
        let schema = if base.schema_name.is_empty() {
            "main"
        } else {
            &base.schema_name
        };
        let mut cols = String::new();
        if !self.column_names.is_empty() {
            cols = format!(" ({})", self.column_names.join(", "));
        }
        format!(
            "CREATE VIEW {}.{}{} AS {};",
            schema, base.name, cols, self.query
        )
    }

    pub fn apply_alter(
        &self,
        base: &CatalogEntryBase,
        info: &AlterInfo,
    ) -> CatalogResult<(CatalogEntryBase, CatalogEntryKind)> {
        let mut new_base = base.clone();
        let new_data = self.clone();
        match &info.kind {
            AlterKind::RenameView { new_name } => {
                new_base.name = new_name.clone();
            }
            AlterKind::SetComment { new_comment } => {
                new_base.comment = new_comment.clone();
            }
            _ => return Err(CatalogError::invalid("Alter kind not applicable to views")),
        }
        Ok((new_base, CatalogEntryKind::View(new_data)))
    }
}

// ─── SequenceEntryData ─────────────────────────────────────────────────────────

/// 序列条目专属数据（C++: `SequenceCatalogEntry`）。
#[derive(Debug, Clone)]
pub struct SequenceEntryData {
    pub usage_count: u64,
    pub counter: i64,
    pub last_value: i64,
    pub increment: i64,
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
}

impl SequenceEntryData {
    pub fn from_create_info(info: &CreateSequenceInfo) -> Self {
        Self {
            usage_count: info.usage_count,
            counter: info.start_value,
            last_value: info.start_value - info.increment,
            increment: info.increment,
            start_value: info.start_value,
            min_value: info.min_value,
            max_value: info.max_value,
            cycle: info.cycle,
        }
    }

    /// 获取当前值（不推进序列）（C++: `CurrentValue`）。
    pub fn current_value(&self) -> i64 {
        self.last_value
    }

    /// 推进序列并返回下一个值（C++: `NextValue`）。
    ///
    /// 返回 None 表示序列已耗尽（不循环）。
    pub fn next_value(&mut self) -> Option<i64> {
        let next = self.counter;
        // 检查溢出
        if self.increment > 0 && next > self.max_value {
            if !self.cycle {
                return None;
            }
            self.counter = self.min_value;
        } else if self.increment < 0 && next < self.min_value {
            if !self.cycle {
                return None;
            }
            self.counter = self.max_value;
        }
        self.last_value = self.counter;
        self.counter = self.counter.saturating_add(self.increment);
        self.usage_count += 1;
        Some(self.last_value)
    }

    /// 回放序列值（从 WAL 恢复）（C++: `ReplayValue`）。
    pub fn replay_value(&mut self, usage_count: u64, last_value: i64) {
        self.usage_count = usage_count;
        self.last_value = last_value;
        self.counter = last_value + self.increment;
    }

    pub fn to_sql(&self, base: &CatalogEntryBase) -> String {
        let schema = if base.schema_name.is_empty() {
            "main"
        } else {
            &base.schema_name
        };
        let cycle_str = if self.cycle { " CYCLE" } else { " NO CYCLE" };
        format!(
            "CREATE SEQUENCE {}.{} INCREMENT BY {} MINVALUE {} MAXVALUE {} START {};",
            schema, base.name, self.increment, self.min_value, self.max_value, self.start_value
        )
    }
}

// ─── IndexEntryData ────────────────────────────────────────────────────────────

/// 索引条目专属数据（C++: `IndexCatalogEntry` / `DuckIndexEntry`）。
#[derive(Debug, Clone)]
pub struct IndexEntryData {
    pub sql: String,
    pub index_type: String,
    pub constraint_type: IndexConstraintType,
    pub column_ids: Vec<usize>,
    pub expressions: Vec<String>,
    pub options: HashMap<String, Value>,
    pub table_name: String,
    pub schema_name: String,
}

impl IndexEntryData {
    pub fn is_unique(&self) -> bool {
        self.constraint_type.is_unique()
    }
    pub fn is_primary(&self) -> bool {
        self.constraint_type.is_primary()
    }

    pub fn to_sql(&self, base: &CatalogEntryBase) -> String {
        if !self.sql.is_empty() {
            return self.sql.clone();
        }
        let unique_str = if self.is_unique() { "UNIQUE " } else { "" };
        let schema = if base.schema_name.is_empty() {
            "main"
        } else {
            &base.schema_name
        };
        format!(
            "CREATE {}INDEX {} ON {}.{} ({});",
            unique_str,
            base.name,
            schema,
            self.table_name,
            self.expressions.join(", ")
        )
    }
}

// ─── TypeEntryData ─────────────────────────────────────────────────────────────

/// 类型条目专属数据（C++: `TypeCatalogEntry`）。
#[derive(Debug, Clone)]
pub struct TypeEntryData {
    pub user_type: LogicalType,
}

impl TypeEntryData {
    pub fn new(user_type: LogicalType) -> Self {
        Self { user_type }
    }

    pub fn to_sql(&self, base: &CatalogEntryBase) -> String {
        let schema = if base.schema_name.is_empty() {
            "main"
        } else {
            &base.schema_name
        };
        format!(
            "CREATE TYPE {}.{} AS {};",
            schema,
            base.name,
            self.user_type.to_sql()
        )
    }
}

// ─── FunctionEntryData ─────────────────────────────────────────────────────────

/// 函数条目专属数据（C++: `FunctionEntry` 及子类）。
#[derive(Debug, Clone)]
pub struct FunctionEntryData {
    /// 函数类型（ScalarFunction / TableFunction 等）。
    pub function_type: CatalogType,
    /// 函数集合（序列化为 SQL）。
    pub overloads: Vec<FunctionOverload>,
    /// 别名（C++: `alias_of`）。
    pub alias_of: String,
    /// 函数描述。
    pub descriptions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FunctionOverload {
    pub return_type: LogicalType,
    pub argument_types: Vec<LogicalType>,
    pub varargs: bool,
    pub description: String,
}

impl FunctionEntryData {
    pub fn new(function_type: CatalogType) -> Self {
        Self {
            function_type,
            overloads: Vec::new(),
            alias_of: String::new(),
            descriptions: Vec::new(),
        }
    }

    pub fn to_sql(&self, base: &CatalogEntryBase) -> String {
        // 函数的 DDL 取决于函数类型；此处仅生成占位 SQL
        let schema = if base.schema_name.is_empty() {
            "main"
        } else {
            &base.schema_name
        };
        format!("-- {} {}.{}", base.entry_type, schema, base.name)
    }
}

// ─── DependencyRelationData ────────────────────────────────────────────────────

/// 依赖关系条目数据（C++: `DependencySubjectEntry` / `DependencyDependentEntry`）。
///
/// 存储于 DependencyManager 的内部 CatalogSet 中，表示一条依赖关系。
#[derive(Debug, Clone)]
pub struct DependencyRelationData {
    /// 被依赖方（Subject）或依赖方（Dependent）的信息。
    pub entry_info: super::dependency::CatalogEntryInfo,
    pub subject_flags: super::dependency::DependencySubjectFlags,
    pub dependent_flags: super::dependency::DependencyDependentFlags,
}

// ─── CatalogEntryNode ──────────────────────────────────────────────────────────

/// MVCC 版本链节点（C++: `CatalogEntry`，含 `child` 旧版本链）。
///
/// - `base`：通用字段（名称、时间戳、删除标志等）。
/// - `kind`：类型专属数据。
/// - `older`：指向旧版本，形成 MVCC 链（C++: `unique_ptr<CatalogEntry> child`）。
#[derive(Debug)]
pub struct CatalogEntryNode {
    pub base: CatalogEntryBase,
    pub kind: CatalogEntryKind,
    /// 旧版本（C++: `unique_ptr<CatalogEntry> child`）。
    pub(crate) older: Option<Box<CatalogEntryNode>>,
    /// 更新此节点的较新版本（C++: `optional_ptr<CatalogEntry> parent`）。
    parent: AtomicUsize,
    /// 所属 CatalogSet（C++: `optional_ptr<CatalogSet> set`）。
    set: AtomicUsize,
}

impl CatalogEntryNode {
    pub fn new(base: CatalogEntryBase, kind: CatalogEntryKind) -> Self {
        Self {
            base,
            kind,
            older: None,
            parent: AtomicUsize::new(0),
            set: AtomicUsize::new(0),
        }
    }

    /// 创建已删除的墓碑节点（C++: 用于 DropEntry 时创建的删除标记）。
    pub fn tombstone(
        oid: u64,
        name: String,
        catalog_name: String,
        schema_name: String,
        entry_type: CatalogType,
    ) -> Self {
        let mut base = CatalogEntryBase::new(oid, entry_type, name, catalog_name, schema_name);
        base.deleted = true;
        Self {
            base,
            kind: CatalogEntryKind::Generic { sql: String::new() },
            older: None,
            parent: AtomicUsize::new(0),
            set: AtomicUsize::new(0),
        }
    }

    /// 生成 SQL 字符串（C++: `virtual string ToSQL() const`）。
    pub fn to_sql(&self) -> String {
        if self.base.deleted {
            return String::new();
        }
        self.kind.to_sql(&self.base)
    }

    /// 获取 CreateInfo（C++: `virtual unique_ptr<CreateInfo> GetInfo() const`）。
    pub fn get_info(&self) -> CreateInfo {
        self.kind.get_create_info(&self.base)
    }

    /// 应用 Alter 操作，返回新版本节点（C++: `virtual AlterEntry`）。
    pub fn alter(&self, info: &AlterInfo) -> CatalogResult<CatalogEntryNode> {
        let (new_base, new_kind) = match &self.kind {
            CatalogEntryKind::Table(entry) => {
                let altered = entry.base.alter_entry(info)?;
                let mut base = self.base.clone();
                let fields = altered.base.fields();
                base.name = fields.name.clone();
                base.comment = fields.comment.clone();
                base.tags = fields.tags.clone();
                let new_entry = DuckTableEntry::new(altered, entry.storage.clone());
                (base, CatalogEntryKind::Table(Arc::new(new_entry)))
            }
            CatalogEntryKind::View(d) => d.apply_alter(&self.base, info)?,
            CatalogEntryKind::Sequence(_) => {
                if let AlterKind::SetComment { new_comment } = &info.kind {
                    let mut b = self.base.clone();
                    b.comment = new_comment.clone();
                    (b, self.kind.clone())
                } else {
                    return Err(CatalogError::invalid("Alter not supported on sequences"));
                }
            }
            _ => {
                return Err(CatalogError::invalid(format!(
                    "Alter not supported on {}",
                    self.base.entry_type
                )));
            }
        };
        Ok(CatalogEntryNode::new(new_base, new_kind))
    }

    /// 克隆节点（不含 older 链）（C++: `virtual Copy`）。
    pub fn copy_current(&self) -> CatalogEntryNode {
        CatalogEntryNode::new(self.base.clone(), self.kind.clone())
    }

    pub fn set_catalog_set(&self, set: *const CatalogSet) {
        self.set.store(set as usize, Ordering::SeqCst);
    }

    pub fn catalog_set(&self) -> Option<&CatalogSet> {
        let ptr = self.set.load(Ordering::SeqCst);
        if ptr == 0 {
            None
        } else {
            // SAFETY: live catalog nodes are stored inside CatalogSet-owned Boxes.
            Some(unsafe { &*(ptr as *const CatalogSet) })
        }
    }

    pub fn set_parent(&self, parent: *mut CatalogEntryNode) {
        self.parent.store(parent as usize, Ordering::SeqCst);
    }

    pub fn clear_parent(&self) {
        self.parent.store(0, Ordering::SeqCst);
    }

    pub fn has_parent(&self) -> bool {
        self.parent.load(Ordering::SeqCst) != 0
    }

    pub fn parent(&self) -> Option<&CatalogEntryNode> {
        let ptr = self.parent.load(Ordering::SeqCst);
        if ptr == 0 {
            None
        } else {
            // SAFETY: the parent pointer always points at the newer version node that owns this child.
            Some(unsafe { &*(ptr as *const CatalogEntryNode) })
        }
    }

    pub fn parent_mut(&mut self) -> Option<&mut CatalogEntryNode> {
        let ptr = self.parent.load(Ordering::SeqCst);
        if ptr == 0 {
            None
        } else {
            // SAFETY: commit/rollback walks the undo buffer serially and the pointed node remains owned by the set.
            Some(unsafe { &mut *(ptr as *mut CatalogEntryNode) })
        }
    }

    pub fn raw_ptr(&self) -> u64 {
        ptr::from_ref(self) as usize as u64
    }

    /// 设置为链表根（最新版本）时的回调（C++: `virtual SetAsRoot`）。
    pub fn set_as_root(&mut self) {
        // 对于 DuckTableEntry，SetAsRoot 会通知 DataTable 切换到新版本。
        // 在 Rust 中，DataTable 的更新通过存储层回调完成，此处记录时间戳即可。
        self.clear_parent();
    }

    /// Drop 时的清理回调（C++: `virtual OnDrop`）。
    pub fn on_drop(&mut self) {
        // 通知存储层表已被删除（具体实现由存储层完成）。
        self.base.deleted = true;
    }
}

// ─── CatalogEntryRef ──────────────────────────────────────────────────────────

/// 对 CatalogEntryNode 的共享引用（用于跨线程/锁共享）。
pub type CatalogEntryRef = Arc<RwLock<CatalogEntryNode>>;

/// 构造 CatalogEntryRef。
pub fn new_entry_ref(node: CatalogEntryNode) -> CatalogEntryRef {
    Arc::new(RwLock::new(node))
}
