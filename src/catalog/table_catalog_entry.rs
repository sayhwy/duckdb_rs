//! `TableCatalogEntry` 的 Rust 实现。
//!
//! 对应 C++:
//!   - `src/include/duckdb/catalog/catalog_entry/table_catalog_entry.hpp`
//!   - `src/catalog/catalog_entry/table_catalog_entry.cpp`
//!
//! # 类层次
//!
//! ```text
//! CatalogEntry
//!   └── InCatalogEntry
//!         └── StandardEntry
//!               └── TableCatalogEntry  ← 本文件
//!                     └── DuckTableEntry  (storage 层实现，见 storage/data_table.rs)
//! ```
//!
//! # 设计说明
//!
//! C++ 中 `TableCatalogEntry` 是一个半抽象类（有纯虚方法 `GetStatistics`,
//! `GetScanFunction`, `GetStorageInfo`）。Rust 使用：
//! - [`TableCatalogEntry`]：包含所有数据字段及非虚方法的具体实现。
//! - [`TableCatalogEntryVirtual`] trait：抽象/虚方法接口，
//!   子类型（如 `DuckTableEntry`）实现此 trait。
//!
//! | C++ 成员                           | Rust 实现                              |
//! |------------------------------------|----------------------------------------|
//! | `StandardEntry` base               | `pub base: StandardEntry`              |
//! | `ColumnList columns`               | `pub columns: ColumnList`              |
//! | `vector<unique_ptr<Constraint>> constraints` | `pub constraints: Vec<ConstraintType>` |
//! | `static CatalogType Type`          | `TableCatalogEntry::CATALOG_TYPE`      |
//! | `static const char* Name`          | `TableCatalogEntry::NAME`              |

use std::collections::HashMap;

use super::standard_entry::StandardEntry;
use super::types::{
    CatalogType, ColumnDefinition, ColumnList, ConstraintType,
    CreateInfo, CreateTableInfo, LogicalType, AlterInfo, AlterKind, Value,
};
use super::error::CatalogError;
use super::dependency::LogicalDependencyList;

// ─── 常量 ──────────────────────────────────────────────────────────────────────

/// rowid 列的虚拟列标识符（C++: `COLUMN_IDENTIFIER_ROW_ID`）。
pub const COLUMN_IDENTIFIER_ROW_ID: u64 = u64::MAX;

// ─── LogicalIndex / PhysicalIndex ──────────────────────────────────────────────

/// 逻辑列索引（C++: `LogicalIndex`）。
///
/// 逻辑列包含生成列；`INVALID_INDEX` 用于"列不存在"的情况。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalIndex(pub usize);

impl LogicalIndex {
    /// 无效索引值，对应 C++ 的 `DConstants::INVALID_INDEX`。
    pub const INVALID: LogicalIndex = LogicalIndex(usize::MAX);

    pub fn is_valid(self) -> bool {
        self.0 != usize::MAX
    }
}

/// 物理列索引（C++: `PhysicalIndex`，不计生成列）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalIndex(pub usize);

impl PhysicalIndex {
    pub const INVALID: PhysicalIndex = PhysicalIndex(usize::MAX);

    pub fn is_valid(self) -> bool {
        self.0 != usize::MAX
    }
}

// ─── ColumnSegmentInfo ─────────────────────────────────────────────────────────

/// 列段信息（C++: `ColumnSegmentInfo`）。
///
/// 用于 `GetColumnSegmentInfo()` 诊断输出，不参与核心存储逻辑。
#[derive(Debug, Clone)]
pub struct ColumnSegmentInfo {
    /// 行组编号。
    pub row_group_index: usize,
    /// 列在行组内的编号。
    pub column_id: usize,
    /// 该段的类型（如 "UNCOMPRESSED", "RLE" 等）。
    pub segment_type: String,
    /// 段内行数。
    pub count: u64,
    /// 该段的压缩统计信息（可选）。
    pub compression_info: String,
}

// ─── IndexInfo / TableStorageInfo ─────────────────────────────────────────────

/// 单个索引的存储信息（C++: `IndexInfo`）。
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// 涉及此索引的物理列集合（C++: `physical_index_set_t column_set`）。
    pub column_set: std::collections::HashSet<usize>,
    /// 索引名称。
    pub index_name: String,
}

/// 表的存储信息（C++: `TableStorageInfo`）。
///
/// 由 `GetStorageInfo()` 返回，用于优化器决策（如 update_is_del_and_insert）。
#[derive(Debug, Clone, Default)]
pub struct TableStorageInfo {
    /// 表上所有索引的信息列表。
    pub index_info: Vec<IndexInfo>,
    /// 表的总行数（估算值）。
    pub estimated_row_count: u64,
}

// ─── VirtualColumn ─────────────────────────────────────────────────────────────

/// 虚拟列描述（C++: `TableColumn` / `virtual_column_map_t`）。
///
/// 虚拟列（如 `rowid`）不存储在物理列中，但可以被查询引用。
#[derive(Debug, Clone)]
pub struct VirtualColumn {
    /// 虚拟列名称（如 `"rowid"`）。
    pub name: String,
    /// 虚拟列的逻辑类型（C++: `LogicalType::ROW_TYPE`，此处用 `BIGINT`）。
    pub logical_type: LogicalType,
}

impl VirtualColumn {
    pub fn new(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self { name: name.into(), logical_type }
    }

    /// rowid 虚拟列（C++: `TableColumn("rowid", LogicalType::ROW_TYPE)`）。
    pub fn rowid() -> Self {
        Self::new("rowid", LogicalType::bigint())
    }
}

/// 虚拟列映射（C++: `virtual_column_map_t`），键为虚拟列 ID。
pub type VirtualColumnMap = HashMap<u64, VirtualColumn>;

// ─── TableCatalogEntry ─────────────────────────────────────────────────────────

/// 表 Catalog 条目（C++: `class TableCatalogEntry : public StandardEntry`）。
///
/// 包含表的列定义与约束，以及所有非虚方法实现。
/// 纯虚方法（`GetStatistics`、`GetScanFunction`、`GetStorageInfo`）
/// 由 [`TableCatalogEntryVirtual`] trait 表达，由 `DuckTableEntry` 等子类实现。
#[derive(Debug, Clone)]
pub struct TableCatalogEntry {
    /// 继承自 `StandardEntry` 的通用字段。
    pub base: StandardEntry,
    /// 表的列定义列表（C++: `ColumnList columns`）。
    pub columns: ColumnList,
    /// 表的约束列表（C++: `vector<unique_ptr<Constraint>> constraints`）。
    pub constraints: Vec<ConstraintType>,
}

impl TableCatalogEntry {
    /// Catalog 类型常量（C++: `static constexpr CatalogType Type = TABLE_ENTRY`）。
    pub const CATALOG_TYPE: CatalogType = CatalogType::TableEntry;

    /// 类型名称字符串（C++: `static constexpr const char* Name = "table"`）。
    pub const NAME: &'static str = "table";

    // ── 构造 ───────────────────────────────────────────────────────────────────

    /// 从 `CreateTableInfo` 构造表条目。
    ///
    /// C++: `TableCatalogEntry(Catalog&, SchemaCatalogEntry&, CreateTableInfo&)`
    ///
    /// 对应 C++ 构造体：
    /// ```cpp
    /// TableCatalogEntry(..., info)
    ///   : StandardEntry(TABLE_ENTRY, schema, catalog, info.table),
    ///     columns(std::move(info.columns)),
    ///     constraints(std::move(info.constraints))
    /// {
    ///     this->temporary = info.temporary;
    ///     this->dependencies = info.dependencies;
    ///     this->comment = info.comment;
    ///     this->tags = info.tags;
    /// }
    /// ```
    pub fn new(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        oid: u64,
        info: CreateTableInfo,
    ) -> Self {
        let mut base = StandardEntry::new(
            Self::CATALOG_TYPE,
            schema_name,
            catalog_name,
            info.table.clone(),
            oid,
        );
        // C++: this->temporary = info.temporary
        base.fields_mut().temporary = info.base.temporary;
        // C++: this->comment = info.comment
        base.fields_mut().comment = info.base.comment.clone();
        // C++: this->tags = info.tags
        base.fields_mut().tags = info.base.tags.clone();
        // C++: this->dependencies = info.dependencies  (通过 StandardEntry::dependencies)
        // dependencies 在 CreateTableInfo 中未单独存储，保持空

        Self {
            base,
            columns: info.columns,
            constraints: info.constraints,
        }
    }

    // ── 列查询 ─────────────────────────────────────────────────────────────────

    /// 是否存在生成列（C++: `bool HasGeneratedColumns() const`）。
    ///
    /// 对应 `columns.LogicalColumnCount() != columns.PhysicalColumnCount()`。
    pub fn has_generated_columns(&self) -> bool {
        self.columns.has_generated_columns()
    }

    /// 检查列名是否存在（C++: `bool ColumnExists(const string& name) const`）。
    pub fn column_exists(&self, name: &str) -> bool {
        self.columns.column_exists(name)
    }

    /// 按名称获取列定义（C++: `const ColumnDefinition& GetColumn(const string& name) const`）。
    ///
    /// 列不存在时返回 `Err`（C++: 抛出 `CatalogException`）。
    pub fn get_column_by_name(&self, name: &str) -> Result<&ColumnDefinition, CatalogError> {
        self.columns
            .get_by_name(name)
            .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, name))
    }

    /// 按逻辑索引获取列定义（C++: `const ColumnDefinition& GetColumn(LogicalIndex idx) const`）。
    pub fn get_column_by_index(&self, idx: LogicalIndex) -> Result<&ColumnDefinition, CatalogError> {
        self.columns
            .get_by_index(idx.0)
            .ok_or_else(|| CatalogError::other(format!("Column index {} out of range", idx.0)))
    }

    /// 返回物理列（非生成列）的类型列表（C++: `vector<LogicalType> GetTypes() const`）。
    ///
    /// 排除生成列，仅返回物理存储的列类型。
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.columns
            .columns
            .iter()
            .filter(|c| !c.is_generated())
            .map(|c| c.logical_type.clone())
            .collect()
    }

    /// 返回完整列列表（C++: `const ColumnList& GetColumns() const`）。
    pub fn get_columns(&self) -> &ColumnList {
        &self.columns
    }

    /// 返回约束列表（C++: `const vector<unique_ptr<Constraint>>& GetConstraints() const`）。
    pub fn get_constraints(&self) -> &[ConstraintType] {
        &self.constraints
    }

    // ── 列索引查找 ─────────────────────────────────────────────────────────────

    /// 按名称查找逻辑列索引（C++: `LogicalIndex GetColumnIndex(string& name, bool if_exists) const`）。
    ///
    /// - `if_exists = false`（默认）：列不存在时返回 `Err`。
    /// - `if_exists = true`：列不存在时返回 `Ok(LogicalIndex::INVALID)`。
    ///
    /// C++ 实现：列不存在时用 `StringUtil::CandidatesErrorMessage` 给出建议。
    /// Rust 实现：列不存在时返回带候选名称的错误信息。
    pub fn get_column_index(
        &self,
        column_name: &str,
        if_exists: bool,
    ) -> Result<LogicalIndex, CatalogError> {
        match self.columns.logical_index_of(column_name) {
            Some(idx) => Ok(LogicalIndex(idx)),
            None if if_exists => Ok(LogicalIndex::INVALID),
            None => {
                // 生成候选列名（C++: StringUtil::CandidatesErrorMessage）
                let candidates = self.find_similar_columns(column_name);
                let hint = if candidates.is_empty() {
                    String::new()
                } else {
                    format!(" Did you mean: {}?", candidates.join(", "))
                };
                Err(CatalogError::other(format!(
                    "Table \"{}\" does not have a column with name \"{}\".{}",
                    self.base.fields().name,
                    column_name,
                    hint
                )))
            }
        }
    }

    /// 查找与给定名称相似的列名（用于错误提示）。
    fn find_similar_columns(&self, name: &str) -> Vec<String> {
        let name_lower = name.to_lowercase();
        self.columns
            .columns
            .iter()
            .filter(|c| {
                let cn = c.name.to_lowercase();
                cn.contains(&name_lower) || name_lower.contains(&cn)
            })
            .map(|c| c.name.clone())
            .take(3)
            .collect()
    }

    // ── 主键 ───────────────────────────────────────────────────────────────────

    /// 返回主键约束的引用（C++: `optional_ptr<Constraint> GetPrimaryKey() const`）。
    ///
    /// 若无主键，返回 `None`（C++: 返回 `nullptr`）。
    pub fn get_primary_key(&self) -> Option<&ConstraintType> {
        self.constraints.iter().find(|c| {
            matches!(c, ConstraintType::Unique { is_primary: true, .. })
        })
    }

    /// 是否存在主键（C++: `bool HasPrimaryKey() const`）。
    pub fn has_primary_key(&self) -> bool {
        self.get_primary_key().is_some()
    }

    // ── 虚拟列 ─────────────────────────────────────────────────────────────────

    /// 返回虚拟列映射（C++: `virtual virtual_column_map_t GetVirtualColumns() const`）。
    ///
    /// 默认实现仅含 `rowid` 虚拟列，对应：
    /// ```cpp
    /// virtual_columns.insert({COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", ROW_TYPE)});
    /// ```
    pub fn get_virtual_columns(&self) -> VirtualColumnMap {
        let mut map = VirtualColumnMap::new();
        map.insert(COLUMN_IDENTIFIER_ROW_ID, VirtualColumn::rowid());
        map
    }

    /// 返回 rowid 列的 column_t 列表（C++: `virtual vector<column_t> GetRowIdColumns() const`）。
    pub fn get_row_id_columns(&self) -> Vec<u64> {
        vec![COLUMN_IDENTIFIER_ROW_ID]
    }

    /// 默认返回空段信息（C++: `virtual vector<ColumnSegmentInfo> GetColumnSegmentInfo(...)`）。
    ///
    /// `DuckTableEntry` 会覆盖此方法以返回真实段信息。
    pub fn get_column_segment_info(&self) -> Vec<ColumnSegmentInfo> {
        Vec::new()
    }

    /// 默认返回 `None`（C++: `virtual unique_ptr<BlockingSample> GetSample()`）。
    pub fn get_sample(&self) -> Option<()> {
        None
    }

    /// 是否为 DuckDB 原生表（C++: `virtual bool IsDuckTable() const { return false; }`）。
    pub fn is_duck_table(&self) -> bool {
        false
    }

    // ── CreateInfo / ToSQL ─────────────────────────────────────────────────────

    /// 获取 `CreateTableInfo`（C++: `unique_ptr<CreateInfo> GetInfo() const override`）。
    ///
    /// 对应 C++ 实现：
    /// ```cpp
    /// result->catalog = catalog.GetName();
    /// result->schema = schema.name;
    /// result->table = name;
    /// result->columns = columns.Copy();
    /// result->constraints = ...;
    /// result->temporary = temporary; ...
    /// ```
    pub fn get_info(&self) -> CreateTableInfo {
        let fields = self.base.fields();
        let mut base_info = CreateInfo::new(CatalogType::TableEntry);
        base_info.catalog = self.base.parent_catalog().to_string();
        base_info.schema  = self.base.parent_schema().to_string();
        base_info.temporary = fields.temporary;
        base_info.internal  = fields.internal;
        base_info.comment   = fields.comment.clone();
        base_info.tags      = fields.tags.clone();

        CreateTableInfo {
            base: base_info,
            table: fields.name.clone(),
            columns: self.columns.clone(),
            constraints: self.constraints.clone(),
        }
    }

    /// 生成 CREATE TABLE SQL 语句（C++: `string ToSQL() const override`）。
    ///
    /// 委托给 `GetInfo().to_sql()`（对应 `create_info->ToString()`）。
    pub fn to_sql(&self) -> String {
        self.get_info().to_sql()
    }

    // ── 静态辅助方法 ──────────────────────────────────────────────────────────

    /// 将列列表和约束生成 `(col1 TYPE, col2 TYPE, CONSTRAINT...)` SQL 片段。
    ///
    /// C++: `static string ColumnsToSQL(const ColumnList&, const vector<unique_ptr<Constraint>>&)`
    ///
    /// 实现逻辑：
    /// 1. 收集 NOT NULL / UNIQUE / PRIMARY KEY 约束到各自集合。
    /// 2. 对每列输出类型、生成表达式/默认值、NOT NULL/PRIMARY KEY/UNIQUE（单列内联）。
    /// 3. 输出多列约束（FOREIGN KEY, CHECK, 多列 PRIMARY KEY / UNIQUE）。
    pub fn columns_to_sql(columns: &ColumnList, constraints: &[ConstraintType]) -> String {
        // 收集各类约束的列集合
        let mut not_null_cols: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut unique_cols:   std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut pk_cols:       std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut multi_pk_names: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut extra_constraints: Vec<String> = Vec::new();

        for c in constraints {
            match c {
                ConstraintType::NotNull { column } => {
                    if let Some(idx) = columns.logical_index_of(column) {
                        not_null_cols.insert(idx);
                    }
                }
                ConstraintType::Unique { columns: cols, is_primary } => {
                    if cols.len() == 1 {
                        // 单列约束 → 内联到列定义
                        if let Some(idx) = columns.logical_index_of(&cols[0]) {
                            if *is_primary { pk_cols.insert(idx); }
                            else           { unique_cols.insert(idx); }
                        }
                    } else {
                        // 多列约束 → 追加到末尾
                        if *is_primary {
                            for col in cols {
                                multi_pk_names.insert(col.clone());
                            }
                        }
                        extra_constraints.push(c.to_sql());
                    }
                }
                ConstraintType::ForeignKey { .. } => {
                    extra_constraints.push(c.to_sql());
                }
                ConstraintType::Check { .. } => {
                    extra_constraints.push(c.to_sql());
                }
            }
        }

        let mut parts: Vec<String> = Vec::new();

        for (idx, col) in columns.columns.iter().enumerate() {
            let mut s = format!("{} {}", quote_identifier(&col.name), col.logical_type.to_sql());

            // 生成列 / 默认值（互斥）
            if let Some(generated) = &col.generated_expression {
                s.push_str(&format!(" GENERATED ALWAYS AS ({}) VIRTUAL", generated));
            } else if let Some(def) = &col.default_value {
                s.push_str(&format!(" DEFAULT({})", def));
            }

            let is_pk       = pk_cols.contains(&idx);
            let is_multi_pk = multi_pk_names.contains(&col.name);
            let is_unique   = unique_cols.contains(&idx);
            let is_not_null = not_null_cols.contains(&idx);

            // NOT NULL：仅当不是主键列时内联输出
            if is_not_null && !is_pk && !is_multi_pk {
                s.push_str(" NOT NULL");
            }
            if is_pk    { s.push_str(" PRIMARY KEY"); }
            if is_unique { s.push_str(" UNIQUE"); }

            parts.push(s);
        }

        // 多列约束追加到末尾
        parts.extend(extra_constraints);

        format!("(\n  {}\n)", parts.join(",\n  "))
    }

    /// 生成列名列表 SQL 片段 `(col1, col2, col3)` 。
    ///
    /// C++: `static string ColumnNamesToSQL(const ColumnList& columns)`
    pub fn column_names_to_sql(columns: &ColumnList) -> String {
        if columns.columns.is_empty() {
            return String::new();
        }
        let names: Vec<String> = columns
            .columns
            .iter()
            .map(|c| quote_identifier(&c.name))
            .collect();
        format!("({})", names.join(", "))
    }

    // ── AlterEntry ────────────────────────────────────────────────────────────

    /// 执行 Alter 操作，返回新版本的 `TableCatalogEntry`。
    ///
    /// C++: `virtual unique_ptr<CatalogEntry> AlterEntry(ClientContext&, AlterInfo&)`
    ///
    /// 所有 Alter 操作产生新条目（MVCC），旧条目不变。
    pub fn alter_entry(&self, info: &AlterInfo) -> Result<TableCatalogEntry, CatalogError> {
        let mut new_entry = self.clone();
        let fields = new_entry.base.fields_mut();

        match &info.kind {
            AlterKind::RenameTable { new_name } => {
                fields.name = new_name.clone();
            }

            AlterKind::RenameColumn { old_name, new_name } => {
                let col = new_entry.columns.get_by_name_mut(old_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, old_name.as_str()))?;
                col.name = new_name.clone();
            }

            AlterKind::AddColumn { column, if_not_exists } => {
                if new_entry.columns.column_exists(&column.name) {
                    if *if_not_exists {
                        return Ok(new_entry);
                    }
                    return Err(CatalogError::already_exists(CatalogType::Invalid, &column.name));
                }
                new_entry.columns.add_column(column.clone());
            }

            AlterKind::RemoveColumn { column_name, if_exists, .. } => {
                match new_entry.columns.logical_index_of(column_name) {
                    None if *if_exists => {}
                    None => return Err(CatalogError::not_found(CatalogType::Invalid, column_name.as_str())),
                    Some(i) => { new_entry.columns.columns.remove(i); }
                }
            }

            AlterKind::AlterColumnType { column_name, new_type, .. } => {
                let col = new_entry.columns.get_by_name_mut(column_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, column_name.as_str()))?;
                col.logical_type = new_type.clone();
            }

            AlterKind::SetDefault { column_name, default_value } => {
                let col = new_entry.columns.get_by_name_mut(column_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, column_name.as_str()))?;
                col.default_value = default_value.clone();
            }

            AlterKind::SetNotNull { column_name } => {
                // 添加 NOT NULL 约束（若不存在）
                let already = new_entry.constraints.iter().any(|c| {
                    matches!(c, ConstraintType::NotNull { column } if column == column_name)
                });
                if !already {
                    new_entry.constraints.push(
                        ConstraintType::NotNull { column: column_name.clone() }
                    );
                }
            }

            AlterKind::DropNotNull { column_name } => {
                new_entry.constraints.retain(|c| {
                    !matches!(c, ConstraintType::NotNull { column } if column == column_name)
                });
            }

            AlterKind::SetComment { new_comment } => {
                new_entry.base.fields_mut().comment = new_comment.clone();
            }

            AlterKind::SetColumnComment { column_name, comment } => {
                let col = new_entry.columns.get_by_name_mut(column_name)
                    .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, column_name.as_str()))?;
                col.comment = comment.clone();
            }

            AlterKind::SetTags { tags } => {
                new_entry.base.fields_mut().tags = tags.clone();
            }

            AlterKind::AddForeignKey { constraint } => {
                new_entry.constraints.push(constraint.clone());
            }

            AlterKind::DropConstraint { constraint_name, .. } => {
                let before = new_entry.constraints.len();
                new_entry.constraints.retain(|c| &c.to_sql() != constraint_name);
                if new_entry.constraints.len() == before {
                    return Err(CatalogError::not_found(CatalogType::Invalid, constraint_name.as_str()));
                }
            }

            other => {
                return Err(CatalogError::invalid(format!(
                    "AlterKind {:?} is not applicable to tables", other
                )));
            }
        }

        Ok(new_entry)
    }

    // ── 获取存储（基类默认实现）──────────────────────────────────────────────

    /// 获取底层存储（C++: `virtual DataTable& GetStorage()`）。
    ///
    /// 基类实现抛出 `InternalException`；`DuckTableEntry` 覆盖此方法。
    pub fn get_storage(&self) -> Result<(), CatalogError> {
        Err(CatalogError::other(
            "Calling GetStorage on a TableCatalogEntry that is not a DuckTableEntry",
        ))
    }
}

// ─── TableCatalogEntryVirtual ──────────────────────────────────────────────────

/// `TableCatalogEntry` 的纯虚/抽象方法接口。
///
/// C++ 中以下方法为纯虚（`= 0`），由具体子类（如 `DuckTableEntry`）实现：
/// - `GetStatistics(ClientContext&, column_t)` → 列统计信息
/// - `GetScanFunction(ClientContext&, ...)` → 扫描函数
/// - `GetStorageInfo(ClientContext&)` → 存储信息
///
/// 此 trait 由 `DuckTableEntry` 等实现。
pub trait TableCatalogEntryVirtual {
    /// 返回指定列的统计信息（C++: `virtual unique_ptr<BaseStatistics> GetStatistics(...) = 0`）。
    ///
    /// `column_id` 对应物理列 ID（或 `COLUMN_IDENTIFIER_ROW_ID`）。
    fn get_statistics(&self, column_id: u64) -> Option<ColumnStatistics>;

    /// 返回表的扫描函数绑定（C++: `virtual TableFunction GetScanFunction(...) = 0`）。
    ///
    /// 返回 `ScanFunctionBinding`，包含函数标识符和绑定数据。
    fn get_scan_function(&self) -> ScanFunctionBinding;

    /// 返回表的存储信息（C++: `virtual TableStorageInfo GetStorageInfo(...) = 0`）。
    fn get_storage_info(&self) -> TableStorageInfo;
}

// ─── ColumnStatistics（简化版）────────────────────────────────────────────────

/// 列统计信息（C++: `BaseStatistics`）。
///
/// 此处仅保留 catalog 层所需的最小字段。
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// 是否包含 NULL 值。
    pub has_null: bool,
    /// 是否包含非 NULL 值。
    pub has_no_null: bool,
    /// 非空行数（估算）。
    pub distinct_count: u64,
}

// ─── ScanFunctionBinding ──────────────────────────────────────────────────────

/// 扫描函数绑定（C++: `TableFunction` + `unique_ptr<FunctionData>`）。
///
/// 持有扫描函数的标识符，实际函数对象由执行引擎解析。
#[derive(Debug, Clone)]
pub struct ScanFunctionBinding {
    /// 扫描函数名称（如 `"seq_scan"`, `"arrow_scan"`）。
    pub function_name: String,
    /// 绑定参数（序列化形式，由执行引擎解析）。
    pub bind_data: Vec<u8>,
}

// ─── 辅助函数 ──────────────────────────────────────────────────────────────────

/// 必要时为标识符添加双引号（C++: `KeywordHelper::WriteOptionallyQuoted`）。
///
/// 如果名称包含特殊字符或是保留字的简单检测：含大写字母、空格或特殊符号则加引号。
fn quote_identifier(name: &str) -> String {
    let needs_quote = name.chars().any(|c| !c.is_alphanumeric() && c != '_')
        || name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false)
        || is_reserved_keyword(name);
    if needs_quote {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// 简单保留关键字检测（C++: `KeywordHelper` 的子集）。
fn is_reserved_keyword(word: &str) -> bool {
    const RESERVED: &[&str] = &[
        "select", "from", "where", "table", "index", "view", "schema",
        "create", "drop", "alter", "insert", "update", "delete",
        "primary", "key", "unique", "foreign", "references", "check",
        "not", "null", "default", "constraint", "column", "order",
        "group", "by", "having", "limit", "offset", "join", "on",
        "as", "and", "or", "in", "between", "like", "is", "case",
        "when", "then", "else", "end", "union", "all", "distinct",
    ];
    RESERVED.contains(&word.to_lowercase().as_str())
}
