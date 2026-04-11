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

use crate::common::errors::CatalogResult;

use crate::catalog::dependency::LogicalDependencyList;
use crate::catalog::error::CatalogError;
use crate::catalog::standard_entry::StandardEntry;
use crate::catalog::types::{
    AlterInfo, AlterKind, CatalogType, ColumnDefinition, ColumnList, ConstraintType, CreateInfo,
    CreateTableInfo, LogicalType, Value,
};

pub const COLUMN_IDENTIFIER_ROW_ID: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalIndex(pub usize);

impl LogicalIndex {
    pub const INVALID: LogicalIndex = LogicalIndex(usize::MAX);

    pub fn is_valid(self) -> bool {
        self.0 != usize::MAX
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PhysicalIndex(pub usize);

impl PhysicalIndex {
    pub const INVALID: PhysicalIndex = PhysicalIndex(usize::MAX);

    pub fn is_valid(self) -> bool {
        self.0 != usize::MAX
    }
}

#[derive(Debug, Clone)]
pub struct ColumnSegmentInfo {
    pub row_group_index: usize,
    pub column_id: usize,
    pub segment_type: String,
    pub count: u64,
    pub compression_info: String,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub column_set: std::collections::HashSet<usize>,
    pub index_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct TableStorageInfo {
    pub index_info: Vec<IndexInfo>,
    pub estimated_row_count: u64,
}

#[derive(Debug, Clone)]
pub struct VirtualColumn {
    pub name: String,
    pub logical_type: LogicalType,
}

impl VirtualColumn {
    pub fn new(name: impl Into<String>, logical_type: LogicalType) -> Self {
        Self {
            name: name.into(),
            logical_type,
        }
    }

    pub fn rowid() -> Self {
        Self::new("rowid", LogicalType::bigint())
    }
}

pub type VirtualColumnMap = HashMap<u64, VirtualColumn>;

#[derive(Debug, Clone)]
pub struct TableCatalogEntry {
    pub base: StandardEntry,
    pub columns: ColumnList,
    pub constraints: Vec<ConstraintType>,
}

impl TableCatalogEntry {
    pub const CATALOG_TYPE: CatalogType = CatalogType::TableEntry;
    pub const NAME: &'static str = "table";

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
        base.fields_mut().temporary = info.base.temporary;
        base.fields_mut().comment = info.base.comment.clone();
        base.fields_mut().tags = info.base.tags.clone();

        Self {
            base,
            columns: info.columns,
            constraints: info.constraints,
        }
    }

    pub fn has_generated_columns(&self) -> bool {
        self.columns.has_generated_columns()
    }

    pub fn column_exists(&self, name: &str) -> bool {
        self.columns.column_exists(name)
    }

    pub fn get_column_by_name(&self, name: &str) -> CatalogResult<&ColumnDefinition> {
        self.columns
            .get_by_name(name)
            .ok_or_else(|| CatalogError::not_found(CatalogType::Invalid, name))
    }

    pub fn get_column_by_index(&self, idx: LogicalIndex) -> CatalogResult<&ColumnDefinition> {
        self.columns
            .get_by_index(idx.0)
            .ok_or_else(|| CatalogError::other(format!("Column index {} out of range", idx.0)))
    }

    pub fn get_types(&self) -> Vec<LogicalType> {
        self.columns
            .columns
            .iter()
            .filter(|c| !c.is_generated())
            .map(|c| c.logical_type.clone())
            .collect()
    }

    pub fn get_columns(&self) -> &ColumnList {
        &self.columns
    }

    pub fn get_constraints(&self) -> &[ConstraintType] {
        &self.constraints
    }

    pub fn get_column_index(
        &self,
        column_name: &str,
        if_exists: bool,
    ) -> CatalogResult<LogicalIndex> {
        match self.columns.logical_index_of(column_name) {
            Some(idx) => Ok(LogicalIndex(idx)),
            None if if_exists => Ok(LogicalIndex::INVALID),
            None => {
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

    pub fn get_primary_key(&self) -> Option<&ConstraintType> {
        self.constraints.iter().find(|c| {
            matches!(
                c,
                ConstraintType::Unique {
                    is_primary: true,
                    ..
                }
            )
        })
    }

    pub fn has_primary_key(&self) -> bool {
        self.get_primary_key().is_some()
    }

    pub fn get_virtual_columns(&self) -> VirtualColumnMap {
        let mut map = VirtualColumnMap::new();
        map.insert(COLUMN_IDENTIFIER_ROW_ID, VirtualColumn::rowid());
        map
    }

    pub fn get_row_id_columns(&self) -> Vec<u64> {
        vec![COLUMN_IDENTIFIER_ROW_ID]
    }

    pub fn get_column_segment_info(&self) -> Vec<ColumnSegmentInfo> {
        Vec::new()
    }

    pub fn get_sample(&self) -> Option<()> {
        None
    }

    pub fn is_duck_table(&self) -> bool {
        false
    }

    pub fn get_info(&self) -> CreateTableInfo {
        let fields = self.base.fields();
        let mut base_info = CreateInfo::new(CatalogType::TableEntry);
        base_info.catalog = self.base.parent_catalog().to_string();
        base_info.schema = self.base.parent_schema().to_string();
        base_info.temporary = fields.temporary;
        base_info.internal = fields.internal;
        base_info.comment = fields.comment.clone();
        base_info.tags = fields.tags.clone();

        CreateTableInfo {
            base: base_info,
            table: fields.name.clone(),
            columns: self.columns.clone(),
            constraints: self.constraints.clone(),
        }
    }

    pub fn to_sql(&self) -> String {
        self.get_info().to_sql()
    }

    pub fn columns_to_sql(columns: &ColumnList, constraints: &[ConstraintType]) -> String {
        let mut not_null_cols: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut unique_cols: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut pk_cols: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut multi_pk_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut extra_constraints: Vec<String> = Vec::new();

        for c in constraints {
            match c {
                ConstraintType::NotNull { column } => {
                    if let Some(idx) = columns.logical_index_of(column) {
                        not_null_cols.insert(idx);
                    }
                }
                ConstraintType::Unique {
                    columns: cols,
                    is_primary,
                } => {
                    if cols.len() == 1 {
                        if let Some(idx) = columns.logical_index_of(&cols[0]) {
                            if *is_primary {
                                pk_cols.insert(idx);
                            } else {
                                unique_cols.insert(idx);
                            }
                        }
                    } else {
                        if *is_primary {
                            for col in cols {
                                multi_pk_names.insert(col.clone());
                            }
                        }
                        extra_constraints.push(c.to_sql());
                    }
                }
                ConstraintType::ForeignKey { .. } => extra_constraints.push(c.to_sql()),
                ConstraintType::Check { .. } => extra_constraints.push(c.to_sql()),
            }
        }

        let mut parts: Vec<String> = Vec::new();

        for (idx, col) in columns.columns.iter().enumerate() {
            let mut s = format!(
                "{} {}",
                quote_identifier(&col.name),
                col.logical_type.to_sql()
            );

            if let Some(generated) = &col.generated_expression {
                s.push_str(&format!(" GENERATED ALWAYS AS ({}) VIRTUAL", generated));
            } else if let Some(def) = &col.default_value {
                s.push_str(&format!(" DEFAULT({})", def));
            }

            let is_pk = pk_cols.contains(&idx);
            let is_multi_pk = multi_pk_names.contains(&col.name);
            let is_unique = unique_cols.contains(&idx);
            let is_not_null = not_null_cols.contains(&idx);

            if is_not_null && !is_pk && !is_multi_pk {
                s.push_str(" NOT NULL");
            }
            if is_pk {
                s.push_str(" PRIMARY KEY");
            }
            if is_unique {
                s.push_str(" UNIQUE");
            }

            parts.push(s);
        }

        parts.extend(extra_constraints);
        format!("(\n  {}\n)", parts.join(",\n  "))
    }

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

    pub fn alter_entry(&self, info: &AlterInfo) -> CatalogResult<TableCatalogEntry> {
        let mut new_entry = self.clone();
        let fields = new_entry.base.fields_mut();

        match &info.kind {
            AlterKind::RenameTable { new_name } => {
                fields.name = new_name.clone();
            }
            AlterKind::RenameColumn { old_name, new_name } => {
                let col = new_entry.columns.get_by_name_mut(old_name).ok_or_else(|| {
                    CatalogError::not_found(CatalogType::Invalid, old_name.as_str())
                })?;
                col.name = new_name.clone();
            }
            AlterKind::AddColumn {
                column,
                if_not_exists,
            } => {
                if new_entry.columns.column_exists(&column.name) {
                    if *if_not_exists {
                        return Ok(new_entry);
                    }
                    return Err(CatalogError::already_exists(CatalogType::Invalid, &column.name));
                }
                new_entry.columns.add_column(column.clone());
            }
            AlterKind::RemoveColumn {
                column_name,
                if_exists,
                ..
            } => match new_entry.columns.logical_index_of(column_name) {
                None if *if_exists => {}
                None => {
                    return Err(CatalogError::not_found(
                        CatalogType::Invalid,
                        column_name.as_str(),
                    ));
                }
                Some(i) => {
                    new_entry.columns.columns.remove(i);
                }
            },
            AlterKind::AlterColumnType {
                column_name,
                new_type,
                ..
            } => {
                let col = new_entry.columns.get_by_name_mut(column_name).ok_or_else(|| {
                    CatalogError::not_found(CatalogType::Invalid, column_name.as_str())
                })?;
                col.logical_type = new_type.clone();
            }
            AlterKind::SetDefault {
                column_name,
                default_value,
            } => {
                let col = new_entry.columns.get_by_name_mut(column_name).ok_or_else(|| {
                    CatalogError::not_found(CatalogType::Invalid, column_name.as_str())
                })?;
                col.default_value = default_value.clone();
            }
            AlterKind::SetNotNull { column_name } => {
                if !new_entry.constraints.iter().any(
                    |c| matches!(c, ConstraintType::NotNull { column } if column == column_name),
                ) {
                    new_entry.constraints.push(ConstraintType::NotNull {
                        column: column_name.clone(),
                    });
                }
            }
            AlterKind::DropNotNull { column_name } => {
                new_entry.constraints.retain(
                    |c| !matches!(c, ConstraintType::NotNull { column } if column == column_name),
                );
            }
            AlterKind::SetComment { new_comment } => {
                fields.comment = new_comment.clone();
            }
            AlterKind::SetColumnComment {
                column_name,
                comment,
            } => {
                let col = new_entry.columns.get_by_name_mut(column_name).ok_or_else(|| {
                    CatalogError::not_found(CatalogType::Invalid, column_name.as_str())
                })?;
                col.comment = comment.clone();
            }
            AlterKind::SetTags { tags } => {
                fields.tags = tags.clone();
            }
            AlterKind::AddForeignKey { constraint } => {
                new_entry.constraints.push(constraint.clone());
            }
            AlterKind::DropConstraint {
                constraint_name, ..
            } => {
                new_entry
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
        Ok(new_entry)
    }

    pub fn get_storage_info(&self) -> CatalogResult<TableStorageInfo> {
        Err(CatalogError::other(
            "Calling GetStorageInfo on a TableCatalogEntry that is not a DuckTableEntry",
        ))
    }

    pub fn bind_update_constraints(&self) -> CatalogResult<()> {
        Ok(())
    }
}

pub trait TableCatalogEntryVirtual {
    fn get_statistics(&self, _column_id: u64) -> CatalogResult<Option<Value>> {
        Ok(None)
    }

    fn get_scan_function(&self) -> CatalogResult<ScanFunctionBinding> {
        Err(CatalogError::other(
            "Calling GetScanFunction on a TableCatalogEntry that is not a DuckTableEntry",
        ))
    }

    fn get_storage_info(&self) -> CatalogResult<TableStorageInfo> {
        Err(CatalogError::other(
            "Calling GetStorageInfo on a TableCatalogEntry that is not a DuckTableEntry",
        ))
    }
}

#[derive(Debug, Clone)]
pub struct ScanFunctionBinding {
    pub function_name: String,
    pub bind_data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    pub has_null: bool,
    pub min: Option<Value>,
    pub max: Option<Value>,
}

fn quote_identifier(s: &str) -> String {
    if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('\"', "\"\""))
    }
}
