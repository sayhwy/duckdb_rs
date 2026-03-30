//! Catalog 基础类型定义。
//!
//! 对应 C++:
//!   - `duckdb/common/enums/catalog_type.hpp`
//!   - `duckdb/common/enums/on_entry_not_found.hpp`
//!   - `duckdb/common/enums/on_create_conflict.hpp`
//!   - `duckdb/common/enums/catalog_lookup_behavior.hpp`
//!   - `duckdb/common/types/value.hpp`（简化版）
//!   - `duckdb/parser/parsed_data/create_info.hpp`（各 CreateInfo 子类）

use std::collections::HashMap;
use std::fmt;

// ─── CatalogType ───────────────────────────────────────────────────────────────

/// Catalog 条目类型（C++: `enum class CatalogType : uint8_t`）。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CatalogType {
    Invalid = 0,
    TableEntry = 1,
    SchemaEntry = 2,
    ViewEntry = 5,
    IndexEntry = 6,
    TableFunctionEntry = 7,
    ScalarFunctionEntry = 8,
    AggregateFunctionEntry = 9,
    CollateCatalogEntry = 10,
    CopyFunctionEntry = 11,
    PragmaFunctionEntry = 12,
    MacroEntry = 13,
    SequenceEntry = 15,
    TypeEntry = 18,
    DatabaseEntry = 19,
    DependencyEntry = 20,
    TableMacroEntry = 24,
}

impl CatalogType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::TableEntry,
            2 => Self::SchemaEntry,
            5 => Self::ViewEntry,
            6 => Self::IndexEntry,
            7 => Self::TableFunctionEntry,
            8 => Self::ScalarFunctionEntry,
            9 => Self::AggregateFunctionEntry,
            10 => Self::CollateCatalogEntry,
            11 => Self::CopyFunctionEntry,
            12 => Self::PragmaFunctionEntry,
            13 => Self::MacroEntry,
            15 => Self::SequenceEntry,
            18 => Self::TypeEntry,
            19 => Self::DatabaseEntry,
            20 => Self::DependencyEntry,
            24 => Self::TableMacroEntry,
            _ => Self::Invalid,
        }
    }
}

impl fmt::Display for CatalogType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CatalogType::TableEntry => "table",
            CatalogType::SchemaEntry => "schema",
            CatalogType::ViewEntry => "view",
            CatalogType::IndexEntry => "index",
            CatalogType::TableFunctionEntry => "table function",
            CatalogType::ScalarFunctionEntry => "scalar function",
            CatalogType::AggregateFunctionEntry => "aggregate function",
            CatalogType::SequenceEntry => "sequence",
            CatalogType::TypeEntry => "type",
            CatalogType::DatabaseEntry => "database",
            CatalogType::MacroEntry => "macro",
            CatalogType::TableMacroEntry => "table macro",
            CatalogType::CollateCatalogEntry => "collation",
            CatalogType::CopyFunctionEntry => "copy function",
            CatalogType::PragmaFunctionEntry => "pragma function",
            CatalogType::DependencyEntry => "dependency",
            CatalogType::Invalid => "invalid",
        };
        write!(f, "{}", s)
    }
}

// ─── Behavior enums ────────────────────────────────────────────────────────────

/// 条目未找到时的行为（C++: `enum class OnEntryNotFound`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnEntryNotFound {
    /// 找不到时返回错误（C++: `THROW_EXCEPTION`）。
    ThrowException,
    /// 找不到时返回 None（C++: `RETURN_NULL`）。
    ReturnNull,
}

/// 条目已存在时的冲突处理（C++: `enum class OnCreateConflict`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnCreateConflict {
    /// 已存在则报错（C++: `ERROR_ON_CONFLICT`）。
    ErrorOnConflict,
    /// 已存在则忽略（C++: `IGNORE_ON_CONFLICT`）。
    IgnoreOnConflict,
    /// 已存在则替换（C++: `REPLACE_ON_CONFLICT`）。
    ReplaceOnConflict,
    /// 修改已有条目（C++: `ALTER_ON_CONFLICT`）。
    AlterOnConflict,
}

/// Catalog 查找行为（C++: `enum class CatalogLookupBehavior`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogLookupBehavior {
    Standard,
    LowerPriority,
    NeverLookup,
}

/// 索引约束类型（C++: `enum class IndexConstraintType : uint8_t`）。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexConstraintType {
    None = 0,
    Unique = 1,
    Primary = 2,
    Foreign = 3,
}

impl IndexConstraintType {
    pub fn is_unique(&self) -> bool {
        matches!(self, Self::Unique | Self::Primary)
    }
    pub fn is_primary(&self) -> bool {
        matches!(self, Self::Primary)
    }
}

// ─── Value ─────────────────────────────────────────────────────────────────────

/// 简化值类型，用于 catalog 条目的注释和属性（C++: `class Value`）。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Value::Text(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        if let Value::Integer(n) = self {
            Some(*n)
        } else {
            None
        }
    }

    pub fn to_sql_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Boolean(b) => {
                if *b {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            Value::Integer(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Blob(b) => format!(
                "decode('{}')",
                b.iter().map(|x| format!("{:02x}", x)).collect::<String>()
            ),
        }
    }

    /// 序列化为字节（用于 WAL / checkpoint）。
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Value::Null => buf.push(0),
            Value::Boolean(b) => {
                buf.push(1);
                buf.push(*b as u8);
            }
            Value::Integer(n) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(3);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::Text(s) => {
                buf.push(4);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Blob(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
        buf
    }

    /// 从字节反序列化。
    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.is_empty() {
            return None;
        }
        match data[0] {
            0 => Some((Value::Null, 1)),
            1 => {
                if data.len() < 2 {
                    return None;
                }
                Some((Value::Boolean(data[1] != 0), 2))
            }
            2 => {
                if data.len() < 9 {
                    return None;
                }
                let n = i64::from_le_bytes(data[1..9].try_into().ok()?);
                Some((Value::Integer(n), 9))
            }
            3 => {
                if data.len() < 9 {
                    return None;
                }
                let f = f64::from_le_bytes(data[1..9].try_into().ok()?);
                Some((Value::Float(f), 9))
            }
            4 => {
                if data.len() < 5 {
                    return None;
                }
                let len = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
                if data.len() < 5 + len {
                    return None;
                }
                let s = String::from_utf8(data[5..5 + len].to_vec()).ok()?;
                Some((Value::Text(s), 5 + len))
            }
            5 => {
                if data.len() < 5 {
                    return None;
                }
                let len = u32::from_le_bytes(data[1..5].try_into().ok()?) as usize;
                if data.len() < 5 + len {
                    return None;
                }
                Some((Value::Blob(data[5..5 + len].to_vec()), 5 + len))
            }
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(n) => write!(f, "{}", n),
            Value::Float(fv) => write!(f, "{}", fv),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "<blob {} bytes>", b.len()),
        }
    }
}

// ─── LogicalType ───────────────────────────────────────────────────────────────

/// 逻辑类型标识（C++: `LogicalTypeId`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalTypeId {
    Invalid,
    SqlNull,
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    HugeInt,
    UHugeInt,
    UTinyInt,
    USmallInt,
    UInteger,
    UBigInt,
    Float,
    Double,
    Timestamp,
    TimestampS,
    TimestampMs,
    TimestampNs,
    Date,
    Time,
    Interval,
    Decimal,
    Varchar,
    Blob,
    List,
    Struct,
    Map,
    Union,
    Enum,
    UserType(String),
    Any,
    Json,
    Uuid,
    Validity, // Added for NULL bitmask columns
}

impl fmt::Display for LogicalTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalTypeId::Integer => write!(f, "INTEGER"),
            LogicalTypeId::BigInt => write!(f, "BIGINT"),
            LogicalTypeId::Boolean => write!(f, "BOOLEAN"),
            LogicalTypeId::Varchar => write!(f, "VARCHAR"),
            LogicalTypeId::Double => write!(f, "DOUBLE"),
            LogicalTypeId::Float => write!(f, "FLOAT"),
            LogicalTypeId::Timestamp => write!(f, "TIMESTAMP"),
            LogicalTypeId::Date => write!(f, "DATE"),
            LogicalTypeId::Time => write!(f, "TIME"),
            LogicalTypeId::Blob => write!(f, "BLOB"),
            LogicalTypeId::Decimal => write!(f, "DECIMAL"),
            LogicalTypeId::Uuid => write!(f, "UUID"),
            LogicalTypeId::Json => write!(f, "JSON"),
            LogicalTypeId::UserType(s) => write!(f, "{}", s),
            other => write!(f, "{:?}", other),
        }
    }
}

/// 逻辑类型（C++: `LogicalType`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalType {
    pub id: LogicalTypeId,
    /// 精度（用于 DECIMAL(p,s)）。
    pub width: u8,
    /// 小数位（用于 DECIMAL(p,s)）。
    pub scale: u8,
    /// 子类型（用于 LIST<T>, MAP<K,V>, STRUCT(...)）。
    pub children: Vec<(String, Box<LogicalType>)>,
}

impl LogicalType {
    pub fn new(id: LogicalTypeId) -> Self {
        Self {
            id,
            width: 0,
            scale: 0,
            children: Vec::new(),
        }
    }

    pub fn varchar() -> Self {
        Self::new(LogicalTypeId::Varchar)
    }
    pub fn integer() -> Self {
        Self::new(LogicalTypeId::Integer)
    }
    pub fn bigint() -> Self {
        Self::new(LogicalTypeId::BigInt)
    }
    pub fn boolean() -> Self {
        Self::new(LogicalTypeId::Boolean)
    }
    pub fn double() -> Self {
        Self::new(LogicalTypeId::Double)
    }
    pub fn timestamp() -> Self {
        Self::new(LogicalTypeId::Timestamp)
    }
    pub fn blob() -> Self {
        Self::new(LogicalTypeId::Blob)
    }

    pub fn decimal(width: u8, scale: u8) -> Self {
        Self {
            id: LogicalTypeId::Decimal,
            width,
            scale,
            children: Vec::new(),
        }
    }

    pub fn list(child: LogicalType) -> Self {
        Self {
            id: LogicalTypeId::List,
            width: 0,
            scale: 0,
            children: vec![("".to_string(), Box::new(child))],
        }
    }

    pub fn to_sql(&self) -> String {
        match &self.id {
            LogicalTypeId::Decimal => format!("DECIMAL({},{})", self.width, self.scale),
            LogicalTypeId::UserType(n) => n.clone(),
            LogicalTypeId::List => {
                let inner = self
                    .children
                    .first()
                    .map(|(_, t)| t.to_sql())
                    .unwrap_or_else(|| "ANY".to_string());
                format!("{}[]", inner)
            }
            other => other.to_string(),
        }
    }

    /// 序列化（仅用于 catalog 持久化，不依赖外部 serde）。
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let tag = match &self.id {
            LogicalTypeId::Invalid => 0u8,
            LogicalTypeId::Boolean => 1,
            LogicalTypeId::TinyInt => 2,
            LogicalTypeId::SmallInt => 3,
            LogicalTypeId::Integer => 4,
            LogicalTypeId::BigInt => 5,
            LogicalTypeId::Float => 6,
            LogicalTypeId::Double => 7,
            LogicalTypeId::Varchar => 8,
            LogicalTypeId::Blob => 9,
            LogicalTypeId::Timestamp => 10,
            LogicalTypeId::Date => 11,
            LogicalTypeId::Time => 12,
            LogicalTypeId::Decimal => 13,
            LogicalTypeId::List => 14,
            LogicalTypeId::Struct => 15,
            LogicalTypeId::Map => 16,
            LogicalTypeId::Uuid => 17,
            LogicalTypeId::Json => 18,
            LogicalTypeId::UserType(_) => 19,
            _ => 255,
        };
        buf.push(tag);
        match &self.id {
            LogicalTypeId::Decimal => {
                buf.push(self.width);
                buf.push(self.scale);
            }
            LogicalTypeId::UserType(name) => {
                let bytes = name.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            LogicalTypeId::List | LogicalTypeId::Struct | LogicalTypeId::Map => {
                buf.extend_from_slice(&(self.children.len() as u32).to_le_bytes());
                for (name, child) in &self.children {
                    let nb = name.as_bytes();
                    buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(nb);
                    let cb = child.serialize();
                    buf.extend_from_slice(&(cb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&cb);
                }
            }
            _ => {}
        }
        buf
    }

    /// 从字节反序列化，返回 (type, consumed_bytes)。
    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.is_empty() {
            return None;
        }
        let tag = data[0];
        let mut pos = 1usize;
        let id = match tag {
            0 => LogicalTypeId::Invalid,
            1 => LogicalTypeId::Boolean,
            2 => LogicalTypeId::TinyInt,
            3 => LogicalTypeId::SmallInt,
            4 => LogicalTypeId::Integer,
            5 => LogicalTypeId::BigInt,
            6 => LogicalTypeId::Float,
            7 => LogicalTypeId::Double,
            8 => LogicalTypeId::Varchar,
            9 => LogicalTypeId::Blob,
            10 => LogicalTypeId::Timestamp,
            11 => LogicalTypeId::Date,
            12 => LogicalTypeId::Time,
            13 => {
                if data.len() < pos + 2 {
                    return None;
                }
                let w = data[pos];
                let s = data[pos + 1];
                pos += 2;
                let mut t = LogicalType::decimal(w, s);
                return Some((t, pos));
            }
            17 => LogicalTypeId::Uuid,
            18 => LogicalTypeId::Json,
            19 => {
                if data.len() < pos + 4 {
                    return None;
                }
                let len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                if data.len() < pos + len {
                    return None;
                }
                let name = String::from_utf8(data[pos..pos + len].to_vec()).ok()?;
                pos += len;
                return Some((LogicalType::new(LogicalTypeId::UserType(name)), pos));
            }
            14 | 15 | 16 => {
                let list_id = match tag {
                    14 => LogicalTypeId::List,
                    15 => LogicalTypeId::Struct,
                    _ => LogicalTypeId::Map,
                };
                if data.len() < pos + 4 {
                    return None;
                }
                let count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                pos += 4;
                let mut children = Vec::new();
                for _ in 0..count {
                    if data.len() < pos + 4 {
                        return None;
                    }
                    let nlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if data.len() < pos + nlen {
                        return None;
                    }
                    let name = String::from_utf8(data[pos..pos + nlen].to_vec()).ok()?;
                    pos += nlen;
                    if data.len() < pos + 4 {
                        return None;
                    }
                    let clen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
                    pos += 4;
                    if data.len() < pos + clen {
                        return None;
                    }
                    let (child, _) = LogicalType::deserialize(&data[pos..pos + clen])?;
                    pos += clen;
                    children.push((name, Box::new(child)));
                }
                return Some((
                    LogicalType {
                        id: list_id,
                        width: 0,
                        scale: 0,
                        children,
                    },
                    pos,
                ));
            }
            _ => LogicalTypeId::Invalid,
        };
        Some((LogicalType::new(id), pos))
    }
}

// ─── ColumnDefinition ──────────────────────────────────────────────────────────

/// 列定义（C++: `ColumnDefinition`）。
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub logical_type: LogicalType,
    /// 默认值表达式（SQL 字符串）。
    pub default_value: Option<String>,
    /// 生成列表达式（SQL 字符串；存在则为生成列）。
    pub generated_expression: Option<String>,
    pub comment: Option<Value>,
}

impl ColumnDefinition {
    pub fn new(name: String, logical_type: LogicalType) -> Self {
        Self {
            name,
            logical_type,
            default_value: None,
            generated_expression: None,
            comment: None,
        }
    }

    pub fn is_generated(&self) -> bool {
        self.generated_expression.is_some()
    }

    pub fn to_sql(&self) -> String {
        let mut s = format!("{} {}", self.name, self.logical_type.to_sql());
        if let Some(d) = &self.default_value {
            s.push_str(&format!(" DEFAULT {}", d));
        }
        if let Some(g) = &self.generated_expression {
            s.push_str(&format!(" GENERATED ALWAYS AS ({}) VIRTUAL", g));
        }
        s
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let nb = self.name.as_bytes();
        buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        buf.extend_from_slice(nb);
        let tb = self.logical_type.serialize();
        buf.extend_from_slice(&(tb.len() as u32).to_le_bytes());
        buf.extend_from_slice(&tb);
        // default_value
        match &self.default_value {
            None => buf.push(0),
            Some(d) => {
                buf.push(1);
                let db = d.as_bytes();
                buf.extend_from_slice(&(db.len() as u32).to_le_bytes());
                buf.extend_from_slice(db);
            }
        }
        // generated_expression
        match &self.generated_expression {
            None => buf.push(0),
            Some(g) => {
                buf.push(1);
                let gb = g.as_bytes();
                buf.extend_from_slice(&(gb.len() as u32).to_le_bytes());
                buf.extend_from_slice(gb);
            }
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        let mut pos = 0;
        let nlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        let name = String::from_utf8(data[pos..pos + nlen].to_vec()).ok()?;
        pos += nlen;
        let tlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        let (logical_type, _) = LogicalType::deserialize(&data[pos..pos + tlen])?;
        pos += tlen;
        let default_value = if data[pos] == 0 {
            pos += 1;
            None
        } else {
            pos += 1;
            let dlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            let s = String::from_utf8(data[pos..pos + dlen].to_vec()).ok()?;
            pos += dlen;
            Some(s)
        };
        let generated_expression = if data[pos] == 0 {
            pos += 1;
            None
        } else {
            pos += 1;
            let glen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            let s = String::from_utf8(data[pos..pos + glen].to_vec()).ok()?;
            pos += glen;
            Some(s)
        };
        Some((
            Self {
                name,
                logical_type,
                default_value,
                generated_expression,
                comment: None,
            },
            pos,
        ))
    }
}

// ─── ColumnList ─────────────────────────────────────────────────────────────────

/// 列列表（C++: `ColumnList`）。
#[derive(Debug, Clone, Default)]
pub struct ColumnList {
    pub columns: Vec<ColumnDefinition>,
}

impl ColumnList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_column(&mut self, col: ColumnDefinition) {
        self.columns.push(col);
    }

    pub fn get_by_name(&self, name: &str) -> Option<&ColumnDefinition> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn get_by_name_mut(&mut self, name: &str) -> Option<&mut ColumnDefinition> {
        self.columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn get_by_index(&self, idx: usize) -> Option<&ColumnDefinition> {
        self.columns.get(idx)
    }

    pub fn logical_index_of(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn has_generated_columns(&self) -> bool {
        self.columns.iter().any(|c| c.is_generated())
    }

    pub fn column_exists(&self, name: &str) -> bool {
        self.get_by_name(name).is_some()
    }

    pub fn logical_types(&self) -> Vec<LogicalType> {
        self.columns
            .iter()
            .map(|c| c.logical_type.clone())
            .collect()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.columns.len() as u32).to_le_bytes());
        for col in &self.columns {
            let cb = col.serialize();
            buf.extend_from_slice(&(cb.len() as u32).to_le_bytes());
            buf.extend_from_slice(&cb);
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        let mut pos = 0;
        let count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            let clen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            let (col, _) = ColumnDefinition::deserialize(&data[pos..pos + clen])?;
            pos += clen;
            columns.push(col);
        }
        Some((Self { columns }, pos))
    }
}

// ─── ConstraintType ────────────────────────────────────────────────────────────

/// 约束类型（C++: `ConstraintType` + parsed constraint data）。
#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintType {
    NotNull {
        column: String,
    },
    Check {
        expression: String,
    },
    Unique {
        columns: Vec<String>,
        is_primary: bool,
    },
    ForeignKey {
        table: String,
        fk_columns: Vec<String>,
        pk_columns: Vec<String>,
    },
}

impl ConstraintType {
    pub fn to_sql(&self) -> String {
        match self {
            ConstraintType::NotNull { column } => format!("{} NOT NULL", column),
            ConstraintType::Check { expression } => format!("CHECK ({})", expression),
            ConstraintType::Unique {
                columns,
                is_primary,
            } => {
                let kw = if *is_primary { "PRIMARY KEY" } else { "UNIQUE" };
                format!("{} ({})", kw, columns.join(", "))
            }
            ConstraintType::ForeignKey {
                table,
                fk_columns,
                pk_columns,
            } => format!(
                "FOREIGN KEY ({}) REFERENCES {} ({})",
                fk_columns.join(", "),
                table,
                pk_columns.join(", ")
            ),
        }
    }
}

// ─── CreateInfo and subclasses ─────────────────────────────────────────────────

/// 创建信息基类（C++: `CreateInfo`）。
#[derive(Debug, Clone)]
pub struct CreateInfo {
    pub catalog_type: CatalogType,
    pub catalog: String,
    pub schema: String,
    pub temporary: bool,
    pub internal: bool,
    pub on_conflict: OnCreateConflict,
    pub comment: Option<Value>,
    pub tags: HashMap<String, String>,
}

impl CreateInfo {
    pub fn new(catalog_type: CatalogType) -> Self {
        Self {
            catalog_type,
            catalog: String::new(),
            schema: String::new(),
            temporary: false,
            internal: false,
            on_conflict: OnCreateConflict::ErrorOnConflict,
            comment: None,
            tags: HashMap::new(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(self.catalog_type as u8);
        Self::write_string(&mut buf, &self.catalog);
        Self::write_string(&mut buf, &self.schema);
        buf.push(self.temporary as u8);
        buf.push(self.internal as u8);
        buf.push(self.on_conflict as u8);
        match &self.comment {
            None => buf.push(0),
            Some(v) => {
                buf.push(1);
                let vb = v.serialize();
                buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                buf.extend_from_slice(&vb);
            }
        }
        buf.extend_from_slice(&(self.tags.len() as u32).to_le_bytes());
        for (k, v) in &self.tags {
            Self::write_string(&mut buf, k);
            Self::write_string(&mut buf, v);
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        let mut pos = 0;
        let catalog_type = CatalogType::from_u8(data[pos]);
        pos += 1;
        let (catalog, n) = Self::read_string(&data[pos..])?;
        pos += n;
        let (schema, n) = Self::read_string(&data[pos..])?;
        pos += n;
        let temporary = data[pos] != 0;
        pos += 1;
        let internal = data[pos] != 0;
        pos += 1;
        let on_conflict = match data[pos] {
            1 => OnCreateConflict::IgnoreOnConflict,
            2 => OnCreateConflict::ReplaceOnConflict,
            3 => OnCreateConflict::AlterOnConflict,
            _ => OnCreateConflict::ErrorOnConflict,
        };
        pos += 1;
        let comment = if data[pos] == 0 {
            pos += 1;
            None
        } else {
            pos += 1;
            let vlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            let (v, _) = Value::deserialize(&data[pos..pos + vlen])?;
            pos += vlen;
            Some(v)
        };
        let tag_count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        let mut tags = HashMap::new();
        for _ in 0..tag_count {
            let (k, n) = Self::read_string(&data[pos..])?;
            pos += n;
            let (v, n) = Self::read_string(&data[pos..])?;
            pos += n;
            tags.insert(k, v);
        }
        Some((
            Self {
                catalog_type,
                catalog,
                schema,
                temporary,
                internal,
                on_conflict,
                comment,
                tags,
            },
            pos,
        ))
    }

    fn write_string(buf: &mut Vec<u8>, s: &str) {
        let b = s.as_bytes();
        buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
        buf.extend_from_slice(b);
    }

    fn read_string(data: &[u8]) -> Option<(String, usize)> {
        if data.len() < 4 {
            return None;
        }
        let len = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        if data.len() < 4 + len {
            return None;
        }
        let s = String::from_utf8(data[4..4 + len].to_vec()).ok()?;
        Some((s, 4 + len))
    }
}

// ─── Specific CreateInfo types ─────────────────────────────────────────────────

/// Schema 创建信息（C++: `CreateSchemaInfo`）。
#[derive(Debug, Clone)]
pub struct CreateSchemaInfo {
    pub base: CreateInfo,
}

impl CreateSchemaInfo {
    pub fn new(schema: String) -> Self {
        let mut base = CreateInfo::new(CatalogType::SchemaEntry);
        base.schema = schema;
        Self { base }
    }

    pub fn schema_name(&self) -> &str {
        &self.base.schema
    }
}

/// 表创建信息（C++: `CreateTableInfo` / `BoundCreateTableInfo`）。
#[derive(Debug, Clone)]
pub struct CreateTableInfo {
    pub base: CreateInfo,
    pub table: String,
    pub columns: ColumnList,
    pub constraints: Vec<ConstraintType>,
}

impl CreateTableInfo {
    pub fn new(schema: String, table: String) -> Self {
        let mut base = CreateInfo::new(CatalogType::TableEntry);
        base.schema = schema;
        Self {
            base,
            table,
            columns: ColumnList::new(),
            constraints: Vec::new(),
        }
    }

    pub fn to_sql(&self) -> String {
        let cols: Vec<String> = self.columns.columns.iter().map(|c| c.to_sql()).collect();
        let mut parts = cols;
        for c in &self.constraints {
            parts.push(c.to_sql());
        }
        format!(
            "CREATE TABLE {}.{} (\n  {}\n)",
            self.base.schema,
            self.table,
            parts.join(",\n  ")
        )
    }
}

/// 视图创建信息（C++: `CreateViewInfo`）。
#[derive(Debug, Clone)]
pub struct CreateViewInfo {
    pub base: CreateInfo,
    pub view_name: String,
    pub aliases: Vec<String>,
    pub types: Vec<LogicalType>,
    pub query: String,
    pub column_names: Vec<String>,
}

impl CreateViewInfo {
    pub fn new(schema: String, view_name: String, query: String) -> Self {
        let mut base = CreateInfo::new(CatalogType::ViewEntry);
        base.schema = schema;
        Self {
            base,
            view_name,
            aliases: Vec::new(),
            types: Vec::new(),
            query,
            column_names: Vec::new(),
        }
    }
}

/// 序列创建信息（C++: `CreateSequenceInfo`）。
#[derive(Debug, Clone)]
pub struct CreateSequenceInfo {
    pub base: CreateInfo,
    pub name: String,
    pub usage_count: u64,
    pub increment: i64,
    pub start_value: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
}

impl Default for CreateSequenceInfo {
    fn default() -> Self {
        Self {
            base: CreateInfo::new(CatalogType::SequenceEntry),
            name: String::new(),
            usage_count: 0,
            increment: 1,
            start_value: 1,
            min_value: 1,
            max_value: i64::MAX,
            cycle: false,
        }
    }
}

/// 类型创建信息（C++: `CreateTypeInfo`）。
#[derive(Debug, Clone)]
pub struct CreateTypeInfo {
    pub base: CreateInfo,
    pub name: String,
    pub logical_type: LogicalType,
}

impl CreateTypeInfo {
    pub fn new(schema: String, name: String, logical_type: LogicalType) -> Self {
        let mut base = CreateInfo::new(CatalogType::TypeEntry);
        base.schema = schema;
        Self {
            base,
            name,
            logical_type,
        }
    }
}

/// 索引创建信息（C++: `CreateIndexInfo`）。
#[derive(Debug, Clone)]
pub struct CreateIndexInfo {
    pub base: CreateInfo,
    pub index_name: String,
    pub table: String,
    pub index_type: String,
    pub constraint_type: IndexConstraintType,
    pub column_ids: Vec<usize>,
    pub expressions: Vec<String>,
    pub options: HashMap<String, Value>,
    pub sql: String,
}

impl CreateIndexInfo {
    pub fn new(schema: String, index_name: String, table: String) -> Self {
        let mut base = CreateInfo::new(CatalogType::IndexEntry);
        base.schema = schema;
        Self {
            base,
            index_name,
            table,
            index_type: "ART".to_string(),
            constraint_type: IndexConstraintType::None,
            column_ids: Vec::new(),
            expressions: Vec::new(),
            options: HashMap::new(),
            sql: String::new(),
        }
    }
}

/// 函数创建信息（C++: `CreateFunctionInfo`）。
#[derive(Debug, Clone)]
pub struct CreateFunctionInfo {
    pub base: CreateInfo,
    pub name: String,
    pub or_replace: bool,
    pub descriptions: Vec<String>,
}

impl CreateFunctionInfo {
    pub fn new(catalog_type: CatalogType, schema: String, name: String) -> Self {
        let mut base = CreateInfo::new(catalog_type);
        base.schema = schema;
        Self {
            base,
            name,
            or_replace: false,
            descriptions: Vec::new(),
        }
    }
}

/// 表函数创建信息。
pub type CreateTableFunctionInfo = CreateFunctionInfo;
/// 复制函数创建信息。
pub type CreateCopyFunctionInfo = CreateFunctionInfo;
/// PRAGMA 函数创建信息。
pub type CreatePragmaFunctionInfo = CreateFunctionInfo;

/// Collation 创建信息（C++: `CreateCollationInfo`）。
#[derive(Debug, Clone)]
pub struct CreateCollationInfo {
    pub base: CreateInfo,
    pub name: String,
    pub combinable: bool,
    pub not_required_for_equality: bool,
}

impl CreateCollationInfo {
    pub fn new(schema: String, name: String) -> Self {
        let mut base = CreateInfo::new(CatalogType::CollateCatalogEntry);
        base.schema = schema;
        Self {
            base,
            name,
            combinable: false,
            not_required_for_equality: false,
        }
    }
}

// ─── AlterInfo ─────────────────────────────────────────────────────────────────

/// Alter 操作类型（C++: `AlterType`）。
#[derive(Debug, Clone)]
pub enum AlterKind {
    RenameTable {
        new_name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    AddColumn {
        column: ColumnDefinition,
        if_not_exists: bool,
    },
    RemoveColumn {
        column_name: String,
        if_exists: bool,
        cascade: bool,
    },
    AlterColumnType {
        column_name: String,
        new_type: LogicalType,
        cast_expr: Option<String>,
    },
    SetDefault {
        column_name: String,
        default_value: Option<String>,
    },
    SetNotNull {
        column_name: String,
    },
    DropNotNull {
        column_name: String,
    },
    SetComment {
        new_comment: Option<Value>,
    },
    SetColumnComment {
        column_name: String,
        comment: Option<Value>,
    },
    RenameView {
        new_name: String,
    },
    AddForeignKey {
        constraint: ConstraintType,
    },
    DropConstraint {
        constraint_name: String,
        cascade: bool,
    },
    SetTags {
        tags: HashMap<String, String>,
    },
    AlterFunction {
        name: String,
        or_replace: bool,
    },
}

/// Alter 信息（C++: `AlterInfo`）。
#[derive(Debug, Clone)]
pub struct AlterInfo {
    pub catalog_type: CatalogType,
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub if_exists: bool,
    pub allow_internal: bool,
    pub kind: AlterKind,
}

impl AlterInfo {
    pub fn new(catalog_type: CatalogType, schema: String, name: String, kind: AlterKind) -> Self {
        Self {
            catalog_type,
            catalog: String::new(),
            schema,
            name,
            if_exists: false,
            allow_internal: false,
            kind,
        }
    }
}

// ─── DropInfo ──────────────────────────────────────────────────────────────────

/// Drop 信息（C++: `DropInfo`）。
#[derive(Debug, Clone)]
pub struct DropInfo {
    pub catalog_type: CatalogType,
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub cascade: bool,
    pub allow_drop_internal: bool,
    pub if_exists: bool,
}

impl DropInfo {
    pub fn new(catalog_type: CatalogType, schema: String, name: String) -> Self {
        Self {
            catalog_type,
            catalog: String::new(),
            schema,
            name,
            cascade: false,
            allow_drop_internal: false,
            if_exists: false,
        }
    }
}

// ─── DatabaseSize ──────────────────────────────────────────────────────────────

/// 数据库大小信息（C++: `DatabaseSize`）。
#[derive(Debug, Clone, Default)]
pub struct DatabaseSize {
    pub file_size: u64,
    pub block_size: u64,
    pub block_count: u64,
    pub free_blocks: u64,
    pub used_blocks: u64,
    pub bytes: u64,
    pub wal_size: u64,
    pub memory_usage: u64,
    pub memory_limit: u64,
}

/// 元数据块信息（C++: `MetadataBlockInfo`）。
#[derive(Debug, Clone)]
pub struct MetadataBlockInfo {
    pub block_id: u64,
    pub total_blocks: u64,
    pub free_list_count: u64,
    pub metadata_offset: u64,
}
