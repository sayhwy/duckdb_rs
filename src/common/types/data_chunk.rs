//! 向量化数据块（DataChunk）及其基础类型。
//!
//! 对应 C++:
//!   `duckdb/common/types/data_chunk.hpp`
//!   `src/common/types/data_chunk.cpp`
//!
//! # 结构层次
//!
//! ```text
//! DataChunk
//!   └── Vec<Vector>          — 每列一个 Vector
//!         ├── VectorType     — Flat / Constant / Dictionary / Sequence
//!         ├── data: Vec<u8>  — 原始字节存储（capacity × physical_size）
//!         ├── ValidityMask   — NULL 位图（每行一位）
//!         └── [child/sel]    — Dictionary 向量的子向量 + 选择向量
//! ```
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class DataChunk` | `struct DataChunk` |
//! | `vector<Vector> data` | `data: Vec<Vector>` |
//! | `idx_t count / capacity` | `count: usize / capacity: usize` |
//! | `vector<VectorCache> vector_caches` | `caches: Vec<VectorCache>` |
//! | `class Vector` | `struct Vector` |
//! | `enum VectorType` | `enum VectorType` |
//! | `ValidityMask` | `struct ValidityMask` |
//! | `SelectionVector` | `struct SelectionVector` |
//!
//! # 设计说明
//!
//! - 数据以 `Vec<u8>` 存储，元素大小由 `LogicalType::physical_size()` 决定。
//! - 复杂的向量化操作（`Hash`、`Serialize`/`Deserialize`、`ToUnifiedFormat`）
//!   保留为 `todo!()` 存根，等执行引擎层完善后填充。
//! - `unsafe` 限于将 `Vec<u8>` 重新解释为类型化 slice（必要时）；
//!   当前实现通过公开 `raw_data()` 接口将此决策推迟给调用方。

use crate::common::errors::{Result, anyhow, bail};
use chrono::{Duration, NaiveDate};
use std::fmt::Write as _;

// ─────────────────────────────────────────────────────────────────────────────
// LogicalType（独立定义，作为 common/types 层的基础类型）
// ─────────────────────────────────────────────────────────────────────────────

/// DuckDB 逻辑类型（C++: `LogicalType`）。
///
/// 包含类型 ID 及可选的嵌套/精度元数据。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalType {
    /// 类型标识（C++: `LogicalTypeId`）。
    pub id: LogicalTypeId,
    /// DECIMAL 的精度（仅 Decimal 类型有效）。
    pub width: u8,
    /// DECIMAL 的 scale（仅 Decimal 类型有效）。
    pub scale: u8,
    /// 子类型（用于 List、Array 等嵌套类型，C++: `child_type`）。
    pub child_type: Option<Box<LogicalType>>,
    /// Array 固定元素数量（仅 Array 类型有效，C++: `ArrayType::GetSize`）。
    pub array_size: usize,
    /// Struct 字段列表（仅 Struct/Variant 有效，C++: `StructType::GetChildTypes`）。
    pub struct_fields: Vec<(String, LogicalType)>,
}

impl LogicalType {
    // ── 基础构造 ──────────────────────────────────────────────────────────────

    /// 通用构造（C++: `LogicalType(LogicalTypeId id)`）。
    pub fn new(id: LogicalTypeId) -> Self {
        Self {
            id,
            width: 0,
            scale: 0,
            child_type: None,
            array_size: 0,
            struct_fields: Vec::new(),
        }
    }

    // ── 构造快捷方式 ──────────────────────────────────────────────────────────

    pub fn boolean() -> Self {
        Self::new(LogicalTypeId::Boolean)
    }
    pub fn tinyint() -> Self {
        Self::new(LogicalTypeId::TinyInt)
    }
    pub fn smallint() -> Self {
        Self::new(LogicalTypeId::SmallInt)
    }
    pub fn integer() -> Self {
        Self::new(LogicalTypeId::Integer)
    }
    pub fn bigint() -> Self {
        Self::new(LogicalTypeId::BigInt)
    }
    pub fn hugeint() -> Self {
        Self::new(LogicalTypeId::HugeInt)
    }
    pub fn float() -> Self {
        Self::new(LogicalTypeId::Float)
    }
    pub fn double() -> Self {
        Self::new(LogicalTypeId::Double)
    }
    pub fn decimal(width: u8, scale: u8) -> Self {
        assert!(width >= scale, "DECIMAL width must be >= scale");
        Self {
            id: LogicalTypeId::Decimal,
            width,
            scale,
            child_type: None,
            array_size: 0,
            struct_fields: Vec::new(),
        }
    }
    pub fn varchar() -> Self {
        Self::new(LogicalTypeId::Varchar)
    }
    pub fn date() -> Self {
        Self::new(LogicalTypeId::Date)
    }
    pub fn row_id() -> Self {
        Self::new(LogicalTypeId::BigInt)
    } // row_t = i64
    pub fn validity() -> Self {
        Self::new(LogicalTypeId::Validity)
    }
    pub fn new_invalid() -> Self {
        Self::new(LogicalTypeId::Invalid)
    }
    pub fn uinteger() -> Self {
        Self::new(LogicalTypeId::UInteger)
    }
    pub fn utinyint() -> Self {
        Self::new(LogicalTypeId::UTinyInt)
    }
    pub fn usmallint() -> Self {
        Self::new(LogicalTypeId::USmallInt)
    }
    pub fn ubigint() -> Self {
        Self::new(LogicalTypeId::UBigInt)
    }
    pub fn blob() -> Self {
        Self::new(LogicalTypeId::Blob)
    }

    /// LIST(child) 类型（C++: `LogicalType::LIST(child_type)`）。
    pub fn list(child_type: LogicalType) -> Self {
        Self {
            id: LogicalTypeId::List,
            width: 0,
            scale: 0,
            child_type: Some(Box::new(child_type)),
            array_size: 0,
            struct_fields: Vec::new(),
        }
    }

    /// ARRAY(child, N) 类型（C++: `LogicalType::ARRAY(child_type, size)`）。
    pub fn array(child_type: LogicalType, size: usize) -> Self {
        Self {
            id: LogicalTypeId::Array,
            width: 0,
            scale: 0,
            child_type: Some(Box::new(child_type)),
            array_size: size,
            struct_fields: Vec::new(),
        }
    }

    /// STRUCT({field_name: type, ...}) 类型（C++: `LogicalType::STRUCT(children)`）。
    pub fn struct_type(fields: Vec<(String, LogicalType)>) -> Self {
        Self {
            id: LogicalTypeId::Struct,
            width: 0,
            scale: 0,
            child_type: None,
            array_size: 0,
            struct_fields: fields,
        }
    }

    /// VARIANT 类型（内部存储为带固定 schema 的 STRUCT，C++: `LogicalType::VARIANT()`）。
    pub fn variant() -> Self {
        let children = vec![
            ("keys".to_string(), Self::list(Self::varchar())),
            (
                "children".to_string(),
                Self::list(Self::struct_type(vec![
                    ("keys_index".to_string(), Self::uinteger()),
                    ("values_index".to_string(), Self::uinteger()),
                ])),
            ),
            (
                "values".to_string(),
                Self::list(Self::struct_type(vec![
                    ("type_id".to_string(), Self::utinyint()),
                    ("byte_offset".to_string(), Self::uinteger()),
                ])),
            ),
            ("data".to_string(), Self::blob()),
        ];
        Self {
            id: LogicalTypeId::Variant,
            width: 0,
            scale: 0,
            child_type: None,
            array_size: 0,
            struct_fields: children,
        }
    }

    /// 返回子类型引用（List/Array 类型，C++: `ArrayType::GetChildType` / `ListType::GetChildType`）。
    pub fn get_child_type(&self) -> Option<&LogicalType> {
        self.child_type.as_deref()
    }

    /// 返回 Array 的固定元素数量（C++: `ArrayType::GetSize`）。
    pub fn get_array_size(&self) -> usize {
        self.array_size
    }

    /// Struct 字段列表（C++: `StructType::GetChildTypes`）。
    pub fn get_struct_fields(&self) -> &[(String, LogicalType)] {
        &self.struct_fields
    }

    /// Struct 字段数量（C++: `StructType::GetChildCount`）。
    pub fn get_struct_child_count(&self) -> usize {
        self.struct_fields.len()
    }

    /// 按下标取 Struct 子类型（C++: `StructType::GetChildType(type, i)`）。
    pub fn get_struct_child_type(&self, i: usize) -> Option<&LogicalType> {
        self.struct_fields.get(i).map(|(_, t)| t)
    }

    /// 该类型每个值占用的字节数（用于 flat vector 的缓冲区分配）。
    ///
    /// DATE 对应 DuckDB `PhysicalType::INT32`（4 字节），与 C++ `date_t` 一致。
    pub fn physical_size(&self) -> usize {
        match self.id {
            LogicalTypeId::Boolean | LogicalTypeId::TinyInt => 1,
            LogicalTypeId::SmallInt => 2,
            LogicalTypeId::Integer | LogicalTypeId::Float | LogicalTypeId::Date => 4,
            LogicalTypeId::BigInt
            | LogicalTypeId::Double
            | LogicalTypeId::Timestamp
            | LogicalTypeId::Time
            | LogicalTypeId::Interval => 8,
            LogicalTypeId::HugeInt => 16,
            LogicalTypeId::Decimal => {
                if self.width <= 4 {
                    2
                } else if self.width <= 9 {
                    4
                } else if self.width <= 18 {
                    8
                } else if self.width <= 38 {
                    16
                } else {
                    panic!("unsupported DECIMAL width {}", self.width);
                }
            }
            // VARCHAR 在 flat vector 中存为 string_t (4 + 4 + 8 bytes = 16)
            LogicalTypeId::Varchar => 16,
            // 其余类型（List / Struct / Map）使用 8 字节指针占位
            _ => 8,
        }
    }

    /// 类型名称字符串（调试用，C++: `LogicalType::ToString()`）。
    pub fn name(&self) -> &'static str {
        match self.id {
            LogicalTypeId::Boolean => "BOOLEAN",
            LogicalTypeId::TinyInt => "TINYINT",
            LogicalTypeId::SmallInt => "SMALLINT",
            LogicalTypeId::Integer => "INTEGER",
            LogicalTypeId::BigInt => "BIGINT",
            LogicalTypeId::HugeInt => "HUGEINT",
            LogicalTypeId::Float => "FLOAT",
            LogicalTypeId::Double => "DOUBLE",
            LogicalTypeId::Decimal => "DECIMAL",
            LogicalTypeId::Varchar => "VARCHAR",
            LogicalTypeId::Timestamp => "TIMESTAMP",
            LogicalTypeId::Date => "DATE",
            LogicalTypeId::Time => "TIME",
            LogicalTypeId::Interval => "INTERVAL",
            LogicalTypeId::List => "LIST",
            LogicalTypeId::Struct => "STRUCT",
            LogicalTypeId::Map => "MAP",
            LogicalTypeId::Array => "ARRAY",
            LogicalTypeId::Variant => "VARIANT",
            LogicalTypeId::UInteger => "UINTEGER",
            LogicalTypeId::UTinyInt => "UTINYINT",
            LogicalTypeId::USmallInt => "USMALLINT",
            LogicalTypeId::UBigInt => "UBIGINT",
            LogicalTypeId::Blob => "BLOB",
            LogicalTypeId::Invalid => "INVALID",
            LogicalTypeId::Validity => "VALIDITY",
        }
    }
}

/// 逻辑类型标识（C++: `LogicalTypeId`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogicalTypeId {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    HugeInt,
    Float,
    Double,
    Decimal,
    Varchar,
    Timestamp,
    Date,
    Time,
    Interval,
    List,
    Struct,
    Map,
    Array,
    Variant,
    UInteger,
    UTinyInt,
    USmallInt,
    UBigInt,
    Blob,
    Invalid,
    Validity, // Added for NULL bitmask columns
}

// ─────────────────────────────────────────────────────────────────────────────
// SelectionVector
// ─────────────────────────────────────────────────────────────────────────────

/// 行选择向量（C++: `SelectionVector`）。
///
/// 存储行下标（u32）的列表，用于 Dictionary 向量和过滤操作。
#[derive(Debug, Clone, Default)]
pub struct SelectionVector {
    /// 行下标数组（C++: `sel_t *sel_data`）。
    pub indices: Vec<u32>,
}

impl SelectionVector {
    /// 创建容量为 `capacity` 的未初始化选择向量。
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            indices: Vec::with_capacity(capacity),
        }
    }

    /// 创建恒等映射：`indices[i] = i`（C++: `SelectionVector(idx_t count)`）。
    pub fn identity(count: usize) -> Self {
        Self {
            indices: (0..count as u32).collect(),
        }
    }

    /// 创建区间 `[offset, offset + count)` 的恒等映射
    /// （C++: `SelectionVector(idx_t offset, idx_t count)`）。
    pub fn range(offset: u32, count: usize) -> Self {
        Self {
            indices: (offset..offset + count as u32).collect(),
        }
    }

    /// 读取第 `i` 个下标（C++: `get_index(idx_t i)`）。
    #[inline]
    pub fn get_index(&self, i: usize) -> usize {
        self.indices[i] as usize
    }

    /// 写入第 `i` 个下标（C++: `set_index(idx_t i, idx_t idx)`）。
    #[inline]
    pub fn set_index(&mut self, i: usize, idx: u32) {
        self.indices[i] = idx;
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ValidityMask
// ─────────────────────────────────────────────────────────────────────────────

/// NULL 位图（C++: `ValidityMask`）。
///
/// 每行占 1 位：1 = 有效（非 NULL），0 = NULL。
/// 内部以 `u64` 字（64 位/字）存储。
#[derive(Debug, Clone)]
pub struct ValidityMask {
    /// 位图数组（每个 u64 覆盖 64 行）。
    bits: Vec<u64>,
    /// 覆盖的行数（C++: `ValidityMask` 本身不存储但调用方知晓）。
    row_count: usize,
    /// 快捷标记：若所有行均有效则为 true，跳过位图检查。
    all_valid: bool,
}

impl ValidityMask {
    /// 创建全部有效（无 NULL）的 mask（C++: 默认构造或 `SetAllValid()`）。
    pub fn all_valid(row_count: usize) -> Self {
        Self {
            bits: Vec::new(),
            row_count,
            all_valid: true,
        }
    }

    /// 创建全部无效（全 NULL）的 mask（C++: `SetAllInvalid()`）。
    pub fn all_invalid(row_count: usize) -> Self {
        let nwords = (row_count + 63) / 64;
        Self {
            bits: vec![0u64; nwords],
            row_count,
            all_valid: false,
        }
    }

    /// 创建有指定行数的 mask，初始全部有效。
    pub fn new(row_count: usize) -> Self {
        Self::all_valid(row_count)
    }

    fn ensure_allocated(&mut self) {
        if self.bits.is_empty() {
            let nwords = (self.row_count + 63) / 64;
            self.bits = vec![!0u64; nwords]; // all bits set = all valid
        }
    }

    /// 第 `row` 行是否有效（非 NULL）（C++: `RowIsValid(idx_t row)`）。
    #[inline]
    pub fn row_is_valid(&self, row: usize) -> bool {
        if self.all_valid {
            return true;
        }
        if self.bits.is_empty() {
            return true;
        }
        let word = row / 64;
        let bit = row % 64;
        (self.bits[word] >> bit) & 1 == 1
    }

    /// 将第 `row` 行设为有效（C++: `SetValid(idx_t row)`）。
    pub fn set_valid(&mut self, row: usize) {
        self.ensure_allocated();
        let word = row / 64;
        let bit = row % 64;
        self.bits[word] |= 1 << bit;
    }

    /// 将第 `row` 行设为 NULL（C++: `SetInvalid(idx_t row)`）。
    pub fn set_invalid(&mut self, row: usize) {
        self.ensure_allocated();
        self.all_valid = false;
        let word = row / 64;
        let bit = row % 64;
        self.bits[word] &= !(1u64 << bit);
    }

    /// 是否所有行都有效（C++: `AllValid()`）。
    pub fn is_all_valid(&self) -> bool {
        if self.all_valid {
            return true;
        }
        self.bits.iter().all(|&w| w == !0u64)
    }

    /// 重置为覆盖 `row_count` 行的全有效状态。
    pub fn reset(&mut self, row_count: usize) {
        self.bits.clear();
        self.row_count = row_count;
        self.all_valid = true;
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// VectorType
// ─────────────────────────────────────────────────────────────────────────────

/// 向量存储类型（C++: `enum VectorType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorType {
    /// 连续内存平坦向量（C++: `FLAT_VECTOR`）。
    #[default]
    Flat,
    /// 单值常量向量（C++: `CONSTANT_VECTOR`）。
    Constant,
    /// 字典向量 = 选择向量 + 子向量（C++: `DICTIONARY_VECTOR`）。
    Dictionary,
    /// 整数序列向量（C++: `SEQUENCE_VECTOR`）。
    Sequence,
}

// ─────────────────────────────────────────────────────────────────────────────
// Vector
// ─────────────────────────────────────────────────────────────────────────────

/// 列向量（C++: `class Vector`）。
///
/// 持有单列的向量化数据，支持 Flat / Constant / Dictionary / Sequence 四种布局。
pub struct Vector {
    // ── 类型 ──────────────────────────────────────────────────────────────
    /// 存储布局（C++: `VectorType vector_type`）。
    pub vector_type: VectorType,
    /// 逻辑类型（C++: `LogicalType type`）。
    pub logical_type: LogicalType,

    // ── Flat 布局：原始字节缓冲区 ─────────────────────────────────────────
    /// 原始数据缓冲区（`capacity × physical_size` 字节）
    /// （C++: `data_ptr_t data`）。
    data: Vec<u8>,

    // ── NULL 位图 ─────────────────────────────────────────────────────────
    /// 有效性位图（C++: `ValidityMask validity`）。
    pub validity: ValidityMask,

    // ── Dictionary 布局 ───────────────────────────────────────────────────
    /// 字典子向量（C++: `buffer = DictionaryBuffer { child_vector, sel_vector }`）。
    child: Option<Box<Vector>>,
    /// STRUCT / VARIANT 子向量（C++: `StructVector::GetEntries()`）。
    struct_children: Vec<Vector>,
    /// 字典选择向量。
    sel: Option<SelectionVector>,

    // ── Sequence 布局 ────────────────────────────────────────────────────
    /// 序列起始值（C++: `sequence_start`）。
    seq_start: i64,
    /// 序列步长（C++: `sequence_increment`）。
    seq_increment: i64,

    /// `VARCHAR` 长字符串的所有权存储。
    string_aux: Vec<Box<[u8]>>,
}

impl Vector {
    // ── 构造 ─────────────────────────────────────────────────────────────

    /// 创建空的 Flat 向量，无缓冲区（C++: `Vector(LogicalType, nullptr)`）。
    pub fn new_empty(logical_type: LogicalType) -> Self {
        let validity = ValidityMask::new(0);
        let struct_children = match logical_type.id {
            LogicalTypeId::Struct | LogicalTypeId::Variant => logical_type
                .get_struct_fields()
                .iter()
                .map(|(_, child_type)| Vector::new_empty(child_type.clone()))
                .collect(),
            _ => Vec::new(),
        };
        Self {
            vector_type: VectorType::Flat,
            logical_type,
            data: Vec::new(),
            validity,
            child: None,
            struct_children,
            sel: None,
            seq_start: 0,
            seq_increment: 0,
            string_aux: Vec::new(),
        }
    }

    /// 创建分配了 `capacity` 行缓冲区的 Flat 向量（C++: `VectorCache(allocator, type, capacity)`）。
    pub fn with_capacity(logical_type: LogicalType, capacity: usize) -> Self {
        let elem_size = logical_type.physical_size();
        let data = vec![0u8; capacity * elem_size];
        let validity = ValidityMask::new(capacity);
        let struct_children = match logical_type.id {
            LogicalTypeId::Struct | LogicalTypeId::Variant => logical_type
                .get_struct_fields()
                .iter()
                .map(|(_, child_type)| Vector::with_capacity(child_type.clone(), capacity))
                .collect(),
            _ => Vec::new(),
        };
        Self {
            vector_type: VectorType::Flat,
            logical_type,
            data,
            validity,
            child: None,
            struct_children,
            sel: None,
            seq_start: 0,
            seq_increment: 0,
            string_aux: Vec::new(),
        }
    }

    /// 创建 Constant 向量（重复同一行 `count` 次）（C++: `ConstantVector`）。
    pub fn new_constant(logical_type: LogicalType) -> Self {
        let elem_size = logical_type.physical_size();
        let data = vec![0u8; elem_size]; // 只存一个元素
        let struct_children = match logical_type.id {
            LogicalTypeId::Struct | LogicalTypeId::Variant => logical_type
                .get_struct_fields()
                .iter()
                .map(|(_, child_type)| Vector::new_constant(child_type.clone()))
                .collect(),
            _ => Vec::new(),
        };
        Self {
            vector_type: VectorType::Constant,
            logical_type,
            data,
            validity: ValidityMask::new(1),
            child: None,
            struct_children,
            sel: None,
            seq_start: 0,
            seq_increment: 0,
            string_aux: Vec::new(),
        }
    }

    /// 创建整数序列向量（C++: `SequenceVector(type, start, increment)`）。
    pub fn new_sequence(logical_type: LogicalType, start: i64, increment: i64) -> Self {
        Self {
            vector_type: VectorType::Sequence,
            logical_type,
            data: Vec::new(),
            validity: ValidityMask::new(0),
            child: None,
            struct_children: Vec::new(),
            sel: None,
            seq_start: start,
            seq_increment: increment,
            string_aux: Vec::new(),
        }
    }

    // ── 类型与布局 ────────────────────────────────────────────────────────

    pub fn get_type(&self) -> &LogicalType {
        &self.logical_type
    }
    pub fn get_vector_type(&self) -> VectorType {
        self.vector_type
    }

    /// Get the child vector (for array/list vectors).
    /// Mirrors `ArrayVector::GetEntry(vector)` / `ListVector::GetEntry(vector)`.
    pub fn get_child(&self) -> Option<&Vector> {
        self.child.as_deref()
    }

    /// Get the mutable child vector.
    pub fn get_child_mut(&mut self) -> Option<&mut Vector> {
        self.child.as_deref_mut()
    }

    pub fn set_child(&mut self, child: Vector) {
        self.child = Some(Box::new(child));
    }

    pub fn get_children(&self) -> &[Vector] {
        &self.struct_children
    }

    pub fn get_children_mut(&mut self) -> &mut [Vector] {
        &mut self.struct_children
    }

    pub fn set_children(&mut self, children: Vec<Vector>) {
        self.struct_children = children;
    }

    /// Raw byte access to the data buffer (e.g. for reading `list_entry_t`).
    pub fn raw_data(&self) -> &[u8] {
        &self.data
    }

    /// 将此向量变为引用 `other` 的 Dictionary 向量（恒等选择）
    /// （C++: `Vector::Reference(const Vector&)`）。
    pub fn reference(&mut self, other: &Vector) {
        self.logical_type = other.logical_type.clone();
        self.vector_type = VectorType::Dictionary;
        self.child = Some(Box::new(other.shallow_clone()));
        self.struct_children.clear();
        self.sel = None; // 使用恒等映射
        self.validity = other.validity.clone();
    }

    /// 将此向量设置为 Dictionary 向量，引用给定子向量与选择向量。
    pub fn set_dictionary(&mut self, child: &Vector, sel: SelectionVector) {
        self.logical_type = child.logical_type.clone();
        self.vector_type = VectorType::Dictionary;
        self.child = Some(Box::new(child.shallow_clone()));
        self.struct_children.clear();
        self.sel = Some(sel);
        self.validity = child.validity.clone();
        self.seq_start = 0;
        self.seq_increment = 0;
        self.data.clear();
        self.string_aux.clear();
    }

    /// 浅克隆（仅克隆元数据，不克隆大缓冲区）。
    pub fn shallow_clone(&self) -> Self {
        let mut result = Self {
            vector_type: self.vector_type,
            logical_type: self.logical_type.clone(),
            data: self.data.clone(),
            validity: self.validity.clone(),
            child: self
                .child
                .as_ref()
                .map(|child| Box::new((**child).shallow_clone())),
            struct_children: self
                .struct_children
                .iter()
                .map(Vector::shallow_clone)
                .collect(),
            sel: self.sel.clone(),
            seq_start: self.seq_start,
            seq_increment: self.seq_increment,
            string_aux: self.string_aux.clone(),
        };
        result.rebind_varchar_pointers();
        result
    }

    // ── 数据访问 ──────────────────────────────────────────────────────────

    /// 返回 Flat 向量的原始字节切片（可写）。
    pub fn raw_data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn rebind_varchar_pointers(&mut self) {
        if self.logical_type.id != LogicalTypeId::Varchar {
            return;
        }
        let mut aux_idx = 0usize;
        for row in 0..(self.data.len() / 16) {
            let offset = row * 16;
            let len = u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
            if len > 12 {
                let Some(bytes) = self.string_aux.get(aux_idx) else {
                    break;
                };
                let ptr = bytes.as_ptr() as u64;
                self.data[offset + 8..offset + 16].copy_from_slice(&ptr.to_le_bytes());
                aux_idx += 1;
            }
        }
    }

    pub fn write_varchar_bytes(&mut self, row: usize, value: &[u8]) {
        debug_assert_eq!(self.logical_type.id, LogicalTypeId::Varchar);
        let offset = row * 16;
        let len = value.len() as u32;
        let dst = &mut self.data[offset..offset + 16];
        dst.fill(0);
        dst[..4].copy_from_slice(&len.to_le_bytes());
        if value.len() <= 12 {
            dst[4..4 + value.len()].copy_from_slice(value);
        } else {
            dst[4..8].copy_from_slice(&value[..4]);
            let bytes: Box<[u8]> = value.to_vec().into_boxed_slice();
            let ptr = bytes.as_ptr() as u64;
            dst[8..16].copy_from_slice(&ptr.to_le_bytes());
            self.string_aux.push(bytes);
        }
        self.validity.set_valid(row);
    }

    pub fn reset_varchar_storage(&mut self) {
        if self.logical_type.id == LogicalTypeId::Varchar {
            self.string_aux.clear();
        }
    }

    pub fn read_varchar_bytes(&self, row: usize) -> Vec<u8> {
        debug_assert_eq!(self.logical_type.id, LogicalTypeId::Varchar);
        if self.vector_type == VectorType::Dictionary {
            if let Some(child) = self.child.as_ref() {
                return child.read_varchar_bytes(self.resolved_row_index(row));
            }
            return Vec::new();
        }
        let offset = self.resolved_row_index(row) * 16;
        let len = u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()) as usize;
        if len <= 12 {
            self.data[offset + 4..offset + 4 + len].to_vec()
        } else {
            let ptr =
                u64::from_le_bytes(self.data[offset + 8..offset + 16].try_into().unwrap()) as *const u8;
            unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
        }
    }

    /// 将向量展平为 Flat 布局，丢弃选择向量等间接层
    /// （C++: `Vector::Flatten(idx_t count)`）。
    pub fn flatten(&mut self, count: usize) {
        match self.vector_type {
            VectorType::Flat => {} // 已经是 flat，无需处理
            VectorType::Constant => {
                // 将单值扩展为 count 行
                let elem_size = self.logical_type.physical_size();
                if self.data.len() >= elem_size {
                    let single = self.data[..elem_size].to_vec();
                    self.data = single.repeat(count);
                } else {
                    self.data = vec![0u8; count * elem_size];
                }
                self.vector_type = VectorType::Flat;
                self.validity.reset(count);
            }
            VectorType::Sequence => {
                // 将序列展开为 count 行的 i64 flat 数据
                let mut flat = vec![0u8; count * 8];
                for i in 0..count {
                    let v = self.seq_start + (i as i64) * self.seq_increment;
                    flat[i * 8..(i + 1) * 8].copy_from_slice(&v.to_le_bytes());
                }
                self.data = flat;
                self.vector_type = VectorType::Flat;
                self.validity.reset(count);
            }
            VectorType::Dictionary => {
                let elem_size = self.logical_type.physical_size();
                let selected_indices = self
                    .sel
                    .as_ref()
                    .map(|sel| sel.indices[..count].to_vec());
                let child_count = selected_indices
                    .as_ref()
                    .and_then(|indices| indices.iter().copied().max())
                    .map(|max_idx| max_idx as usize + 1)
                    .unwrap_or(count);

                if let Some(child) = self.child.as_mut() {
                    child.flatten(child_count);
                    if self.logical_type.id == LogicalTypeId::Varchar {
                        let mut values = Vec::with_capacity(count);
                        let mut row_validity = Vec::with_capacity(count);
                        for row_idx in 0..count {
                            let src_row = selected_indices
                                .as_ref()
                                .map(|indices| indices[row_idx] as usize)
                                .unwrap_or(row_idx);
                            let is_valid = child.validity.row_is_valid(src_row);
                            row_validity.push(is_valid);
                            if is_valid {
                                values.push(child.read_varchar_bytes(src_row));
                            } else {
                                values.push(Vec::new());
                            }
                        }

                        self.data = vec![0u8; count * elem_size];
                        self.validity = ValidityMask::new(count);
                        self.validity.reset(count);
                        self.string_aux.clear();

                        for row_idx in 0..count {
                            if !row_validity[row_idx] {
                                self.validity.set_invalid(row_idx);
                                continue;
                            }
                            self.write_varchar_bytes(row_idx, &values[row_idx]);
                        }
                    } else {
                        let mut flat = vec![0u8; count * elem_size];
                        let mut validity = ValidityMask::new(count);
                        validity.reset(count);

                        for row_idx in 0..count {
                            let src_row = selected_indices
                                .as_ref()
                                .map(|indices| indices[row_idx] as usize)
                                .unwrap_or(row_idx);
                            if src_row * elem_size + elem_size <= child.data.len() {
                                let src = &child.data[src_row * elem_size..(src_row + 1) * elem_size];
                                flat[row_idx * elem_size..(row_idx + 1) * elem_size]
                                    .copy_from_slice(src);
                            }
                            if !child.validity.row_is_valid(src_row) {
                                validity.set_invalid(row_idx);
                            }
                        }

                        self.data = flat;
                        self.validity = validity;
                    }
                } else {
                    self.data = vec![0u8; count * elem_size];
                    self.validity.reset(count);
                    self.string_aux.clear();
                }
                self.child = None;
                self.sel = None;
                self.vector_type = VectorType::Flat;
            }
        }
    }

    /// 将 `other` 的第 `src_row` 行的值写入本向量第 `dst_row` 行
    /// （C++: 对应 `Vector::SetValue` / copy 操作的一部分）。
    pub fn copy_row_from(&mut self, other: &Vector, src_row: usize, dst_row: usize) {
        if self.logical_type.id == LogicalTypeId::Varchar && other.logical_type.id == LogicalTypeId::Varchar {
            if !other.validity.row_is_valid(src_row) {
                self.validity.set_invalid(dst_row);
                return;
            }
            let value = other.read_varchar_bytes(src_row);
            self.write_varchar_bytes(dst_row, &value);
            return;
        }
        let elem_size = self.logical_type.physical_size();
        if src_row * elem_size + elem_size <= other.data.len()
            && dst_row * elem_size + elem_size <= self.data.len()
        {
            let val = other.data[src_row * elem_size..(src_row + 1) * elem_size].to_vec();
            self.data[dst_row * elem_size..(dst_row + 1) * elem_size].copy_from_slice(&val);
            if !other.validity.row_is_valid(src_row) {
                self.validity.set_invalid(dst_row);
            } else {
                self.validity.set_valid(dst_row);
            }
        }
    }

    /// 将 `other` 按 `sel` 选择的行 `copy` 到本向量从 `dst_offset` 起的行
    /// （简化版 `VectorOperations::Copy`）。
    pub fn copy_from_sel(
        &mut self,
        other: &Vector,
        sel: &SelectionVector,
        src_count: usize,
        dst_offset: usize,
    ) {
        for i in 0..src_count {
            let src_row = sel.get_index(i);
            self.copy_row_from(other, src_row, dst_offset + i);
        }
    }

    /// 将 `other` 的前 `count` 行 copy 到本向量从 `dst_offset` 起的行。
    pub fn copy_from(
        &mut self,
        other: &Vector,
        count: usize,
        src_offset: usize,
        dst_offset: usize,
    ) {
        if self.logical_type.id == LogicalTypeId::Varchar && other.logical_type.id == LogicalTypeId::Varchar {
            for i in 0..count {
                self.copy_row_from(other, src_offset + i, dst_offset + i);
            }
            return;
        }
        let elem_size = self.logical_type.physical_size();
        let src_start = src_offset * elem_size;
        let src_end = src_start + count * elem_size;
        let dst_start = dst_offset * elem_size;
        let dst_end = dst_start + count * elem_size;
        if src_end <= other.data.len() && dst_end <= self.data.len() {
            self.data[dst_start..dst_end].copy_from_slice(&other.data[src_start..src_end]);
        }
        for i in 0..count {
            if !other.validity.row_is_valid(src_offset + i) {
                self.validity.set_invalid(dst_offset + i);
            }
        }
    }

    /// 调整 Flat 向量的容量（C++: `Vector::Resize(idx_t cur_size, idx_t new_size)`）。
    pub fn resize(&mut self, new_capacity: usize) {
        let elem_size = self.logical_type.physical_size();
        self.data.resize(new_capacity * elem_size, 0);
    }

    /// 应用选择向量，将向量切片为 `count` 行
    /// （C++: `Vector::Slice(const SelectionVector&, idx_t count)`）。
    pub fn slice(&mut self, sel: &SelectionVector, count: usize) {
        match self.vector_type {
            VectorType::Flat => {
                // 变为 Dictionary
                let child = Box::new(Self {
                    vector_type: VectorType::Flat,
                    logical_type: self.logical_type.clone(),
                    data: std::mem::take(&mut self.data),
                    validity: self.validity.clone(),
                    child: None,
                    struct_children: std::mem::take(&mut self.struct_children),
                    sel: None,
                    seq_start: 0,
                    seq_increment: 0,
                    string_aux: std::mem::take(&mut self.string_aux),
                });
                self.child = Some(child);
                self.sel = Some(SelectionVector {
                    indices: sel.indices[..count].to_vec(),
                });
                self.vector_type = VectorType::Dictionary;
            }
            VectorType::Constant => {} // 常量向量忽略 slice
            VectorType::Dictionary => {
                // 合并选择向量
                if let (Some(old_sel), Some(child)) = (self.sel.take(), self.child.as_ref()) {
                    let _ = child; // 子向量不变
                    let merged: Vec<u32> = (0..count)
                        .map(|i| old_sel.indices[sel.get_index(i)])
                        .collect();
                    self.sel = Some(SelectionVector { indices: merged });
                }
            }
            VectorType::Sequence => {
                todo!("Sequence 向量的 Slice")
            }
        }
    }

    /// 获取序列参数（C++: `SequenceVector::GetSequence()`）。
    pub fn get_sequence(&self) -> Option<(i64, i64)> {
        if self.vector_type == VectorType::Sequence {
            Some((self.seq_start, self.seq_increment))
        } else {
            None
        }
    }

    /// 内存占用估算（C++: `Vector::GetAllocationSize(idx_t cardinality)`）。
    pub fn allocation_size(&self, cardinality: usize) -> usize {
        self.data.capacity()
            + cardinality / 8 + 1   // validity bits
            + self.child.as_ref().map_or(0, |c| c.allocation_size(cardinality))
            + self
                .struct_children
                .iter()
                .map(|c| c.allocation_size(cardinality))
                .sum::<usize>()
            + self.string_aux.iter().map(|v| v.len()).sum::<usize>()
    }

    pub fn resolved_row_index(&self, row: usize) -> usize {
        match self.vector_type {
            VectorType::Constant => 0,
            VectorType::Dictionary => self
                .sel
                .as_ref()
                .map(|sel| sel.get_index(row))
                .unwrap_or(row),
            _ => row,
        }
    }

    pub fn row_is_valid(&self, row: usize) -> bool {
        match self.vector_type {
            VectorType::Dictionary => self
                .child
                .as_ref()
                .map(|child| child.row_is_valid(self.resolved_row_index(row)))
                .unwrap_or(true),
            VectorType::Sequence => true,
            _ => self.validity.row_is_valid(self.resolved_row_index(row)),
        }
    }

    fn row_is_valid_for_display(&self, row: usize) -> bool {
        self.row_is_valid(row)
    }

    fn format_value_at(&self, row: usize) -> String {
        if !self.row_is_valid_for_display(row) {
            return "NULL".to_string();
        }

        match self.vector_type {
            VectorType::Dictionary => {
                if let Some(child) = self.child.as_ref() {
                    return child.format_value_at(self.resolved_row_index(row));
                }
                return "<INVALID DICTIONARY>".to_string();
            }
            VectorType::Sequence => {
                let value = self.seq_start + (row as i64) * self.seq_increment;
                return value.to_string();
            }
            _ => {}
        }

        let row = self.resolved_row_index(row);
        let elem_size = self.logical_type.physical_size();
        let offset = row * elem_size;
        if offset + elem_size > self.data.len() {
            return "<OUT OF BOUNDS>".to_string();
        }

        let bytes = &self.data[offset..offset + elem_size];
        match self.logical_type.id {
            LogicalTypeId::Boolean => {
                if bytes[0] == 0 {
                    "false".to_string()
                } else {
                    "true".to_string()
                }
            }
            LogicalTypeId::TinyInt => i8::from_le_bytes([bytes[0]]).to_string(),
            LogicalTypeId::UTinyInt => bytes[0].to_string(),
            LogicalTypeId::SmallInt => i16::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::USmallInt => u16::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::Integer => i32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::UInteger => u32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::BigInt => i64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::UBigInt => u64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::Float => f32::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::Double => f64::from_le_bytes(bytes.try_into().unwrap()).to_string(),
            LogicalTypeId::Decimal => {
                let scaled = match self.logical_type.physical_size() {
                    2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i128,
                    4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i128,
                    8 => i64::from_le_bytes(bytes.try_into().unwrap()) as i128,
                    16 => i128::from_le_bytes(bytes.try_into().unwrap()),
                    _ => {
                        return format!(
                            "<DECIMAL({}, {})>",
                            self.logical_type.width, self.logical_type.scale
                        )
                    }
                };
                format_decimal_scaled_display(scaled, self.logical_type.scale)
            }
            LogicalTypeId::Varchar => {
                String::from_utf8_lossy(&self.read_varchar_bytes(row)).into_owned()
            }
            LogicalTypeId::Date => {
                let days = i32::from_le_bytes(bytes.try_into().unwrap());
                format_date_days_display(days)
            }
            LogicalTypeId::Blob => format!("<blob:{} bytes>", self.read_varchar_bytes(row).len()),
            _ => format!("<{}>", self.logical_type.name()),
        }
    }
}

impl std::fmt::Debug for Vector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Vector({:?}, {:?}, {} bytes)",
            self.logical_type.id,
            self.vector_type,
            self.data.len()
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// VectorCache（简化版）
// ─────────────────────────────────────────────────────────────────────────────

/// 向量缓存，用于 `DataChunk::Reset()` 时恢复 Flat 向量的初始缓冲区
/// （C++: `class VectorCache`）。
#[derive(Debug)]
struct VectorCache {
    logical_type: LogicalType,
    capacity: usize,
}

impl VectorCache {
    fn new(logical_type: LogicalType, capacity: usize) -> Self {
        Self {
            logical_type,
            capacity,
        }
    }

    /// 将 `vector` 重置为缓存时的初始状态（C++: `Vector::ResetFromCache()`）。
    fn restore(&self, vector: &mut Vector) {
        vector.vector_type = VectorType::Flat;
        vector.logical_type = self.logical_type.clone();
        let elem_size = self.logical_type.physical_size();
        vector.data = vec![0u8; self.capacity * elem_size];
        vector.validity.reset(self.capacity);
        vector.child = None;
        vector.sel = None;
        vector.seq_start = 0;
        vector.seq_increment = 0;
        vector.string_aux.clear();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataChunk
// ─────────────────────────────────────────────────────────────────────────────

/// 向量化数据块（C++: `class DataChunk`）。
///
/// 执行引擎的基本数据单元，持有一组等长的列向量。
/// - 不可复制（C++ 侧 `= delete`，Rust 侧不实现 `Clone`）。
/// - 通过 `&mut DataChunk` 传递给 Scan/Append 等操作。
pub struct DataChunk {
    // ── 列向量 ───────────────────────────────────────────────────────────
    /// 各列的向量（C++: `vector<Vector> data`）。
    pub data: Vec<Vector>,

    // ── 行计数 ───────────────────────────────────────────────────────────
    /// 当前包含的行数（C++: `idx_t count`）。
    count: usize,
    /// 最大行容量（C++: `idx_t capacity`）。
    capacity: usize,
    /// 初始容量（用于 Reset，C++: `idx_t initial_capacity`）。
    initial_capacity: usize,

    // ── 缓存 ─────────────────────────────────────────────────────────────
    /// 各列的向量缓存，供 `reset()` 使用（C++: `vector<VectorCache> vector_caches`）。
    caches: Vec<VectorCache>,
}

impl DataChunk {
    // ── 构造 ─────────────────────────────────────────────────────────────

    /// 创建空的 DataChunk，无列无数据（C++: `DataChunk()`）。
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            count: 0,
            capacity: STANDARD_VECTOR_SIZE,
            initial_capacity: STANDARD_VECTOR_SIZE,
            caches: Vec::new(),
        }
    }

    /// 用类型列表初始化，不分配缓冲区（C++: `InitializeEmpty(types)`）。
    ///
    /// 适用于不需要实际存储数据的"引用 chunk"。
    pub fn initialize_empty(&mut self, types: &[LogicalType]) {
        debug_assert!(
            self.data.is_empty(),
            "DataChunk::initialize_empty 只能在空 chunk 上调用"
        );
        self.capacity = STANDARD_VECTOR_SIZE;
        for t in types {
            self.data.push(Vector::new_empty(t.clone()));
        }
    }

    /// 用类型列表和容量初始化，分配各列缓冲区（C++: `Initialize(allocator, types, capacity)`）。
    pub fn initialize(&mut self, types: &[LogicalType], capacity: usize) {
        self.initialize_with_mask(types, &vec![true; types.len()], capacity);
    }

    /// 创建一个用于填充当前 chunk 的 builder。
    pub fn builder(types: &[LogicalType], capacity: usize) -> DataChunkBuilder {
        DataChunkBuilder::new(types, capacity)
    }

    /// 按 `initialize` 掩码选择性分配列缓冲区
    /// （C++: `Initialize(allocator, types, initialize, capacity)`）。
    pub fn initialize_with_mask(
        &mut self,
        types: &[LogicalType],
        init_mask: &[bool],
        capacity: usize,
    ) {
        debug_assert_eq!(types.len(), init_mask.len());
        debug_assert!(
            self.data.is_empty(),
            "DataChunk::initialize_with_mask 只能在空 chunk 上调用"
        );

        self.capacity = capacity;
        self.initial_capacity = capacity;

        for (i, t) in types.iter().enumerate() {
            if init_mask[i] {
                let vec = Vector::with_capacity(t.clone(), capacity);
                self.caches.push(VectorCache::new(t.clone(), capacity));
                self.data.push(vec);
            } else {
                self.data.push(Vector::new_empty(t.clone()));
                self.caches.push(VectorCache::new(t.clone(), 0));
            }
        }
    }

    // ── 行计数 ────────────────────────────────────────────────────────────

    /// 当前行数（C++: `size()`）。
    #[inline]
    pub fn size(&self) -> usize {
        self.count
    }

    /// 列数（C++: `ColumnCount()`）。
    #[inline]
    pub fn column_count(&self) -> usize {
        self.data.len()
    }

    /// 最大容量（C++: `GetCapacity()`）。
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 设置当前行数（C++: `SetCardinality(idx_t count)`）。
    ///
    /// `count` 不得超过 `capacity()`。
    #[inline]
    pub fn set_cardinality(&mut self, count: usize) {
        debug_assert!(
            count <= self.capacity,
            "DataChunk: cardinality {count} > capacity {}",
            self.capacity
        );
        self.count = count;
    }

    /// 从另一个 chunk 复制行数（C++: `SetCardinality(const DataChunk&)`）。
    pub fn set_cardinality_from(&mut self, other: &DataChunk) {
        self.set_cardinality(other.size());
    }

    /// 设置容量（C++: `SetCapacity(idx_t capacity)`）。
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }

    // ── 生命周期 ──────────────────────────────────────────────────────────

    /// 将 count 清零并从缓存恢复各列向量
    /// （C++: `DataChunk::Reset()`）。
    pub fn reset(&mut self) {
        self.count = 0;
        self.capacity = self.initial_capacity;
        if self.caches.len() != self.data.len() {
            // 无缓存时（initialize_empty 创建的 chunk）仅清零行数
            return;
        }
        for (i, cache) in self.caches.iter().enumerate() {
            cache.restore(&mut self.data[i]);
        }
    }

    /// 销毁所有列和缓存（C++: `DataChunk::Destroy()`）。
    pub fn destroy(&mut self) {
        self.data.clear();
        self.caches.clear();
        self.capacity = 0;
        self.count = 0;
    }

    // ── 数据操作 ──────────────────────────────────────────────────────────

    /// 将 `other` 的行追加到本 chunk（C++: `DataChunk::Append(other, resize, sel, count)`）。
    ///
    /// - `resize`：若容量不足则扩容（否则 panic）。
    /// - `sel` / `sel_count`：若有选择向量则只追加选中的行。
    pub fn append(
        &mut self,
        other: &DataChunk,
        resize: bool,
        sel: Option<(&SelectionVector, usize)>,
    ) {
        let append_count = sel.map_or(other.size(), |(_, c)| c);
        if append_count == 0 {
            return;
        }

        let new_size = self.count + append_count;
        if new_size > self.capacity {
            if resize {
                let new_cap = new_size.next_power_of_two();
                for col in self.data.iter_mut() {
                    col.resize(new_cap);
                }
                self.capacity = new_cap;
            } else {
                panic!(
                    "DataChunk::append: 容量不足（当前 {}, 需要 {}），请设置 resize=true",
                    self.capacity, new_size
                );
            }
        }

        for (i, dst_col) in self.data.iter_mut().enumerate() {
            if let Some((sel_vec, count)) = sel {
                dst_col.copy_from_sel(&other.data[i], sel_vec, count, self.count);
            } else {
                dst_col.copy_from(&other.data[i], append_count, 0, self.count);
            }
        }
        self.count = new_size;
    }

    /// 深拷贝到 `other`，从 `offset` 行开始（C++: `DataChunk::Copy(other, offset)`）。
    pub fn copy_to(&self, other: &mut DataChunk, offset: usize) {
        debug_assert_eq!(self.column_count(), other.column_count());
        debug_assert_eq!(other.size(), 0);
        let count = self.count.saturating_sub(offset);
        for i in 0..self.column_count() {
            other.data[i].copy_from(&self.data[i], count, offset, 0);
        }
        other.set_cardinality(count);
    }

    /// 按选择向量深拷贝（C++: `DataChunk::Copy(other, sel, source_count, offset)`）。
    pub fn copy_to_sel(
        &self,
        other: &mut DataChunk,
        sel: &SelectionVector,
        source_count: usize,
        offset: usize,
    ) {
        debug_assert_eq!(self.column_count(), other.column_count());
        debug_assert_eq!(other.size(), 0);
        let count = source_count.saturating_sub(offset);
        let offset_sel = SelectionVector::range(offset as u32, count);
        let _ = offset_sel; // 简化：直接使用 copy_from_sel
        for i in 0..self.column_count() {
            other.data[i].copy_from_sel(&self.data[i], sel, source_count, 0);
        }
        other.set_cardinality(source_count - offset);
    }

    /// 从列 `split_idx` 起将后半部分移到 `other`（C++: `DataChunk::Split(other, split_idx)`）。
    pub fn split(&mut self, other: &mut DataChunk, split_idx: usize) {
        debug_assert!(other.size() == 0 && other.data.is_empty());
        debug_assert!(split_idx < self.data.len());

        // 将后半段移走
        let cols: Vec<Vector> = self.data.drain(split_idx..).collect();
        let caches: Vec<VectorCache> = self.caches.drain(split_idx..).collect();
        other.data = cols;
        other.caches = caches;
        other.capacity = self.capacity;
        other.count = self.count;
    }

    /// 将 `other` 的所有列合并到本 chunk 右侧（C++: `DataChunk::Fuse(other)`）。
    pub fn fuse(&mut self, other: &mut DataChunk) {
        debug_assert_eq!(self.count, other.count);
        for col in other.data.drain(..) {
            self.data.push(col);
        }
        for cache in other.caches.drain(..) {
            self.caches.push(cache);
        }
        other.destroy();
    }

    /// 使本 chunk 的各列引用 `other` 对应列（C++: `DataChunk::Reference(chunk)`）。
    pub fn reference(&mut self, other: &DataChunk) {
        debug_assert!(other.column_count() <= self.column_count());
        self.set_capacity(other.capacity);
        self.set_cardinality(other.size());
        for i in 0..other.column_count() {
            self.data[i].reference(&other.data[i]);
        }
    }

    /// 使本 chunk 的各列引用 `other` 中 `column_ids` 指定的列
    /// （C++: `DataChunk::ReferenceColumns(other, column_ids)`）。
    pub fn reference_columns(&mut self, other: &DataChunk, column_ids: &[usize]) {
        debug_assert_eq!(self.column_count(), column_ids.len());
        self.reset();
        for (dst_idx, &src_idx) in column_ids.iter().enumerate() {
            self.data[dst_idx].reference(&other.data[src_idx]);
        }
        self.set_cardinality(other.size());
    }

    /// 将所有列展平为 Flat 向量（C++: `DataChunk::Flatten()`）。
    pub fn flatten(&mut self) {
        let count = self.count;
        for col in self.data.iter_mut() {
            col.flatten(count);
        }
    }

    /// 对所有列应用选择向量，切片为 `count` 行（C++: `DataChunk::Slice(sel, count)`）。
    pub fn slice(&mut self, sel: &SelectionVector, count: usize) {
        self.count = count;
        let sel_clone = sel.indices[..count].to_vec();
        let sel_ref = SelectionVector { indices: sel_clone };
        for col in self.data.iter_mut() {
            col.slice(&sel_ref, count);
        }
    }

    /// 从偏移 `offset` 开始切片 `count` 行（C++: `DataChunk::Slice(offset, count)`）。
    pub fn slice_range(&mut self, offset: usize, count: usize) {
        debug_assert!(offset + count <= self.count);
        let sel = SelectionVector::range(offset as u32, count);
        self.slice(&sel, count);
    }

    /// 将 `other` 的数据 move 进来并销毁 `other`（C++: `DataChunk::Move(other)`）。
    pub fn move_from(&mut self, other: &mut DataChunk) {
        self.count = other.count;
        self.capacity = other.capacity;
        self.data = std::mem::take(&mut other.data);
        self.caches = std::mem::take(&mut other.caches);
        other.count = 0;
        other.capacity = 0;
    }

    // ── 信息查询 ──────────────────────────────────────────────────────────

    /// 返回各列的逻辑类型列表（C++: `DataChunk::GetTypes()`）。
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.data.iter().map(|v| v.logical_type.clone()).collect()
    }

    /// 估算总内存占用（C++: `DataChunk::GetAllocationSize()`）。
    pub fn allocation_size(&self) -> usize {
        self.data
            .iter()
            .map(|v| v.allocation_size(self.count))
            .sum()
    }

    /// 所有列是否都是 Constant 向量（C++: `DataChunk::AllConstant()`）。
    pub fn all_constant(&self) -> bool {
        self.data
            .iter()
            .all(|v| v.get_vector_type() == VectorType::Constant)
    }

    /// 调试字符串（C++: `DataChunk::ToString()`）。
    pub fn to_string(&self) -> String {
        format!(
            "Chunk - [{} Columns, {} rows, capacity {}]",
            self.column_count(),
            self.count,
            self.capacity
        )
    }

    /// 以表格形式返回 chunk 内容，便于调试查看。
    pub fn to_pretty_string(&self) -> String {
        const MAX_DISPLAY_ROWS: usize = 12;
        const MAX_COLUMN_WIDTH: usize = 40;

        let mut headers = Vec::with_capacity(self.column_count());
        let display_rows = display_row_indexes(self.size(), MAX_DISPLAY_ROWS);
        let mut rows = Vec::with_capacity(display_rows.len());
        let mut widths = Vec::with_capacity(self.column_count());
        let mut align_right = Vec::with_capacity(self.column_count());

        for (idx, vector) in self.data.iter().enumerate() {
            let header = format!("c{idx}:{}", vector.logical_type.name());
            widths.push(header.chars().count());
            headers.push(header);
            align_right.push(should_right_align_display(&vector.logical_type));
        }

        for row_idx in display_rows {
            let mut row = Vec::with_capacity(self.column_count());
            for (col_idx, vector) in self.data.iter().enumerate() {
                let value = truncate_display_value(&vector.format_value_at(row_idx), MAX_COLUMN_WIDTH);
                widths[col_idx] = widths[col_idx].max(value.chars().count());
                row.push(value);
            }
            rows.push(row);
        }

        for width in &mut widths {
            *width = (*width).clamp(3, MAX_COLUMN_WIDTH);
        }

        let mut out = String::new();
        let _ = writeln!(
            out,
            "{}",
            self.to_string()
        );

        if self.column_count() == 0 {
            out.push_str("(empty chunk)\n");
            return out;
        }

        let separator = |widths: &[usize]| -> String {
            let mut line = String::new();
            line.push('+');
            for width in widths {
                line.push_str(&"-".repeat(*width + 2));
                line.push('+');
            }
            line
        };

        let border = separator(&widths);
        let _ = writeln!(out, "{border}");
        out.push('|');
        for (idx, header) in headers.iter().enumerate() {
            let _ = write!(out, " {:width$} |", header, width = widths[idx]);
        }
        out.push('\n');
        let _ = writeln!(out, "{border}");

        for row in rows {
            out.push('|');
            for (idx, cell) in row.iter().enumerate() {
                if align_right[idx] {
                    let _ = write!(out, " {:>width$} |", cell, width = widths[idx]);
                } else {
                    let _ = write!(out, " {:width$} |", cell, width = widths[idx]);
                }
            }
            out.push('\n');
        }

        let _ = writeln!(out, "{border}");
        if self.size() > MAX_DISPLAY_ROWS {
            let _ = writeln!(out, "... {} rows omitted in this chunk ...", self.size() - MAX_DISPLAY_ROWS);
        }
        out
    }

    /// 直接将 chunk 以表格形式输出到 stdout。
    pub fn pretty_print(&self) {
        print!("{}", self.to_pretty_string());
    }

    /// Debug 验证（C++: `DataChunk::Verify()`）。
    #[cfg(debug_assertions)]
    pub fn verify(&self) {
        assert!(self.count <= self.capacity, "DataChunk: count > capacity");
        for col in &self.data {
            match col.vector_type {
                VectorType::Flat => {
                    let expected = self.capacity * col.logical_type.physical_size();
                    assert!(
                        col.data.len() >= self.count * col.logical_type.physical_size(),
                        "Vector data buffer too small: {} < {}",
                        col.data.len(),
                        expected
                    );
                }
                _ => {}
            }
        }
    }

    #[cfg(not(debug_assertions))]
    pub fn verify(&self) {}

    // ── 序列化（存根）────────────────────────────────────────────────────

    /// 序列化到字节流（C++: `DataChunk::Serialize(serializer)`）。
    pub fn serialize(&self, _out: &mut Vec<u8>) {
        todo!("DataChunk::serialize — 依赖 BinarySerializer 实现")
    }

    /// 从字节流反序列化（C++: `DataChunk::Deserialize(source)`）。
    pub fn deserialize(&mut self, _src: &[u8]) {
        todo!("DataChunk::deserialize — 依赖 BinaryDeserializer 实现")
    }
}

/// 标准向量大小（C++: `STANDARD_VECTOR_SIZE = 2048`）。
pub const STANDARD_VECTOR_SIZE: usize = 2048;

impl Default for DataChunk {
    fn default() -> Self {
        Self::new()
    }
}

fn format_decimal_scaled_display(value: i128, scale: u8) -> String {
    let negative = value < 0;
    let digits = value.abs().to_string();
    let scale = scale as usize;
    let body = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        format!("-{}", body)
    } else {
        body
    }
}

fn format_date_days_display(days: i32) -> String {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid unix epoch");
    (epoch + Duration::days(days as i64)).format("%Y-%m-%d").to_string()
}

fn display_row_indexes(num_rows: usize, max_display_rows: usize) -> Vec<usize> {
    if num_rows <= max_display_rows {
        return (0..num_rows).collect();
    }
    let head = max_display_rows / 2;
    let tail = max_display_rows - head;
    (0..head).chain((num_rows - tail)..num_rows).collect()
}

fn truncate_display_value(value: &str, max_width: usize) -> String {
    if value.chars().count() <= max_width {
        return value.to_string();
    }
    let keep = max_width.saturating_sub(1);
    let prefix: String = value.chars().take(keep).collect();
    format!("{prefix}…")
}

fn should_right_align_display(logical_type: &LogicalType) -> bool {
    matches!(
        logical_type.id,
        LogicalTypeId::Boolean
            | LogicalTypeId::TinyInt
            | LogicalTypeId::UTinyInt
            | LogicalTypeId::SmallInt
            | LogicalTypeId::USmallInt
            | LogicalTypeId::Integer
            | LogicalTypeId::UInteger
            | LogicalTypeId::BigInt
            | LogicalTypeId::UBigInt
            | LogicalTypeId::Float
            | LogicalTypeId::Double
            | LogicalTypeId::Decimal
    )
}

/// `DataChunk` 的便捷构造器。
///
/// 对外暴露按单元格写值的接口，隐藏底层字节布局细节。
pub struct DataChunkBuilder {
    chunk: DataChunk,
    row_count: usize,
}

impl DataChunkBuilder {
    pub fn new(types: &[LogicalType], capacity: usize) -> Self {
        let mut chunk = DataChunk::new();
        chunk.initialize(types, capacity);
        Self { chunk, row_count: 0 }
    }

    pub fn finish(mut self) -> DataChunk {
        self.chunk.set_cardinality(self.row_count);
        self.chunk
    }

    fn touch_row(&mut self, row: usize) {
        self.row_count = self.row_count.max(row + 1);
    }

    fn vector_mut(
        &mut self,
        col: usize,
        row: usize,
        expected: &[LogicalTypeId],
    ) -> Result<&mut Vector> {
        if col >= self.chunk.column_count() {
            bail!(
                "DataChunkBuilder: 列索引越界，col={}，column_count={}",
                col,
                self.chunk.column_count()
            );
        }
        if row >= self.chunk.capacity() {
            bail!(
                "DataChunkBuilder: 行索引越界，row={}，capacity={}",
                row,
                self.chunk.capacity()
            );
        }
        let actual_id = self.chunk.data[col].logical_type.id;
        let actual_name = self.chunk.data[col].logical_type.name();
        if !expected.contains(&actual_id) {
            let expected_names = expected
                .iter()
                .map(|id| LogicalType::new(*id).name())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "DataChunkBuilder: 第 {} 列类型不匹配，expected [{}]，actual {}",
                col,
                expected_names,
                actual_name
            );
        }
        self.touch_row(row);
        Ok(&mut self.chunk.data[col])
    }

    fn write_bytes(
        &mut self,
        col: usize,
        row: usize,
        expected: &[LogicalTypeId],
        bytes: &[u8],
    ) -> Result<&mut Self> {
        let vector = self.vector_mut(col, row, expected)?;
        let elem_size = vector.logical_type.physical_size();
        if bytes.len() != elem_size {
            bail!(
                "DataChunkBuilder: 写入字节长度不匹配，expected {}，actual {}",
                elem_size,
                bytes.len()
            );
        }
        let offset = row * elem_size;
        vector.raw_data_mut()[offset..offset + elem_size].copy_from_slice(bytes);
        vector.validity.set_valid(row);
        Ok(self)
    }

    pub fn set_null(&mut self, col: usize, row: usize) -> Result<&mut Self> {
        let vector = self.vector_mut(col, row, &[
            LogicalTypeId::Boolean,
            LogicalTypeId::TinyInt,
            LogicalTypeId::SmallInt,
            LogicalTypeId::Integer,
            LogicalTypeId::BigInt,
            LogicalTypeId::HugeInt,
            LogicalTypeId::Float,
            LogicalTypeId::Double,
            LogicalTypeId::Decimal,
            LogicalTypeId::Varchar,
            LogicalTypeId::Timestamp,
            LogicalTypeId::Date,
            LogicalTypeId::Time,
            LogicalTypeId::Interval,
            LogicalTypeId::UInteger,
            LogicalTypeId::UTinyInt,
            LogicalTypeId::USmallInt,
            LogicalTypeId::UBigInt,
            LogicalTypeId::Blob,
        ])?;
        let elem_size = vector.logical_type.physical_size();
        let offset = row * elem_size;
        vector.raw_data_mut()[offset..offset + elem_size].fill(0);
        vector.validity.set_invalid(row);
        Ok(self)
    }

    pub fn set_i32(&mut self, col: usize, row: usize, value: i32) -> Result<&mut Self> {
        self.write_bytes(
            col,
            row,
            &[LogicalTypeId::Integer, LogicalTypeId::Date],
            &value.to_le_bytes(),
        )
    }

    pub fn set_i64(&mut self, col: usize, row: usize, value: i64) -> Result<&mut Self> {
        self.write_bytes(col, row, &[LogicalTypeId::BigInt], &value.to_le_bytes())
    }

    pub fn set_f32(&mut self, col: usize, row: usize, value: f32) -> Result<&mut Self> {
        self.write_bytes(col, row, &[LogicalTypeId::Float], &value.to_le_bytes())
    }

    pub fn set_f64(&mut self, col: usize, row: usize, value: f64) -> Result<&mut Self> {
        self.write_bytes(col, row, &[LogicalTypeId::Double], &value.to_le_bytes())
    }

    pub fn set_bool(&mut self, col: usize, row: usize, value: bool) -> Result<&mut Self> {
        self.write_bytes(
            col,
            row,
            &[LogicalTypeId::Boolean],
            &[if value { 1 } else { 0 }],
        )
    }

    pub fn set_varchar_inline(
        &mut self,
        col: usize,
        row: usize,
        value: &str,
    ) -> Result<&mut Self> {
        let vector = self.vector_mut(col, row, &[LogicalTypeId::Varchar])?;
        let bytes = value.as_bytes();
        if bytes.len() > 12 {
            bail!(
                "DataChunkBuilder: inline varchar 仅支持 <= 12 字节，actual {}",
                bytes.len()
            );
        }
        vector.write_varchar_bytes(row, bytes);
        Ok(self)
    }
}

// DataChunk 不可 Clone（C++: `DataChunk(const DataChunk&) = delete`）
// 在 Rust 中不实现 Clone trait 即可。

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn types2() -> Vec<LogicalType> {
        vec![LogicalType::integer(), LogicalType::bigint()]
    }

    // ── LogicalType ──────────────────────────────────────────────────────────

    #[test]
    fn logical_type_physical_sizes() {
        assert_eq!(LogicalType::boolean().physical_size(), 1);
        assert_eq!(LogicalType::integer().physical_size(), 4);
        assert_eq!(LogicalType::bigint().physical_size(), 8);
        assert_eq!(LogicalType::double().physical_size(), 8);
        assert_eq!(LogicalType::hugeint().physical_size(), 16);
    }

    // ── SelectionVector ──────────────────────────────────────────────────────

    #[test]
    fn selection_vector_identity() {
        let sel = SelectionVector::identity(5);
        assert_eq!(sel.len(), 5);
        for i in 0..5 {
            assert_eq!(sel.get_index(i), i);
        }
    }

    #[test]
    fn selection_vector_range() {
        let sel = SelectionVector::range(3, 4);
        assert_eq!(sel.get_index(0), 3);
        assert_eq!(sel.get_index(3), 6);
    }

    // ── ValidityMask ─────────────────────────────────────────────────────────

    #[test]
    fn validity_mask_all_valid() {
        let mask = ValidityMask::new(100);
        for i in 0..100 {
            assert!(mask.row_is_valid(i));
        }
    }

    #[test]
    fn validity_mask_set_invalid() {
        let mut mask = ValidityMask::new(64);
        mask.set_invalid(5);
        assert!(!mask.row_is_valid(5));
        assert!(mask.row_is_valid(4));
        assert!(mask.row_is_valid(6));
    }

    #[test]
    fn validity_mask_all_invalid() {
        let mask = ValidityMask::all_invalid(10);
        for i in 0..10 {
            assert!(!mask.row_is_valid(i));
        }
    }

    // ── Vector ───────────────────────────────────────────────────────────────

    #[test]
    fn vector_with_capacity_has_right_size() {
        let v = Vector::with_capacity(LogicalType::integer(), 100);
        // INTEGER = 4 bytes per row → 400 bytes
        assert_eq!(v.raw_data().len(), 400);
    }

    #[test]
    fn vector_flatten_constant() {
        let mut v = Vector::new_constant(LogicalType::integer());
        // 写入常量值 42
        v.raw_data_mut()[..4].copy_from_slice(&42i32.to_le_bytes());
        v.flatten(3);
        assert_eq!(v.get_vector_type(), VectorType::Flat);
        assert_eq!(v.raw_data().len(), 12); // 3 × 4 bytes
        let val = i32::from_le_bytes(v.raw_data()[0..4].try_into().unwrap());
        assert_eq!(val, 42);
    }

    #[test]
    fn vector_flatten_sequence() {
        let mut v = Vector::new_sequence(LogicalType::bigint(), 10, 2);
        v.flatten(4);
        assert_eq!(v.get_vector_type(), VectorType::Flat);
        let data = v.raw_data();
        // 10, 12, 14, 16
        for i in 0..4usize {
            let val = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(val, 10 + 2 * i as i64);
        }
    }

    #[test]
    fn vector_copy_row() {
        let mut src = Vector::with_capacity(LogicalType::integer(), 4);
        src.raw_data_mut()[0..4].copy_from_slice(&100i32.to_le_bytes());
        src.raw_data_mut()[4..8].copy_from_slice(&200i32.to_le_bytes());

        let mut dst = Vector::with_capacity(LogicalType::integer(), 4);
        dst.copy_row_from(&src, 1, 0); // copy row 1 (200) to dst row 0
        let val = i32::from_le_bytes(dst.raw_data()[0..4].try_into().unwrap());
        assert_eq!(val, 200);
    }

    // ── DataChunk ────────────────────────────────────────────────────────────

    #[test]
    fn data_chunk_new_is_empty() {
        let chunk = DataChunk::new();
        assert_eq!(chunk.size(), 0);
        assert_eq!(chunk.column_count(), 0);
        assert_eq!(chunk.capacity(), STANDARD_VECTOR_SIZE);
    }

    #[test]
    fn data_chunk_initialize_empty() {
        let mut chunk = DataChunk::new();
        chunk.initialize_empty(&types2());
        assert_eq!(chunk.column_count(), 2);
        assert_eq!(chunk.size(), 0);
    }

    #[test]
    fn data_chunk_initialize_with_capacity() {
        let mut chunk = DataChunk::new();
        chunk.initialize(&types2(), 512);
        assert_eq!(chunk.column_count(), 2);
        assert_eq!(chunk.capacity(), 512);
    }

    #[test]
    fn data_chunk_set_and_get_cardinality() {
        let mut chunk = DataChunk::new();
        chunk.initialize(&types2(), 100);
        chunk.set_cardinality(50);
        assert_eq!(chunk.size(), 50);
    }

    #[test]
    fn data_chunk_reset() {
        let mut chunk = DataChunk::new();
        chunk.initialize(&types2(), 64);
        chunk.set_cardinality(20);
        chunk.reset();
        assert_eq!(chunk.size(), 0);
        assert_eq!(chunk.capacity(), 64);
        // Flat 向量的缓冲区应已重置
        assert_eq!(chunk.data[0].raw_data().len(), 64 * 4); // INTEGER = 4 bytes
    }

    #[test]
    fn data_chunk_destroy() {
        let mut chunk = DataChunk::new();
        chunk.initialize(&types2(), 64);
        chunk.destroy();
        assert_eq!(chunk.column_count(), 0);
        assert_eq!(chunk.capacity(), 0);
    }

    #[test]
    fn data_chunk_get_types() {
        let mut chunk = DataChunk::new();
        chunk.initialize(&types2(), 10);
        let types = chunk.get_types();
        assert_eq!(types[0].id, LogicalTypeId::Integer);
        assert_eq!(types[1].id, LogicalTypeId::BigInt);
    }

    #[test]
    fn data_chunk_split_and_fuse() {
        let mut a = DataChunk::new();
        a.initialize(
            &[
                LogicalType::integer(),
                LogicalType::bigint(),
                LogicalType::double(),
            ],
            4,
        );
        a.set_cardinality(4);

        let mut b = DataChunk::new();
        a.split(&mut b, 1); // a 保留列 0，b 得到列 1、2

        assert_eq!(a.column_count(), 1);
        assert_eq!(b.column_count(), 2);

        a.fuse(&mut b);
        assert_eq!(a.column_count(), 3);
        assert_eq!(b.column_count(), 0);
    }

    #[test]
    fn data_chunk_all_constant() {
        let mut chunk = DataChunk::new();
        chunk
            .data
            .push(Vector::new_constant(LogicalType::integer()));
        chunk.data.push(Vector::new_constant(LogicalType::bigint()));
        assert!(chunk.all_constant());
        chunk
            .data
            .push(Vector::with_capacity(LogicalType::float(), 10));
        assert!(!chunk.all_constant());
    }

    #[test]
    fn data_chunk_flatten() {
        let mut chunk = DataChunk::new();
        chunk
            .data
            .push(Vector::new_constant(LogicalType::integer()));
        chunk.set_cardinality(5);
        chunk.flatten();
        assert_eq!(chunk.data[0].get_vector_type(), VectorType::Flat);
    }

    #[test]
    fn data_chunk_append_no_resize() {
        let mut dst = DataChunk::new();
        dst.initialize(&[LogicalType::integer()], 10);

        let mut src = DataChunk::new();
        src.initialize(&[LogicalType::integer()], 4);
        // 写入源数据
        src.data[0].raw_data_mut()[..16]
            .copy_from_slice(&[1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0]);
        src.set_cardinality(4);

        dst.append(&src, false, None);
        assert_eq!(dst.size(), 4);
    }

    #[test]
    fn data_chunk_to_string() {
        let mut chunk = DataChunk::new();
        chunk.initialize(&types2(), 8);
        chunk.set_cardinality(3);
        let s = chunk.to_string();
        assert!(s.contains("2 Columns"));
        assert!(s.contains("3 rows"));
    }

    #[test]
    fn data_chunk_to_pretty_string_shows_rows() {
        let types = vec![
            LogicalType::integer(),
            LogicalType::varchar(),
            LogicalType::boolean(),
        ];
        let mut builder = DataChunkBuilder::new(&types, 2);
        builder.set_i32(0, 0, 42).unwrap();
        builder.set_varchar_inline(1, 0, "Alice").unwrap();
        builder.set_bool(2, 0, true).unwrap();
        builder.set_null(1, 1).unwrap();
        builder.set_bool(2, 1, false).unwrap();
        let chunk = builder.finish();

        let pretty = chunk.to_pretty_string();
        assert!(pretty.contains("c0:INTEGER"));
        assert!(pretty.contains("c1:VARCHAR"));
        assert!(pretty.contains("42"));
        assert!(pretty.contains("Alice"));
        assert!(pretty.contains("NULL"));
        assert!(pretty.contains("false"));
    }

    #[test]
    fn data_chunk_to_pretty_string_handles_non_flat_vectors() {
        let mut chunk = DataChunk::new();
        chunk.data.push(Vector::new_constant(LogicalType::integer()));
        chunk.data.push(Vector::new_sequence(LogicalType::bigint(), 10, 2));
        chunk.set_cardinality(3);
        chunk.data[0].raw_data_mut()[..4].copy_from_slice(&7i32.to_le_bytes());

        let pretty = chunk.to_pretty_string();
        assert!(pretty.contains("7"));
        assert!(pretty.contains("10"));
        assert!(pretty.contains("12"));
        assert!(pretty.contains("14"));
    }

    #[test]
    fn data_chunk_to_pretty_string_formats_decimal_and_date() {
        let types = vec![LogicalType::decimal(18, 2), LogicalType::date()];
        let mut builder = DataChunkBuilder::new(&types, 1);
        builder.set_i32(1, 0, 9568).unwrap();
        let mut chunk = builder.finish();
        chunk.data[0].raw_data_mut()[..8].copy_from_slice(&12345i64.to_le_bytes());

        let pretty = chunk.to_pretty_string();
        assert!(pretty.contains("123.45"));
        assert!(pretty.contains("1996-03-13"));
    }

    #[test]
    fn data_chunk_to_pretty_string_omits_middle_rows_for_large_chunks() {
        let types = vec![LogicalType::integer()];
        let mut builder = DataChunkBuilder::new(&types, 16);
        for row in 0..16 {
            builder.set_i32(0, row, row as i32).unwrap();
        }
        let chunk = builder.finish();

        let pretty = chunk.to_pretty_string();
        assert!(pretty.contains("... 4 rows omitted in this chunk ..."));
        assert!(pretty.contains("|          0 |"));
        assert!(pretty.contains("|         15 |"));
    }

    #[test]
    fn data_chunk_verify_passes() {
        let mut chunk = DataChunk::new();
        chunk.initialize(&types2(), 64);
        chunk.set_cardinality(10);
        chunk.verify(); // should not panic
    }

    #[test]
    fn data_chunk_builder_writes_mixed_values() {
        let types = vec![
            LogicalType::integer(),
            LogicalType::varchar(),
            LogicalType::boolean(),
            LogicalType::double(),
        ];
        let mut builder = DataChunkBuilder::new(&types, 2);
        builder.set_i32(0, 0, 42).unwrap();
        builder.set_varchar_inline(1, 0, "Alice").unwrap();
        builder.set_bool(2, 0, true).unwrap();
        builder.set_f64(3, 0, 88.5).unwrap();
        builder.set_null(1, 1).unwrap();
        let chunk = builder.finish();

        assert_eq!(chunk.size(), 2);
        assert_eq!(
            i32::from_le_bytes(chunk.data[0].raw_data()[..4].try_into().unwrap()),
            42
        );
        assert!(!chunk.data[1].validity.row_is_valid(1));
        assert_eq!(chunk.data[2].raw_data()[0], 1);
    }
}
