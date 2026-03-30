//! 依赖关系类型。
//!
//! 对应 C++:
//!   - `duckdb/catalog/dependency.hpp`
//!   - `duckdb/catalog/dependency_list.hpp`
//!   - `duckdb/catalog/catalog_entry/dependency/dependency_entry.hpp`
//!   - `duckdb/catalog/catalog_entry/dependency/dependency_subject_entry.hpp`
//!   - `duckdb/catalog/catalog_entry/dependency/dependency_dependent_entry.hpp`
//!
//! # 设计说明
//!
//! | C++ | Rust |
//! |-----|------|
//! | `DependencyFlags` (abstract with `uint8_t value`) | `DependencyFlagsBase` struct + newtypes |
//! | `DependencySubjectFlags` | `DependencySubjectFlags` |
//! | `DependencyDependentFlags` | `DependencyDependentFlags` |
//! | `LogicalDependency` | `LogicalDependency` |
//! | `LogicalDependencyList` | `LogicalDependencyList` |
//! | `MangledEntryName` / `MangledDependencyName` | 同名结构体 |

use super::error::CatalogError;
use super::types::CatalogType;
use std::collections::HashSet;

// ─── CatalogEntryInfo ──────────────────────────────────────────────────────────

/// Catalog 条目的标识三元组（C++: `CatalogEntryInfo`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogEntryInfo {
    pub entry_type: CatalogType,
    pub schema: String,
    pub name: String,
}

impl CatalogEntryInfo {
    pub fn new(
        entry_type: CatalogType,
        schema: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            entry_type,
            schema: schema.into(),
            name: name.into(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(self.entry_type as u8);
        let sb = self.schema.as_bytes();
        buf.extend_from_slice(&(sb.len() as u32).to_le_bytes());
        buf.extend_from_slice(sb);
        let nb = self.name.as_bytes();
        buf.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        buf.extend_from_slice(nb);
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.is_empty() {
            return None;
        }
        let mut pos = 0;
        let entry_type = CatalogType::from_u8(data[pos]);
        pos += 1;
        if data.len() < pos + 4 {
            return None;
        }
        let slen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if data.len() < pos + slen {
            return None;
        }
        let schema = String::from_utf8(data[pos..pos + slen].to_vec()).ok()?;
        pos += slen;
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
        Some((
            Self {
                entry_type,
                schema,
                name,
            },
            pos,
        ))
    }
}

// ─── DependencySubjectFlags ────────────────────────────────────────────────────

/// Subject（被依赖方）标志位（C++: `DependencySubjectFlags`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DependencySubjectFlags(u8);

impl DependencySubjectFlags {
    const OWNERSHIP: u8 = 1 << 0;

    pub fn new() -> Self {
        Self(0)
    }

    pub fn is_ownership(&self) -> bool {
        self.0 & Self::OWNERSHIP != 0
    }
    pub fn set_ownership(&mut self) {
        self.0 |= Self::OWNERSHIP;
    }

    pub fn merge(&mut self, other: Self) {
        self.0 |= other.0;
    }

    pub fn value(&self) -> u8 {
        self.0
    }

    pub fn from_u8(v: u8) -> Self {
        Self(v)
    }
}

impl std::fmt::Display for DependencySubjectFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_ownership() {
            write!(f, "OWNERSHIP")
        } else {
            write!(f, "NONE")
        }
    }
}

// ─── DependencyDependentFlags ──────────────────────────────────────────────────

/// Dependent（依赖方）标志位（C++: `DependencyDependentFlags`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DependencyDependentFlags(u8);

impl DependencyDependentFlags {
    const BLOCKING: u8 = 1 << 0;
    const OWNED_BY: u8 = 1 << 1;

    pub fn new() -> Self {
        Self(0)
    }

    pub fn is_blocking(&self) -> bool {
        self.0 & Self::BLOCKING != 0
    }
    pub fn is_owned_by(&self) -> bool {
        self.0 & Self::OWNED_BY != 0
    }
    pub fn set_blocking(&mut self) {
        self.0 |= Self::BLOCKING;
    }
    pub fn set_owned_by(&mut self) {
        self.0 |= Self::OWNED_BY;
    }

    pub fn merge(&mut self, other: Self) {
        self.0 |= other.0;
    }

    pub fn value(&self) -> u8 {
        self.0
    }

    pub fn from_u8(v: u8) -> Self {
        Self(v)
    }
}

impl std::fmt::Display for DependencyDependentFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.is_blocking() {
            parts.push("BLOCKING");
        }
        if self.is_owned_by() {
            parts.push("OWNED_BY");
        }
        if parts.is_empty() {
            parts.push("NONE");
        }
        write!(f, "{}", parts.join("|"))
    }
}

// ─── DependencySubject / DependencyDependent ───────────────────────────────────

/// 被依赖的对象（C++: `DependencySubject`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencySubject {
    pub entry: CatalogEntryInfo,
    pub flags: DependencySubjectFlags,
}

/// 依赖于某对象的对象（C++: `DependencyDependent`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyDependent {
    pub entry: CatalogEntryInfo,
    pub flags: DependencyDependentFlags,
}

/// 完整的依赖关系（C++: `DependencyInfo`）。
#[derive(Debug, Clone)]
pub struct DependencyInfo {
    pub dependent: DependencyDependent,
    pub subject: DependencySubject,
}

impl DependencyInfo {
    pub fn new(subject: DependencySubject, dependent: DependencyDependent) -> Self {
        Self { subject, dependent }
    }
}

// ─── LogicalDependency ─────────────────────────────────────────────────────────

/// 逻辑依赖（序列化时使用，不直接引用活跃对象）（C++: `LogicalDependency`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalDependency {
    pub entry: CatalogEntryInfo,
    pub catalog: String,
}

impl LogicalDependency {
    pub fn new(entry: CatalogEntryInfo, catalog: impl Into<String>) -> Self {
        Self {
            entry,
            catalog: catalog.into(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = self.entry.serialize();
        let cb = self.catalog.as_bytes();
        buf.extend_from_slice(&(cb.len() as u32).to_le_bytes());
        buf.extend_from_slice(cb);
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        let (entry, mut pos) = CatalogEntryInfo::deserialize(data)?;
        if data.len() < pos + 4 {
            return None;
        }
        let clen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if data.len() < pos + clen {
            return None;
        }
        let catalog = String::from_utf8(data[pos..pos + clen].to_vec()).ok()?;
        pos += clen;
        Some((Self { entry, catalog }, pos))
    }
}

// ─── LogicalDependencyList ─────────────────────────────────────────────────────

/// 逻辑依赖集合（C++: `LogicalDependencyList`）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogicalDependencyList {
    set: HashSet<LogicalDependency>,
}

impl LogicalDependencyList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, dep: LogicalDependency) {
        self.set.insert(dep);
    }

    pub fn add_entry(
        &mut self,
        entry_type: CatalogType,
        schema: impl Into<String>,
        name: impl Into<String>,
        catalog: impl Into<String>,
    ) {
        self.add(LogicalDependency::new(
            CatalogEntryInfo::new(entry_type, schema, name),
            catalog,
        ));
    }

    pub fn contains(&self, dep: &LogicalDependency) -> bool {
        self.set.contains(dep)
    }

    pub fn contains_entry(&self, info: &CatalogEntryInfo) -> bool {
        self.set.iter().any(|d| &d.entry == info)
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &LogicalDependency> {
        self.set.iter()
    }

    /// 验证依赖是否都存在（用于写入前校验）（C++: `VerifyDependencies`）。
    pub fn verify_dependencies(
        &self,
        catalog_name: &str,
        entry_name: &str,
    ) -> Result<(), CatalogError> {
        // 在 Rust 中无法在此直接访问 Catalog，调用方需要在更高层验证。
        // 此处仅做基本检查（空名称等）。
        for dep in &self.set {
            if dep.entry.name.is_empty() {
                return Err(CatalogError::invalid(format!(
                    "Entry \"{}\" in catalog \"{}\" has a dependency with an empty name",
                    entry_name, catalog_name
                )));
            }
        }
        Ok(())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.set.len() as u32).to_le_bytes());
        for dep in &self.set {
            let db = dep.serialize();
            buf.extend_from_slice(&(db.len() as u32).to_le_bytes());
            buf.extend_from_slice(&db);
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        let mut pos = 0;
        if data.len() < 4 {
            return None;
        }
        let count = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        let mut set = HashSet::new();
        for _ in 0..count {
            if data.len() < pos + 4 {
                return None;
            }
            let dlen = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
            pos += 4;
            if data.len() < pos + dlen {
                return None;
            }
            let (dep, _) = LogicalDependency::deserialize(&data[pos..pos + dlen])?;
            pos += dlen;
            set.insert(dep);
        }
        Some((Self { set }, pos))
    }
}

// ─── MangledEntryName ──────────────────────────────────────────────────────────

/// 路径混淆名称，用于 dependency CatalogSet 的键（C++: `MangledEntryName`）。
///
/// 格式：`"{type}\0{schema}\0{name}"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MangledEntryName {
    pub name: String,
}

impl MangledEntryName {
    pub fn new(info: &CatalogEntryInfo) -> Self {
        Self {
            name: format!("{}\0{}\0{}", info.entry_type as u8, info.schema, info.name),
        }
    }

    pub fn from_parts(entry_type: CatalogType, schema: &str, name: &str) -> Self {
        Self::new(&CatalogEntryInfo::new(entry_type, schema, name))
    }
}

impl std::fmt::Display for MangledEntryName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// 混淆的依赖名称（C++: `MangledDependencyName`）。
///
/// 格式：`"{mangled_subject}\0{mangled_dependent}"`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MangledDependencyName {
    pub name: String,
}

impl MangledDependencyName {
    pub fn new(subject: &MangledEntryName, dependent: &MangledEntryName) -> Self {
        Self {
            name: format!("{}\0{}", subject.name, dependent.name),
        }
    }
}
