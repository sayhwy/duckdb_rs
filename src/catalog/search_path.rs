//! Catalog 搜索路径。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog_search_path.hpp`（`CatalogSearchEntry`, `CatalogSearchPath`）
//!
//! # 设计说明
//!
//! | C++ | Rust |
//! |-----|------|
//! | `CatalogSearchEntry` (struct) | `CatalogSearchEntry` |
//! | `CatalogSearchPath` (class) | `CatalogSearchPath` |
//! | `ClientContext& context` | 去除，改为值传入 catalog_name |

// ─── CatalogSearchEntry ────────────────────────────────────────────────────────

/// 搜索路径中的一个 catalog.schema 条目（C++: `CatalogSearchEntry`）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CatalogSearchEntry {
    pub catalog: String,
    pub schema: String,
}

impl CatalogSearchEntry {
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
        }
    }

    /// 解析 `"catalog.schema"` 或 `"schema"` 格式的字符串。
    ///
    /// （C++: `CatalogSearchEntry::Parse`）
    pub fn parse(s: &str) -> Self {
        let parts: Vec<&str> = s.splitn(2, '.').collect();
        if parts.len() == 2 {
            Self::new(parts[0], parts[1])
        } else {
            Self::new("", s)
        }
    }

    /// 解析以逗号分隔的搜索路径列表（C++: `CatalogSearchEntry::ParseList`）。
    pub fn parse_list(s: &str) -> Vec<Self> {
        s.split(',').map(|p| Self::parse(p.trim())).collect()
    }

    /// 转换为字符串（C++: `CatalogSearchEntry::ToString`）。
    pub fn to_string_repr(&self) -> String {
        if self.catalog.is_empty() {
            self.schema.clone()
        } else {
            format!("{}.{}", self.catalog, self.schema)
        }
    }

    /// 列表转字符串（C++: `CatalogSearchEntry::ListToString`）。
    pub fn list_to_string(entries: &[Self]) -> String {
        entries
            .iter()
            .map(|e| e.to_string_repr())
            .collect::<Vec<_>>()
            .join(",")
    }
}

// ─── CatalogSetPathType ────────────────────────────────────────────────────────

/// 搜索路径设置类型（C++: `CatalogSearchPath::CatalogSetPathType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogSetPathType {
    /// 设置单个 schema（C++: `SET_SCHEMA`）。
    SetSchema,
    /// 设置多个 schema（C++: `SET_SCHEMAS`）。
    SetSchemas,
    /// 直接设置完整路径（C++: `SET_DIRECTLY`）。
    SetDirectly,
}

// ─── CatalogSearchPath ────────────────────────────────────────────────────────

/// Catalog 搜索路径（C++: `CatalogSearchPath`）。
///
/// 维护按优先级排序的 `(catalog, schema)` 搜索路径列表。
#[derive(Debug, Clone)]
pub struct CatalogSearchPath {
    /// 完整的搜索路径（包括隐式路径）（C++: `vector<CatalogSearchEntry> paths`）。
    paths: Vec<CatalogSearchEntry>,
    /// 用户明确设置的路径（C++: `vector<CatalogSearchEntry> set_paths`）。
    set_paths: Vec<CatalogSearchEntry>,
    /// 默认 catalog 名称。
    default_catalog: String,
}

impl CatalogSearchPath {
    /// 创建默认搜索路径（`main` schema）。
    pub fn new(default_catalog: impl Into<String>) -> Self {
        let catalog = default_catalog.into();
        let default_entry = CatalogSearchEntry::new(catalog.clone(), "main");
        Self {
            paths: vec![default_entry.clone()],
            set_paths: vec![default_entry],
            default_catalog: catalog,
        }
    }

    /// 设置搜索路径（C++: `CatalogSearchPath::Set`）。
    pub fn set(&mut self, entries: Vec<CatalogSearchEntry>, path_type: CatalogSetPathType) {
        match path_type {
            CatalogSetPathType::SetSchema => {
                // 单 schema 设置：将其插入路径首位
                self.set_paths = entries.clone();
                self.rebuild_paths(entries);
            }
            CatalogSetPathType::SetSchemas | CatalogSetPathType::SetDirectly => {
                self.set_paths = entries.clone();
                self.rebuild_paths(entries);
            }
        }
    }

    /// 重置为默认路径（C++: `CatalogSearchPath::Reset`）。
    pub fn reset(&mut self) {
        let default_entry = CatalogSearchEntry::new(self.default_catalog.clone(), "main");
        self.paths = vec![default_entry.clone()];
        self.set_paths = vec![default_entry];
    }

    /// 获取当前完整搜索路径（C++: `CatalogSearchPath::Get`）。
    pub fn get(&self) -> &[CatalogSearchEntry] {
        &self.paths
    }

    /// 获取用户明确设置的路径（C++: `CatalogSearchPath::GetSetPaths`）。
    pub fn get_set_paths(&self) -> &[CatalogSearchEntry] {
        &self.set_paths
    }

    /// 获取默认 schema 名称（C++: `CatalogSearchPath::GetDefault`）。
    pub fn get_default_entry(&self) -> CatalogSearchEntry {
        self.paths
            .first()
            .cloned()
            .unwrap_or_else(|| CatalogSearchEntry {
                catalog: self.default_catalog.clone(),
                schema: "main".to_string(),
            })
    }

    /// 获取指定 catalog 的默认 schema（C++: `GetDefaultSchema(catalog)`）。
    pub fn get_default_schema(&self, catalog: &str) -> String {
        for entry in &self.paths {
            if entry.catalog.eq_ignore_ascii_case(catalog) {
                return entry.schema.clone();
            }
        }
        "main".to_string()
    }

    /// 获取指定 schema 的默认 catalog（C++: `GetDefaultCatalog(schema)`）。
    pub fn get_default_catalog(&self, schema: &str) -> String {
        for entry in &self.paths {
            if entry.schema.eq_ignore_ascii_case(schema) {
                return entry.catalog.clone();
            }
        }
        self.default_catalog.clone()
    }

    /// 获取指定 catalog 的所有 schema（C++: `GetSchemasForCatalog`）。
    pub fn get_schemas_for_catalog(&self, catalog: &str) -> Vec<String> {
        self.paths
            .iter()
            .filter(|e| e.catalog.eq_ignore_ascii_case(catalog))
            .map(|e| e.schema.clone())
            .collect()
    }

    /// 获取指定 schema 的所有 catalog（C++: `GetCatalogsForSchema`）。
    pub fn get_catalogs_for_schema(&self, schema: &str) -> Vec<String> {
        self.paths
            .iter()
            .filter(|e| e.schema.eq_ignore_ascii_case(schema))
            .map(|e| e.catalog.clone())
            .collect()
    }

    /// 判断 (catalog, schema) 是否在搜索路径中（C++: `SchemaInSearchPath`）。
    pub fn schema_in_search_path(&self, catalog: &str, schema: &str) -> bool {
        self.paths.iter().any(|e| {
            e.catalog.eq_ignore_ascii_case(catalog) && e.schema.eq_ignore_ascii_case(schema)
        })
    }

    // ─── 内部辅助 ──────────────────────────────────────────────────────────────

    fn rebuild_paths(&mut self, user_paths: Vec<CatalogSearchEntry>) {
        let mut paths = user_paths;
        // 确保 `temp` schema 始终在搜索路径中（高优先级）
        if !paths.iter().any(|e| e.schema.eq_ignore_ascii_case("temp")) {
            paths.insert(
                0,
                CatalogSearchEntry::new(self.default_catalog.clone(), "temp"),
            );
        }
        // 确保 `main` schema 始终在搜索路径中
        if !paths.iter().any(|e| e.schema.eq_ignore_ascii_case("main")) {
            paths.push(CatalogSearchEntry::new(
                self.default_catalog.clone(),
                "main",
            ));
        }
        self.paths = paths;
    }
}
