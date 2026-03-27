//! Catalog 查找辅助类型。
//!
//! 对应 C++:
//!   - `duckdb/catalog/entry_lookup_info.hpp`
//!   - `duckdb/catalog/similar_catalog_entry.hpp`
//!   - `duckdb/catalog/catalog_search_path.hpp`（部分）

use super::types::CatalogType;
use super::error::CatalogError;

// ─── EntryLookupInfo ───────────────────────────────────────────────────────────

/// 条目查找参数（C++: `EntryLookupInfo`）。
#[derive(Debug, Clone)]
pub struct EntryLookupInfo {
    /// 要查找的条目类型。
    pub catalog_type: CatalogType,
    /// 要查找的条目名称。
    pub name: String,
    /// 错误上下文（用于生成友好的错误消息）。
    pub error_context: String,
}

impl EntryLookupInfo {
    pub fn new(catalog_type: CatalogType, name: impl Into<String>) -> Self {
        Self {
            catalog_type,
            name: name.into(),
            error_context: String::new(),
        }
    }

    pub fn with_context(mut self, ctx: impl Into<String>) -> Self {
        self.error_context = ctx.into();
        self
    }

    /// 构造 schema 级别的查找信息（C++: `EntryLookupInfo::SchemaLookup`）。
    pub fn schema_lookup(name: impl Into<String>) -> Self {
        Self::new(CatalogType::SchemaEntry, name)
    }
}

// ─── CatalogEntryLookup ────────────────────────────────────────────────────────

/// Catalog 查找结果（C++: `CatalogEntryLookup`）。
///
/// 包含查找到的 schema 和 entry，以及可能的错误。
#[derive(Debug, Clone)]
pub struct CatalogEntryLookup {
    /// 找到的 schema 名称（未找到时为 None）。
    pub schema_name: Option<String>,
    /// 找到的条目键（条目在 CatalogSet 中的名称，未找到时为 None）。
    pub entry_name: Option<String>,
    /// 查找失败时的错误。
    pub error: Option<CatalogError>,
}

impl CatalogEntryLookup {
    /// 成功查找。
    pub fn success(schema_name: impl Into<String>, entry_name: impl Into<String>) -> Self {
        Self {
            schema_name: Some(schema_name.into()),
            entry_name: Some(entry_name.into()),
            error: None,
        }
    }

    /// 未找到（返回 None）。
    pub fn missing() -> Self {
        Self { schema_name: None, entry_name: None, error: None }
    }

    /// 未找到（返回错误）。
    pub fn with_error(error: CatalogError) -> Self {
        Self { schema_name: None, entry_name: None, error: Some(error) }
    }

    /// 是否找到结果。
    pub fn found(&self) -> bool {
        self.schema_name.is_some() && self.entry_name.is_some()
    }

    /// 取出错误，如果未找到且没有存储错误则生成默认错误。
    pub fn into_error_or_not_found(
        self,
        lookup: &EntryLookupInfo,
    ) -> CatalogError {
        if let Some(e) = self.error {
            e
        } else {
            CatalogError::not_found(lookup.catalog_type, &lookup.name)
        }
    }
}

// ─── SimilarCatalogEntry ───────────────────────────────────────────────────────

/// 与查找名称相似的 catalog 条目（C++: `SimilarCatalogEntry`）。
#[derive(Debug, Clone, Default)]
pub struct SimilarCatalogEntry {
    /// 相似的名称（未找到时为空字符串）。
    pub name: String,
    /// 相似度分数（0.0 ~ 1.0，越高越相似）。
    pub score: f64,
    /// 所属 schema 名称。
    pub schema_name: Option<String>,
}

impl SimilarCatalogEntry {
    pub fn new(name: impl Into<String>, score: f64, schema_name: Option<String>) -> Self {
        Self { name: name.into(), score, schema_name }
    }

    /// 是否找到相似结果（C++: `bool Found() const`）。
    pub fn found(&self) -> bool { !self.name.is_empty() }

    /// 完全限定名称（C++: `GetQualifiedName`）。
    pub fn qualified_name(&self, qualify_schema: bool) -> String {
        if qualify_schema {
            if let Some(schema) = &self.schema_name {
                return format!("{}.{}", schema, self.name);
            }
        }
        self.name.clone()
    }
}

/// 计算两个字符串的相似度（Jaro-Winkler 近似，用于 "did you mean?" 提示）。
pub fn string_similarity(s1: &str, s2: &str) -> f64 {
    if s1 == s2 { return 1.0; }
    if s1.is_empty() || s2.is_empty() { return 0.0; }

    let s1 = s1.to_lowercase();
    let s2 = s2.to_lowercase();

    let len1 = s1.len();
    let len2 = s2.len();

    // 简单的 Jaccard 系数（使用字符 bigram）
    let bigrams1: std::collections::HashSet<(char, char)> = s1.chars()
        .zip(s1.chars().skip(1))
        .collect();
    let bigrams2: std::collections::HashSet<(char, char)> = s2.chars()
        .zip(s2.chars().skip(1))
        .collect();

    if bigrams1.is_empty() || bigrams2.is_empty() {
        // 单字符时退化为字符集合比较
        let chars1: std::collections::HashSet<char> = s1.chars().collect();
        let chars2: std::collections::HashSet<char> = s2.chars().collect();
        let intersection = chars1.intersection(&chars2).count();
        let union = chars1.union(&chars2).count();
        return if union == 0 { 0.0 } else { intersection as f64 / union as f64 };
    }

    let intersection = bigrams1.intersection(&bigrams2).count();
    let union = bigrams1.union(&bigrams2).count();

    if union == 0 { 0.0 } else { intersection as f64 / union as f64 }
}

/// 从候选列表中找出最相似的条目（C++: `SimilarEntriesInSchemas` 辅助）。
pub fn find_most_similar<'a>(
    target: &str,
    candidates: impl Iterator<Item = (&'a str, &'a str)>,  // (schema, name)
    threshold: f64,
) -> SimilarCatalogEntry {
    let mut best = SimilarCatalogEntry::default();
    for (schema, name) in candidates {
        let score = string_similarity(target, name);
        if score > best.score && score >= threshold {
            best = SimilarCatalogEntry::new(name, score, Some(schema.to_string()));
        }
    }
    best
}
