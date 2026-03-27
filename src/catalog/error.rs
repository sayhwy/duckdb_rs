//! Catalog 错误类型。
//!
//! 对应 C++: `duckdb/common/exception/catalog_exception.hpp`

use std::fmt;
use super::types::CatalogType;

// ─── CatalogError ──────────────────────────────────────────────────────────────

/// Catalog 操作错误（C++: `CatalogException`）。
#[derive(Debug, Clone)]
pub enum CatalogError {
    /// 条目已存在（C++: `Catalog entry ... already exists!`）。
    AlreadyExists {
        catalog_type: CatalogType,
        name: String,
    },
    /// 条目不存在（C++: `... does not exist!`）。
    EntryNotFound {
        catalog_type: CatalogType,
        name: String,
        /// 建议的相近名称。
        did_you_mean: Option<String>,
    },
    /// 依赖冲突（C++: `Cannot drop ... because there are entries that depend on it`）。
    DependencyViolation {
        name: String,
        reason: String,
    },
    /// 类型不匹配（C++: `... is not an ...`）。
    TypeMismatch {
        expected: CatalogType,
        actual: CatalogType,
        name: String,
    },
    /// 事务冲突（C++: `Catalog write-write conflict`）。
    TransactionConflict { name: String },
    /// 无效操作（通用）。
    InvalidOperation(String),
    /// IO 错误（序列化/反序列化）。
    Io(String),
    /// 其他错误。
    Other(String),
}

impl CatalogError {
    pub fn already_exists(catalog_type: CatalogType, name: impl Into<String>) -> Self {
        Self::AlreadyExists { catalog_type, name: name.into() }
    }

    pub fn not_found(catalog_type: CatalogType, name: impl Into<String>) -> Self {
        Self::EntryNotFound { catalog_type, name: name.into(), did_you_mean: None }
    }

    pub fn not_found_with_hint(
        catalog_type: CatalogType,
        name: impl Into<String>,
        hint: impl Into<String>,
    ) -> Self {
        Self::EntryNotFound {
            catalog_type,
            name: name.into(),
            did_you_mean: Some(hint.into()),
        }
    }

    pub fn dependency_violation(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::DependencyViolation { name: name.into(), reason: reason.into() }
    }

    pub fn type_mismatch(expected: CatalogType, actual: CatalogType, name: impl Into<String>) -> Self {
        Self::TypeMismatch { expected, actual, name: name.into() }
    }

    pub fn transaction_conflict(name: impl Into<String>) -> Self {
        Self::TransactionConflict { name: name.into() }
    }

    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::InvalidOperation(msg.into())
    }

    pub fn io(msg: impl Into<String>) -> Self {
        Self::Io(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::AlreadyExists { catalog_type, name } =>
                write!(f, "Catalog {} \"{}\" already exists!", catalog_type, name),

            CatalogError::EntryNotFound { catalog_type, name, did_you_mean } => {
                write!(f, "{} with name \"{}\" does not exist!", catalog_type, name)?;
                if let Some(hint) = did_you_mean {
                    write!(f, " Did you mean \"{}\"?", hint)?;
                }
                Ok(())
            }

            CatalogError::DependencyViolation { name, reason } =>
                write!(f, "Cannot drop \"{}\" because there are entries that depend on it: {}", name, reason),

            CatalogError::TypeMismatch { expected, actual, name } =>
                write!(f, "\"{}\" is not a {} (it is a {})", name, expected, actual),

            CatalogError::TransactionConflict { name } =>
                write!(f, "Catalog write-write conflict on \"{}\"", name),

            CatalogError::InvalidOperation(msg) =>
                write!(f, "Invalid catalog operation: {}", msg),

            CatalogError::Io(msg) =>
                write!(f, "Catalog IO error: {}", msg),

            CatalogError::Other(msg) =>
                write!(f, "Catalog error: {}", msg),
        }
    }
}

impl std::error::Error for CatalogError {}
