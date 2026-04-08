use std::fmt;

pub use anyhow::{Context, Error, anyhow, bail, ensure};

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type CatalogResult<T> = std::result::Result<T, crate::catalog::CatalogError>;
pub type StorageResult<T> = std::result::Result<T, crate::storage::storage_info::StorageError>;
pub type WalResult<T> = std::result::Result<T, crate::storage::wal_replay::WalError>;
pub type TransactionResult<T> =
    std::result::Result<T, crate::transaction::transaction_manager::ErrorData>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    Internal,
    InvalidInput,
    NotFound,
    Catalog,
    Storage,
    Transaction,
    Wal,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::Internal => "internal",
            Self::InvalidInput => "invalid_input",
            Self::NotFound => "not_found",
            Self::Catalog => "catalog",
            Self::Storage => "storage",
            Self::Transaction => "transaction",
            Self::Wal => "wal",
        };
        f.write_str(code)
    }
}

pub trait HasErrorCode {
    fn error_code(&self) -> ErrorCode;
}

pub fn coded(code: ErrorCode, message: impl Into<String>) -> Error {
    anyhow!("[{}] {}", code, message.into())
}
