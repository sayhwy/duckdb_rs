//! 存储层附加选项。
//!
//! 对应 C++:
//!   `duckdb/storage/storage_options.hpp`
//!   `duckdb/common/encryption_state.hpp`（CipherType）
//!
//! # 结构
//!
//! ```text
//! StorageOptions
//!   ├── block_alloc_size    — 数据块分配大小（可选）
//!   ├── row_group_size      — RowGroup 大小（可选）
//!   ├── storage_version     — 目标存储版本（可选）
//!   ├── block_header_size   — 块头大小，仅加密时使用（可选）
//!   ├── compress_in_memory  — 内存压缩策略
//!   ├── encryption          — 是否启用加密
//!   ├── encryption_cipher   — 加密算法
//!   └── user_key            — 加密密钥（Arc<Vec<u8>>）
//! ```

use std::sync::Arc;

// ─── CompressInMemory ────────────────────────────────────────────────────────

/// 内存压缩策略（C++: `enum class CompressInMemory`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressInMemory {
    /// 自动决定（C++: `AUTOMATIC`）。
    #[default]
    Automatic,
    /// 强制压缩（C++: `COMPRESS`）。
    Compress,
    /// 不压缩（C++: `DO_NOT_COMPRESS`）。
    DoNotCompress,
}

// ─── CipherType ──────────────────────────────────────────────────────────────

/// 加密算法类型（C++: `EncryptionTypes::CipherType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum CipherType {
    /// 未设置 / 无效（C++: `INVALID = 0`）。
    #[default]
    Invalid = 0,
    /// AES-GCM（C++: `GCM = 1`）。
    Gcm = 1,
    /// AES-CTR（C++: `CTR = 2`）。
    Ctr = 2,
    /// AES-CBC（C++: `CBC = 3`）。
    Cbc = 3,
}

impl CipherType {
    /// 将算法名字符串解析为 `CipherType`（C++: `EncryptionTypes::StringToCipher()`）。
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GCM" => Some(Self::Gcm),
            "CTR" => Some(Self::Ctr),
            "CBC" => Some(Self::Cbc),
            _ => None,
        }
    }

    /// 返回算法名字符串（C++: `EncryptionTypes::CipherToString()`）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "INVALID",
            Self::Gcm => "GCM",
            Self::Ctr => "CTR",
            Self::Cbc => "CBC",
        }
    }
}

impl std::fmt::Display for CipherType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─── StorageOptions ──────────────────────────────────────────────────────────

/// ATTACH 时传入的存储选项（C++: `struct StorageOptions`）。
///
/// 所有字段均为可选；`None` 表示使用数据库默认值。
#[derive(Debug, Clone, Default)]
pub struct StorageOptions {
    /// 数据块分配大小（字节，必须是 2 的幂且在合法范围内）。
    /// （C++: `optional_idx block_alloc_size`）
    pub block_alloc_size: Option<u64>,

    /// RowGroup 大小（行数）。
    /// （C++: `optional_idx row_group_size`）
    pub row_group_size: Option<u64>,

    /// 目标存储版本号。
    /// （C++: `optional_idx storage_version`）
    pub storage_version: Option<u64>,

    /// 块头大小（字节），仅加密模式使用。
    /// （C++: `optional_idx block_header_size`）
    pub block_header_size: Option<u64>,

    /// 内存压缩策略（C++: `CompressInMemory compress_in_memory`）。
    pub compress_in_memory: CompressInMemory,

    /// 是否启用加密（C++: `bool encryption`）。
    pub encryption: bool,

    /// 加密算法（C++: `EncryptionTypes::CipherType encryption_cipher`）。
    pub encryption_cipher: CipherType,

    /// 加密密钥（C++: `shared_ptr<string> user_key` → `Arc<Vec<u8>>`）。
    pub user_key: Option<Arc<Vec<u8>>>,
}

impl StorageOptions {
    /// 创建默认选项（所有字段为 None / 默认值）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 是否启用了加密。
    pub fn is_encrypted(&self) -> bool {
        self.encryption && self.encryption_cipher != CipherType::Invalid
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_not_encrypted() {
        let opts = StorageOptions::new();
        assert!(!opts.is_encrypted());
        assert_eq!(opts.compress_in_memory, CompressInMemory::Automatic);
        assert_eq!(opts.encryption_cipher, CipherType::Invalid);
    }

    #[test]
    fn cipher_type_roundtrip() {
        for (s, expected) in [
            ("GCM", CipherType::Gcm),
            ("CTR", CipherType::Ctr),
            ("CBC", CipherType::Cbc),
        ] {
            let parsed = CipherType::from_str(s).unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_str(), s);
        }
        assert!(CipherType::from_str("INVALID").is_none());
    }

    #[test]
    fn encrypted_options() {
        let opts = StorageOptions {
            encryption: true,
            encryption_cipher: CipherType::Gcm,
            user_key: Some(std::sync::Arc::new(b"secret".to_vec())),
            ..Default::default()
        };
        assert!(opts.is_encrypted());
    }
}
