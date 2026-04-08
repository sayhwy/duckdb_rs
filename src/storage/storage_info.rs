//! 存储层基础常量、头部结构与 I/O 抽象。
//!
//! 对应 C++:
//!   `duckdb/storage/storage_info.hpp`
//!   `src/storage/storage_info.cpp`
//!   `duckdb/common/file_system.hpp`（FileHandle / FileSystem 部分）
//!
//! # 模块内容
//!
//! - **常量**：块大小、行组大小、版本号等
//! - **版本表**：存储版本 / 序列化版本的名称↔数字映射
//! - **`Storage`**：块大小合法性验证
//! - **`MainHeader`**：文件头（魔数、版本、flags、加密元数据）
//! - **`DatabaseHeader`**：checkpoint 头（iteration、元块指针等）
//! - **`StorageManagerOptions`**：块管理器初始化选项
//! - **`StorageError` / `StorageResult`**：存储层错误类型
//! - **`FileHandle` / `FileSystem`**：文件 I/O 抽象 trait

use crate::storage::buffer::BlockId;
use crate::common::errors::{ErrorCode, HasErrorCode};

// ─────────────────────────────────────────────────────────────────────────────
// 块 / 行组 大小常量
// ─────────────────────────────────────────────────────────────────────────────

/// 磁盘扇区大小，Direct I/O 时对齐用（C++: `Storage::SECTOR_SIZE`）。
pub const SECTOR_SIZE: usize = 4096;

/// 文件头大小（2 个头部槽各占 4096 字节）（C++: `Storage::FILE_HEADER_SIZE`）。
pub const FILE_HEADER_SIZE: usize = 4096;

/// 默认块分配大小 256 KiB（C++: `DEFAULT_BLOCK_ALLOC_SIZE`）。
pub const DEFAULT_BLOCK_ALLOC_SIZE: usize = 262_144;

/// 最小块分配大小 16 KiB（C++: `Storage::MIN_BLOCK_ALLOC_SIZE`）。
pub const MIN_BLOCK_ALLOC_SIZE: usize = 16_384;

/// 最大块分配大小 256 KiB（C++: `Storage::MAX_BLOCK_ALLOC_SIZE`）。
pub const MAX_BLOCK_ALLOC_SIZE: usize = 262_144;

/// 默认块头大小（存储 checksum 的 8 字节）（C++: `DEFAULT_BLOCK_HEADER_STORAGE_SIZE`）。
pub const BLOCK_HEADER_SIZE: usize = 8;

/// 默认块头存储大小（C++: `DEFAULT_BLOCK_HEADER_STORAGE_SIZE`）。
/// 与 BLOCK_HEADER_SIZE 相同，用于计算 checksum 偏移。
pub const DEFAULT_BLOCK_HEADER_STORAGE_SIZE: usize = 8;

/// 加密块头大小（nonce + tag，40 字节）（C++: `DEFAULT_ENCRYPTION_BLOCK_HEADER_SIZE`）。
pub const ENCRYPTION_BLOCK_HEADER_SIZE: usize = 40;

/// 加密缓冲区头大小 32 字节（C++: `DEFAULT_ENCRYPTED_BUFFER_HEADER_SIZE`）。
pub const ENCRYPTED_BUFFER_HEADER_SIZE: usize = 32;

/// 最大块头大小 128 字节（C++: `Storage::MAX_BLOCK_HEADER_SIZE`）。
pub const MAX_BLOCK_HEADER_SIZE: usize = 128;

/// 默认块载荷大小 = alloc - header（C++: `Storage::DEFAULT_BLOCK_SIZE`）。
pub const DEFAULT_BLOCK_SIZE: usize = DEFAULT_BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;

/// 默认行组大小 122,880 行（C++: `DEFAULT_ROW_GROUP_SIZE`）。
pub const DEFAULT_ROW_GROUP_SIZE: u64 = 122_880;

/// 行组最大大小 1 GiB 行数（C++: `Storage::MAX_ROW_GROUP_SIZE`）。
pub const MAX_ROW_GROUP_SIZE: u64 = 1 << 30;

/// 无效块 ID（C++: `INVALID_BLOCK = -1`）。
pub const INVALID_BLOCK: BlockId = -1;

/// 最大合法块 ID（C++: `MAXIMUM_BLOCK = 2^62`）。
pub const MAXIMUM_BLOCK: BlockId = 4_611_686_018_427_388_000;

/// 魔数 "DUCK"（C++: `MainHeader::MAGIC_BYTES`）。
pub const MAGIC_BYTES: [u8; 4] = *b"DUCK";

// ─────────────────────────────────────────────────────────────────────────────
// 存储格式版本号
// ─────────────────────────────────────────────────────────────────────────────

/// 当前默认存储版本（C++: `VERSION_NUMBER = 64`）。
pub const VERSION_NUMBER: u64 = 64;

/// 可读取的最低存储版本（C++: `VERSION_NUMBER_LOWER = 64`）。
pub const VERSION_NUMBER_LOWER: u64 = 64;

/// 可读取的最高存储版本（C++: `VERSION_NUMBER_UPPER = 67`）。
pub const VERSION_NUMBER_UPPER: u64 = 67;

/// 最新序列化版本（C++: `LATEST_SERIALIZATION_VERSION_INFO = 7`）。
pub const LATEST_SERIALIZATION_VERSION: u64 = 7;

/// 默认序列化版本（C++: `DEFAULT_SERIALIZATION_VERSION_INFO = 1`）。
pub const DEFAULT_SERIALIZATION_VERSION: u64 = 1;

// ─────────────────────────────────────────────────────────────────────────────
// 版本查找表（对应 storage_info.cpp 中的静态表）
// ─────────────────────────────────────────────────────────────────────────────

/// 存储版本条目（C++: `struct StorageVersionInfo`）。
struct StorageVersionEntry {
    name: &'static str,
    storage_version: u64,
}

/// 序列化版本条目（C++: `struct SerializationVersionInfo`）。
struct SerializationVersionEntry {
    name: &'static str,
    serialization_version: u64,
}

// 自动生成的存储版本表（对应 storage_info.cpp START OF STORAGE VERSION INFO）
static STORAGE_VERSION_TABLE: &[StorageVersionEntry] = &[
    StorageVersionEntry {
        name: "v0.0.4",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.0",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.1",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.2",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.3",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.4",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.5",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.6",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.7",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.8",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.1.9",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.2.0",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.2.1",
        storage_version: 1,
    },
    StorageVersionEntry {
        name: "v0.2.2",
        storage_version: 4,
    },
    StorageVersionEntry {
        name: "v0.2.3",
        storage_version: 6,
    },
    StorageVersionEntry {
        name: "v0.2.4",
        storage_version: 11,
    },
    StorageVersionEntry {
        name: "v0.2.5",
        storage_version: 13,
    },
    StorageVersionEntry {
        name: "v0.2.6",
        storage_version: 15,
    },
    StorageVersionEntry {
        name: "v0.2.7",
        storage_version: 17,
    },
    StorageVersionEntry {
        name: "v0.2.8",
        storage_version: 18,
    },
    StorageVersionEntry {
        name: "v0.2.9",
        storage_version: 21,
    },
    StorageVersionEntry {
        name: "v0.3.0",
        storage_version: 25,
    },
    StorageVersionEntry {
        name: "v0.3.1",
        storage_version: 27,
    },
    StorageVersionEntry {
        name: "v0.3.2",
        storage_version: 31,
    },
    StorageVersionEntry {
        name: "v0.3.3",
        storage_version: 33,
    },
    StorageVersionEntry {
        name: "v0.3.4",
        storage_version: 33,
    },
    StorageVersionEntry {
        name: "v0.3.5",
        storage_version: 33,
    },
    StorageVersionEntry {
        name: "v0.4.0",
        storage_version: 33,
    },
    StorageVersionEntry {
        name: "v0.5.0",
        storage_version: 38,
    },
    StorageVersionEntry {
        name: "v0.5.1",
        storage_version: 38,
    },
    StorageVersionEntry {
        name: "v0.6.0",
        storage_version: 39,
    },
    StorageVersionEntry {
        name: "v0.6.1",
        storage_version: 39,
    },
    StorageVersionEntry {
        name: "v0.7.0",
        storage_version: 43,
    },
    StorageVersionEntry {
        name: "v0.7.1",
        storage_version: 43,
    },
    StorageVersionEntry {
        name: "v0.8.0",
        storage_version: 51,
    },
    StorageVersionEntry {
        name: "v0.8.1",
        storage_version: 51,
    },
    StorageVersionEntry {
        name: "v0.9.0",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v0.9.1",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v0.9.2",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v0.10.0",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v0.10.1",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v0.10.2",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v0.10.3",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v1.0.0",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v1.1.0",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v1.1.1",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v1.1.2",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v1.1.3",
        storage_version: 64,
    },
    StorageVersionEntry {
        name: "v1.2.0",
        storage_version: 65,
    },
    StorageVersionEntry {
        name: "v1.2.1",
        storage_version: 65,
    },
    StorageVersionEntry {
        name: "v1.2.2",
        storage_version: 65,
    },
    StorageVersionEntry {
        name: "v1.3.0",
        storage_version: 66,
    },
    StorageVersionEntry {
        name: "v1.3.1",
        storage_version: 66,
    },
    StorageVersionEntry {
        name: "v1.3.2",
        storage_version: 66,
    },
    StorageVersionEntry {
        name: "v1.4.0",
        storage_version: 67,
    },
    StorageVersionEntry {
        name: "v1.4.1",
        storage_version: 67,
    },
    StorageVersionEntry {
        name: "v1.4.2",
        storage_version: 67,
    },
    StorageVersionEntry {
        name: "v1.4.3",
        storage_version: 67,
    },
    StorageVersionEntry {
        name: "v1.5.0",
        storage_version: 67,
    },
];

// 自动生成的序列化版本表（对应 storage_info.cpp START OF SERIALIZATION VERSION INFO）
static SERIALIZATION_VERSION_TABLE: &[SerializationVersionEntry] = &[
    SerializationVersionEntry {
        name: "v0.10.0",
        serialization_version: 1,
    },
    SerializationVersionEntry {
        name: "v0.10.1",
        serialization_version: 1,
    },
    SerializationVersionEntry {
        name: "v0.10.2",
        serialization_version: 1,
    },
    SerializationVersionEntry {
        name: "v0.10.3",
        serialization_version: 2,
    },
    SerializationVersionEntry {
        name: "v1.0.0",
        serialization_version: 2,
    },
    SerializationVersionEntry {
        name: "v1.1.0",
        serialization_version: 3,
    },
    SerializationVersionEntry {
        name: "v1.1.1",
        serialization_version: 3,
    },
    SerializationVersionEntry {
        name: "v1.1.2",
        serialization_version: 3,
    },
    SerializationVersionEntry {
        name: "v1.1.3",
        serialization_version: 3,
    },
    SerializationVersionEntry {
        name: "v1.2.0",
        serialization_version: 4,
    },
    SerializationVersionEntry {
        name: "v1.2.1",
        serialization_version: 4,
    },
    SerializationVersionEntry {
        name: "v1.2.2",
        serialization_version: 4,
    },
    SerializationVersionEntry {
        name: "v1.3.0",
        serialization_version: 5,
    },
    SerializationVersionEntry {
        name: "v1.3.1",
        serialization_version: 5,
    },
    SerializationVersionEntry {
        name: "v1.3.2",
        serialization_version: 5,
    },
    SerializationVersionEntry {
        name: "v1.4.0",
        serialization_version: 6,
    },
    SerializationVersionEntry {
        name: "v1.4.1",
        serialization_version: 6,
    },
    SerializationVersionEntry {
        name: "v1.4.2",
        serialization_version: 6,
    },
    SerializationVersionEntry {
        name: "v1.4.3",
        serialization_version: 6,
    },
    SerializationVersionEntry {
        name: "v1.5.0",
        serialization_version: 7,
    },
    SerializationVersionEntry {
        name: "latest",
        serialization_version: 7,
    },
];

/// 按版本字符串查找存储版本号（C++: `GetStorageVersion(version_string)`）。
///
/// 返回 `None` 如果版本字符串未知。
pub fn get_storage_version(version_string: &str) -> Option<u64> {
    STORAGE_VERSION_TABLE
        .iter()
        .find(|e| e.name == version_string)
        .map(|e| e.storage_version)
}

/// 按版本字符串查找序列化版本号（C++: `GetSerializationVersion(version_string)`）。
///
/// 支持 `"latest"` 字符串。
pub fn get_serialization_version(version_string: &str) -> Option<u64> {
    SERIALIZATION_VERSION_TABLE
        .iter()
        .find(|e| e.name == version_string)
        .map(|e| e.serialization_version)
}

/// 返回支持给定存储版本号的所有 DuckDB 版本名称（C++: `GetDuckDBVersions(version_number)`）。
///
/// 返回格式如 `"v0.9.0, v0.9.1 or v1.0.0"`。若无匹配返回空字符串。
pub fn get_duckdb_versions(version_number: u64) -> String {
    let versions: Vec<&str> = STORAGE_VERSION_TABLE
        .iter()
        .filter(|e| e.storage_version == version_number)
        .map(|e| e.name)
        .collect();

    match versions.len() {
        0 => String::new(),
        1 => versions[0].to_string(),
        n => {
            let mut result = String::new();
            for (i, v) in versions.iter().enumerate() {
                if i > 0 {
                    if i + 1 == n {
                        result.push_str(" or ");
                    } else {
                        result.push_str(", ");
                    }
                }
                result.push_str(v);
            }
            result
        }
    }
}

/// 返回序列化版本的最早 DuckDB 版本名（C++: `GetStorageVersionName()`）。
///
/// `add_suffix` 为 `true` 时在名称后追加 `+`。
/// 序列化版本 < 4 时特殊返回 `"v1.0.0+"`。
pub fn get_storage_version_name(serialization_version: u64, add_suffix: bool) -> String {
    if serialization_version < 4 {
        return "v1.0.0+".to_string();
    }
    let first = SERIALIZATION_VERSION_TABLE
        .iter()
        .filter(|e| e.name != "latest")
        .find(|e| e.serialization_version == serialization_version);

    match first {
        None => "--UNKNOWN--".to_string(),
        Some(e) => {
            if add_suffix {
                format!("{}+", e.name)
            } else {
                e.name.to_string()
            }
        }
    }
}

/// 返回所有序列化版本的候选名称列表（C++: `GetSerializationCandidates()`）。
pub fn get_serialization_candidates() -> Vec<&'static str> {
    SERIALIZATION_VERSION_TABLE.iter().map(|e| e.name).collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Storage — 块大小验证
// ─────────────────────────────────────────────────────────────────────────────

/// 块大小相关的常量与验证（C++: `struct Storage`）。
pub struct Storage;

impl Storage {
    /// 验证块分配大小（C++: `Storage::VerifyBlockAllocSize(idx_t)`）。
    ///
    /// 必须：
    /// - 是 2 的幂
    /// - `>= MIN_BLOCK_ALLOC_SIZE (16 KiB)`
    /// - `<= MAX_BLOCK_ALLOC_SIZE (256 KiB)`
    /// - `<= i32::MAX`
    pub fn verify_block_alloc_size(block_alloc_size: usize) -> StorageResult<()> {
        if !block_alloc_size.is_power_of_two() {
            return Err(StorageError::Other(format!(
                "the block size must be a power of two, got {block_alloc_size}"
            )));
        }
        if block_alloc_size < MIN_BLOCK_ALLOC_SIZE {
            return Err(StorageError::Other(format!(
                "the block size must be >= minimum block size of {MIN_BLOCK_ALLOC_SIZE}, got {block_alloc_size}"
            )));
        }
        if block_alloc_size > MAX_BLOCK_ALLOC_SIZE {
            return Err(StorageError::Other(format!(
                "the block size must be <= maximum block size of {MAX_BLOCK_ALLOC_SIZE}, got {block_alloc_size}"
            )));
        }
        if block_alloc_size > i32::MAX as usize {
            return Err(StorageError::Other(format!(
                "the block size must not exceed i32::MAX = {}, got {block_alloc_size}",
                i32::MAX
            )));
        }
        Ok(())
    }

    /// 验证块头大小（C++: `Storage::VerifyBlockHeaderSize(idx_t)`）。
    ///
    /// 必须：
    /// - 是 8 的倍数（checksum 对齐要求）
    /// - `>= BLOCK_HEADER_SIZE (8)`
    /// - `<= MAX_BLOCK_HEADER_SIZE (128)`
    pub fn verify_block_header_size(block_header_size: usize) -> StorageResult<()> {
        if block_header_size % 8 != 0 {
            return Err(StorageError::Other(format!(
                "the block header size must be a multiple of 8, got {block_header_size}"
            )));
        }
        if block_header_size < BLOCK_HEADER_SIZE {
            return Err(StorageError::Other(format!(
                "the block header size must be >= default block header size of {BLOCK_HEADER_SIZE}, got {block_header_size}"
            )));
        }
        if block_header_size > MAX_BLOCK_HEADER_SIZE {
            return Err(StorageError::Other(format!(
                "the block header size must be <= maximum of {MAX_BLOCK_HEADER_SIZE}, got {block_header_size}"
            )));
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DatabaseHeader
// ─────────────────────────────────────────────────────────────────────────────

/// Checkpoint 头部（C++: `struct DatabaseHeader`）。
///
/// 每次 checkpoint 后写入其中一个槽（iteration 决定交替顺序）。
/// 读取时选择 iteration 较大的槽作为活跃头部。
///
/// # 字段变化（相对旧版本）
/// 新增：`block_alloc_size`、`vector_size`、`serialization_compatibility`。
#[derive(Debug, Clone, Default)]
pub struct DatabaseHeader {
    /// 循环写入计数（C++: `uint64_t iteration`）。
    pub iteration: u64,
    /// Catalog 元数据起始 block（C++: `idx_t meta_block`）。
    pub meta_block: BlockId,
    /// 空闲块列表起始 block（C++: `idx_t free_list`）。
    pub free_list: BlockId,
    /// 文件中有效 block 总数（C++: `uint64_t block_count`）。
    pub block_count: u64,
    /// 本文件的块分配大小（C++: `idx_t block_alloc_size`）。
    pub block_alloc_size: u64,
    /// 本文件的向量大小（C++: `idx_t vector_size`）。
    pub vector_size: u64,
    /// 序列化兼容版本（C++: `idx_t serialization_compatibility`）。
    pub serialization_compatibility: u64,
}

impl DatabaseHeader {
    /// 序列化大小：7 × u64 = 56 字节。
    pub const SERIALIZED_SIZE: usize = 56;

    /// 将 `DatabaseHeader` 序列化到 `buf` 的 `[offset, offset+56)`（小端）。
    pub fn serialize(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= Self::SERIALIZED_SIZE);
        buf[0..8].copy_from_slice(&self.iteration.to_le_bytes());
        buf[8..16].copy_from_slice(&(self.meta_block as u64).to_le_bytes());
        buf[16..24].copy_from_slice(&(self.free_list as u64).to_le_bytes());
        buf[24..32].copy_from_slice(&self.block_count.to_le_bytes());
        buf[32..40].copy_from_slice(&self.block_alloc_size.to_le_bytes());
        buf[40..48].copy_from_slice(&self.vector_size.to_le_bytes());
        buf[48..56].copy_from_slice(&self.serialization_compatibility.to_le_bytes());
    }

    /// 从 `buf` 反序列化（小端）。
    pub fn deserialize(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= Self::SERIALIZED_SIZE);
        Self {
            iteration: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            meta_block: i64::from_le_bytes(buf[8..16].try_into().unwrap()),
            free_list: i64::from_le_bytes(buf[16..24].try_into().unwrap()),
            block_count: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            block_alloc_size: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            vector_size: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            serialization_compatibility: u64::from_le_bytes(buf[48..56].try_into().unwrap()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MainHeader
// ─────────────────────────────────────────────────────────────────────────────

/// 文件主头部（C++: `class MainHeader`）。
///
/// 存储在文件开头第 0 块中，偏移 `BLOCK_HEADER_SIZE`（8 字节 checksum 之后）。
/// 写入一次，之后通过 `DatabaseHeader` 槽来追踪 checkpoint 状态。
///
/// # 布局（不含块头 8 字节）
///
/// ```text
/// [  0..  4] magic bytes "DUCK"
/// [  4..  8] padding（保留 4 字节与 MAGIC_BYTE_OFFSET 对齐）
/// [  8.. 16] version_number : u64
/// [ 16.. 48] flags[0..4]    : [u64; 4]
/// [ 48.. 80] library_git_desc : [u8; 32]
/// [ 80..112] library_git_hash : [u8; 32]
/// [112..120] encryption_metadata : [u8; 8]
/// [120..136] db_identifier  : [u8; 16]
/// [136..144] encrypted_canary : [u8; 8]
/// ```
/// 总计 144 字节（不含块头）。
#[derive(Debug, Clone)]
pub struct MainHeader {
    /// 存储格式版本号（C++: `uint64_t version_number`）。
    pub version_number: u64,

    /// 数据库标志位（C++: `uint64_t flags[FLAG_COUNT]`，FLAG_COUNT = 4）。
    ///
    /// `flags[0] & 1 == 1` 表示加密数据库。
    pub flags: [u64; 4],

    /// 编译时的 Git 描述（C++: `data_t library_git_desc[32]`）。
    pub library_git_desc: [u8; 32],

    /// 编译时的 Git 哈希（C++: `data_t library_git_hash[32]`）。
    pub library_git_hash: [u8; 32],

    /// 加密元数据（C++: `data_t encryption_metadata[8]`）。
    ///
    /// `[2]` 字节存储加密算法类型（`CipherType`）。
    pub encryption_metadata: [u8; 8],

    /// 唯一数据库标识符（C++: `data_t db_identifier[16]`）。
    pub db_identifier: [u8; 16],

    /// 加密 canary（已知明文，用于早期检测错误密钥）（C++: `data_t encrypted_canary[8]`）。
    pub encrypted_canary: [u8; 8],
}

impl MainHeader {
    /// 魔数大小（C++: `MAGIC_BYTE_SIZE = 4`）。
    pub const MAGIC_BYTE_SIZE: usize = 4;

    /// 魔数在块内的偏移（C++: `MAGIC_BYTE_OFFSET = DEFAULT_BLOCK_HEADER_SIZE = 8`）。
    ///
    /// 即从块起始跳过 8 字节 checksum 后才是魔数。
    pub const MAGIC_BYTE_OFFSET: usize = BLOCK_HEADER_SIZE; // 8

    /// 加密数据库标志位（C++: `ENCRYPTED_DATABASE_FLAG = 1`）。
    pub const ENCRYPTED_DATABASE_FLAG: u64 = 1;

    /// 默认加密密钥长度 32 字节（C++: `DEFAULT_ENCRYPTION_KEY_LENGTH = 32`）。
    pub const DEFAULT_ENCRYPTION_KEY_LENGTH: usize = 32;

    /// 数据库唯一 ID 长度（C++: `DB_IDENTIFIER_LEN = 16`）。
    pub const DB_IDENTIFIER_LEN: usize = 16;

    /// 加密元数据长度（C++: `ENCRYPTION_METADATA_LEN = 8`）。
    pub const ENCRYPTION_METADATA_LEN: usize = 8;

    /// Canary 大小（C++: `CANARY_BYTE_SIZE = 8`）。
    pub const CANARY_BYTE_SIZE: usize = 8;

    /// 序列化大小（字段总字节数，不含块头）。
    ///
    /// 4 (magic) + 8 (version) + 32 (flags) + 32 (git_desc) +
    /// 32 (git_hash) + 8 (enc_meta) + 16 (db_id) + 8 (canary) = 140
    pub const SERIALIZED_SIZE: usize = 140;

    /// 构造默认 MainHeader（version = 当前 VERSION_NUMBER，魔数有效）。
    pub fn new(version_number: u64) -> Self {
        Self {
            version_number,
            flags: [0; 4],
            library_git_desc: [0; 32],
            library_git_hash: [0; 32],
            encryption_metadata: [0; 8],
            db_identifier: [0; 16],
            encrypted_canary: [0; 8],
        }
    }

    // ── 加密相关辅助 ──────────────────────────────────────────────────────────

    /// 是否为加密数据库（C++: `IsEncrypted()`）。
    pub fn is_encrypted(&self) -> bool {
        self.flags[0] & Self::ENCRYPTED_DATABASE_FLAG != 0
    }

    /// 标记为加密数据库（C++: `SetEncrypted()`）。
    pub fn set_encrypted(&mut self) {
        self.flags[0] |= Self::ENCRYPTED_DATABASE_FLAG;
    }

    /// 设置加密元数据（C++: `SetEncryptionMetadata(data_ptr_t source)`）。
    pub fn set_encryption_metadata(&mut self, src: &[u8]) {
        let len = src.len().min(Self::ENCRYPTION_METADATA_LEN);
        self.encryption_metadata = [0; 8];
        self.encryption_metadata[..len].copy_from_slice(&src[..len]);
    }

    /// 获取加密算法类型字节（C++: `GetEncryptionCipher()`，返回 `encryption_metadata[2]`）。
    pub fn get_encryption_cipher_byte(&self) -> u8 {
        self.encryption_metadata[2]
    }

    // ── DB 标识符 ─────────────────────────────────────────────────────────────

    /// 设置数据库唯一标识符（C++: `SetDBIdentifier(data_ptr_t source)`）。
    pub fn set_db_identifier(&mut self, src: &[u8]) {
        let len = src.len().min(Self::DB_IDENTIFIER_LEN);
        self.db_identifier = [0; 16];
        self.db_identifier[..len].copy_from_slice(&src[..len]);
    }

    /// 比较两个数据库标识符（C++: `CompareDBIdentifiers()`）。
    pub fn compare_db_identifiers(a: &[u8; 16], b: &[u8; 16]) -> bool {
        a == b
    }

    /// 设置加密 canary（C++: `SetEncryptedCanary(data_ptr_t source)`）。
    pub fn set_encrypted_canary(&mut self, src: &[u8]) {
        let len = src.len().min(Self::CANARY_BYTE_SIZE);
        self.encrypted_canary = [0; 8];
        self.encrypted_canary[..len].copy_from_slice(&src[..len]);
    }

    // ── 序列化 ────────────────────────────────────────────────────────────────

    /// 将 `MainHeader` 序列化写入 `buf`（从偏移 0 开始，不含块头）。
    ///
    /// `buf` 长度必须 >= `SERIALIZED_SIZE (140)`。
    ///
    /// 布局（对应 C++ MainHeader::Write）：
    /// [0..4]   magic "DUCK"
    /// [4..12]  version_number : u64
    /// [12..44] flags[4] : [u64; 4]
    /// [44..76] library_git_desc : [u8; 32]
    /// [76..108] library_git_hash : [u8; 32]
    /// [108..116] encryption_metadata : [u8; 8]
    /// [116..132] db_identifier : [u8; 16]
    /// [132..140] encrypted_canary : [u8; 8]
    pub fn serialize(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= Self::SERIALIZED_SIZE);
        // [0..4]  魔数
        buf[0..4].copy_from_slice(&MAGIC_BYTES);
        // [4..12] version_number
        buf[4..12].copy_from_slice(&self.version_number.to_le_bytes());
        // [12..44] flags[4]
        for (i, f) in self.flags.iter().enumerate() {
            let base = 12 + i * 8;
            buf[base..base + 8].copy_from_slice(&f.to_le_bytes());
        }
        // [44..76] library_git_desc
        buf[44..76].copy_from_slice(&self.library_git_desc);
        // [76..108] library_git_hash
        buf[76..108].copy_from_slice(&self.library_git_hash);
        // [108..116] encryption_metadata
        buf[108..116].copy_from_slice(&self.encryption_metadata);
        // [116..132] db_identifier
        buf[116..132].copy_from_slice(&self.db_identifier);
        // [132..140] encrypted_canary
        buf[132..140].copy_from_slice(&self.encrypted_canary);
    }

    /// 从 `buf` 反序列化。
    ///
    /// 若魔数不匹配返回 `None`（C++: `CheckMagicBytes` 抛异常；Rust 返回 Option）。
    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SERIALIZED_SIZE {
            return None;
        }
        let magic: [u8; 4] = buf[0..4].try_into().unwrap();
        if magic != MAGIC_BYTES {
            return None;
        }
        let version_number = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let mut flags = [0u64; 4];
        for (i, f) in flags.iter_mut().enumerate() {
            let base = 12 + i * 8;
            *f = u64::from_le_bytes(buf[base..base + 8].try_into().unwrap());
        }
        let library_git_desc: [u8; 32] = buf[44..76].try_into().unwrap();
        let library_git_hash: [u8; 32] = buf[76..108].try_into().unwrap();
        let encryption_metadata: [u8; 8] = buf[108..116].try_into().unwrap();
        let db_identifier: [u8; 16] = buf[116..132].try_into().unwrap();
        let encrypted_canary: [u8; 8] = buf[132..140].try_into().unwrap();

        Some(Self {
            version_number,
            flags,
            library_git_desc,
            library_git_hash,
            encryption_metadata,
            db_identifier,
            encrypted_canary,
        })
    }

    /// 返回迭代次数较大的 DatabaseHeader 索引（0 或 1）。
    ///
    /// 用于在两个头部槽中选取最新有效的那个。
    pub fn active_header_idx(headers: &[DatabaseHeader; 2]) -> usize {
        if headers[1].iteration > headers[0].iteration {
            1
        } else {
            0
        }
    }
}

impl Default for MainHeader {
    fn default() -> Self {
        Self::new(VERSION_NUMBER)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StorageManagerOptions
// ─────────────────────────────────────────────────────────────────────────────

/// 初始化 `SingleFileBlockManager` 时传入的选项（C++: `StorageManagerOptions`）。
#[derive(Debug, Clone, Default)]
pub struct StorageManagerOptions {
    /// 只读模式（C++: `bool read_only`）。
    pub read_only: bool,

    /// 是否使用 Direct I/O（C++: `bool use_direct_io`）。
    pub use_direct_io: bool,

    /// 调试初始化模式（C++: `DebugInitialize debug_initialize`）。
    ///
    /// `None` = 不做特殊初始化；`Some(byte)` = 用该字节填充新块。
    pub debug_initialize: Option<u8>,

    /// 块分配大小（字节）（C++: `optional_idx block_alloc_size`）。
    ///
    /// `0` 或 `DEFAULT_BLOCK_ALLOC_SIZE` 表示使用默认值；兼容旧调用方。
    pub block_alloc_size: usize,

    /// 块头大小（字节）（C++: `optional_idx block_header_size`）。
    ///
    /// `None` 表示使用默认值 `BLOCK_HEADER_SIZE (8)`。
    pub block_header_size: Option<usize>,

    /// 目标存储版本（C++: `optional_idx storage_version`）。
    pub storage_version: Option<u64>,

    /// 加密选项（C++: `EncryptionOptions encryption_options`）。
    pub encryption_options: EncryptionOptions,
}

// ─────────────────────────────────────────────────────────────────────────────
// EncryptionOptions
// ─────────────────────────────────────────────────────────────────────────────

/// 块管理器初始化时传入的加密选项（C++: `EncryptionOptions` in storage context）。
#[derive(Debug, Clone, Default)]
pub struct EncryptionOptions {
    /// 是否启用加密（C++: `bool encryption_enabled`）。
    pub encryption_enabled: bool,

    /// 用户提供的加密密钥（C++: `shared_ptr<string> user_key`）。
    ///
    /// 初始化完成后应清零（sensitive data）。
    pub user_key: Option<std::sync::Arc<Vec<u8>>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// StorageError / StorageResult
// ─────────────────────────────────────────────────────────────────────────────

/// 存储层错误类型（对应 C++ `IOException` / `InternalException` 在存储路径上的使用）。
#[derive(Debug)]
pub enum StorageError {
    /// 底层 I/O 错误。
    Io(std::io::Error),
    /// 数据文件损坏（checksum 不匹配、魔数错误等）。
    Corrupt { msg: String },
    /// 尝试在只读模式下执行写操作。
    ReadOnly,
    /// 文件不存在。
    NotFound { path: String },
    /// 其他存储错误。
    Other(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Corrupt { msg } => write!(f, "Storage corrupt: {msg}"),
            Self::ReadOnly => write!(f, "Storage is read-only"),
            Self::NotFound { path } => write!(f, "File not found: {path}"),
            Self::Other(msg) => write!(f, "Storage error: {msg}"),
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::error::Error for StorageError {}

impl HasErrorCode for StorageError {
    fn error_code(&self) -> ErrorCode {
        match self {
            StorageError::NotFound { .. } => ErrorCode::NotFound,
            StorageError::Io(_)
            | StorageError::Corrupt { .. }
            | StorageError::ReadOnly
            | StorageError::Other(_) => ErrorCode::Storage,
        }
    }
}

/// 存储层操作结果类型。
pub use crate::common::errors::StorageResult;

// ─────────────────────────────────────────────────────────────────────────────
// FileHandle / FileSystem
// ─────────────────────────────────────────────────────────────────────────────

/// 已打开文件的句柄抽象（C++: `FileHandle` 抽象基类）。
///
/// 不同文件系统实现（本地 FS、只读 FS、加密 FS 等）各自实现此 trait。
pub trait FileHandle: Send + Sync {
    /// 从偏移 `offset` 读取 `buf.len()` 字节（C++: `Read`）。
    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> StorageResult<()>;

    /// 向偏移 `offset` 写入 `buf`（C++: `Write`）。
    fn write_at(&mut self, buf: &[u8], offset: u64) -> StorageResult<()>;

    /// 截断文件到指定大小（C++: `Truncate`）。
    fn truncate(&mut self, new_size: u64) -> StorageResult<()>;

    /// 刷新并 fsync（C++: `Sync`）。
    fn sync(&mut self) -> StorageResult<()>;

    /// 文件路径（C++: `path`）。
    fn path(&self) -> &str;

    /// 文件总大小（字节）（C++: `GetFileSize`）。
    fn file_size(&self) -> StorageResult<u64>;
}

/// 文件系统抽象（C++: `FileSystem` 抽象接口）。
///
/// 负责创建 / 打开 `FileHandle`，以及文件 / 目录操作。
pub trait FileSystem: Send + Sync {
    /// 打开或创建文件（C++: `OpenFile`）。
    fn open_file(&self, path: &str, flags: FileOpenFlags) -> StorageResult<Box<dyn FileHandle>>;

    /// 文件是否存在（C++: `FileExists`）。
    fn file_exists(&self, path: &str) -> bool;

    /// 删除文件，若不存在则 no-op（C++: `TryRemoveFile`）。
    fn try_remove_file(&self, path: &str);

    /// 删除文件（C++: `RemoveFile`，不存在时报错）。
    fn remove_file(&self, path: &str) -> StorageResult<()>;

    /// 移动文件（C++: `MoveFile`）。
    fn move_file(&self, from: &str, to: &str) -> StorageResult<()>;

    /// 创建目录（含父目录，C++: `CreateDirectory`）。
    fn create_directory(&self, path: &str) -> StorageResult<()>;

    /// 列目录（C++: `ListFiles`，返回文件名列表）。
    fn list_files(&self, path: &str) -> StorageResult<Vec<String>>;

    /// 路径拼接（C++: `JoinPath`）。
    fn join_path(&self, base: &str, name: &str) -> String;

    /// 路径展开（C++: `ExpandPath`，解析 `~` 等）。
    fn expand_path(&self, path: &str) -> String {
        path.to_string() // 默认不展开
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FileOpenFlags
// ─────────────────────────────────────────────────────────────────────────────

/// 文件打开标志位组合（C++: `FileFlags` 枚举组合）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FileOpenFlags(pub u32);

impl FileOpenFlags {
    pub const READ: Self = Self(0x01);
    pub const WRITE: Self = Self(0x02);
    pub const CREATE: Self = Self(0x04);
    pub const DIRECT_IO: Self = Self(0x08);
    pub const TRUNCATE: Self = Self(0x10);
    pub const READ_WRITE: Self = Self(0x03); // READ | WRITE

    /// 是否包含指定标志位。
    pub fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }
}

impl std::ops::BitOr for FileOpenFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for FileOpenFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 单元测试
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_header_roundtrip() {
        let hdr = DatabaseHeader {
            iteration: 7,
            meta_block: 42,
            free_list: -1,
            block_count: 100,
            block_alloc_size: DEFAULT_BLOCK_ALLOC_SIZE as u64,
            vector_size: 2048,
            serialization_compatibility: 4,
        };
        let mut buf = [0u8; DatabaseHeader::SERIALIZED_SIZE];
        hdr.serialize(&mut buf);
        let hdr2 = DatabaseHeader::deserialize(&buf);
        assert_eq!(hdr2.iteration, 7);
        assert_eq!(hdr2.meta_block, 42);
        assert_eq!(hdr2.free_list, -1);
        assert_eq!(hdr2.block_count, 100);
        assert_eq!(hdr2.block_alloc_size, DEFAULT_BLOCK_ALLOC_SIZE as u64);
        assert_eq!(hdr2.vector_size, 2048);
        assert_eq!(hdr2.serialization_compatibility, 4);
    }

    #[test]
    fn main_header_roundtrip() {
        let mut hdr = MainHeader::new(VERSION_NUMBER);
        hdr.set_encrypted();
        hdr.set_db_identifier(&[1u8; 16]);
        let mut buf = [0u8; MainHeader::SERIALIZED_SIZE];
        hdr.serialize(&mut buf);
        let hdr2 = MainHeader::deserialize(&buf).unwrap();
        assert_eq!(hdr2.version_number, VERSION_NUMBER);
        assert!(hdr2.is_encrypted());
        assert_eq!(hdr2.db_identifier, [1u8; 16]);
    }

    #[test]
    fn main_header_bad_magic() {
        let mut buf = [0u8; MainHeader::SERIALIZED_SIZE];
        buf[0] = b'X';
        assert!(MainHeader::deserialize(&buf).is_none());
    }

    #[test]
    fn get_storage_version_known() {
        assert_eq!(get_storage_version("v1.0.0"), Some(64));
        assert_eq!(get_storage_version("v1.2.0"), Some(65));
        assert_eq!(get_storage_version("v99.0.0"), None);
    }

    #[test]
    fn get_serialization_version_latest() {
        assert_eq!(
            get_serialization_version("latest"),
            Some(LATEST_SERIALIZATION_VERSION)
        );
        assert_eq!(get_serialization_version("v1.5.0"), Some(7));
    }

    #[test]
    fn get_duckdb_versions_multi() {
        // v0.9.0, v0.9.1 など複数の版本都对应存储版本 64
        let s = get_duckdb_versions(64);
        assert!(s.contains("v0.9.0") || s.contains("v1.0.0"));
    }

    #[test]
    fn verify_block_alloc_size_ok() {
        assert!(Storage::verify_block_alloc_size(DEFAULT_BLOCK_ALLOC_SIZE).is_ok());
        assert!(Storage::verify_block_alloc_size(MIN_BLOCK_ALLOC_SIZE).is_ok());
    }

    #[test]
    fn verify_block_alloc_size_not_power_of_two() {
        assert!(Storage::verify_block_alloc_size(100_000).is_err());
    }

    #[test]
    fn verify_block_alloc_size_too_small() {
        assert!(Storage::verify_block_alloc_size(8192).is_err());
    }

    #[test]
    fn verify_block_header_size_ok() {
        assert!(Storage::verify_block_header_size(BLOCK_HEADER_SIZE).is_ok());
        assert!(Storage::verify_block_header_size(40).is_ok()); // encryption header
    }

    #[test]
    fn verify_block_header_size_not_multiple_of_8() {
        assert!(Storage::verify_block_header_size(9).is_err());
    }
}
