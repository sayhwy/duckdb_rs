// ============================================================
// types.rs — 基础类型与枚举
// 对应 C++:
//   duckdb/common/enums/memory_tag.hpp
//   duckdb/common/enums/destroy_buffer_upon.hpp
//   duckdb/storage/storage_info.hpp
// ============================================================

/// 对应 C++ block_id_t（int64_t）
pub type BlockId = i64;

/// 对应 C++ idx_t（uint64_t/usize）
pub type Idx = usize;

/// 对应 C++ INVALID_BLOCK
pub const INVALID_BLOCK: BlockId = -1;

/// 对应 C++ MAXIMUM_BLOCK（2^62）
/// block_id >= MAXIMUM_BLOCK 表示内存临时块（不在磁盘上有对应位置）
pub const MAXIMUM_BLOCK: BlockId = 4_611_686_018_427_388_000;

/// 对应 C++ DConstants::INVALID_INDEX
pub const INVALID_INDEX: Idx = usize::MAX;

/// 对应 C++ DEFAULT_BLOCK_ALLOC_SIZE（256KB）
pub const DEFAULT_BLOCK_ALLOC_SIZE: usize = 262_144;

/// 对应 C++ DEFAULT_BLOCK_HEADER_STORAGE_SIZE（8 bytes）
pub const DEFAULT_BLOCK_HEADER_SIZE: usize = 8;

// ─── MemoryTag ────────────────────────────────────────────────
/// 对应 C++ enum class MemoryTag : uint8_t
/// 用于在 BufferPool 中按类型统计内存使用量
#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryTag {
    BaseTable = 0,
    HashTable = 1,
    ParquetReader = 2,
    CsvReader = 3,
    OrderBy = 4,
    ArtIndex = 5,
    ColumnData = 6,
    Metadata = 7,
    OverflowStrings = 8,
    InMemoryTable = 9,
    Allocator = 10,
    Extension = 11,
    Transaction = 12,
    ExternalFileCache = 13,
    Window = 14,
}

/// 对应 C++ MEMORY_TAG_COUNT = 15
pub const MEMORY_TAG_COUNT: usize = 15;

// ─── FileBufferType ───────────────────────────────────────────
/// 对应 C++ enum class FileBufferType
/// 决定 buffer 进入哪个驱逐队列，以及驱逐优先级
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileBufferType {
    /// 对应磁盘上的持久化 block（驱逐代价低，直接丢弃即可）
    Block = 0,
    /// 由 BufferManager 管理的内存缓冲区（驱逐时需写临时文件）
    ManagedBuffer = 1,
    /// 小型内存缓冲区（最后驱逐）
    TinyBuffer = 2,
    /// 外部文件缓存
    ExternalFile = 3,
}

/// 对应 C++ FILE_BUFFER_TYPE_COUNT
pub const FILE_BUFFER_TYPE_COUNT: usize = 4;

// ─── BlockState ───────────────────────────────────────────────
/// 对应 C++ enum class BlockState : uint8_t
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockState {
    Unloaded = 0,
    Loaded = 1,
}

// ─── DestroyBufferUpon ────────────────────────────────────────
/// 对应 C++ enum class DestroyBufferUpon : uint8_t
/// 控制 BlockHandle 的数据缓冲区何时被销毁
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestroyBufferUpon {
    /// 块可被驱逐到存储，销毁 BlockHandle 时销毁缓冲区
    Block = 0,
    /// 驱逐时直接销毁（不写临时文件）
    Eviction = 1,
    /// Unpin 时立即销毁，不加入驱逐队列
    Unpin = 2,
}
