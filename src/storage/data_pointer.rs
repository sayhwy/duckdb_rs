//! 列段 / 行组磁盘指针。
//!
//! 对应 C++:
//!   `duckdb/storage/data_pointer.hpp`
//!   `src/storage/data_pointer.cpp`
//!
//! # 结构
//!
//! ```text
//! ColumnSegmentState   — 列段的压缩相关持久化状态（抽象，依压缩算法实现）
//! DataPointer          — 单个列段的磁盘位置 + 统计信息
//! RowGroupPointer      — 单个 RowGroup 的全部列段指针 + 删除信息
//! ```
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `virtual ~ColumnSegmentState()` | `trait ColumnSegmentState` (dyn) |
//! | `unique_ptr<ColumnSegmentState>` | `Option<Box<dyn ColumnSegmentState>>` |
//! | `DataPointer(BaseStatistics stats)` | `DataPointer::new(stats: BaseStatistics)` |
//! | `DataPointer(DataPointer &&) noexcept` | Rust 的 move 语义自动满足 |
//! | `vector<block_id_t> blocks` | `Vec<i64>` |
//! | `vector<MetaBlockPointer> data_pointers` | `Vec<MetaBlockPointer>` |

use crate::storage::metadata::MetaBlockPointer;
use crate::storage::table::types::{BlockId, CompressionType, Idx};

// 使用统一的统计信息模块
pub use crate::storage::statistics::BaseStatistics;

// ─── ColumnSegmentState ──────────────────────────────────────────────────────

/// 列段压缩状态的抽象接口（C++: `struct ColumnSegmentState`）。
///
/// 每种压缩算法可派生自己的具体类型。
/// C++ 通过 `virtual void Serialize(Serializer&)` 和
/// `static unique_ptr<ColumnSegmentState> Deserialize(Deserializer&)` 处理序列化；
/// Rust 中用 trait 对象表示。
pub trait ColumnSegmentState: Send + Sync + std::fmt::Debug {
    /// 序列化到字节流（C++: `Serialize(Serializer &)`）。
    fn serialize(&self, out: &mut Vec<u8>);
}

/// `ColumnSegmentState` 实现了的辅助：块 ID 列表（C++: `vector<block_id_t> blocks`）。
///
/// 基础实现存储压缩块列表，复杂压缩算法可提供更丰富的子类型。
#[derive(Debug, Clone, Default)]
pub struct BlocksColumnSegmentState {
    /// 该列段所跨越的块 ID 列表（C++: `vector<block_id_t> blocks`）。
    pub blocks: Vec<BlockId>,
}

impl ColumnSegmentState for BlocksColumnSegmentState {
    fn serialize(&self, out: &mut Vec<u8>) {
        // 写入 block 数量（8 字节 LE）
        out.extend_from_slice(&(self.blocks.len() as u64).to_le_bytes());
        for &b in &self.blocks {
            out.extend_from_slice(&b.to_le_bytes());
        }
    }
}

// ─── DataPointer ─────────────────────────────────────────────────────────────

/// 单个列段的磁盘位置与元信息（C++: `struct DataPointer`）。
///
/// 不可复制（C++ 侧 `= delete`），可 move（Rust 默认）。
#[derive(Debug)]
pub struct DataPointer {
    /// 该列段所属的起始行号（C++: `uint64_t row_start`）。
    pub row_start: Idx,

    /// 该列段包含的行数（C++: `uint64_t tuple_count`）。
    pub tuple_count: Idx,

    /// 列段数据在磁盘中的块指针（C++: `BlockPointer block_pointer`）。
    pub block_pointer: BlockPointer,

    /// 压缩算法（C++: `CompressionType compression_type`）。
    pub compression_type: CompressionType,

    /// 列统计信息（C++: `BaseStatistics statistics`）。
    pub statistics: BaseStatistics,

    /// 可选的压缩相关持久化状态（C++: `unique_ptr<ColumnSegmentState> segment_state`）。
    pub segment_state: Option<Box<dyn ColumnSegmentState>>,
}

impl DataPointer {
    /// 构造，statistics 必须先提供（C++: `DataPointer(BaseStatistics stats)`）。
    pub fn new(statistics: BaseStatistics) -> Self {
        Self {
            row_start: 0,
            tuple_count: 0,
            block_pointer: BlockPointer::default(),
            compression_type: CompressionType::Uncompressed,
            statistics,
            segment_state: None,
        }
    }
}

// ─── BlockPointer ──────────────────────────────────────────────────────────
// C++ 的 `BlockPointer` 在 DuckDB 中定义为 { block_id_t block_id; uint32_t offset; }

/// 磁盘块 + 块内偏移（C++: `BlockPointer`）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockPointer {
    /// 块标识符（C++: `block_id_t block_id`）。
    pub block_id: BlockId,
    /// 块内字节偏移（C++: `uint32_t offset`）。
    pub offset: u32,
}

impl BlockPointer {
    /// 判断是否为有效指针（block_id != INVALID_BLOCK）。
    pub fn is_valid(&self) -> bool {
        use crate::storage::table::types::INVALID_BLOCK;
        self.block_id != INVALID_BLOCK
    }
}

// ─── RowGroupPointer ─────────────────────────────────────────────────────────

/// 单个 RowGroup 的完整磁盘指针集合（C++: `struct RowGroupPointer`）。
#[derive(Debug, Default)]
pub struct RowGroupPointer {
    /// 该 RowGroup 起始行号（C++: `uint64_t row_start`）。
    pub row_start: Idx,

    /// 该 RowGroup 包含的行数（C++: `uint64_t tuple_count`）。
    pub tuple_count: Idx,

    /// 各列段在磁盘的元块指针（C++: `vector<MetaBlockPointer> data_pointers`）。
    ///
    /// 每列一个。
    pub data_pointers: Vec<MetaBlockPointer>,

    /// 删除信息的元块指针（C++: `vector<MetaBlockPointer> deletes_pointers`）。
    pub deletes_pointers: Vec<MetaBlockPointer>,

    /// 是否记录了所有元数据块（C++: `bool has_metadata_blocks`）。
    pub has_metadata_blocks: bool,

    /// 超出 `data_pointers` 的额外元数据块（宽列使用）
    /// （C++: `vector<idx_t> extra_metadata_blocks`）。
    pub extra_metadata_blocks: Vec<Idx>,
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::LogicalType;

    #[test]
    fn data_pointer_default_state() {
        let stats = BaseStatistics::create_empty(LogicalType::integer());
        let dp = DataPointer::new(stats);
        assert_eq!(dp.row_start, 0);
        assert_eq!(dp.tuple_count, 0);
        assert!(dp.segment_state.is_none());
    }

    #[test]
    fn block_pointer_validity() {
        use crate::storage::table::types::INVALID_BLOCK;
        let valid = BlockPointer { block_id: 42, offset: 0 };
        let invalid = BlockPointer { block_id: INVALID_BLOCK, offset: 0 };
        assert!(valid.is_valid());
        assert!(!invalid.is_valid());
    }

    #[test]
    fn blocks_segment_state_serialize() {
        let state = BlocksColumnSegmentState { blocks: vec![1, 2, 3] };
        let mut buf = Vec::new();
        state.serialize(&mut buf);
        // 8 bytes count + 3 * 8 bytes block ids = 32 bytes
        assert_eq!(buf.len(), 32);
        let count = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(count, 3);
    }

    #[test]
    fn row_group_pointer_defaults() {
        let rgp = RowGroupPointer::default();
        assert_eq!(rgp.row_start, 0);
        assert!(rgp.data_pointers.is_empty());
        assert!(!rgp.has_metadata_blocks);
    }
}
