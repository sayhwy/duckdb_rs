// ============================================================
// metadata/types.rs — 元数据层基础类型
// 对应 C++: duckdb/storage/metadata/metadata_manager.hpp 中的结构体
//           duckdb/common/serializer/read_stream.hpp
//           duckdb/common/serializer/write_stream.hpp
// ============================================================

use crate::storage::buffer::{BlockId, BufferHandle, INVALID_BLOCK};

// ─── MetaBlockPointer ─────────────────────────────────────────

/// 对应 C++ MetaBlockPointer
///
/// 磁盘上元数据块的压缩指针：
///   bits [55..0]  : block_id（持久化块 ID）
///   bits [63..56] : block_index（同一存储块内的第几个 meta 子块，0-63）
///   offset        : 子块内的字节偏移
///
/// 编解码规则（对应 C++ GetBlockId / GetBlockIndex）：
///   block_id    = block_pointer & !(0xFF << 56)
///   block_index = block_pointer >> 56
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct MetaBlockPointer {
    /// 压缩后的块指针（含 block_id + block_index）
    pub block_pointer: u64,
    /// 子块内字节偏移（对应 C++ MetaBlockPointer::offset）
    pub offset: u32,
}

impl MetaBlockPointer {
    /// 无效指针（对应 C++ MetaBlockPointer() 默认构造）
    pub const INVALID: Self = Self {
        block_pointer: u64::MAX,
        offset: 0,
    };

    /// 从 block_id + block_index + offset 构造
    pub fn new(block_id: BlockId, block_index: u8, offset: u32) -> Self {
        let block_pointer = (block_id as u64 & !(0xFFu64 << 56))
            | ((block_index as u64) << 56);
        Self { block_pointer, offset }
    }

    /// 对应 C++ MetaBlockPointer::GetBlockId()
    pub fn get_block_id(&self) -> BlockId {
        (self.block_pointer & !(0xFFu64 << 56)) as BlockId
    }

    /// 对应 C++ MetaBlockPointer::GetBlockIndex()
    pub fn get_block_index(&self) -> u8 {
        (self.block_pointer >> 56) as u8
    }

    /// 对应 C++ MetaBlockPointer::IsValid()
    pub fn is_valid(&self) -> bool {
        self.block_pointer != u64::MAX
    }
}

// ─── BlockPointer ─────────────────────────────────────────────

/// 对应 C++ BlockPointer（简单块指针：block_id + byte offset）
///
/// 与 MetaBlockPointer 的区别：
///   BlockPointer = 直接的 (block_id, byte_offset) 对，没有位打包；
///   MetaBlockPointer = 位打包版本（适合写入磁盘）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct BlockPointer {
    pub block_id: BlockId,
    pub offset: u32,
}

impl BlockPointer {
    pub fn new(block_id: BlockId, offset: u32) -> Self {
        Self { block_id, offset }
    }

    /// 对应 C++ BlockPointer::IsValid()
    pub fn is_valid(&self) -> bool {
        self.block_id != INVALID_BLOCK
    }
}

// ─── MetadataPointer ─────────────────────────────────────────

/// 对应 C++ MetadataPointer（内存中的解压缩版本）
///
/// C++ 使用位域结构体：
///   idx_t block_index : 56;
///   uint8_t index     : 8;
///
/// Rust 直接用两个普通字段（无位域）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataPointer {
    /// 对应块的 block_id（或 block_index 的扩展 56 位值）
    /// 注意：实际存储的是 block_id 的值（block_id_t 截断到 56 位）
    pub block_index: u64,
    /// 同一存储块内的第几个 meta 子块（0-63）
    pub index: u8,
}

impl MetadataPointer {
    pub fn new(block_index: u64, index: u8) -> Self {
        Self { block_index, index }
    }
}

// ─── MetadataHandle ───────────────────────────────────────────

/// 对应 C++ MetadataHandle
///
/// 持有一个已 pin 的存储块（BufferHandle）及其在元数据块表中的位置。
/// Drop 时自动 unpin（通过 BufferHandle::Drop）。
pub struct MetadataHandle {
    /// 块在元数据块表中的位置
    pub pointer: MetadataPointer,
    /// 已 pin 的缓冲区句柄（对应 C++ BufferHandle handle）
    pub handle: BufferHandle,
}

// ─── MetadataBlockInfo ────────────────────────────────────────

/// 对应 C++ MetadataBlockInfo（用于 GetMetadataInfo() 诊断输出）
#[derive(Debug, Clone)]
pub struct MetadataBlockInfo {
    pub block_id:     BlockId,
    pub total_blocks: usize,
    pub free_list:    Vec<u8>,
}

// ─── ReadStream / WriteStream traits ─────────────────────────

/// 对应 C++ ReadStream（抽象只读字节流）
///
/// 元数据读取器（MetadataReader）和其他反序列化器实现此 trait。
pub trait ReadStream {
    /// 从流中读取 `buf.len()` 字节（对应 C++ ReadData(data_ptr_t, idx_t)）
    fn read_data(&mut self, buf: &mut [u8]);

    /// 是否已到达数据末尾（用于优雅地终止解析）
    fn is_eof(&self) -> bool { false }

    // ── 便捷泛型读取方法 ────────────────────────────────────

    /// 以 little-endian 读取 u8
    fn read_u8(&mut self) -> u8 {
        let mut buf = [0u8; 1];
        self.read_data(&mut buf);
        buf[0]
    }

    /// 以 little-endian 读取 u16
    fn read_u16(&mut self) -> u16 {
        let mut buf = [0u8; 2];
        self.read_data(&mut buf);
        u16::from_le_bytes(buf)
    }

    /// 以 little-endian 读取 u32
    fn read_u32(&mut self) -> u32 {
        let mut buf = [0u8; 4];
        self.read_data(&mut buf);
        u32::from_le_bytes(buf)
    }

    /// 以 little-endian 读取 u64 / usize
    fn read_u64(&mut self) -> u64 {
        let mut buf = [0u8; 8];
        self.read_data(&mut buf);
        u64::from_le_bytes(buf)
    }

    /// 以 little-endian 读取 i64（block_id_t）
    fn read_i64(&mut self) -> i64 {
        let mut buf = [0u8; 8];
        self.read_data(&mut buf);
        i64::from_le_bytes(buf)
    }
}

/// 对应 C++ WriteStream（抽象只写字节流）
///
/// 元数据写入器（MetadataWriter）实现此 trait。
pub trait WriteStream {
    /// 向流中写入 `buf` 的全部字节（对应 C++ WriteData(const_data_ptr_t, idx_t)）
    fn write_data(&mut self, buf: &[u8]);

    // ── 便捷泛型写入方法 ────────────────────────────────────

    fn write_u8(&mut self, v: u8) {
        self.write_data(&[v]);
    }

    fn write_u16(&mut self, v: u16) {
        self.write_data(&v.to_le_bytes());
    }

    fn write_u32(&mut self, v: u32) {
        self.write_data(&v.to_le_bytes());
    }

    fn write_u64(&mut self, v: u64) {
        self.write_data(&v.to_le_bytes());
    }

    fn write_i64(&mut self, v: i64) {
        self.write_data(&v.to_le_bytes());
    }
}
