// ============================================================
// block.rs — 磁盘块（FileBuffer 的具名包装）
// 对应 C++: duckdb/storage/block.hpp + block.cpp
// ============================================================
//
// 核心职责：
//   将 FileBuffer（通用内存缓冲区）与一个磁盘 BlockId 绑定，
//   产生 "Block"——代表磁盘上一个具体位置的缓冲区。
//
// C++ → Rust 映射：
//   class Block : public FileBuffer   →  struct Block { inner: FileBuffer, id: BlockId }
//   Block::id                          →  Block::id
//   D_ASSERT(AllocSize() & (SECTOR_SIZE-1) == 0)  →  debug_assert! in constructor
//
// 设计说明：
//   C++ 通过继承复用 FileBuffer 的所有字段和方法。
//   Rust 没有继承；用组合（inner: FileBuffer）替代，
//   并通过 Deref<Target=FileBuffer> 提供透明访问。
//   这使得凡是接受 &FileBuffer 的地方均可传入 &Block。

use std::ops::{Deref, DerefMut};

use crate::storage::buffer::{BlockId, FileBuffer, FileBufferType};

/// 对应 C++ Storage::SECTOR_SIZE（512 字节，用于 Direct I/O 对齐断言）
pub const SECTOR_SIZE: usize = 512;

// ─── Block ────────────────────────────────────────────────────

/// 对应 C++ Block（继承自 FileBuffer，增加 block_id 字段）
///
/// 表示磁盘上一个物理块在内存中的缓冲区表示。
/// 通过 `Deref<Target=FileBuffer>` 将 FileBuffer 的所有方法透明暴露给外部。
pub struct Block {
    /// 内嵌的 FileBuffer（对应 C++ 基类成员）
    inner: FileBuffer,
    /// 对应 C++ Block::id
    pub id: BlockId,
}

impl Block {
    // ─── 构造函数 ──────────────────────────────────────────────

    /// 对应 C++ Block(BlockAllocator&, block_id_t, idx_t block_size, idx_t block_header_size)
    ///
    /// 创建指定大小的 Block（分配大小必须是 SECTOR_SIZE 的整数倍）。
    pub fn new(
        id: BlockId,
        alloc_size: usize,
        block_header_size: usize,
    ) -> Self {
        debug_assert_eq!(
            alloc_size % SECTOR_SIZE,
            0,
            "Block alloc_size must be sector-aligned (multiple of {SECTOR_SIZE})"
        );
        Self {
            inner: FileBuffer::new(FileBufferType::Block, alloc_size, block_header_size),
            id,
        }
    }

    /// 对应 C++ Block(FileBuffer &source, block_id_t id, idx_t block_header_size)
    ///
    /// 从现有 FileBuffer 重新解释为 Block（用于类型转换路径）。
    pub fn from_file_buffer(source: FileBuffer, id: BlockId) -> Self {
        debug_assert_eq!(
            source.alloc_size() % SECTOR_SIZE,
            0,
            "FileBuffer alloc_size must be sector-aligned"
        );
        // 此处直接接管 FileBuffer 所有权（对应 C++ 的拷贝构造路径中的移动语义）
        Self { inner: source, id }
    }

    // ─── 字段访问 ─────────────────────────────────────────────

    /// 返回 block_id（对应 C++ Block::id）
    pub fn block_id(&self) -> BlockId {
        self.id
    }

    /// 获取内部 FileBuffer 引用
    pub fn file_buffer(&self) -> &FileBuffer {
        &self.inner
    }

    /// 获取内部 FileBuffer 可变引用
    pub fn file_buffer_mut(&mut self) -> &mut FileBuffer {
        &mut self.inner
    }

    /// 消耗 Block，返回内部 FileBuffer 所有权
    pub fn into_file_buffer(self) -> FileBuffer {
        self.inner
    }
}

// ─── Deref 实现：Block → FileBuffer ──────────────────────────

/// 透明代理：`*block` / `block.method()` 直接访问内部 FileBuffer
impl Deref for Block {
    type Target = FileBuffer;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

// ─── 格式化 ───────────────────────────────────────────────────

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("id", &self.id)
            .field("alloc_size", &self.inner.alloc_size())
            .finish()
    }
}
