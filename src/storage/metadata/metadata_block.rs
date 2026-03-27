// ============================================================
// metadata/metadata_block.rs — 元数据块
// 对应 C++: MetadataBlock（定义在 metadata_manager.hpp）
// ============================================================
//
// 每个 MetadataBlock 对应磁盘上一个存储块（FILE_HEADER_SIZE 大小），
// 被划分为 METADATA_BLOCK_COUNT（64）个等大小的 meta 子块。
//
// free_blocks: 当前未使用的子块索引列表（0-63），用栈方式管理（pop_back = 分配）。
// dirty:       是否有未写回磁盘的修改（AtomicBool，可在持锁外读取）。
//
// free_blocks 位图编解码（对应 FreeBlocksToInteger / FreeBlocksFromInteger）：
//   bit i = 1 表示 sub-block i 空闲；bit i = 0 表示已使用。
//   序列化/反序列化时用一个 u64 存储。

use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

use crate::storage::buffer::{BlockHandle, BlockId, INVALID_BLOCK};

use super::super::metadata::MetadataManager;
use super::types::{ReadStream, WriteStream};

// ─── MetadataBlock ────────────────────────────────────────────

/// 对应 C++ MetadataBlock
///
/// 不可 Clone（对应 C++ 删除拷贝构造）；但可 move（ `block.block` 通过 swap 转移）。
pub struct MetadataBlock {
    /// 已 pin 的 BlockHandle（对应 C++ shared_ptr<BlockHandle> block）
    /// None 表示该块尚未在内存中注册（如从磁盘读取后尚未 Pin）
    pub block: Option<Arc<BlockHandle>>,

    /// 对应磁盘上的持久化 block ID（对应 C++ block_id_t block_id）
    pub block_id: BlockId,

    /// 当前空闲的 meta 子块索引列表（0-63）
    /// 用栈方式分配：pop 取末尾（对应 C++ vector<uint8_t> free_blocks 的 back() / pop_back()）
    pub free_blocks: Vec<u8>,

    /// 是否存在未写回的修改（对应 C++ atomic<bool> dirty）
    pub dirty: AtomicBool,
}

impl MetadataBlock {
    /// 对应 C++ MetadataBlock() 默认构造
    pub fn new() -> Self {
        Self {
            block: None,
            block_id: INVALID_BLOCK,
            free_blocks: Vec::new(),
            dirty: AtomicBool::new(false),
        }
    }

    /// 将 free_blocks 列表编码为 64 位位图
    ///
    /// 对应 C++ MetadataBlock::FreeBlocksToInteger()：
    ///   bit i = 1 ⟺ sub-block i 在 free_blocks 中（即空闲）
    pub fn free_blocks_to_integer(&self) -> u64 {
        let mut result: u64 = 0;
        for &idx in &self.free_blocks {
            debug_assert!((idx as usize) < MetadataManager::METADATA_BLOCK_COUNT);
            result |= 1u64 << (idx as u64);
        }
        result
    }

    /// 从 64 位位图解码 free_blocks 列表
    ///
    /// 对应 C++ MetadataBlock::FreeBlocksFromInteger()：
    ///   遍历 bit 63 → bit 0，对每个 set bit 将其索引 push 进 free_blocks。
    ///   (push 顺序：高位优先，使得 pop_back 先取低索引)
    pub fn free_blocks_from_integer(&mut self, free_list: u64) {
        self.free_blocks.clear();
        if free_list == 0 {
            return;
        }
        self.free_blocks = Self::blocks_from_integer(free_list);
    }

    /// 对应 C++ MetadataBlock::BlocksFromInteger()（静态方法）
    pub fn blocks_from_integer(free_list: u64) -> Vec<u8> {
        let mut blocks = Vec::new();
        for i in (0u64..64).rev() {
            if (free_list >> i) & 1 != 0 {
                blocks.push(i as u8);
            }
        }
        blocks
    }

    // ─── 序列化 ───────────────────────────────────────────────

    /// 对应 C++ MetadataBlock::Write(WriteStream &sink)
    pub fn write(&self, sink: &mut dyn WriteStream) {
        sink.write_i64(self.block_id);
        sink.write_u64(self.free_blocks_to_integer());
    }

    /// 对应 C++ MetadataBlock::Read(ReadStream &source)（静态工厂）
    pub fn read(source: &mut dyn ReadStream) -> Self {
        let mut block = Self::new();
        block.block_id = source.read_i64();
        let free_list = source.read_u64();
        block.free_blocks_from_integer(free_list);
        block
    }

    // ─── 诊断 ─────────────────────────────────────────────────

    /// 对应 C++ MetadataBlock::ToString()
    pub fn to_string_repr(&self) -> String {
        let free_strs: Vec<String> = {
            let mut idxs: Vec<u8> = self.free_blocks.clone();
            idxs.sort_unstable();
            idxs.iter().map(|i| i.to_string()).collect()
        };
        format!("block_id: {} [{}]", self.block_id, free_strs.join(", "))
    }
}

impl Default for MetadataBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetadataBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataBlock")
            .field("block_id", &self.block_id)
            .field("free_blocks", &self.free_blocks)
            .field("dirty", &self.dirty.load(Ordering::Relaxed))
            .finish()
    }
}

// MetadataBlock 不可 Clone（对应 C++ 删除拷贝构造）
// 但可以安全 move（Rust 的 move 语义自动满足）
