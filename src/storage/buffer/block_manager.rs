// ============================================================
// block_manager.rs — BlockManager / BufferManager trait 定义
// 对应 C++:
//   duckdb/storage/block_manager.hpp + block_manager.cpp
//   duckdb/storage/buffer_manager.hpp（部分接口）
// ============================================================
//
// 设计原则：
//   C++ BlockManager 是抽象基类，Rust 用 trait 替代。
//   Rust trait 不支持虚继承，通过 Arc<dyn BlockManager> 实现多态。
//
//   BlockManager trait 提供存储层操作（读/写/注册/注销 block）。
//   BufferManager trait 提供内存管理层操作（pin/unpin/分配）。
//
//   两者分开定义，通过 BlockManager::buffer_manager() 关联：
//   对应 C++ 中 BlockManager 持有 BufferManager& 成员。

use super::block_handle::BlockHandle;
use super::buffer_handle::BufferHandle;
use super::buffer_pool::BufferPool;
use super::file_buffer::FileBuffer;
use super::types::{BlockId, MAXIMUM_BLOCK, MemoryTag};
use std::any::Any;
use std::sync::Arc;

// ─── BufferManager trait ──────────────────────────────────────
/// 对应 C++ BufferManager（部分接口）
///
/// 负责内存管理层：pin/unpin、临时文件、内存分配。
/// 完整实现在后续 StandardBufferManager 中。
pub trait BufferManager: Send + Sync {
    /// 对应 C++ BufferManager::Pin()
    /// pin 一个 block（加载到内存，递增 reader 计数），返回 BufferHandle。
    fn pin(&self, handle: Arc<BlockHandle>) -> BufferHandle;

    /// 对应 C++ BufferManager::Unpin()
    /// 减少 reader 计数，reader 归零时将 block 加入驱逐队列。
    fn unpin(&self, handle: Arc<BlockHandle>);

    /// 获取关联的 BufferPool
    fn get_buffer_pool(&self) -> Arc<BufferPool>;

    /// 对应 C++ BufferManager::HasTemporaryDirectory()
    fn has_temporary_directory(&self) -> bool;

    /// 对应 C++ BufferManager::ReadTemporaryBuffer()
    /// 从临时文件恢复一个曾经溢出的 ManagedBuffer。
    fn read_temporary_buffer(
        &self,
        tag: MemoryTag,
        block_id: BlockId,
        reusable: Option<Box<FileBuffer>>,
    ) -> Box<FileBuffer>;

    /// 对应 C++ BufferManager::WriteTemporaryBuffer()
    /// 将 ManagedBuffer 溢写到临时文件。
    fn write_temporary_buffer(&self, tag: MemoryTag, block_id: BlockId, buffer: &FileBuffer);

    /// 对应 C++ BufferManager::DeleteTemporaryFile()
    fn delete_temporary_file(&self, block: &BlockHandle);

    /// 对应 C++ BufferManager::AllocateMemory()
    /// 分配一个新的内存 block（ManagedBuffer 类型）。
    fn allocate_memory(
        &self,
        tag: MemoryTag,
        block_manager: Option<&dyn BlockManager>,
        can_destroy: bool,
    ) -> Arc<BlockHandle>;
}

// ─── BlockManager trait ──────────────────────────────────────
/// 对应 C++ BlockManager（抽象基类 → Rust trait）
///
/// 负责存储层：读写磁盘 block、注册/注销 block 句柄、转换持久化块。
pub trait BlockManager: Send + Sync {
    // ── 关联的 BufferManager ─────────────────────────────────

    /// 对应 C++ BlockManager::buffer_manager 成员
    fn buffer_manager(&self) -> Arc<dyn BufferManager>;

    /// 尝试获取 BufferPool（用于 BlockHandle::Drop 中通知 dead node）
    /// 默认实现通过 buffer_manager() 获取
    fn try_get_buffer_pool(&self) -> Option<Arc<BufferPool>> {
        Some(self.buffer_manager().get_buffer_pool())
    }

    // ── Block 大小配置 ───────────────────────────────────────

    /// 对应 C++ GetBlockAllocSize()（含 header 的总分配大小）
    fn get_block_alloc_size(&self) -> usize;

    /// 对应 C++ GetBlockHeaderSize()
    fn get_block_header_size(&self) -> usize;

    /// 对应 C++ GetBlockSize()（payload 大小 = alloc - header）
    fn get_block_size(&self) -> usize {
        self.get_block_alloc_size() - self.get_block_header_size()
    }

    fn has_temporary_directory(&self) -> bool {
        self.buffer_manager().has_temporary_directory()
    }

    // ── Block 注册/注销 ──────────────────────────────────────

    /// 对应 C++ BlockManager::RegisterBlock(block_id_t block_id)
    ///
    /// 幂等：若已注册且 handle 未过期则返回已有 Arc；
    /// 否则创建新 BlockHandle 并弱引用存入 blocks 表。
    fn register_block(&self, block_id: BlockId) -> Arc<BlockHandle>;

    /// 对应 C++ BlockManager::UnregisterBlock(block_id_t id)（按 ID 注销，持久化块）
    fn unregister_block(&self, block_id: BlockId);

    /// 对应 C++ BlockManager::UnregisterBlock(BlockHandle &block)
    /// 区分持久化块（按 ID 注销）和临时块（删除临时文件）。
    fn unregister_block_by_handle(&self, block: &BlockHandle) {
        if block.block_id >= MAXIMUM_BLOCK {
            self.buffer_manager().delete_temporary_file(block);
        } else {
            self.unregister_block(block.block_id);
        }
    }

    /// 对应 C++ BlockManager::BlockIsRegistered()
    fn block_is_registered(&self, block_id: BlockId) -> bool;

    // ── Block 读写 ───────────────────────────────────────────

    /// 对应 C++ CreateBlock(block_id_t, FileBuffer*)
    ///
    /// 分配一个新 FileBuffer 用于持久化 block；
    /// 若提供 `reusable` 且 header_size 匹配则复用。
    fn create_block(&self, block_id: BlockId, reusable: Option<Box<FileBuffer>>)
    -> Box<FileBuffer>;

    /// 对应 C++ Read(FileBuffer &block, block_id_t id)
    /// 从磁盘读取 block_id 对应的数据到 block.raw_mut()。
    fn read_block(&self, block: &mut FileBuffer, block_id: BlockId);

    /// 对应 C++ Write(FileBuffer &block, block_id_t block_id)
    /// 将 block 数据写入磁盘。
    fn write_block(&self, block: &FileBuffer, block_id: BlockId);

    /// 对应 C++ Read(QueryContext, FileBuffer &block)（新签名，默认转发到 read_block）
    fn read_block_with_context(&self, block: &mut FileBuffer, block_id: BlockId) {
        self.read_block(block, block_id);
    }

    // ── 临时文件操作（委托给 BufferManager）─────────────────

    fn read_temporary_buffer(
        &self,
        tag: MemoryTag,
        block_id: BlockId,
        reusable: Option<Box<FileBuffer>>,
    ) -> Box<FileBuffer> {
        self.buffer_manager()
            .read_temporary_buffer(tag, block_id, reusable)
    }

    fn write_temporary_buffer(&self, tag: MemoryTag, block_id: BlockId, buffer: &FileBuffer) {
        self.buffer_manager()
            .write_temporary_buffer(tag, block_id, buffer);
    }

    // ── 持久化转换 ───────────────────────────────────────────

    /// 对应 C++ ConvertBlock(block_id_t, FileBuffer &source)
    ///
    /// 将 source FileBuffer 转换为适合持久化的 Block 格式
    /// （可能需要重新分配以匹配 header 大小）。
    fn convert_block(&self, block_id: BlockId, source: &FileBuffer) -> Box<FileBuffer>;

    /// 对应 C++ ConvertToPersistent(QueryContext, block_id_t, Arc<BlockHandle>)
    ///
    /// 将内存临时块转换为持久化块：
    ///   1. RegisterBlock(block_id) 获取 new_block
    ///   2. 将 old_block 的 buffer 转换并写入磁盘
    ///   3. ConvertToPersistent 更新状态
    ///   4. 加入驱逐队列
    fn convert_to_persistent(
        &self,
        block_id: BlockId,
        old_block: Arc<BlockHandle>,
    ) -> Arc<BlockHandle>;

    // ── 空闲块 ID（MetadataManager 使用）────────────────────

    /// 对应 C++ BlockManager::PeekFreeBlockId()
    /// 查看下一个可用 block ID，但不分配（幂等，多次调用返回相同值）。
    fn peek_free_block_id(&self) -> BlockId {
        MAXIMUM_BLOCK - 1 // 默认实现：不支持 peek，返回哨兵值
    }

    /// 对应 C++ BlockManager::GetFreeBlockIdForCheckpoint()
    /// 分配并返回一个用于 checkpoint 元数据的 block ID。
    fn get_free_block_id_for_checkpoint(&self) -> BlockId {
        // 默认实现委托给 register_block（会在 single_file_block_manager 中覆盖）
        let id = self.peek_free_block_id();
        id
    }

    /// 对应 C++ BlockManager::MarkBlockAsModified(block_id_t)
    /// 将指定 block 标记为已修改（checkpoint 后可回收）。
    /// 默认空实现，SingleFileBlockManager 会覆盖。
    fn mark_block_as_modified(&self, _block_id: BlockId) {}

    /// 对应 C++ BlockManager::BlockCount() / max_block
    /// 返回当前已使用的最大 block ID + 1（即下一个可分配的 block ID）。
    /// 默认实现返回 0。
    fn block_count(&self) -> u64 {
        0
    }

    // ── 元数据管理（后续扩展）────────────────────────────────

    /// 对应 C++ GetMetadataManager()
    /// 返回 trait 对象（具体类型在 metadata 模块定义）；未实现时返回 None。
    fn metadata_manager(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }

    /// 对应 C++ Truncate()（默认空实现）
    fn truncate(&self) {}
}
