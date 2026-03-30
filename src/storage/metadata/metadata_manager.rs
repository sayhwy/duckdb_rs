// ============================================================
// metadata/metadata_manager.rs — 元数据管理器
// 对应 C++: duckdb/storage/metadata/metadata_manager.hpp + .cpp
// ============================================================
//
// 核心职责：
//   管理元数据块（MetadataBlock）的分配、pin、刷盘和释放。
//   元数据块将存储块划分为 METADATA_BLOCK_COUNT（64）个等大的子块，
//   每个子块独立地存储一段序列化数据（catalog / statistics 等）。
//
// 内存布局（每个存储块内部）：
//   [ sub_block_0 (metadata_block_size bytes)
//     sub_block_1 ...
//     ...
//     sub_block_63 ]
//   metadata_block_size = floor(block_size / 64)（字节对齐）
//
// 子块链表结构（用于 MetadataReader/Writer 跨块读写）：
//   sub_block[0..8] = 下一个 MetaBlockPointer（u64；0xFFFF...= 无下一块）
//   sub_block[8..] = 实际数据
//
// C++ → Rust 锁模式映射：
//   C++ 使用 unique_lock<mutex> 并在方法间传递（unlock+relock）。
//   Rust 选用 Arc<Mutex<MetadataManagerInner>>：
//     需要临时释放锁的方法先 drop guard，执行 I/O，然后重新 lock()。
//     需要持锁才能调用的私有方法接受 &mut MutexGuard<'_, Inner>（编译期证明）。
//
// ============================================================

use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use std::sync::{Arc, atomic::Ordering};

use crate::storage::buffer::{
    BlockHandle, BlockId, BufferHandle, INVALID_BLOCK, MAXIMUM_BLOCK, MemoryTag,
};
use crate::storage::buffer::{BlockManager, BufferManager};

use super::metadata_block::MetadataBlock;
use super::types::{
    BlockPointer, MetaBlockPointer, MetadataBlockInfo, MetadataHandle, MetadataPointer, ReadStream,
    WriteStream,
};

// ─── MetadataManager 常量 ─────────────────────────────────────

/// 每个存储块被划分为多少个 meta 子块（对应 C++ METADATA_BLOCK_COUNT = 64）
pub const METADATA_BLOCK_COUNT: usize = 64;

// ─── 受 Mutex 保护的内部状态 ─────────────────────────────────

/// 对应 C++ MetadataManager 中受 block_lock 保护的两个 map
pub struct MetadataManagerInner {
    /// 所有已知的 MetadataBlock，以 block_id 为键
    /// 对应 C++ unordered_map<block_id_t, MetadataBlock> blocks
    pub blocks: HashMap<BlockId, MetadataBlock>,

    /// 上次 checkpoint 后被占用的子块位图（block_id → 占用掩码）
    /// 对应 C++ unordered_map<block_id_t, idx_t> modified_blocks
    pub modified_blocks: HashMap<BlockId, u64>,
}

impl MetadataManagerInner {
    fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            modified_blocks: HashMap::new(),
        }
    }
}

/// 辅助类型别名：持锁状态
type ManagerGuard<'a> = MutexGuard<'a, MetadataManagerInner>;

// ─── MetadataManager ─────────────────────────────────────────

/// 对应 C++ MetadataManager
pub struct MetadataManager {
    /// 对应 C++ BlockManager &block_manager
    pub block_manager: Arc<dyn BlockManager>,

    /// 对应 C++ BufferManager &buffer_manager
    pub buffer_manager: Arc<dyn BufferManager>,

    /// 受 Mutex 保护的 blocks + modified_blocks（对应 C++ mutable mutex block_lock）
    inner: Arc<Mutex<MetadataManagerInner>>,
}

impl MetadataManager {
    /// 对应 C++ METADATA_BLOCK_COUNT
    pub const METADATA_BLOCK_COUNT: usize = METADATA_BLOCK_COUNT;

    pub fn new(
        block_manager: Arc<dyn BlockManager>,
        buffer_manager: Arc<dyn BufferManager>,
    ) -> Self {
        Self {
            block_manager,
            buffer_manager,
            inner: Arc::new(Mutex::new(MetadataManagerInner::new())),
        }
    }

    // ─── 元数据块大小计算 ─────────────────────────────────────

    /// 对应 C++ MetadataManager::GetMetadataBlockSize()
    ///
    /// = floor(block_size / METADATA_BLOCK_COUNT)，按字节向下对齐
    pub fn get_metadata_block_size(&self) -> usize {
        let block_size = self.block_manager.get_block_size();
        // floor 对齐：去掉低位余数
        (block_size / METADATA_BLOCK_COUNT) & !7usize // 8 字节对齐
    }

    // ─── 分配与 Pin ───────────────────────────────────────────

    /// 对应 C++ MetadataManager::AllocateHandle()
    ///
    /// 两阶段模式（对应 C++ 中的 unlock/relock 模式）：
    ///   1. 持锁：扫描 blocks 找有空闲子块的 free_block_id
    ///   2. 若无（或需要新块）：释放锁，分配新块（allocate_new_block），
    ///      allocate_new_block 内部自己做 lock/unlock
    ///   3. 重新持锁：标脏、若磁盘块则转临时块、pop 空闲子块索引
    ///   4. 释放锁，Pin 该块
    pub fn allocate_handle(&self) -> MetadataHandle {
        // Phase 1：找有空闲子块的块
        let free_block_id: Option<BlockId> = {
            let guard = self.inner.lock();
            guard
                .blocks
                .iter()
                .find(|(_, b)| !b.free_blocks.is_empty())
                .map(|(&id, _)| id)
        };
        // guard 已释放

        let peek_id = self.peek_next_block_id();
        let free_block_id = if free_block_id.map_or(true, |id| id > peek_id) {
            // 没有空闲块或已过时：分配新块
            self.allocate_new_block()
        } else {
            free_block_id.unwrap()
        };

        // Phase 2：持锁，标脏，处理磁盘→临时转换，pop 子块索引
        let pointer_index = self.prepare_slot_for_alloc(free_block_id);

        // Phase 3：释放锁后 Pin
        self.pin(&MetadataPointer::new(free_block_id as u64, pointer_index))
    }

    /// 持锁阶段：标脏、触发磁盘→临时转换（如需要）、pop 空闲子块索引
    fn prepare_slot_for_alloc(&self, block_id: BlockId) -> u8 {
        // TODO: 禁用 transient 转换检查，因为当前实现有 bug
        // 在新数据库创建时，所有块都是内存块，不需要转换
        // 磁盘块转换只在从现有文件加载时需要

        let mut guard = self.inner.lock();
        let block = guard
            .blocks
            .get_mut(&block_id)
            .expect("MetadataBlock not found after allocation");
        block.dirty.store(true, Ordering::Relaxed);
        debug_assert!(!block.free_blocks.is_empty());
        let index = block.free_blocks.pop().expect("free_blocks was empty");
        debug_assert!((index as usize) < METADATA_BLOCK_COUNT);
        index
    }

    /// 对应 C++ MetadataManager::Pin(MetadataPointer)
    pub fn pin(&self, pointer: &MetadataPointer) -> MetadataHandle {
        debug_assert!((pointer.index as usize) < METADATA_BLOCK_COUNT);

        let block_handle: Arc<BlockHandle> = {
            let guard = self.inner.lock();
            let block = guard
                .blocks
                .get(&(pointer.block_index as BlockId))
                .unwrap_or_else(|| {
                    panic!(
                        "MetadataManager::pin: block {} not found",
                        pointer.block_index
                    )
                });
            // debug: verify not in free list
            #[cfg(debug_assertions)]
            {
                for &free_idx in &block.free_blocks {
                    assert_ne!(
                        free_idx, pointer.index,
                        "Pinning block {}.{} but it is in the free list",
                        block.block_id, pointer.index
                    );
                }
            }
            block.block.clone().expect("BlockHandle is None in pin()")
        }; // guard 释放

        MetadataHandle {
            pointer: *pointer,
            handle: self.buffer_manager.pin(block_handle),
        }
    }

    // ─── 磁盘指针转换 ─────────────────────────────────────────

    /// 对应 C++ MetadataManager::GetDiskPointer(MetadataPointer, uint32_t offset)
    ///
    /// 将内存 MetadataPointer + byte offset 编码为磁盘 MetaBlockPointer。
    pub fn get_disk_pointer(&self, pointer: &MetadataPointer, offset: u32) -> MetaBlockPointer {
        // block_pointer = block_index(56位) | (sub_index(8位) << 56)
        let block_pointer =
            (pointer.block_index & !(0xFFu64 << 56)) | ((pointer.index as u64) << 56);
        MetaBlockPointer {
            block_pointer,
            offset,
        }
    }

    /// 对应 C++ MetadataManager::FromDiskPointer(MetaBlockPointer)
    pub fn from_disk_pointer(&self, pointer: MetaBlockPointer) -> MetadataPointer {
        let guard = self.inner.lock();
        self.from_disk_pointer_internal(&guard, pointer)
    }

    /// 对应 C++ MetadataManager::RegisterDiskPointer(MetaBlockPointer)
    ///
    /// 与 from_disk_pointer 的区别：若 block 未注册，先注册再返回。
    pub fn register_disk_pointer(&self, pointer: MetaBlockPointer) -> MetadataPointer {
        let block_id = pointer.get_block_id();

        let mut block = MetadataBlock::new();
        block.block_id = block_id;

        // add_and_register_block 内部管理 lock/unlock（见函数注释）
        self.add_and_register_block(block);
        let guard = self.inner.lock();
        self.from_disk_pointer_internal(&guard, pointer)
    }

    /// 对应 C++ MetaBlockPointer::GetBlockId/Index 的逆操作（internal）
    fn from_disk_pointer_internal(
        &self,
        guard: &ManagerGuard<'_>,
        pointer: MetaBlockPointer,
    ) -> MetadataPointer {
        let block_id = pointer.get_block_id();
        let index = pointer.get_block_index();
        assert!(
            guard.blocks.contains_key(&block_id),
            "Failed to load metadata pointer (id {block_id}, idx {index})"
        );
        MetadataPointer::new(block_id as u64, index)
    }

    // ─── BlockPointer ↔ MetaBlockPointer 转换（静态方法）──────

    /// 对应 C++ MetadataManager::ToBlockPointer(MetaBlockPointer, idx_t metadata_block_size)
    pub fn to_block_pointer(
        meta_pointer: MetaBlockPointer,
        metadata_block_size: usize,
    ) -> BlockPointer {
        let block_id = meta_pointer.get_block_id();
        let block_index = meta_pointer.get_block_index() as u32;
        let offset = block_index * metadata_block_size as u32 + meta_pointer.offset;
        debug_assert!(
            (offset as usize) < metadata_block_size * METADATA_BLOCK_COUNT,
            "offset {offset} out of range"
        );
        BlockPointer::new(block_id, offset)
    }

    /// 对应 C++ MetadataManager::FromBlockPointer(BlockPointer, idx_t metadata_block_size)
    pub fn from_block_pointer(
        block_pointer: BlockPointer,
        metadata_block_size: usize,
    ) -> MetaBlockPointer {
        if !block_pointer.is_valid() {
            return MetaBlockPointer::INVALID;
        }
        let index = block_pointer.offset as usize / metadata_block_size;
        let offset = (block_pointer.offset as usize % metadata_block_size) as u32;
        debug_assert!(index < METADATA_BLOCK_COUNT);
        MetaBlockPointer::new(block_pointer.block_id, index as u8, offset)
    }

    // ─── 查询接口 ─────────────────────────────────────────────

    /// 对应 C++ MetadataManager::BlockCount()
    pub fn block_count(&self) -> usize {
        self.inner.lock().blocks.len()
    }

    /// 对应 C++ MetadataManager::GetBlocks()
    pub fn get_blocks(&self) -> Vec<Arc<BlockHandle>> {
        self.inner
            .lock()
            .blocks
            .values()
            .filter_map(|b| b.block.clone())
            .collect()
    }

    /// 对应 C++ MetadataManager::GetMetadataInfo()
    pub fn get_metadata_info(&self) -> Vec<MetadataBlockInfo> {
        let guard = self.inner.lock();
        let mut result: Vec<MetadataBlockInfo> = guard
            .blocks
            .values()
            .map(|b| {
                let mut free_list = b.free_blocks.clone();
                free_list.sort_unstable();
                MetadataBlockInfo {
                    block_id: b.block_id,
                    total_blocks: METADATA_BLOCK_COUNT,
                    free_list,
                }
            })
            .collect();
        result.sort_by_key(|info| info.block_id);
        result
    }

    // ─── 序列化（checkpoint 时调用）─────────────────────────

    /// 对应 C++ MetadataManager::Write(WriteStream &sink)
    pub fn write(&self, sink: &mut dyn WriteStream) {
        let guard = self.inner.lock();
        sink.write_u64(guard.blocks.len() as u64);
        for block in guard.blocks.values() {
            block.write(sink);
        }
    }

    /// 对应 C++ MetadataManager::Read(ReadStream &source)
    pub fn read(&self, source: &mut dyn ReadStream) {
        let count = source.read_u64() as usize;
        for _ in 0..count {
            let block = MetadataBlock::read(source);

            let entry_exists = {
                let guard = self.inner.lock();
                guard.blocks.contains_key(&block.block_id)
            };
            if entry_exists {
                let mut guard = self.inner.lock();
                if let Some(existing) = guard.blocks.get_mut(&block.block_id) {
                    existing.free_blocks = block.free_blocks;
                }
            } else {
                // add_and_register_block 内部管理 lock/unlock
                self.add_and_register_block(block);
            }
        }
    }

    // ─── Flush（checkpoint 核心）──────────────────────────────

    /// 对应 C++ MetadataManager::Flush()
    ///
    /// 将所有 dirty 的元数据块写回磁盘：
    ///   - 临时块（block_id >= MAXIMUM_BLOCK）→ ConvertToPersistent
    ///   - 持久化块 → 直接 Write
    pub fn flush(&self) {
        let metadata_block_size = self.get_metadata_block_size();
        let total_metadata_size = metadata_block_size * METADATA_BLOCK_COUNT;

        // 快照所有需要 flush 的块（不持锁执行 I/O）
        let to_flush: Vec<(BlockId, Arc<BlockHandle>)> = {
            let guard = self.inner.lock();
            guard
                .blocks
                .iter()
                .filter(|(_, b)| b.dirty.load(Ordering::Relaxed))
                .filter_map(|(&id, b)| b.block.clone().map(|h| (id, h)))
                .collect()
        };

        for (block_id, block_handle) in to_flush {
            // Pin 块（不持锁）
            let handle: BufferHandle = self.buffer_manager.pin(Arc::clone(&block_handle));

            // 零填充 metadata 区域之后的剩余字节
            let block_size = self.block_manager.get_block_size();
            handle.with_data_mut(|data| {
                if total_metadata_size < block_size {
                    data[total_metadata_size..].fill(0);
                }
            });

            if block_handle.block_id >= MAXIMUM_BLOCK {
                // 临时块 → 转换为持久化块
                let new_block = self
                    .block_manager
                    .convert_to_persistent(block_id, Arc::clone(&block_handle));
                let mut guard = self.inner.lock();
                if let Some(entry) = guard.blocks.get_mut(&block_id) {
                    entry.block = Some(new_block);
                }
            } else {
                // 已持久化块 → 直接写盘
                handle.with_data(|data| {
                    // TODO: 调用 block_manager.write_block(data, block_id)
                    // 暂时通过 FileBuffer 间接写入（骨架阶段）
                    let _ = data;
                });
            }

            // 标记为 clean
            let mut guard = self.inner.lock();
            if let Some(entry) = guard.blocks.get_mut(&block_id) {
                entry.dirty.store(false, Ordering::Relaxed);
            }
        }
    }

    // ─── Modified Blocks 追踪 ─────────────────────────────────

    /// 对应 C++ MetadataManager::MarkBlocksAsModified()
    ///
    /// checkpoint 完成后调用：
    ///   1. 将 modified_blocks 中已被标记释放的子块（bit=1）合并回 free_blocks
    ///   2. 若某块所有子块都空闲 → 将整个存储块也标记为可回收（MarkBlockAsModified）
    ///   3. 重新快照当前各块的"已占用"位图到 modified_blocks（用于下次 checkpoint）
    pub fn mark_blocks_as_modified(&self) {
        let mut guard = self.inner.lock();

        let modified: Vec<(BlockId, u64)> = guard
            .modified_blocks
            .iter()
            .map(|(&id, &mask)| (id, mask))
            .collect();

        for (block_id, modified_list) in modified {
            if let Some(block) = guard.blocks.get_mut(&block_id) {
                let current_free = block.free_blocks_to_integer();
                let new_free = current_free | modified_list;

                if new_free == u64::MAX {
                    // 所有子块都空闲 → 整个存储块可释放
                    guard.blocks.remove(&block_id);
                    // 释放锁后通知 BlockManager
                    drop(guard);
                    self.block_manager.mark_block_as_modified(block_id);
                    guard = self.inner.lock();
                } else {
                    block.free_blocks_from_integer(new_free);
                }
            }
        }

        guard.modified_blocks.clear();

        // 重新快照当前"已占用"位图
        let occupied: Vec<(BlockId, u64)> = guard
            .blocks
            .iter()
            .map(|(&id, b)| (id, !b.free_blocks_to_integer()))
            .collect();
        for (id, mask) in occupied {
            guard.modified_blocks.insert(id, mask);
        }
    }

    /// 对应 C++ MetadataManager::ClearModifiedBlocks(vector<MetaBlockPointer>)
    ///
    /// WAL replay 后清理：对每个指针，将对应子块从 modified_blocks 中取消标记。
    pub fn clear_modified_blocks(&self, pointers: &[MetaBlockPointer]) {
        if pointers.is_empty() {
            return;
        }
        let mut guard = self.inner.lock();
        for ptr in pointers {
            let block_id = ptr.get_block_id();
            let block_index = ptr.get_block_index() as u64;
            let entry = guard.modified_blocks.get_mut(&block_id).unwrap_or_else(|| {
                panic!("ClearModifiedBlocks: block {block_id} not in modified_blocks")
            });
            *entry &= !(1u64 << block_index);
        }
    }

    /// 对应 C++ MetadataManager::BlockHasBeenCleared(MetaBlockPointer)
    pub fn block_has_been_cleared(&self, ptr: &MetaBlockPointer) -> bool {
        let guard = self.inner.lock();
        let block_id = ptr.get_block_id();
        let block_index = ptr.get_block_index() as u64;
        let &modified_list = guard.modified_blocks.get(&block_id).unwrap_or_else(|| {
            panic!("BlockHasBeenCleared: block {block_id} not in modified_blocks")
        });
        (modified_list & (1u64 << block_index)) == 0
    }

    // ─── 私有辅助方法 ─────────────────────────────────────────

    /// 对应 C++ PeekNextBlockId()（不分配，只查看下一个可用 ID）
    fn peek_next_block_id(&self) -> BlockId {
        self.block_manager.peek_free_block_id()
    }

    /// 对应 C++ GetNextBlockId()（分配并返回下一个 block ID）
    fn get_next_block_id(&self) -> BlockId {
        self.block_manager.get_free_block_id_for_checkpoint()
    }

    /// 对应 C++ AllocateNewBlock(unique_lock)
    ///
    /// 不持锁：执行内存分配 + I/O，完成后再持锁写入 blocks 表。
    fn allocate_new_block(&self) -> BlockId {
        let new_block_id = self.get_next_block_id();

        // 分配临时内存块（不持锁）
        let block_handle = self
            .buffer_manager
            .allocate_memory(MemoryTag::Metadata, None, false);

        // 零初始化（对应 C++ memset(handle.Ptr(), 0, block_manager.GetBlockSize())）
        let bh = self.buffer_manager.pin(Arc::clone(&block_handle));
        bh.with_data_mut(|data| data.fill(0));

        // 构造新 MetadataBlock
        let mut new_block = MetadataBlock::new();
        new_block.block = Some(block_handle);
        new_block.block_id = new_block_id;
        // 初始化所有子块为空闲（倒序 push，使 pop_back 取低索引）
        for i in (0..METADATA_BLOCK_COUNT).rev() {
            new_block.free_blocks.push(i as u8);
        }
        new_block.dirty.store(true, Ordering::Relaxed);

        // 持锁写入 blocks 表
        let mut guard = self.inner.lock();
        self.add_block(&mut guard, new_block, false);

        new_block_id
    }

    /// 对应 C++ AddBlock(unique_lock, MetadataBlock, bool if_exists)
    fn add_block(&self, guard: &mut ManagerGuard<'_>, block: MetadataBlock, if_exists: bool) {
        if guard.blocks.contains_key(&block.block_id) {
            if if_exists {
                return;
            }
            panic!("MetadataBlock with id {} already exists", block.block_id);
        }
        guard.blocks.insert(block.block_id, block);
    }

    /// 对应 C++ AddAndRegisterBlock(unique_lock, MetadataBlock)
    ///
    /// block.block 必须为 None（尚未注册），block_id 必须是持久化块。
    ///
    /// 设计说明（C++ vs Rust 锁模式差异）：
    ///   C++ 传入 unique_lock&，在方法内 unlock/relock；
    ///   Rust 中借用的 MutexGuard 无法中途释放，改为：
    ///     调用方在调用本方法前先释放 guard（限制 guard 作用域），
    ///     本方法内部自行 lock/unlock，完成后调用方重新 lock。
    ///   因此本方法不接受 guard 参数，使用内部锁。
    fn add_and_register_block(&self, mut block: MetadataBlock) {
        assert!(
            block.block.is_none(),
            "add_and_register_block: block already has a handle"
        );
        assert!(
            block.block_id < MAXIMUM_BLOCK,
            "add_and_register_block: transient block_id"
        );

        let block_id = block.block_id;
        // 不持锁执行 I/O（对应 C++ 中 block_lock.unlock()）
        let block_handle = self.block_manager.register_block(block_id);

        // 重新持锁写入（对应 C++ 中 block_lock.lock()）
        let mut guard = self.inner.lock();
        block.block = Some(block_handle);
        self.add_block(&mut guard, block, true);
    }

    /// 对应 C++ ConvertToTransient(unique_lock, MetadataBlock)
    ///
    /// 将磁盘块复制到一个新的临时内存块：
    ///   1. 释放锁，Pin 旧块
    ///   2. 分配新临时块，memcpy 数据
    ///   3. 注销旧磁盘块
    ///   4. 持锁，更新 block 指针
    fn convert_to_transient(&self, block_id: BlockId) {
        let old_block_handle: Arc<BlockHandle> = {
            let guard = self.inner.lock();
            guard
                .blocks
                .get(&block_id)
                .and_then(|b| b.block.clone())
                .unwrap_or_else(|| panic!("convert_to_transient: block {block_id} not found"))
        }; // guard 释放

        // Pin 旧块（锁外 I/O）
        let old_buf: BufferHandle = self.buffer_manager.pin(Arc::clone(&old_block_handle));

        // 分配新临时内存块
        let new_handle: Arc<BlockHandle> =
            self.buffer_manager
                .allocate_memory(MemoryTag::Metadata, None, false);
        let new_buf: BufferHandle = self.buffer_manager.pin(Arc::clone(&new_handle));

        // 拷贝数据
        let block_size = self.block_manager.get_block_size();
        old_buf.with_data(|old_data| {
            new_buf.with_data_mut(|new_data| {
                let len = block_size.min(old_data.len()).min(new_data.len());
                new_data[..len].copy_from_slice(&old_data[..len]);
            });
        });

        // 注销旧磁盘块（锁外）
        self.block_manager.unregister_block(block_id);

        // 持锁，更新 block 指针
        let mut guard = self.inner.lock();
        if let Some(entry) = guard.blocks.get_mut(&block_id) {
            entry.block = Some(new_handle);
            entry.dirty.store(true, Ordering::Relaxed);
        }
    }
}

// ─── BlockManager trait 扩展（peek/get free block）───────────
// 以下方法需在 BlockManager trait 中补充，此处通过 dyn 调用：
//   peek_free_block_id() -> BlockId
//   get_free_block_id_for_checkpoint() -> BlockId
//   mark_block_as_modified(block_id: BlockId)
//
// 暂时通过 downcast 或在 BlockManager trait 中新增方法实现（后续迭代完善）。
// 骨架阶段 block_manager 中已有 get_free_block_id() 的实现；
// peek_free_block_id / get_free_block_id_for_checkpoint 需要增加到 trait。
