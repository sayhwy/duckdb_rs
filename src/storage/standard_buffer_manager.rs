// ============================================================
// standard_buffer_manager.rs — 标准缓冲区管理器
// 对应 C++: duckdb/storage/standard_buffer_manager.hpp
//           duckdb/storage/standard_buffer_manager.cpp
// ============================================================
//
// 核心职责：
//   实现 BufferManager trait，协调 BufferPool（内存限制/驱逐）
//   与 BlockManager（块 I/O）之间的交互。
//
// Pin() 两阶段模式（对应 C++ StandardBufferManager::Pin）：
//   Phase 1: 持锁检查 → 已加载则递增 reader，直接返回 BufferHandle
//   Phase 2: 释放锁 → 驱逐内存 → 重新持锁 → load_guarded()
//
// 临时文件管理（对应 C++ TemporaryFileManager）：
//   按需（第一次溢写时）创建目录，以 block_id 十六进制命名文件。
//
// ============================================================

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::storage::buffer::{
    BlockGuard, BlockHandle, BlockId, BlockManager, BlockState, BufferHandle, BufferManager,
    BufferPool, BufferPoolReservation, DestroyBufferUpon, FileBuffer, FileBufferType,
    MAXIMUM_BLOCK, MemoryTag, MemoryTracker,
};

use super::storage_info::{FileOpenFlags, FileSystem, StorageResult};

// ─── 临时目录状态 ─────────────────────────────────────────────

struct TemporaryDirState {
    created: bool,
}

struct TemporaryFileData {
    directory: String,
    state: Mutex<TemporaryDirState>,
}

impl TemporaryFileData {
    fn new(directory: String) -> Self {
        Self {
            directory,
            state: Mutex::new(TemporaryDirState { created: false }),
        }
    }

    fn ensure_created(&self, fs: &dyn FileSystem) -> StorageResult<()> {
        let mut state = self.state.lock();
        if !state.created {
            fs.create_directory(&self.directory)?;
            state.created = true;
        }
        Ok(())
    }

    fn temp_path_for(&self, block_id: BlockId) -> String {
        format!("{}/tmp_{:016x}.bin", self.directory, block_id as u64)
    }
}

// ─── StandardBufferManager ────────────────────────────────────

/// 对应 C++ StandardBufferManager
pub struct StandardBufferManager {
    /// 内存池（对应 C++ buffer_pool_）
    buffer_pool: Arc<BufferPool>,

    /// 关联的 BlockManager（构造后注入，避免循环依赖）
    block_manager: Mutex<Option<Arc<dyn BlockManager>>>,

    /// 临时文件目录（对应 C++ temp_directory_）
    temp_file_data: Mutex<Option<TemporaryFileData>>,

    /// 文件系统（临时文件 I/O）
    fs: Arc<dyn FileSystem>,

    /// 临时块 ID 生成器（从 MAXIMUM_BLOCK 开始，向下递减）
    temporary_id: AtomicI64,
}

impl StandardBufferManager {
    pub fn new(
        buffer_pool: Arc<BufferPool>,
        temp_directory: Option<String>,
        fs: Arc<dyn FileSystem>,
    ) -> Arc<Self> {
        let temp_file_data = temp_directory.map(TemporaryFileData::new);
        Arc::new(Self {
            buffer_pool,
            block_manager: Mutex::new(None),
            temp_file_data: Mutex::new(temp_file_data),
            fs,
            temporary_id: AtomicI64::new(MAXIMUM_BLOCK),
        })
    }

    /// 注入 BlockManager（打破构造时循环依赖）
    pub fn set_block_manager(&self, bm: Arc<dyn BlockManager>) {
        *self.block_manager.lock() = Some(bm);
    }

    fn get_block_manager(&self) -> Arc<dyn BlockManager> {
        self.block_manager
            .lock()
            .as_ref()
            .expect("BlockManager not initialized")
            .clone()
    }

    /// 生成下一个临时块 ID（对应 C++ GetNextTemporaryId()）
    fn next_temporary_id(&self) -> BlockId {
        self.temporary_id.fetch_sub(1, Ordering::Relaxed)
    }

    /// 尝试预约内存；若超出限制则驱逐后返回新的预约
    fn reserve_memory_for_pin(&self, size: usize, tag: MemoryTag) -> BufferPoolReservation {
        let pool = &self.buffer_pool;
        let max = pool.get_max_memory();
        let current = pool.get_used_memory(false);

        let mut reusable: Option<Box<FileBuffer>> = None;

        if current + size > max {
            // 触发驱逐以腾出空间
            let result = pool.evict_blocks(tag, size, max, &mut reusable);
            if !result.success {
                panic!(
                    "Out of memory: cannot evict enough blocks \
                     (need {size} bytes, used {current}, limit {max})"
                );
            }
            // result.reservation 持有已腾出的内存预约，转移所有权
            result.reservation.into_inner()
        } else {
            // 直接构造一个新预约并 resize 到 size
            let mut r = BufferPoolReservation::new(tag, Arc::clone(pool) as Arc<dyn MemoryTracker>);
            r.resize(size);
            r
        }
    }
}

// ─── BufferManager 实现 ───────────────────────────────────────

impl BufferManager for StandardBufferManager {
    // ─── Pin ──────────────────────────────────────────────────

    /// 对应 C++ StandardBufferManager::Pin()
    fn pin(&self, handle: Arc<BlockHandle>) -> BufferHandle {
        // Phase 1：持锁快速检查
        {
            let guard: BlockGuard<'_> = handle.lock();
            if guard.state == BlockState::Loaded {
                handle.increment_readers();
                return BufferHandle::new(handle.clone());
            }
        }
        // guard 已释放

        // Phase 2：预约内存后重新持锁加载
        let size = handle.memory_usage();
        let reservation = self.reserve_memory_for_pin(size, handle.tag);

        let mut guard: BlockGuard<'_> = handle.lock();
        let bh = BlockHandle::load_guarded(handle.clone(), &mut guard, None);

        if bh.is_valid() {
            // 将预约合并进 block（对应 C++ MergeMemoryReservation）
            handle.merge_memory_reservation(&mut guard, reservation);
        }
        // 若 load 失败，reservation 在此处 drop — 自动归还

        bh
    }

    // ─── Unpin ────────────────────────────────────────────────

    /// 对应 C++ StandardBufferManager::Unpin()
    fn unpin(&self, handle: Arc<BlockHandle>) {
        let new_readers = handle.decrement_readers();
        if new_readers == 0 && handle.must_add_to_eviction_queue() {
            self.buffer_pool.add_to_eviction_queue(&handle);
        }
    }

    // ─── get_buffer_pool ──────────────────────────────────────

    fn get_buffer_pool(&self) -> Arc<BufferPool> {
        Arc::clone(&self.buffer_pool)
    }

    // ─── 临时文件接口 ─────────────────────────────────────────

    fn has_temporary_directory(&self) -> bool {
        self.temp_file_data.lock().is_some()
    }

    /// 对应 C++ StandardBufferManager::ReadTemporaryBuffer()
    fn read_temporary_buffer(
        &self,
        _tag: MemoryTag,
        block_id: BlockId,
        _reusable: Option<Box<FileBuffer>>,
    ) -> Box<FileBuffer> {
        let guard = self.temp_file_data.lock();
        let data = guard.as_ref().expect("No temporary directory configured");
        let path = data.temp_path_for(block_id);

        let mut file = self
            .fs
            .open_file(&path, FileOpenFlags::READ)
            .expect("Failed to open temporary block file");

        let size = file.file_size().expect("temp file size") as usize;
        let mut fb = Box::new(FileBuffer::new(FileBufferType::ManagedBuffer, size, 0));
        file.read_at(fb.raw_mut(), 0).expect("read temp block");
        fb
    }

    /// 对应 C++ StandardBufferManager::WriteTemporaryBuffer()
    fn write_temporary_buffer(&self, _tag: MemoryTag, block_id: BlockId, buf: &FileBuffer) {
        let guard = self.temp_file_data.lock();
        let data = guard.as_ref().expect("No temporary directory configured");
        data.ensure_created(&*self.fs).expect("create temp dir");

        let path = data.temp_path_for(block_id);
        let flags = FileOpenFlags::READ_WRITE | FileOpenFlags::CREATE;
        let mut file = self.fs.open_file(&path, flags).expect("create temp file");
        file.write_at(buf.raw(), 0).expect("write temp block");
        file.sync().expect("sync temp file");
    }

    /// 对应 C++ StandardBufferManager::DeleteTemporaryFile()
    fn delete_temporary_file(&self, block: &BlockHandle) {
        let guard = self.temp_file_data.lock();
        if let Some(data) = guard.as_ref() {
            let path = data.temp_path_for(block.block_id);
            let _ = self.fs.remove_file(&path);
        }
    }

    /// 对应 C++ StandardBufferManager::AllocateMemory()
    ///
    /// 分配一个新的内存块（不与磁盘文件绑定）并返回已加载的 BlockHandle。
    fn allocate_memory(
        &self,
        tag: MemoryTag,
        block_manager: Option<&dyn BlockManager>,
        can_destroy: bool,
    ) -> Arc<BlockHandle> {
        let bm: Arc<dyn BlockManager> = block_manager
            .map(|_bm| {
                // 调用方提供了 block_manager 引用，但 trait 接口不持有 Arc；
                // 此处回退到自身存储的 block_manager
                self.get_block_manager()
            })
            .unwrap_or_else(|| self.get_block_manager());

        let block_alloc_size = bm.get_block_alloc_size();
        let destroy_upon = if can_destroy {
            DestroyBufferUpon::Eviction
        } else {
            DestroyBufferUpon::Block
        };

        let reservation = self.reserve_memory_for_pin(block_alloc_size, tag);
        let block_id = self.next_temporary_id();
        let buffer = bm.create_block(block_id, None);

        BlockHandle::new_loaded(
            Arc::clone(&bm),
            block_id,
            tag,
            buffer,
            destroy_upon,
            block_alloc_size,
            reservation,
        )
    }
}

// ─── MemoryTracker 代理 ───────────────────────────────────────

impl MemoryTracker for StandardBufferManager {
    fn update_used_memory(&self, tag: MemoryTag, delta: i64) {
        self.buffer_pool.update_used_memory(tag, delta);
    }
}
