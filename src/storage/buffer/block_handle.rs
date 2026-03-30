// ============================================================
// block_handle.rs — 块句柄（内存管理核心）
// 对应 C++: duckdb/storage/buffer/block_handle.hpp + block_handle.cpp
// ============================================================
//
// 核心职责：
//   维护一个内存块的状态机（Loaded ↔ Unloaded），
//   管理 reader 计数、驱逐序列号、内存预约，
//   以及与 BlockManager 的交互（加载/卸载/写临时文件）。
//
// C++ → Rust 主要映射：
//   shared_ptr<BlockHandle>      → Arc<BlockHandle>
//   weak_ptr<BlockHandle>        → Weak<BlockHandle>
//   mutex lock + atomic fields   → Mutex<BlockInner> + Atomic 字段
//   enable_shared_from_this      → 由调用方持有 Arc<BlockHandle>；
//                                  需要 Arc<Self> 时通过 OnceLock<Weak<Self>>
//   BlockLock (unique_lock)      → BlockGuard<'_>（parking_lot::MutexGuard）
//
// 无 unsafe 方案：
//   C++ BufferHandle::Ptr() 返回裸指针；
//   Rust 改为 BufferHandle::with_data(closure) 回调，
//   在锁保护下提供数据访问。

use parking_lot::{Mutex, MutexGuard};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use super::block_manager::BlockManager;
use super::buffer_handle::BufferHandle;
use super::buffer_pool_reservation::BufferPoolReservation;
use super::file_buffer::FileBuffer;
use super::types::{
    BlockId, BlockState, DestroyBufferUpon, FileBufferType, INVALID_INDEX, MAXIMUM_BLOCK, MemoryTag,
};

// ─── BlockGuard ───────────────────────────────────────────────
/// 对应 C++ BlockLock (unique_lock<mutex>)
///
/// BlockHandle 的块级锁 guard，保护 BlockInner 的可变状态。
/// 方法签名中持有 `&mut BlockGuard<'_>` 表示"调用者已持锁"的编译期证明。
pub type BlockGuard<'a> = MutexGuard<'a, BlockInner>;

// ─── BlockInner ───────────────────────────────────────────────
/// 受块级 Mutex 保护的可变状态
pub struct BlockInner {
    /// 对应 C++ atomic<BlockState> state（实际被锁保护）
    pub state: BlockState,
    /// 对应 C++ unique_ptr<FileBuffer> buffer
    pub buffer: Option<Box<FileBuffer>>,
    /// 对应 C++ BufferPoolReservation memory_charge
    pub memory_charge: BufferPoolReservation,
}

// ─── BlockHandle ──────────────────────────────────────────────
/// 对应 C++ BlockHandle
///
/// 必须通过 `Arc<BlockHandle>` 使用（对应 C++ shared_ptr<BlockHandle>）。
/// 通过 `BlockHandle::new_unloaded()` 或 `new_loaded()` 创建，
/// 返回 `Arc<Self>`，同时初始化内部弱引用。
pub struct BlockHandle {
    // ── 外部依赖（不持有循环引用所有权）──────────────────────
    /// 对应 C++ BlockManager &block_manager
    /// 注意：BlockManager 持有 Weak<BlockHandle>，此处持有 Arc<dyn BlockManager>；
    /// 不会产生引用环（Arc 计数不会相互阻止 drop）。
    pub block_manager: Arc<dyn BlockManager>,

    // ── 受锁保护的状态 ────────────────────────────────────────
    /// 块级锁，保护 BlockInner（state + buffer + memory_charge）
    inner: Mutex<BlockInner>,

    // ── 无需持锁的原子字段 ───────────────────────────────────
    /// 对应 C++ const block_id_t block_id
    pub block_id: BlockId,
    /// 对应 C++ const MemoryTag tag
    pub tag: MemoryTag,
    /// 对应 C++ const FileBufferType buffer_type
    pub buffer_type: FileBufferType,

    /// 并发 reader 计数（对应 C++ atomic<int32_t> readers）
    readers: AtomicI32,
    /// 驱逐序列号（对应 C++ atomic<idx_t> eviction_seq_num）
    eviction_seq_num: AtomicUsize,
    /// LRU 时间戳 ms（对应 C++ atomic<int64_t> lru_timestamp_msec）
    lru_timestamp_msec: AtomicI64,
    /// 何时销毁 buffer（对应 C++ atomic<DestroyBufferUpon>，以 u8 存储）
    destroy_buffer_upon: AtomicU8,
    /// 内存占用量（对应 C++ atomic<idx_t> memory_usage）
    memory_usage: AtomicUsize,
    /// 驱逐队列 slot 索引（对应 C++ atomic<idx_t> eviction_queue_idx）
    eviction_queue_idx: AtomicUsize,

    // ── shared_from_this 等价 ────────────────────────────────
    /// 对应 C++ enable_shared_from_this<BlockHandle>
    /// 构造后通过 `Arc::downgrade(&arc)` 初始化，供内部方法获取 Arc<Self>。
    weak_self: OnceLock<Weak<BlockHandle>>,
}

impl BlockHandle {
    // ─── 构造 ─────────────────────────────────────────────────

    /// 对应 C++ BlockHandle(BlockManager&, block_id_t, MemoryTag)
    /// 创建未加载状态的持久化 block handle。
    pub fn new_unloaded(
        block_manager: Arc<dyn BlockManager>,
        block_id: BlockId,
        tag: MemoryTag,
        memory_usage: usize,
        reservation: BufferPoolReservation,
    ) -> Arc<Self> {
        let handle = Arc::new(Self {
            inner: Mutex::new(BlockInner {
                state: BlockState::Unloaded,
                buffer: None,
                memory_charge: reservation,
            }),
            block_manager,
            block_id,
            tag,
            buffer_type: FileBufferType::Block,
            readers: AtomicI32::new(0),
            eviction_seq_num: AtomicUsize::new(0),
            lru_timestamp_msec: AtomicI64::new(0),
            destroy_buffer_upon: AtomicU8::new(DestroyBufferUpon::Block as u8),
            memory_usage: AtomicUsize::new(memory_usage),
            eviction_queue_idx: AtomicUsize::new(INVALID_INDEX),
            weak_self: OnceLock::new(),
        });
        // 初始化 weak_self（对应 enable_shared_from_this）
        let _ = handle.weak_self.set(Arc::downgrade(&handle));
        handle
    }

    /// 对应 C++ BlockHandle(BlockManager&, block_id_t, MemoryTag,
    ///                      unique_ptr<FileBuffer>, DestroyBufferUpon, idx_t, BufferPoolReservation&&)
    /// 创建已加载状态的 block handle（含 buffer 和预约）。
    pub fn new_loaded(
        block_manager: Arc<dyn BlockManager>,
        block_id: BlockId,
        tag: MemoryTag,
        buffer: Box<FileBuffer>,
        destroy_upon: DestroyBufferUpon,
        block_size: usize,
        reservation: BufferPoolReservation,
    ) -> Arc<Self> {
        let buffer_type = buffer.buffer_type();
        let handle = Arc::new(Self {
            inner: Mutex::new(BlockInner {
                state: BlockState::Loaded,
                buffer: Some(buffer),
                memory_charge: reservation,
            }),
            block_manager,
            block_id,
            tag,
            buffer_type,
            readers: AtomicI32::new(0),
            eviction_seq_num: AtomicUsize::new(0),
            lru_timestamp_msec: AtomicI64::new(0),
            destroy_buffer_upon: AtomicU8::new(destroy_upon as u8),
            memory_usage: AtomicUsize::new(block_size),
            eviction_queue_idx: AtomicUsize::new(INVALID_INDEX),
            weak_self: OnceLock::new(),
        });
        let _ = handle.weak_self.set(Arc::downgrade(&handle));
        handle
    }

    // ─── shared_from_this 等价 ───────────────────────────────

    /// 对应 C++ shared_from_this()
    /// 返回指向自身的 Arc<BlockHandle>。
    pub fn arc_self(&self) -> Arc<Self> {
        self.weak_self
            .get()
            .expect("BlockHandle::arc_self() called before weak_self initialization")
            .upgrade()
            .expect("BlockHandle::arc_self() called while being dropped")
    }

    // ─── 获取块级锁 ──────────────────────────────────────────

    /// 对应 C++ BlockHandle::GetLock()
    /// 返回 BlockGuard（parking_lot::MutexGuard<BlockInner>）。
    pub fn lock(&self) -> BlockGuard<'_> {
        self.inner.lock()
    }

    // ─── 无需持锁的原子字段访问 ──────────────────────────────

    pub fn eviction_seq_num(&self) -> usize {
        self.eviction_seq_num.load(Ordering::Relaxed)
    }

    /// 对应 C++ NextEvictionSequenceNumber()：递增并返回新序列号
    pub fn next_eviction_seq_num(&self) -> usize {
        self.eviction_seq_num.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn readers(&self) -> i32 {
        self.readers.load(Ordering::Relaxed)
    }

    pub fn increment_readers(&self) -> i32 {
        self.readers.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn decrement_readers(&self) -> i32 {
        self.readers.fetch_sub(1, Ordering::Relaxed) - 1
    }

    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }

    pub fn lru_timestamp(&self) -> i64 {
        self.lru_timestamp_msec.load(Ordering::Relaxed)
    }

    pub fn set_lru_timestamp(&self, ts_msec: i64) {
        self.lru_timestamp_msec.store(ts_msec, Ordering::Relaxed);
    }

    pub fn destroy_buffer_upon(&self) -> DestroyBufferUpon {
        match self.destroy_buffer_upon.load(Ordering::Relaxed) {
            0 => DestroyBufferUpon::Block,
            1 => DestroyBufferUpon::Eviction,
            _ => DestroyBufferUpon::Unpin,
        }
    }

    pub fn set_destroy_buffer_upon(&self, v: DestroyBufferUpon) {
        self.destroy_buffer_upon.store(v as u8, Ordering::Relaxed);
    }

    /// 对应 C++ MustAddToEvictionQueue()
    pub fn must_add_to_eviction_queue(&self) -> bool {
        self.destroy_buffer_upon() != DestroyBufferUpon::Unpin
    }

    /// 对应 C++ MustWriteToTemporaryFile()
    pub fn must_write_to_temp_file(&self) -> bool {
        self.destroy_buffer_upon() == DestroyBufferUpon::Block
    }

    /// 对应 C++ GetEvictionQueueIndex()
    pub fn eviction_queue_idx_raw(&self) -> usize {
        self.eviction_queue_idx.load(Ordering::Relaxed)
    }

    /// 对应 C++ SetEvictionQueueIndex()（只能设置一次）
    pub fn set_eviction_queue_idx(&self, idx: usize) {
        debug_assert_eq!(
            self.eviction_queue_idx.load(Ordering::Relaxed),
            INVALID_INDEX,
            "eviction_queue_idx can only be set once"
        );
        self.eviction_queue_idx.store(idx, Ordering::Relaxed);
    }

    // ─── 快速状态判断（不持锁，可能过时）─────────────────────

    /// 对应 C++ CanUnload()（不持锁版本）
    ///
    /// 注意：C++ 注释明确说明"不持锁时结果可能随时改变"。
    /// 持锁后应调用 `can_unload_guarded(guard)`。
    pub fn can_unload_no_lock(&self) -> bool {
        let inner = self.inner.lock();
        self.can_unload_guarded(&inner)
    }

    /// 对应 C++ CanUnload()（持锁版本）
    pub fn can_unload_guarded(&self, guard: &BlockGuard<'_>) -> bool {
        if guard.state == BlockState::Unloaded {
            return false;
        }
        if self.readers() > 0 {
            return false;
        }
        // 临时块且需要写临时文件，但没有临时目录 → 不能卸载
        if self.block_id >= MAXIMUM_BLOCK
            && self.must_write_to_temp_file()
            && !self.block_manager.has_temporary_directory()
        {
            return false;
        }
        true
    }

    pub fn is_unloaded(&self) -> bool {
        self.inner.lock().state == BlockState::Unloaded
    }

    // ─── 持锁操作 ─────────────────────────────────────────────

    /// 对应 C++ BlockHandle::Load()（静态方法，调用方传入 Arc<Self>）
    ///
    /// 如果已加载，直接递增 reader 计数返回 BufferHandle。
    /// 否则从磁盘或临时文件加载数据到内存。
    pub fn load(handle: Arc<Self>, reusable: Option<Box<FileBuffer>>) -> BufferHandle {
        let mut guard = handle.inner.lock();

        if guard.state == BlockState::Loaded {
            // 已加载：递增 reader 计数，直接返回
            handle.readers.fetch_add(1, Ordering::Relaxed);
            return BufferHandle::new(handle.clone());
        }

        // 需要从磁盘/临时文件加载
        let buffer = if handle.block_id < MAXIMUM_BLOCK {
            // 持久化块：通过 BlockManager 读取
            let mut block = handle.block_manager.create_block(handle.block_id, reusable);
            handle.block_manager.read_block(&mut block, handle.block_id);
            block
        } else if handle.must_write_to_temp_file() {
            // 临时块（已溢出到磁盘）：从临时文件读取
            handle
                .block_manager
                .read_temporary_buffer(handle.tag, handle.block_id, reusable)
        } else {
            // DestroyBufferUpon::Unpin / Eviction：驱逐时已销毁，无法恢复
            return BufferHandle::invalid();
        };

        guard.buffer = Some(buffer);
        guard.state = BlockState::Loaded;
        handle.readers.store(1, Ordering::Relaxed);

        BufferHandle::new(handle.clone())
    }

    /// 对应 C++ BlockHandle::Load()——调用方已持锁版本
    ///
    /// StandardBufferManager::Pin() 的两阶段模式：
    ///   1. 调用方持锁检查状态 → 未加载
    ///   2. 释放锁，驱逐 + 预约内存
    ///   3. 重新持锁，再次检查（可能已被其他线程加载）
    ///   4. 调用本方法（锁已在手）执行实际加载
    pub fn load_guarded(
        handle: Arc<Self>,
        guard: &mut BlockGuard<'_>,
        reusable: Option<Box<FileBuffer>>,
    ) -> BufferHandle {
        if guard.state == BlockState::Loaded {
            // 另一线程已完成加载，直接递增 reader
            handle.readers.fetch_add(1, Ordering::Relaxed);
            return BufferHandle::new(handle.clone());
        }

        let buffer = if handle.block_id < MAXIMUM_BLOCK {
            let mut block = handle.block_manager.create_block(handle.block_id, reusable);
            handle.block_manager.read_block(&mut block, handle.block_id);
            block
        } else if handle.must_write_to_temp_file() {
            handle
                .block_manager
                .read_temporary_buffer(handle.tag, handle.block_id, reusable)
        } else {
            return BufferHandle::invalid();
        };

        guard.buffer = Some(buffer);
        guard.state = BlockState::Loaded;
        handle.readers.store(1, Ordering::Relaxed);

        BufferHandle::new(handle.clone())
    }

    /// 对应 C++ LoadFromBuffer()（静态方法）
    ///
    /// 从提供的内存数据（data 切片）加载到 buffer，
    /// 用于 WAL replay 等场景。
    pub fn load_from_buffer(
        handle: Arc<Self>,
        guard: &mut BlockGuard<'_>,
        data: &[u8],
        reusable: Option<Box<FileBuffer>>,
        reservation: BufferPoolReservation,
    ) -> BufferHandle {
        debug_assert_ne!(guard.state, BlockState::Loaded);
        debug_assert_eq!(handle.readers(), 0);

        let mut block = handle.block_manager.create_block(handle.block_id, reusable);
        block.copy_from(data);

        guard.buffer = Some(block);
        guard.state = BlockState::Loaded;
        handle.readers.store(1, Ordering::Relaxed);
        guard.memory_charge = reservation;

        BufferHandle::new(handle.clone())
    }

    /// 对应 C++ UnloadAndTakeBlock()
    ///
    /// 卸载 block 并取走 FileBuffer 所有权（供 EvictBlocks 重用）。
    pub fn unload_and_take(&self, guard: &mut BlockGuard<'_>) -> Option<Box<FileBuffer>> {
        if guard.state == BlockState::Unloaded {
            return None;
        }
        debug_assert!(self.can_unload_guarded(guard));

        // 临时块需要先写临时文件
        if self.block_id >= MAXIMUM_BLOCK && self.must_write_to_temp_file() {
            if let Some(buf) = &guard.buffer {
                self.block_manager
                    .write_temporary_buffer(self.tag, self.block_id, buf);
            }
        }

        guard.memory_charge.resize(0);
        guard.state = BlockState::Unloaded;
        guard.buffer.take()
    }

    /// 对应 C++ Unload()
    pub fn unload(&self, guard: &mut BlockGuard<'_>) {
        let _ = self.unload_and_take(guard); // 取走后自动 drop
    }

    /// 对应 C++ ChangeMemoryUsage()
    pub fn change_memory_usage(&self, guard: &mut BlockGuard<'_>, delta: i64) {
        debug_assert!(delta < 0, "change_memory_usage only supports shrinking");
        let new_usage = (self.memory_usage.load(Ordering::Relaxed) as i64 + delta).max(0) as usize;
        self.memory_usage.store(new_usage, Ordering::Relaxed);
        guard.memory_charge.resize(new_usage);
    }

    /// 对应 C++ ResizeMemory()
    pub fn resize_memory(&self, guard: &mut BlockGuard<'_>, alloc_size: usize) {
        guard.memory_charge.resize(alloc_size);
    }

    /// 对应 C++ ResizeBuffer()
    pub fn resize_buffer(
        &self,
        guard: &mut BlockGuard<'_>,
        new_payload_size: usize,
        memory_delta: i64,
    ) {
        if let Some(buf) = guard.buffer.as_mut() {
            buf.resize(new_payload_size);
        }
        let new_usage =
            (self.memory_usage.load(Ordering::Relaxed) as i64 + memory_delta).max(0) as usize;
        self.memory_usage.store(new_usage, Ordering::Relaxed);
    }

    /// 对应 C++ MergeMemoryReservation()
    pub fn merge_memory_reservation(
        &self,
        guard: &mut BlockGuard<'_>,
        reservation: BufferPoolReservation,
    ) {
        guard.memory_charge.merge(reservation);
    }

    /// 对应 C++ ConvertToPersistent()
    ///
    /// 将临时块转换为持久化块：
    ///   - 把 self 的 buffer + memory_charge 转移到 new_block
    ///   - self 变为 Unloaded 状态
    pub fn convert_to_persistent(
        &self,
        guard: &mut BlockGuard<'_>,
        new_block: &BlockHandle,
        new_buffer: Box<FileBuffer>,
    ) {
        let mem_size = self.memory_usage.load(Ordering::Relaxed);
        let mut new_inner = new_block.inner.lock();

        // 如果 tag 不同，先归零再以新 tag 重新分配（对应 C++ tag 切换逻辑）
        if self.tag != new_block.tag {
            let old_size = guard.memory_charge.size;
            guard.memory_charge.resize(0);
            // new_inner.memory_charge 已是 new_block 的 tag
            new_inner.memory_charge.resize(old_size);
        } else {
            // 直接转移 size（不触发 tracker，因为总量不变）
            // 等价于：new_inner.memory_charge.size += guard.memory_charge.size; guard... = 0;
            let old_size = guard.memory_charge.size;
            guard.memory_charge.size = 0; // 阻止 Drop 归还
            new_inner.memory_charge.size += old_size;
        }

        new_inner.state = BlockState::Loaded;
        new_inner.buffer = Some(new_buffer);
        new_block.memory_usage.store(mem_size, Ordering::Relaxed);

        // 清空自身
        guard.buffer = None;
        guard.state = BlockState::Unloaded;
        self.memory_usage.store(0, Ordering::Relaxed);
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        // 对应 C++ ~BlockHandle()

        // 先从 inner 读取需要的状态（在 &mut borrow 作用域内完成）
        let (buffer_is_some, is_loaded) = {
            let inner = self.inner.get_mut(); // no lock needed in Drop (exclusive access)
            (inner.buffer.is_some(), inner.state == BlockState::Loaded)
        };
        // 此时 &mut borrow 已释放，可以传 &self

        // 若 buffer 还在，通知 BufferPool 有一个 dead node
        if buffer_is_some && self.buffer_type != FileBufferType::TinyBuffer {
            if let Some(pool) = self.block_manager.try_get_buffer_pool() {
                pool.increment_dead_nodes(self);
            }
        }

        // 若 buffer 仍然 loaded，释放内存（对应 C++ buffer.reset() + memory_charge.Resize(0)）
        {
            let inner = self.inner.get_mut();
            if buffer_is_some && is_loaded {
                debug_assert!(inner.memory_charge.size > 0);
                inner.buffer = None;
                inner.memory_charge.resize(0);
            } else {
                debug_assert_eq!(inner.memory_charge.size, 0);
            }
        }

        // 注销 block（对应 C++ block_manager.UnregisterBlock(*this)）
        self.block_manager.unregister_block_by_handle(self);
    }
}
