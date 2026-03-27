// ============================================================
// buffer_pool.rs — 全局缓冲池
// 对应 C++: duckdb/storage/buffer/buffer_pool.hpp + buffer_pool.cpp
// ============================================================
//
// 核心职责：
//   管理全局内存限制、驱逐队列、内存使用统计。
//   实现 MemoryTracker trait（供 BufferPoolReservation 回调）。
//
// 驱逐队列布局（对应 C++ eviction_queue_sizes = {1, 6, 1}）：
//   队列 0     : Block + ExternalFile  （1 个队列，驱逐优先）
//   队列 1-6   : ManagedBuffer         （6 个队列，分散写竞争）
//   队列 7     : TinyBuffer            （1 个队列，最后驱逐）
//
// MemoryTracker 实现：
//   BufferPool 实现 MemoryTracker trait，
//   BufferPoolReservation 通过 Arc<dyn MemoryTracker> 回调，
//   从而打破 BufferPoolReservation ↔ BufferPool 的直接循环引用。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::Mutex;

use super::types::{FileBufferType, MemoryTag};
use super::file_buffer::FileBuffer;
use super::memory_usage::MemoryUsage;
use super::buffer_pool_reservation::{MemoryTracker, TempBufferPoolReservation};
use super::eviction_queue::{EvictionNode, EvictionQueue};
use super::block_handle::BlockHandle;

// ─── 辅助 trait ───────────────────────────────────────────────
/// 对应 C++ BlockAllocator（用于 FlushAll）
pub trait BlockAllocator: Send + Sync {
    /// 对应 C++ BlockAllocator::FlushAll(idx_t extra_memory)
    fn flush_all(&self, extra_memory: usize);
    fn flush_all_simple(&self) {
        self.flush_all(0);
    }
}

// ─── TemporaryMemoryManager ───────────────────────────────────
/// 对应 C++ TemporaryMemoryManager（骨架，后续在 temporary_memory_manager.rs 展开）
pub struct TemporaryMemoryManager;
impl TemporaryMemoryManager {
    pub fn new() -> Self { Self }
}
impl Default for TemporaryMemoryManager {
    fn default() -> Self { Self::new() }
}

// ─── EvictionResult ───────────────────────────────────────────
/// 对应 C++ BufferPool::EvictionResult
pub struct EvictionResult {
    pub success: bool,
    pub reservation: TempBufferPoolReservation,
}

// ─── 常量 ─────────────────────────────────────────────────────
/// 驱逐类型数（Block+Ext / ManagedBuffer / TinyBuffer）
const EVICTION_QUEUE_TYPES: usize = 3;
/// 每种驱逐类型的队列数量（对应 C++ eviction_queue_sizes = {1, 6, 1}）
const QUEUE_SIZES: [usize; EVICTION_QUEUE_TYPES] = [1, 6, 1];
/// 队列总数 = 1 + 6 + 1
const TOTAL_QUEUES: usize = 8;

// ─── BufferPool ───────────────────────────────────────────────
/// 对应 C++ BufferPool
///
/// 一个 BufferPool 实例可以被多个 database 共享。
/// 必须通过 `Arc<BufferPool>` 使用（被 BufferPoolReservation 引用）。
pub struct BufferPool {
    /// 对应 C++ mutex limit_lock（保护 SetLimit 操作）
    limit_lock: Mutex<()>,
    /// 对应 C++ atomic<idx_t> maximum_memory
    pub maximum_memory: AtomicUsize,
    /// 对应 C++ atomic<idx_t> allocator_bulk_deallocation_flush_threshold
    allocator_bulk_deallocation_flush_threshold: AtomicUsize,
    /// 对应 C++ bool track_eviction_timestamps
    track_eviction_timestamps: bool,

    /// 驱逐队列数组（共 TOTAL_QUEUES 个）
    queues: Vec<EvictionQueue>,

    /// 对应 C++ TemporaryMemoryManager
    pub temporary_memory_manager: Mutex<TemporaryMemoryManager>,

    /// 对应 C++ mutable MemoryUsage memory_usage
    memory_usage: MemoryUsage,

    /// 对应 C++ BlockAllocator &block_allocator
    block_allocator: Arc<dyn BlockAllocator>,
}

impl BufferPool {
    /// 对应 C++ BufferPool(BlockAllocator&, idx_t max_mem, bool track_ts, idx_t flush_threshold)
    pub fn new(
        block_allocator: Arc<dyn BlockAllocator>,
        maximum_memory: usize,
        track_eviction_timestamps: bool,
        allocator_bulk_deallocation_flush_threshold: usize,
    ) -> Arc<Self> {
        // 构建驱逐队列（对应 C++ 构造函数中的 for 循环）
        let type_groups: [Vec<FileBufferType>; EVICTION_QUEUE_TYPES] = [
            vec![FileBufferType::Block, FileBufferType::ExternalFile],
            vec![FileBufferType::ManagedBuffer],
            vec![FileBufferType::TinyBuffer],
        ];

        let mut queues = Vec::with_capacity(TOTAL_QUEUES);
        for (group, &size) in type_groups.iter().zip(QUEUE_SIZES.iter()) {
            for _ in 0..size {
                queues.push(EvictionQueue::new(group.clone()));
            }
        }

        Arc::new(Self {
            limit_lock: Mutex::new(()),
            maximum_memory: AtomicUsize::new(maximum_memory),
            allocator_bulk_deallocation_flush_threshold: AtomicUsize::new(
                allocator_bulk_deallocation_flush_threshold,
            ),
            track_eviction_timestamps,
            queues,
            temporary_memory_manager: Mutex::new(TemporaryMemoryManager::new()),
            memory_usage: MemoryUsage::new(),
            block_allocator,
        })
    }

    // ─── 内存统计 ──────────────────────────────────────────────

    /// 对应 C++ GetUsedMemory(bool flush)
    pub fn get_used_memory(&self, flush: bool) -> usize {
        if flush {
            self.memory_usage.get_used_flush()
        } else {
            self.memory_usage.get_used_no_flush()
        }
    }

    /// 对应 C++ GetMaxMemory()
    pub fn get_max_memory(&self) -> usize {
        self.maximum_memory.load(Ordering::Relaxed)
    }

    /// 对应 C++ GetQueryMaxMemory()（默认等于 GetMaxMemory，子类可覆盖）
    pub fn get_query_max_memory(&self) -> usize {
        self.get_max_memory()
    }

    // ─── 限制管理 ─────────────────────────────────────────────

    /// 对应 C++ SetLimit(idx_t limit, const char *exception_postscript)
    pub fn set_limit(
        self: &Arc<Self>,
        limit: usize,
    ) -> Result<(), String> {
        let _guard = self.limit_lock.lock();

        // 第一次驱逐：尝试腾出空间
        let mut dummy: Option<Box<FileBuffer>> = None;
        if !self.evict_blocks(MemoryTag::Extension, 0, limit, &mut dummy).success {
            return Err(format!(
                "Failed to change memory limit to {limit}: \
                 could not free up enough memory for the new limit"
            ));
        }

        let old_limit = self.maximum_memory.swap(limit, Ordering::Relaxed);

        // 第二次驱逐：确认新限制有效
        if !self.evict_blocks(MemoryTag::Extension, 0, limit, &mut dummy).success {
            self.maximum_memory.store(old_limit, Ordering::Relaxed);
            return Err(format!(
                "Failed to change memory limit to {limit}: \
                 could not free up enough memory for the new limit"
            ));
        }

        self.block_allocator.flush_all_simple();
        Ok(())
    }

    /// 对应 C++ SetAllocatorBulkDeallocationFlushThreshold()
    pub fn set_allocator_flush_threshold(&self, threshold: usize) {
        self.allocator_bulk_deallocation_flush_threshold
            .store(threshold, Ordering::Relaxed);
    }

    pub fn get_allocator_flush_threshold(&self) -> usize {
        self.allocator_bulk_deallocation_flush_threshold
            .load(Ordering::Relaxed)
    }

    // ─── 驱逐队列操作 ─────────────────────────────────────────

    /// 对应 C++ AddToEvictionQueue(shared_ptr<BlockHandle> &handle)
    ///
    /// 返回 true 表示队列已满，应触发 purge。
    pub fn add_to_eviction_queue(&self, handle: &Arc<BlockHandle>) -> bool {
        let queue = self.get_queue_for_handle(handle.as_ref());

        debug_assert_eq!(handle.readers(), 0);

        let seq = handle.next_eviction_seq_num();

        // 记录 LRU 时间戳（用于 PurgeAgedBlocks）
        if self.track_eviction_timestamps {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            handle.set_lru_timestamp(ts);
        }

        // 旧版本的节点变为 dead node
        if seq != 1 {
            queue.increment_dead_nodes();
        }

        queue.add(EvictionNode::new(Arc::downgrade(handle), seq))
    }

    /// 对应 C++ PurgeQueue(const BlockHandle &handle)
    pub fn purge_queue(&self, handle: &BlockHandle) {
        self.get_queue_for_handle(handle).purge();
    }

    /// 对应 C++ IncrementDeadNodes(const BlockHandle &handle)
    pub fn increment_dead_nodes(&self, handle: &BlockHandle) {
        self.get_queue_for_handle(handle).increment_dead_nodes();
    }

    // ─── 驱逐 ─────────────────────────────────────────────────

    /// 对应 C++ EvictBlocks(MemoryTag, idx_t extra, idx_t limit, unique_ptr<FileBuffer>*)
    ///
    /// 按队列优先级顺序尝试驱逐，直到内存满足限制。
    pub fn evict_blocks(
        self: &Arc<Self>,
        tag: MemoryTag,
        extra_memory: usize,
        memory_limit: usize,
        reusable_buffer: &mut Option<Box<FileBuffer>>,
    ) -> EvictionResult {
        for queue in &self.queues {
            let result = self.evict_blocks_internal(
                queue, tag, extra_memory, memory_limit, reusable_buffer,
            );
            // 驱逐成功，或已到达最后一个队列 → 返回
            if result.success {
                return result;
            }
        }
        // 所有队列都失败，返回最后一次的失败结果
        self.evict_blocks_internal(
            self.queues.last().unwrap(),
            tag, extra_memory, memory_limit, reusable_buffer,
        )
    }

    /// 对应 C++ EvictBlocksInternal(EvictionQueue&, ...)
    fn evict_blocks_internal(
        self: &Arc<Self>,
        queue: &EvictionQueue,
        tag: MemoryTag,
        extra_memory: usize,
        memory_limit: usize,
        reusable_buffer: &mut Option<Box<FileBuffer>>,
    ) -> EvictionResult {
        // 预先预约 extra_memory（对应 C++ TempBufferPoolReservation r(tag, *this, extra_memory)）
        let reservation = TempBufferPoolReservation::new(
            tag,
            Arc::clone(self) as Arc<dyn MemoryTracker>,
            extra_memory,
        );

        // 如果预约后内存已满足要求，直接返回成功
        if self.memory_usage.get_used_no_flush() <= memory_limit {
            let flush_threshold = self
                .allocator_bulk_deallocation_flush_threshold
                .load(Ordering::Relaxed);
            if extra_memory > flush_threshold {
                self.block_allocator.flush_all(extra_memory);
            }
            return EvictionResult { success: true, reservation };
        }

        let flush_threshold = self
            .allocator_bulk_deallocation_flush_threshold
            .load(Ordering::Relaxed);
        let pool = Arc::clone(self);
        let mut found = false;

        queue.iterate_unloadable(|handle| {
            // 持锁二次确认（对应 C++ lock 后再次 CanUnload 检查）
            let mut guard = handle.lock();

            if !handle.can_unload_guarded(&guard) {
                // 节点已过时（被 pin 或状态改变）
                return true; // 继续遍历
            }

            // 尝试复用 buffer（避免内存分配，对应 C++ buffer reuse 逻辑）
            if reusable_buffer.is_none() {
                if let Some(buf) = guard.buffer.as_ref() {
                    if buf.alloc_size() == extra_memory {
                        *reusable_buffer = handle.unload_and_take(&mut guard);
                        found = true;
                        return false; // 停止遍历
                    }
                }
            }

            // 普通卸载
            handle.unload(&mut guard);

            if pool.memory_usage.get_used_no_flush() <= memory_limit {
                found = true;
                return false; // 停止遍历
            }

            true // 继续遍历
        });

        if !found {
            // 驱逐失败：归还预约（通过 into_inner 然后 drop）
            // TempBufferPoolReservation 的 Drop 会自动 resize(0)
            return EvictionResult {
                success: false,
                reservation: TempBufferPoolReservation::new(
                    tag,
                    Arc::clone(self) as Arc<dyn MemoryTracker>,
                    0,
                ),
            };
        }

        if extra_memory > flush_threshold {
            self.block_allocator.flush_all(extra_memory);
        }

        EvictionResult { success: true, reservation }
    }

    // ─── 按时间清理（对应 C++ PurgeAgedBlocks）────────────────

    /// 对应 C++ PurgeAgedBlocks(uint32_t max_age_sec)
    ///
    /// 清理超过 max_age_sec 秒未使用的 block，返回释放的字节数。
    pub fn purge_aged_blocks(&self, max_age_sec: u32) -> usize {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let limit_ms = now_ms - (max_age_sec as i64 * 1000);

        let mut purged = 0usize;
        for queue in &self.queues {
            purged += self.purge_aged_blocks_internal(queue, now_ms, limit_ms);
        }
        purged
    }

    fn purge_aged_blocks_internal(
        &self,
        queue: &EvictionQueue,
        now_ms: i64,
        limit_ms: i64,
    ) -> usize {
        let mut purged = 0usize;

        queue.iterate_unloadable(|handle| {
            let mut guard = handle.lock();

            if !handle.can_unload_guarded(&guard) {
                return true; // 继续
            }

            let ts = handle.lru_timestamp();
            let is_fresh = ts >= limit_ms && ts <= now_ms;

            purged += handle.memory_usage();
            handle.unload(&mut guard);

            !is_fresh // is_fresh 则停止（对应 C++ return !is_fresh）
        });

        purged
    }

    // ─── 内部辅助 ─────────────────────────────────────────────

    /// 对应 C++ GetEvictionQueueForBlockHandle()
    fn get_queue_for_handle<'a>(&'a self, handle: &BlockHandle) -> &'a EvictionQueue {
        let type_idx = file_buffer_type_to_queue_type(handle.buffer_type);

        // 计算该类型的队列在 queues 数组中的起始偏移
        let mut queue_offset = 0usize;
        for i in 0..type_idx {
            queue_offset += QUEUE_SIZES[i];
        }

        let queue_size = QUEUE_SIZES[type_idx];
        let eq_idx = handle.eviction_queue_idx_raw();

        // INVALID_INDEX 或超出范围 → 加到最高优先级（队列尾部）
        // 有效 eviction_queue_idx → 加到对应位置
        // （对应 C++ idx < queue_size → queue_size - idx - 1 的倒序映射）
        if eq_idx < queue_size {
            queue_offset += queue_size - eq_idx - 1;
        }
        // else: eq_idx >= queue_size，使用 queue_offset（即第一个队列）

        debug_assert!(self.queues[queue_offset].has_type(handle.buffer_type));
        &self.queues[queue_offset]
    }
}

/// FileBufferType → 驱逐队列类型下标（0/1/2）
fn file_buffer_type_to_queue_type(t: FileBufferType) -> usize {
    match t {
        FileBufferType::Block | FileBufferType::ExternalFile => 0,
        FileBufferType::ManagedBuffer                        => 1,
        FileBufferType::TinyBuffer                           => 2,
    }
}

// ─── MemoryTracker 实现 ───────────────────────────────────────
/// BufferPool 实现 MemoryTracker，使 BufferPoolReservation 可以
/// 通过 Arc<dyn MemoryTracker> 回调，而无需直接持有 Arc<BufferPool>。
impl MemoryTracker for BufferPool {
    fn update_used_memory(&self, tag: MemoryTag, delta: i64) {
        self.memory_usage.update(tag, delta);
    }
}
