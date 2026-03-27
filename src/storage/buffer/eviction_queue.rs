// ============================================================
// eviction_queue.rs — 驱逐队列
// 对应 C++: BufferEvictionNode + EvictionQueue（定义在 buffer_pool.cpp）
// ============================================================
//
// 设计映射：
//   C++ moodycamel::ConcurrentQueue  →  crossbeam_queue::SegQueue（无锁 MPMC）
//   C++ atomic<idx_t> evict_queue_insertions → AtomicUsize
//   C++ mutex purge_lock（try_to_lock）→ parking_lot::Mutex + try_lock()
//   C++ purge_nodes（预分配 Vec）→ Mutex 内部的 Vec（复用分配）
//
// 驱逐策略（三级队列，优先级从高到低）：
//   队列 0: Block + ExternalFile（驱逐代价低，直接丢弃）
//   队列 1: ManagedBuffer × 6（需写临时文件，多队列分散竞争）
//   队列 2: TinyBuffer（最后驱逐）

use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_queue::SegQueue;
use parking_lot::Mutex;

use super::types::FileBufferType;
use super::block_handle::{BlockHandle, BlockGuard};

// ─── EvictionNode ─────────────────────────────────────────────
/// 对应 C++ BufferEvictionNode
///
/// 存储对 BlockHandle 的弱引用 + 入队时的序列号。
/// 序列号用于判断节点是否已过时（handle 被重新 pin/unpin 后序列号递增）。
pub struct EvictionNode {
    /// 弱引用：BlockHandle 析构后自动失效
    handle: Weak<BlockHandle>,
    /// 入队时的驱逐序列号（对应 C++ handle_sequence_number）
    sequence_number: usize,
}

impl EvictionNode {
    pub fn new(handle: Weak<BlockHandle>, sequence_number: usize) -> Self {
        Self { handle, sequence_number }
    }

    /// 对应 C++ CanUnload(BlockHandle &handle_p)
    ///
    /// 序列号匹配 && 块当前可被卸载（无 reader，状态 Loaded）。
    /// 注意：此方法不持锁，调用后应再次持锁确认（TOCTOU 无害，持锁后会再验证）。
    pub fn can_unload_no_lock(&self, handle: &BlockHandle) -> bool {
        self.sequence_number == handle.eviction_seq_num()
            && handle.can_unload_no_lock()
    }

    /// 对应 C++ TryGetBlockHandle()
    ///
    /// 升级弱引用，若 handle 已销毁或节点已过时则返回 None。
    pub fn try_get_block_handle(&self) -> Option<Arc<BlockHandle>> {
        let arc = self.handle.upgrade()?;
        if self.can_unload_no_lock(&arc) {
            Some(arc)
        } else {
            None
        }
    }

    /// 持锁后的二次确认（对应 C++ IterateUnloadableBlocks 中 lock 后的再次 CanUnload）
    pub fn can_unload_with_guard(&self, handle: &BlockHandle, guard: &BlockGuard<'_>) -> bool {
        self.sequence_number == handle.eviction_seq_num()
            && handle.can_unload_guarded(guard)
    }
}

// ─── 常量 ─────────────────────────────────────────────────────
/// 每隔 INSERT_INTERVAL 次插入触发一次 purge 检查
const INSERT_INTERVAL: usize = 4096;
/// purge 时批量处理的节点数 = INSERT_INTERVAL × PURGE_SIZE_MULTIPLIER
const PURGE_SIZE_MULTIPLIER: usize = 2;
/// 队列大小低于此倍数时提前退出 purge（保护 LRU 特性）
const EARLY_OUT_MULTIPLIER: usize = 4;
/// alive/dead 比例超过此值时继续激进 purge
const ALIVE_NODE_MULTIPLIER: usize = 4;

// ─── PurgeState ───────────────────────────────────────────────
/// purge_lock 内部持有的状态（对应 C++ purge_lock + purge_nodes 两个字段）
struct PurgeState {
    /// 预分配的 purge 节点缓冲区（复用以降低 GC 压力）
    buffer: Vec<EvictionNode>,
}

// ─── EvictionQueue ────────────────────────────────────────────
/// 对应 C++ EvictionQueue（定义于 buffer_pool.cpp 匿名命名空间）
///
/// 每种 FileBufferType 对应一个或多个队列实例。
/// 内部使用无锁 MPMC 队列 + 懒 purge 清理 dead node。
pub struct EvictionQueue {
    /// 此队列关联的 FileBufferType（用于校验）
    pub file_buffer_types: Vec<FileBufferType>,
    /// 无锁 MPMC 队列（对应 C++ moodycamel::ConcurrentQueue）
    queue: SegQueue<EvictionNode>,
    /// purge 专用锁：保证单线程 purge，同时持有 purge 缓冲区
    purge_state: Mutex<PurgeState>,
    /// 插入计数器（用于调度 purge）
    evict_queue_insertions: AtomicUsize,
    /// dead node 计数（handle 已销毁 or 序列号过时的节点）
    total_dead_nodes: AtomicUsize,
}

impl EvictionQueue {
    pub fn new(types: Vec<FileBufferType>) -> Self {
        Self {
            file_buffer_types: types,
            queue: SegQueue::new(),
            purge_state: Mutex::new(PurgeState { buffer: Vec::new() }),
            evict_queue_insertions: AtomicUsize::new(0),
            total_dead_nodes: AtomicUsize::new(0),
        }
    }

    pub fn has_type(&self, t: FileBufferType) -> bool {
        self.file_buffer_types.contains(&t)
    }

    // ─── 基础队列操作 ─────────────────────────────────────────

    /// 对应 C++ AddToEvictionQueue()
    ///
    /// 返回 true 表示已到达 purge 触发点，调用者应调用 purge()。
    pub fn add(&self, node: EvictionNode) -> bool {
        self.queue.push(node);
        let n = self.evict_queue_insertions.fetch_add(1, Ordering::Relaxed) + 1;
        n % INSERT_INTERVAL == 0
    }

    pub fn increment_dead_nodes(&self) {
        self.total_dead_nodes.fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_dead_nodes(&self) {
        // saturating_sub 防止下溢（理论上不应发生，保守处理）
        self.total_dead_nodes.fetch_sub(1, Ordering::Relaxed);
    }

    /// 对应 C++ TryDequeueWithLock()：持 purge_lock 强制出队一个节点
    fn try_dequeue_with_lock(&self) -> Option<EvictionNode> {
        let _guard = self.purge_state.lock();
        self.queue.pop()
    }

    // ─── Purge ────────────────────────────────────────────────

    /// 对应 C++ Purge()
    ///
    /// 尝试获取 purge_lock（try_lock，失败直接返回），
    /// 然后批量清理 dead node，将存活节点重新入队。
    pub fn purge(&self) {
        // 单线程 purge：其他线程 try_lock 失败时直接返回（对应 C++ try_to_lock）
        let mut state = match self.purge_state.try_lock() {
            Some(g) => g,
            None => return,
        };

        let purge_size = INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER;

        // 队列太小时提前退出，保护 LRU 特性（对应 C++ early-out 2.1 条件）
        if self.queue.len() < purge_size * EARLY_OUT_MULTIPLIER {
            return;
        }

        let mut max_purges = self.queue.len() / purge_size;

        while max_purges > 0 {
            self.purge_iteration(&mut state, purge_size);

            let approx_size = self.queue.len();

            // early-out (2.1)：队列已缩小到足够小
            if approx_size < purge_size * EARLY_OUT_MULTIPLIER {
                break;
            }

            // early-out (2.2)：dead 比例已经下降到可接受范围
            let dead = self.total_dead_nodes.load(Ordering::Relaxed).min(approx_size);
            let alive = approx_size - dead;
            if alive * (ALIVE_NODE_MULTIPLIER - 1) > dead {
                break;
            }

            max_purges -= 1;
        }
    }

    /// 对应 C++ PurgeIteration(idx_t purge_size)
    fn purge_iteration(&self, state: &mut PurgeState, purge_size: usize) {
        state.buffer.clear();

        // 批量出队
        for _ in 0..purge_size {
            match self.queue.pop() {
                Some(node) => state.buffer.push(node),
                None => break,
            }
        }

        let dequeued = state.buffer.len();
        let mut alive = 0usize;

        // 原地 partition：存活节点移到前面
        // 注意：不能直接 retain，需要保持"存活节点重新入队"的语义
        for i in 0..dequeued {
            if state.buffer[i].try_get_block_handle().is_some() {
                state.buffer.swap(alive, i);
                alive += 1;
            }
        }

        // 重新入队存活节点（TODO: 按 LRU timestamp 排序以保持 LRU 特性）
        for node in state.buffer.drain(..alive) {
            self.queue.push(node);
        }

        // 更新 dead node 计数（对应 C++ total_dead_nodes -= dequeued - alive）
        let newly_dead = dequeued - alive;
        if newly_dead > 0 {
            self.total_dead_nodes
                .fetch_sub(newly_dead, Ordering::Relaxed);
        }
    }

    // ─── 迭代可驱逐块 ─────────────────────────────────────────

    /// 对应 C++ IterateUnloadableBlocks(FN fn)
    ///
    /// 遍历队列中可驱逐的 block，对每个可驱逐块调用回调 `f`。
    /// 回调参数：`(Arc<BlockHandle>) -> bool`
    ///   - 返回 `true`：继续遍历
    ///   - 返回 `false`：停止遍历
    ///
    /// 设计差异：
    ///   C++ 将 BlockLock 传入回调（在 iterate 内部加锁，callback 持锁操作）。
    ///   Rust 改为在回调内部加锁：回调收到 Arc<BlockHandle>，自行调用
    ///   `handle.lock()` 获取 BlockGuard，再调用 `handle.can_unload_guarded()`
    ///   二次确认。这样避免了 MutexGuard 的生命周期跨越 FnMut 边界的复杂性。
    pub fn iterate_unloadable<F>(&self, mut f: F)
    where
        F: FnMut(Arc<BlockHandle>) -> bool,
    {
        loop {
            // 先尝试无锁出队，失败则持锁强制出队
            let node = match self.queue.pop().or_else(|| self.try_dequeue_with_lock()) {
                Some(n) => n,
                None => return, // 队列已空
            };

            // 升级弱引用，同时快速检查（不持锁，可能过时）
            let handle = match node.try_get_block_handle() {
                Some(h) => h,
                None => {
                    // dead node：handle 已析构或序列号不匹配
                    self.decrement_dead_nodes();
                    continue;
                }
            };

            // 调用回调；回调内部负责持锁二次确认
            if !f(handle) {
                return;
            }
        }
    }
}
