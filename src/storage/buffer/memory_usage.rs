// ============================================================
// memory_usage.rs — 分片原子内存计数器
// 对应 C++: BufferPool::MemoryUsage（定义在 buffer_pool.hpp 内部）
// ============================================================
//
// 设计要点：
//   C++ 使用 CPU ID（TaskScheduler::GetEstimatedCPUId()）做分片，
//   避免多核对同一计数器的 CAS 竞争。
//
//   Rust 改用线程 ID 哈希取模，语义等价：同一线程大概率命中同一 slot，
//   不同线程分散写，全局 flush 时才归并到全局计数器。
//
//   精度说明：与 C++ 相同，在 no-flush 模式下误差上限为
//   CACHE_COUNT × CACHE_THRESHOLD = 64 × 32KB = 2MB。

use super::types::{MEMORY_TAG_COUNT, MemoryTag};
use std::sync::atomic::{AtomicI64, Ordering};

/// 每个分片缓存的阈值：超过此值才 flush 到全局计数器
const CACHE_THRESHOLD: i64 = 32 * 1024; // 32KB

/// 分片槽数量（对应 C++ MEMORY_USAGE_CACHE_COUNT = 64）
const CACHE_COUNT: usize = 64;

/// global/cache 数组的总 slot 数：MEMORY_TAG_COUNT 个 tag + 1 个 total
const SLOT_COUNT: usize = MEMORY_TAG_COUNT + 1;

/// total 的 slot 索引（最后一个）
const TOTAL_IDX: usize = MEMORY_TAG_COUNT;

/// 创建一行 AtomicI64 槽（Vec 方式，避免 array::from_fn 对 non-Copy 的限制）
fn make_row() -> Vec<AtomicI64> {
    (0..SLOT_COUNT).map(|_| AtomicI64::new(0)).collect()
}

/// 对应 C++ BufferPool::MemoryUsage
///
/// 两层结构：
///   - `caches[slot_idx][tag]` : per-thread 缓存计数器（减少 CAS 竞争）
///   - `global[tag]`           : 真正的全局计数器
pub struct MemoryUsage {
    /// 全局计数器，length = SLOT_COUNT
    global: Vec<AtomicI64>,
    /// 分片缓存，CACHE_COUNT 行 × SLOT_COUNT 列
    caches: Vec<Vec<AtomicI64>>,
}

impl MemoryUsage {
    pub fn new() -> Self {
        Self {
            global: make_row(),
            caches: (0..CACHE_COUNT).map(|_| make_row()).collect(),
        }
    }

    // ─── 私有辅助 ────────────────────────────────────────────

    /// 用线程 ID 哈希确定分片 slot（对应 C++ GetEstimatedCPUId()）
    fn cache_slot() -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        std::thread::current().id().hash(&mut h);
        (h.finish() as usize) % CACHE_COUNT
    }

    // ─── 公开接口 ────────────────────────────────────────────

    /// 对应 C++ UpdateUsedMemory(MemoryTag tag, int64_t size)
    ///
    /// 小增量写缓存；大增量直接写全局（与 C++ 逻辑一致）。
    pub fn update(&self, tag: MemoryTag, delta: i64) {
        let tag_idx = tag as usize;

        if delta.unsigned_abs() < CACHE_THRESHOLD as u64 {
            let slot = &self.caches[Self::cache_slot()];

            // 更新 tag 分片缓存
            let new_tag = slot[tag_idx].fetch_add(delta, Ordering::Relaxed) + delta;
            if new_tag.unsigned_abs() >= CACHE_THRESHOLD as u64 {
                let flushed = slot[tag_idx].swap(0, Ordering::Relaxed);
                self.global[tag_idx].fetch_add(flushed, Ordering::Relaxed);
            }

            // 更新 total 分片缓存
            let new_total = slot[TOTAL_IDX].fetch_add(delta, Ordering::Relaxed) + delta;
            if new_total.unsigned_abs() >= CACHE_THRESHOLD as u64 {
                let flushed = slot[TOTAL_IDX].swap(0, Ordering::Relaxed);
                self.global[TOTAL_IDX].fetch_add(flushed, Ordering::Relaxed);
            }
        } else {
            // 大块直接写全局
            self.global[tag_idx].fetch_add(delta, Ordering::Relaxed);
            self.global[TOTAL_IDX].fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// 不 flush 缓存，快速读 total（对应 C++ NO_FLUSH 模式）
    /// 可能有最多 2MB 误差，用于内存压力快速判断。
    pub fn get_used_no_flush(&self) -> usize {
        let v = self.global[TOTAL_IDX].load(Ordering::Relaxed);
        v.max(0) as usize
    }

    /// flush 所有分片缓存后读 total（对应 C++ FLUSH 模式）
    /// 精确值，但有全局 swap 开销。
    pub fn get_used_flush(&self) -> usize {
        let mut cached: i64 = 0;
        for row in &self.caches {
            cached += row[TOTAL_IDX].swap(0, Ordering::Relaxed);
        }
        let v = self.global[TOTAL_IDX].fetch_add(cached, Ordering::Relaxed) + cached;
        v.max(0) as usize
    }

    /// 读指定 tag 的使用量（flush 模式）
    pub fn get_tag_used_flush(&self, tag: MemoryTag) -> usize {
        let idx = tag as usize;
        let mut cached: i64 = 0;
        for row in &self.caches {
            cached += row[idx].swap(0, Ordering::Relaxed);
        }
        let v = self.global[idx].fetch_add(cached, Ordering::Relaxed) + cached;
        v.max(0) as usize
    }
}

impl Default for MemoryUsage {
    fn default() -> Self {
        Self::new()
    }
}
