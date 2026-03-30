// ============================================================
// buffer_pool_reservation.rs — 内存预约 RAII 对象
// 对应 C++: BufferPoolReservation / TempBufferPoolReservation
//           (定义在 block_handle.hpp，实现在 buffer_pool_reservation.cpp)
// ============================================================
//
// 设计差异：
//   C++ BufferPoolReservation 持有 `BufferPool &pool`（裸引用）。
//   Rust 改用 `Arc<dyn MemoryTracker>` trait 对象：
//     1. 打破 BufferPoolReservation ↔ BufferPool 循环引用（BufferPool
//        实现 MemoryTracker，但 reservation 无需直接知道 BufferPool 类型）
//     2. 方便单元测试（可 mock MemoryTracker）
//
//   C++ 析构时 D_ASSERT(size == 0)，即调用者必须先 resize(0)。
//   Rust 沿用此约定：Drop 里 debug_assert，Release 下静默放行。

use super::types::MemoryTag;
use std::sync::Arc;

// ─── MemoryTracker trait ──────────────────────────────────────
/// 对应 C++ 中 BufferPool::UpdateUsedMemory() 的最小接口。
/// BufferPool 实现此 trait；测试时可用 mock 实现。
pub trait MemoryTracker: Send + Sync {
    fn update_used_memory(&self, tag: MemoryTag, delta: i64);
}

// ─── BufferPoolReservation ────────────────────────────────────
/// 对应 C++ BufferPoolReservation
///
/// RAII 内存预约：构造时不占用内存，通过 `resize()` 声明占用量，
/// 析构前必须调用 `resize(0)` 归还（否则 debug_assert 报错）。
pub struct BufferPoolReservation {
    pub tag: MemoryTag,
    pub size: usize,
    tracker: Arc<dyn MemoryTracker>,
}

impl BufferPoolReservation {
    /// 对应 C++ BufferPoolReservation(MemoryTag tag, BufferPool &pool)
    pub fn new(tag: MemoryTag, tracker: Arc<dyn MemoryTracker>) -> Self {
        Self {
            tag,
            size: 0,
            tracker,
        }
    }

    /// 对应 C++ Resize(idx_t new_size)
    ///
    /// 计算 delta = new_size - size，通知 MemoryTracker 更新全局计数，
    /// 然后更新 self.size。
    pub fn resize(&mut self, new_size: usize) {
        let delta = new_size as i64 - self.size as i64;
        self.tracker.update_used_memory(self.tag, delta);
        self.size = new_size;
    }

    /// 对应 C++ Merge(BufferPoolReservation src)
    ///
    /// 将 src 的 size 吸收进 self；src.size 归零以阻止其 Drop 双重归还。
    /// 注意：两者必须属于同一 MemoryTag（与 C++ 语义一致）。
    pub fn merge(&mut self, mut src: BufferPoolReservation) {
        debug_assert_eq!(
            self.tag, src.tag,
            "Cannot merge reservations with different MemoryTags"
        );
        self.size += src.size;
        src.size = 0; // 阻止 Drop 触发 assert
    }
}

impl Drop for BufferPoolReservation {
    fn drop(&mut self) {
        // 对应 C++ D_ASSERT(size == 0)
        // 若 size != 0，说明调用者忘记归还内存
        debug_assert_eq!(
            self.size, 0,
            "BufferPoolReservation dropped with non-zero size ({} bytes). \
             Call resize(0) before dropping.",
            self.size
        );
        // Release 模式下：静默归还，避免资源泄漏
        if self.size != 0 {
            self.tracker
                .update_used_memory(self.tag, -(self.size as i64));
        }
    }
}

// ─── TempBufferPoolReservation ────────────────────────────────
/// 对应 C++ TempBufferPoolReservation
///
/// 临时内存预约：构造时预约指定大小，Drop 时自动调用 resize(0) 归还。
/// 用于 EvictBlocks 等临时持有内存的操作。
pub struct TempBufferPoolReservation(BufferPoolReservation);

impl TempBufferPoolReservation {
    /// 对应 C++ TempBufferPoolReservation(MemoryTag, BufferPool &, idx_t size)
    pub fn new(tag: MemoryTag, tracker: Arc<dyn MemoryTracker>, size: usize) -> Self {
        let mut r = BufferPoolReservation::new(tag, tracker);
        r.resize(size);
        Self(r)
    }

    /// 只读访问内部 reservation
    pub fn inner(&self) -> &BufferPoolReservation {
        &self.0
    }

    /// 可写访问内部 reservation（用于 resize 等操作）
    pub fn inner_mut(&mut self) -> &mut BufferPoolReservation {
        &mut self.0
    }

    /// 消耗自身，转移内部 reservation 所有权（不触发自动归零）。
    /// 由接收方负责后续的 resize(0) 或 merge。
    pub fn into_inner(mut self) -> BufferPoolReservation {
        // 先把 size 记下来，然后把内部归零（阻止 Drop 双重扣减），
        // 再构造一个持有相同 size 的新 reservation 返回给调用者。
        let size = self.0.size;
        let tag = self.0.tag;
        let tracker = Arc::clone(&self.0.tracker);
        self.0.size = 0; // 阻止 Drop 里 resize(0)
        let mut r = BufferPoolReservation::new(tag, tracker);
        // 直接设置 size，不通过 resize（避免重复通知 tracker）
        r.size = size;
        r
    }
}

impl Drop for TempBufferPoolReservation {
    fn drop(&mut self) {
        // 自动归还：对应 C++ ~TempBufferPoolReservation() { Resize(0); }
        if self.0.size != 0 {
            self.0.resize(0);
        }
    }
}
