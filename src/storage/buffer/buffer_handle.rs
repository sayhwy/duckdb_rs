// ============================================================
// buffer_handle.rs — 缓冲区 RAII pin guard
// 对应 C++: duckdb/storage/buffer/buffer_handle.hpp + buffer_handle.cpp
// ============================================================
//
// 核心职责：
//   持有对 BlockHandle 的 Arc 引用，确保块在 pin 期间不被驱逐。
//   Drop 时自动调用 Unpin（减少 reader 计数）。
//
// 最关键的设计差异——无 unsafe 替代 C++ Ptr()：
//   C++ BufferHandle::Ptr() 返回 data_ptr_t（裸指针），
//   调用者直接读写，生命周期由 pin 状态隐式保证。
//
//   Rust 提供两种安全方案：
//
//   方案 A（本文件采用）: 回调风格
//     handle.with_data(|bytes| { /* 使用 &[u8] */ })
//     handle.with_data_mut(|bytes| { /* 使用 &mut [u8] */ })
//     → 在闭包期间持有块锁，生命周期由借用检查器保证。
//     优点：完全安全；缺点：锁粒度较粗（整个数据访问期间持锁）。
//
//   方案 B（后续可选优化）:
//     将 FileBuffer 包装在 Arc<RwLock<FileBuffer>> 中，
//     pin 时克隆 Arc，提供 RwLockReadGuard/WriteGuard。
//     优点：允许细粒度锁；缺点：增加 Arc 分配开销。
//
//   当前实现选择方案 A，适合初始骨架。

use super::block_handle::BlockHandle;
use std::sync::Arc;

/// 对应 C++ BufferHandle
///
/// RAII：构造时 block 已 pin（readers 已递增），
/// Drop/destroy 时调用 BufferManager::unpin() 归还。
pub struct BufferHandle {
    /// 对应 C++ shared_ptr<BlockHandle> handle
    /// None 表示无效（对应 C++ handle == nullptr 的空 BufferHandle）
    handle: Option<Arc<BlockHandle>>,
}

impl BufferHandle {
    /// 对应 C++ BufferHandle()（默认构造，无效状态）
    pub fn invalid() -> Self {
        Self { handle: None }
    }

    /// 对应 C++ BufferHandle(shared_ptr<BlockHandle> handle, optional_ptr<FileBuffer> node)
    /// 由 BlockHandle::load() 调用，此时 block 已处于 Loaded 状态。
    pub fn new(handle: Arc<BlockHandle>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    // ─── 状态查询 ─────────────────────────────────────────────

    /// 对应 C++ IsValid()
    pub fn is_valid(&self) -> bool {
        self.handle.is_some()
    }

    /// 获取底层 BlockHandle（只读）
    pub fn block_handle(&self) -> Option<&Arc<BlockHandle>> {
        self.handle.as_ref()
    }

    // ─── 数据访问（无 unsafe 替代 C++ Ptr()）────────────────

    /// 对应 C++ Ptr() const（只读版）
    ///
    /// 持块锁，在回调中提供对 payload 的只读访问。
    /// 回调期间 BlockHandle 的 inner 锁被持有（短暂）。
    ///
    /// 示例：
    /// ```ignore
    /// handle.with_data(|bytes| {
    ///     let value = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    /// });
    /// ```
    pub fn with_data<R, F>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        let handle = self.handle.as_ref()?;
        let guard = handle.lock();
        let buf = guard.buffer.as_ref()?;
        Some(f(buf.payload()))
    }

    /// 对应 C++ Ptr()（可写版）
    ///
    /// 持块锁，在回调中提供对 payload 的可写访问。
    pub fn with_data_mut<R, F>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let handle = self.handle.as_ref()?;
        let mut guard = handle.lock();
        let buf = guard.buffer.as_mut()?;
        Some(f(buf.payload_mut()))
    }

    /// 对应 C++ GetFileBuffer()
    ///
    /// 在回调中访问完整 FileBuffer（含 header）。
    pub fn with_file_buffer<R, F>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&FileBuffer) -> R,
    {
        let handle = self.handle.as_ref()?;
        let guard = handle.lock();
        let buf = guard.buffer.as_ref()?;
        Some(f(buf))
    }

    /// 获取 payload 数据大小（不持锁的快速路径：通过 memory_usage 估算）
    pub fn alloc_size(&self) -> usize {
        self.handle.as_ref().map(|h| h.memory_usage()).unwrap_or(0)
    }

    // ─── 显式释放 ─────────────────────────────────────────────

    /// 对应 C++ Destroy()
    ///
    /// 显式释放 pin：通知 BufferManager 执行 unpin 逻辑
    /// （减少 reader 计数，必要时加入驱逐队列）。
    ///
    /// Drop 时也会自动调用，因此通常不需要手动调用。
    pub fn destroy(&mut self) {
        if let Some(handle) = self.handle.take() {
            // 通过 BlockManager → BufferManager 路径执行 unpin
            let bm = handle.block_manager.buffer_manager();
            bm.unpin(handle);
        }
    }
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        // 对应 C++ ~BufferHandle() { Destroy(); }
        self.destroy();
    }
}

impl std::fmt::Debug for BufferHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferHandle")
            .field("valid", &self.is_valid())
            .finish()
    }
}

// BufferHandle 不可 Clone（对应 C++ 禁用 copy constructor）
// 但可以 move（对应 C++ 启用 move constructor）

// ─── 辅助引用 ─────────────────────────────────────────────────
// 从 block_handle 引入（避免循环，在此重导出 FileBuffer 类型供外部使用）
use super::file_buffer::FileBuffer;
