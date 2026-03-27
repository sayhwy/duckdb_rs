// ============================================================
// storage/buffer/mod.rs
// 对应 C++: src/storage/buffer/ 目录下的所有文件
// ============================================================
//
// 模块依赖关系（自底向上）：
//
//   types           ← 无依赖（基础枚举、常量）
//   file_buffer     ← types
//   memory_usage    ← types
//   buffer_pool_reservation ← types  （通过 MemoryTracker trait 解耦 BufferPool）
//   eviction_queue  ← types, block_handle  （持 Weak<BlockHandle>）
//   block_handle    ← types, file_buffer, buffer_pool_reservation,
//                     buffer_handle, block_manager
//   buffer_handle   ← block_handle, block_manager
//   buffer_pool     ← types, file_buffer, memory_usage,
//                     buffer_pool_reservation, eviction_queue, block_handle
//   block_manager   ← types, file_buffer, block_handle, buffer_handle, buffer_pool
//
// 循环引用说明：
//   block_handle ↔ buffer_handle：均通过 Arc/所有权单向，非编译期循环。
//   block_handle → block_manager → buffer_pool → block_handle：
//     全部通过 Arc<dyn Trait> 指针，Rust 允许此类运行时引用环；
//     实际运行中不构成 Arc 引用环（BlockManager 持 Weak<BlockHandle>）。

mod types;
mod file_buffer;
mod memory_usage;
mod buffer_pool_reservation;
mod eviction_queue;
mod block_handle;
mod buffer_handle;
mod buffer_pool;
mod block_manager;

// ─── 公开导出 ─────────────────────────────────────────────────
// 按层次导出，外部模块直接使用 storage::buffer::Foo 即可

pub use types::{
    BlockId, Idx,
    INVALID_BLOCK, MAXIMUM_BLOCK, INVALID_INDEX,
    DEFAULT_BLOCK_ALLOC_SIZE, DEFAULT_BLOCK_HEADER_SIZE,
    MemoryTag, MEMORY_TAG_COUNT,
    FileBufferType, FILE_BUFFER_TYPE_COUNT,
    BlockState,
    DestroyBufferUpon,
};

pub use file_buffer::FileBuffer;
pub use memory_usage::MemoryUsage;

pub use buffer_pool_reservation::{
    MemoryTracker,
    BufferPoolReservation,
    TempBufferPoolReservation,
};

pub use eviction_queue::{EvictionNode, EvictionQueue};

pub use block_handle::{BlockGuard, BlockInner, BlockHandle};

pub use buffer_handle::BufferHandle;

pub use buffer_pool::{
    BlockAllocator,
    TemporaryMemoryManager,
    EvictionResult,
    BufferPool,
};

pub use block_manager::{BlockManager, BufferManager};
