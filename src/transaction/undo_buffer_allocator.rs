//! UndoBuffer 底层分配器。
//!
//! 对应 C++:
//!   `duckdb/transaction/undo_buffer_allocator.hpp`
//!   `duckdb/transaction/undo_buffer_allocator.cpp`
//!
//! # 内存布局
//!
//! 每个 `UndoBufferSlab` 对应缓冲管理器中的一个 Block（约 256 KB）。
//! 分配器按需申请新 Slab，写满后追加新 Slab。
//!
//! 每条 Undo 条目在 Slab 中的格式：
//! ```text
//! [ UndoFlags (1 byte) | payload_len (4 bytes LE) | payload (payload_len bytes) ]
//! ```

use parking_lot::Mutex;

use super::types::{Idx, UndoFlags};

// ─── UndoBufferSlab ────────────────────────────────────────────────────────────

/// 分配器中的一个内存页切片（C++: `UndoBufferEntry`）。
///
/// 每个 Slab 持有一个内存缓冲区，并追踪当前写入位置与总容量。
pub struct UndoBufferSlab {
    /// 内存缓冲区。
    pub data: Vec<u8>,
    /// 已写入字节数。
    pub position: usize,
    /// Slab 总容量。
    pub capacity: usize,
}

impl UndoBufferSlab {
    /// 创建新的 Slab。
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            position: 0,
            capacity,
        }
    }

    /// 剩余可写字节数。
    #[inline]
    pub fn remaining(&self) -> usize {
        self.capacity.saturating_sub(self.position)
    }

    /// 该 Slab 是否还能容纳 `len` 字节。
    #[inline]
    pub fn can_fit(&self, len: usize) -> bool {
        self.remaining() >= len
    }
}

// ─── UndoBufferReference ───────────────────────────────────────────────────────

/// 已分配的 Undo 条目引用。
pub struct UndoBufferReference {
    /// 在分配器 `slabs` 中的下标。
    pub slab_index: Option<usize>,
    /// 条目载荷起始偏移。
    pub position: usize,
}

impl UndoBufferReference {
    /// 空引用。
    pub fn null() -> Self {
        Self {
            slab_index: None,
            position: 0,
        }
    }

    /// 是否有效。
    pub fn is_set(&self) -> bool {
        self.slab_index.is_some()
    }

    /// 获取对应的弱指针。
    pub fn as_pointer(&self) -> UndoBufferPointer {
        UndoBufferPointer {
            slab_index: self.slab_index,
            position: self.position,
        }
    }
}

// ─── UndoBufferPointer ─────────────────────────────────────────────────────────

/// 未固定的 Undo 条目弱引用。
#[derive(Debug, Clone, Copy)]
pub struct UndoBufferPointer {
    /// Slab 下标；`None` 表示 null。
    pub slab_index: Option<usize>,
    /// 条目载荷起始偏移。
    pub position: usize,
}

impl UndoBufferPointer {
    /// Null 指针。
    pub const fn null() -> Self {
        Self {
            slab_index: None,
            position: 0,
        }
    }

    /// 是否有效。
    pub fn is_set(&self) -> bool {
        self.slab_index.is_some()
    }
}

// ─── UndoBufferAllocatorInner ──────────────────────────────────────────────────

/// Mutex 保护的可变内部状态。
pub struct UndoBufferAllocatorInner {
    /// Slab 链，head = slabs[0]，tail = slabs[last]。
    pub slabs: Vec<UndoBufferSlab>,
}

// ─── UndoBufferAllocator ───────────────────────────────────────────────────────

/// Undo 条目分配器。
pub struct UndoBufferAllocator {
    /// 保护 Slab 链的互斥锁。
    pub(crate) inner: Mutex<UndoBufferAllocatorInner>,
    /// 默认 Slab 容量。
    pub block_alloc_size: Idx,
}

impl UndoBufferAllocator {
    /// 构造分配器。
    pub fn new(block_alloc_size: Idx) -> Self {
        Self {
            inner: Mutex::new(UndoBufferAllocatorInner { slabs: Vec::new() }),
            block_alloc_size,
        }
    }

    /// 分配 `alloc_len` 字节的 Undo 条目，返回引用。
    ///
    /// 若当前 tail Slab 空间不足，申请新 Slab。
    pub fn allocate(&self, alloc_len: usize) -> UndoBufferReference {
        let mut lock = self.inner.lock();

        // 检查是否需要新 Slab
        let needs_new_slab = lock.slabs.last().map_or(true, |s| !s.can_fit(alloc_len));

        if needs_new_slab {
            // 确保新 Slab 能容纳请求
            let new_capacity = std::cmp::max(self.block_alloc_size as usize, alloc_len);
            lock.slabs.push(UndoBufferSlab::new(new_capacity));
        }

        let slab_idx = lock.slabs.len() - 1;
        let slab = &mut lock.slabs[slab_idx];
        let position = slab.position;

        // 预留空间
        slab.position += alloc_len;

        UndoBufferReference {
            slab_index: Some(slab_idx),
            position,
        }
    }

    /// 是否有任何已分配的条目。
    pub fn has_entries(&self) -> bool {
        let lock = self.inner.lock();
        !lock.slabs.is_empty()
    }

    /// 从 head 到 tail 遍历所有已分配条目。
    ///
    /// `callback(flags: UndoFlags, payload: &[u8])`
    pub fn iterate_forward<F>(&self, mut callback: F)
    where
        F: FnMut(UndoFlags, &[u8]),
    {
        let lock = self.inner.lock();

        for slab in &lock.slabs {
            let mut pos = 0;
            while pos + 5 <= slab.position {
                // 读取 flags (1 byte)
                let flags_byte = slab.data[pos];
                let flags = match UndoFlags::try_from(flags_byte) {
                    Ok(f) => f,
                    Err(_) => break, // 无效 flags，停止遍历
                };

                // 读取 payload_len (4 bytes LE)
                let payload_len = u32::from_le_bytes([
                    slab.data[pos + 1],
                    slab.data[pos + 2],
                    slab.data[pos + 3],
                    slab.data[pos + 4],
                ]) as usize;

                let header_end = pos + 5;
                let payload_end = header_end + payload_len;

                if payload_end > slab.position {
                    break; // 数据不完整
                }

                // 回调
                callback(flags, &slab.data[header_end..payload_end]);

                pos = payload_end;
            }
        }
    }

    /// 从 tail 到 head 反向遍历（用于 Rollback）。
    pub fn iterate_reverse<F>(&self, mut callback: F)
    where
        F: FnMut(UndoFlags, &[u8]),
    {
        let lock = self.inner.lock();

        // 从最后一个 Slab 开始反向遍历
        for slab_idx in (0..lock.slabs.len()).rev() {
            let slab = &lock.slabs[slab_idx];

            // 收集该 Slab 中所有条目的位置
            let mut entries = Vec::new();
            let mut pos = 0;
            while pos + 5 <= slab.position {
                let flags_byte = slab.data[pos];
                let flags = match UndoFlags::try_from(flags_byte) {
                    Ok(f) => f,
                    Err(_) => break,
                };

                let payload_len = u32::from_le_bytes([
                    slab.data[pos + 1],
                    slab.data[pos + 2],
                    slab.data[pos + 3],
                    slab.data[pos + 4],
                ]) as usize;

                let header_end = pos + 5;
                let payload_end = header_end + payload_len;

                if payload_end > slab.position {
                    break;
                }

                entries.push((pos, flags, header_end, payload_end));
                pos = payload_end;
            }

            // 反向遍历该 Slab 中的条目
            for (_, flags, header_end, payload_end) in entries.into_iter().rev() {
                callback(flags, &slab.data[header_end..payload_end]);
            }
        }
    }

    /// 计算所有 Slab 已使用字节总和。
    pub fn total_size(&self) -> usize {
        let lock = self.inner.lock();
        lock.slabs.iter().map(|s| s.position).sum()
    }

    /// 写入数据到指定位置。
    pub fn write_at(&self, slab_index: usize, position: usize, data: &[u8]) {
        let mut lock = self.inner.lock();
        if slab_index < lock.slabs.len() {
            let slab = &mut lock.slabs[slab_index];
            let end = position + data.len();
            if end <= slab.capacity {
                slab.data[position..end].copy_from_slice(data);
            }
        }
    }

    /// 读取指定位置的数据。
    pub fn read_at(&self, slab_index: usize, position: usize, len: usize) -> Option<Vec<u8>> {
        let lock = self.inner.lock();
        if slab_index < lock.slabs.len() {
            let slab = &lock.slabs[slab_index];
            let end = position + len;
            if end <= slab.position {
                return Some(slab.data[position..end].to_vec());
            }
        }
        None
    }

    /// 清空所有 Slab。
    pub fn clear(&self) {
        let mut lock = self.inner.lock();
        lock.slabs.clear();
    }
}
