// ============================================================
// metadata/metadata_writer.rs — 元数据顺序写入器
// 对应 C++: duckdb/storage/metadata/metadata_writer.hpp + .cpp
// ============================================================
//
// 核心职责：
//   实现 WriteStream trait，向链式 meta 子块中顺序写入字节流。
//   可跨越多个存储块，按 MetaBlockPointer 链表跳转。
//
// 子块内存布局（每个 meta 子块 = metadata_block_size 字节）：
//   [0..8]     : 下一个 MetaBlockPointer（u64；写 flush 时置 0xFFFF...FF 标记链表结束）
//   [8..size]  : 实际数据
//
// C++ 指针访问 → Rust 本地 Vec 缓存：
//   C++ 使用 BasePtr() + offset 的裸指针直接写入 pin 住的块。
//   Rust 把当前 MetadataHandle 保留在 current_handle，写入时通过
//   handle.with_data_mut 修改底层 payload（闭包内执行写入）。
//   为简化生命周期，使用 "write-through" 策略：
//     每次写入时打开闭包直接修改 payload，不额外维护 Vec。
//
// 写入器状态机：
//   initial   : capacity = 0，offset = 0（尚未分配第一个 handle）
//   active    : capacity > 0，持有 current_handle
//   flushed   : flush() 后 current_handle = None
//
// NextBlock() 流程：
//   1. 若存在当前块：将新块的 MetaBlockPointer 写入当前块的 [0..8]
//   2. 分配新的 MetadataHandle（AllocateHandle）
//   3. 初始化新块 [0..8] = 0xFFFF..FF（链表结束标记）
//   4. offset = 8，capacity = metadata_block_size
//
// ============================================================

use super::metadata_manager::MetadataManager;
use super::types::{BlockPointer, MetaBlockPointer, MetadataHandle, MetadataPointer, WriteStream};

// ─── MetadataWriter ───────────────────────────────────────────

/// 对应 C++ MetadataWriter : public WriteStream
pub struct MetadataWriter<'mgr> {
    /// 对应 C++ MetadataManager &manager
    manager: &'mgr MetadataManager,

    /// 当前 pin 住的 MetadataHandle（对应 C++ MetadataHandle block）
    current_handle: Option<MetadataHandle>,

    /// 当前子块的 MetadataPointer 快照（对应 C++ MetadataPointer current_pointer）
    current_pointer: MetadataPointer,

    /// 已写入块列表（对应 C++ optional_ptr<vector<MetaBlockPointer>> written_pointers）
    written_pointers: Option<Vec<MetaBlockPointer>>,

    /// 子块容量（= metadata_block_size；0 表示尚未分配第一块）
    capacity: usize,

    /// 当前子块内的写入位置（字节偏移）
    offset: usize,
}

impl<'mgr> MetadataWriter<'mgr> {
    // ─── 构造 ─────────────────────────────────────────────────

    /// 对应 C++ MetadataWriter(MetadataManager&, optional written_pointers)
    pub fn new(
        manager: &'mgr MetadataManager,
        written_pointers: Option<Vec<MetaBlockPointer>>,
    ) -> Self {
        Self {
            manager,
            current_handle: None,
            current_pointer: MetadataPointer::default(),
            written_pointers,
            capacity: 0,
            offset: 0,
        }
    }

    // ─── 指针查询 ─────────────────────────────────────────────

    /// 对应 C++ MetadataWriter::GetMetaBlockPointer()
    ///
    /// 若当前块已满则自动推进到下一块。
    pub fn get_meta_block_pointer(&mut self) -> MetaBlockPointer {
        if self.offset >= self.capacity {
            self.next_block();
        }
        self.manager
            .get_disk_pointer(&self.current_pointer, self.offset as u32)
    }

    /// 对应 C++ MetadataWriter::GetBlockPointer()
    pub fn get_block_pointer(&mut self) -> BlockPointer {
        let meta_ptr = self.get_meta_block_pointer();
        MetadataManager::to_block_pointer(meta_ptr, self.manager.get_metadata_block_size())
    }

    // ─── Flush ────────────────────────────────────────────────

    /// 对应 C++ MetadataWriter::Flush()
    ///
    /// 零填充当前块的剩余空间，然后销毁（unpin）当前 handle。
    pub fn flush(&mut self) {
        if self.current_handle.is_none() {
            return; // 从未写入任何数据
        }
        // 零填充剩余字节
        if self.offset < self.capacity {
            let offset = self.offset;
            let capacity = self.capacity;
            let pointer = self.current_pointer;
            self.write_to_current(|data| {
                let sub_start = pointer.index as usize * data.len()
                    / super::metadata_manager::METADATA_BLOCK_COUNT;
                // capacity 是 metadata_block_size，data 是整个存储块 payload
                // BasePtr = data + index * metadata_block_size
                // 此处直接操作相对于 offset 的剩余区域
                let base = pointer.index as usize * capacity;
                let end = base + capacity;
                if end <= data.len() && offset < capacity {
                    data[base + offset..end].fill(0);
                }
                let _ = sub_start;
            });
        }
        // 释放 handle（对应 C++ block.handle.Destroy()）
        self.current_handle = None;
    }

    /// 对应 C++ MetadataWriter::SetWrittenPointers()
    pub fn set_written_pointers(&mut self, written_pointers: Option<Vec<MetaBlockPointer>>) {
        let had_active = self.capacity > 0 && self.offset < self.capacity;
        self.written_pointers = written_pointers;
        if had_active {
            if let Some(ref mut ptrs) = self.written_pointers {
                let disk_ptr = self.manager.get_disk_pointer(&self.current_pointer, 0);
                ptrs.push(disk_ptr);
            }
        }
    }

    pub fn take_written_pointers(&mut self) -> Option<Vec<MetaBlockPointer>> {
        self.written_pointers.take()
    }

    /// 对应 C++ MetadataWriter::NextHandle()（可被子类覆盖）
    /// 默认实现：向 MetadataManager 申请新 handle。
    fn next_handle(&self) -> MetadataHandle {
        self.manager.allocate_handle()
    }

    // ─── 私有：块切换 ─────────────────────────────────────────

    /// 对应 C++ MetadataWriter::NextBlock()
    ///
    /// 1. 将新块的 disk pointer 写入当前块的 [0..8]（next-pointer 字段）
    /// 2. 分配新 handle，初始化新块 [0..8] = 0xFFFF..FF
    /// 3. 更新 offset / capacity / current_pointer
    fn next_block(&mut self) {
        // 1. 分配下一个 handle
        let new_handle = self.next_handle();
        let new_disk_ptr = self.manager.get_disk_pointer(&new_handle.pointer, 0);

        // 2. 若存在当前块：把新块的磁盘指针写入当前块的 [base..base+8]
        if self.capacity > 0 {
            let idx = self.current_pointer.index;
            let capacity = self.capacity;
            self.write_to_current(|data| {
                let base = idx as usize * capacity;
                if base + 8 <= data.len() {
                    data[base..base + 8].copy_from_slice(&new_disk_ptr.block_pointer.to_le_bytes());
                }
            });
        }

        // 3. 切换到新块，初始化 [base..base+8] = 0xFFFF..FF（链表结束）
        self.current_handle = Some(new_handle);
        let ptr = self.current_handle.as_ref().unwrap().pointer;
        self.current_pointer = ptr;
        self.capacity = self.manager.get_metadata_block_size();
        self.offset = std::mem::size_of::<u64>(); // skip next-pointer field

        let idx = ptr.index;
        let cap = self.capacity;
        self.write_to_current(|data| {
            let base = idx as usize * cap;
            if base + 8 <= data.len() {
                data[base..base + 8].fill(0xFF);
            }
        });

        // 4. 记录 written_pointers
        if let Some(ref mut ptrs) = self.written_pointers {
            let disk_ptr = self.manager.get_disk_pointer(&ptr, 0);
            ptrs.push(disk_ptr);
        }
    }

    /// 辅助：对当前 handle 的 payload 执行 write 闭包
    fn write_to_current<F: FnOnce(&mut [u8])>(&self, f: F) {
        if let Some(ref handle) = self.current_handle {
            handle.handle.with_data_mut(f);
        }
    }
}

// ─── WriteStream 实现 ─────────────────────────────────────────

impl<'mgr> WriteStream for MetadataWriter<'mgr> {
    /// 对应 C++ MetadataWriter::WriteData(const_data_ptr_t buffer, idx_t write_size)
    ///
    /// 支持跨越多个 meta 子块的写入（每次跨块时调用 next_block）。
    fn write_data(&mut self, buf: &[u8]) {
        let mut remaining = buf.len();
        let mut read_pos = 0usize;

        while remaining > 0 {
            // 初始化或块已满 → 推进到下一块
            if self.offset >= self.capacity {
                self.next_block();
            }

            let available = self.capacity - self.offset;
            let to_copy = available.min(remaining);

            // 写入当前子块
            let write_offset = self.offset;
            let sub_index = self.current_pointer.index;
            let cap = self.capacity;
            let src = &buf[read_pos..read_pos + to_copy];
            // 需要将 src 的数据写入 payload 中的指定位置
            // 使用 Vec 临时拷贝，再通过 with_data_mut 一次性写入
            let src_owned: Vec<u8> = src.to_vec();
            self.write_to_current(|data| {
                let base = sub_index as usize * cap + write_offset;
                let end = base + to_copy;
                if end <= data.len() {
                    data[base..end].copy_from_slice(&src_owned[..to_copy]);
                }
            });

            self.offset += to_copy;
            read_pos += to_copy;
            remaining -= to_copy;
        }
    }
}

impl<'mgr> Drop for MetadataWriter<'mgr> {
    fn drop(&mut self) {
        // 对应 C++ ~MetadataWriter()
        // 注：C++ 注释说明：异常时析构不 flush 是安全的（未写入的数据不会被引用）
        // Rust 中 Drop 不能 panic，所以也不调用 flush（由调用方显式调用）
        // current_handle 的 Drop 会自动 unpin
    }
}
