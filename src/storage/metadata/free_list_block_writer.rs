// ============================================================
// free_list_block_writer.rs — Free List 块写入器
// 对应 C++: duckdb/storage/single_file_block_manager.cpp (FreeListBlockWriter)
// ============================================================
//
// FreeListBlockWriter 是一个特殊的写入器，
// 它从预先分配的 free list blocks 中获取块，而不是从 MetadataManager 分配。
//
// 这是必要的，因为我们正在写入 free list 本身，
// 如果使用普通的 MetadataWriter，它会尝试从 free list 中分配块，
// 这会导致循环依赖。

use super::metadata_manager::MetadataManager;
use super::types::{MetaBlockPointer, MetadataHandle, MetadataPointer, WriteStream};

/// 对应 C++ FreeListBlockWriter : public MetadataWriter
pub struct FreeListBlockWriter<'mgr> {
    /// 对应 C++ MetadataManager &manager
    manager: &'mgr MetadataManager,

    /// 预先分配的 free list blocks
    free_list_blocks: Vec<MetadataHandle>,

    /// 当前使用的块索引
    index: usize,

    /// 当前 pin 住的 MetadataHandle
    current_handle: Option<MetadataHandle>,

    /// 当前子块的 MetadataPointer
    current_pointer: MetadataPointer,

    /// 子块容量（= metadata_block_size）
    capacity: usize,

    /// 当前子块内的写入位置
    offset: usize,
}

impl<'mgr> FreeListBlockWriter<'mgr> {
    /// 对应 C++ FreeListBlockWriter(MetadataManager &manager, vector<MetadataHandle> free_list_blocks_p)
    pub fn new(manager: &'mgr MetadataManager, free_list_blocks: Vec<MetadataHandle>) -> Self {
        Self {
            manager,
            free_list_blocks,
            index: 0,
            current_handle: None,
            current_pointer: MetadataPointer::default(),
            capacity: 0,
            offset: 0,
        }
    }

    /// 获取 meta block 指针
    pub fn get_meta_block_pointer(&mut self) -> MetaBlockPointer {
        if self.offset >= self.capacity {
            self.next_block();
        }
        self.manager
            .get_disk_pointer(&self.current_pointer, self.offset as u32)
    }

    /// Flush 当前块
    pub fn flush(&mut self) {
        if self.current_handle.is_none() {
            return;
        }
        // 零填充剩余字节
        if self.offset < self.capacity {
            let offset = self.offset;
            let capacity = self.capacity;
            let pointer = self.current_pointer;
            self.write_to_current(|data| {
                let base = pointer.index as usize * capacity;
                let end = base + capacity;
                if end <= data.len() && offset < capacity {
                    data[base + offset..end].fill(0);
                }
            });
        }
        // 释放 handle
        self.current_handle = None;
    }

    /// 获取下一个预分配的 handle
    fn next_handle(&mut self) -> MetadataHandle {
        if self.index >= self.free_list_blocks.len() {
            panic!("Free List Block Writer ran out of blocks");
        }
        let handle = std::mem::replace(
            &mut self.free_list_blocks[self.index],
            MetadataHandle {
                pointer: Default::default(),
                handle: crate::storage::buffer::BufferHandle::invalid(),
            },
        );
        self.index += 1;
        handle
    }

    /// 推进到下一个块
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
    }

    /// 辅助：对当前 handle 的 payload 执行 write 闭包
    fn write_to_current<F: FnOnce(&mut [u8])>(&self, f: F) {
        if let Some(ref handle) = self.current_handle {
            handle.handle.with_data_mut(f);
        }
    }
}

impl<'mgr> WriteStream for FreeListBlockWriter<'mgr> {
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
