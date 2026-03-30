use std::sync::Arc;

use crate::storage::buffer::{
    BlockId, BlockManager, BufferHandle, BufferManager, INVALID_BLOCK, MemoryTag,
};

use super::string_checkpoint_state::{OverflowStringWriter, UncompressedStringSegmentState};

pub struct WriteOverflowStringsToDisk {
    pub partial_block_manager: Arc<dyn BlockManager>,
    pub handle: BufferHandle,
    pub block_id: BlockId,
    pub offset: usize,
}

impl WriteOverflowStringsToDisk {
    pub fn new(partial_block_manager: Arc<dyn BlockManager>) -> Self {
        Self {
            partial_block_manager,
            handle: BufferHandle::invalid(),
            block_id: INVALID_BLOCK,
            offset: 0,
        }
    }

    fn allocate_new_block(
        &mut self,
        state: &mut UncompressedStringSegmentState,
        new_block_id: BlockId,
    ) {
        if self.block_id != INVALID_BLOCK {
            let next = new_block_id.to_le_bytes();
            let trailer_offset = self.get_string_space();
            let _ = self.handle.with_data_mut(|data| {
                let end = trailer_offset + next.len();
                if end <= data.len() {
                    data[trailer_offset..end].copy_from_slice(&next);
                }
            });
            self.flush();
        }

        self.offset = 0;
        self.block_id = new_block_id;
        state.register_block(&self.partial_block_manager, new_block_id);
    }

    fn get_string_space(&self) -> usize {
        self.partial_block_manager.get_block_size() - std::mem::size_of::<BlockId>()
    }

    fn ensure_handle(&mut self) {
        if self.handle.is_valid() {
            return;
        }
        let buffer_manager = self.partial_block_manager.buffer_manager();
        let allocated = buffer_manager.allocate_memory(
            MemoryTag::OverflowStrings,
            Some(&*self.partial_block_manager),
            true,
        );
        self.handle = buffer_manager.pin(allocated);
    }

    fn next_synthetic_block_id(&self) -> BlockId {
        self.partial_block_manager.peek_free_block_id()
    }
}

impl OverflowStringWriter for WriteOverflowStringsToDisk {
    fn write_string(
        &mut self,
        state: &mut UncompressedStringSegmentState,
        string: &[u8],
        result_block: &mut BlockId,
        result_offset: &mut i32,
    ) {
        self.ensure_handle();

        if self.block_id == INVALID_BLOCK
            || self.offset + 2 * std::mem::size_of::<u32>() >= self.get_string_space()
        {
            self.allocate_new_block(state, self.next_synthetic_block_id());
        }

        *result_block = self.block_id;
        *result_offset = self.offset as i32;

        let mut remaining = string.len();
        let mut source_offset = 0usize;
        let len_bytes = (string.len() as u32).to_le_bytes();

        let _ = self.handle.with_data_mut(|data| {
            data[self.offset..self.offset + len_bytes.len()].copy_from_slice(&len_bytes);
        });
        self.offset += len_bytes.len();

        while remaining > 0 {
            let writable = self.get_string_space().saturating_sub(self.offset);
            let to_write = writable.min(remaining);

            if to_write > 0 {
                let start = source_offset;
                let end = start + to_write;
                let target_offset = self.offset;
                let _ = self.handle.with_data_mut(|data| {
                    data[target_offset..target_offset + to_write]
                        .copy_from_slice(&string[start..end]);
                });
                self.offset += to_write;
                source_offset += to_write;
                remaining -= to_write;
            }

            if remaining > 0 {
                self.allocate_new_block(state, self.next_synthetic_block_id());
            }
        }
    }

    fn flush(&mut self) {
        if self.block_id != INVALID_BLOCK && self.offset > 0 {
            let string_space = self.get_string_space();
            let _ = self.handle.with_data_mut(|data| {
                if self.offset < string_space {
                    data[self.offset..string_space].fill(0);
                }
            });

            let _ = self.handle.with_file_buffer(|buf| {
                self.partial_block_manager.write_block(buf, self.block_id);
            });
        }

        self.block_id = INVALID_BLOCK;
        self.offset = 0;
    }
}

impl Drop for WriteOverflowStringsToDisk {
    fn drop(&mut self) {
        debug_assert!(
            self.offset == 0,
            "overflow string writer must be flushed before drop"
        );
    }
}
