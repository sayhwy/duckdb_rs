use std::collections::HashMap;
use std::sync::Arc;

use crate::storage::buffer::{BlockHandle, BlockId, BlockManager};

pub trait CompressedSegmentState: Send + Sync {
    fn get_segment_info(&self) -> String;
}

pub trait OverflowStringWriter: Send + Sync {
    fn write_string(
        &mut self,
        state: &mut UncompressedStringSegmentState,
        string: &[u8],
        result_block: &mut BlockId,
        result_offset: &mut i32,
    );

    fn flush(&mut self);
}

pub struct StringBlock {
    pub block: Arc<BlockHandle>,
    pub offset: usize,
    pub size: usize,
    pub next: Option<Box<StringBlock>>,
}

pub struct UncompressedStringSegmentState {
    pub head: Option<Box<StringBlock>>,
    pub overflow_blocks: HashMap<BlockId, usize>,
    pub overflow_writer: Option<Box<dyn OverflowStringWriter>>,
    pub block_manager: Option<Arc<dyn BlockManager>>,
    pub on_disk_blocks: Vec<BlockId>,
    handles: HashMap<BlockId, Arc<BlockHandle>>,
}

impl UncompressedStringSegmentState {
    pub fn new() -> Self {
        Self {
            head: None,
            overflow_blocks: HashMap::new(),
            overflow_writer: None,
            block_manager: None,
            on_disk_blocks: Vec::new(),
            handles: HashMap::new(),
        }
    }

    pub fn get_handle(
        &mut self,
        manager: &Arc<dyn BlockManager>,
        block_id: BlockId,
    ) -> Arc<BlockHandle> {
        if let Some(handle) = self.handles.get(&block_id) {
            return Arc::clone(handle);
        }
        let effective = self
            .block_manager
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::clone(manager));
        let handle = effective.register_block(block_id);
        self.handles.insert(block_id, Arc::clone(&handle));
        handle
    }

    pub fn register_block(&mut self, manager: &Arc<dyn BlockManager>, block_id: BlockId) {
        if self.handles.contains_key(&block_id) {
            panic!(
                "UncompressedStringSegmentState::register_block: block id {} already exists",
                block_id
            );
        }
        let effective = self
            .block_manager
            .as_ref()
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::clone(manager));
        let handle = effective.register_block(block_id);
        self.handles.insert(block_id, handle);
        self.on_disk_blocks.push(block_id);
    }
}

impl Default for UncompressedStringSegmentState {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressedSegmentState for UncompressedStringSegmentState {
    fn get_segment_info(&self) -> String {
        if self.on_disk_blocks.is_empty() {
            return String::new();
        }
        let blocks = self
            .on_disk_blocks
            .iter()
            .map(|block| block.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("Overflow String Block Ids: {}", blocks)
    }
}
