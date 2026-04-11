use crate::storage::buffer::{BlockAllocator, BlockHandle, BlockManager, BufferManager, BufferPool, FileBuffer};
use crate::storage::metadata::MetadataManager;
use crate::storage::standard_buffer_manager::StandardBufferManager;
use crate::storage::standard_file_system::LocalFileSystem;
use crate::storage::table::types::{Idx, ROW_GROUP_SIZE};
use std::any::Any;
use std::sync::Arc;

pub trait TableIOManager: Send + Sync {
    fn get_index_block_manager(&self) -> Arc<dyn BlockManager>;
    fn get_block_manager_for_row_data(&self) -> Arc<dyn BlockManager>;
    fn get_metadata_manager(&self) -> Arc<MetadataManager>;
    fn get_row_group_size(&self) -> Idx;
}

pub struct SingleFileTableIOManager {
    index_block_manager: Arc<dyn BlockManager>,
    block_manager: Arc<dyn BlockManager>,
    metadata_manager: Arc<MetadataManager>,
    row_group_size: Idx,
}

pub struct InMemoryTableIOManager {
    block_manager: Arc<dyn BlockManager>,
    metadata_manager: Arc<MetadataManager>,
    row_group_size: Idx,
}

struct InMemoryBlockManager {
    buffer_manager: Arc<dyn BufferManager>,
    block_alloc_size: usize,
    block_header_size: usize,
}

struct NoopBlockAllocator;

impl BlockAllocator for NoopBlockAllocator {
    fn flush_all(&self, _extra_memory: usize) {
    }
}

impl SingleFileTableIOManager {
    pub fn new(
        block_manager: Arc<dyn BlockManager>,
        metadata_manager: Arc<MetadataManager>,
    ) -> Arc<Self> {
        Arc::new(Self {
            index_block_manager: block_manager.clone(),
            block_manager,
            metadata_manager,
            row_group_size: ROW_GROUP_SIZE,
        })
    }
}

impl InMemoryTableIOManager {
    pub fn new() -> Arc<Self> {
        let fs = Arc::new(LocalFileSystem);
        let allocator: Arc<dyn BlockAllocator> = Arc::new(NoopBlockAllocator);
        let buffer_pool = BufferPool::new(allocator, 256 * 1024 * 1024, false, 0);
        let buffer_manager = StandardBufferManager::new(buffer_pool, None, fs);
        let block_manager: Arc<dyn BlockManager> = Arc::new(InMemoryBlockManager {
            buffer_manager: buffer_manager.clone() as Arc<dyn BufferManager>,
            block_alloc_size: 256 * 1024,
            block_header_size: 8,
        });
        buffer_manager.set_block_manager(block_manager.clone());
        let metadata_manager = Arc::new(MetadataManager::new(
            block_manager.clone(),
            block_manager.buffer_manager(),
        ));
        Arc::new(Self {
            block_manager,
            metadata_manager,
            row_group_size: ROW_GROUP_SIZE,
        })
    }
}

impl TableIOManager for SingleFileTableIOManager {
    fn get_index_block_manager(&self) -> Arc<dyn BlockManager> {
        self.index_block_manager.clone()
    }

    fn get_block_manager_for_row_data(&self) -> Arc<dyn BlockManager> {
        self.block_manager.clone()
    }

    fn get_metadata_manager(&self) -> Arc<MetadataManager> {
        self.metadata_manager.clone()
    }

    fn get_row_group_size(&self) -> Idx {
        self.row_group_size
    }
}

impl TableIOManager for InMemoryTableIOManager {
    fn get_index_block_manager(&self) -> Arc<dyn BlockManager> {
        self.block_manager.clone()
    }

    fn get_block_manager_for_row_data(&self) -> Arc<dyn BlockManager> {
        self.block_manager.clone()
    }

    fn get_metadata_manager(&self) -> Arc<MetadataManager> {
        self.metadata_manager.clone()
    }

    fn get_row_group_size(&self) -> Idx {
        self.row_group_size
    }
}

impl BlockManager for InMemoryBlockManager {
    fn buffer_manager(&self) -> Arc<dyn BufferManager> {
        self.buffer_manager.clone()
    }

    fn get_block_alloc_size(&self) -> usize {
        self.block_alloc_size
    }

    fn get_block_header_size(&self) -> usize {
        self.block_header_size
    }

    fn register_block(&self, _block_id: crate::storage::buffer::BlockId) -> Arc<BlockHandle> {
        panic!("Cannot perform IO in in-memory database - RegisterBlock!")
    }

    fn unregister_block(&self, _block_id: crate::storage::buffer::BlockId) {}

    fn block_is_registered(&self, _block_id: crate::storage::buffer::BlockId) -> bool {
        false
    }

    fn create_block(
        &self,
        _block_id: crate::storage::buffer::BlockId,
        _reusable: Option<Box<FileBuffer>>,
    ) -> Box<FileBuffer> {
        panic!("Cannot perform IO in in-memory database - CreateBlock!")
    }

    fn read_block(&self, _block: &mut FileBuffer, _block_id: crate::storage::buffer::BlockId) {
        panic!("Cannot perform IO in in-memory database - Read!")
    }

    fn write_block(&self, _block: &FileBuffer, _block_id: crate::storage::buffer::BlockId) {
        panic!("Cannot perform IO in in-memory database - Write!")
    }

    fn convert_block(
        &self,
        _block_id: crate::storage::buffer::BlockId,
        _source: &FileBuffer,
    ) -> Box<FileBuffer> {
        panic!("Cannot perform IO in in-memory database - ConvertBlock!")
    }

    fn convert_to_persistent(
        &self,
        _block_id: crate::storage::buffer::BlockId,
        _old_block: Arc<BlockHandle>,
    ) -> Arc<BlockHandle> {
        panic!("Cannot perform IO in in-memory database - ConvertToPersistent!")
    }

    fn peek_free_block_id(&self) -> crate::storage::buffer::BlockId {
        panic!("Cannot perform IO in in-memory database - PeekFreeBlockId!")
    }

    fn get_free_block_id_for_checkpoint(&self) -> crate::storage::buffer::BlockId {
        panic!("Cannot perform IO in in-memory database - GetFreeBlockIdForCheckpoint!")
    }

    fn mark_block_as_modified(&self, _block_id: crate::storage::buffer::BlockId) {
        panic!("Cannot perform IO in in-memory database - MarkBlockAsModified!")
    }

    fn block_count(&self) -> u64 {
        panic!("Cannot perform IO in in-memory database - BlockCount!")
    }

    fn metadata_manager(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
}
