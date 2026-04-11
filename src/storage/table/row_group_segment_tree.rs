use parking_lot::Mutex;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use crate::common::serializer::BinaryMetadataDeserializer;
use crate::storage::metadata::{BlockReaderType, MetaBlockPointer, MetadataReader};

use super::persistent_table_data::PersistentTableData;
use super::row_group::RowGroup;
use super::row_group_collection::RowGroupCollection;
use super::segment_tree::{LoadedSegment, SegmentTree};
use super::types::Idx;

struct LazyLoadState {
    collection: Weak<RowGroupCollection>,
    current_row_group: AtomicU64,
    max_row_group: AtomicU64,
    next_pointer: Mutex<Option<MetaBlockPointer>>,
    root_pointer: Mutex<MetaBlockPointer>,
}

pub struct RowGroupSegmentTree {
    inner: SegmentTree<RowGroup>,
    state: Arc<LazyLoadState>,
}

impl RowGroupSegmentTree {
    pub fn new(base_row_id: Idx) -> Self {
        let state = Arc::new(LazyLoadState {
            collection: Weak::new(),
            current_row_group: AtomicU64::new(0),
            max_row_group: AtomicU64::new(0),
            next_pointer: Mutex::new(None),
            root_pointer: Mutex::new(MetaBlockPointer::default()),
        });
        Self {
            inner: SegmentTree::new(base_row_id),
            state,
        }
    }

    pub fn with_lazy_loader(collection: &Arc<RowGroupCollection>, base_row_id: Idx) -> Self {
        let state = Arc::new(LazyLoadState {
            collection: Arc::downgrade(collection),
            current_row_group: AtomicU64::new(0),
            max_row_group: AtomicU64::new(0),
            next_pointer: Mutex::new(None),
            root_pointer: Mutex::new(MetaBlockPointer::default()),
        });
        let load_state = state.clone();
        Self {
            inner: SegmentTree::with_loader(base_row_id, move || load_segment(&load_state)),
            state,
        }
    }

    pub fn initialize(&self, data: &PersistentTableData) {
        self.state
            .current_row_group
            .store(0, Ordering::Relaxed);
        self.state
            .max_row_group
            .store(data.row_group_count, Ordering::Relaxed);
        *self.state.next_pointer.lock() = Some(data.block_pointer);
        *self.state.root_pointer.lock() = data.block_pointer;
    }

    pub fn get_root_pointer(&self) -> MetaBlockPointer {
        *self.state.root_pointer.lock()
    }
}

impl Deref for RowGroupSegmentTree {
    type Target = SegmentTree<RowGroup>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

fn load_segment(state: &Arc<LazyLoadState>) -> Option<LoadedSegment<RowGroup>> {
    let current = state.current_row_group.load(Ordering::Relaxed);
    let max = state.max_row_group.load(Ordering::Relaxed);
    if current >= max {
        return None;
    }

    let collection = state.collection.upgrade()?;
    let next_pointer = (*state.next_pointer.lock())?;
    let metadata_manager = collection.info.get_io_manager().get_metadata_manager();
    let mut reader = MetadataReader::new(
        &metadata_manager,
        next_pointer,
        None,
        BlockReaderType::RegisterBlocks,
    );
    let mut deserializer = BinaryMetadataDeserializer::new(&mut reader);
    let pointer = deserializer.read_row_group_pointer().ok()?;
    *state.next_pointer.lock() = Some(reader.get_meta_block_pointer());
    state.current_row_group.fetch_add(1, Ordering::Relaxed);

    let row_start = pointer.row_start;
    let row_group = RowGroup::from_pointer(Arc::downgrade(&collection), pointer, collection.types.len());
    Some(LoadedSegment {
        row_start,
        segment: row_group,
    })
}
