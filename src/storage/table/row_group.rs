//! `RowGroup` — one horizontal partition of a table (~122,880 rows).
//!
//! Mirrors `duckdb/storage/table/row_group.hpp`.
//!
//! A table is stored as a sorted list of `RowGroup`s managed by a
//! `RowGroupCollection`.  Each row group owns:
//! - One `ColumnData` per column.
//! - An optional `RowVersionManager` for MVCC delete/insert tracking.
//! - Metadata pointers for lazy-loading columns from disk.
//!
//! # Design notes vs. C++
//!
//! | C++ | Rust | Notes |
//! |-----|------|-------|
//! | `SegmentBase<RowGroup>::count` | `count: AtomicU64` | |
//! | `reference<RowGroupCollection> collection` | `Weak<RowGroupCollection>` | No circular Arc |
//! | `atomic<optional_ptr<RowVersionManager>> version_info` | `AtomicPtr` → `Mutex` | Lazy init |
//! | `shared_ptr<RowVersionManager> owned_version_info` | `Arc<RowVersionManager>` | |
//! | `mutable vector<shared_ptr<ColumnData>> columns` | `Mutex<Vec<Option<Arc<ColumnDataKind>>>>` | Lazy-loaded |
//! | `mutable unique_ptr<atomic<bool>[]> is_loaded` | `Vec<AtomicBool>` | Per-column load flags |
//! | `mutable mutex row_group_lock` | `Mutex<RowGroupInner>` | |

use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use parking_lot::Mutex;

use super::append_state::{ColumnAppendState, RowGroupAppendState};
use super::chunk_info::SelectionVector;
use super::column_checkpoint_state::PartialBlockManager;
use super::column_data::{
    ColumnData, ColumnDataKind, ColumnKindData, PersistentColumnData, PersistentRowGroupData,
};
use super::column_segment::{SegmentStatistics, UnifiedVectorFormat};
use super::row_version_manager::RowVersionManager;
use super::scan_state::{CollectionScanState, ScanFilterInfo};
use super::segment_base::SegmentBase;
use super::table_statistics::TableStatistics;
use super::types::{
    CompressionType, Idx, LogicalType, MetaBlockPointer, PhysicalType, RowId, STANDARD_VECTOR_SIZE,
    TransactionData, TransactionId,
};
use crate::common::types::{DataChunk, Vector};
use crate::common::serializer::{
    BinaryMetadataDeserializer, MESSAGE_TERMINATOR_FIELD_ID,
};
use crate::storage::table::column_segment::ColumnSegment;

// ─────────────────────────────────────────────────────────────────────────────
// RowGroupWriteInfo
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration passed to `RowGroup::write_to_disk`.
///
/// Mirrors `struct RowGroupWriteInfo`.
pub struct RowGroupWriteInfo {
    pub compression_types: Vec<CompressionType>,
    /// Global partial block manager for packing multiple column segments into
    /// one physical block to improve space efficiency.
    /// Mirrors `PartialBlockManager &manager` in C++.
    pub partial_block_manager: Option<Box<PartialBlockManager>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// RowGroupWriteData
// ─────────────────────────────────────────────────────────────────────────────

/// Result of writing a row group to disk.
///
/// Mirrors `struct RowGroupWriteData`.
pub struct RowGroupWriteData {
    pub result_row_group: Arc<RowGroup>,
    pub reuse_existing_metadata_blocks: bool,
    pub should_checkpoint: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// RowGroupPointer  (on-disk reference)
// ─────────────────────────────────────────────────────────────────────────────

/// On-disk pointer used to lazy-load a `RowGroup`.
#[derive(Debug, Clone)]
pub struct RowGroupPointer {
    pub row_start: Idx,
    pub tuple_count: Idx,
    pub deletes_pointers: Vec<MetaBlockPointer>,
    pub column_pointers: Vec<MetaBlockPointer>,
}

// ─────────────────────────────────────────────────────────────────────────────
// RowGroup
// ─────────────────────────────────────────────────────────────────────────────

/// One horizontal partition of a table, holding up to `ROW_GROUP_SIZE` rows.
///
/// Mirrors `class RowGroup : public SegmentBase<RowGroup>`.
pub struct RowGroup {
    // ── SegmentBase ────────────────────────────────────────────
    count: AtomicU64,

    // ── Back-reference ─────────────────────────────────────────
    /// Non-owning reference to the `RowGroupCollection` that holds this group.
    /// Using `Weak` to avoid a reference cycle.
    collection: Weak<super::row_group_collection::RowGroupCollection>,

    // ── Version info (MVCC) ────────────────────────────────────
    /// Delete/insert visibility data, lazily loaded.
    version_info: Mutex<Option<Arc<RowVersionManager>>>,

    // ── Column data ────────────────────────────────────────────
    /// Columns; `None` means not yet loaded from disk.
    columns: Mutex<Vec<Option<Arc<ColumnData>>>>,

    /// Per-column load flags (parallels `columns`).
    is_loaded: Vec<AtomicBool>,

    // ── Disk layout ────────────────────────────────────────────
    /// Metadata pointers for each column (used when lazy-loading).
    pub column_pointers: Vec<MetaBlockPointer>,

    /// Metadata pointers for the delete info of this row group.
    pub deletes_pointers: Vec<MetaBlockPointer>,

    /// Whether the delete info has been loaded from disk.
    deletes_is_loaded: AtomicBool,

    /// Row id of the first row in this group.
    pub row_start: Idx,

    // ── Misc ───────────────────────────────────────────────────
    pub allocation_size: AtomicU64,
    pub has_changes: AtomicBool,
}

impl RowGroup {
    // ── Constructors ─────────────────────────────────────────

    /// Create a new, empty row group.
    pub fn new(
        collection: Weak<super::row_group_collection::RowGroupCollection>,
        row_start: Idx,
        col_count: usize,
    ) -> Arc<Self> {
        let rg = Arc::new(RowGroup {
            count: AtomicU64::new(0),
            collection,
            version_info: Mutex::new(None),
            columns: Mutex::new(vec![None; col_count]),
            is_loaded: (0..col_count).map(|_| AtomicBool::new(false)).collect(),
            column_pointers: vec![MetaBlockPointer::INVALID; col_count],
            deletes_pointers: Vec::new(),
            deletes_is_loaded: AtomicBool::new(false),
            row_start,
            allocation_size: AtomicU64::new(0),
            has_changes: AtomicBool::new(false),
        });
        rg.initialize_transient_columns();
        rg
    }

    /// Restore a row group from disk using a previously written pointer.
    pub fn from_pointer(
        collection: Weak<super::row_group_collection::RowGroupCollection>,
        pointer: RowGroupPointer,
        col_count: usize,
    ) -> Arc<Self> {
        let rg = Arc::new(RowGroup {
            count: AtomicU64::new(pointer.tuple_count),
            collection,
            version_info: Mutex::new(None),
            columns: Mutex::new(vec![None; col_count]),
            is_loaded: (0..col_count).map(|_| AtomicBool::new(false)).collect(),
            column_pointers: pointer.column_pointers,
            deletes_pointers: pointer.deletes_pointers,
            deletes_is_loaded: AtomicBool::new(false),
            row_start: pointer.row_start,
            allocation_size: AtomicU64::new(0),
            has_changes: AtomicBool::new(false),
        });
        rg
    }

    // ── Basic accessors ───────────────────────────────────────

    pub fn row_start(&self) -> Idx {
        self.row_start
    }

    pub fn row_end(&self) -> Idx {
        self.row_start + self.count()
    }

    pub fn is_persistent(&self) -> bool {
        !self.column_pointers.iter().all(|p| !p.is_valid())
    }

    pub fn has_changes(&self) -> bool {
        self.has_changes.load(Ordering::Relaxed)
    }

    pub fn allocation_size(&self) -> Idx {
        self.allocation_size.load(Ordering::Relaxed)
    }

    // ── Column loading ────────────────────────────────────────

    /// Returns column `col` — loads it lazily from disk if needed.
    pub fn get_column(&self, col: usize) -> Arc<ColumnData> {
        if !self.is_loaded[col].load(Ordering::Acquire) {
            self.load_column(col);
        }
        self.columns.lock()[col].clone().expect("column not loaded")
    }

    fn load_column(&self, col: usize) {
        let Some(collection) = self.collection.upgrade() else {
            return;
        };
        let Some(storage) = collection.info.persistent_storage() else {
            return;
        };
        let logical_type = collection.types[col].clone();
        let pointer = self.column_pointers[col];

        let mut reader = crate::storage::metadata::MetadataReader::new(
            &storage.metadata_manager,
            pointer,
            None,
            crate::storage::metadata::BlockReaderType::RegisterBlocks,
        );
        let mut de = BinaryMetadataDeserializer::new(&mut reader);
        let pointers = read_persistent_column_pointers(&mut de, &logical_type)
            .expect("failed to deserialize persistent column");

        let column = ColumnDataKind::create(
            Arc::clone(&collection.info),
            col as Idx,
            logical_type.clone(),
            super::column_data::ColumnDataType::MainTable,
            false,
        );
        {
            let mut lock = column.ctx.data.lock();
            let mut row_start = 0;
            for pointer in pointers {
                let type_size = logical_type.physical_size() as usize;
                let byte_count = pointer.tuple_count as usize * type_size;

                // Register the block with the BufferPool (idempotent: returns
                // the existing handle if already registered).  Data is NOT read
                // here; it will be loaded lazily on the first pin inside
                // ColumnSegment::initialize_scan → BlockHandle::load().
                let block_handle = storage.block_manager.register_block(pointer.block_id);

                let segment = ColumnSegment::create_persistent_with_handle(
                    logical_type.clone(),
                    pointer.block_id,
                    pointer.offset as Idx, // byte offset into block payload
                    pointer.tuple_count,
                    byte_count as Idx,
                    super::types::CompressionType::Uncompressed,
                    super::column_segment::SegmentStatistics::default(),
                    block_handle,
                );
                let segment = Arc::new(segment);
                column
                    .ctx
                    .data
                    .append_segment(&mut lock, segment, row_start);
                row_start += pointer.tuple_count;
            }
            column.ctx.count.store(row_start, Ordering::Relaxed);
        }
        self.columns.lock()[col] = Some(column);
        self.is_loaded[col].store(true, Ordering::Release);
    }

    fn initialize_transient_columns(&self) {
        if self.is_persistent() {
            return;
        }
        let Some(collection) = self.collection.upgrade() else {
            return;
        };

        let mut columns = self.columns.lock();
        for (idx, logical_type) in collection.types.iter().cloned().enumerate() {
            if columns.get(idx).and_then(|entry| entry.as_ref()).is_some() {
                continue;
            }
            let column = ColumnDataKind::create(
                Arc::clone(&collection.info),
                idx as Idx,
                logical_type,
                super::column_data::ColumnDataType::MainTable,
                false,
            );
            if let Some(slot) = columns.get_mut(idx) {
                *slot = Some(column);
                self.is_loaded[idx].store(true, Ordering::Release);
            }
        }
    }

    // ── Version management ────────────────────────────────────

    /// Returns or creates the `RowVersionManager` for this row group.
    pub fn get_or_create_version_info(&self) -> Arc<RowVersionManager> {
        let mut guard = self.version_info.lock();
        if guard.is_none() {
            let version_info = Arc::new(RowVersionManager::new());
            RowVersionManager::register(&version_info);
            *guard = Some(version_info);
        }
        Arc::clone(guard.as_ref().unwrap())
    }

    /// Returns `None` if version info hasn't been loaded/created yet.
    pub fn get_version_info(&self) -> Option<Arc<RowVersionManager>> {
        self.version_info.lock().clone()
    }

    // ── Scan ─────────────────────────────────────────────────

    /// Initialise `state` to scan this row group from the first vector.
    ///
    /// Mirrors C++ `RowGroup::InitializeScan`.
    ///
    /// Returns `false` if the row group is entirely outside the scan range or
    /// is pruned by the zone map.
    ///
    /// # Differences from C++
    /// - The C++ version takes a `SegmentNode<RowGroup>&`; here we accept a
    ///   bare `node_index: usize` (the row group's index in the segment tree).
    /// - Column-level `InitializeScan` is delegated to `ColumnDataKind::initialize_scan`,
    ///   which is currently a stub (no segment-tree seek).
    /// `node_row_start` is the segment-tree node's row_start (may differ from `self.row_start`
    /// when a row group was merged from local storage via `merge_storage`).
    pub fn initialize_scan(&self, state: &mut CollectionScanState, node_row_start: Idx) -> bool {
        // Zone-map check — prune the whole row group if possible.
        if !self.check_zonemap(&state.filters) {
            return false;
        }

        // Use the segment-tree node's row_start, not self.row_start, because
        // row groups merged from local collections may have an incorrect self.row_start.
        let row_start = node_row_start;
        state.vector_index = 0;
        state.max_row_group_row = if row_start > state.max_row {
            0
        } else {
            // How many rows of this row group fall within [0, max_row)?
            self.count().min(state.max_row - row_start)
        };

        if state.max_row_group_row == 0 {
            return false;
        }

        // Initialise per-column cursors for each projected column.
        //
        // We clone column_ids to avoid borrow-checker conflicts when we
        // also borrow state.column_scans mutably.
        let column_ids: Vec<u64> = state.column_ids.clone();
        for (i, &col_id) in column_ids.iter().enumerate() {
            let col = self.get_column(col_id as usize);
            if let Some(scan) = state.column_scans.get_mut(i) {
                col.initialize_scan(scan);
            }
        }

        true
    }

    /// Initialise `state` to scan this row group starting at `vector_offset` vectors in.
    ///
    /// Mirrors C++ `RowGroup::InitializeScanWithOffset`.
    pub fn initialize_scan_with_offset(
        &self,
        state: &mut CollectionScanState,
        _node_index: usize,
        vector_offset: Idx,
    ) -> bool {
        if !self.check_zonemap(&state.filters) {
            return false;
        }

        let row_start = self.row_start;
        state.vector_index = vector_offset;
        state.max_row_group_row = if row_start > state.max_row {
            0
        } else {
            self.count().min(state.max_row - row_start)
        };

        if state.max_row_group_row == 0 {
            return false;
        }

        let row_number = vector_offset * STANDARD_VECTOR_SIZE;
        let column_ids: Vec<u64> = state.column_ids.clone();
        for (i, &col_id) in column_ids.iter().enumerate() {
            let col = self.get_column(col_id as usize);
            if let Some(scan) = state.column_scans.get_mut(i) {
                col.initialize_scan_with_offset(scan, row_number);
            }
        }

        true
    }

    /// Row-group zone-map check: returns `false` if every filter is provably false for
    /// all rows in this group (i.e. the group can be skipped entirely).
    ///
    /// Mirrors C++ `RowGroup::CheckZonemap`.
    ///
    /// For each filter entry the C++ calls `column.CheckZonemap(filter)`, which
    /// compares the filter predicate against the column's min/max segment statistics.
    /// If any filter is `FILTER_ALWAYS_FALSE` the row group is pruned.
    ///
    /// The column-level `CheckZonemap` API is not yet implemented in Rust
    /// (`ColumnData` does not expose a `check_zonemap` method).  Until it is,
    /// this conservatively returns `true` (never prunes).
    pub fn check_zonemap(&self, filters: &ScanFilterInfo) -> bool {
        if !filters.has_filters() {
            // No filters at all — trivially include this row group.
            return true;
        }

        // When ColumnData::check_zonemap(filter, col_stats) is available, iterate
        // each filter entry here and return false if the filter is always false
        // for this row group's min/max statistics.
        //
        //   for each (col_idx, filter) in filters.entries() {
        //       let col = self.get_column(col_idx);
        //       if col.check_zonemap(&filter) == FilterPropagateResult::FilterAlwaysFalse {
        //           return false;
        //       }
        //   }
        //
        // Conservative fallback: include all row groups.
        true
    }

    /// Advance past the current vector in `state` without producing output.
    ///
    /// Mirrors C++ `RowGroup::NextVector`.
    fn next_vector(&self, state: &mut CollectionScanState) {
        state.vector_index += 1;
        let column_ids: Vec<u64> = state.column_ids.clone();
        for (i, &col_id) in column_ids.iter().enumerate() {
            let col = self.get_column(col_id as usize);
            if let Some(scan) = state.column_scans.get_mut(i) {
                col.skip(scan);
            }
        }
    }

    /// Scan the next output vector from this row group into `result`.
    ///
    /// Mirrors C++ `RowGroup::Scan` → `TemplatedScan<TABLE_SCAN_REGULAR>`.
    ///
    /// # Algorithm
    ///
    /// ```text
    /// loop:
    ///   if vector_index * VECTOR_SIZE >= max_row_group_row → return (done)
    ///   max_count = min(VECTOR_SIZE, max_row_group_row - current_row)
    ///   count = get_sel_vector(transaction, vector_index, valid_sel, max_count)
    ///   if count == 0 → NextVector; continue
    ///   for each column:
    ///     col.scan(transaction, vector_index, scan_state, result.data[i])
    ///   apply MVCC selection vector when count < max_count
    ///   result.set_cardinality(count)
    ///   vector_index++
    ///   return
    /// ```
    pub fn scan(
        &self,
        transaction: TransactionData,
        state: &mut CollectionScanState,
        result: &mut DataChunk,
    ) {
        loop {
            // ── Exhaustion check ──────────────────────────────────────────────
            let current_row = state.vector_index * STANDARD_VECTOR_SIZE;
            if current_row >= state.max_row_group_row {
                return;
            }

            let max_count = STANDARD_VECTOR_SIZE.min(state.max_row_group_row - current_row);

            // ── MVCC visibility: build selection vector ───────────────────────
            let count = match self.get_version_info() {
                Some(vi) => {
                    state.valid_sel.sel.clear();
                    vi.get_sel_vector(
                        transaction,
                        state.vector_index,
                        &mut state.valid_sel,
                        max_count,
                    )
                }
                None => {
                    // No version info → all rows visible.
                    state.valid_sel.sel.clear();
                    for i in 0..max_count {
                        state.valid_sel.sel.push(i as u32);
                    }
                    max_count
                }
            };

            if count == 0 {
                self.next_vector(state);
                continue;
            }

            // ── Column scan ───────────────────────────────────────────────────
            let has_filters = state.filters.has_filters();
            let column_ids: Vec<u64> = state.column_ids.clone();

            if count == max_count && !has_filters {
                // Fast path: no deletions, no filters — scan the full vector.
                for (i, &col_id) in column_ids.iter().enumerate() {
                    let col = self.get_column(col_id as usize);
                    if let (Some(scan), Some(vec)) =
                        (state.column_scans.get_mut(i), result.data.get_mut(i))
                    {
                        col.scan(transaction, state.vector_index, scan, vec);
                    }
                }
            } else {
                // Slow path: MVCC deletions or predicate filters.
                //
                // 1. Scan the full vector for each column.
                // 2. If some rows are invisible (count < max_count), apply the MVCC
                //    selection vector via Vector::slice so the output contains only
                //    visible rows.
                // 3. Per-column predicate filtering (ColumnData::Filter / Select)
                //    is not yet implemented; a full implementation would push each
                //    TableFilter down into the column scan here.
                for (i, &col_id) in column_ids.iter().enumerate() {
                    let col = self.get_column(col_id as usize);
                    if let (Some(scan), Some(vec)) =
                        (state.column_scans.get_mut(i), result.data.get_mut(i))
                    {
                        col.scan(transaction, state.vector_index, scan, vec);

                        // Apply the MVCC visibility selection: compact out invisible rows.
                        if count < max_count {
                            // Convert chunk_info::SelectionVector → common::types::SelectionVector
                            let sel = crate::common::types::SelectionVector {
                                indices: state.valid_sel.sel[..count as usize].to_vec(),
                            };
                            vec.slice(&sel, count as usize);
                        }
                    }
                }
            }

            result.set_cardinality(count as usize);
            state.vector_index += 1;
            return;
        }
    }

    /// Returns column `col` if it is already loaded, `None` otherwise.
    ///
    /// Unlike `get_column`, this does **not** trigger a disk load, so it is
    /// safe to call without a working `load_column` implementation.
    fn try_get_column(&self, col: usize) -> Option<Arc<ColumnData>> {
        if self.is_loaded.get(col)?.load(Ordering::Acquire) {
            self.columns.lock().get(col)?.clone()
        } else {
            None
        }
    }

    // ── Append ───────────────────────────────────────────────

    /// Number of columns in this row group.
    ///
    /// `is_loaded` is always sized to `col_count` at construction time and
    /// never resized, so its length is a cheap way to query the column count
    /// without locking `columns`.
    #[inline]
    pub fn get_column_count(&self) -> usize {
        self.is_loaded.len()
    }

    /// Initialise `state` for appending rows into this row group.
    ///
    /// Mirrors C++ `RowGroup::InitializeAppend`.
    ///
    /// Sets:
    /// - `state.offset_in_row_group` = current committed row count (new rows
    ///   are written after existing ones).
    /// - `state.states` = one freshly-initialised `ColumnAppendState` per
    ///   column, obtained by calling `ColumnData::InitializeAppend` on each
    ///   column.
    ///
    /// # Note on `state.row_group_index`
    /// C++ stores `append_state.row_group = this` (a raw pointer).  In Rust
    /// the equivalent index is set by the caller in `RowGroupCollection::append`
    /// *before* calling this method, so we do not touch it here.
    pub fn initialize_append(&self, state: &mut RowGroupAppendState) {
        // Rows appended go after whatever is already committed.
        state.offset_in_row_group = self.count();

        let col_count = self.get_column_count();

        // Resize (or truncate) the per-column cursor vector to match.
        state
            .states
            .resize_with(col_count, ColumnAppendState::default);

        // Initialise each column's append cursor.
        //
        // `try_get_column` is used instead of `get_column` so that columns
        // that have not yet been created/loaded (e.g. on a brand-new row
        // group) leave their `ColumnAppendState` in the default state rather
        // than triggering a `todo!()` panic in `load_column`.  When column
        // creation is fully implemented they will always be present here.
        for i in 0..col_count {
            if let Some(col) = self.try_get_column(i) {
                col.initialize_append(&mut state.states[i]);
            }
        }
    }

    /// Append `append_count` rows from `chunk` into this row group.
    ///
    /// Mirrors C++ `RowGroup::Append`.
    ///
    /// For each column:
    /// 1. Calls `ColumnData::Append` to write the values, updating per-segment stats.
    /// 2. Merges the per-append `SegmentStatistics` into the collection-level
    ///    `TableStatistics` (C++: `collection.GetStats().GetStats(i).lock()`).
    /// 3. Accumulates the change in `ColumnData::allocation_size` into
    ///    `self.allocation_size`.
    ///
    /// Advances `state.offset_in_row_group` by `append_count` at the end.
    pub fn append(&self, state: &mut RowGroupAppendState, chunk: &DataChunk, append_count: Idx) {
        debug_assert_eq!(chunk.column_count(), self.get_column_count());

        let collection = self.collection.upgrade();
        let col_count = self.get_column_count();

        for i in 0..col_count {
            if let Some(col) = self.try_get_column(i) {
                let prev_alloc = col.allocation_size();
                // Convert the flat Vector to a UnifiedVectorFormat view.
                // C++: chunk.data[i].ToUnifiedFormat(append_count, vdata)
                let vdata = UnifiedVectorFormat::from_flat_vector(&chunk.data[i]);

                // Per-column segment statistics, updated by append.
                // C++: column.Append(*collection.GetStats().GetStats(i).lock(), ...)
                let mut seg_stats = SegmentStatistics::default();
                col.append(&mut seg_stats, &mut state.states[i], &vdata, append_count);

                // Wire per-append SegmentStatistics into the collection-level TableStatistics.
                // Mirrors C++: collection.GetStats().GetStats(i).lock() passed directly to Append.
                if let Some(ref coll) = collection {
                    use crate::storage::statistics::ColumnStatistics;
                    coll.stats.with_stats_mut(i, |cs| {
                        let col_stats = ColumnStatistics::new(seg_stats.statistics().clone());
                        cs.merge(&col_stats);
                    });
                }

                let delta = col.allocation_size().saturating_sub(prev_alloc);
                self.allocation_size.fetch_add(delta, Ordering::Relaxed);
            }
        }

        state.offset_in_row_group += append_count;
        self.count.fetch_add(append_count, Ordering::SeqCst);
    }

    pub fn append_version_info(&self, transaction: TransactionData, count: Idx) {
        let row_group_end = self.row_end();
        let row_group_start = row_group_end.saturating_sub(count);
        self.get_or_create_version_info().append_version_info(
            transaction,
            count,
            row_group_start,
            row_group_end,
        );
        self.has_changes.store(true, Ordering::Relaxed);
    }

    pub fn commit_append(&self, commit_id: TransactionId, start: Idx, count: Idx) {
        if let Some(vi) = self.get_version_info() {
            vi.commit_append(commit_id, start, count);
        }
    }

    pub fn revert_append(&self, new_count: Idx) {
        self.count
            .store(new_count - self.row_start, Ordering::SeqCst);
        if let Some(vi) = self.get_version_info() {
            vi.revert_append(new_count - self.row_start);
        }
    }

    // ── Delete ────────────────────────────────────────────────

    /// Delete rows identified by `row_ids[..count]` from this row group.
    ///
    /// Mirrors C++ `RowGroup::Delete` / `VersionDeleteState`.
    ///
    /// Row IDs are absolute (relative to the table start); they are converted to
    /// row-group-relative offsets before being passed to `RowVersionManager::delete_rows`.
    ///
    /// The C++ `VersionDeleteState` groups incoming row IDs by vector index
    /// (flushing each batch when the vector changes), then calls
    /// `GetOrCreateVersionInfo().DeleteRows(vector_idx, transaction_id, rows, count)`.
    /// This Rust implementation mirrors that logic exactly.
    ///
    /// Returns the number of rows actually deleted (may be less than `count` if
    /// some rows were already deleted by an earlier transaction).
    pub fn delete(
        &self,
        transaction_handle: Option<&Arc<crate::transaction::duck_transaction_manager::DuckTxnHandle>>,
        transaction: TransactionData,
        table: Option<&crate::storage::data_table::DataTable>,
        row_ids: &mut [RowId],
        count: Idx,
        row_group_start: Idx,
    ) -> Idx {
        let vi = self.get_or_create_version_info();

        let mut total_deleted: Idx = 0;
        // Sentinel: Idx::MAX means "no current chunk yet".
        let mut current_vector_idx: Idx = Idx::MAX;
        // Rows belonging to the current vector, as row-group-relative indices
        // within that vector (i.e. values in [0, STANDARD_VECTOR_SIZE)).
        let mut chunk_rows: Vec<RowId> = Vec::with_capacity(STANDARD_VECTOR_SIZE as usize);

        for i in 0..(count as usize) {
            let abs_id = row_ids[i];
            debug_assert!(
                abs_id as Idx >= row_group_start,
                "row_id {} < row_group_start {}",
                abs_id,
                row_group_start
            );
            // Convert to row-group-relative offset.
            let rel_id = (abs_id as Idx) - row_group_start;
            let vector_idx = rel_id / STANDARD_VECTOR_SIZE;
            let idx_in_vector = (rel_id % STANDARD_VECTOR_SIZE) as RowId;

            if vector_idx != current_vector_idx {
                // Flush the current batch before switching to a new vector.
                if current_vector_idx != Idx::MAX && !chunk_rows.is_empty() {
                    let n = chunk_rows.len() as Idx;
                    let deleted = vi.delete_rows(
                        current_vector_idx,
                        transaction.transaction_id,
                        &mut chunk_rows,
                        n,
                    );
                    total_deleted += deleted;
                    if deleted > 0 {
                        push_delete_undo(
                            transaction_handle,
                            table,
                            &vi,
                            current_vector_idx,
                            &chunk_rows,
                            deleted,
                            row_group_start + current_vector_idx * STANDARD_VECTOR_SIZE,
                        );
                    }
                    chunk_rows.clear();
                }
                current_vector_idx = vector_idx;
            }
            chunk_rows.push(idx_in_vector);
        }

        // Flush the final batch.
        if current_vector_idx != Idx::MAX && !chunk_rows.is_empty() {
            let n = chunk_rows.len() as Idx;
            let deleted = vi.delete_rows(
                current_vector_idx,
                transaction.transaction_id,
                &mut chunk_rows,
                n,
            );
            total_deleted += deleted;
            if deleted > 0 {
                push_delete_undo(
                    transaction_handle,
                    table,
                    &vi,
                    current_vector_idx,
                    &chunk_rows,
                    deleted,
                    row_group_start + current_vector_idx * STANDARD_VECTOR_SIZE,
                );
            }
        }

        self.has_changes.store(true, Ordering::Relaxed);
        total_deleted
    }

    // ── Statistics ───────────────────────────────────────────

    pub fn merge_into_statistics(&self, stats: &TableStatistics) {
        let col_count = self.get_column_count();
        for i in 0..col_count {
            if let Some(col) = self.try_get_column(i) {
                col.merge_into_statistics(stats);
            }
        }
    }

    // ── Persistence ──────────────────────────────────────────

    /// Write this row group to disk and return a new `RowGroup` with the
    /// checkpointed (persistent) column data.
    ///
    /// Mirrors C++ `RowGroup::WriteToDisk(RowGroupWriteInfo &info) const`.
    ///
    /// # C++ algorithm
    ///
    /// For each column (column-at-a-time to co-locate data on disk):
    ///   1. Call `column.Checkpoint(row_group, ColumnCheckpointInfo(info, col_idx))`
    ///      which writes transient in-memory segments to disk blocks via
    ///      `PartialBlockManager` and returns a `ColumnCheckpointState`.
    ///   2. `GetFinalResult()` returns the new persistent `ColumnData`.
    ///   3. Merge checkpoint statistics into the column's running statistics.
    ///
    /// Then create a new `RowGroup` whose `columns` are the checkpointed versions
    /// and whose `version_info` is shared with the original.
    ///
    /// # Current Rust implementation
    ///
    /// `ColumnData::checkpoint()` is not yet implemented (the `PartialBlockManager`
    /// is a stub).  Until it is, this implementation:
    /// - Reuses existing metadata when `!has_changes && is_persistent()` (fast path).
    /// - Otherwise, Arc-clones every column (all data remains in memory) and builds
    ///   a fresh `RowGroup` with `has_changes = false` and `should_checkpoint = true`.
    ///   The caller (`OptimisticDataWriter::flush_to_disk`) can interpret
    ///   `should_checkpoint = true` to schedule a real flush later.
    pub fn write_to_disk(&self, info: &RowGroupWriteInfo) -> RowGroupWriteData {
        let col_count = self.get_column_count();

        // Fast path: no changes and already persistent — reuse existing metadata.
        // Mirrors C++ `WriteToDisk(RowGroupWriter&)` early-out when
        // `ExperimentalMetadataReuseSetting` is set and `!HasChanges()`.
        if !self.has_changes() && self.is_persistent() {
            return RowGroupWriteData {
                result_row_group: self.make_shallow_copy(),
                reuse_existing_metadata_blocks: true,
                should_checkpoint: false,
            };
        }

        // Validate that caller provided one compression type per column.
        assert_eq!(
            col_count,
            info.compression_types.len(),
            "write_to_disk: column count ({col_count}) != compression type count ({})",
            info.compression_types.len()
        );

        // Checkpoint each column.
        //
        // Full implementation: call `col.checkpoint(self, ColumnCheckpointInfo { info, col_idx })`
        // which writes transient segments to disk blocks via `info.partial_block_manager`
        // and returns a new persistent `ColumnData`.
        //
        // Current implementation: Arc-clone the existing column.  All data is already
        // in memory; the clone is O(1) reference-count increment.
        let result_columns: Vec<Option<Arc<ColumnData>>> = (0..col_count)
            .map(|col_idx| Some(Arc::clone(&self.get_column(col_idx))))
            .collect();

        // Build the result row group with checkpointed columns and shared version info.
        // C++: `result_row_group->columns = std::move(result_columns);`
        //      `result_row_group->version_info = row_group.version_info.load();`
        let is_loaded: Vec<AtomicBool> = (0..col_count).map(|_| AtomicBool::new(true)).collect();

        let result_rg = Arc::new(RowGroup {
            count: AtomicU64::new(self.count()),
            collection: self.collection.clone(),
            version_info: Mutex::new(self.get_version_info()),
            columns: Mutex::new(result_columns),
            is_loaded,
            column_pointers: self.column_pointers.clone(),
            deletes_pointers: self.deletes_pointers.clone(),
            deletes_is_loaded: AtomicBool::new(self.deletes_is_loaded.load(Ordering::Relaxed)),
            row_start: self.row_start,
            allocation_size: AtomicU64::new(self.allocation_size()),
            has_changes: AtomicBool::new(false),
        });

        RowGroupWriteData {
            result_row_group: result_rg,
            reuse_existing_metadata_blocks: false,
            should_checkpoint: true,
        }
    }

    /// Serialise this row group's storage metadata into a `PersistentRowGroupData`.
    ///
    /// Mirrors C++ `RowGroup::SerializeRowGroupInfo(idx_t row_group_start) const`.
    ///
    /// C++ iterates `columns`, calls `col->Serialize()` on each (which recursively
    /// serialises child columns and collects `DataPointer`s for each segment), and
    /// stores the result alongside `row_group_start` and `count`.
    pub fn serialize_row_group_info(&self, row_group_start: Idx) -> PersistentRowGroupData {
        let types = self
            .collection
            .upgrade()
            .map(|c| c.types.clone())
            .unwrap_or_default();

        let col_count = self.get_column_count();
        let mut column_data = Vec::with_capacity(col_count);

        for col_idx in 0..col_count {
            let col = self.get_column(col_idx);
            let logical_type = types
                .get(col_idx)
                .cloned()
                .unwrap_or_else(LogicalType::integer);
            column_data.push(serialize_column_to_persistent(&col, &logical_type));
        }

        PersistentRowGroupData {
            types,
            column_data,
            start: row_group_start,
            count: self.count(),
        }
    }

    // ── Internal helpers ──────────────────────────────────────

    /// Create a shallow copy of this row group.
    ///
    /// All columns and version info are Arc-cloned (O(1)).  Used by
    /// `write_to_disk` when metadata can be reused unchanged.
    fn make_shallow_copy(&self) -> Arc<RowGroup> {
        let col_count = self.get_column_count();
        let columns: Vec<Option<Arc<ColumnData>>> =
            (0..col_count).map(|i| self.try_get_column(i)).collect();
        let is_loaded: Vec<AtomicBool> = self
            .is_loaded
            .iter()
            .map(|b| AtomicBool::new(b.load(Ordering::Relaxed)))
            .collect();
        Arc::new(RowGroup {
            count: AtomicU64::new(self.count()),
            collection: self.collection.clone(),
            version_info: Mutex::new(self.get_version_info()),
            columns: Mutex::new(columns),
            is_loaded,
            column_pointers: self.column_pointers.clone(),
            deletes_pointers: self.deletes_pointers.clone(),
            deletes_is_loaded: AtomicBool::new(self.deletes_is_loaded.load(Ordering::Relaxed)),
            row_start: self.row_start,
            allocation_size: AtomicU64::new(self.allocation_size()),
            has_changes: AtomicBool::new(self.has_changes()),
        })
    }
}

fn push_delete_undo(
    transaction_handle: Option<&Arc<crate::transaction::duck_transaction_manager::DuckTxnHandle>>,
    table: Option<&crate::storage::data_table::DataTable>,
    version_info: &Arc<RowVersionManager>,
    vector_idx: Idx,
    rows: &[RowId],
    actual_delete_count: Idx,
    base_row: Idx,
) {
    let (Some(handle), Some(table)) = (transaction_handle, table) else {
        return;
    };
    let rows = &rows[..actual_delete_count as usize];
    let is_consecutive = rows
        .iter()
        .enumerate()
        .all(|(idx, row)| *row == idx as RowId);
    let delete_info = crate::transaction::delete_info::DeleteInfo {
        table_id: table.info.table_id(),
        version_info_id: version_info.version_info_id,
        vector_idx,
        count: actual_delete_count,
        base_row,
        is_consecutive,
        rows: if is_consecutive {
            None
        } else {
            Some(rows.iter().map(|row| *row as u16).collect())
        },
    };
    handle.lock_inner().push_delete(delete_info);
}

// ─────────────────────────────────────────────────────────────────────────────
// Serialisation helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Map a `LogicalTypeId` to its physical storage representation.
///
/// Mirrors the `LogicalType::GetInternalType()` mapping in C++.
fn logical_type_id_to_physical(id: crate::common::types::LogicalTypeId) -> PhysicalType {
    use crate::common::types::LogicalTypeId::*;
    match id {
        Boolean => PhysicalType::Bool,
        TinyInt => PhysicalType::Int8,
        SmallInt => PhysicalType::Int16,
        Integer => PhysicalType::Int32,
        // BigInt (i64), Time (μs as i64), Timestamp (μs as i64)
        BigInt | Time | Timestamp => PhysicalType::Int64,
        // Date is stored as int32 days since epoch (DuckDB date_t / PhysicalType::INT32).
        Date => PhysicalType::Int32,
        HugeInt => PhysicalType::Int128,
        Float => PhysicalType::Float,
        Double => PhysicalType::Double,
        Varchar => PhysicalType::VarChar,
        List => PhysicalType::List,
        Struct | Map => PhysicalType::Struct,
        // Validity bitmask columns are stored as arrays of uint8.
        Validity => PhysicalType::Uint8,
        _ => PhysicalType::Invalid,
    }
}

/// Recursively serialise a `ColumnData` into a `PersistentColumnData`.
///
/// Mirrors C++ `ColumnData::Serialize()`:
/// - Collects `DataPointer`s from all segments via `get_data_pointers()`.
/// - Recursively serialises child columns (validity, list/array/struct children).
fn serialize_column_to_persistent(
    col: &ColumnData,
    logical_type: &LogicalType,
) -> PersistentColumnData {
    let physical_type = logical_type_id_to_physical(logical_type.id);
    let logical_type_id = logical_type.id;
    let has_updates = col.has_updates();
    let pointers = col.ctx.get_data_pointers();

    // Build validity-column logical type for child serialisation.
    let validity_type = LogicalType::boolean(); // validity columns stored as bool bitmask

    // Recursively serialise child columns.
    // C++ `ColumnData::Serialize()` recurses into `sub_columns` / children.
    let child_columns: Vec<PersistentColumnData> = match &col.kind {
        ColumnKindData::Standard { validity } => {
            // Standard scalar column: one validity child.
            validity
                .as_ref()
                .map(|v| vec![serialize_column_to_persistent(v, &validity_type)])
                .unwrap_or_default()
        }
        ColumnKindData::Validity => {
            // Validity columns are leaf nodes — no children.
            vec![]
        }
        ColumnKindData::List {
            validity,
            child_column,
        } => {
            // List<T>: [validity, child_data]
            vec![
                serialize_column_to_persistent(validity, &validity_type),
                serialize_column_to_persistent(child_column, logical_type),
            ]
        }
        ColumnKindData::Array {
            validity,
            child_column,
            ..
        } => {
            // Array<T, N>: [validity, child_data]
            vec![
                serialize_column_to_persistent(validity, &validity_type),
                serialize_column_to_persistent(child_column, logical_type),
            ]
        }
        ColumnKindData::Struct {
            validity,
            sub_columns,
        } => {
            // Struct(a T1, b T2, …): [validity, field_0, field_1, …]
            let mut children = vec![serialize_column_to_persistent(validity, &validity_type)];
            children.extend(
                sub_columns
                    .iter()
                    .map(|c| serialize_column_to_persistent(c, logical_type)),
            );
            children
        }
        ColumnKindData::Variant {
            validity,
            sub_columns,
        } => {
            // Variant / semi-structured: [validity?, unshredded, shredded?]
            let mut children = validity
                .as_ref()
                .map(|v| vec![serialize_column_to_persistent(v, &validity_type)])
                .unwrap_or_default();
            children.extend(
                sub_columns
                    .iter()
                    .map(|c| serialize_column_to_persistent(c, logical_type)),
            );
            children
        }
    };

    PersistentColumnData {
        physical_type,
        logical_type_id,
        has_updates,
        pointers,
        child_columns,
        variant_shredded_type: None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Metadata deserialisation (disk → RowGroup)
// ─────────────────────────────────────────────────────────────────────────────

fn read_persistent_column_pointers(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &super::types::LogicalType,
) -> std::io::Result<Vec<super::types::DataPointer>> {
    let mut pointers = Vec::new();
    loop {
        match de.next_field()? {
            100 => {
                let count = de.read_list_len()?;
                pointers.reserve(count);
                for _ in 0..count {
                    pointers.push(de.read_data_pointer(logical_type)?);
                }
            }
            101 => skip_persistent_column(de, &super::types::LogicalType::boolean())?,
            102 => skip_nested_object(de)?,
            115 => skip_nested_object(de)?,
            120 => skip_persistent_column(de, &super::types::LogicalType::boolean())?,
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(pointers),
            _ => skip_nested_object(de)?,
        }
    }
}

fn skip_persistent_column(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &super::types::LogicalType,
) -> std::io::Result<()> {
    loop {
        match de.next_field()? {
            100 => {
                let count = de.read_list_len()?;
                for _ in 0..count {
                    let _ = de.read_data_pointer(logical_type)?;
                }
            }
            101 => skip_persistent_column(de, &super::types::LogicalType::boolean())?,
            102 => skip_nested_object(de)?,
            115 => skip_nested_object(de)?,
            120 => skip_persistent_column(de, &super::types::LogicalType::boolean())?,
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            _ => skip_nested_object(de)?,
        }
    }
}

fn skip_nested_object(de: &mut BinaryMetadataDeserializer<'_>) -> std::io::Result<()> {
    let mut depth = 1usize;
    while depth > 0 {
        match de.next_field()? {
            MESSAGE_TERMINATOR_FIELD_ID => depth -= 1,
            _ => {
                let _ = de.read_varint()?;
            }
        }
    }
    Ok(())
}

impl SegmentBase for RowGroup {
    fn count_atomic(&self) -> &AtomicU64 {
        &self.count
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataChunkPlaceholder
// ─────────────────────────────────────────────────────────────────────────────

/// Minimal placeholder for DuckDB's `DataChunk`.
/// Full implementation lives in the execution layer.
#[derive(Debug, Default)]
pub struct DataChunkPlaceholder {
    pub column_count: usize,
    pub row_count: Idx,
}
