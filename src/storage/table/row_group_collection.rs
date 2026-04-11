//! `RowGroupCollection` — the ordered list of row groups forming one table.
//!
//! Mirrors `duckdb/storage/table/row_group_collection.hpp`.
//!
//! Responsibilities:
//! - Owns the `RowGroupSegmentTree` (a `SegmentTree<RowGroup>`).
//! - Exposes scan / append / delete / update / checkpoint APIs.
//! - Maintains table-level statistics.
//!
//! # Design notes vs. C++
//!
//! | C++ | Rust | Notes |
//! |-----|------|-------|
//! | `BlockManager &` | `Arc<dyn BlockManager>` placeholder | |
//! | `shared_ptr<DataTableInfo> info` | `Arc<DataTableInfo>` | |
//! | `atomic<idx_t> total_rows` | `AtomicU64` | |
//! | `mutable mutex row_group_pointer_lock` | integrated into `Mutex<Arc<SegmentTree>>` | |
//! | `shared_ptr<RowGroupSegmentTree> owned_row_groups` | `Arc<SegmentTree<RowGroup>>` | |
//! | `TableStatistics stats` | `TableStatistics` | |

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::common::errors::StorageResult;
use crate::storage::buffer::BlockManager;
use crate::storage::metadata::MetadataManager;
use crate::storage::storage_info::StorageError;
use super::append_state::TableAppendState;
use super::column_data::{PersistentCollectionData, PersistentRowGroupData};
use super::data_table_info::DataTableInfo;
use super::persistent_table_data::PersistentTableData;
use super::row_group::{RowGroup, RowGroupPointer, RowGroupWriteInfo};
pub use super::row_group_segment_tree::RowGroupSegmentTree;
use super::scan_state::{CollectionScanState, ParallelCollectionScanState, RowGroupSegmentRef};
use super::segment_base::SegmentBase;
use super::table_statistics::TableStatistics;
use super::types::{Idx, LogicalType, MetaBlockPointer, RowId, TransactionData, TransactionId};
use crate::common::types::DataChunk;
use parking_lot::Mutex;

// ─────────────────────────────────────────────────────────────────────────────
// RowGroupCollection
// ─────────────────────────────────────────────────────────────────────────────

/// The primary storage container for a table's row data.
///
/// Mirrors `class RowGroupCollection`.
pub struct RowGroupCollection {
    // ── Identity ───────────────────────────────────────────────
    pub info: Arc<DataTableInfo>,

    /// Column types in schema order.
    pub types: Vec<LogicalType>,

    // ── Sizing ────────────────────────────────────────────────
    /// Maximum rows per row group (default: `ROW_GROUP_SIZE`).
    pub row_group_size: Idx,

    /// Total committed rows across all row groups.
    pub total_rows: AtomicU64,

    // ── Segment tree ──────────────────────────────────────────
    /// The row-group segment tree.  The tree's internal Mutex guards node-list
    /// mutations; this outer Mutex guards pointer *replacement* (used during
    /// schema evolution when the whole tree is swapped out).
    owned_row_groups: Mutex<Arc<RowGroupSegmentTree>>,

    // ── Statistics ────────────────────────────────────────────
    pub stats: TableStatistics,

    // ── Append state ──────────────────────────────────────────
    /// Append lock: held for the duration of any append operation.
    pub append_lock: Mutex<()>,

    /// When `true`, the next append must start a new row group.
    requires_new_row_group: Mutex<bool>,

    /// Total bytes occupied by transient (not-yet-checkpointed) segments.
    pub allocation_size: AtomicU64,

    /// If loaded from disk: root metadata pointer for lazy-loading row groups.
    pub metadata_pointer: Option<MetaBlockPointer>,
}

impl RowGroupCollection {
    // ── Constructors ─────────────────────────────────────────

    /// Create a new, empty collection.
    pub fn new(info: Arc<DataTableInfo>, types: Vec<LogicalType>, row_start: Idx) -> Arc<Self> {
        let tree = Arc::new(RowGroupSegmentTree::new(row_start));
        Arc::new(RowGroupCollection {
            info,
            types,
            row_group_size: super::types::ROW_GROUP_SIZE,
            total_rows: AtomicU64::new(0),
            owned_row_groups: Mutex::new(tree),
            stats: TableStatistics::new(),
            append_lock: Mutex::new(()),
            requires_new_row_group: Mutex::new(false),
            allocation_size: AtomicU64::new(0),
            metadata_pointer: None,
        })
    }

    // ── Initialisation from persistent data ──────────────────

    /// Load collection from previously checkpointed data.
    pub fn initialize(self: &Arc<Self>, data: PersistentCollectionData) {
        todo!("populate owned_row_groups from data.row_group_data")
    }

    pub fn initialize_from_table_data(self: &Arc<Self>, data: &PersistentTableData) {
        self.total_rows.store(data.total_rows, Ordering::Relaxed);
        let tree = Arc::new(RowGroupSegmentTree::with_lazy_loader(self, 0));
        tree.initialize(data);
        *self.owned_row_groups.lock() = tree;
        if data.table_stats.is_empty() {
            self.stats.initialize_empty(&self.types);
        } else {
            self.stats.set_stats(&data.table_stats);
        }
    }

    /// Bootstrap an empty table (no rows, one empty row group).
    pub fn initialize_empty(self: &Arc<Self>) {
        let tree_guard = self.owned_row_groups.lock();
        let tree = Arc::clone(&*tree_guard);
        drop(tree_guard); // release outer lock before locking inner tree
        let mut seg_lock = tree.lock();
        let col_count = self.types.len();
        // Use 0 as the starting row for the first row group, not base_row_id
        // This matches DuckDB's behavior where AppendRowGroup uses the passed start_row
        let rg = RowGroup::new(Arc::downgrade(self), 0, col_count);
        tree.append_segment(&mut seg_lock, rg, 0);
    }

    pub fn set_append_requires_new_row_group(&self) {
        *self.requires_new_row_group.lock() = true;
    }

    pub fn get_block_manager(&self) -> Arc<dyn BlockManager> {
        self.info.get_io_manager().get_block_manager_for_row_data()
    }

    pub fn get_metadata_manager(&self) -> Arc<MetadataManager> {
        self.info.get_io_manager().get_metadata_manager()
    }

    // ── Row-group access ──────────────────────────────────────

    pub fn total_rows(&self) -> Idx {
        self.total_rows.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.total_rows() == 0
    }

    /// Returns the `n`th row group (negative = from end).
    pub fn get_row_group(&self, index: i64) -> Option<Arc<RowGroup>> {
        let tree_guard = self.owned_row_groups.lock();
        let tree = Arc::clone(&*tree_guard);
        drop(tree_guard);
        let mut lock = tree.lock();
        tree.get_segment_by_index(&mut lock, index).map(|n| n.arc())
    }

    // ── Scan ─────────────────────────────────────────────────

    /// Prepare `state` for a sequential scan of this collection.
    ///
    /// Mirrors `RowGroupCollection::InitializeScan(context, CollectionScanState&,
    ///     column_ids, table_filters)`.
    ///
    /// Sets up:
    /// - `state.column_ids`         — projected column indices.
    /// - `state.column_scans`       — one per projected column.
    /// - `state.max_row`            — upper row bound (= total_rows).
    /// - `state.current_row_group`  — first row group to scan.
    ///
    /// Also calls `RowGroup::initialize_scan` on the root row group so that
    /// column scan states are ready before the first call to `scan`.
    pub fn initialize_scan(
        self: &Arc<Self>,
        state: &mut CollectionScanState,
        column_ids: &[u64],
        // table_filters: optional — not yet wired
    ) {
        // C++: state.column_ids = column_ids;
        //      state.Initialize(context, GetTypes());  ← sizes column_scans to column_ids.len()
        state.set_column_ids(column_ids.to_vec());

        // Get the root row group (first segment).
        let tree_guard = self.owned_row_groups.lock();
        let tree = Arc::clone(&*tree_guard);
        drop(tree_guard);
        state.row_groups = Some(Arc::clone(&tree));
        let (root_arc, root_idx, base_row_id, root_row_start) = {
            let mut lock = tree.lock();
            let base = tree.base_row_id();
            match tree.get_root_segment(&mut lock) {
                None => {
                    state.current_row_group = None;
                    // Empty collection.
                    state.max_row = 0;
                    return;
                }
                Some(node) => (node.arc(), node.index(), base, node.row_start()),
            }
        };

        // C++: state.max_row = row_start + GetTotalRows()
        // base_row_id is the collection's starting row offset (may be i64::MAX for local storage).
        state.max_row = base_row_id + self.total_rows();
        state.current_row_group = Some(RowGroupSegmentRef {
            row_start: root_row_start,
            index: root_idx,
            row_group: Arc::clone(&root_arc),
        });
        // C++: row_group->GetNode().InitializeScan(*state, *row_group)
        // Pass the SegmentNode's row_start so merged row groups (with wrong self.row_start) work.
        root_arc.initialize_scan(state, root_row_start);
    }

    /// Scan one output vector from the persistent row-group collection.
    ///
    /// Returns `true` if `result` was filled with ≥ 1 row; `false` when
    /// the scan is fully exhausted.
    ///
    /// Mirrors `CollectionScanState::Scan(DuckTransaction&, DataChunk&)`:
    ///
    /// ```text
    /// while row_group:
    ///   row_group.scan(transaction, state, result)
    ///   if result.size() > 0 → return true
    ///   if max_row ≤ rg_row_start + rg_count → row_group = None; return false
    ///   do:
    ///     row_group = GetNextRowGroup(...)
    ///     if row_group.row_start ≥ max_row → row_group = None; break
    ///     if row_group.InitializeScan(state) → break (found next valid rg)
    ///   while row_group
    /// return false
    /// ```
    pub fn scan(
        &self,
        transaction: TransactionData,
        state: &mut CollectionScanState,
        result: &mut DataChunk,
    ) -> bool {
        state.scan(transaction, result)
    }

    pub fn initialize_parallel_scan(&self, state: &mut ParallelCollectionScanState) {
        state.row_groups = Some(self.get_row_groups());
        state.current_row_group = None;
        state.next_row_group_index = 0;
        state.max_row = self.total_rows();
    }

    // ── Append ───────────────────────────────────────────────

    /// Prepare an append state for writing rows to this collection.
    pub fn initialize_append(self: &Arc<Self>, state: &mut TableAppendState) {
        state.row_start = self.total_rows() as i64;
        state.current_row = state.row_start;
        state.total_append_count = 0;
        state.row_group_start = self.total_rows();

        let (target, target_idx, row_group_start) = {
            let tree = self.get_row_groups();
            let mut lock = tree.lock();
            let need_new_row_group = tree.is_empty(&mut lock) || *self.requires_new_row_group.lock();
            if need_new_row_group {
                let start_row = self.total_rows();
                let col_count = self.types.len();
                let rg = RowGroup::new(Arc::downgrade(self), start_row, col_count);
                tree.append_segment(&mut lock, rg, start_row);
                *self.requires_new_row_group.lock() = false;
            }
            let last = tree
                .get_last_segment(&mut lock)
                .expect("row group collection should have a root row group");
            (last.arc(), last.index(), last.row_start())
        };

        state.row_group_append_state.row_group_index = Some(target_idx);
        state.start_row_group = Some(target_idx);
        state.row_group_start = row_group_start;
        target.initialize_append(&mut state.row_group_append_state);
    }

    /// Append `chunk` rows.  Returns `true` if a new row group was started.
    ///
    /// Mirrors `RowGroupCollection::Append` in C++.
    ///
    /// # Algorithm
    /// ```text
    /// loop:
    ///   append_count = min(remaining, row_group_size - offset_in_row_group)
    ///   row_group.append(state, chunk, append_count)
    ///   remaining -= append_count
    ///   if remaining == 0 → break
    ///   chunk.slice_range(append_count, remaining)   // skip appended rows
    ///   create new row group, reinitialise append state
    /// update stats (distinct counts per column)
    /// ```
    pub fn append(self: &Arc<Self>, chunk: &mut DataChunk, state: &mut TableAppendState) -> bool {
        let row_group_size = self.row_group_size;
        debug_assert_eq!(chunk.column_count(), self.types.len());
        chunk.verify();

        let mut new_row_group = false;
        let total_append_count = chunk.size() as Idx;
        let mut remaining = total_append_count;
        state.total_append_count += total_append_count;

        loop {
            // ── Retrieve the current target row group ─────────────────────────
            //
            // `row_group_index` is set by `initialize_append` / previous iterations.
            // Fall back to the last segment when no index is recorded yet.
            let current_rg: Arc<RowGroup> = {
                let tree = self.get_row_groups();
                let lock = tree.lock();
                let node = match state.row_group_append_state.row_group_index {
                    Some(idx) => lock.0.get(idx),
                    None => lock.0.last(),
                };
                node.map(|n| n.arc())
                    .expect("no current row group during append")
            };

            // ── Compute how many rows fit in the current row group ─────────────
            let space_in_rg =
                row_group_size.saturating_sub(state.row_group_append_state.offset_in_row_group);
            let append_count = remaining.min(space_in_rg);

            if append_count > 0 {
                let prev_alloc = current_rg.allocation_size();
                current_rg.append(&mut state.row_group_append_state, chunk, append_count);
                // Track allocation growth atomically.
                let delta = current_rg.allocation_size().saturating_sub(prev_alloc);
                self.allocation_size.fetch_add(delta, Ordering::Relaxed);
                // Merge row-group zone-map stats into the collection-level stats.
                current_rg.merge_into_statistics(&self.stats);
            }

            remaining -= append_count;
            if remaining == 0 {
                break;
            }

            // A single chunk should never overflow more than one row group.
            debug_assert_eq!(chunk.size() as Idx, remaining + append_count);

            // Slice the chunk so subsequent appends start after the written rows.
            if remaining < chunk.size() as Idx {
                chunk.slice_range(append_count as usize, remaining as usize);
            }

            // ── Start a new row group ─────────────────────────────────────────
            new_row_group = true;
            let next_start =
                state.row_group_start + state.row_group_append_state.offset_in_row_group;

            let (new_rg, new_idx) = {
                let tree = self.get_row_groups();
                let mut lock = tree.lock();
                let col_count = self.types.len();
                let rg = RowGroup::new(Arc::downgrade(self), next_start, col_count);
                tree.append_segment(&mut lock, rg, next_start);
                // Access the last node directly through the lock to avoid a
                // re-entrant mutex acquisition inside get_last_segment.
                let last = lock.0.last().expect("segment just appended");
                (last.arc(), last.index())
            };

            state.row_group_append_state.row_group_index = Some(new_idx);
            new_rg.initialize_append(&mut state.row_group_append_state);
            state.row_group_start = next_start;
        }

        state.current_row += total_append_count as RowId;

        // ── Update per-column distinct statistics in the append state ─────────
        //
        // `chunk` at this point reflects the last (possibly sliced) portion of
        // the original input, consistent with C++ which updates stats after the loop.
        for col_idx in 0..self.types.len() {
            state.stats.with_stats_mut(col_idx, |col_stats| {
                // 更新 distinct 统计信息（使用预计算的哈希值）
                col_stats.update_distinct_statistics(&state.hashes, chunk.size());
            });
        }

        new_row_group
    }

    /// Finalise the append (release append lock, update total_rows).
    pub fn finalize_append(&self, transaction: TransactionData, state: &mut TableAppendState) {
        let _ = transaction;
        let append_start = state.row_start as Idx;
        let append_end = append_start.saturating_add(state.total_append_count);
        let mut row_group_index = state.start_row_group;

        while let Some(idx) = row_group_index {
            if append_start >= append_end {
                break;
            }

            let row_group = {
                let tree = self.get_row_groups();
                let lock = tree.lock();
                let Some(node) = lock.0.get(idx) else {
                    break;
                };
                node.arc()
            };

            let rg_start = row_group.row_start();
            let rg_end = row_group.row_end();
            let overlap_start = append_start.max(rg_start);
            let overlap_end = append_end.min(rg_end);
            let append_count = overlap_end.saturating_sub(overlap_start);

            row_group.append_version_info(transaction, append_count);
            row_group_index = Some(idx + 1);
        }
        self.total_rows
            .fetch_add(state.total_append_count, Ordering::SeqCst);
        let local_stats = state.stats.clone();
        self.stats.merge_stats(&local_stats);
        state.total_append_count = 0;
    }

    /// Merge another RowGroupCollection into this one (C++: `RowGroupCollection::MergeStorage`).
    ///
    /// This is called during transaction commit to merge LocalStorage data into the main table.
    ///
    /// Flow:
    /// 1. Extract all row groups from source collection
    /// 2. Move them to this collection
    /// 3. Update total_rows and statistics
    pub fn merge_storage(&self, source: Arc<RowGroupCollection>) -> StorageResult<()> {
        // Validate types match
        if self.types.len() != source.types.len() {
            return Err(StorageError::Corrupt {
                msg: format!(
                    "Type mismatch in merge_storage: {} vs {}",
                    self.types.len(),
                    source.types.len()
                ),
            });
        }

        // Get the segment tree and extract all row groups from source
        let source_tree = source.get_row_groups();
        let source_segments = {
            let mut lock = source_tree.lock();
            // Move all segments out of source
            let mut segments = Vec::new();
            std::mem::swap(&mut segments, &mut lock.0);
            segments
        };

        if source_segments.is_empty() {
            return Ok(());
        }

        let is_persistent = source_segments
            .last()
            .map(|segment| segment.node().is_persistent())
            .unwrap_or(false);

        // DuckDB appends after fully materializing the current segment tree.
        let tree = self.get_row_groups();
        let mut tree_lock = tree.lock();
        let _ = tree.get_last_segment(&mut tree_lock);
        let start_index = self.total_rows();

        // Merge each row group
        let mut index = start_index;
        let mut total_merged_rows = 0u64;

        for segment in source_segments {
            let count = segment.node().count();
            let row_group = segment.node().shallow_copy_with_row_start(index);

            tree.append_segment(&mut tree_lock, row_group, index);

            total_merged_rows += count;
            index += count;
        }

        // Update total rows
        self.total_rows
            .fetch_add(total_merged_rows, Ordering::SeqCst);

        // Merge statistics
        self.stats.merge_stats(&source.stats);

        if is_persistent {
            self.set_append_requires_new_row_group();
        }

        Ok(())
    }

    /// Commit an append operation by marking rows as visible (C++: `RowGroupCollection::CommitAppend`).
    pub fn commit_append(&self, _commit_id: u64, _row_start: Idx, _count: Idx) {
        // In DuckDB, this marks the ChunkInfo as committed
        // For now, we don't track per-chunk commit info in the Rust version
        // All appended rows are immediately visible
        // TODO: Find affected row groups and call row_group.commit_append
    }

    pub fn initialize_scan_with_offset(
        self: &Arc<Self>,
        state: &mut CollectionScanState,
        column_ids: &[u64],
        start_row: Idx,
        end_row: Idx,
    ) {
        state.set_column_ids(column_ids.to_vec());
        state.max_row = end_row.min(self.total_rows());
        state.row_groups = Some(self.get_row_groups());
        if start_row >= state.max_row {
            state.current_row_group = None;
            state.max_row_group_row = 0;
            return;
        }

        let tree = self.get_row_groups();
        let mut lock = tree.lock();
        let Some(node) = tree.get_segment(&mut lock, start_row)
        else {
            state.current_row_group = None;
            state.max_row_group_row = 0;
            return;
        };
        let node_index = node.index();
        let row_group_start = node.row_start();
        let row_group = node.arc();
        drop(lock);

        state.current_row_group = Some(RowGroupSegmentRef {
            row_start: row_group_start,
            index: node_index,
            row_group: row_group.clone(),
        });
        let vector_offset = (start_row - row_group_start) / super::types::STANDARD_VECTOR_SIZE;
        let _ = row_group.initialize_scan_with_offset(state, row_group_start, vector_offset);
    }

    pub fn revert_append_internal(&self, start_row: Idx) {
        // Trim total_rows back to start_row.
        self.total_rows.store(start_row, Ordering::SeqCst);
        // Remove all row groups that start at or after start_row.
        let tree = self.get_row_groups();
        let mut lock = tree.lock();
        lock.0.retain(|node| node.row_start() < start_row);
    }

    // ── Delete ────────────────────────────────────────────────

    pub fn delete(
        &self,
        transaction_handle: Option<
            &Arc<crate::transaction::duck_transaction_manager::DuckTxnHandle>,
        >,
        transaction: TransactionData,
        table: Option<&crate::storage::data_table::DataTable>,
        ids: &mut [RowId],
        count: Idx,
    ) -> Idx {
        if count == 0 {
            return 0;
        }
        let tree = self.get_row_groups();
        let lock = tree.lock();
        let mut pos = 0usize;
        let mut delete_count = 0;

        while pos < count as usize {
            let row_id = ids[pos] as Idx;
            let Some(node) = lock.0.iter().find(|node| {
                let start = node.row_start();
                let end = start + node.node().count();
                row_id >= start && row_id < end
            }) else {
                break;
            };

            let row_group = node.arc();
            let row_start = node.row_start();
            let row_end = row_start + node.node().count();

            let start = pos;
            pos += 1;
            while pos < count as usize {
                let current_row = ids[pos] as Idx;
                if current_row < row_start || current_row >= row_end {
                    break;
                }
                pos += 1;
            }

            delete_count += row_group.delete(
                transaction_handle,
                transaction,
                table,
                &mut ids[start..pos],
                (pos - start) as Idx,
                row_start,
            );
        }
        delete_count
    }

    // ── Update ────────────────────────────────────────────────

    pub fn update(
        &self,
        transaction_handle: Option<
            &Arc<crate::transaction::duck_transaction_manager::DuckTxnHandle>,
        >,
        transaction: TransactionData,
        table: Option<&crate::storage::data_table::DataTable>,
        ids: &[RowId],
        column_ids: &[crate::catalog::PhysicalIndex],
        updates: &crate::common::types::DataChunk,
    ) -> crate::common::errors::StorageResult<()> {
        if ids.is_empty() || updates.size() == 0 {
            return Ok(());
        }
        debug_assert_eq!(ids.len(), updates.size(), "ids must match updates.size()");
        debug_assert_eq!(
            column_ids.len(),
            updates.column_count(),
            "column_ids must match updates.column_count()"
        );

        let tree = self.get_row_groups();
        let lock = tree.lock();
        let mut start = 0usize;
        while start < ids.len() {
            let row_id = ids[start];
            let Some(node) = lock.0.iter().find(|node| {
                let start = node.row_start();
                let end = start + node.node().count();
                row_id as Idx >= start && (row_id as Idx) < end
            }) else {
                return Err(crate::storage::storage_info::StorageError::Other(format!(
                    "RowGroupCollection::update: row_id {} not found in any row group",
                    row_id
                )));
            };
            let rg = node.arc();
            let rg_row_start = node.row_start();
            let rg_row_end = rg_row_start + node.node().count();

            let mut end = start + 1;
            while end < ids.len() {
                let next_row = ids[end] as Idx;
                if next_row < rg_row_start || next_row >= rg_row_end {
                    break;
                }
                end += 1;
            }

            let batch_count = end - start;
            let batch_sel = crate::common::types::SelectionVector {
                indices: (start..end).map(|idx| idx as u32).collect(),
            };
            let batch_row_ids = &ids[start..end];

            for (u_col_idx, &col_id) in column_ids.iter().enumerate() {
                let col_usize = col_id.0;
                let col = rg.get_column(col_usize);
                let type_size = col.base.logical_type.physical_size() as usize;
                if type_size == 0 {
                    return Err(crate::storage::storage_info::StorageError::Other(format!(
                        "RowGroupCollection::update: unsupported variable-width update on column {}",
                        col_usize
                    )));
                }

                let mut update_vector = crate::common::types::Vector::with_capacity(
                    updates.data[u_col_idx].logical_type.clone(),
                    batch_count,
                );
                update_vector.copy_from_sel(&updates.data[u_col_idx], &batch_sel, batch_count, 0);
                update_vector.flatten(batch_count);

                if col.base.is_persistent() {
                    let table = table.ok_or_else(|| {
                        crate::storage::storage_info::StorageError::Other(
                            "RowGroupCollection::update: persistent updates require table metadata"
                                .to_string(),
                        )
                    })?;
                    let update_segment = {
                        let _update_lock = col.base.update_lock.lock();
                        let mut updates_guard = col.base.updates.lock();
                        Arc::clone(updates_guard.get_or_insert_with(|| {
                            let segment = Arc::new(super::update_segment::UpdateSegment::new(
                                type_size as Idx,
                            ));
                            super::update_segment::UpdateSegment::register(&segment);
                            segment
                        }))
                    };
                    update_segment.update(
                        transaction_handle,
                        transaction,
                        table,
                        col_usize as Idx,
                        &update_vector,
                        batch_row_ids,
                        batch_count,
                        rg.row_start(),
                    )?;
                } else {
                    let raw = update_vector.raw_data();
                    for (row_idx, &row_id) in batch_row_ids.iter().enumerate() {
                        let local_row = row_id as Idx - rg.row_start();
                        let off = row_idx * type_size;
                        col.update_in_place_transient(local_row, &raw[off..off + type_size])?;
                    }
                }
            }

            start = end;
        }
        Ok(())
    }

    // ── Checkpoint ───────────────────────────────────────────

    pub fn checkpoint(&self, stats: &mut TableStatistics) {
        todo!("iterate row groups, call row_group.write_to_disk for each")
    }

    // ── Schema evolution ──────────────────────────────────────

    pub fn add_column(self: &Arc<Self>, new_type: LogicalType) -> Arc<RowGroupCollection> {
        todo!("create new collection with extra column, copy row groups with AddColumn applied")
    }

    pub fn remove_column(self: &Arc<Self>, col_idx: usize) -> Arc<RowGroupCollection> {
        todo!()
    }

    // ── Drop / Destroy ────────────────────────────────────────

    /// Mark all blocks as modified so they can be reclaimed (C++: `CommitDropTable`).
    pub fn commit_drop_table(&self) {
        // No block manager in the Rust implementation yet.
    }

    /// Free the storage for one column (C++: `CommitDropColumn`).
    pub fn commit_drop_column(&self, _column_index: usize) {
        // No block manager in the Rust implementation yet.
    }

    /// Destroy all row groups (C++: `Destroy`).
    pub fn destroy(&self) {
        let tree = self.get_row_groups();
        let mut lock = tree.lock();
        lock.0.clear();
        self.total_rows.store(0, Ordering::SeqCst);
    }

    // ── Statistics ────────────────────────────────────────────

    /// Copy statistics for one column (C++: `CopyStats`).
    pub fn copy_stats(&self, column_id: Idx) -> Option<crate::storage::statistics::BaseStatistics> {
        self.stats
            .copy_column_stats(column_id as usize)
            .map(|stats| stats.statistics().clone())
    }

    // ── Debug info ────────────────────────────────────────────

    /// Collect column segment info for debugging (C++: `GetColumnSegmentInfo`).
    pub fn get_column_segment_info(&self) -> Vec<(u64, u64, String)> {
        // Returns (column_id, row_group_index, segment_type) tuples.
        // Full implementation requires RowGroup::GetColumnSegmentInfo.
        Vec::new()
    }

    // ── Internals ─────────────────────────────────────────────

    pub fn get_row_groups(&self) -> Arc<RowGroupSegmentTree> {
        Arc::clone(&*self.owned_row_groups.lock())
    }

    fn append_row_group(&self, start_row: Idx) {
        let tree = self.owned_row_groups.lock();
        let lock = tree.lock();
        // row group count is self.types.len() columns
        let col_count = self.types.len();
        // RowGroup needs Weak to self — requires Arc<Self>
        // This is awkward; callers that have Arc<Self> should use the Arc variant
        todo!("create RowGroup with Weak<Self> back-reference and append to tree")
    }

    // ── Parallel scan ─────────────────────────────────────────────────────────

    /// Advance a parallel scan to the next available row group (C++: `NextParallelScan`).
    ///
    /// Atomically claims the next un-scanned row group index. Returns `true` when
    /// `scan_state` has been initialised with a valid row group, `false` when all
    /// row groups have been consumed.
    pub fn next_parallel_scan(
        self: &Arc<Self>,
        state: &mut ParallelCollectionScanState,
        scan_state: &mut CollectionScanState,
    ) -> bool {
        // C++:
        //   auto &row_group_index = state.current_row_group;
        //   loop:
        //     auto current = row_group_index++;
        //     if (current >= row_groups.size()) return false;
        //     if (row_group->InitializeScan(scan_state)) return true;
        let Some(tree) = state.row_groups.clone() else {
            return false;
        };
        loop {
            let (current_ref, max_row) = {
                let _guard = state.lock.lock();
                let idx = state.next_row_group_index;
                state.next_row_group_index += 1;
                let current_ref = {
                    let mut lock = tree.lock();
                    tree.get_segment_by_index(&mut lock, idx as i64).map(|node| RowGroupSegmentRef {
                        row_start: node.row_start(),
                        index: node.index(),
                        row_group: node.arc(),
                    })
                };
                state.current_row_group = current_ref.clone();
                let max_row = current_ref
                    .as_ref()
                    .map(|current_row_group| {
                        current_row_group
                            .row_start
                            .saturating_add(current_row_group.row_group.count())
                            .min(state.max_row)
                    })
                    .unwrap_or(0);
                state.batch_index += 1;
                (current_ref, max_row)
            };

            let Some(current_row_group) = current_ref else {
                state.current_row_group = None;
                return false;
            };
            if current_row_group.row_start >= state.max_row {
                state.current_row_group = None;
                return false;
            }

            // Attempt to initialise the scan for this row group.
            scan_state.row_groups = Some(Arc::clone(&tree));
            scan_state.current_row_group = Some(current_row_group.clone());
            scan_state.max_row = max_row;
            scan_state.batch_index = state.batch_index;
            if current_row_group
                .row_group
                .initialize_scan(scan_state, current_row_group.row_start)
            {
                return true;
            }
            // Zone-map pruned — try the next one.
        }
    }

    // ── Fetch ─────────────────────────────────────────────────────────────────

    /// Point-lookup fetch by row IDs (C++: `RowGroupCollection::Fetch`).
    ///
    /// Fills `chunk` with columns `col_ids` for each row ID in `row_ids[..count]`.
    /// Delegates to each row group that contains matching row IDs.
    pub fn fetch(
        &self,
        transaction: TransactionData,
        chunk: &mut crate::common::types::DataChunk,
        col_ids: &[u64],
        row_ids: &[RowId],
        count: Idx,
        fetch_state: &mut super::scan_state::ColumnFetchState,
    ) {
        // In a full implementation this iterates row_ids, locates the row group
        // for each id, and calls row_group.fetch_row. The necessary infrastructure
        // (ColumnData::Fetch) is not yet available in this Rust port, so we return
        // an empty chunk. Callers should handle empty results gracefully.
        let _ = (transaction, col_ids, row_ids, count, fetch_state);
        // chunk remains unmodified (empty)
    }

    /// Returns `true` if `row_id` is within the range of this collection and
    /// the version info indicates it is visible to `transaction` (C++: `CanFetch`).
    pub fn can_fetch(&self, transaction: TransactionData, row_id: RowId) -> bool {
        // C++: iterates row groups looking for one whose range contains row_id,
        // then delegates to row_group.HasChanges() / version manager.
        // Simplified: row_id is fetchable iff it is within [0, total_rows).
        let _ = transaction;
        row_id >= 0 && (row_id as Idx) < self.total_rows()
    }

    // ── Partition statistics ───────────────────────────────────────────────────

    /// Returns per-row-group partition statistics (C++: `GetPartitionStats`).
    pub fn get_partition_stats(&self) -> Vec<crate::storage::local_storage::PartitionStatistics> {
        let tree = self.get_row_groups();
        let lock = tree.lock();
        lock.0
            .iter()
            .map(|node| {
                let count = node.node().count();
                crate::storage::local_storage::PartitionStatistics {
                    row_count: count,
                    deleted_count: 0, // deleted count requires version-manager access
                }
            })
            .collect()
    }
}
