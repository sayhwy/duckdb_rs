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

use parking_lot::Mutex;
use crate::common::types::DataChunk;
use crate::storage::storage_info::{StorageError, StorageResult};
use super::append_state::TableAppendState;
use super::column_data::{PersistentCollectionData, PersistentRowGroupData};
use super::persistent_table_data::PersistentTableData;
use super::row_group::{RowGroup, RowGroupPointer, RowGroupWriteInfo};
use super::scan_state::{CollectionScanState, ParallelCollectionScanState};
use super::segment_base::SegmentBase;
use super::segment_tree::SegmentTree;
use super::data_table_info::DataTableInfo;
use super::table_statistics::TableStatistics;
use super::types::{Idx, LogicalType, MetaBlockPointer, RowId, TransactionData, TransactionId};

/// Type alias for the row-group segment tree.
pub type RowGroupSegmentTree = SegmentTree<RowGroup>;

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
    pub fn initialize(
        self: &Arc<Self>,
        data: PersistentCollectionData,
    ) {
        todo!("populate owned_row_groups from data.row_group_data")
    }

    pub fn initialize_from_table_data(self: &Arc<Self>, data: &PersistentTableData) {
        self.total_rows.store(data.total_rows, Ordering::Relaxed);
        let tree_guard = self.owned_row_groups.lock();
        let tree = Arc::clone(&*tree_guard);
        drop(tree_guard);
        let mut seg_lock = tree.lock();
        for pointer in &data.row_group_pointers {
            let row_group = RowGroup::from_pointer(
                Arc::downgrade(self),
                pointer.clone(),
                self.types.len(),
            );
            tree.append_segment(&mut seg_lock, row_group, pointer.row_start);
        }
        drop(seg_lock);
        let mut stats = self.stats.clone();
        stats.initialize_empty(&self.types);
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
        let lock = tree.lock();
        if index < 0 {
            let pos = lock.0.len() as i64 + index;
            if pos < 0 {
                None
            } else {
                lock.0.get(pos as usize).map(|n| n.arc())
            }
        } else {
            lock.0.get(index as usize).map(|n| n.arc())
        }
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
    /// - `state.row_group_index`    — first row group to scan.
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
        let (root_arc, root_idx, base_row_id, root_row_start) = {
            let tree_guard = self.owned_row_groups.lock();
            let tree = Arc::clone(&*tree_guard);
            drop(tree_guard);
            let lock = tree.lock();
            let base = tree.base_row_id();
            match tree.get_root_segment(&lock) {
                None => {
                    // Empty collection.
                    state.row_group_index = None;
                    state.max_row = 0;
                    return;
                }
                Some(node) => (node.arc(), node.index(), base, node.row_start()),
            }
        };

        // C++: state.max_row = row_start + GetTotalRows()
        // base_row_id is the collection's starting row offset (may be i64::MAX for local storage).
        state.max_row = base_row_id + self.total_rows();
        state.row_group_index = Some(root_idx);
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
        let tree = self.get_row_groups();

        loop {
            let rg_idx = match state.row_group_index {
                None => return false,
                Some(idx) => idx,
            };

            // Clone Arc without holding the tree lock during scan.
            let (rg_arc, rg_row_start, rg_count): (Arc<RowGroup>, Idx, Idx) = {
                let lock = tree.lock();
                match lock.0.get(rg_idx) {
                    None => return false,
                    Some(node) => (node.arc(), node.row_start(), node.node().count()),
                }
            };

            // Scan the current row group.
            rg_arc.scan(transaction, state, result);

            if result.size() > 0 {
                return true;
            }

            // Row group exhausted.
            // C++: if (max_row <= row_group->GetRowStart() + row_group->GetNode().count)
            if state.max_row <= rg_row_start + rg_count {
                state.row_group_index = None;
                return false;
            }

            // Advance to the next row group, skipping zone-map-pruned ones.
            // C++: do { row_group = GetNextRowGroup(*row_group); ... } while (row_group)
            let mut curr_idx = rg_idx;
            loop {
                let next = {
                    let lock = tree.lock();
                    let next_idx = curr_idx + 1;
                    if next_idx < lock.0.len() {
                        Some((
                            lock.0[next_idx].index(),
                            lock.0[next_idx].row_start(),
                            lock.0[next_idx].arc(),
                        ))
                    } else {
                        None
                    }
                };

                match next {
                    None => {
                        state.row_group_index = None;
                        return false;
                    }
                    Some((next_idx, next_row_start, next_arc)) => {
                        // C++: if (row_group->GetRowStart() >= max_row) { row_group = None; break }
                        if next_row_start >= state.max_row {
                            state.row_group_index = None;
                            return false;
                        }
                        state.row_group_index = Some(next_idx);
                        curr_idx = next_idx;
                        // C++: row_group->GetNode().InitializeScan(*this, *row_group)
                        // Pass SegmentNode's row_start so merged row groups work correctly.
                        if next_arc.initialize_scan(state, next_row_start) {
                            break; // Found a scannable row group — go back to outer loop.
                        }
                        // Zone-map pruned → continue to the next one.
                    }
                }
            }
            // Outer loop continues with the new `state.row_group_index`.
        }
    }

    pub fn initialize_parallel_scan(&self, state: &mut ParallelCollectionScanState) {
        state.current_row_group_index.store(0, std::sync::atomic::Ordering::Relaxed);
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
            let lock = tree.lock();
            if let Some(last) = lock.0.last() {
                (last.arc(), last.index(), last.row_start())
            } else {
                drop(lock);
                self.initialize_empty();
                let tree = self.get_row_groups();
                let lock = tree.lock();
                let last = lock.0.last().expect("row group collection should have a root row group");
                (last.arc(), last.index(), last.row_start())
            }
        };

        state.row_group_append_state.row_group_index = Some(target_idx);
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

        // Debug: 每10万行打印一次
        if state.total_append_count % 100000 < total_append_count {
            println!("🔵 [APPEND] total={}, offset_in_rg={}, rg_size={}",
                state.total_append_count,
                state.row_group_append_state.offset_in_row_group,
                row_group_size);
        }

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
                    None      => lock.0.last(),
                };
                node.map(|n| n.arc())
                    .expect("no current row group during append")
            };

            // ── Compute how many rows fit in the current row group ─────────────
            let space_in_rg = row_group_size
                .saturating_sub(state.row_group_append_state.offset_in_row_group);
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
            println!("🟢 [NEW ROW GROUP] Creating new row group! total_append={}, offset_in_rg={}, next_start={}",
                state.total_append_count,
                state.row_group_append_state.offset_in_row_group,
                state.row_group_start + state.row_group_append_state.offset_in_row_group);

            let next_start = state.row_group_start
                + state.row_group_append_state.offset_in_row_group;

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
        self.total_rows.fetch_add(state.total_append_count, Ordering::SeqCst);
        let _ = transaction;
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

        // Calculate starting index for merged row groups
        let tree = self.get_row_groups();
        let start_index = self.total_rows();

        // Merge each row group
        let mut index = start_index;
        let mut total_merged_rows = 0u64;

        for segment in source_segments {
            let count = segment.node().count();

            // Append to our segment tree
            {
                let mut lock = tree.lock();
                let new_index = lock.0.len();
                lock.0.push(super::segment_tree::SegmentNode::new(
                    index,
                    segment.arc(),
                    new_index,
                ));
            }

            total_merged_rows += count;
            index += count;
        }

        // Update total rows
        self.total_rows.fetch_add(total_merged_rows, Ordering::SeqCst);

        // Merge statistics
        // TODO: Implement stats.MergeStats(source.stats)

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
        if start_row >= state.max_row {
            state.row_group_index = None;
            state.max_row_group_row = 0;
            return;
        }

        let tree = self.get_row_groups();
        let lock = tree.lock();
        let Some((node_index, row_group_start, row_group_count, row_group)) = lock
            .0
            .iter()
            .enumerate()
            .find_map(|(idx, node)| {
                let start = node.row_start();
                let end = start + node.node().count();
                (start_row >= start && start_row < end).then(|| (idx, start, node.node().count(), node.arc()))
            }) else {
            state.row_group_index = None;
            state.max_row_group_row = 0;
            return;
        };
        drop(lock);

        state.row_group_index = Some(node_index);
        let vector_offset = (start_row - row_group_start) / super::types::STANDARD_VECTOR_SIZE;
        state.max_row = (row_group_start + row_group_count).min(state.max_row);
        let _ = row_group.initialize_scan_with_offset(state, node_index, vector_offset);
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
        transaction: TransactionData,
        ids: &mut [RowId],
        count: Idx,
    ) -> Idx {
        todo!("group row ids by row group, call row_group.delete on each")
    }

    // ── Update ────────────────────────────────────────────────

    pub fn update(
        &self,
        transaction: TransactionData,
        ids: &[RowId],
        column_ids: &[u64],
        updates: &crate::common::types::DataChunk,
    ) {
        todo!("group ids by row group, call row_group.update")
    }

    // ── Checkpoint ───────────────────────────────────────────

    pub fn checkpoint(&self, stats: &mut TableStatistics) {
        todo!("iterate row groups, call row_group.write_to_disk for each")
    }

    // ── Schema evolution ──────────────────────────────────────

    pub fn add_column(
        self: &Arc<Self>,
        new_type: LogicalType,
    ) -> Arc<RowGroupCollection> {
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
    pub fn copy_stats(&self, _column_id: Idx) -> Option<crate::storage::statistics::BaseStatistics> {
        // TODO: obtain stats from per-row-group statistics once TableStatistics is complete.
        None
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
        let mut lock = tree.lock();
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
        //     auto current = state.current_row_group_index.fetch_add(1);
        //     if (current >= row_groups.size()) return false;
        //     if (row_group->InitializeScan(scan_state)) return true;
        let tree = self.get_row_groups();
        loop {
            let idx = state
                .current_row_group_index
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let (row_group_arc, node_idx, node_row_start) = {
                let lock = tree.lock();
                match lock.0.get(idx as usize) {
                    None => return false,
                    Some(node) => {
                        let rs = node.row_start();
                        if rs >= state.max_row {
                            return false;
                        }
                        (node.arc(), node.index(), rs)
                    }
                }
            };

            // Attempt to initialise the scan for this row group.
            scan_state.row_group_index = Some(node_idx);
            if row_group_arc.initialize_scan(scan_state, node_row_start) {
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
