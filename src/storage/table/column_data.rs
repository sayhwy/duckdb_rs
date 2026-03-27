//! `ColumnData` — the storage unit for one column within a `RowGroup`.
//!
//! Mirrors `duckdb/storage/table/column_data.hpp` / `column_data.cpp`.
//!
//! # Design notes vs C++
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class ColumnData` (virtual base) | `ColumnDataContext` + `ColumnDataKind` struct |
//! | `class StandardColumnData : public ColumnData` | `ColumnKindData::Standard` variant |
//! | `class ValidityColumnData : public ColumnData` | `ColumnKindData::Validity` variant |
//! | `class ListColumnData : public ColumnData` | `ColumnKindData::List` variant |
//! | `class ArrayColumnData : public ColumnData` | `ColumnKindData::Array` variant |
//! | `class StructColumnData : public ColumnData` | `ColumnKindData::Struct` variant |
//! | `class VariantColumnData : public ColumnData` | `ColumnKindData::Variant` variant |
//! | `PersistentColumnData` | `PersistentColumnData` |
//! | `PersistentRowGroupData` | `PersistentRowGroupData` |
//! | `PersistentCollectionData` | `PersistentCollectionData` |
//!
//! The auxiliary per-variant structs (`StandardColumnData`, `ValidityColumnData`,
//! etc.) in their own files still exist for compatibility; they wrap
//! `ColumnDataContext` but are separate from the `ColumnDataKind` enum.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;

use super::append_state::ColumnAppendState;
use super::column_segment::{
    ColumnSegment, ColumnSegmentType, SegmentStatistics, UnifiedVectorFormat,
};
use super::column_segment_tree::ColumnSegmentTree;
use super::data_table_info::DataTableInfo;
use super::scan_state::ColumnScanState;
use super::segment_base::SegmentBase;
use super::table_statistics::TableStatistics;
use super::types::{
    CompressionType, DataPointer, Idx, LogicalType, PhysicalType, TransactionData, STANDARD_VECTOR_SIZE,
};
use super::update_segment::UpdateSegment;
use crate::common::types::{LogicalTypeId, Vector};

// ─────────────────────────────────────────────────────────────────────────────
// ColumnDataType
// ─────────────────────────────────────────────────────────────────────────────

/// Role of a `ColumnData` instance.
///
/// Mirrors `enum class ColumnDataType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnDataType {
    /// Normal, checkpointable main-table storage.
    MainTable,
    /// Transaction-local transient storage (not yet committed).
    InitialTransactionLocal,
    /// Temporary checkpoint write target; normalised to `MainTable` at construction.
    CheckpointTarget,
}

// ─────────────────────────────────────────────────────────────────────────────
// ScanVectorType re-export + ScanVectorMode / FilterPropagateResult
// ─────────────────────────────────────────────────────────────────────────────

// `ScanVectorType` is defined in `column_segment` (the layer that dispatches
// scan variants) and re-exported here for callers at the column-data level.
pub use super::column_segment::ScanVectorType;

/// MVCC visibility policy for column scans.
///
/// Mirrors `enum class ScanVectorMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanVectorMode {
    /// Normal MVCC scan (own transaction changes visible).
    RegularScan,
    /// Read committed data as of commit time.
    ScanCommitted,
    /// Committed data only; uncommitted updates are forbidden.
    ScanCommittedNoUpdates,
}

/// Result of evaluating a zone-map filter against a column segment.
///
/// Mirrors `enum class FilterPropagateResult`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterPropagateResult {
    NoPruningPossible,
    FilterAlwaysTrue,
    FilterAlwaysFalse,
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnDataContext
// ─────────────────────────────────────────────────────────────────────────────

/// Fields shared by every column variant — the C++ `ColumnData` base class.
///
/// Composite and sub-type column structs (`StandardColumnData`,
/// `ValidityColumnData`, etc.) embed this as a `pub ctx: ColumnDataContext`
/// field.  The polymorphic `ColumnDataKind` enum also stores one here.
pub struct ColumnDataContext {
    /// Total rows in this column (C++: `idx_t count`).
    pub count: AtomicU64,

    /// Table metadata shared across the row group (C++: `DataTableInfo &info`).
    pub info: Arc<DataTableInfo>,

    /// Schema position of this column (C++: `idx_t column_index`).
    pub column_index: Idx,

    /// Logical type of stored values (C++: `LogicalType type`).
    pub logical_type: LogicalType,

    /// Bytes allocated for all transient segments (C++: `idx_t allocation_size`).
    pub allocation_size: AtomicU64,

    /// Role of this column (C++: `ColumnDataType data_type`).
    pub data_type: Mutex<ColumnDataType>,

    /// Ordered list of compressed column segments (C++: `ColumnSegmentTree data`).
    pub data: ColumnSegmentTree,

    /// Per-column statistics (only present when this column has no parent).
    ///
    /// C++: `unique_ptr<SegmentStatistics> stats` — created when `parent == nullptr`.
    pub stats: Option<Mutex<SegmentStatistics>>,

    /// Lock protecting `stats` mutations (C++: `mutable mutex stats_lock`).
    pub stats_lock: Mutex<()>,

    /// MVCC delta store; populated lazily when rows are first updated.
    ///
    /// C++: `unique_ptr<UpdateSegment> updates`.
    pub updates: Mutex<Option<Arc<UpdateSegment>>>,

    /// Lock protecting `updates` access (C++: `mutable mutex update_lock`).
    pub update_lock: Mutex<()>,

    /// Uniform compression for all segments; `None` if mixed or not yet set.
    ///
    /// C++: `optional_ptr<CompressionFunction> compression`.
    pub compression: Mutex<Option<CompressionType>>,
}

impl ColumnDataContext {
    /// Construct a new context.
    ///
    /// Mirrors `ColumnData::ColumnData(...)`.
    /// `CheckpointTarget` is normalised to `MainTable` here, matching C++.
    /// Statistics are allocated only when `has_parent = false`.
    pub fn new(
        info: Arc<DataTableInfo>,
        column_index: Idx,
        logical_type: LogicalType,
        data_type: ColumnDataType,
        has_parent: bool,
    ) -> Self {
        let data_type = if data_type == ColumnDataType::CheckpointTarget {
            ColumnDataType::MainTable
        } else {
            data_type
        };

        let stats = if !has_parent {
            Some(Mutex::new(SegmentStatistics::default()))
        } else {
            None
        };

        ColumnDataContext {
            count: AtomicU64::new(0),
            info,
            column_index,
            logical_type,
            allocation_size: AtomicU64::new(0),
            data_type: Mutex::new(data_type),
            data: ColumnSegmentTree::new(0),
            stats,
            stats_lock: Mutex::new(()),
            updates: Mutex::new(None),
            update_lock: Mutex::new(()),
            compression: Mutex::new(None),
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    pub fn count(&self) -> Idx {
        self.count.load(Ordering::Relaxed)
    }

    pub fn allocation_size(&self) -> Idx {
        self.allocation_size.load(Ordering::Relaxed)
    }

    /// `true` if any MVCC update records exist for this column.
    ///
    /// C++: `ColumnData::HasUpdates`.
    pub fn has_updates(&self) -> bool {
        self.updates.lock().is_some()
    }

    // ── Scan ──────────────────────────────────────────────────────────────────

    /// Set up `state` for a forward scan starting at row 0.
    ///
    /// C++: `ColumnData::InitializeScan`.
    pub fn initialize_scan(&self, state: &mut ColumnScanState) {
        // Access lock.0 directly to avoid re-entrant mutex deadlock:
        // SegmentTree::get_root_segment tries self.nodes.lock() if empty,
        // which would deadlock since we already hold the guard here.
        let lock = self.data.lock();
        let first = lock.0.first();
        let row_start = first.map_or(0, |n| n.row_start());
        state.current_segment_index = first.map(|n| n.index());
        state.offset_in_column = row_start;
        state.segment_row_start = row_start;
        state.internal_index = row_start;
        state.initialized = false;
        state.scan_state = None;
        state.last_offset = 0;
    }

    /// Set up `state` for a forward scan starting at `row_idx`.
    ///
    /// C++: `ColumnData::InitializeScanWithOffset`.
    pub fn initialize_scan_with_offset(&self, state: &mut ColumnScanState, row_idx: Idx) {
        assert!(
            row_idx <= self.count(),
            "row_idx {} out of range (count={})",
            row_idx,
            self.count()
        );
        let mut lock = self.data.lock();
        // get_segment is safe: its inner helper takes &mut nodes directly,
        // never calls self.nodes.lock() again.
        let seg = self.data.get_segment(&mut lock, row_idx);
        let seg_row_start = seg.map_or(0, |n| n.row_start());
        state.current_segment_index = seg.map(|n| n.index());
        state.segment_row_start = seg_row_start;
        state.internal_index = seg_row_start;
        state.offset_in_column = row_idx;
        state.initialized = false;
        state.scan_state = None;
        state.last_offset = 0;
    }

    /// Number of rows to scan for the given vector-index window.
    ///
    /// C++: `ColumnData::GetVectorCount`.
    pub fn get_vector_count(&self, vector_index: Idx) -> Idx {
        let current_row = vector_index * STANDARD_VECTOR_SIZE;
        STANDARD_VECTOR_SIZE.min(self.count().saturating_sub(current_row))
    }

    /// Determine the scan type based on whether we need to scan across segment boundaries.
    ///
    /// Mirrors `ColumnData::GetVectorScanType`.
    ///
    /// Returns `ScanFlatVector` if:
    /// - The column has updates (MVCC deltas need to be merged), or
    /// - The scan will cross segment boundaries (remaining_in_segment < scan_count)
    ///
    /// Otherwise returns `ScanEntireVector` for the fast path.
    pub fn get_vector_scan_type(&self, state: &ColumnScanState, scan_count: Idx) -> ScanVectorType {
        // If we have updates, we need to merge them in — always use flat vectors
        if self.has_updates() {
            return ScanVectorType::ScanFlatVector;
        }

        // Check if the current segment has enough data remaining
        let lock = self.data.lock();
        if let Some(idx) = state.current_segment_index {
            if idx < lock.0.len() {
                let node = &lock.0[idx];
                let current_start = node.row_start();
                let current_count = node.node().count();
                let remaining_in_segment = current_start + current_count - state.offset_in_column;

                if remaining_in_segment < scan_count {
                    // Not enough data in current segment — need to scan across segments
                    // Use flat vectors for cross-segment scans
                    return ScanVectorType::ScanFlatVector;
                }
            }
        }

        // Fast path: entire vector fits in one segment
        ScanVectorType::ScanEntireVector
    }

    // ── Scan (internal helpers) ───────────────────────────────────────────────

    /// Ensure the codec scan state is ready for the current segment.
    ///
    /// Mirrors `ColumnData::BeginScanVectorInternal`.
    ///
    /// # Steps
    /// 1. Clear `previous_states` (C++: `state.previous_states.clear()`).
    /// 2. If not yet initialised:
    ///    - Call `ColumnSegment::initialize_scan` on the current segment.
    ///    - Set `state.segment_row_start` and `state.internal_index` to the
    ///      segment's `row_start`.
    ///    - Mark `state.initialized = true`.
    /// 3. If `internal_index < offset_in_column`, call `ColumnSegment::skip`
    ///    to fast-forward the codec cursor, then sync `internal_index`.
    pub fn begin_scan_vector_internal(&self, state: &mut ColumnScanState) {
        state.previous_states.clear();

        if !state.initialized {
            let (seg_arc, row_start) = {
                let lock = self.data.lock();
                let idx = state.current_segment_index
                    .expect("begin_scan_vector_internal: state not initialised via initialize_scan");
                let node = &lock.0[idx];
                (node.arc(), node.row_start())
            };
            // Set the segment row-start so position_in_segment() works correctly.
            state.segment_row_start = row_start;
            // C++: state.internal_index = state.current->GetRowStart()
            state.internal_index = row_start;
            seg_arc.initialize_scan(state);
            state.initialized = true;
        }

        // C++: if (state.internal_index < state.offset_in_column) { current.Skip(state); }
        if state.internal_index < state.offset_in_column {
            let seg_arc = {
                let lock = self.data.lock();
                let idx = state.current_segment_index.unwrap();
                lock.0[idx].arc()
            };
            seg_arc.skip(state);
            // C++: state.internal_index = state.offset_in_column
            state.internal_index = state.offset_in_column;
        }
    }

    /// Scan up to `remaining` rows into `result`.
    ///
    /// Returns the number of rows actually scanned (may be less than `remaining`
    /// if the column has fewer rows).
    ///
    /// Mirrors `ColumnData::ScanVector(ColumnScanState&, Vector&, idx_t remaining,
    ///     ScanVectorType, idx_t base_result_offset)`.
    ///
    /// # Algorithm
    /// ```text
    /// begin_scan_vector_internal(state)
    /// while remaining > 0:
    ///   scan_count = min(remaining, current_segment_end - offset_in_column)
    ///   segment.scan(state, scan_count, result, result_offset, scan_type)
    ///   offset_in_column += scan_count; remaining -= scan_count
    ///   if remaining > 0:
    ///     move to next segment, initialize it
    /// state.internal_index = state.offset_in_column
    /// return initial_remaining - remaining
    /// ```
    pub fn scan_vector(
        &self,
        state: &mut ColumnScanState,
        result: &mut Vector,
        remaining: Idx,
        scan_type: ScanVectorType,
        base_result_offset: Idx,
    ) -> Idx {
        self.begin_scan_vector_internal(state);

        let initial_remaining = remaining;
        let mut remaining = remaining;

        while remaining > 0 {
            // Clone Arc to avoid holding the tree lock during scan.
            let (seg_arc, current_start, current_count) = {
                let lock = self.data.lock();
                match state.current_segment_index {
                    None => break,
                    Some(idx) if idx < lock.0.len() => {
                        let node = &lock.0[idx];
                        (node.arc(), node.row_start(), node.node().count())
                    }
                    _ => break,
                }
            };

            debug_assert!(
                state.offset_in_column >= current_start
                    && state.offset_in_column <= current_start + current_count,
                "offset {} outside segment [{}, {})",
                state.offset_in_column, current_start, current_start + current_count
            );

            let scan_count   = remaining.min(current_start + current_count - state.offset_in_column);
            let result_offset = base_result_offset + (initial_remaining - remaining);

            if scan_count > 0 {
                seg_arc.scan(state, scan_count, result, result_offset, scan_type);
                state.offset_in_column += scan_count;
                remaining -= scan_count;
            }

            if remaining > 0 {
                // Advance to the next segment.
                let next = {
                    let lock = self.data.lock();
                    let curr_idx = state.current_segment_index.unwrap();
                    let next_idx = curr_idx + 1;
                    if next_idx < lock.0.len() {
                        Some((lock.0[next_idx].index(), lock.0[next_idx].row_start(), lock.0[next_idx].arc()))
                    } else {
                        None
                    }
                };

                match next {
                    None => break,
                    Some((next_idx, next_row_start, next_arc)) => {
                        // Stash the old scan state (C++: previous_states.emplace_back).
                        if let Some(old) = state.scan_state.take() {
                            state.previous_states.push(old);
                        }
                        state.current_segment_index = Some(next_idx);
                        state.segment_row_start = next_row_start;
                        state.segment_checked = false;
                        // C++: state.current->GetNode().InitializeScan(state)
                        next_arc.initialize_scan(state);
                        state.internal_index = next_row_start;
                        state.initialized = true;
                    }
                }
            }
        }

        // C++: state.internal_index = state.offset_in_column
        state.internal_index = state.offset_in_column;
        initial_remaining - remaining
    }

    /// Advance the scan cursor by `STANDARD_VECTOR_SIZE` rows without output.
    ///
    /// Mirrors `ColumnData::Skip(ColumnScanState&, idx_t)` which calls
    /// `state.Next(count)`.  In Rust we also update `current_segment_index`
    /// when the skip crosses a segment boundary.
    pub fn skip(&self, state: &mut ColumnScanState) {
        state.offset_in_column += STANDARD_VECTOR_SIZE;
        state.internal_index = state.offset_in_column;

        // If we've advanced past the current segment, find the new one.
        let needs_advance = {
            let lock = self.data.lock();
            match state.current_segment_index {
                None => false,
                Some(idx) if idx < lock.0.len() => {
                    let node = &lock.0[idx];
                    state.offset_in_column > node.row_start() + node.node().count()
                }
                _ => false,
            }
        };

        if needs_advance {
            let lock = self.data.lock();
            let curr_idx = state.current_segment_index.unwrap();
            let next_idx = curr_idx + 1;
            if next_idx < lock.0.len() {
                state.current_segment_index = Some(next_idx);
                state.segment_row_start = lock.0[next_idx].row_start();
                state.initialized = false; // will be re-initialised on next scan
            } else {
                state.current_segment_index = None;
            }
            // Unpin the old block immediately; the new segment will be pinned
            // by initialize_scan() when the next scan starts.
            state.pinned_buffer = None;
        }
    }

    // ── Append ────────────────────────────────────────────────────────────────

    /// Create a new transient (in-memory) segment starting at `start_row`.
    ///
    /// C++: `ColumnData::AppendTransientSegment`.
    pub fn append_transient_segment(&self, start_row: Idx) {
        // Segment must fit within a block's payload (block size minus 8-byte header).
        let segment_size: Idx = 262_144 - 8; // DEFAULT_BLOCK_ALLOC_SIZE - DEFAULT_BLOCK_HEADER_SIZE
        let compression = CompressionType::Uncompressed;
        let seg = Arc::new(ColumnSegment::create_transient(
            self.logical_type.clone(),
            segment_size,
            compression,
        ));
        self.allocation_size.fetch_add(segment_size, Ordering::Relaxed);
        let mut lock = self.data.lock();
        self.data.append_segment(&mut lock, seg, start_row);
    }

    /// Prepare `state` for appending to this column.
    ///
    /// Creates a transient segment if needed (empty tree or last is persistent).
    ///
    /// C++: `ColumnData::InitializeAppend`.
    pub fn initialize_append(&self, state: &mut ColumnAppendState) {
        // Use lock.0 directly to avoid re-entrant deadlock:
        // SegmentTree::get_last_segment internally calls self.nodes.lock() again.
        {
            let lock = self.data.lock();
            if lock.0.is_empty() {
                drop(lock);
                self.append_transient_segment(0);
            } else {
                let last = lock.0.last().unwrap();
                let seg = last.node();
                if seg.segment_type == ColumnSegmentType::Persistent {
                    let total = last.row_start() + seg.count();
                    drop(lock);
                    self.append_transient_segment(total);
                }
                // else: existing transient segment is reusable
            }
        }
        let lock = self.data.lock();
        state.current_segment_index = lock.0.last().map(|n| n.index());
    }

    /// Append `append_count` values from `vdata` into the active segment chain.
    ///
    /// Mirrors `void ColumnData::AppendData(BaseStatistics &append_stats,
    ///     ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t append_count)`.
    ///
    /// # Algorithm
    ///
    /// ```text
    /// offset = 0
    /// loop:
    ///   copied = current_segment.append(state, vdata, offset, remaining)
    ///   self.count += copied
    ///   append_stats.merge(current_segment.stats)
    ///   if copied == remaining → done
    ///
    ///   // segment full — allocate a new transient segment
    ///   next_row_start = current_node.row_start + current_segment.count
    ///   append_transient_segment(next_row_start)
    ///   state.current ← new last segment
    ///   new_segment.initialize_append(state)
    ///   offset += copied; remaining -= copied
    /// ```
    ///
    /// # Ownership model vs C++
    ///
    /// In C++ `state.current` is a raw `SegmentNode<ColumnSegment>*`; here it
    /// is `current_segment_index: Option<usize>`, an index into the
    /// `ColumnSegmentTree`.  We clone the `Arc<ColumnSegment>` out of the tree
    /// (requiring only a brief lock) and then call methods on the Arc—no lock
    /// is held during the actual segment write, matching C++ semantics.
    pub fn append_data(
        &self,
        append_stats: &mut SegmentStatistics,
        state: &mut ColumnAppendState,
        vdata: &UnifiedVectorFormat<'_>,
        append_count: Idx,
    ) {
        let mut offset: Idx = 0;
        let mut remaining: Idx = append_count;

        loop {
            // ── Step 1: borrow the current segment (brief lock to clone Arc) ──
            //
            // C++: `auto &append_segment = state.current->GetNode();`
            let seg_arc = {
                let lock = self.data.lock();
                let idx = state
                    .current_segment_index
                    .expect("append_data: ColumnAppendState not initialised; \
                             call initialize_append() first");
                lock.0[idx].arc()
            };

            // ── Step 2: write into the segment ────────────────────────────────
            //
            // C++: `idx_t copied_elements =
            //           append_segment.Append(state, vdata, offset, append_count);`
            //
            // `ColumnSegment::append` holds `&self` and uses Mutex-wrapped fields
            // for any internal state it modifies; the SegmentTree lock is NOT held.
            let copied_elements = seg_arc.append(state, vdata, offset, remaining);

            // ── Step 3: update column-level row count ─────────────────────────
            //
            // C++: `this->count += copied_elements;`
            self.count.fetch_add(copied_elements, Ordering::Relaxed);

            // ── Step 4: merge segment statistics into the caller's accumulator ─
            //
            // C++: `append_stats.Merge(append_segment.stats.statistics);`
            append_stats.merge(&seg_arc.stats.lock());

            if copied_elements == remaining {
                // All rows have been written — exit the loop.
                break;
            }

            // ── Step 5: segment full — create a new transient segment ─────────
            //
            // C++:
            //   auto l = data.Lock();
            //   AppendTransientSegment(l, state.current->GetRowStart()
            //                              + append_segment.count);
            //   state.current = data.GetLastSegment(l);
            //   state.current->GetNode().InitializeAppend(state);

            // Compute the start row of the new segment.
            // row_start is stored in the SegmentNode (not in ColumnSegment itself).
            let next_row_start = {
                let lock = self.data.lock();
                let idx = state.current_segment_index.unwrap();
                // C++: state.current->GetRowStart() + append_segment.count
                lock.0[idx].row_start() + seg_arc.count()
            };

            // Allocate the new transient segment (acquires + releases data lock internally).
            self.append_transient_segment(next_row_start);

            // Advance state.current to the newly appended segment and clone its Arc.
            let new_seg_arc = {
                let lock = self.data.lock();
                // C++: state.current = data.GetLastSegment(l);
                let last = lock.0.last().expect("segment list must be non-empty after append");
                state.current_segment_index = Some(last.index());
                last.arc()
            };

            // C++: state.current->GetNode().InitializeAppend(state);
            new_seg_arc.initialize_append(state);

            // Advance input cursor.
            offset    += copied_elements;
            remaining -= copied_elements;
        }
    }

    /// Undo rows beyond `new_count` by trimming the segment tree.
    ///
    /// C++: `ColumnData::RevertAppend`.
    pub fn revert_append(&self, new_count: Idx) {
        todo!(
            "ColumnDataContext::revert_append: truncate segment tree to {}",
            new_count
        )
    }

    // ── Statistics ────────────────────────────────────────────────────────────

    /// `true` if any segment is transient or there are pending MVCC updates.
    ///
    /// C++: `ColumnData::HasChanges` / `HasAnyChanges`.
    pub fn has_any_changes(&self) -> bool {
        let lock = self.data.lock();
        let transient = lock.0.iter().any(|n| {
            n.node().segment_type == ColumnSegmentType::Transient
        });
        transient || self.updates.lock().is_some()
    }

    /// Merge segment-level stats into `target`.
    ///
    /// C++: `ColumnData::MergeIntoStatistics`.
    pub fn merge_into_statistics(&self, _target: &TableStatistics) {
        // TODO: merge self.stats.statistics into target once statistics types
        // are fully implemented.
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    /// `true` if every segment has been checkpointed to disk.
    ///
    /// C++: `ColumnData::IsPersistent`.
    pub fn is_persistent(&self) -> bool {
        let lock = self.data.lock();
        lock.0.iter().all(|n| n.node().segment_type == ColumnSegmentType::Persistent)
    }

    /// Collect `DataPointer`s for all column segments.
    ///
    /// C++: `ColumnData::GetDataPointers`.
    pub fn get_data_pointers(&self) -> Vec<DataPointer> {
        let lock = self.data.lock();
        let mut row_start = 0u64;
        lock.0.iter().map(|n| {
            let seg = n.node();
            let dp = DataPointer {
                block_id: seg.block_id,
                offset: seg.block_offset as u32, // C++: block_pointer.offset (u32)
                row_start,
                tuple_count: seg.count(),
            };
            row_start += seg.count();
            dp
        }).collect()
    }

    /// Visits every segment in order together with its row start.
    pub fn for_each_segment<F>(&self, mut f: F)
    where
        F: FnMut(&ColumnSegment, Idx),
    {
        let lock = self.data.lock();
        let mut row_start = 0u64;
        for node in &*lock.0 {
            let seg = node.node();
            f(seg, row_start);
            row_start += seg.count();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnKindData
// ─────────────────────────────────────────────────────────────────────────────

/// Type-specific extra data for a `ColumnDataKind`.
///
/// Mirrors the per-subclass fields in C++.
pub enum ColumnKindData {
    /// Flat scalar column (`INTEGER`, `FLOAT`, `VARCHAR`, …).
    Standard {
        /// NULL bitmask child; absent for `NOT NULL` columns.
        validity: Option<Arc<ColumnDataKind>>,
    },
    /// NULL bitmask column (always a child of another column).
    Validity,
    /// Variable-length list column (`LIST(T)`).
    List {
        /// Offset + element data child.
        child_column: Arc<ColumnDataKind>,
        /// Top-level null bitmask.
        validity: Arc<ColumnDataKind>,
    },
    /// Fixed-length array column (`ARRAY(T, N)`).
    Array {
        /// Flat element data (length = `row_count * array_size`).
        child_column: Arc<ColumnDataKind>,
        /// Top-level null bitmask.
        validity: Arc<ColumnDataKind>,
        /// Fixed element count per row (the `N` in `ARRAY(T, N)`).
        array_size: u32,
    },
    /// Nested struct column (`STRUCT(a T1, b T2, …)`).
    Struct {
        /// One sub-column per field, in schema order.
        sub_columns: Vec<Arc<ColumnDataKind>>,
        /// Top-level null bitmask.
        validity: Arc<ColumnDataKind>,
    },
    /// JSON / Variant semi-structured column.
    Variant {
        /// Sub-columns: `[unshredded]` or `[unshredded, shredded]`.
        sub_columns: Vec<Arc<ColumnDataKind>>,
        /// Top-level null bitmask (optional).
        validity: Option<Arc<ColumnDataKind>>,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnDataKind  (the polymorphic column type)
// ─────────────────────────────────────────────────────────────────────────────

/// Polymorphic column storage — replaces C++ virtual dispatch.
///
/// Each instance owns:
/// - A `ColumnDataContext` with the segment tree, statistics, and MVCC log.
/// - A `ColumnKindData` with type-specific child column references.
///
/// Prefer using the `ColumnData` type alias for brevity.
pub struct ColumnDataKind {
    /// Shared base fields (segment tree, stats, MVCC log, …).
    pub ctx: ColumnDataContext,

    /// Type-specific extra data and child column references.
    pub kind: ColumnKindData,
}

/// Type alias so code written as `Arc<ColumnData>` continues to compile.
pub type ColumnData = ColumnDataKind;

impl ColumnDataKind {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Create a standard (flat scalar) column.
    pub fn standard(
        info: Arc<DataTableInfo>,
        column_index: Idx,
        logical_type: LogicalType,
        data_type: ColumnDataType,
        has_parent: bool,
    ) -> Arc<Self> {
        // Create validity child column (required by DuckDB format)
        let validity = Self::validity(info.clone(), column_index, data_type);

        Arc::new(ColumnDataKind {
            ctx: ColumnDataContext::new(info, column_index, logical_type, data_type, has_parent),
            kind: ColumnKindData::Standard { validity: Some(validity) },
        })
    }

    /// Create a validity (NULL bitmask) column (always a child).
    pub fn validity(
        info: Arc<DataTableInfo>,
        column_index: Idx,
        data_type: ColumnDataType,
    ) -> Arc<Self> {
        Arc::new(ColumnDataKind {
            ctx: ColumnDataContext::new(
                info,
                column_index,
                LogicalType { id: LogicalTypeId::Validity },
                data_type,
                true, // validity columns always have a parent
            ),
            kind: ColumnKindData::Validity,
        })
    }

    /// Factory: dispatch on `logical_type` to the appropriate column variant.
    ///
    /// Mirrors `ColumnData::CreateColumn`.
    pub fn create(
        info: Arc<DataTableInfo>,
        column_index: Idx,
        logical_type: LogicalType,
        data_type: ColumnDataType,
        has_parent: bool,
    ) -> Arc<Self> {
        // TODO: dispatch to Variant / Struct / List / Array / Validity based on
        // logical_type.id and InternalType.
        Self::standard(info, column_index, logical_type, data_type, has_parent)
    }

    // ── Context delegation ────────────────────────────────────────────────────

    /// Access the shared base context.
    #[inline]
    pub fn ctx(&self) -> &ColumnDataContext {
        &self.ctx
    }

    /// Total rows in this column.
    pub fn count(&self) -> Idx {
        self.ctx.count()
    }

    /// Bytes allocated for transient segments.
    pub fn allocation_size(&self) -> Idx {
        self.ctx.allocation_size()
    }

    /// `true` if there are any pending MVCC updates.
    pub fn has_updates(&self) -> bool {
        self.ctx.has_updates()
    }

    /// `true` if any segment is transient or has pending updates.
    pub fn has_any_changes(&self) -> bool {
        self.ctx.has_any_changes()
    }

    // ── Scan ──────────────────────────────────────────────────────────────────

    /// Set up `state` for a forward scan from row 0.
    pub fn initialize_scan(&self, state: &mut ColumnScanState) {
        self.ctx.initialize_scan(state);
    }

    /// Set up `state` for a forward scan from `row_idx`.
    pub fn initialize_scan_with_offset(&self, state: &mut ColumnScanState, row_idx: Idx) {
        self.ctx.initialize_scan_with_offset(state, row_idx);
    }

    /// Number of rows to scan for the given vector-index window.
    pub fn get_vector_count(&self, vector_index: Idx) -> Idx {
        self.ctx.get_vector_count(vector_index)
    }

    // ── Append ────────────────────────────────────────────────────────────────

    /// Prepare `state` for appending to this column.
    ///
    /// Also initialises child-column append states (validity, children).
    pub fn initialize_append(&self, state: &mut ColumnAppendState) {
        self.ctx.initialize_append(state);
        match &self.kind {
            ColumnKindData::Standard { validity } => {
                if let Some(v) = validity {
                    if state.child_appends.is_empty() {
                        state.child_appends.push(ColumnAppendState::default());
                    }
                    v.initialize_append(&mut state.child_appends[0]);
                }
            }
            ColumnKindData::List { child_column, validity } => {
                while state.child_appends.len() < 2 {
                    state.child_appends.push(ColumnAppendState::default());
                }
                validity.initialize_append(&mut state.child_appends[0]);
                child_column.initialize_append(&mut state.child_appends[1]);
            }
            ColumnKindData::Array { child_column, validity, .. } => {
                while state.child_appends.len() < 2 {
                    state.child_appends.push(ColumnAppendState::default());
                }
                validity.initialize_append(&mut state.child_appends[0]);
                child_column.initialize_append(&mut state.child_appends[1]);
            }
            ColumnKindData::Struct { sub_columns, validity } => {
                let n = sub_columns.len() + 1;
                while state.child_appends.len() < n {
                    state.child_appends.push(ColumnAppendState::default());
                }
                validity.initialize_append(&mut state.child_appends[0]);
                for (i, col) in sub_columns.iter().enumerate() {
                    col.initialize_append(&mut state.child_appends[i + 1]);
                }
            }
            ColumnKindData::Variant { sub_columns, validity } => {
                let n = sub_columns.len() + validity.is_some() as usize;
                while state.child_appends.len() < n {
                    state.child_appends.push(ColumnAppendState::default());
                }
                let mut idx = 0;
                if let Some(v) = validity {
                    v.initialize_append(&mut state.child_appends[idx]);
                    idx += 1;
                }
                for col in sub_columns {
                    col.initialize_append(&mut state.child_appends[idx]);
                    idx += 1;
                }
            }
            ColumnKindData::Validity => {}
        }
    }

    /// Append `append_count` values from `vdata`, updating `append_stats`.
    ///
    /// Delegates to `ColumnDataContext::append_data` and also appends to child columns.
    ///
    /// C++: `ColumnData::AppendData(BaseStatistics&, ColumnAppendState&,
    ///           UnifiedVectorFormat&, idx_t)`
    pub fn append(
        &self,
        append_stats: &mut SegmentStatistics,
        state: &mut ColumnAppendState,
        vdata: &UnifiedVectorFormat<'_>,
        append_count: Idx,
    ) {
        // Append to main column
        self.ctx.append_data(append_stats, state, vdata, append_count);

        // Append to child columns (validity, etc.)
        match &self.kind {
            ColumnKindData::Standard { validity } => {
                if let Some(v) = validity {
                    if !state.child_appends.is_empty() {
                        v.append(append_stats, &mut state.child_appends[0], vdata, append_count);
                    }
                }
            }
            ColumnKindData::List { child_column, validity } => {
                if state.child_appends.len() >= 2 {
                    validity.append(append_stats, &mut state.child_appends[0], vdata, append_count);
                    child_column.append(append_stats, &mut state.child_appends[1], vdata, append_count);
                }
            }
            ColumnKindData::Array { child_column, validity, .. } => {
                if state.child_appends.len() >= 2 {
                    validity.append(append_stats, &mut state.child_appends[0], vdata, append_count);
                    child_column.append(append_stats, &mut state.child_appends[1], vdata, append_count);
                }
            }
            ColumnKindData::Struct { sub_columns, validity } => {
                if !state.child_appends.is_empty() {
                    validity.append(append_stats, &mut state.child_appends[0], vdata, append_count);
                    for (i, col) in sub_columns.iter().enumerate() {
                        if i + 1 < state.child_appends.len() {
                            col.append(append_stats, &mut state.child_appends[i + 1], vdata, append_count);
                        }
                    }
                }
            }
            ColumnKindData::Variant { sub_columns, validity } => {
                let mut idx = 0;
                if let Some(v) = validity {
                    if idx < state.child_appends.len() {
                        v.append(append_stats, &mut state.child_appends[idx], vdata, append_count);
                    }
                    idx += 1;
                }
                for col in sub_columns {
                    if idx < state.child_appends.len() {
                        col.append(append_stats, &mut state.child_appends[idx], vdata, append_count);
                    }
                    idx += 1;
                }
            }
            ColumnKindData::Validity => {}
        }
    }

    /// Undo rows beyond `new_count`.
    pub fn revert_append(&self, new_count: Idx) {
        self.ctx.revert_append(new_count);
    }

    // ── Scan operations ───────────────────────────────────────────────────────

    /// Scan one vector of values into `result`.
    ///
    /// Mirrors `ColumnData::Scan(TransactionData, idx_t vector_index,
    ///     ColumnScanState&, Vector& result)`.
    ///
    /// Returns the number of rows actually scanned.
    ///
    /// # Current limitations
    /// - MVCC update merging is not yet implemented (`transaction` is ignored).
    /// - Only uncompressed transient segments are supported.
    pub fn scan(
        &self,
        _transaction: TransactionData,
        vector_index: Idx,
        state: &mut ColumnScanState,
        result: &mut Vector,
    ) -> Idx {
        // C++: auto target_count = GetVectorCount(vector_index)
        let target_count = self.ctx.get_vector_count(vector_index);
        // Determine scan type based on whether we need to scan across segments
        let scan_type = self.ctx.get_vector_scan_type(state, target_count);
        self.ctx.scan_vector(state, result, target_count, scan_type, 0)
    }

    /// Advance the scan cursor by one vector without producing output.
    ///
    /// Mirrors `ColumnData::Skip(ColumnScanState&, idx_t count)` which calls
    /// `state.Next(STANDARD_VECTOR_SIZE)` and follows segment boundaries.
    pub fn skip(&self, state: &mut ColumnScanState) {
        self.ctx.skip(state);
    }

    // ── Statistics ────────────────────────────────────────────────────────────

    /// Merge segment stats into the row-group-level `TableStatistics`.
    pub fn merge_into_statistics(&self, stats: &TableStatistics) {
        self.ctx.merge_into_statistics(stats);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PersistentColumnData
// ─────────────────────────────────────────────────────────────────────────────

/// Serialised form of one column's storage, produced during checkpoint.
///
/// Mirrors `struct PersistentColumnData` in `column_data.hpp`.
#[derive(Debug, Clone)]
pub struct PersistentColumnData {
    /// Physical storage type (C++: `PhysicalType physical_type`).
    pub physical_type: PhysicalType,

    /// Logical type identifier for deserialization context (C++: `LogicalTypeId`).
    pub logical_type_id: LogicalTypeId,

    /// Whether this column has uncommitted MVCC updates (C++: `bool has_updates`).
    pub has_updates: bool,

    /// On-disk block pointers, one per segment (C++: `vector<DataPointer> pointers`).
    pub pointers: Vec<DataPointer>,

    /// Child column data: `[validity, …type-specific children…]`
    ///
    /// C++: `vector<PersistentColumnData> child_columns`.
    pub child_columns: Vec<PersistentColumnData>,

    /// For `VARIANT` columns with shredding: the shredded struct type.
    ///
    /// C++: `LogicalType variant_shredded_type`.
    pub variant_shredded_type: Option<LogicalType>,
}

impl PersistentColumnData {
    /// Create from a `LogicalType`.
    pub fn new(logical_type: &LogicalType) -> Self {
        PersistentColumnData {
            physical_type: PhysicalType::Invalid,
            logical_type_id: logical_type.id,
            has_updates: false,
            pointers: Vec::new(),
            child_columns: Vec::new(),
            variant_shredded_type: None,
        }
    }

    /// `true` if this column or any child has uncommitted updates.
    ///
    /// C++: `PersistentColumnData::HasUpdates`.
    pub fn has_updates_recursive(&self) -> bool {
        self.has_updates || self.child_columns.iter().any(|c| c.has_updates_recursive())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PersistentRowGroupData
// ─────────────────────────────────────────────────────────────────────────────

/// Serialised form of one row group, used for checkpoint metadata.
///
/// Mirrors `struct PersistentRowGroupData` in `column_data.hpp`.
#[derive(Debug)]
pub struct PersistentRowGroupData {
    /// Column types in schema order (needed for deserialisation).
    pub types: Vec<LogicalType>,

    /// Per-column serialised storage data.
    pub column_data: Vec<PersistentColumnData>,

    /// Start row index of this row group.
    pub start: Idx,

    /// Number of rows in this row group.
    pub count: Idx,
}

impl PersistentRowGroupData {
    /// Create an empty record for a row group with the given column types.
    pub fn new(types: Vec<LogicalType>) -> Self {
        PersistentRowGroupData {
            types,
            column_data: Vec::new(),
            start: 0,
            count: 0,
        }
    }

    /// `true` if any column has uncommitted updates.
    pub fn has_updates(&self) -> bool {
        self.column_data.iter().any(|c| c.has_updates_recursive())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PersistentCollectionData
// ─────────────────────────────────────────────────────────────────────────────

/// Serialised form of an entire `RowGroupCollection`.
///
/// Mirrors `struct PersistentCollectionData` in `column_data.hpp`.
#[derive(Debug)]
pub struct PersistentCollectionData {
    /// Serialised data for each row group.
    pub row_group_data: Vec<PersistentRowGroupData>,
}

impl PersistentCollectionData {
    pub fn new() -> Self {
        PersistentCollectionData { row_group_data: Vec::new() }
    }

    /// `true` if any row group has uncommitted updates.
    pub fn has_updates(&self) -> bool {
        self.row_group_data.iter().any(|rg| rg.has_updates())
    }
}

impl Default for PersistentCollectionData {
    fn default() -> Self {
        Self::new()
    }
}
