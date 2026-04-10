//! Scan-state structs for column, row-group, and table scans.
//!
//! Mirrors `duckdb/storage/table/scan_state.hpp`.
//!
//! # Hierarchy
//! ```text
//! TableScanState
//!   ├── table_state: CollectionScanState
//!   │     ├── row_group: Option<SegmentNodeRef<RowGroup>>
//!   │     └── column_scans: Vec<ColumnScanState>
//!   └── local_state: CollectionScanState  (transaction-local)
//! ```
//!
//! # Design notes vs. C++
//!
//! | C++ | Rust |
//! |-----|------|
//! | raw `ColumnSegmentTree *` pointer | `Option<Arc<Mutex<...>>>` |
//! | `unsafe_vector<ColumnScanState>` | `Vec<ColumnScanState>` |
//! | `unique_ptr<SegmentScanState>` | `Option<Box<dyn SegmentScanState>>` |
//! | `optional_ptr<SegmentNode<RowGroup>>` | `RowGroupSegmentRef` |

use std::sync::Arc;
use std::any::Any;

use parking_lot::Mutex;

use super::chunk_info::SelectionVector;
use super::row_group::RowGroup;
use super::row_group_collection::RowGroupSegmentTree;
use super::segment_base::SegmentBase;
use super::types::{Idx, LogicalType, TransactionData};
use crate::storage::buffer::BufferHandle;

// ─────────────────────────────────────────────────────────────────────────────
// SegmentScanState
// ─────────────────────────────────────────────────────────────────────────────

/// Codec-specific scan cursor for one `ColumnSegment`.
///
/// Mirrors `struct SegmentScanState`.
pub trait SegmentScanState: Send + Sync + std::fmt::Debug + Any {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnScanState
// ─────────────────────────────────────────────────────────────────────────────

/// Cursor for scanning one column within a row group.
///
/// Mirrors `struct ColumnScanState`.
#[derive(Debug, Default)]
pub struct ColumnScanState {
    /// Index of the current segment node inside the segment tree.
    pub current_segment_index: Option<usize>,

    /// Absolute row offset within this column (monotonically increases).
    pub offset_in_column: Idx,

    /// Row start of the current segment (C++: `current->GetRowStart()`).
    ///
    /// Set by `ColumnDataBase::begin_scan_vector_internal` when a segment is
    /// initialized.  Used by `position_in_segment()` to compute the per-segment
    /// row offset without holding the segment tree lock.
    pub segment_row_start: Idx,

    /// Mirrors C++ `internal_index`: the absolute row number up to which the
    /// codec scan cursor has been advanced.  Set to `row_start` at segment init
    /// and to `offset_in_column` after `Skip`.
    ///
    /// Used in `begin_scan_vector_internal` to decide whether a Skip is needed.
    pub internal_index: Idx,

    /// Codec-specific scan state for the active segment.
    pub scan_state: Option<Box<dyn SegmentScanState>>,

    /// Codec states kept alive for segments we already scanned in the same
    /// `DataChunk` window (C++: `previous_states`).
    pub previous_states: Vec<Box<dyn SegmentScanState>>,

    /// Recursive child states for LIST / STRUCT / ARRAY columns.
    pub child_states: Vec<ColumnScanState>,

    /// `true` once `InitializeScan` has been called for the current segment.
    pub initialized: bool,

    /// `true` if the current segment has already been zone-map checked.
    pub segment_checked: bool,

    /// Last read child-offset (used by LIST columns only).
    pub last_offset: Idx,

    /// Which child columns to scan (for STRUCT columns).
    pub scan_child_column: Vec<bool>,

    /// RAII pin guard for persistent segment scans.
    ///
    /// Holds the block in the BufferPool (via reader-count increment) for the
    /// duration of this vector scan.  Set by `ColumnSegment::initialize_scan`
    /// when the segment is persistent; dropped (unpinned) when a new segment is
    /// initialized or when this `ColumnScanState` is dropped.
    ///
    /// Mirrors the `BufferHandle handle` stored inside C++ `FixedSizeScanState`.
    pub pinned_buffer: Option<BufferHandle>,
}

impl ColumnScanState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advance the scan cursor by `count` rows (including child states).
    ///
    /// Mirrors `ColumnScanState::Next(idx_t count)` in C++.
    /// Note: does **not** update `current_segment_index` — callers that cross
    /// segment boundaries must do so explicitly (see `ColumnDataBase::skip`).
    pub fn next(&mut self, count: Idx) {
        self.offset_in_column += count;
        self.internal_index += count;
        for child in &mut self.child_states {
            child.next(count);
        }
    }

    /// Row offset within the current segment (0-based).
    ///
    /// Mirrors `ColumnScanState::GetPositionInSegment()`:
    /// ```cpp
    /// return offset_in_column - (current ? current->GetRowStart() : 0);
    /// ```
    pub fn position_in_segment(&self) -> Idx {
        self.offset_in_column.saturating_sub(self.segment_row_start)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnFetchState
// ─────────────────────────────────────────────────────────────────────────────

/// State for point-lookup fetches (used by UPDATE, DELETE, index scans).
///
/// Mirrors `struct ColumnFetchState`.
#[derive(Debug, Default)]
pub struct ColumnFetchState {
    /// Cached buffer handles keyed by block id.
    pub handles: std::collections::HashMap<i64, ()>, // TODO: actual BufferHandle

    /// Child fetch states for nested columns.
    pub child_states: Vec<ColumnFetchState>,

    /// The current row group being fetched (index in tree).
    pub row_group_index: Option<usize>,
}

// ─────────────────────────────────────────────────────────────────────────────
// ScanFilter
// ─────────────────────────────────────────────────────────────────────────────

/// One pushed-down predicate for a scan.
///
/// Mirrors `struct ScanFilter`.
#[derive(Debug, Clone)]
pub struct ScanFilter {
    /// Column index in the *scan projection*.
    pub scan_column_index: Idx,
    /// Column index in the *table schema*.
    pub table_column_index: Idx,
    /// Whether the filter is always true and can be skipped.
    pub always_true: bool,
    // TODO: TableFilter + TableFilterState
}

// ─────────────────────────────────────────────────────────────────────────────
// ScanFilterInfo
// ─────────────────────────────────────────────────────────────────────────────

/// Collection of active scan filters for a query.
///
/// Mirrors `class ScanFilterInfo`.
#[derive(Debug, Default, Clone)]
pub struct ScanFilterInfo {
    pub filter_list: Vec<ScanFilter>,
    /// Per-column: is any filter currently active?
    pub column_has_filter: Vec<bool>,
    /// Count of filters that are currently always-true.
    pub always_true_filters: usize,
}

impl ScanFilterInfo {
    pub fn has_filters(&self) -> bool {
        self.always_true_filters < self.filter_list.len()
    }

    pub fn column_has_filters(&self, col_idx: usize) -> bool {
        self.column_has_filter
            .get(col_idx)
            .copied()
            .unwrap_or(false)
    }

    pub fn set_filter_always_true(&mut self, filter_idx: usize) {
        if let Some(f) = self.filter_list.get_mut(filter_idx) {
            if !f.always_true {
                f.always_true = true;
                self.always_true_filters += 1;
            }
        }
    }

    pub fn check_all_filters(&mut self) {
        self.always_true_filters = 0;
        for f in &mut self.filter_list {
            f.always_true = false;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TableScanOptions  /  ScanSamplingInfo
// ─────────────────────────────────────────────────────────────────────────────

/// Scan-level configuration flags.
#[derive(Debug, Default, Clone)]
pub struct TableScanOptions {
    /// Fetch rows one-at-a-time instead of vectorised batches.
    pub force_fetch_row: bool,
}

/// Bernoulli / system sampling configuration.
#[derive(Debug, Default, Clone)]
pub struct ScanSamplingInfo {
    pub do_system_sample: bool,
    pub sample_rate: f64,
}

// ─────────────────────────────────────────────────────────────────────────────
// CollectionScanState
// ─────────────────────────────────────────────────────────────────────────────

/// Cursor for scanning an entire `RowGroupCollection`.
///
/// Mirrors `class CollectionScanState`.
///
/// # C++ → Rust differences
///
/// In C++, `CollectionScanState` holds a back-pointer to the parent `TableScanState`
/// (for `column_ids`, `filters`, `options`, `sampling_info`). In Rust we share these
/// fields through `TableScanSharedState` to avoid self-referential structs.
#[derive(Debug, Default)]
struct TableScanSharedState {
    column_ids: Vec<u64>,
    options: TableScanOptions,
    filters: ScanFilterInfo,
    sampling_info: ScanSamplingInfo,
}

#[derive(Clone)]
pub struct RowGroupSegmentRef {
    pub row_start: Idx,
    pub index: usize,
    pub row_group: Arc<RowGroup>,
}

pub struct CollectionScanState {
    /// Current row group node being scanned.
    pub current_row_group: Option<RowGroupSegmentRef>,

    /// Row-group segment tree being scanned.
    pub row_groups: Option<Arc<RowGroupSegmentTree>>,

    /// Current vector index within the row group (C++: `vector_index`).
    pub vector_index: Idx,

    /// Number of rows to scan in the current row group (C++: `max_row_group_row`).
    pub max_row_group_row: Idx,

    /// Per-column scan cursors (one entry per projected column).
    pub column_scans: Vec<ColumnScanState>,

    /// Overall row upper bound for the scan (C++: `max_row`).
    /// Defaults to `u64::MAX` (scan all rows).
    pub max_row: Idx,

    /// Current output batch index for parallel scans.
    pub batch_index: Idx,

    /// Scratch selection-vector for MVCC visibility filtering (C++: `valid_sel`).
    pub valid_sel: SelectionVector,

    shared: Arc<Mutex<TableScanSharedState>>,
}

impl CollectionScanState {
    fn with_shared(shared: Arc<Mutex<TableScanSharedState>>) -> Self {
        CollectionScanState {
            current_row_group: None,
            row_groups: None,
            vector_index: 0,
            max_row_group_row: 0,
            column_scans: Vec::new(),
            max_row: u64::MAX,
            batch_index: 0,
            valid_sel: SelectionVector::default(),
            shared,
        }
    }

    pub fn new() -> Self {
        Self::with_shared(Arc::new(Mutex::new(TableScanSharedState::default())))
    }

    /// Initialise `column_scans` to the correct length for `types`.
    pub fn initialize(&mut self, types: &[LogicalType]) {
        self.column_scans = types.iter().map(|_| ColumnScanState::new()).collect();
    }

    /// Set column ids and resize `column_scans` accordingly.
    pub fn set_column_ids(&mut self, ids: Vec<u64>) {
        let n = ids.len();
        self.shared.lock().column_ids = ids;
        self.column_scans.resize_with(n, ColumnScanState::new);
    }

    pub fn get_column_ids(&self) -> Vec<u64> {
        self.shared.lock().column_ids.clone()
    }

    pub fn get_filter_info(&self) -> ScanFilterInfo {
        self.shared.lock().filters.clone()
    }

    pub fn get_options(&self) -> TableScanOptions {
        self.shared.lock().options.clone()
    }

    pub fn get_sampling_info(&self) -> ScanSamplingInfo {
        self.shared.lock().sampling_info.clone()
    }

    pub fn scan(&mut self, transaction: TransactionData, result: &mut crate::common::types::DataChunk) -> bool {
        let Some(tree) = self.row_groups.clone() else {
            return false;
        };

        loop {
            let current = match self.current_row_group.clone() {
                None => return false,
                Some(current) => current,
            };

            let (rg_row_start, rg_count): (Idx, Idx) = {
                let lock = tree.lock();
                match lock.0.get(current.index) {
                    None => return false,
                    Some(node) => (node.row_start(), node.node().count()),
                }
            };

            current.row_group.scan(transaction, self, result);
            if result.size() > 0 {
                return true;
            }

            if self.max_row <= rg_row_start + rg_count {
                self.current_row_group = None;
                return false;
            }

            let mut curr_idx = current.index;
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
                        self.current_row_group = None;
                        return false;
                    }
                    Some((next_idx, next_row_start, next_arc)) => {
                        if next_row_start >= self.max_row {
                            self.current_row_group = None;
                            return false;
                        }
                        self.current_row_group = Some(RowGroupSegmentRef {
                            row_start: next_row_start,
                            index: next_idx,
                            row_group: Arc::clone(&next_arc),
                        });
                        curr_idx = next_idx;
                        if next_arc.initialize_scan(self, next_row_start) {
                            break;
                        }
                    }
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TableScanState
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level scan state for a full table scan.
///
/// Mirrors `class TableScanState`.
pub struct TableScanState {
    /// Scan cursor over the persisted row-group collection.
    pub table_state: CollectionScanState,
    /// Scan cursor over the transaction-local row-group collection.
    pub local_state: CollectionScanState,
    shared: Arc<Mutex<TableScanSharedState>>,
}

impl TableScanState {
    pub fn new() -> Self {
        let shared = Arc::new(Mutex::new(TableScanSharedState::default()));
        TableScanState {
            table_state: CollectionScanState::with_shared(Arc::clone(&shared)),
            local_state: CollectionScanState::with_shared(Arc::clone(&shared)),
            shared,
        }
    }

    pub fn initialize(&mut self, column_ids: Vec<u64>) {
        self.table_state.set_column_ids(column_ids.clone());
        self.local_state.set_column_ids(column_ids.clone());
        self.shared.lock().column_ids = column_ids;
    }

    pub fn column_ids(&self) -> Vec<u64> {
        self.shared.lock().column_ids.clone()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ParallelCollectionScanState / ParallelTableScanState
// ─────────────────────────────────────────────────────────────────────────────

/// Shared scan state for parallel collection scans.
///
/// Mirrors `struct ParallelCollectionScanState`.
/// Protected by its own `Mutex`.
pub struct ParallelCollectionScanState {
    /// Collection row-group tree.
    pub row_groups: Option<Arc<RowGroupSegmentTree>>,
    /// Last row group claimed from the shared scan state.
    pub current_row_group: Option<RowGroupSegmentRef>,
    /// Next row group index to hand out.
    pub next_row_group_index: Idx,
    pub vector_index: Idx,
    pub max_row: Idx,
    pub batch_index: Idx,
    pub processed_rows: Idx,
    pub lock: Mutex<()>,
}

/// Top-level parallel scan state.
///
/// Mirrors `struct ParallelTableScanState`.
pub struct ParallelTableScanState {
    pub scan_state: ParallelCollectionScanState,
    pub local_state: ParallelCollectionScanState,
}

// ─────────────────────────────────────────────────────────────────────────────
// PrefetchState
// ─────────────────────────────────────────────────────────────────────────────

/// Pre-fetch hints: a list of blocks to load ahead of a scan.
///
/// Mirrors `struct PrefetchState`.
#[derive(Debug, Default)]
pub struct PrefetchState {
    /// Block ids to prefetch.
    pub blocks: Vec<i64>,
}
