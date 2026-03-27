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
//! | `optional_ptr<SegmentNode<RowGroup>>` | `usize` (index) + reference to tree |

use std::sync::Arc;

use parking_lot::Mutex;

use super::chunk_info::SelectionVector;
use super::types::{Idx, LogicalType, TransactionData};
use crate::storage::buffer::BufferHandle;

// ─────────────────────────────────────────────────────────────────────────────
// SegmentScanState
// ─────────────────────────────────────────────────────────────────────────────

/// Codec-specific scan cursor for one `ColumnSegment`.
///
/// Mirrors `struct SegmentScanState`.
pub trait SegmentScanState: Send + Sync + std::fmt::Debug {}

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
    /// Set by `ColumnDataContext::begin_scan_vector_internal` when a segment is
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
    /// segment boundaries must do so explicitly (see `ColumnDataContext::skip`).
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
#[derive(Debug)]
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
#[derive(Debug, Default)]
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
        self.column_has_filter.get(col_idx).copied().unwrap_or(false)
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
/// (for `column_ids`, `filters`, `options`, `sampling_info`). In Rust we inline those
/// fields here to avoid the circular reference.
#[derive(Debug)]
pub struct CollectionScanState {
    /// Index of the current row group node in the segment tree.
    pub row_group_index: Option<usize>,

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

    /// Projected column ids — indices into the table's physical column list.
    ///
    /// Mirrors C++ `TableScanState::column_ids` (accessible via
    /// `CollectionScanState::GetColumnIds()` in C++).
    pub column_ids: Vec<u64>,

    /// Scratch selection-vector for MVCC visibility filtering (C++: `valid_sel`).
    pub valid_sel: SelectionVector,

    /// Pushed-down filter predicates (C++: `ScanFilterInfo`, via `GetFilterInfo()`).
    pub filters: ScanFilterInfo,

    /// Scan-level option flags (C++: `TableScanOptions`, via `GetOptions()`).
    pub options: TableScanOptions,

    /// Sampling configuration (C++: `ScanSamplingInfo`, via `GetSamplingInfo()`).
    pub sampling_info: ScanSamplingInfo,
}

impl CollectionScanState {
    pub fn new() -> Self {
        CollectionScanState {
            row_group_index: None,
            vector_index: 0,
            max_row_group_row: 0,
            column_scans: Vec::new(),
            max_row: u64::MAX,
            batch_index: 0,
            column_ids: Vec::new(),
            valid_sel: SelectionVector::default(),
            filters: ScanFilterInfo::default(),
            options: TableScanOptions::default(),
            sampling_info: ScanSamplingInfo::default(),
        }
    }

    /// Initialise `column_scans` to the correct length for `types`.
    pub fn initialize(&mut self, types: &[LogicalType]) {
        self.column_scans = types.iter().map(|_| ColumnScanState::new()).collect();
    }

    /// Set column ids and resize `column_scans` accordingly.
    pub fn set_column_ids(&mut self, ids: Vec<u64>) {
        let n = ids.len();
        self.column_ids = ids;
        self.column_scans.resize_with(n, ColumnScanState::new);
    }

    pub fn get_column_ids(&self) -> &[u64] {
        &self.column_ids
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TableScanState
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level scan state for a full table scan.
///
/// Mirrors `class TableScanState`.
#[derive(Debug)]
pub struct TableScanState {
    /// Scan cursor over the persisted row-group collection.
    pub table_state: CollectionScanState,
    /// Scan cursor over the transaction-local row-group collection.
    pub local_state: CollectionScanState,
    /// Scan options.
    pub options: TableScanOptions,
    /// Filter predicates.
    pub filters: ScanFilterInfo,
    /// Sampling configuration.
    pub sampling_info: ScanSamplingInfo,
    /// Which columns to project.
    column_ids: Vec<u64>,
}

impl TableScanState {
    pub fn new() -> Self {
        TableScanState {
            table_state: CollectionScanState::new(),
            local_state: CollectionScanState::new(),
            options: TableScanOptions::default(),
            filters: ScanFilterInfo::default(),
            sampling_info: ScanSamplingInfo::default(),
            column_ids: Vec::new(),
        }
    }

    pub fn initialize(&mut self, column_ids: Vec<u64>) {
        self.column_ids = column_ids;
    }

    pub fn column_ids(&self) -> &[u64] {
        &self.column_ids
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
    /// Index of the current row group (atomically advanced by workers).
    pub current_row_group_index: Arc<std::sync::atomic::AtomicU64>,
    pub vector_index: Idx,
    pub max_row: Idx,
    pub batch_index: Idx,
    pub processed_rows: std::sync::atomic::AtomicU64,
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
