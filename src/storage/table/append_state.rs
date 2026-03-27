//! Append-state structs for column, row-group, and table appends.
//!
//! Mirrors `duckdb/storage/table/append_state.hpp`.
//!
//! # Hierarchy
//! ```text
//! TableAppendState
//!   └── row_group_append_state: RowGroupAppendState
//!         └── states: Vec<ColumnAppendState>
//!               └── append_state: Option<CompressionAppendState>
//! ```

use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};

use super::table_statistics::TableStatistics;
use super::types::{Idx, RowId, TransactionData};

// ─────────────────────────────────────────────────────────────────────────────
// ColumnAppendState
// ─────────────────────────────────────────────────────────────────────────────

/// Per-column cursor during an append operation.
///
/// Mirrors `struct ColumnAppendState`.
#[derive(Debug, Default)]
pub struct ColumnAppendState {
    /// Index of the current segment we are writing to.
    pub current_segment_index: Option<usize>,

    /// Codec-specific state (e.g. bit-packing write buffer).
    pub append_state: Option<Box<dyn CompressionAppendState>>,

    /// Child append states for nested columns (LIST, STRUCT, ARRAY).
    pub child_appends: Vec<ColumnAppendState>,
}

/// Codec-specific write state for compressed segment appends.
pub trait CompressionAppendState: Send + Sync + std::fmt::Debug {}

// ─────────────────────────────────────────────────────────────────────────────
// RowGroupAppendState
// ─────────────────────────────────────────────────────────────────────────────

/// Cursor for appending a batch of rows to a single row group.
///
/// Mirrors `struct RowGroupAppendState`.
#[derive(Debug)]
pub struct RowGroupAppendState {
    /// Index of the target row group within the segment tree.
    pub row_group_index: Option<usize>,

    /// Per-column append cursors (one per column in the table).
    pub states: Vec<ColumnAppendState>,

    /// How many rows have been written into this row group so far.
    pub offset_in_row_group: Idx,
}

impl RowGroupAppendState {
    pub fn new() -> Self {
        RowGroupAppendState {
            row_group_index: None,
            states: Vec::new(),
            offset_in_row_group: 0,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TableAppendState
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level append state for a table write operation.
///
/// Mirrors `struct TableAppendState`.
///
/// # Design notes vs. C++
///
/// | C++ | Rust | Notes |
/// |-----|------|-------|
/// | `unique_lock<mutex> append_lock` | `Option<parking_lot::MutexGuard>` | Held for append duration |
/// | `TableStatistics stats` | `TableStatistics` | |
/// | `Vector hashes` | `Vec<u64>` placeholder | |
/// | `TransactionData transaction` | `Option<TransactionData>` | Set in InitializeAppend |
pub struct TableAppendState {
    pub row_group_append_state: RowGroupAppendState,

    /// The first row appended by this operation.
    pub row_start: RowId,
    /// The current row being appended.
    pub current_row: RowId,
    /// Total rows appended so far.
    pub total_append_count: Idx,
    /// Row offset at the start of the current row group.
    pub row_group_start: Idx,

    /// Transaction performing the append.
    pub transaction: Option<TransactionData>,

    /// Per-column statistics accumulated during the append.
    pub stats: TableStatistics,

    /// Pre-computed hash values for constraint checking.
    pub hashes: Vec<u64>,
}

impl TableAppendState {
    pub fn new() -> Self {
        TableAppendState {
            row_group_append_state: RowGroupAppendState::new(),
            row_start: 0,
            current_row: 0,
            total_append_count: 0,
            row_group_start: 0,
            transaction: None,
            stats: TableStatistics::new(),
            hashes: Vec::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BoundConstraint
// ─────────────────────────────────────────────────────────────────────────────

/// A single verified/bound table constraint ready for evaluation.
///
/// Mirrors C++ `BoundConstraint` from `duckdb/planner/bound_constraint.hpp`.
///
/// Full implementation lives in the planner/binder layer; here we provide a
/// minimal placeholder so the storage layer can compile.
pub struct BoundConstraint;

// ─────────────────────────────────────────────────────────────────────────────
// ConstraintState
// ─────────────────────────────────────────────────────────────────────────────

/// Per-append constraint verification state.
///
/// Mirrors `struct ConstraintState` from `append_state.hpp`:
/// ```cpp
/// struct ConstraintState {
///     explicit ConstraintState(TableCatalogEntry &table_p,
///                              const vector<unique_ptr<BoundConstraint>> &bound_constraints);
/// };
/// ```
///
/// # C++ → Rust differences
/// - C++ keeps a `reference<TableCatalogEntry>` for looking up active
///   constraints at verification time.  Here we store the table name as a
///   cheap identifier; a full implementation would store the bound constraint
///   expressions.
pub struct ConstraintState {
    /// Owning table name (used for error messages).
    pub table_name: String,
    // TODO: Vec<BoundConstraint> for actual constraint evaluation
}

impl ConstraintState {
    /// Mirrors `ConstraintState(TableCatalogEntry&, vector<unique_ptr<BoundConstraint>>&)`.
    pub fn new(table_name: impl Into<String>, _bound_constraints: &[BoundConstraint]) -> Self {
        Self { table_name: table_name.into() }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalAppendState
// ─────────────────────────────────────────────────────────────────────────────

/// Append state for transaction-local (not-yet-committed) writes.
///
/// Mirrors `struct LocalAppendState` from `append_state.hpp`:
/// ```cpp
/// struct LocalAppendState {
///     TableAppendState  append_state;
///     LocalTableStorage *storage;
///     unique_ptr<ConstraintState> constraint_state;
/// };
/// ```
///
/// # C++ → Rust differences
///
/// | C++ | Rust |
/// |-----|------|
/// | `LocalTableStorage *storage` | `storage: Option<Arc<Mutex<LocalTableStorage>>>` |
/// | `unique_ptr<ConstraintState> constraint_state` | `constraint_state: Option<Box<ConstraintState>>` |
pub struct LocalAppendState {
    /// Low-level append cursor into the local `RowGroupCollection`.
    pub append_state: TableAppendState,

    /// The transaction-local storage being written to.
    ///
    /// Set by `LocalStorage::initialize_append_state`; stays `None` until
    /// `initialize_local_append` has been called.
    pub storage: Option<Arc<Mutex<crate::storage::local_storage::LocalTableStorage>>>,

    /// Constraint verification state for this insert batch.
    ///
    /// Set by `DataTable::initialize_local_append`; `None` means constraints
    /// have not yet been bound (or the table has no constraints).
    pub constraint_state: Option<Box<ConstraintState>>,
}

impl LocalAppendState {
    pub fn new() -> Self {
        LocalAppendState {
            append_state: TableAppendState::new(),
            storage: None,
            constraint_state: None,
        }
    }
}

impl Default for LocalAppendState {
    fn default() -> Self { Self::new() }
}
