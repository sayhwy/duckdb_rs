//! Per-vector transaction visibility info within a row group.
//!
//! Mirrors `duckdb/storage/table/chunk_info.hpp`.
//!
//! # Design notes
//!
//! C++ uses a class hierarchy: `ChunkInfo → ChunkConstantInfo / ChunkVectorInfo`.
//! In Rust we use an enum, eliminating virtual dispatch.
//!
//! `EMPTY_INFO` in C++ is represented as `None<ChunkInfo>` in Rust — callers
//! use `Option<ChunkInfo>`.

use super::types::{
    Idx, NOT_DELETED_ID, RowId, TRANSACTION_ID_START, TransactionData, TransactionId,
    is_row_visible, use_deleted_version, use_inserted_version,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public enum
// ─────────────────────────────────────────────────────────────────────────────

/// Visibility record for one SIMD vector (up to `STANDARD_VECTOR_SIZE` rows).
///
/// `None` corresponds to C++'s `EMPTY_INFO` — all rows are visible.
#[derive(Debug)]
pub enum ChunkInfo {
    /// All rows were inserted/deleted by the same transaction (fast path).
    Constant(ChunkConstantInfo),
    /// Per-row insert/delete transaction ids (general case).
    Vector(ChunkVectorInfo),
}

impl ChunkInfo {
    /// Row index of the first row in this chunk.
    pub fn start(&self) -> Idx {
        match self {
            ChunkInfo::Constant(c) => c.start,
            ChunkInfo::Vector(v) => v.start,
        }
    }

    /// Fills `sel_vector` with indices of rows visible to `transaction`.
    /// Returns the number of visible rows.
    pub fn get_sel_vector(
        &self,
        transaction: TransactionData,
        sel_vector: &mut SelectionVector,
        max_count: Idx,
    ) -> Idx {
        match self {
            ChunkInfo::Constant(c) => c.get_sel_vector(transaction, sel_vector, max_count),
            ChunkInfo::Vector(v) => v.get_sel_vector(transaction, sel_vector, max_count),
        }
    }

    /// Returns `true` if this chunk has any deleted rows.
    pub fn has_deletes(&self) -> bool {
        match self {
            ChunkInfo::Constant(c) => c.has_deletes(),
            ChunkInfo::Vector(v) => v.has_deletes(),
        }
    }

    /// Commits an append: marks rows `[start, end)` with `commit_id`.
    pub fn commit_append(&mut self, commit_id: TransactionId, start: Idx, end: Idx) {
        match self {
            ChunkInfo::Constant(c) => c.commit_append(commit_id, start, end),
            ChunkInfo::Vector(v) => v.commit_append(commit_id, start, end),
        }
    }

    /// Whether or not this `ChunkInfo` can be freed (all transactions that
    /// could see its state have advanced past `lowest_transaction`).
    pub fn can_cleanup(&self, lowest_transaction: TransactionId) -> bool {
        match self {
            ChunkInfo::Constant(c) => c.can_cleanup(lowest_transaction),
            ChunkInfo::Vector(v) => v.can_cleanup(lowest_transaction),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SelectionVector placeholder
// ─────────────────────────────────────────────────────────────────────────────

/// Minimal placeholder for DuckDB's `SelectionVector`.
/// Full implementation lives in the execution layer.
#[derive(Debug, Default)]
pub struct SelectionVector {
    pub sel: Vec<u32>,
}

// ─────────────────────────────────────────────────────────────────────────────
// ChunkConstantInfo
// ─────────────────────────────────────────────────────────────────────────────

/// All rows in the chunk were inserted / deleted by a single transaction.
///
/// Mirrors `ChunkConstantInfo`.
#[derive(Debug, Clone)]
pub struct ChunkConstantInfo {
    /// Row index of the first row (same as parent `start` field).
    pub start: Idx,
    /// The transaction that inserted all rows.
    /// `0` means committed-before-any-running-txn (always visible).
    pub insert_id: TransactionId,
    /// The transaction that deleted all rows, or `TransactionId::MAX` if not deleted.
    pub delete_id: TransactionId,
}

impl ChunkConstantInfo {
    pub const NOT_DELETED: TransactionId = TransactionId::MAX;

    pub fn new(start: Idx) -> Self {
        ChunkConstantInfo {
            start,
            insert_id: 0,
            delete_id: Self::NOT_DELETED,
        }
    }

    pub fn has_deletes(&self) -> bool {
        self.delete_id != Self::NOT_DELETED
    }

    /// Fill `sel_vector` with visible row indices (C++: `ChunkConstantInfo::GetSelVector`).
    ///
    /// Because every row in this chunk was inserted/deleted by the same transaction, the
    /// answer is uniform: either **all** rows are visible or **none** are.
    pub fn get_sel_vector(
        &self,
        transaction: TransactionData,
        sel_vector: &mut SelectionVector,
        max_count: Idx,
    ) -> Idx {
        if is_row_visible(transaction, self.insert_id, self.delete_id) {
            // All rows are visible — fill with [0, 1, …, max_count-1].
            sel_vector.sel.clear();
            for i in 0..max_count {
                sel_vector.sel.push(i as u32);
            }
            max_count
        } else {
            // No rows are visible.
            sel_vector.sel.clear();
            0
        }
    }

    /// Committed sel-vector: rows that are committed-inserted AND not committed-deleted.
    ///
    /// Mirrors `ChunkConstantInfo::GetCommittedSelVector` (CommittedVersionOperator).
    /// A row is "committed-inserted" if `insert_id < TRANSACTION_ID_START`.
    /// A row is "not committed-deleted" if `delete_id >= TRANSACTION_ID_START || delete_id == NOT_DELETED_ID`.
    pub fn get_committed_sel_vector(
        &self,
        _min_start_id: TransactionId,
        _min_transaction_id: TransactionId,
        sel_vector: &mut SelectionVector,
        max_count: Idx,
    ) -> Idx {
        let committed_inserted = self.insert_id < TRANSACTION_ID_START;
        let not_committed_deleted =
            self.delete_id >= TRANSACTION_ID_START || self.delete_id == NOT_DELETED_ID;
        if committed_inserted && not_committed_deleted {
            sel_vector.sel.clear();
            for i in 0..max_count {
                sel_vector.sel.push(i as u32);
            }
            max_count
        } else {
            sel_vector.sel.clear();
            0
        }
    }

    pub fn commit_append(&mut self, commit_id: TransactionId, _start: Idx, _end: Idx) {
        self.insert_id = commit_id;
    }

    /// Returns `true` when both the insert and delete (if any) are old enough that
    /// no active transaction can observe a different answer (C++: `ChunkConstantInfo::Cleanup`).
    pub fn can_cleanup(&self, lowest_transaction: TransactionId) -> bool {
        // insert is committed and old enough
        let insert_old = self.insert_id < lowest_transaction;
        // delete is either not present, or committed and old enough
        let delete_old = self.delete_id == NOT_DELETED_ID || self.delete_id < lowest_transaction;
        insert_old && delete_old
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ChunkVectorInfo
// ─────────────────────────────────────────────────────────────────────────────

/// Per-row insert/delete transaction ids for a single SIMD vector.
///
/// Mirrors `ChunkVectorInfo`.
///
/// # Design note
/// C++ stores `inserted_data` and `deleted_data` as `IndexPointer` handles
/// into a `FixedSizeAllocator`.  Here we use plain `Vec<TransactionId>` for
/// simplicity; a future iteration can switch to a pooled allocator.
#[derive(Debug)]
pub struct ChunkVectorInfo {
    /// Row index of the first row.
    pub start: Idx,

    /// Per-row insert transaction ids.
    /// `None` if all rows share `constant_insert_id`.
    inserted: Option<Vec<TransactionId>>,

    /// Single insert id covering all rows (used when `inserted` is `None`).
    constant_insert_id: TransactionId,

    /// Per-row delete transaction ids.
    /// `TransactionId::MAX` = not deleted.
    deleted: Vec<TransactionId>,
}

impl ChunkVectorInfo {
    pub fn new(start: Idx, insert_id: TransactionId, capacity: usize) -> Self {
        ChunkVectorInfo {
            start,
            inserted: None,
            constant_insert_id: insert_id,
            deleted: vec![TransactionId::MAX; capacity],
        }
    }

    pub fn has_deletes(&self) -> bool {
        self.deleted.iter().any(|&d| d != TransactionId::MAX)
    }

    pub fn set_all_deleted(&mut self, delete_id: TransactionId) {
        self.deleted.fill(delete_id);
    }

    pub fn any_deleted(&self) -> bool {
        self.has_deletes()
    }

    pub fn has_constant_insertion_id(&self) -> bool {
        self.inserted.is_none()
    }

    pub fn constant_insert_id(&self) -> TransactionId {
        self.constant_insert_id
    }

    /// Fill `sel_vector` with indices of rows visible to `transaction`.
    ///
    /// Mirrors `ChunkVectorInfo::TemplatedGetSelVector<TransactionVersionOperator>`.
    ///
    /// # Logic
    /// - If all rows share a constant insert-id and that insert is not visible → 0 rows.
    /// - If all rows share a constant insert-id and that insert IS visible → iterate only
    ///   `deleted` to filter out rows this transaction can already see as deleted.
    /// - If per-row insert-ids exist → check both `inserted[i]` and `deleted[i]` per row.
    pub fn get_sel_vector(
        &self,
        transaction: TransactionData,
        sel_vector: &mut SelectionVector,
        max_count: Idx,
    ) -> Idx {
        sel_vector.sel.clear();
        let start_time = transaction.start_time;
        let txn_id = transaction.transaction_id;

        match &self.inserted {
            None => {
                // ── Constant insert-id fast path ──────────────────────────────
                if !use_inserted_version(start_time, txn_id, self.constant_insert_id) {
                    // Insert not yet visible to us — no rows.
                    return 0;
                }
                // Insert visible: filter only on delete-ids.
                let mut count = 0u64;
                for i in 0..max_count as usize {
                    let del = self.deleted.get(i).copied().unwrap_or(NOT_DELETED_ID);
                    if use_deleted_version(start_time, txn_id, del) {
                        sel_vector.sel.push(i as u32);
                        count += 1;
                    }
                }
                count
            }
            Some(inserted) => {
                // ── Per-row insert-ids general path ───────────────────────────
                let mut count = 0u64;
                for i in 0..max_count as usize {
                    let ins = inserted.get(i).copied().unwrap_or(self.constant_insert_id);
                    let del = self.deleted.get(i).copied().unwrap_or(NOT_DELETED_ID);
                    if use_inserted_version(start_time, txn_id, ins)
                        && use_deleted_version(start_time, txn_id, del)
                    {
                        sel_vector.sel.push(i as u32);
                        count += 1;
                    }
                }
                count
            }
        }
    }

    /// Marks rows `[start, end)` as inserted with `commit_id`.
    pub fn append(&mut self, start: Idx, end: Idx, commit_id: TransactionId) {
        if start >= end {
            return;
        }
        let local_start = start.saturating_sub(self.start) as usize;
        let local_end = end.saturating_sub(self.start) as usize;
        let capacity = self.deleted.len();
        if local_start >= capacity {
            return;
        }
        let local_end = local_end.min(capacity);
        if self.inserted.is_none() {
            self.inserted = Some(vec![self.constant_insert_id; capacity]);
        }
        let inserted = self.inserted.as_mut().unwrap();
        for idx in local_start..local_end {
            inserted[idx] = commit_id;
        }
    }

    /// Marks rows in `rows[..count]` as deleted by `transaction_id`.
    /// Returns the number of rows actually deleted (skips already-deleted rows).
    pub fn delete_rows(
        &mut self,
        transaction_id: TransactionId,
        rows: &mut [RowId],
        count: usize,
    ) -> Idx {
        let mut deleted = 0;
        for idx_in_rows in 0..count {
            let row = rows[idx_in_rows];
            if row < 0 {
                continue;
            }
            let idx = row as usize;
            if idx >= self.deleted.len() {
                continue;
            }
            if self.deleted[idx] == NOT_DELETED_ID {
                self.deleted[idx] = transaction_id;
                rows[deleted as usize] = row;
                deleted += 1;
            }
        }
        deleted
    }

    pub fn commit_append(&mut self, commit_id: TransactionId, start: Idx, end: Idx) {
        if let Some(inserted) = self.inserted.as_mut() {
            let local_start = start.saturating_sub(self.start) as usize;
            let local_end = end.saturating_sub(self.start) as usize;
            let local_end = local_end.min(inserted.len());
            for idx in local_start.min(inserted.len())..local_end {
                inserted[idx] = commit_id;
            }
        } else {
            self.constant_insert_id = commit_id;
        }
    }

    pub fn set_deleted_rows(&mut self, rows: &[u16], delete_id: TransactionId) {
        for row in rows {
            let idx = *row as usize;
            if idx < self.deleted.len() {
                self.deleted[idx] = delete_id;
            }
        }
    }

    pub fn set_consecutive_deleted(&mut self, count: usize, delete_id: TransactionId) {
        for idx in 0..count.min(self.deleted.len()) {
            self.deleted[idx] = delete_id;
        }
    }

    pub fn can_cleanup(&self, lowest_transaction: TransactionId) -> bool {
        let inserts_old = match &self.inserted {
            Some(inserted) => inserted.iter().all(|&id| id < lowest_transaction),
            None => self.constant_insert_id < lowest_transaction,
        };
        let deletes_old = self
            .deleted
            .iter()
            .all(|&id| id == NOT_DELETED_ID || id < lowest_transaction);
        inserts_old && deletes_old
    }

    pub fn get_committed_sel_vector(
        &self,
        min_start_id: TransactionId,
        min_transaction_id: TransactionId,
        sel_vector: &mut SelectionVector,
        max_count: Idx,
    ) -> Idx {
        let _ = min_transaction_id;
        sel_vector.sel.clear();
        let max_count = max_count.min(self.deleted.len() as Idx) as usize;
        let mut visible = 0;
        match &self.inserted {
            None => {
                let committed_inserted = self.constant_insert_id < TRANSACTION_ID_START
                    && self.constant_insert_id < min_start_id;
                if !committed_inserted {
                    return 0;
                }
                for idx in 0..max_count {
                    let delete_id = self.deleted[idx];
                    let committed_deleted = delete_id != NOT_DELETED_ID
                        && delete_id < TRANSACTION_ID_START
                        && delete_id < min_start_id;
                    if !committed_deleted {
                        sel_vector.sel.push(idx as u32);
                        visible += 1;
                    }
                }
            }
            Some(inserted) => {
                for idx in 0..max_count {
                    let insert_id = inserted[idx];
                    let delete_id = self.deleted[idx];
                    let committed_inserted =
                        insert_id < TRANSACTION_ID_START && insert_id < min_start_id;
                    let committed_deleted = delete_id != NOT_DELETED_ID
                        && delete_id < TRANSACTION_ID_START
                        && delete_id < min_start_id;
                    if committed_inserted && !committed_deleted {
                        sel_vector.sel.push(idx as u32);
                        visible += 1;
                    }
                }
            }
        }
        visible
    }
}
