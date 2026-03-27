//! `RowVersionManager` — MVCC delete/insert visibility per row group.
//!
//! Mirrors `duckdb/storage/table/row_version_manager.hpp`.
//!
//! Stores a sparse array of `ChunkInfo` entries — one per SIMD vector —
//! tracking which rows were inserted/deleted by which transaction.

use parking_lot::Mutex;

use super::chunk_info::{ChunkInfo, ChunkVectorInfo, SelectionVector};
use super::types::{Idx, MetaBlockPointer, RowId, TransactionData, TransactionId};

// ─────────────────────────────────────────────────────────────────────────────
// RowVersionManager
// ─────────────────────────────────────────────────────────────────────────────

/// Per-row-group insert/delete version info.
///
/// Mirrors `class RowVersionManager`.
///
/// # Design notes vs. C++
///
/// | C++ | Rust |
/// |-----|------|
/// | `mutex version_lock` | `Mutex<RowVersionManagerInner>` |
/// | `FixedSizeAllocator allocator` | placeholder — `Vec<ChunkInfo>` directly |
/// | `vector<unique_ptr<ChunkInfo>> vector_info` | `inner.vector_info: Vec<Option<ChunkInfo>>` |
/// | `bool has_unserialized_changes` | `inner.has_unserialized_changes` |
/// | `vector<MetaBlockPointer> storage_pointers` | `inner.storage_pointers` |
pub struct RowVersionManager {
    inner: Mutex<RvmInner>,
}

struct RvmInner {
    /// Sparse: `None` means no version info (all rows visible).
    vector_info: Vec<Option<ChunkInfo>>,
    has_unserialized_changes: bool,
    storage_pointers: Vec<MetaBlockPointer>,
}

impl RowVersionManager {
    pub fn new() -> Self {
        RowVersionManager {
            inner: Mutex::new(RvmInner {
                vector_info: Vec::new(),
                has_unserialized_changes: false,
                storage_pointers: Vec::new(),
            }),
        }
    }

    // ── Query ─────────────────────────────────────────────────

    /// Count rows that are committed-deleted (visible to checkpoint).
    pub fn get_committed_deleted_count(&self, count: Idx) -> Idx {
        todo!("sum deleted-count over all vectors up to `count` rows")
    }

    /// Returns `true` if this row group should be checkpointed.
    pub fn should_checkpoint_row_group(&self, checkpoint_id: TransactionId, count: Idx) -> bool {
        todo!("check if any vector has changes after checkpoint_id")
    }

    /// Fill `sel_vector` with row indices visible to `transaction`.
    pub fn get_sel_vector(
        &self,
        transaction: TransactionData,
        vector_idx: Idx,
        sel_vector: &mut SelectionVector,
        max_count: Idx,
    ) -> Idx {
        let inner = self.inner.lock();
        let info = inner.vector_info.get(vector_idx as usize).and_then(|v| v.as_ref());
        match info {
            None => {
                // no version info → all rows visible
                for i in 0..max_count {
                    sel_vector.sel.push(i as u32);
                }
                max_count
            }
            Some(chunk) => chunk.get_sel_vector(transaction, sel_vector, max_count),
        }
    }

    /// Fill `sel_vector` with rows visible to committed readers.
    pub fn get_committed_sel_vector(
        &self,
        start_time: TransactionId,
        transaction_id: TransactionId,
        vector_idx: Idx,
        sel_vector: &mut SelectionVector,
        max_count: Idx,
    ) -> Idx {
        todo!()
    }

    /// Returns whether a single row should be included for `transaction`.
    pub fn fetch(&self, transaction: TransactionData, row: Idx) -> bool {
        todo!()
    }

    // ── Append lifecycle ──────────────────────────────────────

    /// Record that `count` rows were appended (creates `ChunkConstantInfo` entries).
    pub fn append_version_info(
        &self,
        transaction: TransactionData,
        count: Idx,
        row_group_start: Idx,
        row_group_end: Idx,
    ) {
        todo!("create ChunkConstantInfo entries for newly appended vectors")
    }

    /// Commit a previous append: mark rows `[row_group_start, row_group_start + count)`.
    pub fn commit_append(&self, commit_id: TransactionId, row_group_start: Idx, count: Idx) {
        todo!()
    }

    /// Revert an append: drop any version info past `new_count`.
    pub fn revert_append(&self, new_count: Idx) {
        let mut inner = self.inner.lock();
        let keep_vectors = (new_count as usize + super::types::STANDARD_VECTOR_SIZE as usize - 1)
            / super::types::STANDARD_VECTOR_SIZE as usize;
        inner.vector_info.truncate(keep_vectors);
    }

    pub fn cleanup_append(
        &self,
        lowest_active_transaction: TransactionId,
        row_group_start: Idx,
        count: Idx,
    ) {
        todo!()
    }

    // ── Delete ────────────────────────────────────────────────

    /// Delete rows in `rows[..count]` from vector `vector_idx`.
    /// Returns the number of rows actually deleted.
    pub fn delete_rows(
        &self,
        vector_idx: Idx,
        transaction_id: TransactionId,
        rows: &mut [RowId],
        count: Idx,
    ) -> Idx {
        todo!("upsert ChunkVectorInfo at vector_idx and call delete_rows on it")
    }

    // ── Persistence ───────────────────────────────────────────

    /// Serialise to metadata blocks; returns written block pointers.
    pub fn checkpoint(&self, _manager: &()) -> Vec<MetaBlockPointer> {
        todo!("write ChunkInfo entries to metadata blocks, return pointers")
    }

    pub fn has_unserialized_changes(&self) -> bool {
        self.inner.lock().has_unserialized_changes
    }

    pub fn get_storage_pointers(&self) -> Vec<MetaBlockPointer> {
        self.inner.lock().storage_pointers.clone()
    }

    // ── Internal ──────────────────────────────────────────────

    fn get_or_create_vector_info<'a>(&self, inner: &'a mut RvmInner, vector_idx: Idx) -> &'a mut ChunkInfo {
        let idx = vector_idx as usize;
        while inner.vector_info.len() <= idx {
            inner.vector_info.push(None);
        }
        if inner.vector_info[idx].is_none() {
            inner.vector_info[idx] = Some(ChunkInfo::Vector(ChunkVectorInfo::new(
                vector_idx * super::types::STANDARD_VECTOR_SIZE,
                0,
                super::types::STANDARD_VECTOR_SIZE as usize,
            )));
        }
        inner.vector_info[idx].as_mut().unwrap()
    }
}

impl Default for RowVersionManager {
    fn default() -> Self {
        Self::new()
    }
}
