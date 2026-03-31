//! `RowVersionManager` — MVCC delete/insert visibility per row group.
//!
//! Mirrors `duckdb/storage/table/row_version_manager.hpp`.
//!
//! Stores a sparse array of `ChunkInfo` entries — one per SIMD vector —
//! tracking which rows were inserted/deleted by which transaction.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use super::chunk_info::{ChunkConstantInfo, ChunkInfo, ChunkVectorInfo, SelectionVector};
use super::types::{
    Idx, MetaBlockPointer, RowId, TransactionData, TransactionId, TRANSACTION_ID_START,
};

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
    pub version_info_id: u64,
}

struct RvmInner {
    /// Sparse: `None` means no version info (all rows visible).
    vector_info: Vec<Option<ChunkInfo>>,
    has_unserialized_changes: bool,
    storage_pointers: Vec<MetaBlockPointer>,
}

static NEXT_VERSION_INFO_ID: AtomicU64 = AtomicU64::new(1);
static VERSION_INFO_REGISTRY: OnceLock<Mutex<HashMap<u64, Weak<RowVersionManager>>>> = OnceLock::new();

fn version_info_registry() -> &'static Mutex<HashMap<u64, Weak<RowVersionManager>>> {
    VERSION_INFO_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

impl RowVersionManager {
    pub fn new() -> Self {
        RowVersionManager {
            inner: Mutex::new(RvmInner {
                vector_info: Vec::new(),
                has_unserialized_changes: false,
                storage_pointers: Vec::new(),
            }),
            version_info_id: NEXT_VERSION_INFO_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn register(this: &Arc<Self>) {
        version_info_registry()
            .lock()
            .insert(this.version_info_id, Arc::downgrade(this));
    }

    pub fn lookup(version_info_id: u64) -> Option<Arc<Self>> {
        version_info_registry()
            .lock()
            .get(&version_info_id)
            .and_then(Weak::upgrade)
    }

    // ── Query ─────────────────────────────────────────────────

    /// Count rows that are committed-deleted (visible to checkpoint).
    pub fn get_committed_deleted_count(&self, count: Idx) -> Idx {
        let inner = self.inner.lock();
        let mut deleted = 0;
        for (vector_idx, info) in inner.vector_info.iter().enumerate() {
            let Some(info) = info else {
                continue;
            };
            let read_count = vector_idx as Idx * super::types::STANDARD_VECTOR_SIZE;
            if read_count >= count {
                break;
            }
            let max_count = (count - read_count).min(super::types::STANDARD_VECTOR_SIZE);
            let mut sel = SelectionVector::default();
            let visible = match info {
                ChunkInfo::Constant(c) => {
                    c.get_committed_sel_vector(TRANSACTION_ID_START, TRANSACTION_ID_START, &mut sel, max_count)
                }
                ChunkInfo::Vector(v) => {
                    v.get_committed_sel_vector(TRANSACTION_ID_START, TRANSACTION_ID_START, &mut sel, max_count)
                }
            };
            deleted += max_count - visible;
        }
        deleted
    }

    /// Returns `true` if this row group should be checkpointed.
    pub fn should_checkpoint_row_group(&self, checkpoint_id: TransactionId, count: Idx) -> bool {
        let checkpoint_txn = TransactionData {
            start_time: checkpoint_id,
            transaction_id: checkpoint_id,
        };
        let inner = self.inner.lock();
        let mut total_count = 0;
        let mut read_count = 0;
        let mut vector_idx = 0usize;
        while read_count < count {
            let max_count = (count - read_count).min(super::types::STANDARD_VECTOR_SIZE);
            let checkpoint_count = match inner.vector_info.get(vector_idx).and_then(|entry| entry.as_ref()) {
                None => max_count,
                Some(info) => {
                    let mut sel = SelectionVector::default();
                    info.get_sel_vector(checkpoint_txn, &mut sel, max_count)
                }
            };
            if checkpoint_count > 0 {
                if total_count != read_count {
                    panic!(
                        "partial checkpointed row-group entry at vector {} (checkpoint_count={}, total_count={}, read_count={})",
                        vector_idx, checkpoint_count, total_count, read_count
                    );
                }
                total_count += checkpoint_count;
            }
            read_count += super::types::STANDARD_VECTOR_SIZE;
            vector_idx += 1;
        }
        if total_count == 0 {
            return false;
        }
        if total_count != count {
            panic!(
                "RowVersionManager::should_checkpoint_row_group encountered partially checkpointed entry (checkpoint_count={}, row_group_count={})",
                total_count, count
            );
        }
        true
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
        let info = inner
            .vector_info
            .get(vector_idx as usize)
            .and_then(|v| v.as_ref());
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
        let inner = self.inner.lock();
        match inner
            .vector_info
            .get(vector_idx as usize)
            .and_then(|entry| entry.as_ref())
        {
            None => {
                sel_vector.sel.clear();
                for idx in 0..max_count {
                    sel_vector.sel.push(idx as u32);
                }
                max_count
            }
            Some(ChunkInfo::Constant(info)) => {
                info.get_committed_sel_vector(start_time, transaction_id, sel_vector, max_count)
            }
            Some(ChunkInfo::Vector(info)) => {
                info.get_committed_sel_vector(start_time, transaction_id, sel_vector, max_count)
            }
        }
    }

    /// Returns whether a single row should be included for `transaction`.
    pub fn fetch(&self, transaction: TransactionData, row: Idx) -> bool {
        let inner = self.inner.lock();
        let vector_idx = (row / super::types::STANDARD_VECTOR_SIZE) as usize;
        let row_in_vector = row % super::types::STANDARD_VECTOR_SIZE;
        let Some(info) = inner.vector_info.get(vector_idx).and_then(|entry| entry.as_ref()) else {
            return true;
        };
        let mut sel = SelectionVector::default();
        let visible = info.get_sel_vector(transaction, &mut sel, row_in_vector + 1);
        visible > 0 && sel.sel.iter().any(|&idx| idx as Idx == row_in_vector)
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
        if count == 0 || row_group_end <= row_group_start {
            return;
        }
        let mut inner = self.inner.lock();
        inner.has_unserialized_changes = true;
        let start_vector_idx = (row_group_start / super::types::STANDARD_VECTOR_SIZE) as usize;
        let end_vector_idx = ((row_group_end - 1) / super::types::STANDARD_VECTOR_SIZE) as usize;
        while inner.vector_info.len() <= end_vector_idx {
            inner.vector_info.push(None);
        }
        for vector_idx in start_vector_idx..=end_vector_idx {
            let vector_base = vector_idx as Idx * super::types::STANDARD_VECTOR_SIZE;
            let vector_start = if vector_idx == start_vector_idx {
                row_group_start - vector_base
            } else {
                0
            };
            let vector_end = if vector_idx == end_vector_idx {
                row_group_end - vector_base
            } else {
                super::types::STANDARD_VECTOR_SIZE
            };
            if vector_start == 0 && vector_end == super::types::STANDARD_VECTOR_SIZE {
                let mut info = ChunkConstantInfo::new(vector_base);
                info.insert_id = transaction.transaction_id;
                info.delete_id = super::types::NOT_DELETED_ID;
                inner.vector_info[vector_idx] = Some(ChunkInfo::Constant(info));
            } else {
                let entry = self.get_or_create_vector_info(&mut inner, vector_idx as Idx);
                if let ChunkInfo::Vector(vector) = entry {
                    vector.append(
                        vector_base + vector_start,
                        vector_base + vector_end,
                        transaction.transaction_id,
                    );
                }
            }
        }
    }

    /// Commit a previous append: mark rows `[row_group_start, row_group_start + count)`.
    pub fn commit_append(&self, commit_id: TransactionId, row_group_start: Idx, count: Idx) {
        if count == 0 {
            return;
        }
        let row_group_end = row_group_start + count;
        let mut inner = self.inner.lock();
        let start_vector_idx = (row_group_start / super::types::STANDARD_VECTOR_SIZE) as usize;
        let end_vector_idx = ((row_group_end - 1) / super::types::STANDARD_VECTOR_SIZE) as usize;
        for vector_idx in start_vector_idx..=end_vector_idx {
            let vector_base = vector_idx as Idx * super::types::STANDARD_VECTOR_SIZE;
            let vstart = if vector_idx == start_vector_idx {
                row_group_start - vector_base
            } else {
                0
            };
            let vend = if vector_idx == end_vector_idx {
                row_group_end - vector_base
            } else {
                super::types::STANDARD_VECTOR_SIZE
            };
            if let Some(info) = inner.vector_info.get_mut(vector_idx).and_then(|entry| entry.as_mut()) {
                info.commit_append(commit_id, vector_base + vstart, vector_base + vend);
            }
        }
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
        if count == 0 {
            return;
        }
        let row_group_end = row_group_start + count;
        let mut inner = self.inner.lock();
        let start_vector_idx = (row_group_start / super::types::STANDARD_VECTOR_SIZE) as usize;
        let end_vector_idx = ((row_group_end - 1) / super::types::STANDARD_VECTOR_SIZE) as usize;
        for vector_idx in start_vector_idx..=end_vector_idx {
            let Some(info) = inner.vector_info.get(vector_idx).and_then(|entry| entry.as_ref()) else {
                continue;
            };
            if info.can_cleanup(lowest_active_transaction) {
                inner.has_unserialized_changes = true;
                inner.vector_info[vector_idx] = None;
            }
        }
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
        let mut inner = self.inner.lock();
        inner.has_unserialized_changes = true;
        let entry = self.get_or_create_vector_info(&mut inner, vector_idx);
        match entry {
            ChunkInfo::Constant(constant) => {
                let mut vector = ChunkVectorInfo::new(
                    constant.start,
                    constant.insert_id,
                    super::types::STANDARD_VECTOR_SIZE as usize,
                );
                if constant.delete_id != super::types::NOT_DELETED_ID {
                    vector.set_all_deleted(constant.delete_id);
                }
                let deleted = vector.delete_rows(transaction_id, rows, count as usize);
                *entry = ChunkInfo::Vector(vector);
                deleted
            }
            ChunkInfo::Vector(vector) => vector.delete_rows(transaction_id, rows, count as usize),
        }
    }

    pub fn rollback_delete(&self, info: &crate::transaction::delete_info::DeleteInfo) {
        self.update_delete_ids(info, super::types::NOT_DELETED_ID);
    }

    pub fn commit_delete(&self, info: &crate::transaction::delete_info::DeleteInfo, commit_id: TransactionId) {
        self.update_delete_ids(info, commit_id);
    }

    pub fn cleanup_delete(
        &self,
        _info: &crate::transaction::delete_info::DeleteInfo,
        _lowest_active_transaction: TransactionId,
    ) {
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

    fn get_or_create_vector_info<'a>(
        &self,
        inner: &'a mut RvmInner,
        vector_idx: Idx,
    ) -> &'a mut ChunkInfo {
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

    fn update_delete_ids(
        &self,
        info: &crate::transaction::delete_info::DeleteInfo,
        delete_id: TransactionId,
    ) {
        let mut inner = self.inner.lock();
        let Some(entry) = inner
            .vector_info
            .get_mut(info.vector_idx as usize)
            .and_then(|entry| entry.as_mut())
        else {
            return;
        };
        match entry {
            ChunkInfo::Constant(constant) => {
                constant.delete_id = delete_id;
            }
            ChunkInfo::Vector(vector) => {
                if info.is_consecutive {
                    vector.set_consecutive_deleted(info.count as usize, delete_id);
                } else if let Some(rows) = &info.rows {
                    vector.set_deleted_rows(rows, delete_id);
                }
            }
        }
    }
}

impl Default for RowVersionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for RowVersionManager {
    fn drop(&mut self) {
        version_info_registry().lock().remove(&self.version_info_id);
    }
}
