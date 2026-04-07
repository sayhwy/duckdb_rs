//! `UpdateSegment` - per-column in-memory delta store for MVCC updates.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::{Mutex, RwLock};

use crate::common::types::Vector;
use crate::storage::storage_info::{StorageError, StorageResult};
use crate::transaction::update_info::UpdateInfo as TxnUpdateInfo;

use super::types::{Idx, RowId, STANDARD_VECTOR_SIZE, TransactionData, TransactionId};

static NEXT_SEGMENT_ID: AtomicU64 = AtomicU64::new(1);
static SEGMENT_REGISTRY: OnceLock<Mutex<HashMap<u64, Weak<UpdateSegment>>>> = OnceLock::new();

fn segment_registry() -> &'static Mutex<HashMap<u64, Weak<UpdateSegment>>> {
    SEGMENT_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub struct UpdateSegment {
    inner: RwLock<UpdateSegmentInner>,
    pub type_size: Idx,
    pub segment_id: u64,
}

struct UpdateSegmentInner {
    root: UpdateNode,
    stats: UpdateStatistics,
}

impl UpdateSegment {
    pub fn new(type_size: Idx) -> Self {
        Self {
            inner: RwLock::new(UpdateSegmentInner {
                root: UpdateNode::new(),
                stats: UpdateStatistics::default(),
            }),
            type_size,
            segment_id: NEXT_SEGMENT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn register(this: &Arc<Self>) {
        segment_registry()
            .lock()
            .insert(this.segment_id, Arc::downgrade(this));
    }

    pub fn lookup(segment_id: u64) -> Option<Arc<Self>> {
        segment_registry()
            .lock()
            .get(&segment_id)
            .and_then(Weak::upgrade)
    }

    pub fn has_updates(&self) -> bool {
        !self.inner.read().root.info.is_empty()
    }

    pub fn has_uncommitted_updates(&self, vector_index: Idx) -> bool {
        self.has_updates_at(vector_index)
    }

    pub fn has_updates_at(&self, vector_index: Idx) -> bool {
        self.inner
            .read()
            .root
            .info
            .get(vector_index as usize)
            .map(|entry| entry.is_some())
            .unwrap_or(false)
    }

    pub fn has_updates_in_range(&self, start_row: Idx, end_row: Idx) -> bool {
        let start_vector = start_row / STANDARD_VECTOR_SIZE;
        let end_vector = (end_row.saturating_sub(1)) / STANDARD_VECTOR_SIZE;
        (start_vector..=end_vector).any(|vector_idx| self.has_updates_at(vector_idx))
    }

    pub fn fetch_updates(
        &self,
        _transaction: TransactionData,
        vector_index: Idx,
        result: &mut [u8],
    ) {
        self.fetch_committed(vector_index, result);
    }

    pub fn fetch_committed(&self, vector_index: Idx, result: &mut [u8]) {
        if self.type_size == 0 {
            return;
        }
        let guard = self.inner.read();
        let Some(Some(info)) = guard.root.info.get(vector_index as usize) else {
            return;
        };
        apply_update_info(info, self.type_size as usize, result);
    }

    pub fn fetch_committed_range(&self, start_row: Idx, count: Idx, result: &mut [u8]) {
        if self.type_size == 0 || count == 0 {
            return;
        }
        let type_size = self.type_size as usize;
        for idx in 0..count as usize {
            let row_id = start_row + idx as u64;
            self.fetch_row(
                TransactionData {
                    start_time: 0,
                    transaction_id: 0,
                },
                row_id,
                result,
                idx,
            );
            let _ = type_size;
        }
    }

    pub fn fetch_row(
        &self,
        _transaction: TransactionData,
        row_id: Idx,
        result: &mut [u8],
        result_idx: usize,
    ) {
        if self.type_size == 0 {
            return;
        }
        let type_size = self.type_size as usize;
        let vector_index = row_id / STANDARD_VECTOR_SIZE;
        let row_in_vector = (row_id % STANDARD_VECTOR_SIZE) as u32;
        let dst_off = result_idx * type_size;
        let guard = self.inner.read();
        let Some(Some(info)) = guard.root.info.get(vector_index as usize) else {
            return;
        };
        if let Some(pos) = info.ids.iter().position(|id| *id == row_in_vector) {
            let src_off = pos * type_size;
            if dst_off + type_size <= result.len() && src_off + type_size <= info.data.len() {
                result[dst_off..dst_off + type_size]
                    .copy_from_slice(&info.data[src_off..src_off + type_size]);
            }
        }
    }

    pub fn update(
        &self,
        transaction_handle: Option<
            &Arc<crate::transaction::duck_transaction_manager::DuckTxnHandle>,
        >,
        transaction: TransactionData,
        table: &crate::storage::data_table::DataTable,
        column_index: Idx,
        update_vector: &Vector,
        row_ids: &[RowId],
        update_count: usize,
        row_group_start: Idx,
    ) -> StorageResult<()> {
        let type_size = self.type_size as usize;
        if type_size == 0 {
            return Err(StorageError::Other(
                "UpdateSegment::update: invalid fixed-size payload".to_string(),
            ));
        }
        if update_count == 0 {
            return Ok(());
        }

        let raw = update_vector.raw_data();
        let expected_bytes = update_count * type_size;
        if raw.len() < expected_bytes {
            return Err(StorageError::Other(format!(
                "UpdateSegment::update: payload too short ({} < {})",
                raw.len(),
                expected_bytes
            )));
        }
        if row_ids.len() < update_count {
            return Err(StorageError::Other(format!(
                "UpdateSegment::update: row_ids too short ({} < {})",
                row_ids.len(),
                update_count
            )));
        }

        let mut start = 0usize;
        while start < update_count {
            let first_relative_row = (row_ids[start] as u64)
                .checked_sub(row_group_start)
                .ok_or_else(|| {
                    StorageError::Other(
                        "UpdateSegment::update: row_id before row_group_start".to_string(),
                    )
                })?;
            let vector_index = first_relative_row / STANDARD_VECTOR_SIZE;
            let mut end = start + 1;
            while end < update_count {
                let relative_row = (row_ids[end] as u64)
                    .checked_sub(row_group_start)
                    .ok_or_else(|| {
                        StorageError::Other(
                            "UpdateSegment::update: row_id before row_group_start".to_string(),
                        )
                    })?;
                if relative_row / STANDARD_VECTOR_SIZE != vector_index {
                    break;
                }
                end += 1;
            }

            let entry_count = end - start;
            let mut txn_info = TxnUpdateInfo::new(
                self.segment_id,
                table.info.table_id(),
                transaction.transaction_id,
                column_index,
                row_group_start,
                entry_count as u16,
                type_size,
            );
            txn_info.vector_index = vector_index;
            txn_info.n = entry_count as u16;

            let mut ids = Vec::with_capacity(entry_count);
            let mut data = vec![0u8; entry_count * type_size];
            for (dst_idx, src_idx) in (start..end).enumerate() {
                let relative_row = (row_ids[src_idx] as u64)
                    .checked_sub(row_group_start)
                    .ok_or_else(|| {
                        StorageError::Other(
                            "UpdateSegment::update: row_id before row_group_start".to_string(),
                        )
                    })?;
                let row_in_vector = (relative_row % STANDARD_VECTOR_SIZE) as u32;
                ids.push(row_in_vector);
                txn_info.tuples.push(row_in_vector);
                let src_off = src_idx * type_size;
                let dst_off = dst_idx * type_size;
                data[dst_off..dst_off + type_size]
                    .copy_from_slice(&raw[src_off..src_off + type_size]);
                txn_info.values[dst_off..dst_off + type_size]
                    .copy_from_slice(&raw[src_off..src_off + type_size]);
            }

            {
                let mut guard = self.inner.write();
                let slot = ensure_update_slot(&mut guard.root, vector_index as usize);
                let previous = slot.take();
                let info = Box::new(UpdateInfo {
                    version_number: transaction.transaction_id,
                    ids,
                    data,
                    prev: previous,
                    next_version: 0,
                });
                *slot = Some(info);
                guard.stats.has_no_null = true;
            }

            if let Some(handle) = transaction_handle {
                let mut payload = vec![0u8; txn_info.serialized_size()];
                txn_info.serialize(&mut payload);
                handle.lock_inner().push_update_payload(payload);
            }

            start = end;
        }

        Ok(())
    }

    pub fn rollback_update(&self, info: &TxnUpdateInfo) {
        let mut guard = self.inner.write();
        let Some(slot) = guard.root.info.get_mut(info.vector_index as usize) else {
            return;
        };
        let Some(current) = slot.take() else {
            return;
        };
        if current.ids == info.tuples
            && current.version_number == info.version_number.load(Ordering::Relaxed)
        {
            *slot = current.prev;
        } else {
            *slot = Some(current);
        }
    }

    pub fn cleanup_update(&self, _info: &TxnUpdateInfo) {}

    pub fn commit_update(&self, info: &TxnUpdateInfo, commit_id: TransactionId) {
        let mut guard = self.inner.write();
        if let Some(Some(current)) = guard.root.info.get_mut(info.vector_index as usize) {
            if current.ids == info.tuples {
                current.version_number = commit_id;
            }
        }
    }

    pub fn revert_commit(&self, info: &TxnUpdateInfo, transaction_id: TransactionId) {
        let mut guard = self.inner.write();
        if let Some(Some(current)) = guard.root.info.get_mut(info.vector_index as usize) {
            if current.ids == info.tuples {
                current.version_number = transaction_id;
            }
        }
    }

    pub fn get_statistics(&self) -> UpdateStatistics {
        self.inner.read().stats.clone()
    }
}

impl Drop for UpdateSegment {
    fn drop(&mut self) {
        segment_registry().lock().remove(&self.segment_id);
    }
}

pub struct UpdateNode {
    pub info: Vec<Option<Box<UpdateInfo>>>,
}

impl UpdateNode {
    pub fn new() -> Self {
        Self { info: Vec::new() }
    }
}

#[derive(Debug)]
pub struct UpdateInfo {
    pub version_number: TransactionId,
    pub ids: Vec<u32>,
    pub data: Vec<u8>,
    pub prev: Option<Box<UpdateInfo>>,
    pub next_version: u64,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateStatistics {
    pub has_null: bool,
    pub has_no_null: bool,
}

fn ensure_update_slot(root: &mut UpdateNode, vector_index: usize) -> &mut Option<Box<UpdateInfo>> {
    if root.info.len() <= vector_index {
        root.info.resize_with(vector_index + 1, || None);
    }
    &mut root.info[vector_index]
}

fn apply_update_info(info: &UpdateInfo, type_size: usize, result: &mut [u8]) {
    for (idx, row_in_vector) in info.ids.iter().copied().enumerate() {
        let dst_off = row_in_vector as usize * type_size;
        let src_off = idx * type_size;
        if dst_off + type_size <= result.len() && src_off + type_size <= info.data.len() {
            result[dst_off..dst_off + type_size]
                .copy_from_slice(&info.data[src_off..src_off + type_size]);
        }
    }
}
