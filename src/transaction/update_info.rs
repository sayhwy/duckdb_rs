//! Update 操作的 Undo 条目载荷。
//!
//! 对应 C++: `duckdb/transaction/update_info.hpp`
//!
//! # C++ 内存布局（行内灵活数组）
//!
//! C++ 通过 `GetAllocSize()` 在 UndoBuffer 中一次性分配：
//! ```text
//! [ UpdateInfo (固定字段) | sel_t tuples[max] | T data[max] ]
//! ```
//! 三段连续内存，通过裸指针偏移访问。
//!
//! Rust 拆分为三个独立字段（无 unsafe）：
//! ```text
//! UpdateInfo { fixed_fields, tuples: Vec<u32>, values: Vec<u8> }
//! ```
//! 序列化/反序列化时保持相同的字节布局。
//!
//! # MVCC 链表
//!
//! `UpdateInfo` 通过 `prev/next`（`UndoBufferPointer`）形成版本链：
//! ```text
//! base_version → older_update → … → newest_update
//! ```
//! 扫描时从 newest 向 oldest 遍历，找到对当前事务可见的版本。

use super::types::{Idx, SelT, TransactionId};
use super::undo_buffer_allocator::UndoBufferPointer;
use std::sync::atomic::{AtomicU64, Ordering};

/// Update 操作的单版本记录（C++: `struct UpdateInfo`）。
///
/// 表示对一个 **向量**（最多 2048 行）中 **若干行** 的 **单列** 更新，
/// 是 MVCC 版本链的一个节点。
pub struct UpdateInfo {
    /// 所在的更新段 ID（C++: `UpdateSegment *segment`）。
    pub segment_id: u64,

    /// 所在的数据表 ID（C++: `DataTable *table`）。
    pub table_id: u64,

    /// 被更新的列下标（C++: `idx_t column_index`）。
    pub column_index: Idx,

    /// 所在 RowGroup 的起始行号（C++: `idx_t row_group_start`）。
    pub row_group_start: Idx,

    /// 在向量组内的向量下标（C++: `idx_t vector_index`）。
    pub vector_index: Idx,

    /// 创建此版本的事务 ID 或 commit ID
    /// （C++: `atomic<transaction_t> version_number`）。
    pub version_number: AtomicU64,

    /// 前一个（更旧）版本（C++: `UndoBufferPointer prev`）。
    pub prev: UndoBufferPointer,

    /// 下一个（更新）版本（C++: `UndoBufferPointer next`）。
    pub next: UndoBufferPointer,

    /// 本次实际更新的行数（C++: `sel_t N`）。
    pub n: u16,

    /// 该 UpdateInfo 能容纳的最大行数（C++: `sel_t max`）。
    pub max: u16,

    /// 更新行的向量内偏移，升序排列（C++: `sel_t *GetTuples()`）。
    pub tuples: Vec<SelT>,

    /// 更新后的列值（原始字节，C++: `data_ptr_t GetValues()`）。
    pub values: Vec<u8>,
}

impl UpdateInfo {
    /// 创建新的 UpdateInfo。
    pub fn new(
        segment_id: u64,
        table_id: u64,
        transaction_id: TransactionId,
        column_index: Idx,
        row_group_start: Idx,
        max: u16,
        value_type_size: usize,
    ) -> Self {
        Self {
            segment_id,
            table_id,
            column_index,
            row_group_start,
            vector_index: 0,
            version_number: AtomicU64::new(transaction_id),
            prev: UndoBufferPointer::null(),
            next: UndoBufferPointer::null(),
            n: 0,
            max,
            tuples: Vec::with_capacity(max as usize),
            values: vec![0u8; max as usize * value_type_size],
        }
    }

    /// 此版本对给定事务是否可见。
    pub fn applies_to_transaction(
        &self,
        start_time: TransactionId,
        transaction_id: TransactionId,
    ) -> bool {
        let v = self.version_number.load(Ordering::Acquire);
        v > start_time && v != transaction_id
    }

    /// 是否存在前驱版本。
    pub fn has_prev(&self) -> bool {
        self.prev.is_set()
    }

    /// 是否存在后继版本。
    pub fn has_next(&self) -> bool {
        self.next.is_set()
    }

    /// 从 UndoBuffer 载荷字节反序列化。
    ///
    /// 格式：固定字段 + tuples + values
    pub fn deserialize(bytes: &[u8], value_type_size: usize) -> Self {
        // 固定字段大小
        let fixed_size = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 2 + 2; // segment_id + table_id + column_index + row_group_start + vector_index + version_number + prev + next + n + max

        assert!(bytes.len() >= fixed_size, "UpdateInfo payload too short");

        let segment_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let table_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let column_index = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let row_group_start = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let vector_index = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let version_number = u64::from_le_bytes(bytes[40..48].try_into().unwrap());

        // prev 和 next 指针（简化处理）
        let prev_idx = u64::from_le_bytes(bytes[48..56].try_into().unwrap());
        let prev_pos = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
        let prev = if prev_idx < u64::MAX {
            UndoBufferPointer {
                slab_index: Some(prev_idx as usize),
                position: prev_pos as usize,
            }
        } else {
            UndoBufferPointer::null()
        };

        let next_idx = u64::from_le_bytes(bytes[64..72].try_into().unwrap());
        let next_pos = u64::from_le_bytes(bytes[72..80].try_into().unwrap());
        let next = if next_idx < u64::MAX {
            UndoBufferPointer {
                slab_index: Some(next_idx as usize),
                position: next_pos as usize,
            }
        } else {
            UndoBufferPointer::null()
        };

        let n = u16::from_le_bytes(bytes[80..82].try_into().unwrap());
        let max = u16::from_le_bytes(bytes[82..84].try_into().unwrap());

        // 读取 tuples
        let tuples_start = fixed_size;
        let tuples_end = tuples_start + (n as usize) * 4;
        assert!(
            bytes.len() >= tuples_end,
            "UpdateInfo tuples data too short"
        );

        let mut tuples = Vec::with_capacity(n as usize);
        for i in 0..n as usize {
            let offset = tuples_start + i * 4;
            tuples.push(u32::from_le_bytes(
                bytes[offset..offset + 4].try_into().unwrap(),
            ));
        }

        // 读取 values
        let values_start = tuples_end;
        let values_end = values_start + (n as usize) * value_type_size;
        assert!(
            bytes.len() >= values_end,
            "UpdateInfo values data too short"
        );

        let values = bytes[values_start..values_end].to_vec();

        Self {
            segment_id,
            table_id,
            column_index,
            row_group_start,
            vector_index,
            version_number: AtomicU64::new(version_number),
            prev,
            next,
            n,
            max,
            tuples,
            values,
        }
    }

    /// 序列化到 UndoBuffer 载荷字节。
    pub fn serialize(&self, out: &mut [u8]) {
        let fixed_size = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 2 + 2;
        let tuples_size = (self.n as usize) * 4;
        let values_size = self.values.len();
        let total = fixed_size + tuples_size + values_size;

        assert!(out.len() >= total, "UpdateInfo output buffer too short");

        out[0..8].copy_from_slice(&self.segment_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.table_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.column_index.to_le_bytes());
        out[24..32].copy_from_slice(&self.row_group_start.to_le_bytes());
        out[32..40].copy_from_slice(&self.vector_index.to_le_bytes());
        out[40..48].copy_from_slice(&self.version_number.load(Ordering::Relaxed).to_le_bytes());

        // prev 指针
        if let Some(idx) = self.prev.slab_index {
            out[48..56].copy_from_slice(&(idx as u64).to_le_bytes());
            out[56..64].copy_from_slice(&(self.prev.position as u64).to_le_bytes());
        } else {
            out[48..56].copy_from_slice(&u64::MAX.to_le_bytes());
            out[56..64].copy_from_slice(&0u64.to_le_bytes());
        }

        // next 指针
        if let Some(idx) = self.next.slab_index {
            out[64..72].copy_from_slice(&(idx as u64).to_le_bytes());
            out[72..80].copy_from_slice(&(self.next.position as u64).to_le_bytes());
        } else {
            out[64..72].copy_from_slice(&u64::MAX.to_le_bytes());
            out[72..80].copy_from_slice(&0u64.to_le_bytes());
        }

        out[80..82].copy_from_slice(&self.n.to_le_bytes());
        out[82..84].copy_from_slice(&self.max.to_le_bytes());

        // tuples
        for (i, tuple) in self.tuples.iter().enumerate().take(self.n as usize) {
            let offset = fixed_size + i * 4;
            out[offset..offset + 4].copy_from_slice(&tuple.to_le_bytes());
        }

        // values
        let values_start = fixed_size + tuples_size;
        out[values_start..values_start + values_size].copy_from_slice(&self.values);
    }

    /// 序列化大小。
    pub fn serialized_size(&self) -> usize {
        let fixed_size = 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 2 + 2;
        let tuples_size = (self.max as usize) * 4;
        let values_size = self.values.len();
        fixed_size + tuples_size + values_size
    }
}
