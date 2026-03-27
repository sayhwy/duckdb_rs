//! Delete 操作的 Undo 条目载荷。
//!
//! 对应 C++: `duckdb/transaction/delete_info.hpp`
//!
//! # C++ 内存布局（灵活数组成员）
//!
//! ```text
//! [ DeleteInfo (固定字段) | uint16_t rows[count]（仅 !is_consecutive 时存在）]
//! ```
//!
//! C++ 通过 `uint16_t rows[1]` + 堆外分配实现灵活数组；
//! Rust 改为 `rows: Option<Vec<u16>>`，语义等价且安全。
//!
//! # MVCC 语义
//!
//! - Rollback：将 `rows` 中每行的版本号从 `transaction_id` 改回 `NOT_DELETED_ID`。
//! - Commit：将版本号从 `transaction_id` 改为 `commit_id`。
//! - Cleanup：若版本号对所有活跃事务均不可见，则可清除 ChunkVectorInfo 中的行。

use super::types::Idx;

/// Delete 操作的 Undo 记录（C++: `struct DeleteInfo`）。
///
/// 记录一次针对单个向量（vector_idx 所指的 2048 行块）的删除，
/// 可表示连续行删除（`is_consecutive = true`）或稀疏行删除。
#[derive(Debug, Clone)]
pub struct DeleteInfo {
    /// 被删除行所在的表（C++: `DataTable *table`，用 ID 替代裸指针）。
    pub table_id: u64,

    /// 被删除行所在的 `RowVersionManager`（C++: `RowVersionManager *version_info`，用 ID 替代）。
    pub version_info_id: u64,

    /// 在 RowGroup 内的向量下标（C++: `idx_t vector_idx`）。
    pub vector_idx: Idx,

    /// 删除的行数（C++: `idx_t count`）。
    pub count: Idx,

    /// 向量基础行号（C++: `idx_t base_row`）。
    pub base_row: Idx,

    /// 是否为连续行删除（C++: `bool is_consecutive`）。
    pub is_consecutive: bool,

    /// 稀疏删除时的行内偏移列表（C++: `uint16_t rows[1]`，灵活数组成员）。
    pub rows: Option<Vec<u16>>,
}

impl DeleteInfo {
    /// 获取行偏移列表（C++: `GetRows()`，非连续时才合法）。
    pub fn get_rows(&self) -> &[u16] {
        match &self.rows {
            Some(r) => r.as_slice(),
            None => panic!("DeleteInfo::get_rows called on consecutive delete"),
        }
    }

    /// 从 UndoBuffer 载荷字节反序列化。
    ///
    /// 格式：`table_id(8) | version_info_id(8) | vector_idx(8) | count(8) | base_row(8) | is_consecutive(1) | rows(count*2)`
    pub fn deserialize(bytes: &[u8]) -> Self {
        let min_size = 8 + 8 + 8 + 8 + 8 + 1;
        assert!(bytes.len() >= min_size, "DeleteInfo payload too short");

        let table_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let version_info_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let vector_idx = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let count = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let base_row = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let is_consecutive = bytes[40] != 0;

        let rows = if is_consecutive {
            None
        } else {
            let rows_start = min_size;
            let rows_end = rows_start + (count as usize) * 2;
            assert!(bytes.len() >= rows_end, "DeleteInfo rows data too short");

            let mut rows = Vec::with_capacity(count as usize);
            for i in 0..count as usize {
                let offset = rows_start + i * 2;
                rows.push(u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()));
            }
            Some(rows)
        };

        Self {
            table_id,
            version_info_id,
            vector_idx,
            count,
            base_row,
            is_consecutive,
            rows,
        }
    }

    /// 序列化到 UndoBuffer 载荷字节。
    pub fn serialize(&self, out: &mut [u8]) {
        let min_size = 8 + 8 + 8 + 8 + 8 + 1;
        assert!(out.len() >= self.serialized_size(), "DeleteInfo output buffer too short");

        out[0..8].copy_from_slice(&self.table_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.version_info_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.vector_idx.to_le_bytes());
        out[24..32].copy_from_slice(&self.count.to_le_bytes());
        out[32..40].copy_from_slice(&self.base_row.to_le_bytes());
        out[40] = if self.is_consecutive { 1 } else { 0 };

        if let Some(ref rows) = self.rows {
            for (i, row) in rows.iter().enumerate() {
                let offset = min_size + i * 2;
                out[offset..offset + 2].copy_from_slice(&row.to_le_bytes());
            }
        }
    }

    /// 序列化载荷大小（字节）。
    pub fn serialized_size(&self) -> usize {
        let fixed = 8 + 8 + 8 + 8 + 8 + 1;
        let rows_size = if self.is_consecutive {
            0
        } else {
            self.count as usize * std::mem::size_of::<u16>()
        };
        fixed + rows_size
    }
}