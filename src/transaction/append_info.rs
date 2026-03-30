//! Append 操作的 Undo 条目载荷。
//!
//! 对应 C++: `duckdb/transaction/append_info.hpp`
//!
//! # 布局（Undo 条目载荷内）
//! ```text
//! [ AppendInfo ]
//! ```
//!
//! Rollback 时：将对应行区间的行版本标记为已回滚（MVCC 不可见）。
//! Commit 时：无需额外操作（行本身已写入 RowGroup）。

use super::types::Idx;

/// Append 操作的 Undo 记录（C++: `struct AppendInfo`）。
///
/// 记录一次 `DataTable::Append()` 写入的起始行和行数，
/// 用于 Rollback 时将这些行标记为已删除。
#[derive(Debug, Clone)]
pub struct AppendInfo {
    /// 追加到的表（C++: `DataTable *table`）。
    pub table_id: u64,

    /// 追加行的起始 row_id（C++: `idx_t start_row`）。
    pub start_row: Idx,

    /// 追加的行数（C++: `idx_t count`）。
    pub count: Idx,
}

impl AppendInfo {
    /// 从 UndoBuffer 载荷字节反序列化（小端序）。
    ///
    /// 格式：`table_id(8B) | start_row(8B) | count(8B)`
    pub fn deserialize(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= Self::serialized_size(),
            "AppendInfo payload too short"
        );

        let table_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let start_row = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let count = u64::from_le_bytes(bytes[16..24].try_into().unwrap());

        Self {
            table_id,
            start_row,
            count,
        }
    }

    /// 序列化到 UndoBuffer 载荷字节。
    pub fn serialize(&self, out: &mut [u8]) {
        assert!(
            out.len() >= Self::serialized_size(),
            "AppendInfo output buffer too short"
        );

        out[0..8].copy_from_slice(&self.table_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.start_row.to_le_bytes());
        out[16..24].copy_from_slice(&self.count.to_le_bytes());
    }

    /// 序列化大小（固定）。
    pub const fn serialized_size() -> usize {
        8 + 8 + 8 // table_id + start_row + count
    }
}
