//! 持久化表数据元信息。
//!
//! 对应 C++: `duckdb/storage/table/persistent_table_data.hpp`
//!
//! # 职责
//!
//! `PersistentTableData` 在从磁盘加载表时持有中间元数据：
//! - 指向元数据块的指针（`base_table_pointer`, `block_pointer`）。
//! - 全局统计信息（`table_stats`）。
//! - 总行数和 RowGroup 数量。
//!
//! `PersistentColumnData` 是单列的持久化状态，供 `ColumnCheckpointState::ToPersistentData()` 生成。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `MetaBlockPointer base_table_pointer` | `MetaBlockPointer` |
//! | `TableStatistics table_stats` | `TableStatistics` |
//! | `idx_t total_rows` | `u64` |
//! | `idx_t row_group_count` | `u64` |

use super::table_statistics::TableStatistics;
use super::types::{Idx, MetaBlockPointer};
use super::row_group::RowGroupPointer;

// Re-export the canonical persistent data structs defined in column_data.rs.
pub use super::column_data::{PersistentColumnData, PersistentRowGroupData};

// ─── PersistentTableData ──────────────────────────────────────────────────────

/// 从磁盘加载表时的中间元数据（C++: `class PersistentTableData`）。
pub struct PersistentTableData {
    /// 表元数据起始块指针（C++: `MetaBlockPointer base_table_pointer`）。
    pub base_table_pointer: MetaBlockPointer,

    /// 全局列统计信息（C++: `TableStatistics table_stats`）。
    pub table_stats: TableStatistics,

    /// 表的总行数（C++: `idx_t total_rows`）。
    pub total_rows: Idx,

    /// RowGroup 总数（C++: `idx_t row_group_count`）。
    pub row_group_count: Idx,

    /// RowGroup 序列化数据的根块指针（C++: `MetaBlockPointer block_pointer`）。
    pub block_pointer: MetaBlockPointer,

    /// 已反序列化的 RowGroup 指针列表。
    pub row_group_pointers: Vec<RowGroupPointer>,
}

impl PersistentTableData {
    /// 构造（C++: `PersistentTableData(idx_t column_count)`）。
    pub fn new(_column_count: usize) -> Self {
        Self {
            base_table_pointer: MetaBlockPointer::default(),
            table_stats: TableStatistics::new(),
            total_rows: 0,
            row_group_count: 0,
            block_pointer: MetaBlockPointer::default(),
            row_group_pointers: Vec::new(),
        }
    }
}
