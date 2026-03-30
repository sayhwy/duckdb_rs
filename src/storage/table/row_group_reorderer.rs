//! RowGroup 重排序器（用于 ORDER BY 优化）。
//!
//! 对应 C++: `duckdb/storage/table/row_group_reorderer.hpp`
//!
//! # 职责
//!
//! `RowGroupReorderer` 在执行带有 ORDER BY 的扫描时，根据列统计信息
//! 对 RowGroup 的访问顺序进行重排，以利用数据局部性和提前剪枝：
//!
//! - 读取每个 RowGroup 的 MIN/MAX 统计值。
//! - 按 ASC/DESC 排序 RowGroup 访问顺序。
//! - 通过 `row_offset` 支持 OFFSET 剪枝（跳过统计值之外的行）。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `const RowGroupOrderOptions options` | `options: RowGroupOrderOptions` |
//! | `vector<reference<SegmentNode<RowGroup>>>` | `Vec<usize>`（RowGroup 下标） |
//! | `bool initialized` | `initialized: bool` |
//! | `idx_t offset` | `offset: u64` |

use super::types::Idx;

// ─── 枚举 ──────────────────────────────────────────────────────────────────────

/// 按 MIN 还是 MAX 统计值排序（C++: `enum class OrderByStatistics`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByStatistics {
    Min,
    Max,
}

/// 升序还是降序（C++: `enum class RowGroupOrderType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowGroupOrderType {
    Asc,
    Desc,
}

/// 排序列的数据类型类别（C++: `enum class OrderByColumnType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByColumnType {
    Numeric,
    String,
}

// ─── RowGroupOrderOptions ──────────────────────────────────────────────────────

/// 排序配置（C++: `struct RowGroupOrderOptions`）。
pub struct RowGroupOrderOptions {
    /// 排序列下标（C++: `column_t column_idx`）。
    pub column_idx: u64,
    /// 使用 MIN 还是 MAX 统计值（C++: `OrderByStatistics order_by`）。
    pub order_by: OrderByStatistics,
    /// 升序还是降序（C++: `RowGroupOrderType order_type`）。
    pub order_type: RowGroupOrderType,
    /// 列类型类别（C++: `OrderByColumnType column_type`）。
    pub column_type: OrderByColumnType,
    /// 行数上限（C++: `optional_idx row_limit`）。
    pub row_limit: Option<Idx>,
    /// RowGroup 起始偏移（C++: `idx_t row_group_offset`）。
    pub row_group_offset: Idx,
}

// ─── OffsetPruningResult ───────────────────────────────────────────────────────

/// OFFSET 剪枝结果（C++: `struct OffsetPruningResult`）。
#[derive(Debug, Clone, Copy)]
pub struct OffsetPruningResult {
    /// 剪枝后剩余的行偏移（C++: `idx_t offset_remainder`）。
    pub offset_remainder: Idx,
    /// 被剪枝掉的 RowGroup 数量（C++: `idx_t pruned_row_group_count`）。
    pub pruned_row_group_count: Idx,
}

// ─── RowGroupReorderer ─────────────────────────────────────────────────────────

/// RowGroup 访问顺序重排器（C++: `class RowGroupReorderer`）。
pub struct RowGroupReorderer {
    /// 排序配置（C++: `const RowGroupOrderOptions options`）。
    pub options: RowGroupOrderOptions,

    /// 当前 OFFSET（C++: `idx_t offset`）。
    pub offset: Idx,

    /// 是否已初始化排序列表（C++: `bool initialized`）。
    pub initialized: bool,

    /// 排序后的 RowGroup 下标列表（C++: `vector<reference<SegmentNode<RowGroup>>>`，用下标替代引用）。
    pub ordered_row_group_indices: Vec<usize>,

    /// 当前访问位置（迭代时递增）。
    pub current_index: usize,
}

impl RowGroupReorderer {
    /// 构造（C++: `RowGroupReorderer(const RowGroupOrderOptions&)`）。
    pub fn new(options: RowGroupOrderOptions) -> Self {
        Self {
            options,
            offset: 0,
            initialized: false,
            ordered_row_group_indices: Vec::new(),
            current_index: 0,
        }
    }

    /// 获取第一个 RowGroup（C++: `GetRootSegment(RowGroupSegmentTree&)`）。
    ///
    /// 首次调用时读取所有 RowGroup 的统计值，按配置排序，返回第一个。
    pub fn get_root_segment_index(&mut self, row_group_count: usize) -> Option<usize> {
        if !self.initialized {
            self.initialize_order(row_group_count);
        }
        self.ordered_row_group_indices.first().copied()
    }

    /// 获取下一个 RowGroup（C++: `GetNextRowGroup(SegmentNode<RowGroup>&)`）。
    pub fn get_next_segment_index(&mut self) -> Option<usize> {
        self.current_index += 1;
        self.ordered_row_group_indices
            .get(self.current_index)
            .copied()
    }

    /// 从统计值中提取排序键（C++: `RetrieveStat(stats, order_by, column_type)`）。
    pub fn retrieve_stat(
        order_by: OrderByStatistics,
        column_type: OrderByColumnType,
        stats_min: f64,
        stats_max: f64,
    ) -> f64 {
        match order_by {
            OrderByStatistics::Min => stats_min,
            OrderByStatistics::Max => stats_max,
        }
    }

    /// OFFSET 剪枝（C++: `GetOffsetAfterPruning()`）。
    pub fn get_offset_after_pruning(
        order_by: OrderByStatistics,
        column_type: OrderByColumnType,
        order_type: RowGroupOrderType,
        column_idx: u64,
        row_offset: Idx,
        row_group_row_counts: &[Idx],
    ) -> OffsetPruningResult {
        todo!(
            "按排序方向累积 RowGroup 行数，直到 row_offset 耗尽；\
             返回剩余 offset 和被跳过的 RowGroup 数"
        )
    }

    // ── 私有辅助 ───────────────────────────────────────────────────────────────

    fn initialize_order(&mut self, row_group_count: usize) {
        todo!(
            "读取每个 RowGroup 第 column_idx 列的 MIN/MAX 统计值；\
             按 order_type(ASC/DESC) 排序 RowGroup 下标；\
             存入 ordered_row_group_indices；\
             处理 row_group_offset 和 row_limit 剪枝；\
             设 initialized = true"
        )
    }
}
