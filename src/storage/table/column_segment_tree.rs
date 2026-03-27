//! 列段有序树（`ColumnSegmentTree`）。
//!
//! 对应 C++: `duckdb/storage/table/column_segment_tree.hpp`
//!
//! C++ 定义极简：
//! ```cpp
//! class ColumnSegmentTree : public SegmentTree<ColumnSegment> {};
//! ```
//!
//! Rust 直接用类型别名，无需额外行为。

use super::column_segment::ColumnSegment;
use super::segment_tree::SegmentTree;

/// 对 `SegmentTree<ColumnSegment>` 的类型别名（C++: `class ColumnSegmentTree`）。
pub type ColumnSegmentTree = SegmentTree<ColumnSegment>;
