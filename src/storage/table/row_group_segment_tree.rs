//! RowGroup 有序树（带懒加载支持）。
//!
//! 对应 C++: `duckdb/storage/table/row_group_segment_tree.hpp`
//!
//! # 职责
//!
//! `RowGroupSegmentTree` 是 `SegmentTree<RowGroup>` 的懒加载特化：
//! - 初始化时只读取元数据，不加载 RowGroup 的列数据。
//! - 首次访问某个 RowGroup 时，通过 `load_segment()` 从磁盘读取。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `SegmentTree<RowGroup, true>` 模板特化 | `SegmentTree<RowGroup>` + 可选的 `load_fn` |
//! | `virtual LoadSegment()` | `load_fn: Option<Box<dyn Fn() -> Arc<RowGroup>>>` |
//! | `mutable unique_ptr<MetadataReader> reader` | `reader: Mutex<Option<MetadataReaderStub>>` |
//! | `MetaBlockPointer root_pointer` | `root_pointer: MetaBlockPointer` |
//! | `mutable idx_t current_row_group` | `current_row_group: AtomicU64` |

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::row_group::RowGroup;
use super::segment_tree::SegmentTree;
use super::types::{Idx, MetaBlockPointer};

// ─── MetadataReader 占位 ──────────────────────────────────────────────────────

/// 元数据读取器存根（C++: `MetadataReader`）。
pub struct MetadataReaderStub;

// ─── RowGroupSegmentTree ───────────────────────────────────────────────────────

/// 带懒加载支持的 RowGroup 有序树（C++: `class RowGroupSegmentTree`）。
///
/// 持有对 `RowGroupCollection` 的引用（Rust 用 collection_id 代替）以便回调加载。
pub struct RowGroupSegmentTree {
    /// 底层段树（C++: `SegmentTree<RowGroup, true>` 基类）。
    pub inner: SegmentTree<RowGroup>,

    /// 所属 RowGroupCollection ID（C++: `RowGroupCollection &collection`）。
    pub collection_id: u64,

    /// 当前已加载的 RowGroup 下标（C++: `mutable idx_t current_row_group`）。
    current_row_group: AtomicU64,

    /// 懒加载时的最大 RowGroup 下标（C++: `mutable idx_t max_row_group`）。
    max_row_group: u64,

    /// 元数据读取器（C++: `mutable unique_ptr<MetadataReader> reader`）。
    reader: Mutex<Option<MetadataReaderStub>>,

    /// 根块指针（C++: `MetaBlockPointer root_pointer`）。
    pub root_pointer: MetaBlockPointer,
}

impl RowGroupSegmentTree {
    /// 构造（C++: `RowGroupSegmentTree(RowGroupCollection&, idx_t base_row_id)`）。
    pub fn new(collection_id: u64, base_row_id: Idx) -> Self {
        Self {
            inner: SegmentTree::new(base_row_id),
            collection_id,
            current_row_group: AtomicU64::new(0),
            max_row_group: 0,
            reader: Mutex::new(None),
            root_pointer: MetaBlockPointer::default(),
        }
    }

    /// 从持久化数据初始化（C++: `Initialize(PersistentTableData&)`）。
    ///
    /// 读取元数据，设置 `root_pointer` 和 `max_row_group`，
    /// 但不加载任何 RowGroup（懒加载）。
    pub fn initialize(
        &mut self,
        persistent_data: &super::persistent_table_data::PersistentTableData,
    ) {
        self.root_pointer = persistent_data.block_pointer;
        self.max_row_group = persistent_data.row_group_count;
        todo!("初始化 MetadataReader，设置 reader，不加载 RowGroup 数据")
    }

    /// 获取根块指针（C++: `GetRootPointer()`）。
    pub fn root_pointer(&self) -> MetaBlockPointer {
        self.root_pointer
    }

    /// 懒加载下一个 RowGroup（C++: `LoadSegment()`，protected virtual）。
    fn load_segment(&self) -> Option<Arc<RowGroup>> {
        let idx = self.current_row_group.load(Ordering::Relaxed);
        if idx >= self.max_row_group {
            return None;
        }
        self.current_row_group.fetch_add(1, Ordering::Relaxed);
        todo!(
            "通过 reader 读取第 idx 个 RowGroup 的元数据；\
             反序列化 PersistentRowGroupData；\
             构造 RowGroup（列数据懒加载）；\
             插入 inner SegmentTree"
        )
    }
}
