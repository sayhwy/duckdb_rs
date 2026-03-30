//! 表的物理索引列表。
//!
//! 对应 C++: `duckdb/storage/table/table_index_list.hpp`
//!
//! # 职责
//!
//! `TableIndexList` 管理一张表的所有物理索引（ART 等），提供：
//! - 线程安全的增删改查。
//! - 批量扫描（`scan`）。
//! - 外键索引查找（`find_foreign_key_index`）。
//! - 索引序列化到磁盘（`serialize_to_disk`）。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `vector<unique_ptr<IndexEntry>>` | `Vec<Box<IndexEntry>>` |
//! | `mutex index_entries_lock` | `Mutex<Vec<Box<IndexEntry>>>` |
//! | `atomic<IndexBindState>` | `AtomicU8` |
//! | `unique_ptr<BoundIndex>` | `Option<Box<BoundIndexStub>>` |

use parking_lot::Mutex;
use std::sync::atomic::{AtomicU8, Ordering};

use super::data_table_info::IndexStorageInfo;

// ─── 外部类型占位 ──────────────────────────────────────────────────────────────

/// 绑定索引存根（C++: `BoundIndex`）。
pub struct BoundIndexStub {
    pub name: String,
}

/// 未绑定索引存根（C++: `Index`）。
pub struct IndexStub {
    pub name: String,
}

/// 外键类型（C++: `ForeignKeyType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyType {
    PrimaryKey,
    ForeignKey,
}

// ─── IndexBindState ────────────────────────────────────────────────────────────

/// 索引绑定阶段（C++: `enum class IndexBindState : uint8_t`）。
///
/// 防止锁顺序颠倒：绑定时先设为 `Binding`，完成后设为 `Bound`。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexBindState {
    Unbound = 0,
    Binding = 1,
    Bound = 2,
}

// ─── IndexEntry ────────────────────────────────────────────────────────────────

/// 索引容器（C++: `struct IndexEntry`）。
///
/// 包含绑定状态原子值 + 索引本身 + 可选的 `deleted_rows_in_use`。
pub struct IndexEntry {
    /// 当前绑定阶段（C++: `atomic<IndexBindState> bind_state`）。
    pub bind_state: AtomicU8,

    /// 未绑定索引（C++: `unique_ptr<Index> index`）。
    pub index: Option<Box<IndexStub>>,

    /// 用于记录删除行的索引副本（C++: `unique_ptr<BoundIndex> deleted_rows_in_use`）。
    pub deleted_rows_in_use: Mutex<Option<Box<BoundIndexStub>>>,
}

impl IndexEntry {
    pub fn new(index: Box<IndexStub>) -> Self {
        Self {
            bind_state: AtomicU8::new(IndexBindState::Unbound as u8),
            index: Some(index),
            deleted_rows_in_use: Mutex::new(None),
        }
    }

    /// 当前绑定状态（C++: `bind_state.load()`）。
    pub fn bind_state(&self) -> IndexBindState {
        match self.bind_state.load(Ordering::Acquire) {
            0 => IndexBindState::Unbound,
            1 => IndexBindState::Binding,
            _ => IndexBindState::Bound,
        }
    }

    /// 尝试将状态从 `Unbound` 切换到 `Binding`（CAS 操作）。
    pub fn try_start_binding(&self) -> bool {
        self.bind_state
            .compare_exchange(
                IndexBindState::Unbound as u8,
                IndexBindState::Binding as u8,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// 绑定完成，设为 `Bound`。
    pub fn finish_binding(&self) {
        self.bind_state
            .store(IndexBindState::Bound as u8, Ordering::Release);
    }
}

// ─── TableIndexList ────────────────────────────────────────────────────────────

/// 表的索引列表（C++: `class TableIndexList`）。
pub struct TableIndexList {
    /// 受锁保护的索引条目列表（C++: `mutex + vector<unique_ptr<IndexEntry>>`）。
    entries: Mutex<TableIndexListInner>,
}

struct TableIndexListInner {
    entries: Vec<Box<IndexEntry>>,
    /// 未绑定索引计数（C++: `idx_t unbound_count`）。
    unbound_count: usize,
}

impl TableIndexList {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(TableIndexListInner {
                entries: Vec::new(),
                unbound_count: 0,
            }),
        }
    }

    /// 添加索引（C++: `AddIndex(unique_ptr<Index>)`）。
    pub fn add_index(&self, index: Box<IndexStub>) {
        let mut inner = self.entries.lock();
        inner.unbound_count += 1;
        inner.entries.push(Box::new(IndexEntry::new(index)));
    }

    /// 移除索引（C++: `RemoveIndex(const string&)`）。
    pub fn remove_index(&self, name: &str) {
        let mut inner = self.entries.lock();
        inner
            .entries
            .retain(|e| e.index.as_ref().map(|i| i.name.as_str()) != Some(name));
    }

    /// 是否为空（C++: `Empty()`）。
    pub fn is_empty(&self) -> bool {
        self.entries.lock().entries.is_empty()
    }

    /// 索引数量（C++: `Count()`）。
    pub fn count(&self) -> usize {
        self.entries.lock().entries.len()
    }

    /// 是否有未绑定索引（C++: `HasUnbound()`）。
    pub fn has_unbound(&self) -> bool {
        self.entries.lock().unbound_count > 0
    }

    /// 扫描所有索引条目，callback 返回 true 时停止（C++: `Scan<T>(callback)`）。
    pub fn scan<F>(&self, mut callback: F)
    where
        F: FnMut(&IndexStub) -> bool,
    {
        let inner = self.entries.lock();
        for entry in &inner.entries {
            if let Some(idx) = &entry.index {
                if callback(idx) {
                    break;
                }
            }
        }
    }

    /// 查找指定名称的已绑定索引（C++: `Find(const string&)`）。
    pub fn find(&self, name: &str) -> Option<String> {
        let inner = self.entries.lock();
        inner
            .entries
            .iter()
            .find(|e| e.index.as_ref().map(|i| i.name.as_str()) == Some(name))
            .map(|_| name.to_string())
    }

    /// 检查索引名称是否唯一（即尚未存在同名索引）（C++: `NameIsUnique(name)`）。
    pub fn name_is_unique(&self, name: &str) -> bool {
        let inner = self.entries.lock();
        !inner
            .entries
            .iter()
            .any(|e| e.index.as_ref().map(|i| i.name.as_str()) == Some(name))
    }

    /// 查找外键索引（C++: `FindForeignKeyIndex()`）。
    pub fn find_foreign_key_index(&self, _fk_keys: &[u32], _fk_type: ForeignKeyType) -> bool {
        todo!("遍历索引，找到与 fk_keys 匹配且类型为 fk_type 的索引")
    }

    /// 序列化所有索引到磁盘（C++: `SerializeToDisk()`）。
    pub fn serialize_to_disk(&self) -> Vec<IndexStorageInfo> {
        todo!("对每个已绑定索引调用 BoundIndex::Serialize，收集 IndexStorageInfo")
    }

    /// 获取索引覆盖的列集合（C++: `GetRequiredColumns()`）。
    pub fn get_required_columns(&self) -> std::collections::HashSet<u64> {
        todo!("对每个索引收集其覆盖的列 ID")
    }
}
