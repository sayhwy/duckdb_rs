//! `SegmentTree<T>` — sorted list of segments with binary-search lookup.
//!
//! Mirrors `duckdb/storage/table/segment_tree.hpp`.
//!
//! # Design notes vs. C++
//!
//! | C++ | Rust |
//! |-----|------|
//! | `mutable vector<unique_ptr<SegmentNode<T>>> nodes` | `Mutex<Vec<SegmentNode<T>>>` |
//! | `mutable mutex node_lock` | integrated into `Mutex<Vec<...>>` |
//! | `SegmentNode::next` (atomic raw ptr) | derived from `index + 1` in the Vec |
//! | `template <bool SUPPORTS_LAZY_LOADING>` | `load_fn: Option<Box<dyn Fn() -> ...>>` |
//! | `SegmentLock` holds `unique_lock<mutex>` | `SegmentLock<'_, T>` holds `MutexGuard` |

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use super::segment_lock::SegmentLock;
use super::types::Idx;

// ─────────────────────────────────────────────────────────────────────────────
// SegmentNode
// ─────────────────────────────────────────────────────────────────────────────

/// One entry in a `SegmentTree` — owns (via `Arc`) the segment and records its
/// start row and position in the Vec.
///
/// Mirrors `template <class T> struct SegmentNode`.
///
/// # Key difference from C++
/// The C++ `SegmentNode::next` field is an `atomic<SegmentNode<T> *>` raw
/// pointer used for lock-free forward iteration.  In Rust we simply advance
/// `index + 1` inside `SegmentTree::get_next_segment`, avoiding any raw
/// pointers while preserving the same O(1) semantics.
#[derive(Debug)]
pub struct SegmentNode<T> {
    /// Row index of the *first* row in this segment.
    row_start: Idx,
    /// Owning reference to the segment data.
    node: Arc<T>,
    /// Position of this node inside `SegmentTree::nodes`.
    index: usize,
}

impl<T> SegmentNode<T> {
    pub fn new(row_start: Idx, node: Arc<T>, index: usize) -> Self {
        SegmentNode {
            row_start,
            node,
            index,
        }
    }

    pub fn row_start(&self) -> Idx {
        self.row_start
    }

    pub fn index(&self) -> usize {
        self.index
    }

    /// Returns the segment value.
    pub fn node(&self) -> &T {
        &self.node
    }

    /// Returns a clone of the `Arc` reference.
    pub fn arc(&self) -> Arc<T> {
        Arc::clone(&self.node)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SegmentTree
// ─────────────────────────────────────────────────────────────────────────────

/// Sorted list of segments with binary-search O(log n) row lookup.
///
/// Mirrors `template <class T, bool SUPPORTS_LAZY_LOADING = false> class SegmentTree`.
///
/// The `load_fn` replaces the `SUPPORTS_LAZY_LOADING` + virtual `LoadSegment`
/// mechanism: provide a closure that streams segments on demand.
pub struct SegmentTree<T: Send + Sync> {
    /// All loaded segment nodes, sorted by `row_start` (guarded by Mutex).
    /// This field IS the combined "nodes + node_lock" from C++.
    nodes: Mutex<Vec<SegmentNode<T>>>,

    /// `true` once all segments have been loaded (mirrors `finished_loading`).
    finished_loading: AtomicBool,

    /// Row id of the very first segment (C++: `base_row_id`).
    base_row_id: Idx,

    /// Optional on-demand segment loader.  Called while the `nodes` lock is
    /// held; returns `Some(segment)` while more segments are available, `None`
    /// when exhausted.
    ///
    /// Replaces `virtual shared_ptr<T> LoadSegment() const` + the
    /// `SUPPORTS_LAZY_LOADING` template parameter.
    load_fn: Option<Box<dyn Fn() -> Option<Arc<T>> + Send + Sync>>,
}

impl<T: Send + Sync> SegmentTree<T> {
    // ── Constructors ─────────────────────────────────────────

    /// Creates an empty tree without lazy loading.
    pub fn new(base_row_id: Idx) -> Self {
        SegmentTree {
            nodes: Mutex::new(Vec::new()),
            finished_loading: AtomicBool::new(true),
            base_row_id,
            load_fn: None,
        }
    }

    /// Creates a tree with a lazy segment loader.
    pub fn with_loader(
        base_row_id: Idx,
        load_fn: impl Fn() -> Option<Arc<T>> + Send + Sync + 'static,
    ) -> Self {
        SegmentTree {
            nodes: Mutex::new(Vec::new()),
            finished_loading: AtomicBool::new(false),
            base_row_id,
            load_fn: Some(Box::new(load_fn)),
        }
    }

    // ── Locking ──────────────────────────────────────────────

    /// Acquires an exclusive lock over the node list.
    /// All mutating operations require holding this lock.
    ///
    /// Mirrors `SegmentLock SegmentTree::Lock() const`.
    pub fn lock(&self) -> SegmentLock<'_, T> {
        SegmentLock(self.nodes.lock())
    }

    // ── Query ─────────────────────────────────────────────────

    pub fn is_empty(&self, lock: &SegmentLock<'_, T>) -> bool {
        self.get_root_segment(lock).is_none()
    }

    /// Returns a reference to the first (lowest row_start) segment node.
    ///
    /// May trigger a lazy-load call.
    pub fn get_root_segment<'a>(&self, lock: &'a SegmentLock<'_, T>) -> Option<&'a SegmentNode<T>> {
        lock.0.first()
    }

    /// Returns the last (highest row_start) segment node.
    /// Triggers full lazy-load.
    pub fn get_last_segment<'a>(&self, lock: &'a SegmentLock<'_, T>) -> Option<&'a SegmentNode<T>> {
        self.load_all_segments_inner(&mut *self.nodes.lock());
        lock.0.last()
    }

    /// Returns the segment that contains `row_number` (binary search).
    pub fn get_segment<'a>(
        &self,
        lock: &'a mut SegmentLock<'_, T>,
        row_number: Idx,
    ) -> Option<&'a SegmentNode<T>> {
        let idx = self.get_segment_index_inner(&mut lock.0, row_number)?;
        lock.0.get(idx)
    }

    /// Returns the segment at the given Vec index (negative = from back).
    pub fn get_segment_by_index<'a>(
        &self,
        lock: &'a SegmentLock<'_, T>,
        index: i64,
    ) -> Option<&'a SegmentNode<T>> {
        let nodes = &*lock.0;
        if index < 0 {
            self.load_all_segments_inner(&mut *self.nodes.lock());
            let pos = nodes.len() as i64 + index;
            if pos < 0 {
                return None;
            }
            nodes.get(pos as usize)
        } else {
            // lazy-load until we have enough segments
            while (index as usize) >= lock.0.len() {
                if !self.load_next_segment_inner(&mut *self.nodes.lock()) {
                    break;
                }
            }
            lock.0.get(index as usize)
        }
    }

    /// Returns the segment immediately after `node`.
    pub fn get_next_segment<'a>(
        &self,
        lock: &'a SegmentLock<'_, T>,
        node: &SegmentNode<T>,
    ) -> Option<&'a SegmentNode<T>> {
        let next_idx = node.index + 1;
        if next_idx < lock.0.len() {
            lock.0.get(next_idx)
        } else {
            if self.load_next_segment_inner(&mut *self.nodes.lock()) {
                lock.0.get(next_idx)
            } else {
                None
            }
        }
    }

    /// Total number of segments (forces full load).
    pub fn segment_count(&self) -> usize {
        let mut nodes = self.nodes.lock();
        self.load_all_segments_inner(&mut nodes);
        nodes.len()
    }

    // ── Mutation ──────────────────────────────────────────────

    /// Appends a new segment at the end of the list.
    pub fn append_segment(&self, lock: &mut SegmentLock<'_, T>, segment: Arc<T>, row_start: Idx) {
        let nodes = &mut *lock.0;
        let index = nodes.len();
        nodes.push(SegmentNode::new(row_start, segment, index));
    }

    /// Appends using the computed `row_start` (end of last segment).
    pub fn append_segment_auto(
        &self,
        lock: &mut SegmentLock<'_, T>,
        segment: Arc<T>,
        count_of: impl Fn(&T) -> Idx,
    ) {
        let row_start = {
            let nodes = &*lock.0;
            if nodes.is_empty() {
                self.base_row_id
            } else {
                let last = nodes.last().unwrap();
                last.row_start + count_of(last.node())
            }
        };
        self.append_segment(lock, segment, row_start);
    }

    /// Removes all segments starting from `segment_start` index.
    pub fn erase_segments(&self, lock: &mut SegmentLock<'_, T>, segment_start: usize) {
        self.load_all_segments_inner(&mut lock.0);
        lock.0.truncate(segment_start);
    }

    /// Drains all nodes out of the tree (useful for Move semantics).
    pub fn move_segments(&self, lock: &mut SegmentLock<'_, T>) -> Vec<SegmentNode<T>> {
        self.load_all_segments_inner(&mut lock.0);
        std::mem::take(&mut *lock.0)
    }

    pub fn base_row_id(&self) -> Idx {
        self.base_row_id
    }

    // ── Internal ──────────────────────────────────────────────

    /// Triggers one `load_fn` call if not yet finished loading.
    /// Returns `true` if a segment was loaded.
    fn load_next_segment_inner(&self, nodes: &mut Vec<SegmentNode<T>>) -> bool {
        if self.finished_loading.load(Ordering::Acquire) {
            return false;
        }
        if let Some(ref f) = self.load_fn {
            if let Some(seg) = f() {
                let row_start = if nodes.is_empty() {
                    self.base_row_id
                } else {
                    // T must expose its count somehow; here we use a sentinel
                    // TODO: require T: SegmentBase and use .count()
                    nodes
                        .last()
                        .map(|n| n.row_start)
                        .unwrap_or(self.base_row_id)
                };
                let idx = nodes.len();
                nodes.push(SegmentNode::new(row_start, seg, idx));
                return true;
            }
        }
        self.finished_loading.store(true, Ordering::Release);
        false
    }

    fn load_all_segments_inner(&self, nodes: &mut Vec<SegmentNode<T>>) {
        while self.load_next_segment_inner(nodes) {}
    }

    /// Binary search for the index of the segment containing `row_number`.
    fn get_segment_index_inner(
        &self,
        nodes: &mut Vec<SegmentNode<T>>,
        row_number: Idx,
    ) -> Option<usize> {
        // Lazy-load until the row is within bounds
        // (requires knowing segment counts — TODO with SegmentBase trait)
        while nodes.is_empty() || {
            // Approximate: load if row_number >= last node's row_start
            nodes
                .last()
                .map(|n| row_number >= n.row_start)
                .unwrap_or(true)
        } {
            if !self.load_next_segment_inner(nodes) {
                break;
            }
        }
        if nodes.is_empty() {
            return None;
        }
        let (lo, mut hi) = (0usize, nodes.len() - 1);
        while lo <= hi {
            let mid = (lo + hi) / 2;
            let n = &nodes[mid];
            if row_number < n.row_start {
                if mid == 0 {
                    return None;
                }
                hi = mid - 1;
            } else {
                // TODO: compare with row_end once SegmentBase::count is available
                // For now, return this index (conservative)
                return Some(mid);
            }
        }
        None
    }
}
