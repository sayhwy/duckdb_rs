//! `SegmentLock` — RAII exclusive lock for segment tree node lists.
//!
//! Mirrors `duckdb/storage/table/segment_lock.hpp`.
//!
//! In C++, `SegmentLock` wraps a `unique_lock<mutex>`.  Here we wrap
//! `parking_lot::MutexGuard` so that:
//!   - The lock is `Send` (required when passing guards across async tasks).
//!   - `release()` is an explicit opt-in (just drop the guard otherwise).

use parking_lot::MutexGuard;

/// Exclusive guard over a segment tree's internal node vector.
///
/// Obtained via `SegmentTree::lock()`.  Held for the duration of any
/// operation that reads or mutates the node list.
///
/// # Usage
/// ```rust,ignore
/// let lock = tree.lock();
/// let root = tree.get_root_segment(&lock);
/// ```
pub struct SegmentLock<'a, T: Send + Sync>(pub(super) MutexGuard<'a, Vec<super::segment_tree::SegmentNode<T>>>);

impl<'a, T: Send + Sync> SegmentLock<'a, T> {
    /// Explicitly release the lock ahead of its natural scope end.
    /// After this call the guard is consumed and the lock is freed.
    pub fn release(self) {
        drop(self.0)
    }
}
