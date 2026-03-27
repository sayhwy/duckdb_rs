//! 存储层读写锁。
//!
//! 对应 C++:
//!   `duckdb/storage/storage_lock.hpp`
//!   `src/storage/storage_lock.cpp`
//!
//! # 设计
//!
//! C++ 原始实现使用一个 `std::mutex` 作为独占锁，加上 `atomic<idx_t> read_count` 记录共享持有数。
//! 独占锁获取时先锁 mutex，再自旋等待 read_count 归零；释放时 unlock mutex。
//! 共享锁获取时短暂锁 mutex 来原子性增加 read_count，然后立即 unlock。
//!
//! Rust 实现使用 `parking_lot::Mutex<LockState> + 两个 Condvar` 实现相同语义，无需 unsafe：
//!
//! ```text
//! StorageLockInternals
//!   ├── state: Mutex<LockState>      — 保护 { exclusive: bool, readers: usize }
//!   ├── no_exclusive: Condvar        — exclusive 释放时唤醒等待者
//!   └── no_readers:   Condvar        — readers 归零时唤醒独占等待者
//! ```
//!
//! # 特殊语义：TryUpgradeCheckpointLock
//!
//! 仅用于 checkpoint：调用者已持有一把 Shared 锁，若该 Shared 锁是当前唯一活跃的
//! 共享锁，则同时颁发一把 Exclusive 锁（此时 readers == 1）。
//! 返回后调用者同时持有 Shared + Exclusive 两把锁，这是 DuckDB checkpoint 的预期行为。

use std::sync::Arc;
use parking_lot::{Condvar, Mutex};

// ─── LockState ──────────────────────────────────────────────────────────────

struct LockState {
    /// 是否有独占持有者。
    exclusive: bool,
    /// 当前活跃共享持有数。
    readers: usize,
}

impl LockState {
    fn new() -> Self {
        Self { exclusive: false, readers: 0 }
    }
}

// ─── StorageLockType ─────────────────────────────────────────────────────────

/// 锁类型（C++: `enum class StorageLockType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageLockType {
    Shared    = 0,
    Exclusive = 1,
}

// ─── StorageLockInternals ────────────────────────────────────────────────────

/// 锁的内部状态，由 `StorageLock` 和所有 `StorageLockKey` 共享（C++: `StorageLockInternals`）。
struct StorageLockInternals {
    state:        Mutex<LockState>,
    no_exclusive: Condvar,
    no_readers:   Condvar,
}

impl StorageLockInternals {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state:        Mutex::new(LockState::new()),
            no_exclusive: Condvar::new(),
            no_readers:   Condvar::new(),
        })
    }

    /// 获取独占锁（阻塞）（C++: `GetExclusiveLock()`）。
    ///
    /// 1. 等待当前独占持有者释放。
    /// 2. 标记 exclusive = true。
    /// 3. 等待所有共享持有者释放（readers == 0）。
    fn get_exclusive_lock(self: &Arc<Self>) -> Box<StorageLockKey> {
        let mut state = self.state.lock();
        // 等待独占锁空闲
        while state.exclusive {
            self.no_exclusive.wait(&mut state);
        }
        state.exclusive = true;
        // 等待所有共享持有者退出
        while state.readers > 0 {
            self.no_readers.wait(&mut state);
        }
        drop(state);
        Box::new(StorageLockKey {
            internals: Arc::clone(self),
            lock_type: StorageLockType::Exclusive,
        })
    }

    /// 获取共享锁（阻塞）（C++: `GetSharedLock()`）。
    fn get_shared_lock(self: &Arc<Self>) -> Box<StorageLockKey> {
        let mut state = self.state.lock();
        // 等待独占锁空闲
        while state.exclusive {
            self.no_exclusive.wait(&mut state);
        }
        state.readers += 1;
        drop(state);
        Box::new(StorageLockKey {
            internals: Arc::clone(self),
            lock_type: StorageLockType::Shared,
        })
    }

    /// 尝试获取独占锁（非阻塞）（C++: `TryGetExclusiveLock()`）。
    ///
    /// 若当前有独占持有者或任何共享持有者，立即返回 `None`。
    fn try_get_exclusive_lock(self: &Arc<Self>) -> Option<Box<StorageLockKey>> {
        let mut state = self.state.try_lock()?;
        if state.exclusive || state.readers > 0 {
            return None;
        }
        state.exclusive = true;
        drop(state);
        Some(Box::new(StorageLockKey {
            internals: Arc::clone(self),
            lock_type: StorageLockType::Exclusive,
        }))
    }

    /// 尝试将一把 Shared 锁升级为 Exclusive 锁（C++: `TryUpgradeCheckpointLock()`）。
    ///
    /// 仅在该 Shared 锁是当前唯一活跃共享锁（readers == 1）时才能成功，否则返回 `None`。
    /// 成功后调用者同时持有 Shared + Exclusive，这是 checkpoint 的预期状态。
    fn try_upgrade_checkpoint_lock(
        self: &Arc<Self>,
        lock: &StorageLockKey,
    ) -> Option<Box<StorageLockKey>> {
        debug_assert_eq!(
            lock.lock_type,
            StorageLockType::Shared,
            "TryUpgradeCheckpointLock 只能对 Shared 锁调用"
        );
        let mut state = self.state.try_lock()?;
        if state.exclusive {
            // 已有独占持有者
            return None;
        }
        if state.readers != 1 {
            // 存在其他共享持有者
            debug_assert!(state.readers > 0);
            return None;
        }
        // 仅剩调用者的 Shared 锁：升级成功
        state.exclusive = true;
        drop(state);
        Some(Box::new(StorageLockKey {
            internals: Arc::clone(self),
            lock_type: StorageLockType::Exclusive,
        }))
    }

    /// 释放独占锁（C++: `ReleaseExclusiveLock()`）。
    fn release_exclusive(&self) {
        {
            let mut state = self.state.lock();
            debug_assert!(state.exclusive);
            state.exclusive = false;
        }
        self.no_exclusive.notify_all();
    }

    /// 释放共享锁（C++: `ReleaseSharedLock()`）。
    fn release_shared(&self) {
        let notify = {
            let mut state = self.state.lock();
            debug_assert!(state.readers > 0);
            state.readers -= 1;
            state.readers == 0
        };
        if notify {
            self.no_readers.notify_all();
        }
    }
}

// ─── StorageLockKey ──────────────────────────────────────────────────────────

/// 持有一把存储锁的 RAII 凭证（C++: `class StorageLockKey`）。
///
/// Drop 时自动归还锁。
pub struct StorageLockKey {
    internals: Arc<StorageLockInternals>,
    lock_type: StorageLockType,
}

impl StorageLockKey {
    /// 返回锁类型（C++: `GetType()`）。
    pub fn get_type(&self) -> StorageLockType {
        self.lock_type
    }
}

impl Drop for StorageLockKey {
    fn drop(&mut self) {
        match self.lock_type {
            StorageLockType::Exclusive => self.internals.release_exclusive(),
            StorageLockType::Shared    => self.internals.release_shared(),
        }
    }
}

// ─── StorageLock ─────────────────────────────────────────────────────────────

/// 存储层读写锁（C++: `class StorageLock`）。
///
/// 可克隆（内部为 `Arc`），多处可持有同一把锁的引用。
pub struct StorageLock {
    inner: Arc<StorageLockInternals>,
}

impl StorageLock {
    /// 创建新锁（C++: `StorageLock::StorageLock()`）。
    pub fn new() -> Self {
        Self { inner: StorageLockInternals::new() }
    }

    /// 获取独占锁（阻塞）（C++: `GetExclusiveLock()`）。
    pub fn get_exclusive_lock(&self) -> Box<StorageLockKey> {
        self.inner.get_exclusive_lock()
    }

    /// 获取共享锁（阻塞）（C++: `GetSharedLock()`）。
    pub fn get_shared_lock(&self) -> Box<StorageLockKey> {
        self.inner.get_shared_lock()
    }

    /// 尝试获取独占锁（非阻塞）（C++: `TryGetExclusiveLock()`）。
    ///
    /// 无法立即获取时返回 `None`。
    pub fn try_get_exclusive_lock(&self) -> Option<Box<StorageLockKey>> {
        self.inner.try_get_exclusive_lock()
    }

    /// 尝试将 Shared 锁升级为 Exclusive 锁（C++: `TryUpgradeCheckpointLock()`）。
    ///
    /// 参数 `lock` 必须是由本 `StorageLock` 颁发的 Shared 锁。
    pub fn try_upgrade_checkpoint_lock(
        &self,
        lock: &StorageLockKey,
    ) -> Option<Box<StorageLockKey>> {
        self.inner.try_upgrade_checkpoint_lock(lock)
    }
}

impl Default for StorageLock {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn shared_locks_do_not_block_each_other() {
        let lock = StorageLock::new();
        let k1 = lock.get_shared_lock();
        let k2 = lock.get_shared_lock();
        assert_eq!(k1.get_type(), StorageLockType::Shared);
        assert_eq!(k2.get_type(), StorageLockType::Shared);
        // both alive simultaneously — no deadlock
    }

    #[test]
    fn exclusive_blocks_while_shared_active() {
        let lock = Arc::new(StorageLock::new());
        let shared = lock.get_shared_lock();

        // try_get_exclusive_lock should fail with an active shared lock
        assert!(lock.try_get_exclusive_lock().is_none());
        drop(shared);
        // now it should succeed
        assert!(lock.try_get_exclusive_lock().is_some());
    }

    #[test]
    fn upgrade_succeeds_when_only_caller_shared() {
        let lock = StorageLock::new();
        let shared = lock.get_shared_lock();
        let exclusive = lock.try_upgrade_checkpoint_lock(&shared);
        assert!(exclusive.is_some());
    }

    #[test]
    fn upgrade_fails_with_other_shared_holders() {
        let lock = StorageLock::new();
        let shared1 = lock.get_shared_lock();
        let shared2 = lock.get_shared_lock();
        let exclusive = lock.try_upgrade_checkpoint_lock(&shared1);
        assert!(exclusive.is_none());
        drop(shared2);
    }

    #[test]
    fn exclusive_lock_blocks_shared_try() {
        let lock = StorageLock::new();
        let _ex = lock.get_exclusive_lock();
        // With an exclusive lock held, try_get_exclusive_lock should fail
        assert!(lock.try_get_exclusive_lock().is_none());
    }
}
