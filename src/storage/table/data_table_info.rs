//! 表的存储元数据。
//!
//! 对应 C++: `duckdb/storage/table/data_table_info.hpp`
//!
//! # 职责
//!
//! `DataTableInfo` 是 `DataTable` 和 `RowGroupCollection` 共同持有的不可变元数据：
//! - 表名 / Schema 名。
//! - 索引列表（`TableIndexList`）。
//! - Checkpoint 读写锁（`StorageLock`）。
//! - 最后一次看到的 checkpoint ID（用于并发 checkpoint 安全检测）。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `AttachedDatabase &db` | `db_id: u64` |
//! | `shared_ptr<TableIOManager>` | `table_io_manager_id: u64` |
//! | `TableIndexList indexes` | `indexes: TableIndexList` |
//! | `StorageLock checkpoint_lock` | `checkpoint_lock: StorageLock` |
//! | `optional_idx last_seen_checkpoint` | `Option<u64>` |
//! | `mutex name_lock` | `Mutex<TableName>` |

use parking_lot::Mutex;

use super::persistent_table_data::PersistentStorageRuntime;
use super::table_index_list::TableIndexList;
use crate::storage::storage_lock::{StorageLock, StorageLockKey};
use std::sync::Arc;

// ─── IndexStorageInfo ──────────────────────────────────────────────────────────

/// 索引磁盘存储信息（C++: `IndexStorageInfo`）。
pub struct IndexStorageInfo {
    pub name: String,
}

// ─── TableName ─────────────────────────────────────────────────────────────────

struct TableName {
    schema: String,
    table: String,
}

// ─── DataTableInfo ─────────────────────────────────────────────────────────────

/// 表的共享元数据（C++: `struct DataTableInfo`）。
///
/// 由 `DataTable` 和 `RowGroupCollection` 通过 `Arc` 共享。
/// 不包含行数据，只包含索引、锁和命名信息。
pub struct DataTableInfo {
    /// 所属数据库 ID（C++: `AttachedDatabase &db`）。
    pub db_id: u64,

    /// IO 管理器 ID（C++: `shared_ptr<TableIOManager> table_io_manager`）。
    pub table_io_manager_id: u64,

    /// 受 name_lock 保护的表名（C++: `mutex name_lock; string schema; string table`）。
    name: Mutex<TableName>,

    /// 物理索引列表（C++: `TableIndexList indexes`）。
    pub indexes: TableIndexList,

    /// 磁盘持久化索引元数据列表（C++: `vector<IndexStorageInfo> index_storage_infos`）。
    pub index_storage_infos: Mutex<Vec<IndexStorageInfo>>,

    /// Checkpoint 读写锁（C++: `StorageLock checkpoint_lock`）。
    pub checkpoint_lock: StorageLock,

    /// 最后一次看到的 checkpoint ID（C++: `optional_idx last_seen_checkpoint`）。
    pub last_seen_checkpoint: Mutex<Option<u64>>,

    /// 真实 DuckDB 文件读取所需的运行时句柄。
    persistent_storage: Mutex<Option<Arc<PersistentStorageRuntime>>>,
}

impl DataTableInfo {
    /// 构造（C++: `DataTableInfo(AttachedDatabase&, shared_ptr<TableIOManager>, string, string)`）。
    pub fn new(db_id: u64, table_io_manager_id: u64, schema: String, table: String) -> Self {
        Self {
            db_id,
            table_io_manager_id,
            name: Mutex::new(TableName { schema, table }),
            indexes: TableIndexList::new(),
            index_storage_infos: Mutex::new(Vec::new()),
            checkpoint_lock: StorageLock::new(),
            last_seen_checkpoint: Mutex::new(None),
            persistent_storage: Mutex::new(None),
        }
    }

    // ── 表名访问 ──────────────────────────────────────────────────────────────

    /// 获取 Schema 名（C++: `GetSchemaName()`）。
    pub fn get_schema_name(&self) -> String {
        self.name.lock().schema.clone()
    }

    /// 获取表名（C++: `GetTableName()`）。
    pub fn get_table_name(&self) -> String {
        self.name.lock().table.clone()
    }

    /// 设置表名（C++: `SetTableName(string)`，用于 ALTER TABLE RENAME）。
    pub fn set_table_name(&self, name: String) {
        self.name.lock().table = name;
    }

    // ── 其他 ──────────────────────────────────────────────────────────────────

    /// 是否为临时表（C++: `IsTemporary()`）。
    pub fn is_temporary(&self) -> bool {
        false
    }

    /// 获取共享 checkpoint 锁凭证（C++: `GetSharedLock()`）。
    pub fn get_shared_lock(&self) -> Box<StorageLockKey> {
        self.checkpoint_lock.get_shared_lock()
    }

    /// 返回此表的唯一数字 ID，供 `LocalStorage` 以 `table_id` 为键查找本地缓冲区。
    ///
    /// 临时方案：将 `db_id` 和 `table_io_manager_id` 组合成一个 `u64`。
    /// 完整实现应使用 catalog 层分配的持久化 OID。
    pub fn table_id(&self) -> u64 {
        // 高 32 位 = db_id，低 32 位 = table_io_manager_id
        (self.db_id << 32) | (self.table_io_manager_id & 0xFFFF_FFFF)
    }

    /// 检查 checkpoint_id 是否为本表尚未见过的 checkpoint（C++: `IsUnseenCheckpoint()`）。
    pub fn is_unseen_checkpoint(&self, checkpoint_id: u64) -> bool {
        let mut guard = self.last_seen_checkpoint.lock();
        match *guard {
            Some(last) if last == checkpoint_id => false,
            _ => {
                *guard = Some(checkpoint_id);
                true
            }
        }
    }

    /// 设置磁盘索引元数据（C++: `SetIndexStorageInfo()`）。
    pub fn set_index_storage_infos(&self, infos: Vec<IndexStorageInfo>) {
        *self.index_storage_infos.lock() = infos;
    }

    /// 获取磁盘索引元数据快照。
    pub fn get_index_storage_infos(&self) -> Vec<IndexStorageInfo> {
        self.index_storage_infos.lock().iter().map(|i| IndexStorageInfo { name: i.name.clone() }).collect()
    }

    pub fn set_persistent_storage(&self, storage: Arc<PersistentStorageRuntime>) {
        *self.persistent_storage.lock() = Some(storage);
    }

    pub fn persistent_storage(&self) -> Option<Arc<PersistentStorageRuntime>> {
        self.persistent_storage.lock().clone()
    }
}
