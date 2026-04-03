//! StorageManager — 数据库存储层入口。
//!
//! 对应 C++:
//!   `duckdb/storage/storage_manager.hpp`
//!   `src/storage/storage_manager.cpp`
//!
//! # 结构层次
//!
//! ```text
//! StorageManager (trait)               — 抽象存储管理器
//!   └── SingleFileStorageManager       — 单文件存储管理器（主要实现）
//! StorageCommitState (trait)           — 抽象提交状态
//!   └── SingleFileStorageCommitState   — 单文件提交状态
//! StorageOptions                       — ATTACH 阶段的存储选项
//! DatabaseSize                         — 数据库磁盘使用统计
//! CheckpointOptions                    — Checkpoint 配置
//! ```
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class StorageManager` (abstract base) | `trait StorageManager` |
//! | `class SingleFileStorageManager` | `struct SingleFileStorageManager` |
//! | `class StorageCommitState` (abstract) | `trait StorageCommitState` |
//! | `class SingleFileStorageCommitState` | `struct SingleFileStorageCommitState` |
//! | `StorageOptions` | `StorageOptions` |
//! | `unique_ptr<WriteAheadLog> wal` | `Option<Box<WriteAheadLog>>` |
//! | `atomic<idx_t> wal_size` | `AtomicU64` |
//! | `mutex wal_lock` | `Mutex<WalState>` |
//! | `bool load_complete` | `AtomicBool` |
//! | `optional_idx storage_version` | `Option<u64>` |

use parking_lot::Mutex;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use crate::db::connection::DatabaseInstance;
use super::StandardBufferManager;
use super::buffer::{BlockAllocator, BlockManager, BufferPool};
use super::metadata::MetaBlockPointer;
use super::single_file_block_manager::SingleFileBlockManager;
use super::standard_file_system::LocalFileSystem;
use super::storage_info::{
    BLOCK_HEADER_SIZE, DatabaseHeader, FileHandle, FileOpenFlags, FileSystem, MainHeader,
    StorageManagerOptions,
};
use super::storage_info::{StorageError, StorageResult};
use super::write_ahead_log::{WalInitState, WalWriter, WriteAheadLog};

// ─── FileWalWriter ─────────────────────────────────────────────────────────────

/// WAL 文件写入器（C++: `BufferedFileWriter` 的 `WalWriter` 适配）。
///
/// 内部使用 4 KiB 缓冲区，`sync()` 时将缓冲刷入文件并 fsync。
/// `total_written` 初始化为打开时的文件大小，后续追踪已刷盘字节数；
/// 这与 C++ `BufferedFileWriter::total_written = handle->GetFileSize()` 一致。
struct FileWalWriter {
    handle: Box<dyn FileHandle>,
    buffer: Vec<u8>,
    /// 尚未刷入磁盘的缓冲字节数。
    offset: usize,
    /// 已刷入磁盘的累计字节数（从文件初始大小起算）。
    total_written: u64,
}

const FILE_WAL_BUFFER_SIZE: usize = 4096;

impl FileWalWriter {
    /// 打开 WAL 文件，初始化写入器。
    ///
    /// `total_written` 初始化为文件当前大小，确保后续写入追加到文件末尾。
    fn new(handle: Box<dyn FileHandle>) -> io::Result<Self> {
        let initial_size = handle
            .file_size()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(Self {
            handle,
            buffer: vec![0u8; FILE_WAL_BUFFER_SIZE],
            offset: 0,
            total_written: initial_size,
        })
    }

    /// 将缓冲区内容刷入磁盘（不 fsync）。
    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.offset == 0 {
            return Ok(());
        }
        let write_at = self.total_written;
        let pending = self.offset;
        self.handle
            .write_at(&self.buffer[..pending], write_at)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        self.total_written += pending as u64;
        self.offset = 0;
        Ok(())
    }
}

impl WalWriter for FileWalWriter {
    fn write_data(&mut self, data: &[u8]) -> io::Result<()> {
        let mut pos = 0;
        while pos < data.len() {
            let available = FILE_WAL_BUFFER_SIZE - self.offset;
            let to_copy = (data.len() - pos).min(available);
            self.buffer[self.offset..self.offset + to_copy]
                .copy_from_slice(&data[pos..pos + to_copy]);
            self.offset += to_copy;
            pos += to_copy;
            if self.offset == FILE_WAL_BUFFER_SIZE {
                self.flush_buffer()?;
            }
        }
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.handle
            .sync()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    fn truncate(&mut self, size: u64) -> io::Result<()> {
        // C++: if (persistent <= size) { offset = size - persistent; }
        //      else { handle->Truncate(size); offset = 0; total_written = size; }
        if size >= self.total_written {
            self.offset = (size - self.total_written) as usize;
        } else {
            self.offset = 0;
            self.total_written = size;
            self.handle
                .truncate(size)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        Ok(())
    }

    fn file_size(&self) -> u64 {
        self.total_written + self.offset as u64
    }

    fn total_written(&self) -> u64 {
        self.total_written + self.offset as u64
    }
}

// ─── StorageOptions ────────────────────────────────────────────────────────────

/// 压缩内存模式（C++: `enum class CompressInMemory`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressInMemory {
    /// 自动（磁盘 DB 默认不压缩，内存 DB 可选）（C++: `AUTOMATIC`）。
    #[default]
    Automatic,
    /// 强制压缩（C++: `COMPRESS`）。
    Compress,
    /// 强制不压缩（C++: `DO_NOT_COMPRESS`）。
    DoNotCompress,
}

/// ATTACH 时传入的存储选项（C++: `struct StorageOptions`）。
///
/// 从 `ATTACH` 语句的 `WITH (...)` 选项解析而来。
#[derive(Debug, Clone, Default)]
pub struct StorageOptions {
    /// 块分配大小（C++: `optional_idx block_alloc_size`）。
    pub block_alloc_size: Option<u64>,

    /// 块头大小（加密时使用）（C++: `optional_idx block_header_size`）。
    pub block_header_size: Option<u64>,

    /// 是否加密（C++: `bool encryption`）。
    pub encryption: bool,

    /// 用户提供的加密密钥（C++: `shared_ptr<string> user_key`）。
    ///
    /// 初始化完成后应清零（`ClearUserKey`）。
    pub user_key: Option<Arc<Vec<u8>>>,

    /// RowGroup 大小（C++: `optional_idx row_group_size`）。
    pub row_group_size: Option<u64>,

    /// 存储序列化版本（C++: `optional_idx storage_version`）。
    pub storage_version: Option<u64>,

    /// 内存压缩模式（C++: `CompressInMemory compress_in_memory`）。
    pub compress_in_memory: CompressInMemory,
}

// ─── CheckpointOptions ─────────────────────────────────────────────────────────

/// Checkpoint 动作（C++: `enum class CheckpointAction`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CheckpointAction {
    /// 只在有 WAL 内容时才 checkpoint（C++: `CHECKPOINT_IF_DIRTY`）。
    #[default]
    CheckpointIfDirty,
    /// 强制 checkpoint（C++: `FORCE_CHECKPOINT`）。
    ForceCheckpoint,
    /// 总是 checkpoint（C++: `ALWAYS_CHECKPOINT`）。
    AlwaysCheckpoint,
}

/// Checkpoint 配置（C++: `struct CheckpointOptions`）。
#[derive(Debug, Clone, Default)]
pub struct CheckpointOptions {
    /// checkpoint 动作（C++: `CheckpointAction action`）。
    pub action: CheckpointAction,

    /// 触发 checkpoint 的事务 ID（C++: `optional_idx transaction_id`）。
    pub transaction_id: Option<u64>,
}

// ─── DatabaseSize ──────────────────────────────────────────────────────────────

/// 数据库磁盘使用统计（C++: `struct DatabaseSize`）。
#[derive(Debug, Default, Clone)]
pub struct DatabaseSize {
    /// 总块数（C++: `idx_t total_blocks`）。
    pub total_blocks: u64,
    /// 每块字节数（C++: `idx_t block_size`）。
    pub block_size: u64,
    /// 空闲块数（C++: `idx_t free_blocks`）。
    pub free_blocks: u64,
    /// 已用块数（C++: `idx_t used_blocks`）。
    pub used_blocks: u64,
    /// 总字节数（C++: `idx_t bytes`）。
    pub bytes: u64,
    /// WAL 大小（C++: `idx_t wal_size`）。
    pub wal_size: u64,
}

// ─── MetadataBlockInfo ─────────────────────────────────────────────────────────

/// 元数据块信息（C++: `struct MetadataBlockInfo`）。
#[derive(Debug, Clone)]
pub struct MetadataBlockInfo {
    pub block_id: u64,
    pub total_blocks: u64,
    pub free_blocks: u64,
}

// ─── PersistentCollectionData stub ─────────────────────────────────────────────

/// 持久化集合数据存根（C++: `struct PersistentCollectionData`）。
///
/// 用于序列化 RowGroupCollection 的元数据，支持乐观写入。
/// 完整实现需要 RowGroupCollection 序列化支持。
#[derive(Debug, Clone, Default)]
pub struct PersistentCollectionData {
    /// 是否包含更新（C++: `HasUpdates()`）。
    pub has_updates: bool,
    /// 序列化后的数据（占位）。
    pub data: Vec<u8>,
}

impl PersistentCollectionData {
    /// 创建新的空持久化数据。
    pub fn new() -> Self {
        Self::default()
    }

    /// 是否包含内存中的更新（C++: `HasUpdates()`）。
    ///
    /// 若有更新，无法序列化乐观写入的块指针。
    pub fn has_updates(&self) -> bool {
        self.has_updates
    }
}

// ─── StorageCommitState trait ──────────────────────────────────────────────────

/// 单次事务提交状态的抽象（C++: `class StorageCommitState`）。
///
/// # 生命周期
///
/// ```text
/// 创建 → InProgress
///   ├─→ FlushCommit() → Flushed（提交成功）
///   └─→ RevertCommit() → Truncated（提交失败/析构）
/// ```
///
/// 析构时若未调用 `flush_commit()`，自动调用 `revert_commit()`
/// （对应 C++: destructor calls `RevertCommit()` if not already flushed）。
///
/// # C++ 源码对应
/// ```cpp
/// class StorageCommitState {
/// public:
///     virtual ~StorageCommitState() { }
///     virtual void RevertCommit() = 0;
///     virtual void FlushCommit() = 0;
///     virtual void AddRowGroupData(DataTable &table, idx_t start_index, idx_t count,
///                                  unique_ptr<PersistentCollectionData> row_group_data) = 0;
///     virtual optional_ptr<PersistentCollectionData> GetRowGroupData(DataTable &table, idx_t start_index,
///                                                                    idx_t &count) = 0;
///     virtual bool HasRowGroupData() { return false; }
/// };
/// ```
pub trait StorageCommitState: Send {
    /// 回滚本次提交（截断 WAL 到提交前大小）（C++: `RevertCommit()`）。
    ///
    /// 调用时机：
    /// - 提交过程中发生异常
    /// - 析构时未调用 `flush_commit()`
    fn revert_commit(&mut self);

    /// 持久化本次提交（fsync WAL）（C++: `FlushCommit()`）。
    ///
    /// 成功后 WAL 数据已持久化到磁盘。
    /// 返回 `Ok(())` 表示成功，`Err` 表示 I/O 错误。
    fn flush_commit(&mut self) -> StorageResult<()>;

    /// 记录乐观写入的 RowGroup 数据（C++: `AddRowGroupData()`）。
    ///
    /// # 参数
    /// - `table_id`：表标识（替代 C++ 的 DataTable 引用）
    /// - `start_index`：RowGroup 起始行索引
    /// - `count`：RowGroup 行数
    /// - `row_group_data`：序列化后的 RowGroup 元数据
    ///
    /// # 跳过条件（与 C++ 一致）
    /// - `row_group_data.has_updates()` 为 true（无法序列化乐观块指针）
    /// - 表有索引（无法维护索引的乐观指针）
    fn add_row_group_data(
        &mut self,
        table_id: u64,
        start_index: u64,
        count: u64,
        row_group_data: PersistentCollectionData,
    );

    /// 获取之前记录的 RowGroup 数据（C++: `GetRowGroupData()`）。
    ///
    /// 返回 `Some((&PersistentCollectionData, count))` 或 `None`（未乐观写入）。
    fn get_row_group_data(
        &self,
        table_id: u64,
        start_index: u64,
    ) -> Option<(&PersistentCollectionData, u64)>;

    /// 是否有任何乐观写入的 RowGroup 数据（C++: `HasRowGroupData()`）。
    fn has_row_group_data(&self) -> bool {
        false
    }

    /// 返回当前提交阶段关联的 WAL（若存在）。
    fn wal(&self) -> Option<Arc<WriteAheadLog>> {
        None
    }
}

// ─── WAL 提交阶段状态 ─────────────────────────────────────────────────────────

/// WAL 提交阶段（C++: `enum class WALCommitState`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalCommitState {
    /// 提交进行中（C++: `IN_PROGRESS`）。
    InProgress,
    /// 已 fsync（C++: `FLUSHED`）。
    Flushed,
    /// 已截断（回滚）（C++: `TRUNCATED`）。
    Truncated,
}

// ─── OptimisticallyWrittenRowGroupData ────────────────────────────────────────

/// 乐观写入的行组数据条目（C++: `struct OptimisticallyWrittenRowGroupData`）。
///
/// 对应 C++:
/// ```cpp
/// struct OptimisticallyWrittenRowGroupData {
///     idx_t start;
///     idx_t count;
///     unique_ptr<PersistentCollectionData> row_group_data;
/// };
/// ```
struct OptimisticallyWrittenEntry {
    /// RowGroup 行数（C++: `idx_t count`）。
    count: u64,
    /// 序列化后的 RowGroup 元数据（C++: `unique_ptr<PersistentCollectionData> row_group_data`）。
    row_group_data: PersistentCollectionData,
}

// ─── SingleFileStorageCommitState ─────────────────────────────────────────────

/// 单文件存储提交状态（C++: `class SingleFileStorageCommitState`）。
///
/// # 职责
///
/// 1. 记录提交前的 WAL 大小，用于失败时截断
/// 2. 管理乐观写入的 RowGroup 元数据
/// 3. 在 Flush/Revert 时更新状态
///
/// # C++ 源码对应
/// ```cpp
/// class SingleFileStorageCommitState : public StorageCommitState {
/// public:
///     SingleFileStorageCommitState(StorageManager &storage, WriteAheadLog &wal);
///     ~SingleFileStorageCommitState() override;
///
///     void RevertCommit() override;
///     void FlushCommit() override;
///
///     void AddRowGroupData(DataTable &table, idx_t start_index, idx_t count,
///                          unique_ptr<PersistentCollectionData> row_group_data) override;
///     optional_ptr<PersistentCollectionData> GetRowGroupData(DataTable &table, idx_t start_index,
///                                                            idx_t &count) override;
///     bool HasRowGroupData() override;
///
/// private:
///     idx_t initial_wal_size = 0;
///     idx_t initial_written = 0;
///     WriteAheadLog &wal;
///     WALCommitState state;
///     reference_map_t<DataTable, unordered_map<idx_t, OptimisticallyWrittenRowGroupData>> optimistically_written_data;
/// };
/// ```
///
/// 创建时记录 WAL 的初始大小；析构时若未 flush，截断 WAL 到初始大小。
pub struct SingleFileStorageCommitState {
    /// 提交前的 WAL 文件大小（用于回滚截断）（C++: `idx_t initial_wal_size`）。
    initial_wal_size: u64,

    /// 提交前写入的总字节数（用于判断是否有新内容）（C++: `idx_t initial_written`）。
    initial_written: u64,

    /// WAL 引用（C++: `WriteAheadLog &wal`）。
    wal: Arc<Mutex<WalRef>>,

    /// 当前提交阶段（C++: `WALCommitState state`）。
    state: WalCommitState,

    /// 乐观写入的行组：table_id → (start_index → entry)。
    /// C++: `reference_map_t<DataTable, unordered_map<idx_t, OptimisticallyWrittenRowGroupData>>`
    optimistically_written:
        std::collections::HashMap<u64, std::collections::HashMap<u64, OptimisticallyWrittenEntry>>,
}

/// WAL 操作的简化包装，避免持有 WriteAheadLog 的直接引用。
struct WalRef {
    /// 对 WriteAheadLog 的共享引用（C++ 中是引用成员）。
    wal: Option<Arc<WriteAheadLog>>,
}

impl SingleFileStorageCommitState {
    /// 构造（C++: `SingleFileStorageCommitState(StorageManager&, WriteAheadLog&)`）。
    pub fn new(initial_wal_size: u64, initial_written: u64, wal: Arc<WriteAheadLog>) -> Self {
        Self {
            initial_wal_size,
            initial_written,
            wal: Arc::new(Mutex::new(WalRef { wal: Some(wal) })),
            state: WalCommitState::InProgress,
            optimistically_written: Default::default(),
        }
    }
}

impl StorageCommitState for SingleFileStorageCommitState {
    /// 截断 WAL 到提交前大小（C++: `RevertCommit()`）。
    ///
    /// # C++ 源码
    /// ```cpp
    /// void SingleFileStorageCommitState::RevertCommit() {
    ///     if (state != WALCommitState::IN_PROGRESS) {
    ///         return;
    ///     }
    ///     if (wal.GetTotalWritten() > initial_written) {
    ///         wal.Truncate(initial_wal_size);
    ///     }
    ///     state = WALCommitState::TRUNCATED;
    /// }
    /// ```
    fn revert_commit(&mut self) {
        if self.state != WalCommitState::InProgress {
            // 已处理过（Flushed 或 Truncated）
            return;
        }

        let wal_ref = self.wal.lock();
        if let Some(wal) = &wal_ref.wal {
            // C++: if (wal.GetTotalWritten() > initial_written)
            if wal.total_written() > self.initial_written {
                // C++: wal.Truncate(initial_wal_size)
                // 截断 WAL 到提交前的大小
                let _ = wal.truncate(self.initial_wal_size);
            }
        }
        self.state = WalCommitState::Truncated;
    }

    /// fsync WAL（C++: `FlushCommit()`）。
    ///
    /// # C++ 源码
    /// ```cpp
    /// void SingleFileStorageCommitState::FlushCommit() {
    ///     if (state != WALCommitState::IN_PROGRESS) {
    ///         return;
    ///     }
    ///     wal.Flush();
    ///     state = WALCommitState::FLUSHED;
    /// }
    /// ```
    fn flush_commit(&mut self) -> StorageResult<()> {
        if self.state != WalCommitState::InProgress {
            // 已处理过，幂等返回
            return Ok(());
        }

        let wal_ref = self.wal.lock();
        if let Some(wal) = &wal_ref.wal {
            // C++: wal.Flush()
            wal.flush().map_err(StorageError::Io)?;
        }
        self.state = WalCommitState::Flushed;
        Ok(())
    }

    /// 记录乐观写入的 RowGroup 数据（C++: `AddRowGroupData()`）。
    ///
    /// # C++ 源码
    /// ```cpp
    /// void SingleFileStorageCommitState::AddRowGroupData(DataTable &table, idx_t start_index, idx_t count,
    ///                                                      unique_ptr<PersistentCollectionData> row_group_data) {
    ///     if (row_group_data->HasUpdates()) {
    ///         // cannot serialize optimistic block pointers if in-memory updates exist
    ///         return;
    ///     }
    ///     if (table.HasIndexes()) {
    ///         // cannot serialize optimistic block pointers if the table has indexes
    ///         return;
    ///     }
    ///     auto &entries = optimistically_written_data[table];
    ///     auto entry = entries.find(start_index);
    ///     if (entry != entries.end()) {
    ///         throw InternalException("FIXME: AddOptimisticallyWrittenRowGroup is writing a duplicate row group");
    ///     }
    ///     entries.insert(make_pair(start_index, OptimisticallyWrittenRowGroupData(start_index, count, std::move(row_group_data))));
    /// }
    /// ```
    fn add_row_group_data(
        &mut self,
        table_id: u64,
        start_index: u64,
        count: u64,
        row_group_data: PersistentCollectionData,
    ) {
        // C++: if (row_group_data->HasUpdates()) { return; }
        if row_group_data.has_updates() {
            // 无法序列化包含内存更新的乐观块指针
            return;
        }

        // 注意：C++ 中还会检查 table.HasIndexes()
        // Rust 版本需要调用方在传入前检查，或扩展 trait 以支持传入 has_indexes 参数

        let entries = self.optimistically_written.entry(table_id).or_default();

        // C++: if (entry != entries.end()) { throw InternalException(...); }
        if entries.contains_key(&start_index) {
            // 重复的 RowGroup，忽略或 panic
            // 当前实现：忽略（不抛出异常）
            return;
        }

        entries.insert(
            start_index,
            OptimisticallyWrittenEntry {
                count,
                row_group_data,
            },
        );
    }

    /// 获取之前记录的 RowGroup 数据（C++: `GetRowGroupData()`）。
    ///
    /// # C++ 源码
    /// ```cpp
    /// optional_ptr<PersistentCollectionData> SingleFileStorageCommitState::GetRowGroupData(DataTable &table,
    ///                                                                                       idx_t start_index, idx_t &count) {
    ///     auto entry = optimistically_written_data.find(table);
    ///     if (entry == optimistically_written_data.end()) {
    ///         return nullptr;
    ///     }
    ///     auto &row_groups = entry->second;
    ///     auto start_entry = row_groups.find(start_index);
    ///     if (start_entry == row_groups.end()) {
    ///         return nullptr;
    ///     }
    ///     count = start_entry->second.count;
    ///     return start_entry->second.row_group_data.get();
    /// }
    /// ```
    fn get_row_group_data(
        &self,
        table_id: u64,
        start_index: u64,
    ) -> Option<(&PersistentCollectionData, u64)> {
        let entries = self.optimistically_written.get(&table_id)?;
        let entry = entries.get(&start_index)?;
        Some((&entry.row_group_data, entry.count))
    }

    /// 是否有任何乐观写入的 RowGroup 数据（C++: `HasRowGroupData()`）。
    fn has_row_group_data(&self) -> bool {
        !self.optimistically_written.is_empty()
    }

    fn wal(&self) -> Option<Arc<WriteAheadLog>> {
        self.wal.lock().wal.clone()
    }
}

impl Drop for SingleFileStorageCommitState {
    /// 析构时若未 flush，截断 WAL（C++: `~SingleFileStorageCommitState()`）。
    fn drop(&mut self) {
        if self.state != WalCommitState::InProgress {
            return;
        }
        self.revert_commit();
    }
}

// ─── StorageManager trait ──────────────────────────────────────────────────────

/// 存储管理器抽象接口（C++: `class StorageManager`）。
///
/// 由 `SingleFileStorageManager`（磁盘 DB）和 `InMemoryStorageManager`（内存 DB）实现。
pub trait StorageManager: Send + Sync {
    // ── 核心生命周期 ──────────────────────────────────────────────────────────

    /// 初始化存储，加载数据库（C++: `Initialize(QueryContext)`）。
    fn initialize(&self) -> StorageResult<()>;

    /// 是否为内存数据库（C++: `InMemory()`）。
    fn in_memory(&self) -> bool;

    /// 数据库路径（C++: `GetDBPath()`）。
    fn db_path(&self) -> &str;

    /// 是否已完成加载（C++: `IsLoaded()`）。
    fn is_loaded(&self) -> bool;

    // ── WAL 管理 ──────────────────────────────────────────────────────────────

    /// 当前 WAL 大小（字节）（C++: `GetWALSize()`）。
    fn wal_size(&self) -> u64;

    /// 是否有 WAL（C++: `HasWAL()`）。
    fn has_wal(&self) -> bool;

    /// 递增 WAL 大小统计（C++: `AddWALSize(idx_t)`）。
    fn add_wal_size(&self, size: u64);

    /// 设置 WAL 大小统计（C++: `SetWALSize(idx_t)`）。
    fn set_wal_size(&self, size: u64);

    /// WAL 文件路径（C++: `GetWALPath(suffix="wal")`）。
    fn wal_path(&self) -> String;

    /// Checkpoint WAL 文件路径（C++: `GetCheckpointWALPath()`）。
    fn checkpoint_wal_path(&self) -> String;

    // ── Checkpoint ────────────────────────────────────────────────────────────

    /// 开始 checkpoint：写 CHECKPOINT 记录，切换 WAL（C++: `WALStartCheckpoint()`）。
    ///
    /// 返回 `true` 如果确实有 WAL 需要处理。
    fn wal_start_checkpoint(
        &self,
        meta_block: MetaBlockPointer,
        options: &mut CheckpointOptions,
    ) -> StorageResult<bool>;

    /// 结束 checkpoint：合并临时 WAL（C++: `WALFinishCheckpoint()`）。
    fn wal_finish_checkpoint(&self) -> StorageResult<()>;

    /// 是否需要自动触发 checkpoint（C++: `AutomaticCheckpoint(idx_t)`）。
    fn automatic_checkpoint(&self, estimated_wal_bytes: u64) -> bool;

    /// 执行 checkpoint（C++: `CreateCheckpoint(QueryContext, CheckpointOptions)`）。
    fn create_checkpoint(&self, options: CheckpointOptions) -> StorageResult<()>;

    /// 最后一次 checkpoint 是否干净（无未持久化数据）（C++: `IsCheckpointClean()`）。
    fn is_checkpoint_clean(&self, checkpoint_id: MetaBlockPointer) -> bool;

    // ── 存储状态 ──────────────────────────────────────────────────────────────

    /// 获取数据库磁盘使用统计（C++: `GetDatabaseSize()`）。
    fn database_size(&self) -> DatabaseSize;

    /// 获取元数据块信息（C++: `GetMetadataInfo()`）。
    fn metadata_info(&self) -> Vec<MetadataBlockInfo>;

    /// 获取存储序列化版本（C++: `GetStorageVersion()`）。
    fn storage_version(&self) -> Option<u64>;

    /// 设置存储序列化版本（C++: `SetStorageVersion()`）。
    fn set_storage_version(&self, version: u64);

    // ── 块管理器 ──────────────────────────────────────────────────────────────

    /// 获取块管理器（C++: `GetBlockManager()`）。
    fn block_manager(&self) -> &dyn BlockManager;

    // ── 提交状态工厂 ──────────────────────────────────────────────────────────

    /// 为当前事务创建提交状态（C++: `GenStorageCommitState(WriteAheadLog&)`）。
    fn gen_storage_commit_state(&self) -> Option<Box<dyn StorageCommitState>>;

    // ── 销毁 ──────────────────────────────────────────────────────────────────

    /// 销毁所有表数据（C++: `Destroy()`）。
    fn destroy(&self) {}
}

// ─── SingleFileStorageManager ──────────────────────────────────────────────────

/// 内部可变 WAL 状态（Mutex 保护）。
struct WalState {
    /// 当前活跃的 WAL（C++: `unique_ptr<WriteAheadLog> wal`）。
    wal: Option<Arc<WriteAheadLog>>,
    /// WAL 文件路径（C++: `string wal_path`）。
    wal_path: String,
}

/// 单文件存储管理器（C++: `class SingleFileStorageManager : public StorageManager`）。
///
/// 将整个数据库存储在一个文件中（`*.db`），WAL 存储在 `*.wal`。
pub struct SingleFileStorageManager {
    /// 数据库文件路径（C++: `string path`）。
    path: String,

    /// 是否只读（C++: `bool read_only`）。
    read_only: bool,

    /// WAL 相关状态（C++: `unique_ptr<WriteAheadLog> wal` + `mutex wal_lock` + `string wal_path`）。
    wal_state: Mutex<WalState>,

    /// WAL 大小统计（原子，供并发读取）（C++: `atomic<idx_t> wal_size`）。
    wal_size_atomic: Arc<AtomicU64>,

    /// 是否完成加载（C++: `bool load_complete`）。
    load_complete: AtomicBool,

    /// 存储序列化版本（C++: `optional_idx storage_version`）。
    storage_version: Mutex<Option<u64>>,

    /// 存储选项（C++: `StorageOptions storage_options`）。
    pub storage_options: StorageOptions,

    /// 块管理器（C++: `unique_ptr<BlockManager> block_manager`）。
    ///
    /// 内部使用 `Mutex` 保护，允许延迟初始化。
    block_manager_inner: Mutex<Option<Arc<dyn BlockManager>>>,
    db_instance: Mutex<Option<Weak<DatabaseInstance>>>,
}

impl SingleFileStorageManager {
    // ─── 内存路径常量 ──────────────────────────────────────────────────────────

    /// 内存数据库的特殊路径标识（C++: `IN_MEMORY_PATH = ":memory:"`）。
    pub const IN_MEMORY_PATH: &'static str = ":memory:";

    /// 构造（C++: `SingleFileStorageManager(AttachedDatabase&, string, AttachOptions&)`）。
    pub fn new(path: String, read_only: bool, options: StorageOptions) -> Self {
        let resolved_path = if path.is_empty() {
            Self::IN_MEMORY_PATH.to_string()
        } else {
            path
        };
        let wal_path = Self::compute_wal_path(&resolved_path, ".wal");
        Self {
            path: resolved_path,
            read_only,
            wal_state: Mutex::new(WalState {
                wal: None,
                wal_path,
            }),
            wal_size_atomic: Arc::new(AtomicU64::new(0)),
            load_complete: AtomicBool::new(false),
            storage_version: Mutex::new(None),
            storage_options: options,
            block_manager_inner: Mutex::new(None),
            db_instance: Mutex::new(None),
        }
    }

    // ── WAL 路径计算 ──────────────────────────────────────────────────────────

    /// 计算 WAL 路径（C++: `GetWALPath(suffix)`）。
    ///
    /// 在 `?` 参数前（或文件末尾）插入 suffix。
    /// 不处理 Windows 长路径（`\\?\` 前缀）。
    fn compute_wal_path(db_path: &str, suffix: &str) -> String {
        if db_path == Self::IN_MEMORY_PATH {
            return String::new();
        }
        // Windows 长路径跳过 ? 处理
        if db_path.starts_with("\\\\?\\") {
            return format!("{}{}", db_path, suffix);
        }
        match db_path.find('?') {
            Some(pos) => {
                let mut result = db_path.to_string();
                result.insert_str(pos, suffix);
                result
            }
            None => format!("{}{}", db_path, suffix),
        }
    }

    // ── 内部 WAL 操作 ─────────────────────────────────────────────────────────

    /// 获取当前活跃 WAL（若存在）。
    pub fn get_wal(&self) -> Option<Arc<WriteAheadLog>> {
        if self.in_memory() || self.read_only || !self.load_complete.load(Ordering::Acquire) {
            return None;
        }
        self.wal_state.lock().wal.clone()
    }

    /// 获取数据库磁盘 WAL 大小（字节）（C++: `GetWALSize()`）。
    pub fn get_wal_size(&self) -> u64 {
        self.wal_size_atomic.load(Ordering::Relaxed)
    }

    pub fn bind_database_instance(&self, db_instance: Weak<DatabaseInstance>) {
        *self.db_instance.lock() = Some(db_instance);
    }

    fn checkpoint_tables(&self) -> StorageResult<Vec<crate::storage::checkpoint_manager::TableInfo>> {
        let db_instance = self
            .db_instance
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                StorageError::Other("StorageManager is not bound to DatabaseInstance".into())
            })?;

        let tables = db_instance.tables.lock();
        Ok(tables
            .values()
            .map(|handle| crate::storage::checkpoint_manager::TableInfo {
                entry: Arc::new(handle.catalog_entry.clone()),
                storage: handle.storage.clone(),
            })
            .collect())
    }

    fn read_active_header(&self, fs: &dyn FileSystem) -> StorageResult<DatabaseHeader> {
        let mut file = fs.open_file(&self.path, FileOpenFlags::READ)?;

        let mut main_header_buf = vec![0u8; super::storage_info::FILE_HEADER_SIZE];
        file.read_at(&mut main_header_buf, 0)?;
        let payload = &main_header_buf[BLOCK_HEADER_SIZE..];
        MainHeader::deserialize(payload).ok_or_else(|| StorageError::Corrupt {
            msg: "Invalid DuckDB main header".to_string(),
        })?;

        let mut header_buf = vec![0u8; super::storage_info::FILE_HEADER_SIZE];
        file.read_at(
            &mut header_buf,
            super::storage_info::FILE_HEADER_SIZE as u64,
        )?;
        let h0 = DatabaseHeader::deserialize(
            &header_buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + DatabaseHeader::SERIALIZED_SIZE],
        );
        file.read_at(
            &mut header_buf,
            (super::storage_info::FILE_HEADER_SIZE * 2) as u64,
        )?;
        let h1 = DatabaseHeader::deserialize(
            &header_buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + DatabaseHeader::SERIALIZED_SIZE],
        );
        let headers = [h0, h1];
        Ok(headers[MainHeader::active_header_idx(&headers)].clone())
    }

    pub fn install_recovered_wal(&self, wal_size: u64, init_state: WalInitState) {
        if self.in_memory() || self.read_only {
            return;
        }
        let wal_path = self.wal_path();
        self.wal_state.lock().wal = Some(Arc::new(WriteAheadLog::new(
            self,
            wal_path,
            wal_size,
            init_state,
            None,
        )));
    }
}

struct NoopBlockAllocator;

impl BlockAllocator for NoopBlockAllocator {
    fn flush_all(&self, _extra_memory: usize) {}
}

impl StorageManager for SingleFileStorageManager {
    fn initialize(&self) -> StorageResult<()> {
        if self.in_memory() && self.read_only {
            return Err(StorageError::Other(
                "Cannot launch in-memory database in read-only mode!".into(),
            ));
        }
        if self.load_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.in_memory() {
            self.load_complete.store(true, Ordering::Release);
            return Ok(());
        }

        let fs = Arc::new(LocalFileSystem);
        let allocator: Arc<dyn BlockAllocator> = Arc::new(NoopBlockAllocator);
        let buffer_pool = BufferPool::new(allocator, 512 * 1024 * 1024, false, 0);
        let buffer_manager = StandardBufferManager::new(buffer_pool, None, fs.clone());
        let block_options = StorageManagerOptions {
            read_only: self.read_only,
            use_direct_io: false,
            debug_initialize: None,
            block_alloc_size: self.storage_options.block_alloc_size.unwrap_or(0) as usize,
            block_header_size: self.storage_options.block_header_size.map(|v| v as usize),
            storage_version: self.storage_options.storage_version,
            encryption_options: Default::default(),
        };
        let block_manager = SingleFileBlockManager::new(
            buffer_manager.clone(),
            fs,
            self.path.clone(),
            &block_options,
        );
        buffer_manager.set_block_manager(block_manager.clone());
        block_manager.initialize()?;
        let block_manager: Arc<dyn BlockManager> = block_manager;
        *self.block_manager_inner.lock() = Some(block_manager);

        // 初始化 WAL（C++: wal = make_uniq<WriteAheadLog>(*this, wal_path)）
        if !self.read_only {
            let wal_path = self.wal_path();
            if !wal_path.is_empty() {
                // C++: make_uniq<WriteAheadLog>(*this, wal_path, 0, NO_WAL, nullopt)
                let wal = Arc::new(WriteAheadLog::new(
                    self,
                    wal_path,
                    0,
                    WalInitState::NoWal,
                    None,
                ));
                self.wal_state.lock().wal = Some(wal);
            }
        }

        self.load_complete.store(true, Ordering::Release);
        Ok(())
    }

    fn in_memory(&self) -> bool {
        self.path == Self::IN_MEMORY_PATH
    }

    fn db_path(&self) -> &str {
        &self.path
    }

    fn is_loaded(&self) -> bool {
        self.load_complete.load(Ordering::Acquire)
    }

    // ── WAL ───────────────────────────────────────────────────────────────────

    fn wal_size(&self) -> u64 {
        self.wal_size_atomic.load(Ordering::Relaxed)
    }

    fn has_wal(&self) -> bool {
        if self.in_memory() || self.read_only || !self.load_complete.load(Ordering::Acquire) {
            return false;
        }
        true
    }

    fn add_wal_size(&self, size: u64) {
        self.wal_size_atomic.fetch_add(size, Ordering::Relaxed);
    }

    fn set_wal_size(&self, size: u64) {
        self.wal_size_atomic.store(size, Ordering::Relaxed);
    }

    fn wal_path(&self) -> String {
        self.wal_state.lock().wal_path.clone()
    }

    fn checkpoint_wal_path(&self) -> String {
        Self::compute_wal_path(&self.path, ".checkpoint.wal")
    }

    // ── Checkpoint ────────────────────────────────────────────────────────────

    fn wal_start_checkpoint(
        &self,
        meta_block: MetaBlockPointer,
        options: &mut CheckpointOptions,
    ) -> StorageResult<bool> {
        // C++:
        //   1. lock_guard<mutex> guard(wal_lock)
        //   2. options.transaction_id = transaction_manager.GetNewCheckpointId()
        //   3. if (!wal) return false
        //   4. if (GetWALSize() == 0) return false
        //   5. wal->WriteCheckpoint(meta_block); wal->Flush()
        //   6. wal.reset()
        //   7. 创建 checkpoint WAL（.checkpoint.wal），next_checkpoint_iteration

        println!("🟡 [CHECKPOINT] WALStartCheckpoint 开始");
        println!("   - 数据库路径: {}", self.path);
        println!(
            "   - 当前 WAL 大小: {} bytes",
            self.wal_size_atomic.load(Ordering::Relaxed)
        );

        let mut wal_state = self.wal_state.lock();

        // 设置 checkpoint 事务 ID（简化：使用当前时间戳作为 ID 占位）
        options.transaction_id = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0),
        );

        let wal = match &wal_state.wal {
            Some(w) => w.clone(),
            None => {
                println!("   ⚠️  WAL 不存在，跳过 checkpoint");
                return Ok(false);
            }
        };

        if self.wal_size_atomic.load(Ordering::Relaxed) == 0 {
            println!("   ⚠️  WAL 大小为 0，跳过 checkpoint");
            return Ok(false);
        }

        // 验证 WAL 路径匹配
        let base_wal_path = wal_state.wal_path.clone();
        if wal.wal_path != base_wal_path {
            return Err(StorageError::Other(format!(
                "Current WAL path {} does not match base WAL path {}",
                wal.wal_path, base_wal_path
            )));
        }

        // 写 CHECKPOINT 记录并 flush
        wal.write_checkpoint(meta_block).map_err(StorageError::Io)?;
        wal.flush().map_err(StorageError::Io)?;

        // 关闭主 WAL
        wal_state.wal = None;

        // 创建 checkpoint WAL（.checkpoint.wal）
        // C++: fs.TryRemoveFile(checkpoint_wal_path) — 删除残留的旧 checkpoint WAL
        let checkpoint_wal_path = Self::compute_wal_path(&self.path, ".checkpoint.wal");
        let _ = std::fs::remove_file(&checkpoint_wal_path); // 忽略不存在的情况

        // C++: SingleFileBlockManager::GetCheckpointIteration() + 1
        // 这里简化为 None，由 WAL 内部处理
        let next_checkpoint_iteration: Option<u64> = None;

        // C++: wal = make_uniq<WriteAheadLog>(*this, checkpoint_wal_path, 0ULL, NO_WAL, next_checkpoint_iteration)
        // 并发写入的新事务将写入此临时 checkpoint WAL 文件
        use super::write_ahead_log::{WalInitState, WriteAheadLog as Wal};
        let checkpoint_wal = Wal::new(
            self,
            checkpoint_wal_path.clone(),
            0,
            WalInitState::NoWal,
            next_checkpoint_iteration,
        );
        wal_state.wal = Some(Arc::new(checkpoint_wal));

        println!("   ✅ WALStartCheckpoint 完成");
        println!("   - 主 WAL 已关闭");
        println!("   - Checkpoint WAL 已创建: {}", checkpoint_wal_path);

        Ok(true)
    }

    fn wal_finish_checkpoint(&self) -> StorageResult<()> {
        // C++:
        //   1. lock_guard<mutex> guard(wal_lock)
        //   2. if (!wal->Initialized()) → 无并发写 → 删除主 WAL，重新创建空 WAL
        //   3. 否则：关闭 checkpoint WAL，MoveFile(.checkpoint.wal → .wal)，重新打开

        println!("🟡 [CHECKPOINT] WALFinishCheckpoint 开始");

        let mut wal_state = self.wal_state.lock();
        let wal = wal_state
            .wal
            .take()
            .ok_or_else(|| StorageError::Other("WAL not set in WALFinishCheckpoint".into()))?;

        if !wal.is_initialized() {
            // 情形 A：无并发写 — checkpoint WAL 从未初始化（没有新的并发事务写入）
            println!("   - 情形 A: 无并发写入，删除主 WAL");
            // C++: fs.TryRemoveFile(wal_path) — 删除主 WAL 文件
            let _ = std::fs::remove_file(&wal_state.wal_path);

            // 重新创建空 WAL（Uninitialized 状态），下次有写操作时才真正打开文件
            let new_wal = super::write_ahead_log::WriteAheadLog::new(
                self,
                wal_state.wal_path.clone(),
                0,
                super::write_ahead_log::WalInitState::Uninitialized,
                None,
            );
            wal_state.wal = Some(Arc::new(new_wal));
        } else {
            // 情形 B：有并发写 — checkpoint WAL 已初始化（收到了并发事务写入）
            println!("   - 情形 B: 有并发写入，合并 checkpoint WAL");
            // C++: drop(wal) — 关闭 checkpoint WAL writer（确保数据 flush 到磁盘）
            let checkpoint_path = wal.wal_path.clone();
            drop(wal);

            // C++: fs.MoveFile(.checkpoint.wal → .wal) — 原子替换主 WAL
            // 这样主 WAL 包含 Checkpoint 之后的并发写入，可安全重放
            if let Err(e) = std::fs::rename(&checkpoint_path, &wal_state.wal_path) {
                // 若 rename 失败（跨设备等情况），尝试 copy + remove
                if let Ok(data) = std::fs::read(&checkpoint_path) {
                    let _ = std::fs::write(&wal_state.wal_path, &data);
                    let _ = std::fs::remove_file(&checkpoint_path);
                } else {
                    return Err(StorageError::Io(e));
                }
            }

            // 重新打开主 WAL（追加模式），接收后续写入
            // C++: new WriteAheadLog(..., Uninitialized) → Initialize()
            let new_wal = super::write_ahead_log::WriteAheadLog::new(
                self,
                wal_state.wal_path.clone(),
                0,
                super::write_ahead_log::WalInitState::UninitializedRequiresTruncate,
                None,
            );
            wal_state.wal = Some(Arc::new(new_wal));
        }

        println!("   ✅ WALFinishCheckpoint 完成");
        println!(
            "   - WAL 大小: {} bytes",
            self.wal_size_atomic.load(Ordering::Relaxed)
        );

        Ok(())
    }

    fn automatic_checkpoint(&self, estimated_wal_bytes: u64) -> bool {
        // C++: initial_size + estimated_wal_bytes > config.options.checkpoint_wal_size
        // 默认阈值 16 MiB，与 DuckDB C++ 保持一致
        // 配置项：DBConfig::options.checkpoint_wal_size = 1 << 24
        const DEFAULT_CHECKPOINT_WAL_SIZE: u64 = 16 * 1024 * 1024; // 16 MiB
        if self.in_memory() || self.read_only {
            // 内存数据库和只读数据库不需要自动 Checkpoint
            return false;
        }

        // 获取实际的 WAL 文件大小，而不是使用 wal_size_atomic
        // 因为 wal_size_atomic 在 WAL flush 后没有被更新
        // 参考 DuckDB: initial_size = GetWALSize()，其中 GetWALSize() 返回 wal_size 成员变量
        // 在 DuckDB 中，wal_size 在 WAL 的 Flush()/Truncate() 后通过 SetWALSize() 更新
        let initial_size = self.get_wal_size();

        // 当前 WAL 大小 + 本次事务预估写入量 > 阈值 → 触发 Checkpoint
        initial_size.saturating_add(estimated_wal_bytes) > DEFAULT_CHECKPOINT_WAL_SIZE
    }

    fn create_checkpoint(&self, mut options: CheckpointOptions) -> StorageResult<()> {
        if self.read_only || !self.load_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.in_memory() {
            return Ok(());
        }

        let wal_size = self.get_wal_size();
        let force_checkpoint = matches!(
            options.action,
            CheckpointAction::ForceCheckpoint | CheckpointAction::AlwaysCheckpoint
        );
        if wal_size == 0 && !force_checkpoint {
            return Ok(());
        }

        let fs = Arc::new(LocalFileSystem);
        let allocator: Arc<dyn BlockAllocator> = Arc::new(NoopBlockAllocator);
        let buffer_pool = BufferPool::new(allocator, 256 * 1024 * 1024, false, 0);
        let buffer_manager = StandardBufferManager::new(buffer_pool.clone(), None, fs.clone());
        let current_header = self.read_active_header(&*fs)?;

        let block_manager = SingleFileBlockManager::new(
            buffer_manager.clone(),
            fs,
            self.path.clone(),
            &StorageManagerOptions {
                read_only: false,
                use_direct_io: false,
                debug_initialize: None,
                block_alloc_size: current_header.block_alloc_size as usize,
                block_header_size: Some(BLOCK_HEADER_SIZE),
                storage_version: Some(current_header.serialization_compatibility),
                encryption_options: Default::default(),
            },
        );
        buffer_manager.set_block_manager(block_manager.clone());
        block_manager.initialize()?;

        let metadata_manager = Arc::new(super::metadata::MetadataManager::new(
            block_manager.clone() as Arc<dyn BlockManager>,
            buffer_manager,
        ));
        block_manager.set_metadata_manager(metadata_manager.clone());

        let table_infos = self.checkpoint_tables()?;
        let checkpoint_manager = crate::storage::checkpoint_manager::CheckpointManager::new(
            block_manager.clone() as Arc<dyn BlockManager>,
            metadata_manager,
        );
        let mut has_wal = false;
        let new_header =
            checkpoint_manager.create_checkpoint_with_meta(&table_infos, |meta_block| {
                has_wal = self.wal_start_checkpoint(meta_block, &mut options)?;
                Ok(())
            })?;
        block_manager.write_header_with_free_list(new_header)?;
        if has_wal {
            self.wal_finish_checkpoint()?;
        }
        Ok(())
    }

    fn is_checkpoint_clean(&self, checkpoint_id: MetaBlockPointer) -> bool {
        if self.in_memory() || !self.load_complete.load(Ordering::Acquire) {
            return false;
        }
        let fs = LocalFileSystem;
        self.read_active_header(&fs)
            .map(|header| {
                header.meta_block == checkpoint_id.block_pointer as i64 && checkpoint_id.offset == 0
            })
            .unwrap_or(false)
    }

    fn database_size(&self) -> DatabaseSize {
        if self.in_memory() {
            return DatabaseSize::default();
        }
        // C++: block_manager.TotalBlocks(), FreeBlocks(), GetBlockAllocSize()
        // 返回当前已知的 WAL 大小（原子计数器），其余字段从 block_manager 获取
        DatabaseSize {
            wal_size: self.wal_size_atomic.load(Ordering::Relaxed),
            ..DatabaseSize::default()
        }
    }

    fn metadata_info(&self) -> Vec<MetadataBlockInfo> {
        // C++: block_manager.GetMetadataManager().GetMetadataInfo()
        Vec::new()
    }

    fn storage_version(&self) -> Option<u64> {
        *self.storage_version.lock()
    }

    fn set_storage_version(&self, version: u64) {
        *self.storage_version.lock() = Some(version);
    }

    fn block_manager(&self) -> &dyn BlockManager {
        panic!(
            "SingleFileStorageManager::block_manager requires Arc-based access in the current Rust port"
        )
    }

    fn gen_storage_commit_state(&self) -> Option<Box<dyn StorageCommitState>> {
        // C++: auto wal = storage_manager.GetWAL();
        //      commit_state = storage_manager.GenStorageCommitState(*wal);
        //
        // 先在锁内取出 WAL（Arc），再释放锁后懒初始化写入器，
        // 避免 wal_state 锁与 WriteAheadLog.writer 锁交叉持有。
        let wal = {
            let wal_state = self.wal_state.lock();
            wal_state.wal.clone()?
        };

        // C++: WriteAheadLog::LazyInit() → Initialize() — 创建底层 BufferedFileWriter
        // Rust 中通过 initialize() + 工厂闭包延迟创建 FileWalWriter。
        let init_result = wal.initialize(|wal_path, _wal_size| {
            let fs = LocalFileSystem;
            let flags = FileOpenFlags::WRITE | FileOpenFlags::CREATE;
            let handle = fs
                .open_file(wal_path, flags)
                .map_err(|e: StorageError| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            FileWalWriter::new(handle).map(|w| Box::new(w) as Box<dyn WalWriter>)
        });

        if let Err(e) = init_result {
            eprintln!("[WAL] initialize failed: {e}");
            return None;
        }

        let initial_wal_size = self.wal_size_atomic.load(Ordering::Relaxed);
        let initial_written = wal.total_written();
        Some(Box::new(SingleFileStorageCommitState::new(
            initial_wal_size,
            initial_written,
            wal,
        )))
    }

    fn destroy(&self) {
        // C++: 扫描所有 schema，对每张表调用 DataTable::Destroy()
        todo!("扫描 Catalog 中所有表，调用 data_table.Destroy()")
    }
}
