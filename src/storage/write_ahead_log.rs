//! Write-Ahead Log（WAL）写入端。
//!
//! 对应 C++:
//!   `duckdb/storage/write_ahead_log.hpp`
//!   `src/storage/write_ahead_log.cpp`
//!
//! # 结构层次
//!
//! ```text
//! WriteAheadLog                  — 公开 API（WriteCreateTable / WriteInsert / Flush …）
//!   └── WalSerializer            — 每次写操作的临时序列化会话
//!         └── ChecksumWriter     — 内存缓冲 + 校验和计算，Flush 时写入文件
//!               └── dyn WalWriter — 底层文件写入抽象（对应 BufferedFileWriter）
//! ```
//!
//! # WAL 条目格式（版本 2）
//!
//! ```text
//! ┌──────────────────────────────────────────┐
//! │  entry_size : u64  (小端)                 │
//! │  checksum   : u64  (小端，Adler32/CRC64)  │
//! │  payload    : [u8; entry_size]            │
//! │    ├─ wal_type : u8  (field_id=100)       │
//! │    └─ ...具体字段 (field_id=101…)         │
//! └──────────────────────────────────────────┘
//! ```
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `unique_ptr<BufferedFileWriter> writer` | `Option<Box<dyn WalWriter>>` |
//! | `mutex wal_lock` | `Mutex<WalInner>` |
//! | `atomic<WALInitState> init_state` | `AtomicU8` |
//! | `ChecksumWriter` (内部类) | `ChecksumWriter` |
//! | `WriteAheadLogSerializer` (内部类) | `WalSerializer` |
//! | `optional_idx checkpoint_iteration` | `Option<u64>` |

use parking_lot::Mutex;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU8, Ordering};

use super::metadata::MetaBlockPointer;
use super::storage_info::{BLOCK_HEADER_SIZE, DatabaseHeader, FileOpenFlags, FileSystem, MainHeader};
use super::storage_manager::StorageManager;

// ─── WAL 版本常量 ──────────────────────────────────────────────────────────────

/// 明文 WAL 版本（C++: `WAL_VERSION_NUMBER = 2`）。
pub const WAL_VERSION_NUMBER: u64 = 2;
/// 加密 WAL 版本（C++: `WAL_ENCRYPTED_VERSION_NUMBER = 3`）。
pub const WAL_ENCRYPTED_VERSION_NUMBER: u64 = 3;

// ─── WalType ───────────────────────────────────────────────────────────────────

/// WAL 条目类型（C++: `enum class WALType : uint8_t`）。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalType {
    Invalid = 0,
    // Catalog DDL
    CreateTable = 1,
    DropTable = 2,
    CreateSchema = 3,
    DropSchema = 4,
    CreateView = 5,
    DropView = 6,
    CreateSequence = 8,
    DropSequence = 9,
    SequenceValue = 10,
    CreateMacro = 11,
    DropMacro = 12,
    CreateType = 13,
    DropType = 14,
    AlterInfo = 20,
    CreateTableMacro = 21,
    DropTableMacro = 22,
    CreateIndex = 23,
    DropIndex = 24,
    // DML
    UseTable = 25,
    InsertTuple = 26,
    DeleteTuple = 27,
    UpdateTuple = 28,
    RowGroupData = 29,
    // 控制
    WalVersion = 98,
    Checkpoint = 99,
    WalFlush = 100,
}

impl TryFrom<u8> for WalType {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Invalid),
            1 => Ok(Self::CreateTable),
            2 => Ok(Self::DropTable),
            3 => Ok(Self::CreateSchema),
            4 => Ok(Self::DropSchema),
            5 => Ok(Self::CreateView),
            6 => Ok(Self::DropView),
            8 => Ok(Self::CreateSequence),
            9 => Ok(Self::DropSequence),
            10 => Ok(Self::SequenceValue),
            11 => Ok(Self::CreateMacro),
            12 => Ok(Self::DropMacro),
            13 => Ok(Self::CreateType),
            14 => Ok(Self::DropType),
            20 => Ok(Self::AlterInfo),
            21 => Ok(Self::CreateTableMacro),
            22 => Ok(Self::DropTableMacro),
            23 => Ok(Self::CreateIndex),
            24 => Ok(Self::DropIndex),
            25 => Ok(Self::UseTable),
            26 => Ok(Self::InsertTuple),
            27 => Ok(Self::DeleteTuple),
            28 => Ok(Self::UpdateTuple),
            29 => Ok(Self::RowGroupData),
            98 => Ok(Self::WalVersion),
            99 => Ok(Self::Checkpoint),
            100 => Ok(Self::WalFlush),
            other => Err(other),
        }
    }
}

// ─── WalInitState ─────────────────────────────────────────────────────────────

/// WAL 文件初始化阶段（C++: `enum class WALInitState`）。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalInitState {
    /// 无 WAL 文件（C++: `NO_WAL`）。
    NoWal = 0,
    /// 未初始化（C++: `UNINITIALIZED`）。
    Uninitialized = 1,
    /// 未初始化但需要截断（C++: `UNINITIALIZED_REQUIRES_TRUNCATE`）。
    UninitializedRequiresTruncate = 2,
    /// 已初始化，writer 可用（C++: `INITIALIZED`）。
    Initialized = 3,
}

// ─── WalReplayState ───────────────────────────────────────────────────────────

/// WAL 回放阶段（C++: `enum class WALReplayState`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReplayState {
    /// 正常 WAL 回放（C++: `MAIN_WAL`）。
    MainWal,
    /// Checkpoint 之后的 WAL 回放（C++: `CHECKPOINT_WAL`）。
    CheckpointWal,
}

// ─── WalWriter trait ──────────────────────────────────────────────────────────

/// WAL 底层文件写入抽象（C++: `BufferedFileWriter`）。
///
/// 对应 C++ 的 `BufferedFileWriter`，抽象为 trait 便于测试替换。
pub trait WalWriter: Send {
    /// 写入字节（C++: `WriteData(buffer, size)`）。
    fn write_data(&mut self, data: &[u8]) -> io::Result<()>;
    /// 刷新并 fsync（C++: `Sync()`）。
    fn sync(&mut self) -> io::Result<()>;
    /// 截断到指定大小（C++: `Truncate(size)`）。
    fn truncate(&mut self, size: u64) -> io::Result<()>;
    /// 当前文件大小（C++: `GetFileSize()`）。
    fn file_size(&self) -> u64;
    /// 自启动以来写入的总字节数（C++: `GetTotalWritten()`）。
    fn total_written(&self) -> u64;
}

// ─── ChecksumWriter ───────────────────────────────────────────────────────────

/// 带校验和的缓冲写入器（C++: `class ChecksumWriter : public WriteStream`，内部类）。
///
/// 将载荷先写入内存缓冲，`flush()` 时：
/// 1. 计算校验和（Adler32 / CRC64）。
/// 2. 写 `[entry_size(8B) | checksum(8B) | payload]`。
/// 3. 清空缓冲。
struct ChecksumWriter<'a> {
    /// 指向底层 WAL writer（懒初始化后赋值）。
    writer: &'a mut dyn WalWriter,
    /// 当前条目的内存缓冲（C++: `MemoryStream memory_stream`）。
    buffer: Vec<u8>,
}

impl<'a> ChecksumWriter<'a> {
    fn new(writer: &'a mut dyn WalWriter) -> Self {
        Self {
            writer,
            buffer: Vec::new(),
        }
    }

    /// 将缓冲内容连同长度+校验和写入底层 writer（C++: `ChecksumWriter::Flush()`）。
    fn flush(&mut self) -> io::Result<()> {
        let size = self.buffer.len() as u64;
        let checksum = compute_checksum(&self.buffer);
        self.writer.write_data(&size.to_le_bytes())?;
        self.writer.write_data(&checksum.to_le_bytes())?;
        self.writer.write_data(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }
}

impl Write for ChecksumWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(()) // 实际 flush 通过 self.flush() 主动调用
    }
}

// ─── WalSerializer ────────────────────────────────────────────────────────────

/// 单次 WAL 条目序列化会话（C++: `class WriteAheadLogSerializer`，内部类）。
///
/// 构造时写 `wal_type`，`end()` 时触发 `ChecksumWriter::flush()`。
///
/// # 字段 ID 约定（与 C++ BinarySerializer 对齐）
///
/// - field_id = 100 → "wal_type"
/// - field_id = 101 → 主体字段（表名、Chunk 等）
/// - field_id = 102, 103, … → 附加字段
struct WalSerializer<'a> {
    checksum_writer: ChecksumWriter<'a>,
}

impl<'a> WalSerializer<'a> {
    /// 开始一个新条目（C++: `WriteAheadLogSerializer(wal, wal_type)` 构造函数）。
    fn begin(writer: &'a mut dyn WalWriter, wal_type: WalType) -> io::Result<Self> {
        let mut cw = ChecksumWriter::new(writer);
        // 写 wal_type（field_id=100, tag="wal_type"）
        write_field_u8(&mut cw, 100, wal_type as u8)?;
        Ok(Self {
            checksum_writer: cw,
        })
    }

    /// 写 u64 字段（C++: `serializer.WriteProperty(field_id, tag, value)`）。
    fn write_u64(&mut self, field_id: u8, value: u64) -> io::Result<()> {
        write_field_u64(&mut self.checksum_writer, field_id, value)
    }

    /// 写字符串字段。
    fn write_str(&mut self, field_id: u8, value: &str) -> io::Result<()> {
        write_field_str(&mut self.checksum_writer, field_id, value)
    }

    /// 写原始字节字段。
    fn write_bytes(&mut self, field_id: u8, data: &[u8]) -> io::Result<()> {
        write_field_bytes(&mut self.checksum_writer, field_id, data)
    }

    /// 结束条目，触发校验和写入（C++: `serializer.End()`）。
    fn end(mut self) -> io::Result<()> {
        self.checksum_writer.flush()
    }
}

// ─── 辅助序列化函数 ────────────────────────────────────────────────────────────

/// 计算 WAL 条目校验和，与 C++ `Checksum(uint8_t*, size_t)` 完全兼容。
///
/// 算法来自 `src/common/checksum.cpp`：
/// - 初始值 5381
/// - 每 8 字节（小端）：`result ^= chunk * 0xbf58476d1ce4e5b9`
/// - 尾部 1-7 字节：`ChecksumRemainder`（MurmurHash2 风格混合）
fn compute_checksum(data: &[u8]) -> u64 {
    #[inline(always)]
    fn hash_u64(x: u64) -> u64 {
        x.wrapping_mul(0xbf58476d1ce4e5b9)
    }

    fn checksum_remainder(tail: &[u8]) -> u64 {
        const M: u64 = 0xc6a4a7935bd1e995;
        const R: u32 = 47;

        let n = tail.len();
        let mut h: u64 = 0xe17a1465_u64 ^ (n as u64).wrapping_mul(M);

        if n >= 7 {
            h ^= (tail[6] as u64) << 48;
        }
        if n >= 6 {
            h ^= (tail[5] as u64) << 40;
        }
        if n >= 5 {
            h ^= (tail[4] as u64) << 32;
        }
        if n >= 4 {
            h ^= (tail[3] as u64) << 24;
        }
        if n >= 3 {
            h ^= (tail[2] as u64) << 16;
        }
        if n >= 2 {
            h ^= (tail[1] as u64) << 8;
        }
        if n >= 1 {
            h ^= tail[0] as u64;
            h = h.wrapping_mul(M);
        }

        h ^= h >> R;
        h = h.wrapping_mul(M);
        h ^= h >> R;
        h
    }

    let mut result: u64 = 5381;
    let n_blocks = data.len() / 8;

    for i in 0..n_blocks {
        let val = u64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
        result ^= hash_u64(val);
    }

    let tail = &data[n_blocks * 8..];
    if !tail.is_empty() {
        result ^= checksum_remainder(tail);
    }

    result
}

fn write_field_u8(w: &mut impl Write, field_id: u8, value: u8) -> io::Result<()> {
    w.write_all(&[field_id, value])
}

fn write_field_u64(w: &mut impl Write, field_id: u8, value: u64) -> io::Result<()> {
    w.write_all(&[field_id])?;
    w.write_all(&value.to_le_bytes())
}

fn write_field_u32(w: &mut impl Write, field_id: u8, value: u32) -> io::Result<()> {
    w.write_all(&[field_id])?;
    w.write_all(&value.to_le_bytes())
}

fn write_field_str(w: &mut impl Write, field_id: u8, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();
    w.write_all(&[field_id])?;
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(bytes)
}

fn write_field_bytes(w: &mut impl Write, field_id: u8, data: &[u8]) -> io::Result<()> {
    w.write_all(&[field_id])?;
    w.write_all(&(data.len() as u32).to_le_bytes())?;
    w.write_all(data)
}

// ─── WAL 内部可变状态（Mutex 保护）────────────────────────────────────────────

// ─── WriteAheadLog ────────────────────────────────────────────────────────────

/// Write-Ahead Log 写入端（C++: `class WriteAheadLog`）。
///
/// 由 `StorageManager` 持有（`Arc<WriteAheadLog>`）。
/// 所有写方法均先通过 `ensure_initialized()` 懒初始化文件 writer，
/// 再调用内部 `WalSerializer` 序列化条目。
pub struct WriteAheadLog {
    storage_manager: &'static dyn StorageManager,

    /// WAL 文件路径（C++: `string wal_path`）。
    pub wal_path: String,

    /// 初始化状态（C++: `atomic<WALInitState> init_state`）。
    init_state: AtomicU8,

    /// checkpoint 迭代号（C++: `optional_idx checkpoint_iteration`）。
    pub checkpoint_iteration: Option<u64>,

    /// 底层文件写入器（C++: `unique_ptr<BufferedFileWriter> writer`）。
    writer: Mutex<Option<Box<dyn WalWriter>>>,
}

impl WriteAheadLog {
    // ── 构造 ──────────────────────────────────────────────────────────────────

    /// 构造（C++: `WriteAheadLog(StorageManager&, path, wal_size, init_state, checkpoint_iteration)`）。
    ///
    /// 与 C++ 构造函数完全对应：
    /// ```cpp
    /// WriteAheadLog::WriteAheadLog(StorageManager &storage_manager, ...) {
    ///     storage_manager.SetWALSize(wal_size);
    /// }
    /// ```
    pub fn new(
        storage_manager: &dyn StorageManager,
        wal_path: String,
        wal_size: u64,
        init_state: WalInitState,
        checkpoint_iteration: Option<u64>,
    ) -> Self {
        // C++: storage_manager.SetWALSize(wal_size)
        storage_manager.set_wal_size(wal_size);
        Self {
            storage_manager: unsafe {
                std::mem::transmute::<&dyn StorageManager, &'static dyn StorageManager>(
                    storage_manager,
                )
            },
            wal_path,
            init_state: AtomicU8::new(init_state as u8),
            checkpoint_iteration,
            writer: Mutex::new(None),
        }
    }

    // ── 状态查询 ──────────────────────────────────────────────────────────────

    /// WAL 是否已初始化（C++: `Initialized()`）。
    pub fn is_initialized(&self) -> bool {
        self.init_state.load(Ordering::Acquire) == WalInitState::Initialized as u8
    }

    /// WAL 文件路径（C++: `GetPath()`）。
    pub fn path(&self) -> &str {
        &self.wal_path
    }

    /// 自启动以来写入的总字节数（C++: `GetTotalWritten()`）。
    pub fn total_written(&self) -> u64 {
        let writer = self.writer.lock();
        writer.as_ref().map(|w| w.total_written()).unwrap_or(0)
    }

    /// 获取 WAL 文件大小（字节）。
    ///
    /// 返回 WAL 文件的实际大小，在 flush() 后会更新。
    /// 这个值用于 StorageManager 跟踪 WAL 大小以触发自动 checkpoint。
    pub fn file_size(&self) -> u64 {
        let writer = self.writer.lock();
        writer
            .as_ref()
            .map(|w| w.file_size())
            .unwrap_or_else(|| self.storage_manager().wal_size())
    }

    // ── 初始化 ────────────────────────────────────────────────────────────────

    /// 懒初始化 WAL 文件 writer（C++: `Initialize()`）。
    ///
    /// 若已初始化，直接返回（幂等）。
    /// 调用方提供 `writer_factory` 用于创建底层写入器，解耦文件系统依赖。
    pub fn initialize<F>(&self, writer_factory: F) -> io::Result<()>
    where
        F: FnOnce(&str, u64) -> io::Result<Box<dyn WalWriter>>,
    {
        if self.is_initialized() {
            return Ok(());
        }
        let mut writer_guard = self.writer.lock();
        if writer_guard.is_none() {
            let state = WalInitState::try_from(self.init_state.load(Ordering::Relaxed))
                .unwrap_or(WalInitState::Uninitialized);
            let truncate_to = if state == WalInitState::UninitializedRequiresTruncate {
                Some(self.storage_manager().wal_size())
            } else {
                None
            };
            let mut writer = writer_factory(&self.wal_path, self.storage_manager().wal_size())?;
            if let Some(size) = truncate_to {
                writer.truncate(size)?;
            } else {
                self.storage_manager().set_wal_size(writer.file_size());
            }
            *writer_guard = Some(writer);
            self.init_state
                .store(WalInitState::Initialized as u8, Ordering::Release);
        }
        Ok(())
    }

    // ── 头部写入 ──────────────────────────────────────────────────────────────

    /// 写 WAL 文件头（幂等，仅在文件为空时写一次）（C++: `WriteHeader()`）。
    ///
    /// 头部不附加校验和，直接序列化到文件。
    /// 内容：WAL_VERSION + 版本号 + DB 标识符 + checkpoint_iteration。
    pub fn write_header(&self, version: u64, db_identifier: &[u8], checkpoint_iteration: u64) {
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            if writer.file_size() > 0 {
                return;
            }
            // 直接写（无校验和）：wal_type=WAL_VERSION；用 Vec<u8> 中转再写入
            let mut buf = Vec::new();
            let _ = write_field_u8(&mut buf, 100, WalType::WalVersion as u8);
            let _ = write_field_u64(&mut buf, 101, version);
            let _ = write_field_bytes(&mut buf, 102, db_identifier);
            let _ = write_field_u64(&mut buf, 103, checkpoint_iteration);
            let _ = writer.write_data(&buf);
        }
    }

    // ── DDL 写入 ──────────────────────────────────────────────────────────────

    /// 写 CREATE TABLE（C++: `WriteCreateTable(TableCatalogEntry&)`）。
    ///
    /// `payload`：序列化后的 TableCatalogEntry 二进制数据（field_id=101）。
    pub fn write_create_table(&self, payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::CreateTable, |s| s.write_bytes(101, payload))
    }

    /// 写 DROP TABLE（C++: `WriteDropTable()`）。
    pub fn write_drop_table(&self, schema: &str, name: &str) -> io::Result<()> {
        self.write_entry(WalType::DropTable, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)
        })
    }

    /// 写 CREATE SCHEMA（C++: `WriteCreateSchema()`）。
    pub fn write_create_schema(&self, schema: &str) -> io::Result<()> {
        self.write_entry(WalType::CreateSchema, |s| s.write_str(101, schema))
    }

    /// 写 DROP SCHEMA（C++: `WriteDropSchema()`）。
    pub fn write_drop_schema(&self, schema: &str) -> io::Result<()> {
        self.write_entry(WalType::DropSchema, |s| s.write_str(101, schema))
    }

    /// 写 CREATE VIEW（C++: `WriteCreateView()`）。
    pub fn write_create_view(&self, payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::CreateView, |s| s.write_bytes(101, payload))
    }

    /// 写 DROP VIEW（C++: `WriteDropView()`）。
    pub fn write_drop_view(&self, schema: &str, name: &str) -> io::Result<()> {
        self.write_entry(WalType::DropView, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)
        })
    }

    /// 写 CREATE SEQUENCE（C++: `WriteCreateSequence()`）。
    pub fn write_create_sequence(&self, payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::CreateSequence, |s| s.write_bytes(101, payload))
    }

    /// 写 DROP SEQUENCE（C++: `WriteDropSequence()`）。
    pub fn write_drop_sequence(&self, schema: &str, name: &str) -> io::Result<()> {
        self.write_entry(WalType::DropSequence, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)
        })
    }

    /// 写序列当前值（C++: `WriteSequenceValue(SequenceValue)`）。
    pub fn write_sequence_value(
        &self,
        schema: &str,
        name: &str,
        usage_count: i64,
        counter: i64,
    ) -> io::Result<()> {
        self.write_entry(WalType::SequenceValue, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)?;
            s.write_u64(103, usage_count as u64)?;
            s.write_u64(104, counter as u64)
        })
    }

    /// 写 CREATE MACRO（C++: `WriteCreateMacro()`）。
    pub fn write_create_macro(&self, payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::CreateMacro, |s| s.write_bytes(101, payload))
    }

    /// 写 DROP MACRO（C++: `WriteDropMacro()`）。
    pub fn write_drop_macro(&self, schema: &str, name: &str) -> io::Result<()> {
        self.write_entry(WalType::DropMacro, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)
        })
    }

    /// 写 CREATE TABLE MACRO（C++: `WriteCreateTableMacro()`）。
    pub fn write_create_table_macro(&self, payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::CreateTableMacro, |s| s.write_bytes(101, payload))
    }

    /// 写 DROP TABLE MACRO（C++: `WriteDropTableMacro()`）。
    pub fn write_drop_table_macro(&self, schema: &str, name: &str) -> io::Result<()> {
        self.write_entry(WalType::DropTableMacro, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)
        })
    }

    /// 写 CREATE INDEX（C++: `WriteCreateIndex()`）。
    ///
    /// `catalog_payload`：IndexCatalogEntry 序列化（field_id=101）。
    /// `index_storage`：索引数据块列表（field_id=102/103）。
    pub fn write_create_index(
        &self,
        catalog_payload: &[u8],
        index_storage: &[u8],
    ) -> io::Result<()> {
        self.write_entry(WalType::CreateIndex, |s| {
            s.write_bytes(101, catalog_payload)?;
            s.write_bytes(102, index_storage)
        })
    }

    /// 写 DROP INDEX（C++: `WriteDropIndex()`）。
    pub fn write_drop_index(&self, schema: &str, name: &str) -> io::Result<()> {
        self.write_entry(WalType::DropIndex, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)
        })
    }

    /// 写 CREATE TYPE（C++: `WriteCreateType()`）。
    pub fn write_create_type(&self, payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::CreateType, |s| s.write_bytes(101, payload))
    }

    /// 写 DROP TYPE（C++: `WriteDropType()`）。
    pub fn write_drop_type(&self, schema: &str, name: &str) -> io::Result<()> {
        self.write_entry(WalType::DropType, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, name)
        })
    }

    /// 写 ALTER（C++: `WriteAlter(entry, info)`）。
    ///
    /// `alter_payload`：AlterInfo 序列化（field_id=101）；
    /// `index_storage`：若 ALTER 包含 ADD PRIMARY KEY，附带索引数据（可为空）。
    pub fn write_alter(
        &self,
        alter_payload: &[u8],
        index_storage: Option<&[u8]>,
    ) -> io::Result<()> {
        self.write_entry(WalType::AlterInfo, |s| {
            s.write_bytes(101, alter_payload)?;
            if let Some(idx) = index_storage {
                s.write_bytes(102, idx)?;
            }
            Ok(())
        })
    }

    // ── DML 写入 ──────────────────────────────────────────────────────────────

    /// 设置当前操作的表（C++: `WriteSetTable(schema, table)`）。
    pub fn write_set_table(&self, schema: &str, table: &str) -> io::Result<()> {
        self.write_entry(WalType::UseTable, |s| {
            s.write_str(101, schema)?;
            s.write_str(102, table)
        })
    }

    /// 写 INSERT（C++: `WriteInsert(DataChunk&)`）。
    ///
    /// `chunk_payload`：序列化后的 DataChunk（field_id=101）。
    pub fn write_insert(&self, chunk_payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::InsertTuple, |s| s.write_bytes(101, chunk_payload))
    }

    /// 写 DELETE（C++: `WriteDelete(DataChunk&)`）。
    ///
    /// Chunk 为单列 ROW_ID 向量。
    pub fn write_delete(&self, chunk_payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::DeleteTuple, |s| s.write_bytes(101, chunk_payload))
    }

    /// 写 UPDATE（C++: `WriteUpdate(DataChunk&, column_indexes)`）。
    ///
    /// `column_indexes_payload`：列路径（field_id=101）；
    /// `chunk_payload`：(更新值列, ROW_ID列)（field_id=102）。
    pub fn write_update(
        &self,
        column_indexes_payload: &[u8],
        chunk_payload: &[u8],
    ) -> io::Result<()> {
        self.write_entry(WalType::UpdateTuple, |s| {
            s.write_bytes(101, column_indexes_payload)?;
            s.write_bytes(102, chunk_payload)
        })
    }

    /// 写 RowGroup 数据（C++: `WriteRowGroupData(PersistentCollectionData&)`）。
    pub fn write_row_group_data(&self, data_payload: &[u8]) -> io::Result<()> {
        self.write_entry(WalType::RowGroupData, |s| s.write_bytes(101, data_payload))
    }

    // ── 控制操作 ──────────────────────────────────────────────────────────────

    /// 写 CHECKPOINT 记录（C++: `WriteCheckpoint(MetaBlockPointer)`）。
    pub fn write_checkpoint(&self, meta_block: MetaBlockPointer) -> io::Result<()> {
        self.write_entry(WalType::Checkpoint, |s| {
            // MetaBlockPointer 序列化：block_pointer(8B) + offset(4B)
            s.write_u64(101, meta_block.block_pointer)?;
            write_field_u32(&mut s.checksum_writer, 102, meta_block.offset)
        })
    }

    /// Flush：写空 WAL_FLUSH 条目并 fsync（C++: `Flush()`）。
    pub fn flush(&self) -> io::Result<()> {
        {
            // 写空 flush 标记
            let mut writer_guard = self.writer.lock();
            if let Some(writer) = writer_guard.as_mut() {
                let s = WalSerializer::begin(writer.as_mut(), WalType::WalFlush)?;
                s.end()?;
            }
        }
        // fsync
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            writer.sync()?;
            self.storage_manager().set_wal_size(writer.file_size());
        }
        Ok(())
    }

    /// 截断 WAL 到指定大小（C++: `Truncate(idx_t size)`）。
    pub fn truncate(&self, size: u64) -> io::Result<()> {
        let state = WalInitState::try_from(self.init_state.load(Ordering::Relaxed))
            .unwrap_or(WalInitState::Uninitialized);
        if state == WalInitState::NoWal {
            return Ok(());
        }
        let mut writer_guard = self.writer.lock();
        if let Some(writer) = writer_guard.as_mut() {
            writer.truncate(size)?;
            self.storage_manager().set_wal_size(writer.file_size());
        } else {
            self.init_state.store(
                WalInitState::UninitializedRequiresTruncate as u8,
                Ordering::Release,
            );
            self.storage_manager().set_wal_size(size);
        }
        Ok(())
    }

    // ── 内部辅助 ──────────────────────────────────────────────────────────────

    /// 通用写入辅助：加锁 → 获取 writer → 构造 WalSerializer → 调用 f → end()。
    fn write_entry<F>(&self, wal_type: WalType, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut WalSerializer<'_>) -> io::Result<()>,
    {
        let mut writer_guard = self.writer.lock();
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "WAL not initialized"))?;
        if writer.file_size() == 0 {
            let (db_identifier, checkpoint_iteration) = self.storage_manager_header_info()?;
            let version = if self.storage_manager_uses_encryption() {
                WAL_ENCRYPTED_VERSION_NUMBER
            } else {
                WAL_VERSION_NUMBER
            };
            drop(writer_guard);
            self.write_header(version, &db_identifier, checkpoint_iteration);
            writer_guard = self.writer.lock();
        }
        let writer = writer_guard
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "WAL not initialized"))?;
        let mut serializer = WalSerializer::begin(writer.as_mut(), wal_type)?;
        f(&mut serializer)?;
        serializer.end()
    }

    fn storage_manager(&self) -> &dyn StorageManager {
        self.storage_manager
    }

    fn storage_manager_header_info(&self) -> io::Result<([u8; MainHeader::DB_IDENTIFIER_LEN], u64)> {
        let path = self.storage_manager().db_path();
        let fs = crate::storage::standard_file_system::LocalFileSystem;
        let mut file = fs
            .open_file(path, FileOpenFlags::READ)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut main_header_buf = vec![0u8; super::storage_info::FILE_HEADER_SIZE];
        file.read_at(&mut main_header_buf, 0)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let main_header = MainHeader::deserialize(&main_header_buf[BLOCK_HEADER_SIZE..])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid DuckDB main header"))?;

        let mut header_buf = vec![0u8; super::storage_info::FILE_HEADER_SIZE];
        file.read_at(&mut header_buf, super::storage_info::FILE_HEADER_SIZE as u64)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let h0 = DatabaseHeader::deserialize(
            &header_buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + DatabaseHeader::SERIALIZED_SIZE],
        );
        file.read_at(&mut header_buf, (super::storage_info::FILE_HEADER_SIZE * 2) as u64)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let h1 = DatabaseHeader::deserialize(
            &header_buf[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + DatabaseHeader::SERIALIZED_SIZE],
        );
        let headers = [h0, h1];
        let checkpoint_iteration = headers[MainHeader::active_header_idx(&headers)].iteration;
        Ok((main_header.db_identifier, checkpoint_iteration))
    }

    fn storage_manager_uses_encryption(&self) -> bool {
        false
    }
}

// ─── WalInitState TryFrom ─────────────────────────────────────────────────────

impl TryFrom<u8> for WalInitState {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::NoWal),
            1 => Ok(Self::Uninitialized),
            2 => Ok(Self::UninitializedRequiresTruncate),
            3 => Ok(Self::Initialized),
            other => Err(other),
        }
    }
}
