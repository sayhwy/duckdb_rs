//! Write-Ahead Log（WAL）读取与回放端。
//!
//! 对应 C++:
//!   `src/storage/wal_replay.cpp`
//!   （写入端见 `write_ahead_log.rs`）
//!
//! # 整体架构
//!
//! ```text
//! WalReplayer<R, C>                   — 驱动完整回放流程（对应 WriteAheadLog::ReplayInternal）
//!   ├── WalFrameReader<R>             — 按版本读取/校验二进制帧（对应 GetEntryDeserializer 静态工厂）
//!   │     ├── v1: 直接读流，无校验和
//!   │     ├── v2: size(8B)+checksum(8B)+payload
//!   │     └── v3: size(8B)+nonce(12B)+ciphertext+tag(16B)，AES-GCM 解密
//!   ├── WalEntryDecoder               — 字节流 → WalEntry 枚举（对应各 ReplayXxx 方法）
//!   ├── ReplayState                   — 回放期间的可变上下文（对应 class ReplayState）
//!   └── dyn CatalogOps               — Catalog/执行层抽象（对应 Catalog+ClientContext 操作）
//! ```
//!
//! # 两阶段回放流程（对应 ReplayInternal）
//!
//! ```text
//! 阶段1 (deserialize_only=true) ─── 扫描 CHECKPOINT 标记，不修改任何状态
//! 阶段2 (deserialize_only=false) ── 从头重放，实际写入 Catalog
//! ```
//!
//! # WAL 帧格式对照
//!
//! | 版本 | 帧结构 |
//! |------|--------|
//! | v1   | `[payload]` （无帧头） |
//! | v2   | `size:u64 \| checksum:u64 \| payload:[u8;size]` |
//! | v3   | `size:u64 \| nonce:[u8;12] \| ciphertext:[u8;size+8] \| tag:[u8;16]` |
//!
//! # C++ → Rust 核心映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class ReplayState` | `ReplayState` |
//! | `ReplayState::ReplayIndexInfo` | `ReplayIndexInfo` |
//! | `class WriteAheadLogDeserializer` | `WalFrameReader<R>` + `WalEntryDecoder` |
//! | `GetEntryDeserializer(state, reader, only)` | `WalFrameReader::read_frame()` |
//! | `ReplayEntry() → bool` | `WalEntryDecoder::decode() → WalEntry` |
//! | `WriteAheadLog::ReplayInternal(...)` | `WalReplayer::replay()` |
//! | C++ 异常 (SerializationException) | `Err(WalError::Corrupt(...))` |

use std::io::{self, Read, Seek, SeekFrom};

use crate::common::errors::{ErrorCode, HasErrorCode};

use super::metadata::MetaBlockPointer;
use super::write_ahead_log::WalType;

// ─── 常量 ─────────────────────────────────────────────────────────────────────

/// AES-GCM nonce 长度（C++: `EncryptionNonce`，12 字节）。
pub const NONCE_SIZE: usize = 12;
/// AES-GCM 认证标签长度（C++: `EncryptionTag`，16 字节）。
pub const TAG_SIZE: usize = 16;
/// DB 标识符长度（C++: `MainHeader::DB_IDENTIFIER_LEN`）。
pub const DB_IDENTIFIER_LEN: usize = 16;

// ─── WalError ─────────────────────────────────────────────────────────────────

/// WAL 回放过程中的错误类型（对应 C++ 抛出的各类异常）。
#[derive(Debug)]
pub enum WalError {
    /// I/O 错误（文件读取失败等）。
    Io(io::Error),
    /// 损坏的 WAL：校验和不匹配、帧大小超出文件等（C++: `IOException` / `SerializationException`）。
    Corrupt(String),
    /// WAL 版本不匹配或 checkpoint iteration 不一致（C++: `IOException` 版本校验）。
    VersionMismatch(String),
    /// checkpoint WAL 中出现了 CHECKPOINT 标记（不允许嵌套 checkpoint）。
    InvalidCheckpointWal(String),
    /// 回放数据条目时当前表未设置（C++: `InternalException("Corrupt WAL: ...")`）。
    NoCurrentTable,
    /// 列索引越界（UPDATE 条目校验）。
    ColumnIndexOutOfBounds,
    /// 行 ID 超出表范围（DELETE 条目校验）。
    RowIdOutOfBounds,
    /// 不支持的 WAL 版本号。
    UnsupportedVersion(u64),
    /// 无法解析的 WalType 字节。
    UnknownWalType(u8),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "WAL I/O error: {e}"),
            Self::Corrupt(msg) => write!(f, "Corrupt WAL: {msg}"),
            Self::VersionMismatch(msg) => write!(f, "WAL version mismatch: {msg}"),
            Self::InvalidCheckpointWal(msg) => write!(f, "Invalid checkpoint WAL: {msg}"),
            Self::NoCurrentTable => write!(f, "WAL entry references table before USE_TABLE"),
            Self::ColumnIndexOutOfBounds => write!(f, "WAL UPDATE column index out of bounds"),
            Self::RowIdOutOfBounds => write!(f, "WAL DELETE row id out of bounds"),
            Self::UnsupportedVersion(v) => write!(f, "Unsupported WAL version: {v}"),
            Self::UnknownWalType(t) => write!(f, "Unknown WAL entry type: {t}"),
        }
    }
}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        // EOF 映射为 Corrupt（截断 WAL），与 C++ 的 SerializationException 语义一致
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Self::Corrupt(format!("unexpected EOF: {e}"))
        } else {
            Self::Io(e)
        }
    }
}

impl std::error::Error for WalError {}

impl HasErrorCode for WalError {
    fn error_code(&self) -> ErrorCode {
        match self {
            WalError::NoCurrentTable
            | WalError::ColumnIndexOutOfBounds
            | WalError::RowIdOutOfBounds
            | WalError::Io(_)
            | WalError::Corrupt(_)
            | WalError::VersionMismatch(_)
            | WalError::InvalidCheckpointWal(_)
            | WalError::UnsupportedVersion(_)
            | WalError::UnknownWalType(_) => ErrorCode::Wal,
        }
    }
}

pub use crate::common::errors::WalResult;

// ─── WalVersion ───────────────────────────────────────────────────────────────

/// 当前已知的 WAL 版本（C++: `wal_version` 字段，由 `ReplayVersion` 读入）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalVersion {
    /// 旧格式，无帧校验和（C++: version == 1）。
    V1,
    /// 带 size+checksum 帧头的明文格式（C++: version == 2）。
    V2,
    /// AES-GCM 加密格式（C++: version == 3）。
    V3Encrypted,
}

impl TryFrom<u64> for WalVersion {
    type Error = WalError;
    fn try_from(v: u64) -> WalResult<Self> {
        match v {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3Encrypted),
            other => Err(WalError::UnsupportedVersion(other)),
        }
    }
}

// ─── WalEntry ─────────────────────────────────────────────────────────────────

/// 一条已解码的 WAL 条目（对应 C++ 各 `Replay*` 方法读取的数据）。
///
/// 字段均以原始字节携带，交由上层 `CatalogOps` 进一步解析。
/// 这样可以将"读 WAL 格式"与"应用到 Catalog"两个阶段完全解耦。
#[derive(Debug)]
pub enum WalEntry {
    // ── 版本头 ──────────────────────────────────────────────────────────────
    /// WAL 文件头（C++: `ReplayVersion`）。
    ///
    /// - `version`: WAL 格式版本号（1/2/3）
    /// - `db_identifier`: 数据库文件唯一标识（`MainHeader::DB_IDENTIFIER_LEN` 字节），v1 无此字段
    /// - `checkpoint_iteration`: 对应数据库文件的 checkpoint 轮次，v1 无此字段
    Version {
        version: u64,
        db_identifier: Option<[u8; DB_IDENTIFIER_LEN]>,
        checkpoint_iteration: Option<u64>,
    },

    // ── DDL：Table ─────────────────────────────────────────────────────────
    /// CREATE TABLE（C++: `ReplayCreateTable`，field_id=101 = CreateInfo 序列化）。
    CreateTable { payload: Vec<u8> },
    /// DROP TABLE（C++: `ReplayDropTable`，field_id=101 schema, 102 name）。
    DropTable { schema: String, name: String },
    /// ALTER TABLE（C++: `ReplayAlter`，field_id=101 AlterInfo，102 IndexStorageInfo，103 index_storage）。
    ///
    /// `index_storage` 仅在 ALTER 包含 ADD PRIMARY KEY 时存在。
    AlterInfo {
        payload: Vec<u8>,
        index_storage_info: Option<Vec<u8>>,
        index_storage_data: Option<Vec<u8>>,
    },

    // ── DDL：Schema ────────────────────────────────────────────────────────
    /// CREATE SCHEMA（C++: `ReplayCreateSchema`，field_id=101 schema name）。
    CreateSchema { name: String },
    /// DROP SCHEMA（C++: `ReplayDropSchema`，field_id=101 schema name）。
    DropSchema { name: String },

    // ── DDL：View ──────────────────────────────────────────────────────────
    /// CREATE VIEW（C++: `ReplayCreateView`，field_id=101 = CreateInfo 序列化）。
    CreateView { payload: Vec<u8> },
    /// DROP VIEW（C++: `ReplayDropView`，field_id=101 schema, 102 name）。
    DropView { schema: String, name: String },

    // ── DDL：Sequence ──────────────────────────────────────────────────────
    /// CREATE SEQUENCE（C++: `ReplayCreateSequence`，field_id=101）。
    CreateSequence { payload: Vec<u8> },
    /// DROP SEQUENCE（C++: `ReplayDropSequence`，field_id=101 schema, 102 name）。
    DropSequence { schema: String, name: String },
    /// 序列当前值更新（C++: `ReplaySequenceValue`，field_id=101..104）。
    SequenceValue {
        schema: String,
        name: String,
        usage_count: u64,
        counter: i64,
    },

    // ── DDL：Macro ─────────────────────────────────────────────────────────
    /// CREATE MACRO（C++: `ReplayCreateMacro`，field_id=101）。
    CreateMacro { payload: Vec<u8> },
    /// DROP MACRO（C++: `ReplayDropMacro`，field_id=101 schema, 102 name）。
    DropMacro { schema: String, name: String },
    /// CREATE TABLE MACRO（C++: `ReplayCreateTableMacro`，field_id=101）。
    CreateTableMacro { payload: Vec<u8> },
    /// DROP TABLE MACRO（C++: `ReplayDropTableMacro`，field_id=101 schema, 102 name）。
    DropTableMacro { schema: String, name: String },

    // ── DDL：Index ─────────────────────────────────────────────────────────
    /// CREATE INDEX（C++: `ReplayCreateIndex`，field_id=101 catalog_entry, 102 storage_info, 103 data）。
    CreateIndex {
        catalog_payload: Vec<u8>,
        storage_info: Vec<u8>,
        storage_data: Vec<u8>,
    },
    /// DROP INDEX（C++: `ReplayDropIndex`，field_id=101 schema, 102 name）。
    DropIndex { schema: String, name: String },

    // ── DDL：Type ──────────────────────────────────────────────────────────
    /// CREATE TYPE（C++: `ReplayCreateType`，field_id=101）。
    CreateType { payload: Vec<u8> },
    /// DROP TYPE（C++: `ReplayDropType`，field_id=101 schema, 102 name）。
    DropType { schema: String, name: String },

    // ── DML ────────────────────────────────────────────────────────────────
    /// 设置当前操作目标表（C++: `ReplayUseTable`，field_id=101 schema, 102 table）。
    UseTable { schema: String, table: String },
    /// INSERT 数据（C++: `ReplayInsert`，field_id=101 = DataChunk 序列化）。
    Insert { chunk_payload: Vec<u8> },
    /// RowGroup 批量写入（C++: `ReplayRowGroupData`，field_id=101 = PersistentCollectionData）。
    RowGroupData { data_payload: Vec<u8> },
    /// DELETE 行（C++: `ReplayDelete`，field_id=101 = ROW_ID 列 DataChunk）。
    Delete { chunk_payload: Vec<u8> },
    /// UPDATE 行（C++: `ReplayUpdate`，field_id=101 column_indexes, 102 chunk）。
    Update {
        column_indexes_payload: Vec<u8>,
        chunk_payload: Vec<u8>,
    },

    // ── 控制 ───────────────────────────────────────────────────────────────
    /// CHECKPOINT 标记（C++: `ReplayCheckpoint`，field_id=101 MetaBlockPointer）。
    Checkpoint { meta_block: MetaBlockPointer },
    /// WAL_FLUSH：事务提交点（C++: `WAL_FLUSH` 类型，无附加字段）。
    WalFlush,
}

// ─── ReplayIndexInfo ──────────────────────────────────────────────────────────

/// 回放期间待加入 TableIndexList 的索引信息（C++: `ReplayState::ReplayIndexInfo`）。
///
/// C++ 在每个 `WalFlush` 后才将索引批量注册进表，Rust 侧同样延迟到 `WalFlush` 处理。
pub struct ReplayIndexInfo {
    /// 目标表所在 schema。
    pub table_schema: String,
    /// 目标表名。
    pub table_name: String,
    /// 序列化的索引数据（交由上层解析并注册）。
    pub index_payload: Vec<u8>,
    /// 序列化的索引存储信息（交由上层解析并注册）。
    pub storage_info: Vec<u8>,
}

// ─── ReplayState ──────────────────────────────────────────────────────────────

/// 回放过程中的可变上下文（C++: `class ReplayState`）。
///
/// `WalReplayer` 和 `WalFrameReader` 共同维护此状态。
pub struct ReplayState {
    /// 当前 WAL 格式版本（C++: `wal_version`，初始 1，由 `Version` 条目更新）。
    pub wal_version: WalVersion,

    /// 当前 DML 操作的目标表（schema, name）（C++: `optional_ptr<TableCatalogEntry> current_table`）。
    ///
    /// 对应 `UseTable` 条目写入，`Insert`/`Delete`/`Update` 消费。
    pub current_table: Option<(String, String)>,

    /// WAL 内发现的 CHECKPOINT 标记（C++: `MetaBlockPointer checkpoint_id`）。
    pub checkpoint_id: Option<MetaBlockPointer>,

    /// CHECKPOINT 条目在文件中的字节偏移（C++: `optional_idx checkpoint_position`）。
    ///
    /// 用于 WAL 合并时截取主 WAL 中 checkpoint 之前的部分。
    pub checkpoint_position: Option<u64>,

    /// 读取每条 WAL 帧前的文件偏移（C++: `optional_idx current_position`）。
    pub current_position: Option<u64>,

    /// 期望的 checkpoint iteration（C++: `optional_idx expected_checkpoint_id`）。
    ///
    /// 当 WAL checkpoint_iteration + 1 == db checkpoint_iteration 时设置，
    /// 表示 checkpoint 已写入数据库但 WAL 未被截断，属于预期情况。
    pub expected_checkpoint_id: Option<u64>,

    /// 待提交的索引列表（C++: `vector<ReplayIndexInfo> replay_index_infos`）。
    ///
    /// 在 `WalFlush` 时批量交给上层注册，之后清空。
    pub pending_indexes: Vec<ReplayIndexInfo>,
}

impl ReplayState {
    /// 创建初始状态（C++: `ReplayState(db, context)` 构造）。
    pub fn new() -> Self {
        Self {
            wal_version: WalVersion::V1, // 初始版本，由 Version 条目覆写
            current_table: None,
            checkpoint_id: None,
            checkpoint_position: None,
            current_position: None,
            expected_checkpoint_id: None,
            pending_indexes: Vec::new(),
        }
    }

    /// 重置为全新状态（第二阶段重放时使用）。
    pub fn reset(&mut self) {
        *self = Self::new();
    }
}

impl Default for ReplayState {
    fn default() -> Self {
        Self::new()
    }
}

// ─── WalReader trait ──────────────────────────────────────────────────────────

/// WAL 文件读取抽象（对应 C++: `BufferedFileReader`）。
///
/// 要求 `Read + Seek`，便于两阶段回放时 `Reset()` 到文件头。
pub trait WalReader: Read + Seek {}

impl<T: Read + Seek> WalReader for T {}

// ─── WalFrameReader ───────────────────────────────────────────────────────────

/// 按 WAL 版本读取并校验单帧数据（C++: `GetEntryDeserializer` 静态工厂）。
///
/// 每次调用 `read_frame()` 消耗底层流中的一帧，返回 `Vec<u8>` payload。
/// 由 `WalReplayer` 驱动，不直接持有 `ReplayState`。
pub struct WalFrameReader<R: WalReader> {
    reader: R,
    header_pending: bool,
    /// 当前使用的帧格式（C++: `state.wal_version`）。
    version: WalVersion,
    /// 文件总字节数（用于帧大小边界检查）。
    file_size: u64,
}

impl<R: WalReader> WalFrameReader<R> {
    /// 从任意 `WalReader` 构造（C++: 由 `ReplayInternal` 创建 `BufferedFileReader`）。
    pub fn new(mut reader: R) -> io::Result<Self> {
        let file_size = {
            let end = reader.seek(SeekFrom::End(0))?;
            reader.seek(SeekFrom::Start(0))?;
            end
        };
        Ok(Self {
            reader,
            header_pending: true,
            version: WalVersion::V1,
            file_size,
        })
    }

    /// 将读取器重置到文件头（C++: `reader.Reset()`）。
    pub fn reset(&mut self) -> io::Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        self.header_pending = true;
        self.version = WalVersion::V1;
        Ok(())
    }

    /// 当前字节偏移（C++: `reader.CurrentOffset()`）。
    pub fn current_offset(&mut self) -> io::Result<u64> {
        self.reader.stream_position()
    }

    /// 文件是否已读完（C++: `reader.Finished()`）。
    pub fn is_finished(&mut self) -> io::Result<bool> {
        let pos = self.reader.stream_position()?;
        Ok(pos >= self.file_size)
    }

    /// 更新帧读取版本（由 `WalReplayer` 在解析到 `Version` 条目后调用）。
    pub fn set_version(&mut self, version: WalVersion) {
        self.version = version;
    }

    /// 读取一帧并返回 payload 字节（C++: `GetEntryDeserializer(state, reader, only)` 逻辑）。
    ///
    /// # 帧格式
    /// - v1: 直接返回流，由调用方用 `BinaryDeserializer` 读取（此处返回空 Vec，由外层处理）
    /// - v2: `size:u64 | checksum:u64 | payload:[u8;size]`
    /// - v3: `size:u64 | nonce:[u8;12] | ciphertext:[u8;size+8] | tag:[u8;16]`（需外部解密）
    ///
    /// 返回 `(offset_before_read, payload)`，offset 用于填充 `current_position`。
    pub fn read_frame(&mut self) -> WalResult<(u64, WalFrame)> {
        let offset = self.reader.stream_position()?;

        if self.header_pending {
            self.header_pending = false;
            return Ok((
                offset,
                WalFrame::Header {
                    payload: self.read_header_payload()?,
                },
            ));
        }

        let frame = match self.version {
            WalVersion::V1 => {
                return Err(WalError::UnsupportedVersion(1));
            }
            WalVersion::V2 => {
                let size = self.read_u64()?;
                let stored_checksum = self.read_u64()?;
                let cur = self.reader.stream_position()?;

                if cur + size > self.file_size {
                    return Err(WalError::Corrupt(format!(
                        "entry size {size} exceeded remaining file at offset {cur} (file_size={})",
                        self.file_size
                    )));
                }

                let mut buf = vec![0u8; size as usize];
                self.reader.read_exact(&mut buf)?;

                let computed = compute_checksum(&buf);
                if stored_checksum != computed {
                    return Err(WalError::Corrupt(format!(
                        "checksum mismatch at offset {cur}: stored={stored_checksum:#x} computed={computed:#x}"
                    )));
                }

                WalFrame::V2 { payload: buf }
            }
            WalVersion::V3Encrypted => {
                // v3: size(8) + nonce(12) + ciphertext(size+8) + tag(16)
                // 加密数据交由上层 EncryptionOps 解密，此处仅读取原始字节
                let size = self.read_u64()?;
                let ciphertext_size = size + 8; // 含内嵌 checksum
                let cur = self.reader.stream_position()?;

                if cur + NONCE_SIZE as u64 + ciphertext_size + TAG_SIZE as u64 > self.file_size {
                    return Err(WalError::Corrupt(format!(
                        "encrypted entry size {size} exceeded remaining file at offset {cur}"
                    )));
                }

                let mut nonce = [0u8; NONCE_SIZE];
                self.reader.read_exact(&mut nonce)?;

                let mut ciphertext = vec![0u8; ciphertext_size as usize];
                self.reader.read_exact(&mut ciphertext)?;

                let mut tag = [0u8; TAG_SIZE];
                self.reader.read_exact(&mut tag)?;

                WalFrame::V3Encrypted {
                    plaintext_size: size,
                    nonce,
                    ciphertext,
                    tag,
                }
            }
        };

        Ok((offset, frame))
    }

    fn read_header_payload(&mut self) -> WalResult<Vec<u8>> {
        let mut payload = Vec::with_capacity(41);

        let mut byte = [0u8; 1];
        self.reader.read_exact(&mut byte)?;
        payload.extend_from_slice(&byte);
        self.reader.read_exact(&mut byte)?;
        payload.extend_from_slice(&byte);

        self.reader.read_exact(&mut byte)?;
        payload.extend_from_slice(&byte);
        let mut version = [0u8; 8];
        self.reader.read_exact(&mut version)?;
        payload.extend_from_slice(&version);

        self.reader.read_exact(&mut byte)?;
        payload.extend_from_slice(&byte);
        let mut len = [0u8; 4];
        self.reader.read_exact(&mut len)?;
        payload.extend_from_slice(&len);
        let db_id_len = u32::from_le_bytes(len) as usize;
        let mut db_id = vec![0u8; db_id_len];
        self.reader.read_exact(&mut db_id)?;
        payload.extend_from_slice(&db_id);

        self.reader.read_exact(&mut byte)?;
        payload.extend_from_slice(&byte);
        let mut checkpoint_iteration = [0u8; 8];
        self.reader.read_exact(&mut checkpoint_iteration)?;
        payload.extend_from_slice(&checkpoint_iteration);

        Ok(payload)
    }

    fn read_u64(&mut self) -> WalResult<u64> {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

// ─── WalFrame ─────────────────────────────────────────────────────────────────

/// 一帧已读取的 WAL 数据（`WalFrameReader::read_frame()` 的返回值）。
///
/// 在 v3 加密模式下，payload 须先经过 `EncryptionOps::decrypt()` 还原明文。
pub enum WalFrame {
    /// WAL 头部版本条目（未带 frame 头）。
    Header { payload: Vec<u8> },
    /// v2 模式：已验证校验和的明文 payload。
    V2 { payload: Vec<u8> },
    /// v3 模式：加密帧，需外部解密后得到明文 payload。
    V3Encrypted {
        /// 明文字节数（不含内嵌 checksum）。
        plaintext_size: u64,
        nonce: [u8; NONCE_SIZE],
        ciphertext: Vec<u8>,
        tag: [u8; TAG_SIZE],
    },
}

impl WalFrame {
    /// 尝试取出明文 payload（v1/v2 可直接取；v3 加密帧需先解密，此处返回 None）。
    pub fn into_plaintext(self) -> Option<Vec<u8>> {
        match self {
            Self::Header { payload } => Some(payload),
            Self::V2 { payload } => Some(payload),
            _ => None,
        }
    }
}

// ─── WalEntryDecoder ──────────────────────────────────────────────────────────

/// 将 payload 字节流解码为 `WalEntry`（对应 C++ 各 `Replay*` 的读取部分）。
///
/// 字段读取遵循 BinarySerializer/BinaryDeserializer 的字段 ID 编码约定：
/// `[field_id: u8 | length: u32 (for str/bytes) | value: ...]`
///
/// # 设计说明
///
/// C++ `WriteAheadLogDeserializer` 把"读字段"和"应用到 Catalog"混在一起。
/// Rust 侧将这两步拆开：
/// - `WalEntryDecoder` 只做字段解析，返回 `WalEntry`（可测试、可序列化）
/// - `WalReplayer` 持有 `dyn CatalogOps`，负责将 `WalEntry` 应用到系统
pub struct WalEntryDecoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WalEntryDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// 解码一条 WAL 条目（C++: `ReplayEntry(wal_type)` 的字段读取部分）。
    ///
    /// 返回 `WalEntry::WalFlush` 表示遇到 flush 标记（C++: `return true`）。
    pub fn decode(&mut self) -> WalResult<WalEntry> {
        // field_id=100 → wal_type : u8
        let wal_type_byte = self.read_field_u8(100)?;
        let wal_type = WalType::try_from(wal_type_byte).map_err(|b| WalError::UnknownWalType(b))?;

        match wal_type {
            WalType::WalFlush => Ok(WalEntry::WalFlush),

            WalType::WalVersion => self.decode_version(),

            WalType::CreateTable => Ok(WalEntry::CreateTable {
                payload: self.read_field_bytes(101)?,
            }),
            WalType::DropTable => Ok(WalEntry::DropTable {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
            }),
            WalType::AlterInfo => self.decode_alter(),

            WalType::CreateSchema => Ok(WalEntry::CreateSchema {
                name: self.read_field_str(101)?,
            }),
            WalType::DropSchema => Ok(WalEntry::DropSchema {
                name: self.read_field_str(101)?,
            }),

            WalType::CreateView => Ok(WalEntry::CreateView {
                payload: self.read_field_bytes(101)?,
            }),
            WalType::DropView => Ok(WalEntry::DropView {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
            }),

            WalType::CreateSequence => Ok(WalEntry::CreateSequence {
                payload: self.read_field_bytes(101)?,
            }),
            WalType::DropSequence => Ok(WalEntry::DropSequence {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
            }),
            WalType::SequenceValue => Ok(WalEntry::SequenceValue {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
                usage_count: self.read_field_u64(103)?,
                counter: self.read_field_i64(104)?,
            }),

            WalType::CreateMacro => Ok(WalEntry::CreateMacro {
                payload: self.read_field_bytes(101)?,
            }),
            WalType::DropMacro => Ok(WalEntry::DropMacro {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
            }),
            WalType::CreateTableMacro => Ok(WalEntry::CreateTableMacro {
                payload: self.read_field_bytes(101)?,
            }),
            WalType::DropTableMacro => Ok(WalEntry::DropTableMacro {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
            }),

            WalType::CreateIndex => self.decode_create_index(),
            WalType::DropIndex => Ok(WalEntry::DropIndex {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
            }),

            WalType::CreateType => Ok(WalEntry::CreateType {
                payload: self.read_field_bytes(101)?,
            }),
            WalType::DropType => Ok(WalEntry::DropType {
                schema: self.read_field_str(101)?,
                name: self.read_field_str(102)?,
            }),

            WalType::UseTable => Ok(WalEntry::UseTable {
                schema: self.read_field_str(101)?,
                table: self.read_field_str(102)?,
            }),
            WalType::InsertTuple => Ok(WalEntry::Insert {
                chunk_payload: self.read_field_bytes(101)?,
            }),
            WalType::RowGroupData => Ok(WalEntry::RowGroupData {
                data_payload: self.read_field_bytes(101)?,
            }),
            WalType::DeleteTuple => Ok(WalEntry::Delete {
                chunk_payload: self.read_field_bytes(101)?,
            }),
            WalType::UpdateTuple => Ok(WalEntry::Update {
                column_indexes_payload: self.read_field_bytes(101)?,
                chunk_payload: self.read_field_bytes(102)?,
            }),

            WalType::Checkpoint => {
                // MetaBlockPointer: block_pointer(8B) + offset(4B)
                let block_pointer = self.read_field_u64(101)?;
                let offset = self.read_field_u64(102)? as u32;
                Ok(WalEntry::Checkpoint {
                    meta_block: MetaBlockPointer {
                        block_pointer,
                        offset,
                    },
                })
            }

            WalType::Invalid => Err(WalError::Corrupt("WAL_INVALID entry type".into())),
        }
    }

    // ── 版本头解码 ────────────────────────────────────────────────────────────

    fn decode_version(&mut self) -> WalResult<WalEntry> {
        let version = self.read_field_u64(101)?;
        // field_id=102 db_identifier 是可选的（老版本 WAL 没有）
        let db_identifier = self.try_read_field_bytes_fixed::<DB_IDENTIFIER_LEN>(102)?;
        // field_id=103 checkpoint_iteration 也是可选的
        let checkpoint_iteration = if db_identifier.is_some() {
            self.try_read_field_u64(103)?
        } else {
            None
        };
        Ok(WalEntry::Version {
            version,
            db_identifier,
            checkpoint_iteration,
        })
    }

    // ── ALTER 解码 ─────────────────────────────────────────────────────────────

    fn decode_alter(&mut self) -> WalResult<WalEntry> {
        let payload = self.read_field_bytes(101)?;
        // field_id=102 index_storage_info 仅在 ADD PRIMARY KEY 时存在
        let index_storage_info = self.try_read_field_bytes(102)?;
        let index_storage_data = if index_storage_info.is_some() {
            self.try_read_field_bytes(103)?
        } else {
            None
        };
        Ok(WalEntry::AlterInfo {
            payload,
            index_storage_info,
            index_storage_data,
        })
    }

    // ── CREATE INDEX 解码 ──────────────────────────────────────────────────────

    fn decode_create_index(&mut self) -> WalResult<WalEntry> {
        let catalog_payload = self.read_field_bytes(101)?;
        let storage_info = self.read_field_bytes(102)?;
        let storage_data = self.read_field_bytes(103)?;
        Ok(WalEntry::CreateIndex {
            catalog_payload,
            storage_info,
            storage_data,
        })
    }

    // ── 基础读取辅助 ───────────────────────────────────────────────────────────

    fn peek_field_id(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    fn read_field_id(&mut self) -> WalResult<u8> {
        if self.pos >= self.buf.len() {
            return Err(WalError::Corrupt("unexpected end of entry payload".into()));
        }
        let id = self.buf[self.pos];
        self.pos += 1;
        Ok(id)
    }

    fn read_field_u8(&mut self, expected_id: u8) -> WalResult<u8> {
        let id = self.read_field_id()?;
        if id != expected_id {
            return Err(WalError::Corrupt(format!(
                "expected field_id={expected_id}, got {id}"
            )));
        }
        if self.pos >= self.buf.len() {
            return Err(WalError::Corrupt("truncated u8 field".into()));
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_field_u64(&mut self, expected_id: u8) -> WalResult<u64> {
        let id = self.read_field_id()?;
        if id != expected_id {
            return Err(WalError::Corrupt(format!(
                "expected field_id={expected_id}, got {id}"
            )));
        }
        self.read_u64_raw()
    }

    fn try_read_field_u64(&mut self, expected_id: u8) -> WalResult<Option<u64>> {
        if self.peek_field_id() != Some(expected_id) {
            return Ok(None);
        }
        Ok(Some(self.read_field_u64(expected_id)?))
    }

    fn read_field_i64(&mut self, expected_id: u8) -> WalResult<i64> {
        Ok(self.read_field_u64(expected_id)? as i64)
    }

    fn read_field_u32(&mut self, expected_id: u8) -> WalResult<u32> {
        let id = self.read_field_id()?;
        if id != expected_id {
            return Err(WalError::Corrupt(format!(
                "expected field_id={expected_id}, got {id}"
            )));
        }
        if self.pos + 4 > self.buf.len() {
            return Err(WalError::Corrupt("truncated u32 field".into()));
        }
        let v = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_field_str(&mut self, expected_id: u8) -> WalResult<String> {
        let bytes = self.read_field_bytes(expected_id)?;
        String::from_utf8(bytes)
            .map_err(|e| WalError::Corrupt(format!("invalid UTF-8 in string field: {e}")))
    }

    fn read_field_bytes(&mut self, expected_id: u8) -> WalResult<Vec<u8>> {
        let id = self.read_field_id()?;
        if id != expected_id {
            return Err(WalError::Corrupt(format!(
                "expected field_id={expected_id}, got {id}"
            )));
        }
        let len = {
            if self.pos + 4 > self.buf.len() {
                return Err(WalError::Corrupt("truncated bytes length".into()));
            }
            let v =
                u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
            self.pos += 4;
            v
        };
        if self.pos + len > self.buf.len() {
            return Err(WalError::Corrupt(format!(
                "bytes field len={len} exceeds payload"
            )));
        }
        let data = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(data)
    }

    fn try_read_field_bytes(&mut self, expected_id: u8) -> WalResult<Option<Vec<u8>>> {
        if self.peek_field_id() != Some(expected_id) {
            return Ok(None);
        }
        Ok(Some(self.read_field_bytes(expected_id)?))
    }

    fn try_read_field_bytes_fixed<const N: usize>(
        &mut self,
        expected_id: u8,
    ) -> WalResult<Option<[u8; N]>> {
        if self.peek_field_id() != Some(expected_id) {
            return Ok(None);
        }
        let bytes = self.read_field_bytes(expected_id)?;
        if bytes.len() != N {
            return Err(WalError::Corrupt(format!(
                "fixed-size field expected {N} bytes, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; N];
        arr.copy_from_slice(&bytes);
        Ok(Some(arr))
    }

    fn read_u64_raw(&mut self) -> WalResult<u64> {
        if self.pos + 8 > self.buf.len() {
            return Err(WalError::Corrupt("truncated u64 value".into()));
        }
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }
}

// ─── CatalogOps trait ─────────────────────────────────────────────────────────

/// Catalog 与执行层操作抽象（对应 C++ 的 `Catalog` + `ClientContext` 调用）。
///
/// 实现者负责：
/// - DDL 操作：创建/删除 Table、Schema、View 等
/// - DML 操作：Append、Delete、Update
/// - 索引注册：将回放期间收集的索引注册进 TableIndexList
///
/// 所有方法的 `payload` 参数均为 BinarySerializer 序列化的字节，
/// 实现者自行用对应的 BinaryDeserializer 解析。
pub trait CatalogOps {
    // ── DDL ─────────────────────────────────────────────────────────────────

    fn create_table(&mut self, payload: &[u8]) -> WalResult<()>;
    fn drop_table(&mut self, schema: &str, name: &str) -> WalResult<()>;
    fn alter_table(
        &mut self,
        payload: &[u8],
        index_storage_info: Option<&[u8]>,
        index_storage_data: Option<&[u8]>,
    ) -> WalResult<Option<ReplayIndexInfo>>;

    fn create_schema(&mut self, name: &str) -> WalResult<()>;
    fn drop_schema(&mut self, name: &str) -> WalResult<()>;

    fn create_view(&mut self, payload: &[u8]) -> WalResult<()>;
    fn drop_view(&mut self, schema: &str, name: &str) -> WalResult<()>;

    fn create_sequence(&mut self, payload: &[u8]) -> WalResult<()>;
    fn drop_sequence(&mut self, schema: &str, name: &str) -> WalResult<()>;
    fn update_sequence_value(
        &mut self,
        schema: &str,
        name: &str,
        usage_count: u64,
        counter: i64,
    ) -> WalResult<()>;

    fn create_macro(&mut self, payload: &[u8]) -> WalResult<()>;
    fn drop_macro(&mut self, schema: &str, name: &str) -> WalResult<()>;

    fn create_table_macro(&mut self, payload: &[u8]) -> WalResult<()>;
    fn drop_table_macro(&mut self, schema: &str, name: &str) -> WalResult<()>;

    fn create_index(
        &mut self,
        catalog_payload: &[u8],
        storage_info: &[u8],
        storage_data: &[u8],
    ) -> WalResult<ReplayIndexInfo>;
    fn drop_index(&mut self, schema: &str, name: &str) -> WalResult<()>;

    fn create_type(&mut self, payload: &[u8]) -> WalResult<()>;
    fn drop_type(&mut self, schema: &str, name: &str) -> WalResult<()>;

    // ── DML ─────────────────────────────────────────────────────────────────

    /// 向当前表追加数据（C++: `storage.LocalWALAppend(...)`）。
    fn append_chunk(&mut self, schema: &str, table: &str, chunk_payload: &[u8]) -> WalResult<()>;

    /// 向当前表合并 RowGroup 数据（C++: `storage.MergeStorage(...)`）。
    fn merge_row_group_data(
        &mut self,
        schema: &str,
        table: &str,
        data_payload: &[u8],
    ) -> WalResult<()>;

    /// 删除行（C++: `storage.Delete(...)`）。
    fn delete_rows(&mut self, schema: &str, table: &str, chunk_payload: &[u8]) -> WalResult<()>;

    /// 更新行（C++: `storage.UpdateColumn(...)`）。
    fn update_rows(
        &mut self,
        schema: &str,
        table: &str,
        column_indexes_payload: &[u8],
        chunk_payload: &[u8],
    ) -> WalResult<()>;

    // ── 事务控制 ─────────────────────────────────────────────────────────────

    /// 提交当前事务（C++: `con.Commit()`）。
    fn commit(&mut self) -> WalResult<()>;

    /// 回滚当前事务（C++: `con.Query("ROLLBACK")`）。
    fn rollback(&mut self) -> WalResult<()>;

    /// 开始新事务（C++: `con.BeginTransaction()`）。
    fn begin_transaction(&mut self) -> WalResult<()>;

    /// 将收集到的索引批量注册（C++: `info.index_list.get().AddIndex(std::move(info.index))`）。
    fn commit_indexes(&mut self, indexes: Vec<ReplayIndexInfo>) -> WalResult<()>;

    // ── 版本验证 ─────────────────────────────────────────────────────────────

    /// 验证 WAL 与数据库文件的 db_identifier 和 checkpoint_iteration 是否匹配。
    ///
    /// 返回 `Ok(None)` 表示验证通过；
    /// 返回 `Ok(Some(expected))` 表示 iteration 差 1（需设置 `expected_checkpoint_id`）；
    /// 返回 `Err` 表示验证失败。
    fn verify_wal_version(
        &self,
        db_identifier: Option<[u8; DB_IDENTIFIER_LEN]>,
        checkpoint_iteration: Option<u64>,
    ) -> WalResult<Option<u64>>;
}

// ─── EncryptionOps trait ──────────────────────────────────────────────────────

/// AES-GCM 解密抽象（对应 C++: `EncryptionKeyManager` + `EncryptionState`）。
///
/// 仅在 WAL 版本为 v3 时使用。
pub trait EncryptionOps {
    /// 解密一帧 v3 密文，返回明文 payload（不含内嵌 checksum）。
    ///
    /// 实现需：
    /// 1. 用 nonce + key 初始化 AES-GCM 解密器
    /// 2. 解密 `ciphertext`（含 8B checksum 前缀）
    /// 3. 验证 `tag`（Finalize）
    /// 4. 提取并校验内嵌 checksum
    /// 5. 返回明文（去掉前 8B checksum 的部分）
    fn decrypt_frame(
        &self,
        nonce: &[u8; NONCE_SIZE],
        ciphertext: &[u8],
        tag: &[u8; TAG_SIZE],
        plaintext_size: u64,
    ) -> WalResult<Vec<u8>>;
}

// ─── WalReplayer ──────────────────────────────────────────────────────────────

/// WAL 回放驱动器（对应 C++: `WriteAheadLog::ReplayInternal`）。
///
/// # 使用方式
///
/// ```ignore
/// let replayer = WalReplayer::new(reader, catalog, None);
/// let result = replayer.replay(WalReplayMode::MainWal)?;
/// ```
pub struct WalReplayer<R: WalReader, C: CatalogOps> {
    frame_reader: WalFrameReader<R>,
    catalog: C,
    /// 可选加密支持（v3 WAL 时必须提供）。
    encryption: Option<Box<dyn EncryptionOps>>,
}

impl<R: WalReader, C: CatalogOps> WalReplayer<R, C> {
    /// 构造（C++: `ReplayInternal(context, storage_manager, handle, replay_state)`）。
    pub fn new(
        reader: R,
        catalog: C,
        encryption: Option<Box<dyn EncryptionOps>>,
    ) -> io::Result<Self> {
        Ok(Self {
            frame_reader: WalFrameReader::new(reader)?,
            catalog,
            encryption,
        })
    }

    /// 执行完整两阶段 WAL 回放（C++: `ReplayInternal` 主体）。
    ///
    /// # 流程
    /// 1. **扫描阶段**（deserialize_only）：找 CHECKPOINT 标记，确定 `checkpoint_position`
    /// 2. **回放阶段**：从头重放，到 `checkpoint_position` 截止（若存在），逐条应用到 Catalog
    ///
    /// # 返回
    /// `WalReplayResult` 包含：回放到的最后一个成功 offset、init_state 建议。
    pub fn replay(&mut self, is_checkpoint_wal: bool) -> WalResult<WalReplayResult> {
        // ── 阶段1：扫描（deserialize_only=true） ────────────────────────────
        let mut scan_state = ReplayState::new();
        self.scan_phase(&mut scan_state)?;

        // checkpoint WAL 不允许包含 CHECKPOINT 标记
        if is_checkpoint_wal && scan_state.checkpoint_id.is_some() {
            return Err(WalError::InvalidCheckpointWal(
                "checkpoint WAL cannot contain a checkpoint marker".into(),
            ));
        }

        // ── 阶段2：回放（deserialize_only=false） ───────────────────────────
        self.frame_reader.reset().map_err(WalError::Io)?;
        let mut replay_state = ReplayState::new();

        let replay_result = self.replay_phase(&mut replay_state, &scan_state)?;
        Ok(replay_result)
    }

    // ── 阶段1：扫描 ──────────────────────────────────────────────────────────

    pub fn inspect(&mut self, is_checkpoint_wal: bool) -> WalResult<WalScanResult> {
        let mut scan_state = ReplayState::new();
        self.scan_phase(&mut scan_state)?;

        if is_checkpoint_wal && scan_state.checkpoint_id.is_some() {
            return Err(WalError::InvalidCheckpointWal(
                "checkpoint WAL cannot contain a checkpoint marker".into(),
            ));
        }

        Ok(WalScanResult {
            checkpoint_id: scan_state.checkpoint_id,
            checkpoint_position: scan_state.checkpoint_position,
            expected_checkpoint_id: scan_state.expected_checkpoint_id,
        })
    }

    fn scan_phase(&mut self, state: &mut ReplayState) -> WalResult<()> {
        loop {
            if self.frame_reader.is_finished().map_err(WalError::Io)? {
                break;
            }
            let (offset, frame) = self.frame_reader.read_frame()?;
            state.current_position = Some(offset);

            let payload = self.resolve_frame(frame, state.wal_version)?;
            let entry = WalEntryDecoder::new(&payload).decode();

            let entry = match entry {
                Ok(e) => e,
                // 截断 WAL（SerializationException 语义）：停止扫描
                Err(WalError::Corrupt(_)) => break,
                Err(e) => return Err(e),
            };

            self.update_scan_state(state, &entry)?;

            if matches!(entry, WalEntry::WalFlush) {
                // 继续扫描，寻找后续的 CHECKPOINT
                continue;
            }
        }
        Ok(())
    }

    fn update_scan_state(&mut self, state: &mut ReplayState, entry: &WalEntry) -> WalResult<()> {
        match entry {
            WalEntry::Version {
                version,
                db_identifier,
                checkpoint_iteration,
            } => {
                state.wal_version = WalVersion::try_from(*version)?;
                self.frame_reader.set_version(state.wal_version);

                let expected = self
                    .catalog
                    .verify_wal_version(*db_identifier, *checkpoint_iteration)?;
                state.expected_checkpoint_id = expected;
            }
            WalEntry::Checkpoint { meta_block } => {
                state.checkpoint_id = Some(*meta_block);
                state.checkpoint_position = state.current_position;
            }
            _ => {}
        }
        Ok(())
    }

    // ── 阶段2：回放 ──────────────────────────────────────────────────────────

    fn replay_phase(
        &mut self,
        state: &mut ReplayState,
        scan_state: &ReplayState,
    ) -> WalResult<WalReplayResult> {
        // 若扫描阶段发现 expected_checkpoint_id 但未找到 CHECKPOINT 条目，
        // 说明 WAL 版本与数据库文件不一致（C++: ThrowVersionError）
        if let Some(expected) = scan_state.expected_checkpoint_id {
            if scan_state.checkpoint_id.is_none() {
                return Err(WalError::VersionMismatch(format!(
                    "WAL was created for checkpoint iteration {}, but no checkpoint marker found",
                    expected - 1
                )));
            }
        }

        self.catalog.begin_transaction()?;

        let mut last_success_offset: u64 = 0;
        let mut all_succeeded = false;

        loop {
            if self.frame_reader.is_finished().map_err(WalError::Io)? {
                all_succeeded = true;
                break;
            }
            let (offset, frame) = match self.frame_reader.read_frame() {
                Ok(v) => v,
                Err(WalError::Corrupt(_)) => break, // 截断 WAL，停止回放
                Err(e) => {
                    let _ = self.catalog.rollback();
                    return Err(e);
                }
            };
            state.current_position = Some(offset);

            let payload = match self.resolve_frame(frame, state.wal_version) {
                Ok(p) => p,
                Err(WalError::Corrupt(_)) => break,
                Err(e) => {
                    let _ = self.catalog.rollback();
                    return Err(e);
                }
            };

            let entry = match WalEntryDecoder::new(&payload).decode() {
                Ok(e) => e,
                Err(WalError::Corrupt(_)) => break,
                Err(e) => {
                    let _ = self.catalog.rollback();
                    return Err(e);
                }
            };

            match self.apply_entry(state, entry) {
                Ok(true) => {
                    // WalFlush：提交当前事务，注册索引
                    self.catalog.commit()?;
                    let indexes = std::mem::take(&mut state.pending_indexes);
                    self.catalog.commit_indexes(indexes)?;
                    last_success_offset =
                        self.frame_reader.current_offset().map_err(WalError::Io)?;

                    if self.frame_reader.is_finished().map_err(WalError::Io)? {
                        all_succeeded = true;
                        break;
                    }
                    self.catalog.begin_transaction()?;
                }
                Ok(false) => {} // 继续
                Err(WalError::Corrupt(_)) => {
                    let _ = self.catalog.rollback();
                    break;
                }
                Err(e) => {
                    let _ = self.catalog.rollback();
                    return Err(e);
                }
            }
        }

        Ok(WalReplayResult {
            last_success_offset,
            all_succeeded,
            checkpoint_id: scan_state.checkpoint_id,
            checkpoint_position: scan_state.checkpoint_position,
        })
    }

    /// 应用一条条目到 Catalog（C++: `ReplayEntry(wal_type)` 的应用部分）。
    ///
    /// 返回 `Ok(true)` 表示 WalFlush（事务提交点）；`Ok(false)` 表示普通条目。
    fn apply_entry(&mut self, state: &mut ReplayState, entry: WalEntry) -> WalResult<bool> {
        match entry {
            WalEntry::WalFlush => return Ok(true),

            WalEntry::Version {
                version,
                db_identifier,
                checkpoint_iteration,
            } => {
                state.wal_version = WalVersion::try_from(version)?;
                self.frame_reader.set_version(state.wal_version);
                let expected = self
                    .catalog
                    .verify_wal_version(db_identifier, checkpoint_iteration)?;
                state.expected_checkpoint_id = expected;
            }

            // ── DDL ────────────────────────────────────────────────────────
            WalEntry::CreateTable { payload } => self.catalog.create_table(&payload)?,
            WalEntry::DropTable { schema, name } => {
                // 同时清除 pending_indexes 中属于该表的条目
                state
                    .pending_indexes
                    .retain(|i| !(i.table_schema == schema && i.table_name == name));
                self.catalog.drop_table(&schema, &name)?;
            }
            WalEntry::AlterInfo {
                payload,
                index_storage_info,
                index_storage_data,
            } => {
                if let Some(info) = self.catalog.alter_table(
                    &payload,
                    index_storage_info.as_deref(),
                    index_storage_data.as_deref(),
                )? {
                    state.pending_indexes.push(info);
                }
            }

            WalEntry::CreateSchema { name } => self.catalog.create_schema(&name)?,
            WalEntry::DropSchema { name } => self.catalog.drop_schema(&name)?,

            WalEntry::CreateView { payload } => self.catalog.create_view(&payload)?,
            WalEntry::DropView { schema, name } => self.catalog.drop_view(&schema, &name)?,

            WalEntry::CreateSequence { payload } => self.catalog.create_sequence(&payload)?,
            WalEntry::DropSequence { schema, name } => {
                self.catalog.drop_sequence(&schema, &name)?
            }
            WalEntry::SequenceValue {
                schema,
                name,
                usage_count,
                counter,
            } => {
                self.catalog
                    .update_sequence_value(&schema, &name, usage_count, counter)?;
            }

            WalEntry::CreateMacro { payload } => self.catalog.create_macro(&payload)?,
            WalEntry::DropMacro { schema, name } => self.catalog.drop_macro(&schema, &name)?,
            WalEntry::CreateTableMacro { payload } => self.catalog.create_table_macro(&payload)?,
            WalEntry::DropTableMacro { schema, name } => {
                self.catalog.drop_table_macro(&schema, &name)?
            }

            WalEntry::CreateIndex {
                catalog_payload,
                storage_info,
                storage_data,
            } => {
                let info =
                    self.catalog
                        .create_index(&catalog_payload, &storage_info, &storage_data)?;
                state.pending_indexes.push(info);
            }
            WalEntry::DropIndex { schema, name } => {
                state.pending_indexes.retain(|i| {
                    !(i.table_schema == schema && i.storage_info.starts_with(name.as_bytes()))
                });
                self.catalog.drop_index(&schema, &name)?;
            }

            WalEntry::CreateType { payload } => self.catalog.create_type(&payload)?,
            WalEntry::DropType { schema, name } => self.catalog.drop_type(&schema, &name)?,

            // ── DML ────────────────────────────────────────────────────────
            WalEntry::UseTable { schema, table } => {
                state.current_table = Some((schema, table));
            }
            WalEntry::Insert { chunk_payload } => {
                let (schema, table) = state
                    .current_table
                    .as_ref()
                    .map(|(s, t)| (s.as_str(), t.as_str()))
                    .ok_or(WalError::NoCurrentTable)?;
                self.catalog.append_chunk(schema, table, &chunk_payload)?;
            }
            WalEntry::RowGroupData { data_payload } => {
                let (schema, table) = state
                    .current_table
                    .as_ref()
                    .map(|(s, t)| (s.as_str(), t.as_str()))
                    .ok_or(WalError::NoCurrentTable)?;
                self.catalog
                    .merge_row_group_data(schema, table, &data_payload)?;
            }
            WalEntry::Delete { chunk_payload } => {
                let (schema, table) = state
                    .current_table
                    .as_ref()
                    .map(|(s, t)| (s.as_str(), t.as_str()))
                    .ok_or(WalError::NoCurrentTable)?;
                self.catalog.delete_rows(schema, table, &chunk_payload)?;
            }
            WalEntry::Update {
                column_indexes_payload,
                chunk_payload,
            } => {
                let (schema, table) = state
                    .current_table
                    .as_ref()
                    .map(|(s, t)| (s.as_str(), t.as_str()))
                    .ok_or(WalError::NoCurrentTable)?;
                self.catalog
                    .update_rows(schema, table, &column_indexes_payload, &chunk_payload)?;
            }

            // ── 控制 ───────────────────────────────────────────────────────
            WalEntry::Checkpoint { meta_block } => {
                state.checkpoint_id = Some(meta_block);
                state.checkpoint_position = state.current_position;
            }
        }

        Ok(false)
    }

    // ── 帧解析辅助 ────────────────────────────────────────────────────────────

    /// 将 `WalFrame` 转为明文 payload 字节（v3 调用 EncryptionOps 解密）。
    fn resolve_frame(&self, frame: WalFrame, _version: WalVersion) -> WalResult<Vec<u8>> {
        match frame {
            WalFrame::Header { payload } => Ok(payload),
            WalFrame::V2 { payload } => Ok(payload),
            WalFrame::V3Encrypted {
                plaintext_size,
                nonce,
                ciphertext,
                tag,
            } => {
                let enc = self.encryption.as_ref().ok_or_else(|| {
                    WalError::Corrupt("v3 encrypted WAL but no EncryptionOps provided".into())
                })?;
                enc.decrypt_frame(&nonce, &ciphertext, &tag, plaintext_size)
            }
        }
    }
}

// ─── WalReplayResult ──────────────────────────────────────────────────────────

/// `WalReplayer::replay()` 的返回值，供调用方决定后续操作。
///
/// 对应 C++: `make_uniq<WriteAheadLog>(storage_manager, wal_path, successful_offset, init_state)` 的参数。
#[derive(Debug)]
pub struct WalReplayResult {
    /// 最后一个成功 commit 之后的文件偏移（C++: `successful_offset`）。
    ///
    /// - 若 `all_succeeded=true`：等于文件末尾
    /// - 若中途失败：WAL 需要截断到此位置（`UNINITIALIZED_REQUIRES_TRUNCATE`）
    pub last_success_offset: u64,

    /// 是否所有条目都成功回放（C++: `all_succeeded`）。
    pub all_succeeded: bool,

    /// 扫描阶段发现的 CHECKPOINT 元数据（C++: `checkpoint_state.checkpoint_id`）。
    pub checkpoint_id: Option<MetaBlockPointer>,

    /// CHECKPOINT 条目在文件中的字节偏移（C++: `checkpoint_state.checkpoint_position`）。
    pub checkpoint_position: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct WalScanResult {
    pub checkpoint_id: Option<MetaBlockPointer>,
    pub checkpoint_position: Option<u64>,
    pub expected_checkpoint_id: Option<u64>,
}

// ─── 校验和 ────────────────────────────────────────────────────────────────────

/// 计算 WAL 条目校验和，与 C++ `Checksum(uint8_t*, size_t)` 完全兼容。
///
/// 算法来自 `src/common/checksum.cpp`：
/// - 初始值 5381
/// - 每 8 字节（小端）：`result ^= chunk * 0xbf58476d1ce4e5b9`
/// - 尾部 1-7 字节：`ChecksumRemainder`（MurmurHash2 风格混合）
fn compute_checksum(data: &[u8]) -> u64 {
    /// 对应 C++ `hash_t Checksum(uint64_t x)`。
    #[inline(always)]
    fn hash_u64(x: u64) -> u64 {
        x.wrapping_mul(0xbf58476d1ce4e5b9)
    }

    /// 对应 C++ `hash_t ChecksumRemainder(void*, size_t)`。
    ///
    /// `tail` 长度严格 < 8（从 `compute_checksum` 调用时保证）。
    fn checksum_remainder(tail: &[u8]) -> u64 {
        const M: u64 = 0xc6a4a7935bd1e995;
        const R: u32 = 47;

        let n = tail.len(); // 0 < n < 8
        let mut h: u64 = 0xe17a1465_u64 ^ (n as u64).wrapping_mul(M);

        // n < 8 → n_blocks = 0，跳过 8 字节循环，直接处理尾部字节。
        // 对应 C++ switch 的 fallthrough（从 case n 向下直到 case 1）：
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
        // 按小端读取 u64（与 C++ x86 原生字节序一致）
        let val = u64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
        result ^= hash_u64(val);
    }

    let tail = &data[n_blocks * 8..];
    if !tail.is_empty() {
        result ^= checksum_remainder(tail);
    }

    result
}
