//! WAL 写入状态机。
//!
//! 对应 C++:
//!   `duckdb/transaction/wal_write_state.hpp`
//!   `src/transaction/wal_write_state.cpp`
//!
//! # 职责
//!
//! `WALWriteState` 在 `UndoBuffer::WriteToWAL()` 中使用，
//! 正向遍历所有 Undo 条目，将变更序列化写入 Write-Ahead Log（WAL）：
//!
//! - **CatalogEntry**：写 `WAL_CREATE_*` 或 `WAL_DROP_*` 记录。
//! - **DeleteTuple**：写 `WAL_DELETE` 记录（行 ID 列表）。
//! - **UpdateTuple**：写 `WAL_UPDATE` 记录（新值向量 + 列路径）。
//! - **SequenceValue**：写 `WAL_SEQUENCE_VALUE` 记录。
//!
//! WAL 按表分组写入：切换表时写 `WAL_USE_TABLE` 记录。
//!
//! # C++ → Rust 关键差异
//!
//! C++ 在 UndoBuffer 中直接存储原始指针（`CatalogEntry*`、`AppendInfo*` 等），
//! Rust 不能存储生命周期受限的原始指针，因此所有 Undo 载荷均为**序列化字节流**。
//! 本模块定义了各载荷的 Rust 序列化格式（见 [`CatalogEntryUndoData`]、
//! [`SequenceValueUndoData`]）。
//!
//! INSERT_TUPLE 对应 C++ 中的 `info->table->WriteToLog(...)` 调用；
//! 这是 DataTable 层的职责，WALWriteState 通过可选回调 [`AppendWriter`] 委托给调用方。

use std::collections::HashMap;

use super::append_info::AppendInfo;
use super::delete_info::DeleteInfo;
use super::types::UndoFlags;
use super::update_info::UpdateInfo;
use crate::storage::table::types::TransactionData;
use crate::storage::storage_manager::StorageCommitState;
use crate::storage::write_ahead_log::WriteAheadLog;

// ─── CatalogWalOp ─────────────────────────────────────────────────────────────

/// Catalog WAL 操作类型（C++: 由 `parent.type` / `entry.type` 共同决定）。
///
/// 编码 C++ `WriteCatalogEntry()` 中的分支逻辑，存储于 [`CatalogEntryUndoData`] 中。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogWalOp {
    CreateTable = 0,
    DropTable = 1,
    CreateSchema = 2,
    DropSchema = 3,
    CreateView = 4,
    DropView = 5,
    CreateSequence = 6,
    DropSequence = 7,
    CreateType = 8,
    DropType = 9,
    CreateMacro = 10,
    DropMacro = 11,
    CreateTableMacro = 12,
    DropTableMacro = 13,
    CreateIndex = 14,
    DropIndex = 15,
    AlterTable = 16,
    /// 不需要写入 WAL（临时对象、RENAMED_ENTRY 等）。
    Ignore = 255,
}

impl TryFrom<u8> for CatalogWalOp {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::CreateTable),
            1 => Ok(Self::DropTable),
            2 => Ok(Self::CreateSchema),
            3 => Ok(Self::DropSchema),
            4 => Ok(Self::CreateView),
            5 => Ok(Self::DropView),
            6 => Ok(Self::CreateSequence),
            7 => Ok(Self::DropSequence),
            8 => Ok(Self::CreateType),
            9 => Ok(Self::DropType),
            10 => Ok(Self::CreateMacro),
            11 => Ok(Self::DropMacro),
            12 => Ok(Self::CreateTableMacro),
            13 => Ok(Self::DropTableMacro),
            14 => Ok(Self::CreateIndex),
            15 => Ok(Self::DropIndex),
            16 => Ok(Self::AlterTable),
            255 => Ok(Self::Ignore),
            other => Err(other),
        }
    }
}

// ─── CatalogEntryUndoData ─────────────────────────────────────────────────────

/// Catalog Undo 条目的完整序列化数据（C++: `CatalogEntry*` + extra_data）。
///
/// C++ 在 UndoBuffer 中存储 `CatalogEntry*` 原始指针并在提交时解引用；
/// Rust 将所有 WAL 写入所需字段完整序列化，避免原始指针依赖。
///
/// # 序列化格式（小端序）
///
/// ```text
/// [op:              u8]
/// [is_temporary:    u8]
/// [schema_len:      u32][schema:           bytes…]
/// [name_len:        u32][name:             bytes…]
/// [payload_len:     u32][payload:          bytes…]   — CREATE* / ALTER 载荷
/// [secondary_len:   u32][secondary:        bytes…]   — CREATE INDEX 索引数据 / ALTER index
/// ```
pub struct CatalogEntryUndoData {
    /// WAL 操作类型（由 C++ `parent.type` + `entry.type` 推导）。
    pub op: CatalogWalOp,
    /// 是否为临时对象（对应 `entry.temporary || parent.temporary`）。
    pub is_temporary: bool,
    /// Schema 名称。
    pub schema: String,
    /// 条目名称。
    pub name: String,
    /// 主载荷：CREATE* 时为序列化的 CreateInfo；ALTER 时为序列化的 AlterInfo。
    pub payload: Vec<u8>,
    /// 次要载荷：CREATE INDEX 时为索引存储数据；ALTER 附带索引时使用。
    pub secondary_payload: Vec<u8>,
}

impl CatalogEntryUndoData {
    /// 序列化为字节。
    pub fn serialize(&self) -> Vec<u8> {
        let schema_b = self.schema.as_bytes();
        let name_b = self.name.as_bytes();
        let cap = 1
            + 1
            + 4
            + schema_b.len()
            + 4
            + name_b.len()
            + 4
            + self.payload.len()
            + 4
            + self.secondary_payload.len();

        let mut out = Vec::with_capacity(cap);
        out.push(self.op as u8);
        out.push(self.is_temporary as u8);
        out.extend_from_slice(&(schema_b.len() as u32).to_le_bytes());
        out.extend_from_slice(schema_b);
        out.extend_from_slice(&(name_b.len() as u32).to_le_bytes());
        out.extend_from_slice(name_b);
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&(self.secondary_payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.secondary_payload);
        out
    }

    /// 从字节反序列化，失败时返回 `None`。
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        let mut p = 0usize;

        let op = CatalogWalOp::try_from(*bytes.get(p)?).ok()?;
        p += 1;
        let is_temporary = *bytes.get(p)? != 0;
        p += 1;

        let schema = read_str(bytes, &mut p)?;
        let name = read_str(bytes, &mut p)?;
        let payload = read_bytes(bytes, &mut p)?;
        let secondary_payload = read_bytes(bytes, &mut p)?;

        Some(Self {
            op,
            is_temporary,
            schema,
            name,
            payload,
            secondary_payload,
        })
    }
}

// ─── SequenceValueUndoData ────────────────────────────────────────────────────

/// 序列值 Undo 条目载荷（C++: `SequenceValue` 中 `entry->GetSchemaName()` + `entry->name` + 计数器）。
///
/// C++ `SequenceValue` 含 `SequenceCatalogEntry*` 指针用于读取 schema/name；
/// Rust 将 schema/name 直接序列化到 Undo 载荷中。
///
/// # 序列化格式（小端序）
///
/// ```text
/// [schema_len:   u32][schema:   bytes…]
/// [name_len:     u32][name:     bytes…]
/// [usage_count:  i64]
/// [counter:      i64]
/// ```
pub struct SequenceValueUndoData {
    pub schema: String,
    pub name: String,
    pub usage_count: i64,
    pub counter: i64,
}

impl SequenceValueUndoData {
    /// 序列化为字节。
    pub fn serialize(&self) -> Vec<u8> {
        let schema_b = self.schema.as_bytes();
        let name_b = self.name.as_bytes();
        let cap = 4 + schema_b.len() + 4 + name_b.len() + 8 + 8;
        let mut out = Vec::with_capacity(cap);
        out.extend_from_slice(&(schema_b.len() as u32).to_le_bytes());
        out.extend_from_slice(schema_b);
        out.extend_from_slice(&(name_b.len() as u32).to_le_bytes());
        out.extend_from_slice(name_b);
        out.extend_from_slice(&self.usage_count.to_le_bytes());
        out.extend_from_slice(&self.counter.to_le_bytes());
        out
    }

    /// 从字节反序列化，失败时返回 `None`。
    pub fn deserialize(bytes: &[u8]) -> Option<Self> {
        let mut p = 0usize;
        let schema = read_str(bytes, &mut p)?;
        let name = read_str(bytes, &mut p)?;
        let usage_count = read_i64(bytes, &mut p)?;
        let counter = read_i64(bytes, &mut p)?;
        Some(Self {
            schema,
            name,
            usage_count,
            counter,
        })
    }
}

// ─── AppendWriter trait ───────────────────────────────────────────────────────

/// INSERT_TUPLE 的 WAL 写入委托（C++: `DataTable::WriteToLog(transaction, log, start, count, commit_state)`）。
///
/// C++ 在 `WALWriteState::CommitEntry` 中直接调用 `info->table->WriteToLog(…)`，
/// 由 DataTable 负责将提交后的行序列化写入 WAL（含乐观 RowGroup 数据路径）。
/// Rust WALWriteState 通过此 trait 将同样的委托传递给调用方。
///
/// # 参数（对应 C++）
/// - `table_id`：被追加的表 ID（C++: `DataTable*`）。
/// - `start_row`：追加起始行号（C++: `idx_t start_row`）。
/// - `count`：追加行数（C++: `idx_t count`）。
pub trait AppendWriter {
    fn write_append(
        &mut self,
        transaction: TransactionData,
        log: &WriteAheadLog,
        commit_state: Option<&mut dyn StorageCommitState>,
        table_id: u64,
        start_row: u64,
        count: u64,
    );
}

// ─── WALWriteState ────────────────────────────────────────────────────────────

/// WAL 写入阶段 Undo 遍历状态机（C++: `class WALWriteState`）。
pub struct WALWriteState<'wal> {
    /// 提交事务的可见性上下文（C++: `DuckTransaction &transaction`）。
    transaction: TransactionData,
    /// WAL 写入器（C++: `WriteAheadLog &log`）。
    log: &'wal WriteAheadLog,

    /// 可选的存储提交状态（C++: `optional_ptr<StorageCommitState> commit_state`）。
    commit_state: Option<&'wal mut dyn StorageCommitState>,

    /// 当前正在写入的表（schema, table）（C++: `optional_ptr<DataTableInfo> current_table_info`）。
    current_table: Option<(String, String)>,

    /// 表 ID → (schema, table) 查找表（C++: 通过 `DataTableInfo::GetSchemaName/GetTableName()` 实现）。
    ///
    /// 在 C++ 中，`DeleteInfo` 和 `UpdateInfo` 含有 DataTable/UpdateSegment 指针，
    /// 可直接访问 `DataTableInfo` 获取 schema/name；Rust 以 ID 查表代替。
    table_names: HashMap<u64, (String, String)>,

    /// INSERT_TUPLE 委托（C++: `info->table->WriteToLog(…)`）。
    append_writer: Option<Box<dyn AppendWriter + 'wal>>,
}

impl<'wal> WALWriteState<'wal> {
    /// 构造（C++: `WALWriteState(DuckTransaction&, WriteAheadLog&, optional_ptr<StorageCommitState>)`）。
    ///
    /// `table_names`：将表 ID 映射到 `(schema, table)` 名称，用于 [`SwitchTable`] 写 USE_TABLE 记录。
    /// `append_writer`：处理 INSERT_TUPLE 的委托，对应 C++ `DataTable::WriteToLog`。
    pub fn new(
        transaction: TransactionData,
        log: &'wal WriteAheadLog,
        commit_state: Option<&'wal mut dyn StorageCommitState>,
        table_names: HashMap<u64, (String, String)>,
        append_writer: Option<Box<dyn AppendWriter + 'wal>>,
    ) -> Self {
        Self {
            transaction,
            log,
            commit_state,
            current_table: None,
            table_names,
            append_writer,
        }
    }

    // ── 公开接口 ──────────────────────────────────────────────────────────────

    /// 处理一条 Undo 条目（C++: `WALWriteState::CommitEntry()`）。
    pub fn commit_entry(&mut self, flags: UndoFlags, payload: &[u8]) {
        match flags {
            UndoFlags::CatalogEntry => {
                // C++: auto catalog_entry = Load<CatalogEntry *>(data);
                //      WriteCatalogEntry(*catalog_entry, data + sizeof(CatalogEntry *));
                self.write_catalog_entry(payload);
            }
            UndoFlags::Append => {
                // C++: info->table->WriteToLog(transaction, log, info->start_row, info->count, commit_state)
                // 由 DataTable 负责；Rust 通过 AppendWriter 委托。
                self.write_append(payload);
            }
            UndoFlags::DeleteTuple => {
                // C++: if (!info->table->IsTemporary()) WriteDelete(*info);
                self.write_delete(payload);
            }
            UndoFlags::UpdateTuple => {
                // C++: if (!info->segment->column_data.GetTableInfo().IsTemporary()) WriteUpdate(*info);
                self.write_update(payload);
            }
            UndoFlags::SequenceValue => {
                // C++: log.WriteSequenceValue(*info);
                self.write_sequence_value(payload);
            }
            UndoFlags::Attach | UndoFlags::Empty => {
                // C++: case ATTACHED_DATABASE: break;（无需写 WAL）
            }
        }
    }

    // ── 私有：SwitchTable ─────────────────────────────────────────────────────

    /// 切换当前操作的表，必要时写 `USE_TABLE` 记录（C++: `WALWriteState::SwitchTable()`）。
    fn switch_table(&mut self, schema: &str, table: &str) {
        // C++: if (current_table_info != &table_info) { log.WriteSetTable(...); current_table_info = table_info; }
        let changed = match &self.current_table {
            Some((s, t)) => s.as_str() != schema || t.as_str() != table,
            None => true,
        };
        if changed {
            let _ = self.log.write_set_table(schema, table);
            self.current_table = Some((schema.to_string(), table.to_string()));
        }
    }

    // ── 私有：WriteCatalogEntry ───────────────────────────────────────────────

    /// 将 Catalog 条目变更写入 WAL（C++: `WALWriteState::WriteCatalogEntry()`）。
    ///
    /// 载荷格式（由 `DuckTransaction::push_catalog_entry` 写入）：
    /// ```text
    /// [catalog_entry_id: u64]
    /// [extra_data_size:  u64]    ← 存在则 > 0
    /// [extra_data:       bytes]  ← CatalogEntryUndoData::serialize()
    /// ```
    fn write_catalog_entry(&mut self, payload: &[u8]) {
        // C++: auto catalog_entry = Load<CatalogEntry *>(data);
        //      if (entry.temporary || entry.Parent().temporary) return;
        //      WriteCatalogEntry(*catalog_entry, data + sizeof(CatalogEntry *));

        // 跳过 catalog_entry_id（8 字节）
        if payload.len() < 8 {
            return;
        }
        let after_id = &payload[8..];

        // 读取 extra_data_size（8 字节），0 表示无 extra_data
        if after_id.len() < 8 {
            return;
        }
        let extra_size = u64::from_le_bytes(after_id[0..8].try_into().unwrap()) as usize;
        if extra_size == 0 || after_id.len() < 8 + extra_size {
            return;
        }

        let data = &after_id[8..8 + extra_size];
        let entry = match CatalogEntryUndoData::deserialize(data) {
            Some(e) => e,
            None => return,
        };

        // C++: if (entry.temporary || entry.Parent().temporary) return;
        if entry.is_temporary {
            return;
        }

        // C++: switch (parent.type) { … }
        match entry.op {
            CatalogWalOp::CreateTable => {
                // C++: log.WriteCreateTable(parent.Cast<TableCatalogEntry>());
                let _ = self.log.write_create_table(&entry.payload);
            }
            CatalogWalOp::DropTable => {
                // C++: log.WriteDropTable(table_entry);
                let _ = self.log.write_drop_table(&entry.schema, &entry.name);
            }
            CatalogWalOp::CreateSchema => {
                // C++: log.WriteCreateSchema(parent.Cast<SchemaCatalogEntry>());
                let _ = self.log.write_create_schema(&entry.schema);
            }
            CatalogWalOp::DropSchema => {
                // C++: log.WriteDropSchema(entry.Cast<SchemaCatalogEntry>());
                let _ = self.log.write_drop_schema(&entry.schema);
            }
            CatalogWalOp::CreateView => {
                // C++: log.WriteCreateView(parent.Cast<ViewCatalogEntry>());
                let _ = self.log.write_create_view(&entry.payload);
            }
            CatalogWalOp::DropView => {
                // C++: log.WriteDropView(entry.Cast<ViewCatalogEntry>());
                let _ = self.log.write_drop_view(&entry.schema, &entry.name);
            }
            CatalogWalOp::CreateSequence => {
                // C++: log.WriteCreateSequence(parent.Cast<SequenceCatalogEntry>());
                let _ = self.log.write_create_sequence(&entry.payload);
            }
            CatalogWalOp::DropSequence => {
                // C++: log.WriteDropSequence(entry.Cast<SequenceCatalogEntry>());
                let _ = self.log.write_drop_sequence(&entry.schema, &entry.name);
            }
            CatalogWalOp::CreateType => {
                // C++: log.WriteCreateType(parent.Cast<TypeCatalogEntry>());
                let _ = self.log.write_create_type(&entry.payload);
            }
            CatalogWalOp::DropType => {
                // C++: log.WriteDropType(entry.Cast<TypeCatalogEntry>());
                let _ = self.log.write_drop_type(&entry.schema, &entry.name);
            }
            CatalogWalOp::CreateMacro => {
                // C++: log.WriteCreateMacro(parent.Cast<ScalarMacroCatalogEntry>());
                let _ = self.log.write_create_macro(&entry.payload);
            }
            CatalogWalOp::DropMacro => {
                // C++: log.WriteDropMacro(entry.Cast<ScalarMacroCatalogEntry>());
                let _ = self.log.write_drop_macro(&entry.schema, &entry.name);
            }
            CatalogWalOp::CreateTableMacro => {
                // C++: log.WriteCreateTableMacro(parent.Cast<TableMacroCatalogEntry>());
                let _ = self.log.write_create_table_macro(&entry.payload);
            }
            CatalogWalOp::DropTableMacro => {
                // C++: log.WriteDropTableMacro(entry.Cast<TableMacroCatalogEntry>());
                let _ = self.log.write_drop_table_macro(&entry.schema, &entry.name);
            }
            CatalogWalOp::CreateIndex => {
                // C++: log.WriteCreateIndex(parent.Cast<IndexCatalogEntry>());
                let _ = self
                    .log
                    .write_create_index(&entry.payload, &entry.secondary_payload);
            }
            CatalogWalOp::DropIndex => {
                // C++: log.WriteDropIndex(entry.Cast<IndexCatalogEntry>());
                let _ = self.log.write_drop_index(&entry.schema, &entry.name);
            }
            CatalogWalOp::AlterTable => {
                // C++: log.WriteAlter(entry, alter_info);
                let index_storage = if entry.secondary_payload.is_empty() {
                    None
                } else {
                    Some(entry.secondary_payload.as_slice())
                };
                let _ = self.log.write_alter(&entry.payload, index_storage);
            }
            CatalogWalOp::Ignore => {
                // C++: 对应 RENAMED_ENTRY / SCHEMA_ENTRY ALTER 等无需写 WAL 的情况
            }
        }
    }

    // ── 私有：WriteAppend ─────────────────────────────────────────────────────

    /// 委托 INSERT_TUPLE 给 `AppendWriter`（C++: `info->table->WriteToLog(…)`）。
    fn write_append(&mut self, payload: &[u8]) {
        if payload.len() < AppendInfo::serialized_size() {
            return;
        }
        let info = AppendInfo::deserialize(payload);

        // C++: if (!info->table->IsTemporary())
        //          info->table->WriteToLog(transaction, log, start_row, count, commit_state);
        // 临时表不写 WAL；table_names 中不存在的 ID 按临时表处理。
        if !self.table_names.contains_key(&info.table_id) {
            return;
        }

        if let Some(writer) = self.append_writer.as_mut() {
            let commit_state = self
                .commit_state
                .as_deref_mut()
                .map(|state| state as *mut dyn StorageCommitState);
            writer.write_append(
                self.transaction,
                self.log,
                commit_state.map(|ptr| unsafe { &mut *ptr }),
                info.table_id,
                info.start_row,
                info.count,
            );
        }
    }

    // ── 私有：WriteDelete ─────────────────────────────────────────────────────

    /// 将行删除写入 WAL（C++: `WALWriteState::WriteDelete()`）。
    ///
    /// WAL DELETE 载荷格式：
    /// ```text
    /// [count:      u32]
    /// [row_id[0]:  i64]
    /// …
    /// [row_id[n]:  i64]
    /// ```
    fn write_delete(&mut self, payload: &[u8]) {
        // C++: SwitchTable(*info.table->GetDataTableInfo(), UndoFlags::DELETE_TUPLE);
        let info = DeleteInfo::deserialize(payload);

        if let Some((schema, table)) = self.table_names.get(&info.table_id).cloned() {
            self.switch_table(&schema, &table);
        }

        let count = info.count as usize;
        // C++: delete_chunk->Initialize(…, {ROW_TYPE}); rows = FlatVector::GetData<row_t>(…);
        let mut chunk = Vec::with_capacity(4 + count * 8);
        chunk.extend_from_slice(&(count as u32).to_le_bytes());

        if info.is_consecutive {
            // C++: for (idx_t i = 0; i < info.count; i++) rows[i] = info.base_row + i;
            for i in 0..count {
                let row_id = (info.base_row as i64) + i as i64;
                chunk.extend_from_slice(&row_id.to_le_bytes());
            }
        } else {
            // C++: auto delete_rows = info.GetRows();
            //      for (idx_t i = 0; i < info.count; i++)
            //          rows[i] = UnsafeNumericCast<int64_t>(info.base_row) + delete_rows[i];
            let rows = info.get_rows();
            for i in 0..count {
                let row_id = (info.base_row as i64) + (rows[i] as i64);
                chunk.extend_from_slice(&row_id.to_le_bytes());
            }
        }

        // C++: log.WriteDelete(*delete_chunk);
        let _ = self.log.write_delete(&chunk);
    }

    // ── 私有：WriteUpdate ─────────────────────────────────────────────────────

    /// 将列更新写入 WAL（C++: `WALWriteState::WriteUpdate()`）。
    ///
    /// WAL 列路径载荷格式：
    /// ```text
    /// [count:       u32]
    /// [col_idx[0]:  u64]
    /// …
    /// ```
    ///
    /// WAL UPDATE 数据块载荷格式：
    /// ```text
    /// [n:           u32]
    /// [row_id[0]:   i64]   ← row_group_start + vector_index*STANDARD_VECTOR_SIZE + tuples[i]
    /// …
    /// [values:      bytes] ← UpdateInfo.values 原始字节
    /// ```
    fn write_update(&mut self, payload: &[u8]) {
        // C++: auto &column_data = info.segment->column_data;
        //      auto &table_info  = column_data.GetTableInfo();
        //      SwitchTable(table_info, UndoFlags::UPDATE_TUPLE);

        // UpdateInfo 固定字段大小（不读 tuples/values，仅读头部）：
        // segment_id(8)+table_id(8)+column_index(8)+row_group_start(8)+vector_index(8)
        // +version_number(8)+prev(16)+next(16)+n(2)+max(2) = 84 字节
        const HEADER_SIZE: usize = 84;
        if payload.len() < HEADER_SIZE {
            return;
        }

        // 直接从字节读取关键字段，避免完整反序列化（C++ 通过指针直接访问结构体成员）。
        let table_id = u64::from_le_bytes(payload[8..16].try_into().unwrap());
        let column_index = u64::from_le_bytes(payload[16..24].try_into().unwrap());
        let row_group_start = u64::from_le_bytes(payload[24..32].try_into().unwrap());
        let vector_index = u64::from_le_bytes(payload[32..40].try_into().unwrap());
        let n = u16::from_le_bytes(payload[80..82].try_into().unwrap()) as usize;

        // C++: SwitchTable(table_info, UndoFlags::UPDATE_TUPLE);
        if let Some((schema, table)) = self.table_names.get(&table_id).cloned() {
            self.switch_table(&schema, &table);
        }

        // C++: idx_t start = info.row_group_start + info.vector_index * STANDARD_VECTOR_SIZE;
        //      auto tuples = info.GetTuples();
        //      for (idx_t i = 0; i < info.N; i++)
        //          row_ids[tuples[i]] = UnsafeNumericCast<int64_t>(start + tuples[i]);
        const STANDARD_VECTOR_SIZE: u64 = 2048;
        let start = row_group_start + vector_index * STANDARD_VECTOR_SIZE;

        // 读取 tuples（sel_t = u32，位于 HEADER_SIZE 之后）
        let tuples_offset = HEADER_SIZE;
        let tuples_end = tuples_offset + n * 4;
        if payload.len() < tuples_end {
            return;
        }

        let mut chunk = Vec::with_capacity(4 + n * 8);
        chunk.extend_from_slice(&(n as u32).to_le_bytes());

        for i in 0..n {
            let off = tuples_offset + i * 4;
            let tuple_idx = u32::from_le_bytes(payload[off..off + 4].try_into().unwrap()) as u64;
            let row_id = (start + tuple_idx) as i64;
            chunk.extend_from_slice(&row_id.to_le_bytes());
        }

        // C++: info.segment->FetchCommitted(info.vector_index, update_chunk->data[0]);
        // Rust: values 已序列化在 UpdateInfo.values 中，直接附加。
        let values_offset = tuples_end;
        chunk.extend_from_slice(&payload[values_offset..]);

        // C++: vector<column_t> column_indexes;
        //      reference<const ColumnData> current = column_data;
        //      while (current.get().HasParent()) {
        //          column_indexes.push_back(current.get().column_index);
        //          current = current.get().Parent();
        //      }
        //      column_indexes.push_back(info.column_index);
        //      std::reverse(column_indexes.begin(), column_indexes.end());
        //      log.WriteUpdate(*update_chunk, column_indexes);
        //
        // Rust UpdateInfo 仅存储叶级 column_index，不含父列路径。
        // 若需要嵌套列路径，需从 UpdateSegment 中读取；此处按平表处理（路径长度为 1）。
        let mut col_idx_payload = Vec::with_capacity(4 + 8);
        col_idx_payload.extend_from_slice(&1u32.to_le_bytes());
        col_idx_payload.extend_from_slice(&column_index.to_le_bytes());

        let _ = self.log.write_update(&col_idx_payload, &chunk);
    }

    // ── 私有：WriteSequenceValue ──────────────────────────────────────────────

    /// 将序列值写入 WAL（C++: `log.WriteSequenceValue(*info)`）。
    fn write_sequence_value(&mut self, payload: &[u8]) {
        // C++: auto info = reinterpret_cast<SequenceValue *>(data);
        //      log.WriteSequenceValue(*info);
        // info 含 SequenceCatalogEntry* entry，用于获取 schema/name。
        // Rust: SequenceValueUndoData 中直接包含 schema/name。
        if let Some(sv) = SequenceValueUndoData::deserialize(payload) {
            let _ = self
                .log
                .write_sequence_value(&sv.schema, &sv.name, sv.usage_count, sv.counter);
        }
    }
}

// ─── 反序列化辅助函数 ──────────────────────────────────────────────────────────

/// 从 `bytes[*pos..]` 读取长度前缀（u32 LE）的 UTF-8 字符串，推进 `*pos`。
fn read_str(bytes: &[u8], pos: &mut usize) -> Option<String> {
    let len = u32::from_le_bytes(bytes.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
    *pos += 4;
    let s = std::str::from_utf8(bytes.get(*pos..*pos + len)?)
        .ok()?
        .to_string();
    *pos += len;
    Some(s)
}

/// 从 `bytes[*pos..]` 读取长度前缀（u32 LE）的字节切片，推进 `*pos`。
fn read_bytes(bytes: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let len = u32::from_le_bytes(bytes.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
    *pos += 4;
    let v = bytes.get(*pos..*pos + len)?.to_vec();
    *pos += len;
    Some(v)
}

/// 从 `bytes[*pos..]` 读取 i64（小端序），推进 `*pos`。
fn read_i64(bytes: &[u8], pos: &mut usize) -> Option<i64> {
    let v = i64::from_le_bytes(bytes.get(*pos..*pos + 8)?.try_into().ok()?);
    *pos += 8;
    Some(v)
}
