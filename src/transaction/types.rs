//! 事务层基础类型。
//!
//! 对应 C++:
//!   - `duckdb/common/types.hpp`（transaction_t, idx_t）
//!   - `duckdb/common/enums/undo_flags.hpp`（UndoFlags）
//!   - `duckdb/common/enums/active_transaction_state.hpp`（ActiveTransactionState）
//!   - `duckdb/transaction/commit_state.hpp`（CommitMode）
//!   - `duckdb/transaction/meta_transaction.hpp`（TransactionState）

// ─── 基础类型别名 ──────────────────────────────────────────────────────────────

/// 单调递增事务 ID（C++: `transaction_t`，`u64`）。
pub type TransactionId = u64;

/// 通用索引 / 行计数（C++: `idx_t`，`u64`）。
pub type Idx = u64;

/// 行选择向量元素（C++: `sel_t`，`u32`）。
pub type SelT = u32;

/// 时间戳（C++: `timestamp_t`，`i64` 微秒 epoch）。
pub type Timestamp = i64;

// ─── 常量 ──────────────────────────────────────────────────────────────────────

/// 事务 ID 起始值（C++: `TRANSACTION_ID_START = 4611686018427387904 = 2^62`）。
/// 保证比所有合法时间戳大，使得未提交数据对旧事务不可见。
pub const TRANSACTION_ID_START: TransactionId = 1 << 62;

/// 最大合法事务 ID（C++: `MAX_TRANSACTION_ID = UINT64_MAX`）。
pub const MAX_TRANSACTION_ID: TransactionId = u64::MAX;

/// 标记行"从未被删除"的哨兵（C++: `NOT_DELETED_ID = UINT64_MAX - 1`）。
pub const NOT_DELETED_ID: TransactionId = u64::MAX - 1;

/// 无活跃查询时 active_query 的哨兵（C++: `MAXIMUM_QUERY_ID = UINT64_MAX`）。
pub const MAXIMUM_QUERY_ID: TransactionId = u64::MAX;

/// Undo 条目头部大小（flags u8 + len u32）。
pub const UNDO_ENTRY_HEADER_SIZE: usize = 1 + 4;

// ─── UndoFlags ──────────────────────────────────────────────────────────────────

/// Undo 日志条目类型（C++: `enum class UndoFlags : uint8_t`）。
///
/// 每个条目写入 UndoBuffer 时先写 `UndoFlags`（1 byte）再写长度（4 bytes），
/// 再写有效载荷。
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UndoFlags {
    /// 空条目（已被清除）（C++: `EMPTY_ENTRY = 0`）。
    Empty = 0,
    /// Catalog 条目变更（建表/改列/删索引等）（C++: `CATALOG_ENTRY = 1`）。
    CatalogEntry = 1,
    /// 行删除（C++: `DELETE_TUPLE = 2`）。
    DeleteTuple = 2,
    /// 行更新（C++: `UPDATE_TUPLE = 3`）。
    UpdateTuple = 3,
    /// Sequence 使用（C++: `SEQUENCE_VALUE = 4`）。
    SequenceValue = 4,
    /// ATTACH DATABASE（C++: `ATTACH = 5`）。
    Attach = 5,
    /// Append 行操作（rollback_state / cleanup_state 内部使用）。
    Append = 6,
}

impl TryFrom<u8> for UndoFlags {
    type Error = u8;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Empty),
            1 => Ok(Self::CatalogEntry),
            2 => Ok(Self::DeleteTuple),
            3 => Ok(Self::UpdateTuple),
            4 => Ok(Self::SequenceValue),
            5 => Ok(Self::Attach),
            6 => Ok(Self::Append),
            other => Err(other),
        }
    }
}

// ─── ActiveTransactionState ─────────────────────────────────────────────────────

/// 提交时其他事务的活跃状态（C++: `enum class ActiveTransactionState`）。
///
/// 影响 cleanup 策略：若提交时无其他活跃事务，旧版本可立即回收。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActiveTransactionState {
    /// 尚未确定（C++: `UNSET`）。
    #[default]
    Unset,
    /// 提交时存在其他活跃事务（C++: `HAS_ACTIVE_TRANSACTIONS`）。
    HasActiveTransactions,
    /// 提交时无其他活跃事务（C++: `NO_ACTIVE_TRANSACTIONS`）。
    NoActiveTransactions,
}

// ─── CommitMode ─────────────────────────────────────────────────────────────────

/// 提交处理模式（C++: `enum class CommitMode`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// 正常提交（C++: `COMMIT`）。
    Commit,
    /// 回退已提交的修改（C++: `REVERT_COMMIT`）。
    RevertCommit,
}

// ─── TransactionState ───────────────────────────────────────────────────────────

/// MetaTransaction 的生命周期状态（C++: `enum class TransactionState`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Uncommitted,
    Committed,
    RolledBack,
}
