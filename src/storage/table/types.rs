//! Common type aliases shared across the table storage layer.
//!
//! These mirror DuckDB's typedefs (`idx_t`, `row_t`, `transaction_t`, etc.)
//! from `duckdb/common/types.hpp`.

/// Unsigned row count / index (C++: `idx_t`).
pub type Idx = u64;

/// Signed row identifier, used in mutations (C++: `row_t`).
pub type RowId = i64;

/// Monotonically-increasing transaction identifier (C++: `transaction_t`).
pub type TransactionId = u64;

/// Block identifier on disk (C++: `block_id_t`).
pub type BlockId = i64;

/// Sentinel value for an invalid/unset block id (C++: `INVALID_BLOCK`).
pub const INVALID_BLOCK: BlockId = -1;

/// Standard vector size — number of rows per SIMD vector (C++: `STANDARD_VECTOR_SIZE`).
pub const STANDARD_VECTOR_SIZE: Idx = 2048;

/// Default row-group size (C++: `Storage::ROW_GROUP_SIZE = 122880`).
pub const ROW_GROUP_SIZE: Idx = 122880;

/// A pointer to a metadata block on disk (C++: `MetaBlockPointer`).
pub use crate::storage::metadata::MetaBlockPointer;

/// A data pointer that locates column data on disk (C++: `DataPointer`).
#[derive(Debug, Clone)]
pub struct DataPointer {
    pub block_id: BlockId,
    pub offset: u32,
    pub row_start: Idx,
    pub tuple_count: Idx,
    pub compression_type: CompressionType,
    pub statistics: crate::storage::statistics::BaseStatistics,
}

impl Default for DataPointer {
    fn default() -> Self {
        Self {
            block_id: INVALID_BLOCK,
            offset: 0,
            row_start: 0,
            tuple_count: 0,
            compression_type: CompressionType::Uncompressed,
            statistics: crate::storage::statistics::BaseStatistics::create_empty(
                crate::common::types::LogicalType::integer(),
            ),
        }
    }
}

/// Bundled transaction context passed to storage methods (C++: `TransactionData`).
#[derive(Debug, Clone, Copy)]
pub struct TransactionData {
    pub start_time: TransactionId,
    pub transaction_id: TransactionId,
}

/// 统一使用 common 层的 `LogicalType`（消除与 `crate::common::types::LogicalType` 的重复定义）。
pub use crate::common::types::LogicalType;

/// Placeholder for `PhysicalType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType {
    Bool,
    Bit,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float,
    Double,
    VarChar,
    List,
    Struct,
    Array,
    Invalid,
}

/// First transaction id in the "uncommitted" range (C++: `TRANSACTION_ID_START`, 2^62).
///
/// Transaction ids below this are committed timestamps; at or above are live transaction ids.
pub const TRANSACTION_ID_START: TransactionId = 4_611_686_018_427_388_000;

/// Sentinel "not deleted" value stored in per-row delete fields (C++: `NOT_DELETED_ID`).
///
/// Equal to `u64::MAX - 1` (2^64 − 2).  A delete-id of this value means the row has never
/// been deleted.
pub const NOT_DELETED_ID: TransactionId = TransactionId::MAX - 1;

// ─── MVCC visibility helpers ─────────────────────────────────────────────────

/// Returns `true` if a row whose insert-id is `id` is visible to `transaction`.
///
/// Mirrors C++ `TransactionVersionOperator::UseInsertedVersion`:
/// `id < start_time || id == transaction_id`
///
/// - `id < start_time` → committed before our snapshot started → visible.
/// - `id == transaction_id` → we inserted it ourselves → visible.
#[inline]
pub fn use_inserted_version(
    start_time: TransactionId,
    transaction_id: TransactionId,
    id: TransactionId,
) -> bool {
    id < start_time || id == transaction_id
}

/// Returns `true` if a row whose delete-id is `id` should be treated as **not** deleted
/// from the perspective of `transaction` (i.e. the deletion is invisible to us).
///
/// Mirrors C++ `TransactionVersionOperator::UseDeletedVersion`:
/// `!UseInsertedVersion(id)` = `id >= start_time && id != transaction_id`
///
/// - `id == NOT_DELETED_ID` → never deleted → always returns `true` (not deleted).
/// - delete by another uncommitted txn (`id >= start_time && id != transaction_id`) → invisible deletion → row still visible.
#[inline]
pub fn use_deleted_version(
    start_time: TransactionId,
    transaction_id: TransactionId,
    id: TransactionId,
) -> bool {
    !use_inserted_version(start_time, transaction_id, id)
}

/// Returns `true` if the row is visible to `transaction` (combined insert + delete check).
#[inline]
pub fn is_row_visible(
    transaction: TransactionData,
    insert_id: TransactionId,
    delete_id: TransactionId,
) -> bool {
    use_inserted_version(
        transaction.start_time,
        transaction.transaction_id,
        insert_id,
    ) && use_deleted_version(
        transaction.start_time,
        transaction.transaction_id,
        delete_id,
    )
}

// ─────────────────────────────────────────────────────────────────────────────

/// Placeholder for compression type (C++: `CompressionType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Auto,
    Uncompressed,
    Constant,
    Rle,
    Dictionary,
    PforDelta,
    BitPacking,
    Fsst,
    Chimp,
    Patas,
    Alp,
    Alprd,
    ZStd,
    Roaring,
    Empty,
    DictFSST,
}
