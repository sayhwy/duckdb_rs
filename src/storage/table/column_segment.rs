//! `ColumnSegment` — one contiguous run of column data on a buffer page.
//!
//! Mirrors `duckdb/storage/table/column_segment.hpp`.
//!
//! A column is stored as a sorted list of non-overlapping `ColumnSegment`s
//! managed by a `ColumnSegmentTree` (a `SegmentTree<ColumnSegment>`).
//!
//! Each segment is either:
//! - **Transient**: lives in an in-memory buffer (during active appends).
//! - **Persistent**: backed by an on-disk block (after checkpointing).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::append_state::ColumnAppendState;
use super::scan_state::ColumnScanState;
use super::segment_base::SegmentBase;
use super::types::{BlockId, CompressionType, Idx, LogicalType};
use crate::common::types::{SelectionVector, ValidityMask, Vector, VectorType};
use crate::storage::buffer::BlockHandle;

// ─────────────────────────────────────────────────────────────────────────────
// ScanVectorType
// ─────────────────────────────────────────────────────────────────────────────

/// Whether the scan result should be a fully-encoded vector or a flat vector.
///
/// Mirrors `enum class ScanVectorType` in DuckDB.
///
/// | Variant | Meaning |
/// |---------|---------|
/// | `ScanEntireVector` | Preserve dictionary / sequence encoding when possible |
/// | `ScanFlatVector`   | Always produce a flat (row-major) vector; required when merging across segments |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanVectorType {
    /// Full-vector scan: may return dictionary or sequence vectors.
    ScanEntireVector,
    /// Partial / flat scan: always writes into a flat result buffer.
    ScanFlatVector,
}

// ─────────────────────────────────────────────────────────────────────────────
// UnifiedVectorFormat
// ─────────────────────────────────────────────────────────────────────────────

/// Normalised view of a `Vector` suitable for low-level segment writes.
///
/// Mirrors `struct UnifiedVectorFormat` in `duckdb/common/types/vector.hpp`.
///
/// | C++ field               | Rust field | Notes                             |
/// |-------------------------|------------|-----------------------------------|
/// | `sel`                   | `sel`      | `None` = identity (flat) mapping  |
/// | `const_data_ptr_t data` | `data`     | raw byte slice over the payload   |
/// | `ValidityMask validity` | `validity` | NULL bitmask                      |
///
/// A caller constructs this from a `Vector` just before calling
/// `ColumnSegment::append`; the lifetime `'a` ties the format to the source
/// vector so no data is copied.
pub struct UnifiedVectorFormat<'a> {
    /// Optional selection vector; `None` means a contiguous identity mapping.
    pub sel: Option<&'a SelectionVector>,
    /// Raw payload bytes (layout depends on logical type).
    pub data: &'a [u8],
    /// Per-row validity (NULL) bitmask.
    pub validity: &'a ValidityMask,
}

impl<'a> UnifiedVectorFormat<'a> {
    /// Construct a `UnifiedVectorFormat` directly from a flat `Vector`.
    ///
    /// This is the Rust equivalent of `Vector::ToUnifiedFormat(count, vdata)`
    /// for flat (non-dictionary, non-constant) vectors.  Dictionary and
    /// constant vectors should be flattened first via `Vector::flatten`.
    ///
    /// C++:
    /// ```cpp
    /// UnifiedVectorFormat vdata;
    /// chunk.data[i].ToUnifiedFormat(append_count, vdata);
    /// ```
    pub fn from_flat_vector(vector: &'a Vector) -> Self {
        UnifiedVectorFormat {
            sel: None, // identity selection — rows are contiguous
            data: vector.raw_data(),
            validity: &vector.validity,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnSegmentType
// ─────────────────────────────────────────────────────────────────────────────

/// Whether the segment's data lives in memory or on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnSegmentType {
    /// In-memory segment; not yet persisted.  Supports appends.
    Transient,
    /// On-disk segment.  Read-only; backed by a `BlockHandle`.
    Persistent,
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnSegment
// ─────────────────────────────────────────────────────────────────────────────

/// One contiguous run of column data stored in a single buffer block.
///
/// Mirrors `class ColumnSegment : public SegmentBase<ColumnSegment>`.
///
/// # Key fields
/// | C++ | Rust | Notes |
/// |-----|------|-------|
/// | `SegmentBase::count` | `count: AtomicU64` | Implements `SegmentBase` |
/// | `LogicalType type` | `logical_type` | Value type stored |
/// | `ColumnSegmentType segment_type` | `segment_type` | Transient/Persistent |
/// | `shared_ptr<BlockHandle> block` | `buffer: Mutex<Vec<u8>>` | In-memory data buffer |
/// | `block_id_t block_id` | `block_id` | Persistent only |
/// | `idx_t offset` | `block_offset` | Byte offset inside block |
/// | `idx_t segment_size` | `segment_size` | Max bytes on this segment |
/// | `CompressionFunction &function` | `compression: CompressionType` | Codec |
/// | `SegmentStatistics stats` | `stats: SegmentStatistics` | Min/max/null counts |
/// | `unique_ptr<CompressedSegmentState>` | `segment_state: Option<Box<dyn ...>>` | Codec state |
pub struct ColumnSegment {
    // ── SegmentBase ────────────────────────────────────────────
    /// Number of rows stored in this segment.
    count: AtomicU64,

    // ── Identity ───────────────────────────────────────────────
    /// The DuckDB logical type of each row in this segment.
    pub logical_type: LogicalType,

    /// In-memory size of a single value (bytes).
    /// 0 for variable-width types (not yet supported).
    pub type_size: Idx,

    /// Transient = in-memory; Persistent = on-disk.
    pub segment_type: ColumnSegmentType,

    // ── Buffer / disk location ─────────────────────────────────
    /// In-memory data buffer for transient segments.
    ///
    /// For transient segments this holds `segment_size` bytes of raw data.
    /// For persistent segments this is empty (data lives on disk / buffer pool).
    ///
    /// Wrapped in `Mutex` for interior mutability: `append` takes `&self`
    /// because the segment is held behind `Arc` in the segment tree.
    ///
    /// Replaces C++ `BufferHandle` / `BlockHandle` for the in-memory case.
    pub buffer: Mutex<Vec<u8>>,

    /// Block id for persistent segments; `INVALID_BLOCK` for transient ones.
    pub block_id: BlockId,

    /// Byte offset into the block where this segment's data begins.
    pub block_offset: Idx,

    /// Maximum number of bytes allocated for this segment on its block.
    pub segment_size: Idx,

    // ── Statistics ────────────────────────────────────────────
    /// Per-segment zone-map statistics (min/max/null counts).
    ///
    /// Wrapped in `Mutex` so `append` and `finalize_append` can update stats
    /// via `&self`, matching the C++ model where `state.current->GetNode()`
    /// returns a non-owning mutable reference while the segment is Arc-shared.
    pub stats: Mutex<SegmentStatistics>,

    // ── Compression ───────────────────────────────────────────
    pub compression: CompressionType,

    /// Optional codec-specific state (scan pointer, dictionary, etc.).
    ///
    /// Also `Mutex`-wrapped for the same interior-mutability reason as `stats`.
    pub segment_state: Mutex<Option<Box<dyn SegmentState>>>,

    // ── BlockPool handle (persistent segments only) ────────────
    /// For persistent segments: the BlockPool handle that owns the on-disk block
    /// backing this segment.  `None` for transient (in-memory) segments.
    ///
    /// Mirrors C++ `shared_ptr<BlockHandle> block` in `ColumnSegment`.
    /// Pinned via `BlockHandle::load()` inside `initialize_scan`; the resulting
    /// `BufferHandle` is stored in `ColumnScanState::pinned_buffer` (RAII unpin).
    pub block_handle: Option<Arc<BlockHandle>>,
}

impl ColumnSegment {
    // ── Constructors ─────────────────────────────────────────

    /// Creates a new transient (in-memory) segment.
    ///
    /// Pre-allocates `segment_size` bytes in `buffer`.
    /// For validity columns (BIT type), the buffer is initialized to 0xFF (all valid).
    /// For other types, the buffer is zeroed.
    pub fn create_transient(
        logical_type: LogicalType,
        segment_size: Idx,
        compression: CompressionType,
    ) -> Self {
        let type_size = logical_type.physical_size() as Idx;
        // For validity columns, initialize buffer to 0xFF (all bits set = all valid)
        // This matches DuckDB's ValidityInitSegment behavior
        let buffer = if logical_type.id == crate::common::types::LogicalTypeId::Validity {
            vec![0xFFu8; segment_size as usize]
        } else {
            vec![0u8; segment_size as usize]
        };
        ColumnSegment {
            count: AtomicU64::new(0),
            logical_type,
            type_size,
            segment_type: ColumnSegmentType::Transient,
            buffer: Mutex::new(buffer),
            block_id: super::types::INVALID_BLOCK,
            block_offset: 0,
            segment_size,
            stats: Mutex::new(SegmentStatistics::default()),
            compression,
            segment_state: Mutex::new(None),
            block_handle: None,
        }
    }

    /// Creates a persistent segment from an on-disk block (no BlockPool handle).
    ///
    /// The `buffer` is empty; this variant does **not** wire up a `BlockHandle`
    /// so scanning will not work.  Prefer `create_persistent_with_handle` for
    /// segments that will actually be scanned.
    pub fn create_persistent(
        logical_type: LogicalType,
        block_id: BlockId,
        offset: Idx,
        count: Idx,
        segment_size: Idx,
        compression: CompressionType,
        stats: SegmentStatistics,
    ) -> Self {
        let type_size = logical_type.physical_size() as Idx;
        ColumnSegment {
            count: AtomicU64::new(count),
            logical_type,
            type_size,
            segment_type: ColumnSegmentType::Persistent,
            buffer: Mutex::new(Vec::new()),
            block_id,
            block_offset: offset,
            segment_size,
            stats: Mutex::new(stats),
            compression,
            segment_state: Mutex::new(None),
            block_handle: None,
        }
    }

    /// Creates a persistent segment backed by a BufferPool-managed `BlockHandle`.
    ///
    /// This is the correct way to create scannable persistent segments:
    /// - Data is **not** copied into `buffer`; it stays in the block pool.
    /// - `initialize_scan` will pin the block via `BlockHandle::load()`.
    /// - `scan_vector_internal` / `scan_partial_internal` read from the pinned
    ///   payload slice; the block is unpinned when `ColumnScanState` is dropped
    ///   or a new segment is initialized.
    ///
    /// Mirrors the C++ path where `ColumnSegment` holds `shared_ptr<BlockHandle>`.
    ///
    /// # Parameters
    /// - `offset` — byte offset into the block's payload where this segment's
    ///   data starts (C++: `block_pointer.offset`).
    /// - `count` — number of rows in this segment.
    /// - `segment_size` — byte size of this segment's data on the block.
    pub fn create_persistent_with_handle(
        logical_type: LogicalType,
        block_id: BlockId,
        offset: Idx,
        count: Idx,
        segment_size: Idx,
        compression: CompressionType,
        stats: SegmentStatistics,
        block_handle: Arc<BlockHandle>,
    ) -> Self {
        let type_size = logical_type.physical_size() as Idx;
        ColumnSegment {
            count: AtomicU64::new(count),
            logical_type,
            type_size,
            segment_type: ColumnSegmentType::Persistent,
            buffer: Mutex::new(Vec::new()), // empty; data lives in the block pool
            block_id,
            block_offset: offset,
            segment_size,
            stats: Mutex::new(stats),
            compression,
            segment_state: Mutex::new(None),
            block_handle: Some(block_handle),
        }
    }

    // ── Accessors ────────────────────────────────────────────

    pub fn segment_size(&self) -> Idx {
        self.segment_size
    }

    pub fn block_offset(&self) -> Idx {
        self.block_offset
    }

    pub fn is_persistent(&self) -> bool {
        self.segment_type == ColumnSegmentType::Persistent
    }

    pub fn set_count(&self, count: Idx) {
        self.count.store(count, Ordering::Relaxed);
    }

    // ── Scan ─────────────────────────────────────────────────

    /// Prepare codec scan state for this segment.
    ///
    /// C++: `ColumnSegment::InitializeScan(ColumnScanState &state)`
    /// → For uncompressed types: `FixedSizeInitScan` pins the block into a
    ///   `FixedSizeScanState { BufferHandle handle }`.
    ///
    /// Rust equivalent:
    /// - **Transient**: buffer is a `Vec<u8>` already in memory; nothing to pin.
    /// - **Persistent**: pin the block via `BlockHandle::load()` and store the
    ///   resulting `BufferHandle` in `state.pinned_buffer` (RAII unpin on drop).
    pub fn initialize_scan(&self, state: &mut ColumnScanState) {
        match self.segment_type {
            ColumnSegmentType::Transient => {
                // Buffer already in Vec<u8>; nothing to pin.
                state.pinned_buffer = None;
            }
            ColumnSegmentType::Persistent => {
                // Pin through BufferManager so the memory reservation and
                // reader lifecycle match DuckDB's block pool behavior.
                state.pinned_buffer =
                    self.block_handle.as_ref().map(|handle: &Arc<BlockHandle>| {
                        handle.block_manager.buffer_manager().pin(handle.clone())
                    });
            }
        }
    }

    /// Scan `scan_count` rows from this segment into `result` at `result_offset`.
    ///
    /// Dispatches to `scan_vector_internal` (full vector) or
    /// `scan_partial_internal` (partial / flat).
    ///
    /// Mirrors `ColumnSegment::Scan(ColumnScanState&, idx_t, Vector&, idx_t, ScanVectorType)`.
    pub fn scan(
        &self,
        state: &ColumnScanState,
        scan_count: Idx,
        result: &mut Vector,
        result_offset: Idx,
        scan_type: ScanVectorType,
    ) {
        match scan_type {
            ScanVectorType::ScanEntireVector => {
                debug_assert_eq!(result_offset, 0);
                self.scan_vector_internal(state, scan_count, result);
            }
            ScanVectorType::ScanFlatVector => {
                self.scan_partial_internal(state, scan_count, result, result_offset);
            }
        }
    }

    /// Full-vector uncompressed scan.
    ///
    /// Mirrors `FixedSizeScan<T>` in `fixed_size_uncompressed.cpp`.
    ///
    /// - **Transient**: copies `scan_count * type_size` bytes from `self.buffer`.
    /// - **Persistent**: copies from `state.pinned_buffer` (the pinned block
    ///   payload), starting at `self.block_offset + position_in_segment * type_size`.
    fn scan_vector_internal(&self, state: &ColumnScanState, scan_count: Idx, result: &mut Vector) {
        let type_size = self.type_size as usize;
        if type_size == 0 {
            return;
        }

        let start = state.position_in_segment() as usize;
        let len = scan_count as usize * type_size;

        result.vector_type = VectorType::Flat;

        match self.segment_type {
            ColumnSegmentType::Transient => {
                let buf = self.buffer.lock();
                let src = &buf[start * type_size..start * type_size + len];
                result.raw_data_mut()[..len].copy_from_slice(src);
            }
            ColumnSegmentType::Persistent => {
                // Read from the BufferPool-managed block.
                // block_offset is the byte offset into the block's payload where
                // this segment's data starts.
                let dst = result.raw_data_mut();
                let read_ok = state.pinned_buffer.as_ref().and_then(
                    |handle: &crate::storage::buffer::BufferHandle| {
                        handle.with_data(|block_data| {
                            let src_start = self.block_offset as usize + start * type_size;
                            dst[..len].copy_from_slice(&block_data[src_start..src_start + len]);
                        })
                    },
                );
                if read_ok.is_none() {
                    // Defensive: block not pinned (initialize_scan was not called or
                    // block_handle is missing). Zero-fill so callers get defined data.
                    result.raw_data_mut()[..len].fill(0);
                }
            }
        }
    }

    /// Partial flat scan: writes `scan_count` rows at `result_offset` inside a
    /// pre-allocated flat result buffer.
    ///
    /// Mirrors `FixedSizeScanPartial<T>` in `fixed_size_uncompressed.cpp`.
    fn scan_partial_internal(
        &self,
        state: &ColumnScanState,
        scan_count: Idx,
        result: &mut Vector,
        result_offset: Idx,
    ) {
        let type_size = self.type_size as usize;
        if type_size == 0 {
            return;
        }

        let start = state.position_in_segment() as usize;
        let src_row_off = start * type_size;
        let dst_off = result_offset as usize * type_size;
        let len = scan_count as usize * type_size;

        result.vector_type = VectorType::Flat;

        match self.segment_type {
            ColumnSegmentType::Transient => {
                let buf = self.buffer.lock();
                let dst = result.raw_data_mut();
                dst[dst_off..dst_off + len].copy_from_slice(&buf[src_row_off..src_row_off + len]);
            }
            ColumnSegmentType::Persistent => {
                let dst = result.raw_data_mut();
                let read_ok = state.pinned_buffer.as_ref().and_then(
                    |handle: &crate::storage::buffer::BufferHandle| {
                        handle.with_data(|block_data| {
                            let src_start = self.block_offset as usize + src_row_off;
                            dst[dst_off..dst_off + len]
                                .copy_from_slice(&block_data[src_start..src_start + len]);
                        })
                    },
                );
                if read_ok.is_none() {
                    result.raw_data_mut()[dst_off..dst_off + len].fill(0);
                }
            }
        }
    }

    /// Advance the codec scan cursor without producing output.
    ///
    /// C++: `ColumnSegment::Skip(ColumnScanState &state)`
    /// → For uncompressed: `UncompressedFunctions::EmptySkip` (no-op).
    /// The caller (`ColumnDataContext::skip`) then sets
    /// `state.internal_index = state.offset_in_column`.
    pub fn skip(&self, _state: &mut ColumnScanState) {
        // Uncompressed: no codec state to advance.
    }

    // ── Append ───────────────────────────────────────────────

    /// Initialize internal state for appending rows to this segment.
    ///
    /// Takes `&self` instead of `&mut self` because `ColumnSegment` is
    /// typically held behind `Arc` during append; codec state is wrapped in
    /// `Mutex` for interior mutability.
    ///
    /// Only valid on transient segments.
    ///
    /// C++: `ColumnSegment::InitializeAppend(ColumnAppendState &state)`
    /// → `FixedSizeInitAppend`: pins the block buffer, stores handle in
    ///   `state.append_state`.  In Rust `buffer` is already allocated.
    pub fn initialize_append(&self, _state: &mut ColumnAppendState) {
        debug_assert_eq!(
            self.segment_type,
            ColumnSegmentType::Transient,
            "initialize_append called on a persistent segment"
        );
        // Uncompressed: buffer already allocated; nothing else to do.
    }

    /// Append `append_count` rows from `vdata` starting at logical `offset`.
    ///
    /// Returns the number of rows **actually** written (may be less than
    /// `append_count` if the segment's buffer is full).
    ///
    /// Takes `&self` for the same interior-mutability reason as
    /// `initialize_append`; stats are updated via their respective `Mutex` guard.
    ///
    /// C++: `idx_t ColumnSegment::Append(ColumnAppendState&, UnifiedVectorFormat&, idx_t offset, idx_t count)`
    /// → dispatches to `function.get().append(...)` → `FixedSizeAppend<T, StandardFixedSizeAppend>`.
    pub fn append(
        &self,
        _state: &mut ColumnAppendState,
        vdata: &UnifiedVectorFormat<'_>,
        offset: Idx,
        append_count: Idx,
    ) -> Idx {
        match self.compression {
            CompressionType::Uncompressed => self.fixed_size_append(vdata, offset, append_count),
            other => panic!(
                "compression {:?} not yet implemented for ColumnSegment::append",
                other
            ),
        }
    }

    /// Uncompressed fixed-size append logic.
    ///
    /// Mirrors `FixedSizeAppend<T, StandardFixedSizeAppend>` in
    /// `fixed_size_uncompressed.cpp`.
    ///
    /// # Algorithm
    /// ```text
    /// max_tuple_count = segment_size / type_size
    /// copy_count      = min(append_count, max_tuple_count - self.count)
    /// for i in 0..copy_count:
    ///   src = vdata.sel.map(|s| s.get_index(offset + i)).unwrap_or(offset + i)
    ///   dst = self.count + i
    ///   if valid: buf[dst * type_size ..] = vdata.data[src * type_size ..]
    ///   else:     buf[dst * type_size ..] = 0   (NullValue<T>)
    /// self.count += copy_count
    /// return copy_count
    /// ```
    fn fixed_size_append(
        &self,
        vdata: &UnifiedVectorFormat<'_>,
        offset: Idx,
        append_count: Idx,
    ) -> Idx {
        let type_size = self.type_size as usize;
        if type_size == 0 {
            return 0;
        }

        let current_count = self.count() as usize;
        let max_count = self.segment_size as usize / type_size;
        let copy_count = (append_count as usize).min(max_count.saturating_sub(current_count));
        if copy_count == 0 {
            return 0;
        }

        let mut buf = self.buffer.lock();
        let mut stats = self.stats.lock();
        let all_valid = vdata.validity.is_all_valid();

        for i in 0..copy_count {
            let source_idx = vdata
                .sel
                .map(|s| s.get_index(offset as usize + i))
                .unwrap_or(offset as usize + i);
            let target_idx = current_count + i;
            let src_byte = source_idx * type_size;
            let dst_byte = target_idx * type_size;

            if all_valid || vdata.validity.row_is_valid(source_idx) {
                stats.set_has_no_null();
                if src_byte + type_size <= vdata.data.len() {
                    buf[dst_byte..dst_byte + type_size]
                        .copy_from_slice(&vdata.data[src_byte..src_byte + type_size]);
                }
            } else {
                stats.set_has_null();
                buf[dst_byte..dst_byte + type_size].fill(0); // NullValue<T>() = 0
            }
        }

        self.count.fetch_add(copy_count as u64, Ordering::Relaxed);
        copy_count as Idx
    }

    /// Finalize the append phase; flush codec state and return bytes used.
    ///
    /// C++: `idx_t ColumnSegment::FinalizeAppend(ColumnAppendState &state)`
    /// → `FixedSizeFinalizeAppend<T>`: returns `segment.count * sizeof(T)`.
    pub fn finalize_append(&self, _state: &mut ColumnAppendState) -> Idx {
        self.count() * self.type_size
    }

    /// Revert appended rows back to `new_count`.
    ///
    /// C++: `void ColumnSegment::RevertAppend(idx_t start_row)`
    pub fn revert_append(&self, new_count: Idx) {
        self.count.store(new_count, Ordering::SeqCst);
    }

    // ── Persistence ──────────────────────────────────────────

    /// Promote this transient segment to a persistent one backed by `block_id`.
    pub fn convert_to_persistent(&mut self, block_id: BlockId) {
        debug_assert_eq!(self.segment_type, ColumnSegmentType::Transient);
        self.block_id = block_id;
        self.segment_type = ColumnSegmentType::Persistent;
    }
}

impl SegmentBase for ColumnSegment {
    fn count_atomic(&self) -> &AtomicU64 {
        &self.count
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Supporting types
// ─────────────────────────────────────────────────────────────────────────────

// 使用统一的统计信息模块
pub use crate::storage::statistics::SegmentStatistics;

/// Marker trait for codec-specific segment state (e.g. dictionary, bitpacking
/// metadata).  Mirrors `CompressedSegmentState`.
pub trait SegmentState: Send + Sync + std::fmt::Debug {}
