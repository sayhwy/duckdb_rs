//! Optimistic data writer for pre-emptive row group flushing.
//!
//! Mirrors `duckdb/storage/optimistic_data_writer.hpp` and
//! `duckdb/src/storage/optimistic_data_writer.cpp`.
//!
//! # Responsibilities
//!
//! `OptimisticDataWriter` manages the pre-emptive writing of row groups to disk
//! during large INSERT operations. Instead of buffering all data in memory until
//! commit, it flushes complete row groups to disk incrementally, reducing memory
//! pressure and improving performance for bulk inserts.
//!
//! # Key Components
//!
//! - `OptimisticWriteCollection`: Holds a `RowGroupCollection` and tracks which
//!   row groups have been flushed to disk.
//! - `OptimisticDataWriter`: Coordinates the flushing process, managing partial
//!   block allocation and deciding when to write row groups.
//!
//! # Design Notes
//!
//! The writer checks a threshold (`WriteBufferRowGroupCountSetting`, default 2)
//! to determine when to flush. When the number of unflushed complete row groups
//! exceeds this threshold, they are written to disk using the partial block manager.

use std::sync::Arc;
use parking_lot::Mutex;

use crate::storage::data_table::{ClientContext, ColumnDefinition, DataTable};
use crate::storage::table::column_checkpoint_state::PartialBlockManager;
use crate::storage::table::row_group::RowGroup;
use crate::storage::table::row_group_collection::RowGroupCollection;
use crate::storage::table::data_table_info::DataTableInfo;
use crate::storage::table::types::{CompressionType, Idx, LogicalType};

// ─────────────────────────────────────────────────────────────────────────────
// OptimisticWritePartialManagers
// ─────────────────────────────────────────────────────────────────────────────

/// Strategy for allocating partial block managers.
///
/// Mirrors `enum class OptimisticWritePartialManagers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimisticWritePartialManagers {
    /// One partial block manager per column (default).
    PerColumn,
    /// Single global partial block manager for all columns.
    Global,
}

// ─────────────────────────────────────────────────────────────────────────────
// OptimisticWriteCollection
// ─────────────────────────────────────────────────────────────────────────────

/// A collection of row groups being written optimistically.
///
/// Mirrors `struct OptimisticWriteCollection`.
pub struct OptimisticWriteCollection {
    /// The row group collection being written.
    pub collection: Arc<RowGroupCollection>,

    /// Index of the last row group that was flushed to disk.
    pub last_flushed: Idx,

    /// Total number of complete row groups (excluding the current incomplete one).
    pub complete_row_groups: Idx,

    /// Per-column partial block managers (if using PerColumn strategy).
    pub partial_block_managers: Vec<Box<PartialBlockManager>>,
}

impl OptimisticWriteCollection {
    /// Create a new empty collection.
    pub fn new(collection: Arc<RowGroupCollection>) -> Self {
        Self {
            collection,
            last_flushed: 0,
            complete_row_groups: 0,
            partial_block_managers: Vec::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OptimisticDataWriter
// ─────────────────────────────────────────────────────────────────────────────

/// Manages pre-emptive flushing of row groups to disk during bulk inserts.
///
/// Mirrors `class OptimisticDataWriter`.
pub struct OptimisticDataWriter {
    /// The client context (holds transaction state, settings, etc.).
    context: Arc<Mutex<ClientContext>>,

    /// The table being written to.
    table: Arc<DataTable>,

    /// Global partial block manager for this writer.
    partial_manager: Option<Box<PartialBlockManager>>,
}

impl OptimisticDataWriter {
    // ── Constructors ─────────────────────────────────────────────────────────────

    /// Create a new optimistic writer for the given table.
    ///
    /// Mirrors `OptimisticDataWriter(ClientContext &context, DataTable &table)`.
    pub fn new(context: Arc<Mutex<ClientContext>>, table: Arc<DataTable>) -> Self {
        Self {
            context,
            table,
            partial_manager: None,
        }
    }

    /// Create a child writer that shares context with a parent.
    ///
    /// Mirrors `OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent)`.
    ///
    /// If the parent has a partial manager, it is cleared (blocks are released).
    pub fn new_with_parent(table: Arc<DataTable>, parent: &mut OptimisticDataWriter) -> Self {
        // Clear parent's partial manager blocks
        if let Some(ref mut pm) = parent.partial_manager {
            pm.clear_blocks();
        }

        Self {
            context: Arc::clone(&parent.context),
            table,
            partial_manager: None,
        }
    }

    // ── Public API ───────────────────────────────────────────────────────────────

    /// Create a new collection for optimistic writing.
    ///
    /// Mirrors `CreateCollection(DataTable &storage, const vector<LogicalType> &insert_types,
    ///                           OptimisticWritePartialManagers type)`.
    pub fn create_collection(
        &mut self,
        storage: Arc<DataTable>,
        insert_types: &[LogicalType],
        manager_type: OptimisticWritePartialManagers,
    ) -> OptimisticWriteCollection {
        let table_info = Arc::clone(&storage.info);

        // Create the row group collection with a large max_row_id
        const MAX_ROW_ID: Idx = i64::MAX as Idx;
        let row_groups = RowGroupCollection::new(
            table_info,
            insert_types.to_vec(),
            MAX_ROW_ID,
        );

        let mut collection = OptimisticWriteCollection::new(row_groups);

        // Allocate per-column partial block managers if requested
        if manager_type == OptimisticWritePartialManagers::PerColumn {
            for _ in 0..insert_types.len() {
                // Create a new partial block manager for each column
                // In a full implementation, this would get the block manager from table IO manager
                collection.partial_block_managers.push(Box::new(PartialBlockManager));
            }
        }

        collection
    }

    /// Called when a new complete row group has been written.
    ///
    /// Mirrors `WriteNewRowGroup(OptimisticWriteCollection &row_groups)`.
    ///
    /// Checks if we should flush accumulated row groups to disk based on the
    /// write buffer threshold setting.
    pub fn write_new_row_group(&mut self, row_groups: &mut OptimisticWriteCollection) {
        // Check if we should write to disk
        if !self.prepare_write() {
            return;
        }

        row_groups.complete_row_groups += 1;
        let unflushed_row_groups = row_groups.complete_row_groups - row_groups.last_flushed;

        // Get the threshold from settings (default: 2)
        let threshold = self.get_write_buffer_row_group_count();

        if unflushed_row_groups >= threshold {
            // Flush all unflushed row groups to disk
            let mut to_flush = Vec::new();
            let mut segment_indexes = Vec::new();

            for i in row_groups.last_flushed..row_groups.complete_row_groups {
                let segment_index = i as i64;
                if let Some(rg) = row_groups.collection.get_row_group(segment_index) {
                    to_flush.push(rg);
                    segment_indexes.push(segment_index);
                }
            }

            self.flush_to_disk(row_groups, &to_flush, &segment_indexes);
            row_groups.last_flushed = row_groups.complete_row_groups;
        }
    }

    /// Flush the last (incomplete) row group to disk.
    ///
    /// Mirrors `WriteLastRowGroup(OptimisticWriteCollection &row_groups)`.
    ///
    /// Called at the end of a bulk insert to ensure all data is written.
    pub fn write_last_row_group(&mut self, row_groups: &mut OptimisticWriteCollection) {
        if !self.prepare_write() {
            return;
        }

        // Flush any remaining complete row groups
        let mut to_flush = Vec::new();
        let mut segment_indexes = Vec::new();

        for i in row_groups.last_flushed..row_groups.complete_row_groups {
            let segment_index = i as i64;
            if let Some(rg) = row_groups.collection.get_row_group(segment_index) {
                to_flush.push(rg);
                segment_indexes.push(segment_index);
            }
        }

        // Add the last (incomplete) row group
        if let Some(last_rg) = row_groups.collection.get_row_group(-1) {
            to_flush.push(last_rg);
            segment_indexes.push(-1);
        }

        self.flush_to_disk(row_groups, &to_flush, &segment_indexes);

        // Merge all per-column partial block managers into the global one
        for mut pbm in row_groups.partial_block_managers.drain(..) {
            self.merge_partial_manager(&mut pbm);
        }
    }

    /// Perform final flush of all partial blocks.
    ///
    /// Mirrors `FinalFlush()`.
    ///
    /// Called at transaction commit to ensure all buffered data is written.
    pub fn final_flush(&mut self) {
        if let Some(ref mut pm) = self.partial_manager {
            pm.flush_partial_blocks();
        }
        self.partial_manager = None;
    }

    /// Merge another writer's partial manager into this one.
    ///
    /// Mirrors `Merge(OptimisticDataWriter &other)`.
    pub fn merge(&mut self, other: &mut OptimisticDataWriter) {
        if let Some(mut other_pm) = other.partial_manager.take() {
            self.merge_partial_manager(&mut other_pm);
        }
    }

    /// Merge a partial block manager into this writer's global manager.
    ///
    /// Mirrors `Merge(unique_ptr<PartialBlockManager> &other_manager)`.
    pub fn merge_partial_manager(&mut self, other_manager: &mut Box<PartialBlockManager>) {
        if self.partial_manager.is_none() {
            // Take ownership of the other manager
            self.partial_manager = Some(Box::new(PartialBlockManager));
            // In a full implementation, we would move the actual manager here
            return;
        }

        // Merge the other manager into ours
        if let Some(ref mut pm) = self.partial_manager {
            pm.merge(other_manager);
        }
    }

    /// Rollback all partial writes.
    ///
    /// Mirrors `Rollback()`.
    ///
    /// Called when a transaction is aborted to discard all buffered data.
    pub fn rollback(&mut self) {
        if let Some(ref mut pm) = self.partial_manager {
            pm.rollback();
        }
        self.partial_manager = None;
    }

    /// Get the client context.
    ///
    /// Mirrors `GetClientContext()`.
    pub fn get_client_context(&self) -> Arc<Mutex<ClientContext>> {
        Arc::clone(&self.context)
    }

    // ── Private Methods ──────────────────────────────────────────────────────────

    /// Check if we should pre-emptively write to disk.
    ///
    /// Mirrors `PrepareWrite()`.
    ///
    /// Returns `false` if the table is temporary or the storage is in-memory.
    fn prepare_write(&mut self) -> bool {
        // Check if table is temporary
        if self.is_temporary() {
            return false;
        }

        // Check if storage is in-memory
        if self.is_in_memory() {
            return false;
        }

        // Allocate partial block manager if not yet allocated
        if self.partial_manager.is_none() {
            // In a full implementation, we would get the block manager from the table
            // and create a proper PartialBlockManager
            self.partial_manager = Some(Box::new(PartialBlockManager));
        }

        true
    }

    /// Flush specific row groups to disk.
    ///
    /// Mirrors `FlushToDisk(OptimisticWriteCollection &collection,
    ///                      const vector<const_reference<RowGroup>> &row_groups,
    ///                      const vector<int64_t> &segment_indexes)`.
    fn flush_to_disk(
        &mut self,
        collection: &mut OptimisticWriteCollection,
        row_groups: &[Arc<RowGroup>],
        segment_indexes: &[i64],
    ) {
        // Get compression types for all columns
        let compression_types: Vec<CompressionType> = self.table
            .column_definitions
            .iter()
            .map(|col| self.get_compression_type(col))
            .collect();

        // Create write info
        let write_info = crate::storage::table::row_group::RowGroupWriteInfo {
            compression_types,
            partial_block_manager: None,
        };

        // Write each row group to disk
        for (i, rg) in row_groups.iter().enumerate() {
            let write_data = rg.write_to_disk(&write_info);

            // Replace the row group in the collection with the checkpointed version
            collection.collection.set_row_group(
                segment_indexes[i],
                write_data.result_row_group,
            );
        }
    }

    /// Get the write buffer row group count threshold from settings.
    ///
    /// Mirrors `DBConfig::GetSetting<WriteBufferRowGroupCountSetting>(context)`.
    ///
    /// Default value is 2 (flush after 2 complete row groups).
    fn get_write_buffer_row_group_count(&self) -> Idx {
        // In a full implementation, this would read from ClientContext settings
        // Default value from DuckDB: 2
        2
    }

    /// Check if the table is temporary.
    fn is_temporary(&self) -> bool {
        // In a full implementation, check table.IsTemporary()
        // For now, assume all tables are persistent
        false
    }

    /// Check if storage is in-memory.
    fn is_in_memory(&self) -> bool {
        // In a full implementation, check StorageManager::Get(table.GetAttached()).InMemory()
        // For now, assume all storage is on-disk
        false
    }

    /// Get compression type for a column.
    fn get_compression_type(&self, _col: &ColumnDefinition) -> CompressionType {
        // In a full implementation, this would return col.CompressionType()
        // For now, return Auto (let the system choose)
        CompressionType::Auto
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PartialBlockManager Extensions
// ─────────────────────────────────────────────────────────────────────────────

impl PartialBlockManager {
    /// Clear all blocks in this manager.
    ///
    /// Mirrors `ClearBlocks()`.
    pub fn clear_blocks(&mut self) {
        // In a full implementation, this would release all allocated blocks
    }

    /// Flush all partial blocks to disk.
    ///
    /// Mirrors `FlushPartialBlocks()`.
    pub fn flush_partial_blocks(&mut self) {
        // In a full implementation, this would write all buffered data to disk
    }

    /// Merge another partial block manager into this one.
    ///
    /// Mirrors `Merge(PartialBlockManager &other)`.
    pub fn merge(&mut self, _other: &mut Box<PartialBlockManager>) {
        // In a full implementation, this would merge the blocks from other into self
    }

    /// Rollback all partial writes.
    ///
    /// Mirrors `Rollback()`.
    pub fn rollback(&mut self) {
        // In a full implementation, this would discard all buffered data
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RowGroupCollection Extensions
// ─────────────────────────────────────────────────────────────────────────────

impl RowGroupCollection {
    /// Replace a row group at the given index.
    ///
    /// Mirrors `SetRowGroup(int64_t segment_index, shared_ptr<RowGroup> row_group)`.
    pub fn set_row_group(&self, _index: i64, _row_group: Arc<RowGroup>) {
        // In a full implementation, this would update the segment tree
    }
}
