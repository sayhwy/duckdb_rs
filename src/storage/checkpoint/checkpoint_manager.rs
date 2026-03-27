// ============================================================
// checkpoint_manager.rs — Checkpoint 管理器
// 对应 C++: duckdb/storage/checkpoint_manager.hpp/.cpp
// ============================================================
//
// Checkpoint 流程：
// 1. 创建 MetadataWriter 用于写入 catalog
// 2. 遍历所有 catalog entries（schema, table, view 等）
// 3. 对每个表，写入表数据和表元数据
// 4. 序列化 catalog 到 metadata
// 5. 更新 DatabaseHeader 的 meta_block 指针

use std::sync::Arc;

use crate::common::types::LogicalTypeId;
use crate::catalog::TableCatalogEntry;
use crate::storage::buffer::BlockManager;
use crate::storage::data_table::DataTable;
use crate::storage::metadata::{MetaBlockPointer, MetadataManager, MetadataWriter};
use crate::storage::storage_info::{DatabaseHeader, INVALID_BLOCK};
use crate::storage::checkpoint::catalog_serializer;
use crate::storage::checkpoint::binary_serializer::BinarySerializer;
use crate::storage::metadata::WriteStream;
use crate::storage::table::column_data::ColumnData;
use crate::storage::table::column_segment::ColumnSegmentType;
use crate::storage::table::row_group::RowGroupPointer;
use crate::storage::table::segment_base::SegmentBase;
use crate::storage::table::types::{CompressionType, Idx, STANDARD_VECTOR_SIZE};

/// Checkpoint 管理器
pub struct CheckpointManager {
    block_manager: Arc<dyn BlockManager>,
    metadata_manager: Arc<MetadataManager>,
}

/// 表信息
pub struct TableInfo {
    pub entry: Arc<TableCatalogEntry>,
    pub storage: Arc<DataTable>,
}

impl CheckpointManager {
    pub fn new(
        block_manager: Arc<dyn BlockManager>,
        metadata_manager: Arc<MetadataManager>,
    ) -> Self {
        Self {
            block_manager,
            metadata_manager,
        }
    }

    /// 创建 checkpoint
    ///
    /// 返回新的 DatabaseHeader
    pub fn create_checkpoint(
        &self,
        tables: &[TableInfo],
    ) -> DatabaseHeader {
        // 1. 创建 metadata writer
        let mut metadata_writer = MetadataWriter::new(&self.metadata_manager, None);
        let mut table_metadata_writer = MetadataWriter::new(&self.metadata_manager, None);

        // 2. 获取第一个 meta block 指针（catalog 根）
        let meta_block = metadata_writer.get_meta_block_pointer();

        // 3. 收集所有 catalog entries 和它们的存储信息
        let mut table_data: Vec<(&TableCatalogEntry, Option<MetaBlockPointer>, u64)> = Vec::new();

        for table_info in tables {
            let total_rows = table_info.storage.get_total_rows();
            let table_pointer = self.write_table_data(&mut table_metadata_writer, &table_info.storage);
            table_data.push((&table_info.entry, table_pointer, total_rows));
        }

        // 4. 序列化 catalog
        catalog_serializer::write_catalog(&mut metadata_writer, &table_data);

        // 5. Flush metadata writers
        metadata_writer.flush();
        table_metadata_writer.flush();

        // 6. Flush metadata blocks to disk
        self.metadata_manager.flush();

        // 7. 返回新的 header
        let header = DatabaseHeader {
            iteration: 0,
            meta_block: meta_block.block_pointer as i64,
            free_list: INVALID_BLOCK,
            block_count: self.block_manager.block_count(),
            block_alloc_size: self.block_manager.get_block_alloc_size() as u64,
            vector_size: 2048, // STANDARD_VECTOR_SIZE
            serialization_compatibility: 7, // LATEST_SERIALIZATION_VERSION
        };

        header
    }

    /// 写入表数据
    fn write_table_data(
        &self,
        writer: &mut MetadataWriter<'_>,
        table: &DataTable,
    ) -> Option<MetaBlockPointer> {
        let total_rows = table.get_total_rows();
        if total_rows == 0 {
            return None;
        }

        // Step 1: Checkpoint all row groups - this writes column data to disk
        let mut row_group_pointers = Vec::new();
        let mut idx = 0i64;
        while let Some(row_group) = table.row_groups.get_row_group(idx) {
            if row_group.count() > 0 {
                let pointer = self.write_row_group(writer, &row_group);
                row_group_pointers.push(pointer);
            }
            idx += 1;
        }

        // Step 2: Get table_pointer AFTER writing column data
        let table_pointer = writer.get_meta_block_pointer();

        // Step 3: Write TableStatistics
        write_minimal_table_statistics(writer, table.columns(), total_rows);

        // Step 4: Write row_group_count (uint64)
        writer.write_u64(row_group_pointers.len() as u64);

        // Step 5: Write each RowGroupPointer wrapped in BinarySerializer
        for pointer in row_group_pointers.iter() {
            let mut serializer = BinarySerializer::new(writer as &mut dyn WriteStream);
            serializer.begin_root_object();
            write_row_group_pointer(&mut serializer, pointer);
            serializer.end_object();
        }

        Some(table_pointer)
    }

    fn write_row_group(
        &self,
        writer: &mut MetadataWriter<'_>,
        row_group: &std::sync::Arc<crate::storage::table::row_group::RowGroup>,
    ) -> RowGroupPointer {
        let mut column_pointers = Vec::new();
        let col_count = row_group.column_pointers.len();
        for col_idx in 0..col_count {
            let column = row_group.get_column(col_idx);
            column_pointers.push(self.write_column(writer, &column));
        }

        RowGroupPointer {
            row_start: row_group.row_start(),
            tuple_count: row_group.count(),
            deletes_pointers: Vec::new(),
            column_pointers,
        }
    }

    fn write_column(
        &self,
        writer: &mut MetadataWriter<'_>,
        column: &std::sync::Arc<ColumnData>,
    ) -> MetaBlockPointer {
        let pointer = writer.get_meta_block_pointer();
        let data_pointers = self.write_column_segments(column);

        let mut serializer = BinarySerializer::new(writer as &mut dyn WriteStream);

        serializer.begin_root_object();

        // Field 100: data_pointers for main column
        serializer.begin_list(100, data_pointers.len());
        for data_pointer in &data_pointers {
            serializer.list_write_object(|s| {
                write_data_pointer(s, data_pointer.tuple_count, data_pointer.block_id, data_pointer.offset);
            });
        }
        serializer.end_list();

        // Field 101: validity child column (complete PersistentColumnData)
        // Get validity column based on column type
        let validity_column = match &column.kind {
            crate::storage::table::column_data::ColumnKindData::Standard { validity } => validity.as_ref(),
            crate::storage::table::column_data::ColumnKindData::List { validity, .. } => Some(validity),
            crate::storage::table::column_data::ColumnKindData::Array { validity, .. } => Some(validity),
            crate::storage::table::column_data::ColumnKindData::Struct { validity, .. } => Some(validity),
            crate::storage::table::column_data::ColumnKindData::Variant { validity, .. } => validity.as_ref(),
            crate::storage::table::column_data::ColumnKindData::Validity => None,
        };

        if let Some(validity_col) = validity_column {
            let validity_pointers = self.write_column_segments(validity_col);

            serializer.write_field_id(101);
            // Write validity as nested PersistentColumnData
            // Use simplified DataPointer format for validity (no complex statistics)
            serializer.begin_list(100, validity_pointers.len());
            for data_pointer in &validity_pointers {
                serializer.list_write_object(|s| {
                    write_data_pointer_simple(s, data_pointer.tuple_count, data_pointer.block_id, data_pointer.offset);
                });
            }
            serializer.end_list();
            // Write terminator for validity PersistentColumnData
            serializer.write_terminator();
        }

        // OnObjectEnd for parent PersistentColumnData
        serializer.end_object();
        pointer
    }

    fn write_column_segments(
        &self,
        column: &std::sync::Arc<ColumnData>,
    ) -> Vec<crate::storage::table::types::DataPointer> {
        use crate::common::types::LogicalTypeId;

        let mut result = Vec::new();
        column.ctx.for_each_segment(|segment, row_start| {
            let tuple_count = segment.count();
            if tuple_count == 0 {
                return;
            }

            // Calculate the actual byte size for this segment
            // For validity columns (BIT type), use bitmask format
            let is_validity = segment.logical_type.id == LogicalTypeId::Validity;

            match segment.segment_type {
                ColumnSegmentType::Persistent => {
                    result.push(crate::storage::table::types::DataPointer {
                        block_id: segment.block_id,
                        offset: segment.block_offset as u32,
                        row_start,
                        tuple_count,
                    });
                }
                ColumnSegmentType::Transient => {
                    let block_id = self.block_manager.get_free_block_id_for_checkpoint();
                    let mut block = self.block_manager.create_block(block_id, None);

                    if is_validity {
                        // Validity mask format:
                        // - Each vector (STANDARD_VECTOR_SIZE rows) uses 32 bytes (STANDARD_MASK_SIZE)
                        // - All bits set to 1 means all valid (0xFF bytes)
                        // - We compute the size per DuckDB's ValidityFinalizeAppend:
                        //   ((count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE) * STANDARD_MASK_SIZE
                        let vector_count = (tuple_count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
                        let used_bytes = vector_count as usize * 32; // STANDARD_MASK_SIZE = 32

                        // Write validity mask - for now assume all valid (0xFF)
                        // This matches DuckDB's ValidityInitSegment behavior
                        for byte in block.payload_mut()[..used_bytes].iter_mut() {
                            *byte = 0xFF;
                        }
                        self.block_manager.write_block(&block, block_id);
                        result.push(crate::storage::table::types::DataPointer {
                            block_id,
                            offset: 0,
                            row_start,
                            tuple_count,
                        });
                    } else {
                        let used_bytes = tuple_count as usize * segment.type_size as usize;
                        {
                            let buffer = segment.buffer.lock();
                            block.payload_mut()[..used_bytes].copy_from_slice(&buffer[..used_bytes]);
                        }
                        self.block_manager.write_block(&block, block_id);
                        result.push(crate::storage::table::types::DataPointer {
                            block_id,
                            offset: 0,
                            row_start,
                            tuple_count,
                        });
                    }
                }
            }
        });
        result
    }
}

// Simplified DataPointer serialization for validity columns
fn write_data_pointer_simple(
    serializer: &mut BinarySerializer<'_>,
    tuple_count: Idx,
    block_id: i64,
    offset: u32,
) {
    serializer.write_varint(101, tuple_count);
    serializer.begin_object(102);
    serializer.write_signed_varint(100, block_id);
    serializer.write_varint(101, offset as u64);
    serializer.end_object();
    serializer.write_u8(103, compression_tag(CompressionType::Uncompressed));
    // Simplified statistics for validity - write minimal BaseStatistics without DistinctStatistics
    serializer.begin_object(104);
    serializer.write_bool(100, false);  // has_null
    serializer.write_bool(101, true);   // has_no_null (validity columns typically have no nulls)
    serializer.write_varint(102, 0);    // null_count
    // Field 103: DistinctStatistics - write empty/default
    serializer.begin_object(103);
    serializer.end_object();
    serializer.end_object();
}

fn write_minimal_table_statistics(
    writer: &mut MetadataWriter<'_>,
    columns: &[crate::storage::data_table::ColumnDefinition],
    total_rows: Idx,
) {
    let mut serializer = BinarySerializer::new(writer as &mut dyn WriteStream);

    // Field 100: column_stats (list of ColumnStatistics)
    serializer.begin_list(100, columns.len());
    for column in columns {
        serializer.list_write_nullable_object(true, |s| {
            // Field 100: BaseStatistics
            s.write_field_id(100);
            write_minimal_base_statistics(s, &column.logical_type, total_rows);
            s.end_object();
        });
    }
    serializer.end_list();

    // Field 101: table_sample (optional BlockingSample)
    serializer.write_nullable_field(101, false);

    serializer.end_object();
}

fn write_empty_distinct_statistics(
    serializer: &mut BinarySerializer<'_>,
    logical_type: &crate::common::types::LogicalType,
) {
    match logical_type.id {
        LogicalTypeId::Boolean
        | LogicalTypeId::TinyInt
        | LogicalTypeId::SmallInt
        | LogicalTypeId::Integer
        | LogicalTypeId::BigInt
        | LogicalTypeId::Float
        | LogicalTypeId::Double
        | LogicalTypeId::Varchar => {
            // ColumnStatistics::Serialize writes:
            // - Field 101: distinct (DistinctStatistics, nullable)
            // DistinctStatistics::Serialize writes:
            // - Field 100: sample_count
            // - Field 101: total_count
            // - Field 102: log (HyperLogLog)
            //
            // For now, skip writing DistinctStatistics entirely.
            // This will write null for the distinct_stats field.
            // If needed, we can add proper HyperLogLog serialization later.
        }
        _ => {}
    }
}

fn write_minimal_base_statistics(
    serializer: &mut BinarySerializer<'_>,
    logical_type: &crate::common::types::LogicalType,
    total_rows: Idx,
) {
    serializer.write_bool(100, false);
    serializer.write_bool(101, total_rows > 0);
    serializer.write_varint(102, 0);
    serializer.begin_object(103);
    match logical_type.id {
        LogicalTypeId::Boolean
        | LogicalTypeId::TinyInt
        | LogicalTypeId::SmallInt
        | LogicalTypeId::Integer
        | LogicalTypeId::BigInt
        | LogicalTypeId::Float
        | LogicalTypeId::Double => {
            serializer.begin_object(200);
            serializer.write_bool(100, false);
            serializer.end_object();
            serializer.begin_object(201);
            serializer.write_bool(100, false);
            serializer.end_object();
        }
        LogicalTypeId::Varchar => {
            serializer.write_string(200, "");
            serializer.write_string(201, "");
            serializer.write_bool(202, false);
            serializer.write_bool(203, true);
            serializer.write_varint(204, 0);
        }
        _ => {}
    }
    serializer.end_object();
}

fn write_row_group_pointer(serializer: &mut BinarySerializer<'_>, pointer: &RowGroupPointer) {
    serializer.write_varint(100, pointer.row_start);
    serializer.write_varint(101, pointer.tuple_count);
    serializer.begin_list(102, pointer.column_pointers.len());
    for column_pointer in &pointer.column_pointers {
        // Write MetaBlockPointer - only write non-default fields
        // WritePropertyWithDefault for idx_t/uint32_t only writes if non-zero
        serializer.list_write_object(|s| {
            if column_pointer.block_pointer != 0 {
                s.write_varint(100, column_pointer.block_pointer);
            }
            if column_pointer.offset != 0 {
                s.write_varint(101, column_pointer.offset as u64);
            }
        });
    }
    serializer.end_list();
    serializer.begin_list(103, 0);
    serializer.end_list();
}

fn write_data_pointer(
    serializer: &mut BinarySerializer<'_>,
    tuple_count: Idx,
    block_id: i64,
    offset: u32,
) {
    serializer.write_varint(101, tuple_count);
    serializer.begin_object(102);
    serializer.write_signed_varint(100, block_id);
    serializer.write_varint(101, offset as u64);
    serializer.end_object();
    serializer.write_u8(103, compression_tag(CompressionType::Uncompressed));
    serializer.begin_object(104);
    serializer.write_bool(100, false);
    serializer.write_bool(101, true);
    serializer.write_varint(102, 0);
    serializer.begin_object(103);
    serializer.begin_object(200);
    serializer.write_bool(100, false);
    serializer.end_object();
    serializer.begin_object(201);
    serializer.write_bool(100, false);
    serializer.end_object();
    serializer.end_object();
    serializer.end_object();
}

fn compression_tag(compression: CompressionType) -> u8 {
    match compression {
        CompressionType::Uncompressed => 1,
        CompressionType::Constant => 2,
        CompressionType::Rle => 3,
        CompressionType::BitPacking => 4,
        CompressionType::Dictionary => 5,
        CompressionType::Fsst => 6,
        CompressionType::Chimp => 7,
        CompressionType::Patas => 8,
        CompressionType::Alprd => 9,
        CompressionType::ZStd => 10,
        CompressionType::Auto => 0,
    }
}
