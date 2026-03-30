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

use crate::catalog::TableCatalogEntry;
use crate::common::types::LogicalTypeId;
use crate::storage::buffer::BlockManager;
use crate::storage::checkpoint::binary_serializer::BinarySerializer;
use crate::storage::checkpoint::catalog_serializer;
use crate::storage::data_table::DataTable;
use crate::storage::metadata::WriteStream;
use crate::storage::metadata::{MetaBlockPointer, MetadataManager, MetadataWriter};
use crate::storage::storage_info::{DatabaseHeader, INVALID_BLOCK, StorageResult};
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
    pub fn create_checkpoint(&self, tables: &[TableInfo]) -> DatabaseHeader {
        self.create_checkpoint_with_meta(tables, |_| Ok(()))
            .expect("checkpoint creation without WAL hook should not fail")
    }

    pub fn create_checkpoint_with_meta<F>(
        &self,
        tables: &[TableInfo],
        mut on_meta_block: F,
    ) -> StorageResult<DatabaseHeader>
    where
        F: FnMut(MetaBlockPointer) -> StorageResult<()>,
    {
        // 1. ?? metadata writer
        let mut metadata_writer = MetadataWriter::new(&self.metadata_manager, None);
        let mut table_metadata_writer = MetadataWriter::new(&self.metadata_manager, None);

        // 2. ????? meta block ???catalog ??
        let meta_block = metadata_writer.get_meta_block_pointer();
        on_meta_block(meta_block)?;

        // 3. ???? catalog entries ????????
        let mut table_data: Vec<(&TableCatalogEntry, Option<MetaBlockPointer>, u64)> = Vec::new();

        for table_info in tables {
            let total_rows = table_info.storage.get_total_rows();
            let table_pointer =
                self.write_table_data(&mut table_metadata_writer, &table_info.storage);
            table_data.push((&table_info.entry, table_pointer, total_rows));
        }

        // 4. ??? catalog
        catalog_serializer::write_catalog(&mut metadata_writer, &table_data);

        // 5. Flush metadata writers
        metadata_writer.flush();
        table_metadata_writer.flush();

        // 6. Flush metadata blocks to disk
        self.metadata_manager.flush();

        // 7. ???? header
        let header = DatabaseHeader {
            iteration: 0,
            meta_block: meta_block.block_pointer as i64,
            free_list: INVALID_BLOCK,
            block_count: self.block_manager.block_count(),
            block_alloc_size: self.block_manager.get_block_alloc_size() as u64,
            vector_size: 2048,
            serialization_compatibility: 7,
        };

        Ok(header)
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
        use crate::common::types::LogicalTypeId;

        let pointer = writer.get_meta_block_pointer();
        let data_pointers = self.write_column_segments(column);
        let col_type = &column.ctx.logical_type;

        let mut serializer = BinarySerializer::new(writer as &mut dyn WriteStream);

        serializer.begin_root_object();

        // Field 100: data_pointers for main column
        serializer.begin_list(100, data_pointers.len());
        for data_pointer in &data_pointers {
            serializer.list_write_object(|s| {
                if col_type.id == LogicalTypeId::Varchar {
                    write_data_pointer_varchar(
                        s,
                        data_pointer.tuple_count,
                        data_pointer.block_id,
                        data_pointer.offset,
                    );
                } else {
                    write_data_pointer(
                        s,
                        data_pointer.tuple_count,
                        data_pointer.block_id,
                        data_pointer.offset,
                    );
                }
            });
        }
        serializer.end_list();

        // Field 101: validity child column (complete PersistentColumnData)
        // Get validity column based on column type
        let validity_column = match &column.kind {
            crate::storage::table::column_data::ColumnKindData::Standard { validity } => {
                validity.as_ref()
            }
            crate::storage::table::column_data::ColumnKindData::List { validity, .. } => {
                Some(validity)
            }
            crate::storage::table::column_data::ColumnKindData::Array { validity, .. } => {
                Some(validity)
            }
            crate::storage::table::column_data::ColumnKindData::Struct { validity, .. } => {
                Some(validity)
            }
            crate::storage::table::column_data::ColumnKindData::Variant { validity, .. } => {
                validity.as_ref()
            }
            crate::storage::table::column_data::ColumnKindData::Validity => None,
        };

        if validity_column.is_some() {
            // The in-memory validity-column implementation is not checkpoint-compatible yet.
            // Persist it as DuckDB's COMPRESSION_EMPTY validity segment instead, which means
            // "all rows valid" to the official reader.
            write_empty_validity_column(&mut serializer, column.count());
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

                    let is_varchar = segment.logical_type.id == LogicalTypeId::Varchar;

                    if is_varchar {
                        // VARCHAR 列使用字符串字典格式（对应 DuckDB UncompressedStringStorage）
                        //
                        // 块布局（匹配 UncompressedStringStorage 扫描逻辑）:
                        //   [0..4]             uint32_t dict_size  （字符串数据字节总数）
                        //   [4..8]             uint32_t dict_end   （默认=block_alloc_size）
                        //   [8..8+n*4]         int32_t[n] offsets  （逐行累计长度）
                        //   [..]                字符串数据（通过 dict_pos=dict_end-dict_offset 定位）
                        let block_alloc = self.block_manager.get_block_alloc_size() as usize;
                        let n = tuple_count as usize;
                        let buf = segment.buffer.lock();

                        // 从 string_t (16字节) 提取字符串内容
                        let mut str_data: Vec<Vec<u8>> = Vec::with_capacity(n);
                        for i in 0..n {
                            let base = i * 16;
                            let len = u32::from_le_bytes(
                                buf[base..base + 4].try_into().unwrap(),
                            ) as usize;
                            if len <= 12 {
                                let bytes = buf[base + 4..base + 4 + len].to_vec();
                                str_data.push(bytes);
                            } else {
                                // 超出 inline 长度，暂不支持，写入空串
                                str_data.push(Vec::new());
                            }
                        }
                        drop(buf);

                        let dict_size: usize = str_data.iter().map(|s| s.len()).sum();
                        let offset_section = 8 + n * 4;

                        let payload = block.payload_mut();
                        // Compact dictionary so that:
                        //   dict_end = offset_section + dict_size
                        // i.e. dictionary bytes start right after the offsets array.
                        let total_size = offset_section + dict_size;
                        let dict_end = total_size;
                        assert!(
                            dict_end <= payload.len(),
                            "VARCHAR segment too large for one block"
                        );

                        // dict_size
                        payload[0..4].copy_from_slice(&(dict_size as u32).to_le_bytes());
                        // dict_end points to the end of the dictionary region.
                        payload[4..8].copy_from_slice(&(dict_end as u32).to_le_bytes());

                        // 写入累计偏移量
                        let mut cumulative = 0u32;
                        for (i, s) in str_data.iter().enumerate() {
                            cumulative += s.len() as u32;
                            let pos = 8 + i * 4;
                            payload[pos..pos + 4]
                                .copy_from_slice(&(cumulative as i32).to_le_bytes());
                            if !s.is_empty() {
                                // Place the string exactly like DuckDB:
                                // dict_pos = dict_end - dict_offset (where dict_offset == cumulative).
                                let dict_pos = dict_end - cumulative as usize;
                                payload[dict_pos..dict_pos + s.len()].copy_from_slice(s);
                            }
                        }

                        self.block_manager.write_block(&block, block_id);
                        result.push(crate::storage::table::types::DataPointer {
                            block_id,
                            offset: 0,
                            row_start,
                            tuple_count,
                        });
                    } else if is_validity {
                        // Validity mask format:
                        // - Each vector (STANDARD_VECTOR_SIZE rows) uses 32 bytes (STANDARD_MASK_SIZE)
                        // - All bits set to 1 means all valid (0xFF bytes)
                        // - We compute the size per DuckDB's ValidityFinalizeAppend:
                        //   ((count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE) * STANDARD_MASK_SIZE
                        let vector_count =
                            (tuple_count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
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
                            block.payload_mut()[..used_bytes]
                                .copy_from_slice(&buffer[..used_bytes]);
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

fn write_empty_validity_column(serializer: &mut BinarySerializer<'_>, tuple_count: Idx) {
    serializer.begin_object(101);
    serializer.begin_list(100, 1);
    serializer.list_write_object(|s| {
        // DuckDB internal enum value: CompressionType::COMPRESSION_EMPTY = 14.
        write_data_pointer_with_compression(s, tuple_count, -1, 0, 14, true, false);
    });
    serializer.end_list();
    serializer.end_object();
}

fn write_data_pointer_with_compression(
    serializer: &mut BinarySerializer<'_>,
    tuple_count: Idx,
    block_id: i64,
    offset: u32,
    compression_tag: u8,
    has_no_null: bool,
    has_null: bool,
) {
    serializer.write_varint(101, tuple_count);
    serializer.begin_object(102);
    serializer.write_signed_varint(100, block_id);
    serializer.write_varint(101, offset as u64);
    serializer.end_object();
    serializer.write_u8(103, compression_tag);
    serializer.begin_object(104);
    serializer.write_bool(100, has_null);
    serializer.write_bool(101, has_no_null);
    serializer.write_varint(102, if has_null { tuple_count } else { 0 });
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
        | LogicalTypeId::Double
        | LogicalTypeId::Date => {
            serializer.begin_object(200);
            serializer.write_bool(100, false);
            serializer.end_object();
            serializer.begin_object(201);
            serializer.write_bool(100, false);
            serializer.end_object();
        }
        LogicalTypeId::Varchar => {
            // StringStats: min/max 均为 8 字节 blob（MAX_STRING_MINMAX_SIZE = 8）
            serializer.write_bytes(200, &[0u8; 8]);   // min
            serializer.write_bytes(201, &[0xFFu8; 8]); // max
            serializer.write_bool(202, false);  // has_unicode
            serializer.write_bool(203, true);   // has_max_string_length
            serializer.write_varint(204, 0);    // max_string_length
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

/// VARCHAR 列的数据指针写入（使用 StringStats 格式代替 NumericStats）。
///
/// 对应 DuckDB `DataPointer::Serialize` + `StringStats::Serialize`。
///
/// # StringStats 格式
/// DuckDB 的 `StringStats::Serialize` 用 `WriteProperty(id, name, data_ptr, 8)` 写入
/// min/max，其中 `8 = StringStatsData::MAX_STRING_MINMAX_SIZE`。
/// `BinaryDeserializer::ReadDataPtr` 会校验 varint 长度是否等于期望字节数，因此
/// 必须写入精确 8 字节的 blob，而非普通 string。
fn write_data_pointer_varchar(
    serializer: &mut BinarySerializer<'_>,
    tuple_count: Idx,
    block_id: i64,
    offset: u32,
) {
    // StringStatsData::MAX_STRING_MINMAX_SIZE = 8
    const STR_STATS_SIZE: usize = 8;

    serializer.write_varint(101, tuple_count);
    serializer.begin_object(102);
    serializer.write_signed_varint(100, block_id);
    serializer.write_varint(101, offset as u64);
    serializer.end_object();
    serializer.write_u8(103, compression_tag(CompressionType::Uncompressed));
    serializer.begin_object(104);
    serializer.write_bool(100, false);  // has_null
    serializer.write_bool(101, true);   // has_no_null
    serializer.write_varint(102, 0);    // distinct_count
    serializer.begin_object(103);       // type_specific stats (StringStats)
    // Field 200: min — 8 字节 blob（全 0 表示最小值为空串）
    serializer.write_bytes(200, &[0u8; STR_STATS_SIZE]);
    // Field 201: max — 8 字节 blob（全 0xFF 表示最大值为最高字节序）
    serializer.write_bytes(201, &[0xFFu8; STR_STATS_SIZE]);
    serializer.write_bool(202, false);  // has_unicode
    serializer.write_bool(203, true);   // has_max_string_length
    serializer.write_varint(204, 0);    // max_string_length
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
