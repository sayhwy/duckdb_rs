// ============================================================
// checkpoint_manager.rs 鈥?Checkpoint 绠＄悊鍣?
// 瀵瑰簲 C++: duckdb/storage/checkpoint_manager.hpp/.cpp
// ============================================================
//
// Checkpoint 娴佺▼锛?
// 1. 鍒涘缓 MetadataWriter 鐢ㄤ簬鍐欏叆 catalog
// 2. 閬嶅巻鎵€鏈?catalog entries锛坰chema, table, view 绛夛級
// 3. 瀵规瘡涓〃锛屽啓鍏ヨ〃鏁版嵁鍜岃〃鍏冩暟鎹?
// 4. 搴忓垪鍖?catalog 鍒?metadata
// 5. 鏇存柊 DatabaseHeader 鐨?meta_block 鎸囬拡

use std::sync::Arc;

use crate::catalog::{ColumnDefinition, LogicalType, TableCatalogEntry};
use crate::common::errors::StorageResult;
use crate::common::serializer::BinarySerializer;
use crate::storage::buffer::BlockManager;
use crate::storage::data_table::DataTable;
use crate::storage::metadata::WriteStream;
use crate::storage::metadata::{MetaBlockPointer, MetadataManager, MetadataWriter};
use crate::storage::serialization as storage_serialization;
use crate::storage::storage_info::{DatabaseHeader, INVALID_BLOCK};
use crate::storage::table::column_data::ColumnData;
use crate::storage::table::column_segment::ColumnSegmentType;
use crate::storage::table::row_group::RowGroupPointer;
use crate::storage::table::segment_base::SegmentBase;
use crate::storage::table::types::{CompressionType, Idx, STANDARD_VECTOR_SIZE};

/// Checkpoint 绠＄悊鍣?
pub struct CheckpointManager {
    block_manager: Arc<dyn BlockManager>,
    metadata_manager: Arc<MetadataManager>,
}

/// 琛ㄤ俊鎭?
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

    /// 鍒涘缓 checkpoint
    ///
    /// 杩斿洖鏂扮殑 DatabaseHeader
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
        write_catalog(&mut metadata_writer, &table_data);

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

    /// 鍐欏叆琛ㄦ暟鎹?
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
        storage_serialization::write_minimal_table_statistics(writer, table.columns(), total_rows);

        // Step 4: Write row_group_count (uint64)
        writer.write_u64(row_group_pointers.len() as u64);

        // Step 5: Write each RowGroupPointer wrapped in BinarySerializer
        for pointer in row_group_pointers.iter() {
            let mut serializer = BinarySerializer::new(writer as &mut dyn WriteStream);
            serializer.begin_root_object();
            storage_serialization::write_row_group_pointer(&mut serializer, pointer);
            serializer.end_object();
        }

        // DuckDB forces subsequent appends to start a fresh row group after a
        // checkpoint so new writes do not continue inside persisted row groups.
        if table.info.indexes.is_empty() {
            table.row_groups.set_append_requires_new_row_group();
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
                    storage_serialization::write_data_pointer_varchar(
                        s,
                        data_pointer.tuple_count,
                        data_pointer.block_id,
                        data_pointer.offset,
                    );
                } else {
                    storage_serialization::write_data_pointer(
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
                        // VARCHAR 鍒椾娇鐢ㄥ瓧绗︿覆瀛楀吀鏍煎紡锛堝搴?DuckDB UncompressedStringStorage锛?
                        //
                        // 鍧楀竷灞€锛堝尮閰?UncompressedStringStorage 鎵弿閫昏緫锛?
                        //   [0..4]             uint32_t dict_size  锛堝瓧绗︿覆鏁版嵁瀛楄妭鎬绘暟锛?
                        //   [4..8]             uint32_t dict_end   锛堥粯璁?block_alloc_size锛?
                        //   [8..8+n*4]         int32_t[n] offsets  锛堥€愯绱闀垮害锛?
                        //   [..]                瀛楃涓叉暟鎹紙閫氳繃 dict_pos=dict_end-dict_offset 瀹氫綅锛?
                        let block_alloc = self.block_manager.get_block_alloc_size() as usize;
                        let n = tuple_count as usize;
                        let buf = segment.buffer.lock();

                        // 浠?string_t (16瀛楄妭) 鎻愬彇瀛楃涓插唴瀹?
                        let mut str_data: Vec<Vec<u8>> = Vec::with_capacity(n);
                        for i in 0..n {
                            let base = i * 16;
                            let len = u32::from_le_bytes(buf[base..base + 4].try_into().unwrap())
                                as usize;
                            if len <= 12 {
                                let bytes = buf[base + 4..base + 4 + len].to_vec();
                                str_data.push(bytes);
                            } else {
                                // 瓒呭嚭 inline 闀垮害锛屾殏涓嶆敮鎸侊紝鍐欏叆绌轰覆
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

                        // 鍐欏叆绱鍋忕Щ閲?
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

mod catalog_type {
    pub const TABLE_ENTRY: u8 = 1;
    pub const SCHEMA_ENTRY: u8 = 2;
}

mod logical_type_tag {
    pub const BOOLEAN: u8 = 10;
    pub const TINYINT: u8 = 11;
    pub const SMALLINT: u8 = 12;
    pub const INTEGER: u8 = 13;
    pub const BIGINT: u8 = 14;
    pub const DATE: u8 = 15;
    pub const TIME: u8 = 16;
    pub const TIMESTAMP: u8 = 19;
    pub const DECIMAL: u8 = 21;
    pub const FLOAT: u8 = 22;
    pub const DOUBLE: u8 = 23;
    pub const VARCHAR: u8 = 25;
    pub const BLOB: u8 = 26;
}

mod table_column_type {
    pub const STANDARD: u8 = 0;
    pub const GENERATED: u8 = 1;
}

mod on_create_conflict {
    pub const ERROR_ON_CONFLICT: u8 = 0;
}

fn write_catalog<W: WriteStream>(
    stream: &mut W,
    entries: &[(&TableCatalogEntry, Option<MetaBlockPointer>, u64)],
) {
    let mut serializer = BinarySerializer::new(stream);
    let mut schemas: Vec<(String, String)> = Vec::new();
    for (entry, _, _) in entries {
        let schema_name = entry.base.parent_schema().to_string();
        let catalog_name = entry.base.parent_catalog().to_string();
        if !schemas.iter().any(|(schema, _)| schema == &schema_name) {
            schemas.push((schema_name, catalog_name));
        }
    }

    serializer.begin_list(100, schemas.len() + entries.len());
    for (schema_name, catalog_name) in &schemas {
        serializer.list_write_object(|s| write_schema_entry(s, catalog_name, schema_name));
    }
    for (entry, table_pointer, total_rows) in entries {
        serializer
            .list_write_object(|s| write_catalog_entry(s, entry, *table_pointer, *total_rows));
    }
    serializer.end_list();
    serializer.end_object();
}

fn write_schema_entry(
    serializer: &mut BinarySerializer<'_>,
    catalog_name: &str,
    schema_name: &str,
) {
    serializer.write_u8(99, catalog_type::SCHEMA_ENTRY);
    serializer.begin_nullable_object(100);
    write_create_schema_info(serializer, catalog_name, schema_name);
    serializer.end_object();
    serializer.end_nullable_object();
}

fn write_create_schema_info(
    serializer: &mut BinarySerializer<'_>,
    catalog_name: &str,
    schema_name: &str,
) {
    serializer.begin_root_object();
    serializer.write_u8(100, catalog_type::SCHEMA_ENTRY);
    let _ = catalog_name;
    if !schema_name.is_empty() {
        serializer.write_string(102, schema_name);
    }
    serializer.write_u8(105, on_create_conflict::ERROR_ON_CONFLICT);
}

fn write_catalog_entry(
    serializer: &mut BinarySerializer<'_>,
    entry: &TableCatalogEntry,
    table_pointer: Option<MetaBlockPointer>,
    total_rows: u64,
) {
    serializer.write_u8(99, catalog_type::TABLE_ENTRY);
    write_table(serializer, entry, table_pointer, total_rows);
}

fn write_table(
    serializer: &mut BinarySerializer<'_>,
    entry: &TableCatalogEntry,
    table_pointer: Option<MetaBlockPointer>,
    total_rows: u64,
) {
    serializer.begin_nullable_object(100);
    write_create_table_info(serializer, entry);
    serializer.end_object();
    serializer.end_nullable_object();

    if let Some(pointer) = table_pointer {
        write_table_storage_info(serializer, pointer, total_rows);
    }
}

fn write_create_table_info(serializer: &mut BinarySerializer<'_>, entry: &TableCatalogEntry) {
    serializer.begin_root_object();
    serializer.write_u8(100, catalog_type::TABLE_ENTRY);

    let catalog = entry.base.parent_catalog();
    if !catalog.is_empty() {
        serializer.write_string(101, catalog);
    }

    let schema = entry.base.parent_schema();
    if !schema.is_empty() {
        serializer.write_string(102, schema);
    }

    serializer.write_u8(105, on_create_conflict::ERROR_ON_CONFLICT);
    serializer.write_string(200, &entry.base.fields().name);

    serializer.write_field_id(201);
    write_column_list(serializer, &entry.columns);
    serializer.end_object();
}

fn write_column_list(serializer: &mut BinarySerializer<'_>, columns: &crate::catalog::ColumnList) {
    serializer.begin_list(100, columns.columns.len());
    for column in &columns.columns {
        serializer.list_write_object(|s| write_column_definition(s, column));
    }
    serializer.end_list();
}

fn write_column_definition(serializer: &mut BinarySerializer<'_>, column: &ColumnDefinition) {
    serializer.write_string(100, &column.name);
    serializer.write_field_id(101);
    write_logical_type(serializer, &column.logical_type);
    serializer.write_u8(
        103,
        if column.is_generated() {
            table_column_type::GENERATED
        } else {
            table_column_type::STANDARD
        },
    );
    serializer.write_u8(104, 0);
}

fn write_logical_type(serializer: &mut BinarySerializer<'_>, logical_type: &LogicalType) {
    serializer.begin_root_object();
    serializer.write_u8(100, logical_type_id_from(logical_type));
    if logical_type.id == crate::catalog::LogicalTypeId::Decimal {
        serializer.begin_nullable_object(101);
        serializer.write_u8(100, 2);
        serializer.write_u8(200, logical_type.width);
        serializer.write_u8(201, logical_type.scale);
        serializer.end_object();
        serializer.end_nullable_object();
    }
    serializer.end_object();
}

fn logical_type_id_from(logical_type: &LogicalType) -> u8 {
    use crate::catalog::LogicalTypeId;

    match logical_type.id {
        LogicalTypeId::Boolean => logical_type_tag::BOOLEAN,
        LogicalTypeId::TinyInt => logical_type_tag::TINYINT,
        LogicalTypeId::SmallInt => logical_type_tag::SMALLINT,
        LogicalTypeId::Integer => logical_type_tag::INTEGER,
        LogicalTypeId::BigInt => logical_type_tag::BIGINT,
        LogicalTypeId::Float => logical_type_tag::FLOAT,
        LogicalTypeId::Double => logical_type_tag::DOUBLE,
        LogicalTypeId::Varchar => logical_type_tag::VARCHAR,
        LogicalTypeId::Date => logical_type_tag::DATE,
        LogicalTypeId::Time => logical_type_tag::TIME,
        LogicalTypeId::Timestamp => logical_type_tag::TIMESTAMP,
        LogicalTypeId::Blob => logical_type_tag::BLOB,
        LogicalTypeId::Decimal => logical_type_tag::DECIMAL,
        _ => 0,
    }
}

fn write_table_storage_info(
    serializer: &mut BinarySerializer<'_>,
    table_pointer: MetaBlockPointer,
    total_rows: u64,
) {
    serializer.write_field_id(101);
    storage_serialization::write_meta_block_pointer(serializer, &table_pointer);
    serializer.write_terminator();
    serializer.write_varint(102, total_rows);
    serializer.begin_list(103, 0);
    serializer.end_list();
    serializer.begin_list(104, 0);
    serializer.end_list();
}
