use crate::catalog::{CreateTableInfo, LogicalType as CatalogLogicalType, LogicalTypeId};
use crate::common::serializer::{BinaryMetadataDeserializer, MESSAGE_TERMINATOR_FIELD_ID};
use crate::storage::metadata::{BlockReaderType, MetaBlockPointer, MetadataReader, ReadStream};
use crate::storage::table::persistent_table_data::{PersistentStorageRuntime, PersistentTableData};
use crate::storage::table::row_group::RowGroupPointer;
use crate::storage::table::table_statistics::TableStatistics;
use std::sync::Arc;

pub struct BoundCreateTableInfo {
    pub base: CreateTableInfo,
    pub data: Option<Box<PersistentTableData>>,
}

impl BoundCreateTableInfo {
    pub fn new(base: CreateTableInfo) -> Self {
        Self { base, data: None }
    }
}

pub struct TableDataReader<'a, 'mgr> {
    pub reader: &'a mut MetadataReader<'mgr>,
    pub info: &'a mut BoundCreateTableInfo,
}

impl<'a, 'mgr> TableDataReader<'a, 'mgr> {
    pub fn new(
        reader: &'a mut MetadataReader<'mgr>,
        info: &'a mut BoundCreateTableInfo,
        table_pointer: MetaBlockPointer,
        runtime: Option<Arc<PersistentStorageRuntime>>,
    ) -> Self {
        let mut data = PersistentTableData::new(info.base.columns.columns.len());
        data.base_table_pointer = table_pointer;
        data.runtime = runtime;
        info.data = Some(Box::new(data));
        Self { reader, info }
    }

    pub fn read_table_data(&mut self) {
        if let Err(e) = self.read_table_data_inner() {
            eprintln!("[table_data_reader] ERROR reading table data: {}", e);
        }
    }

    fn read_table_data_inner(&mut self) -> std::io::Result<()> {
        let runtime = self
            .info
            .data
            .as_ref()
            .and_then(|persistent| persistent.runtime.as_ref().cloned());
        let data = self.info.data.as_mut().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "TableDataReader::new must initialize PersistentTableData",
            )
        })?;

        // Read TableStatistics
        {
            let mut deserializer = BinaryMetadataDeserializer::new(self.reader);
            let storage_types: Vec<crate::common::types::LogicalType> = self
                .info
                .base
                .columns
                .columns
                .iter()
                .map(|col| catalog_type_to_storage_type(&col.logical_type))
                .collect();
            data.table_stats =
                TableStatistics::deserialize_checkpoint(&mut deserializer, &storage_types)
                    .unwrap_or_else(|e| {
                        panic!(
                            "failed to deserialize table statistics for table {}.{}: {}",
                            self.info.base.base.schema, self.info.base.table, e
                        )
                    });
        }

        // DuckDB 先只读取 row_group_count，然后记录当前 MetadataReader 的位置。
        // 后续 RowGroupCollection::Initialize 会基于这个 block_pointer 继续反序列化 row groups。
        let mut bytes = [0u8; 8];
        self.reader.read_data(&mut bytes);
        data.row_group_count = u64::from_le_bytes(bytes);
        data.block_pointer = self.reader.get_meta_block_pointer();
        data.row_group_pointers.clear();

        // 当前 Rust 实现还没有完整接入 DuckDB 的 RowGroupSegmentTree 懒加载器，
        // 这里保持与 DuckDB 一致的 block_pointer 语义，同时在单独的 reader 上预读取
        // row group pointers，避免破坏当前 scan 路径。
        if data.row_group_count > 0 {
            if let Some(ref runtime) = runtime {
                data.row_group_pointers =
                    read_row_group_pointers(&runtime, data.block_pointer, data.row_group_count)?;
            }
        }
        Ok(())
    }
}

fn read_row_group_pointers(
    runtime: &Arc<PersistentStorageRuntime>,
    block_pointer: MetaBlockPointer,
    row_group_count: u64,
) -> std::io::Result<Vec<RowGroupPointer>> {
    let mut reader = MetadataReader::new(
        &runtime.metadata_manager,
        block_pointer,
        None,
        BlockReaderType::RegisterBlocks,
    );
    let mut deserializer = BinaryMetadataDeserializer::new(&mut reader);
    let mut row_group_pointers = Vec::with_capacity(row_group_count as usize);
    for i in 0..row_group_count {
        let pointer = deserializer.read_row_group_pointer().map_err(|e| {
            std::io::Error::new(e.kind(), format!("read_row_group_pointer[{i}]: {e}"))
        })?;
        row_group_pointers.push(pointer);
    }
    Ok(row_group_pointers)
}

fn catalog_type_to_storage_type(
    logical_type: &CatalogLogicalType,
) -> crate::common::types::LogicalType {
    use crate::common::types::{LogicalType as StorageLogicalType, LogicalTypeId as StorageTypeId};
    match logical_type.id {
        LogicalTypeId::Boolean => StorageLogicalType::boolean(),
        LogicalTypeId::TinyInt => StorageLogicalType::tinyint(),
        LogicalTypeId::SmallInt => StorageLogicalType::smallint(),
        LogicalTypeId::Integer => StorageLogicalType::integer(),
        LogicalTypeId::BigInt => StorageLogicalType::bigint(),
        LogicalTypeId::Float => StorageLogicalType::float(),
        LogicalTypeId::Double => StorageLogicalType::double(),
        LogicalTypeId::Varchar => StorageLogicalType::varchar(),
        LogicalTypeId::Date => StorageLogicalType::date(),
        LogicalTypeId::Timestamp => StorageLogicalType::new(StorageTypeId::Timestamp),
        _ => StorageLogicalType::new(StorageTypeId::Invalid),
    }
}

fn skip_table_statistics(
    de: &mut BinaryMetadataDeserializer<'_>,
    column_types: &[LogicalTypeId],
) -> std::io::Result<()> {
    // Field 100: column_stats (list of ColumnStatistics)
    let field = de.next_field()?;
    if field != 100 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("expected TableStatistics field 100, got {}", field),
        ));
    }

    let count = de.read_list_len()?;
    let to_read = count.min(column_types.len());
    for idx in 0..to_read {
        let logical_type = column_types.get(idx).unwrap_or(&LogicalTypeId::Integer);
        skip_column_statistics(de, logical_type)?;
    }

    // Field 101: table_sample (optional BlockingSample)
    let field = de.next_field()?;
    if field != 101 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "expected TableStatistics field 101 (table_sample), got {}",
                field
            ),
        ));
    }

    let present = de.read_u8();
    if present != 0 {
        skip_blocking_sample(de)?;
    }

    Ok(())
}

fn skip_column_statistics(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalTypeId,
) -> std::io::Result<()> {
    let present = de.read_u8();
    if present == 0 {
        return Ok(());
    }
    loop {
        let field = de.next_field()?;
        match field {
            100 => skip_base_statistics(de, logical_type)?,
            101 => {
                // DistinctStatistics is nullable - read present flag first
                let distinct_present = de.read_u8();
                if distinct_present != 0 {
                    skip_distinct_statistics(de)?;
                }
            }
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected ColumnStatistics field {other}"),
                ));
            }
        }
    }
}

fn skip_base_statistics(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalTypeId,
) -> std::io::Result<()> {
    loop {
        let field = de.next_field()?;
        match field {
            100 | 101 => {
                let _ = de.read_u8();
            }
            102 => {
                let _ = de.read_varint()?;
            }
            103 => skip_type_statistics(de, logical_type)?,
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected BaseStatistics field {other}"),
                ));
            }
        }
    }
}

fn skip_type_statistics(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalTypeId,
) -> std::io::Result<()> {
    loop {
        match de.next_field()? {
            200 | 201 => skip_numeric_stat_value(de, logical_type)?,
            202 | 203 => {
                let _ = de.read_u8();
            }
            204 => {
                let _ = de.read_varint()?;
            }
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected type statistics field {other}"),
                ));
            }
        }
    }
}

fn skip_numeric_stat_value(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalTypeId,
) -> std::io::Result<()> {
    let mut has_value = false;
    loop {
        match de.next_field()? {
            100 => {
                has_value = de.read_u8() != 0;
            }
            101 => {
                if has_value {
                    skip_numeric_value(de, logical_type)?;
                }
            }
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected numeric stats value field {other}"),
                ));
            }
        }
    }
}

fn skip_numeric_value(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalTypeId,
) -> std::io::Result<()> {
    match logical_type {
        LogicalTypeId::Float => {
            de.skip_bytes(4);
        }
        LogicalTypeId::Double => {
            de.skip_bytes(8);
        }
        _ => {
            let _ = de.read_varint()?;
        }
    }
    Ok(())
}

fn skip_distinct_statistics(de: &mut BinaryMetadataDeserializer<'_>) -> std::io::Result<()> {
    loop {
        match de.next_field()? {
            100 | 101 => {
                let _ = de.read_varint()?;
            }
            102 => {
                if de.read_u8() != 0 {
                    skip_hyperloglog(de)?;
                }
            }
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected DistinctStatistics field {other}"),
                ));
            }
        }
    }
}

fn skip_hyperloglog(de: &mut BinaryMetadataDeserializer<'_>) -> std::io::Result<()> {
    const HLL_V1_DENSE_SIZE: usize = 17 + (((1 << 12) * 6 + 7) / 8);
    let mut storage_type = 2u64;
    loop {
        match de.next_field()? {
            100 => {
                storage_type = de.read_varint()?;
            }
            101 => match storage_type {
                2 => de.skip_sized_bytes(64)?,
                1 => de.skip_sized_bytes(HLL_V1_DENSE_SIZE)?,
                other => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown HyperLogLog storage type {other}"),
                    ));
                }
            },
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected HyperLogLog field {other}"),
                ));
            }
        }
    }
}

fn skip_optional_blocking_sample(de: &mut BinaryMetadataDeserializer<'_>) -> std::io::Result<()> {
    if de.read_u8() == 0 {
        return Ok(());
    }
    skip_blocking_sample(de)
}

fn skip_blocking_sample(de: &mut BinaryMetadataDeserializer<'_>) -> std::io::Result<()> {
    let mut sample_type = 0u64;
    loop {
        match de.next_field()? {
            100 => skip_optional_base_reservoir_sampling(de)?,
            101 => sample_type = de.read_varint()?,
            102 => {
                let _ = de.read_u8();
            }
            200 => {
                if sample_type == 2 {
                    de.skip_bytes(8);
                } else {
                    let _ = de.read_varint()?;
                }
            }
            201 => {
                if sample_type == 2 {
                    let _ = de.read_varint()?;
                } else {
                    skip_optional_nested_object(de)?;
                }
            }
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected BlockingSample field {other}"),
                ));
            }
        }
    }
}

fn skip_optional_base_reservoir_sampling(
    de: &mut BinaryMetadataDeserializer<'_>,
) -> std::io::Result<()> {
    if de.read_u8() == 0 {
        return Ok(());
    }
    loop {
        match de.next_field()? {
            100 | 102 | 103 | 104 => {
                let _ = de.read_varint()?;
            }
            101 => de.skip_bytes(8),
            105 => skip_priority_queue_pairs(de)?,
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected BaseReservoirSampling field {other}"),
                ));
            }
        }
    }
}

fn skip_priority_queue_pairs(de: &mut BinaryMetadataDeserializer<'_>) -> std::io::Result<()> {
    let count = de.read_list_len()?;
    for _ in 0..count {
        loop {
            match de.next_field()? {
                0 => de.skip_bytes(8),
                1 => {
                    let _ = de.read_varint()?;
                }
                MESSAGE_TERMINATOR_FIELD_ID => break,
                other => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unexpected reservoir weight pair field {other}"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn skip_optional_nested_object(de: &mut BinaryMetadataDeserializer<'_>) -> std::io::Result<()> {
    if de.read_u8() == 0 {
        return Ok(());
    }
    let mut depth = 1usize;
    while depth > 0 {
        match de.next_field()? {
            MESSAGE_TERMINATOR_FIELD_ID => depth -= 1,
            _ => {
                let _ = de.read_varint()?;
            }
        }
    }
    Ok(())
}
