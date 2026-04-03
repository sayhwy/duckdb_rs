use crate::catalog::{CreateTableInfo, LogicalType as CatalogLogicalType, LogicalTypeId};
use crate::common::serializer::{
    BinaryMetadataDeserializer, MESSAGE_TERMINATOR_FIELD_ID,
};
use crate::storage::metadata::{MetaBlockPointer, MetadataReader, ReadStream};
use crate::storage::table::persistent_table_data::PersistentTableData;
use crate::storage::table::table_statistics::TableStatistics;

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
    ) -> Self {
        let mut data = PersistentTableData::new(info.base.columns.columns.len());
        data.base_table_pointer = table_pointer;
        info.data = Some(Box::new(data));
        Self { reader, info }
    }

    pub fn read_table_data(&mut self) {
        if let Err(e) = self.read_table_data_inner() {
            eprintln!("[table_data_reader] ERROR reading table data: {}", e);
        }
    }

    fn read_table_data_inner(&mut self) -> std::io::Result<()> {
        let data = self
            .info
            .data
            .as_mut()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData,
                "TableDataReader::new must initialize PersistentTableData"))?;

        // Read TableStatistics
        {
            let column_type_ids: Vec<LogicalTypeId> = self
                .info
                .base
                .columns
                .columns
                .iter()
                .map(|col| col.logical_type.id.clone())
                .collect();
            let mut deserializer = BinaryMetadataDeserializer::new(self.reader);
            skip_table_statistics(&mut deserializer, &column_type_ids)
                .map_err(|e| std::io::Error::new(e.kind(),
                    format!("skip_table_statistics: {}", e)))?;
            data.table_stats = TableStatistics::new();
        }

        // Read row_group_count
        let mut bytes = [0u8; 8];
        self.reader.read_data(&mut bytes);
        data.row_group_count = u64::from_le_bytes(bytes);
        data.block_pointer = data.base_table_pointer;
        data.row_group_pointers.clear();

        // Read RowGroupPointers
        let mut deserializer = BinaryMetadataDeserializer::new(self.reader);
        for i in 0..data.row_group_count {
            let pointer = deserializer
                .read_row_group_pointer()
                .map_err(|e| std::io::Error::new(e.kind(),
                    format!("read_row_group_pointer[{}]: {}", i, e)))?;
            data.row_group_pointers.push(pointer);
        }
        Ok(())
    }
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
    loop {
        match de.next_field()? {
            100 => {
                let _ = de.read_varint()?;
            }
            101 => {
                let len = de.read_varint()? as usize;
                de.skip_bytes(len);
            }
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
