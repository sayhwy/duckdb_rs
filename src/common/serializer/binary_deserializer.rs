use std::io;

use crate::common::types::LogicalType;
use crate::storage::metadata::{MetaBlockPointer, ReadStream};
use crate::storage::table::row_group::RowGroupPointer;
use crate::storage::table::types::{CompressionType, DataPointer, INVALID_BLOCK, Idx};

pub const MESSAGE_TERMINATOR_FIELD_ID: u16 = 0xFFFF;

pub struct BinaryMetadataDeserializer<'a> {
    stream: &'a mut dyn ReadStream,
    buffered_field: Option<u16>,
}

impl<'a> BinaryMetadataDeserializer<'a> {
    pub fn new(stream: &'a mut dyn ReadStream) -> Self {
        Self {
            stream,
            buffered_field: None,
        }
    }

    pub fn read_u8(&mut self) -> u8 {
        self.stream.read_u8()
    }

    pub fn read_u16(&mut self) -> u16 {
        let mut buf = [0u8; 2];
        self.stream.read_data(&mut buf);
        u16::from_le_bytes(buf)
    }

    pub fn skip_bytes(&mut self, len: usize) {
        let mut buf = vec![0u8; len];
        self.stream.read_data(&mut buf);
    }

    pub fn read_f64(&mut self) -> f64 {
        let mut buf = [0u8; 8];
        self.stream.read_data(&mut buf);
        f64::from_le_bytes(buf)
    }

    pub fn read_f32(&mut self) -> f32 {
        let mut buf = [0u8; 4];
        self.stream.read_data(&mut buf);
        f32::from_le_bytes(buf)
    }

    pub fn read_bytes(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_varint()? as usize;
        let mut buf = vec![0u8; len];
        self.stream.read_data(&mut buf);
        Ok(buf)
    }

    pub fn read_fixed_bytes(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let actual_len = self.read_varint()? as usize;
        if actual_len != len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected {len} bytes, got {actual_len}"),
            ));
        }
        let mut buf = vec![0u8; len];
        self.stream.read_data(&mut buf);
        Ok(buf)
    }

    pub fn skip_sized_bytes(&mut self, expected_len: usize) -> io::Result<()> {
        let actual_len = self.read_varint()? as usize;
        if actual_len != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected {expected_len} bytes, got {actual_len}"),
            ));
        }
        self.skip_bytes(actual_len);
        Ok(())
    }

    pub fn read_sized_bytes(&mut self, expected_len: usize) -> io::Result<Vec<u8>> {
        let bytes = self.read_bytes()?;
        if bytes.len() != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected {expected_len} bytes, got {}", bytes.len()),
            ));
        }
        Ok(bytes)
    }

    pub fn read_varint(&mut self) -> io::Result<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            let b = self.read_u8() as u64;
            result |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varint overflow",
                ));
            }
        }
    }

    pub fn read_i64_varint(&mut self) -> io::Result<i64> {
        let mut result = 0i64;
        let mut shift = 0u32;
        let mut byte: u8;
        loop {
            byte = self.read_u8();
            result |= ((byte & 0x7F) as i64) << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                break;
            }
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "signed varint too large",
                ));
            }
        }
        if shift < 64 && (byte & 0x40) != 0 {
            result |= (!0i64) << shift;
        }
        Ok(result)
    }

    pub fn read_u32_varint(&mut self) -> io::Result<u32> {
        Ok(self.read_varint()? as u32)
    }

    pub fn peek_field(&mut self) -> io::Result<u16> {
        if let Some(field) = self.buffered_field {
            return Ok(field);
        }
        let field = self.read_u16();
        self.buffered_field = Some(field);
        Ok(field)
    }

    pub fn next_field(&mut self) -> io::Result<u16> {
        if let Some(field) = self.buffered_field.take() {
            return Ok(field);
        }
        Ok(self.read_u16())
    }

    pub fn expect_field(&mut self, expected: u16) -> io::Result<()> {
        let field = self.next_field()?;
        if field != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected field {expected}, got {field}"),
            ));
        }
        Ok(())
    }

    pub fn read_list_len(&mut self) -> io::Result<usize> {
        Ok(self.read_varint()? as usize)
    }

    pub fn skip_object(&mut self) -> io::Result<()> {
        loop {
            match self.next_field()? {
                MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "generic object skipping is not supported for this payload",
                    ));
                }
            }
        }
    }

    pub fn read_meta_block_pointer(&mut self) -> io::Result<MetaBlockPointer> {
        let mut block_pointer = u64::MAX;
        let mut offset = 0u32;
        loop {
            match self.next_field()? {
                100 => block_pointer = self.read_varint()?,
                101 => offset = self.read_u32_varint()?,
                MESSAGE_TERMINATOR_FIELD_ID => {
                    return Ok(MetaBlockPointer {
                        block_pointer,
                        offset,
                    });
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected MetaBlockPointer field {other}"),
                    ));
                }
            }
        }
    }

    pub fn read_data_pointer(&mut self, logical_type: &LogicalType) -> io::Result<DataPointer> {
        let mut tuple_count = 0;
        let mut block_id = INVALID_BLOCK;
        let mut offset = 0u32;
        let mut compression = CompressionType::Uncompressed;
        loop {
            match self.next_field()? {
                101 => tuple_count = self.read_varint()?,
                102 => {
                    let (bid, off) = self.read_block_pointer()?;
                    block_id = bid;
                    offset = off;
                }
                103 => compression = read_compression_type(self.read_u8()),
                104 => skip_base_statistics(self, logical_type)?,
                105 => skip_optional_nullable(self)?,
                MESSAGE_TERMINATOR_FIELD_ID => {
                    return Ok(DataPointer {
                        block_id,
                        offset,
                        row_start: 0,
                        tuple_count,
                    });
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected DataPointer field {other}"),
                    ));
                }
            }
        }
    }

    fn read_block_pointer(&mut self) -> io::Result<(i64, u32)> {
        let mut block_id = INVALID_BLOCK;
        let mut offset = 0u32;
        loop {
            match self.next_field()? {
                100 => block_id = self.read_i64_varint()?,
                101 => offset = self.read_u32_varint()?,
                MESSAGE_TERMINATOR_FIELD_ID => return Ok((block_id, offset)),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected BlockPointer field {other}"),
                    ));
                }
            }
        }
    }

    pub fn read_row_group_pointer(&mut self) -> io::Result<RowGroupPointer> {
        let mut row_start = 0;
        let mut tuple_count = 0;
        let mut column_pointers = Vec::new();
        let mut deletes_pointers = Vec::new();
        let mut has_metadata_blocks = false;
        let mut extra_metadata_blocks = Vec::new();

        loop {
            match self.next_field()? {
                100 => row_start = self.read_varint()?,
                101 => tuple_count = self.read_varint()?,
                102 => {
                    let count = self.read_list_len()?;
                    column_pointers.reserve(count);
                    for _ in 0..count {
                        column_pointers.push(self.read_meta_block_pointer()?);
                    }
                }
                103 => {
                    let count = self.read_list_len()?;
                    deletes_pointers.reserve(count);
                    for _ in 0..count {
                        deletes_pointers.push(self.read_meta_block_pointer()?);
                    }
                }
                104 => has_metadata_blocks = self.read_u8() != 0,
                105 => {
                    let count = self.read_list_len()?;
                    extra_metadata_blocks.reserve(count);
                    for _ in 0..count {
                        extra_metadata_blocks.push(self.read_varint()? as Idx);
                    }
                }
                MESSAGE_TERMINATOR_FIELD_ID => {
                    return Ok(RowGroupPointer {
                        row_start,
                        tuple_count,
                        deletes_pointers,
                        column_pointers,
                    });
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected RowGroupPointer field {other}"),
                    ));
                }
            }
        }
    }
}

fn read_compression_type(value: u8) -> CompressionType {
    match value {
        1 => CompressionType::Uncompressed,
        2 => CompressionType::Constant,
        3 => CompressionType::Rle,
        4 => CompressionType::BitPacking,
        5 => CompressionType::Dictionary,
        6 => CompressionType::Fsst,
        7 => CompressionType::Chimp,
        8 => CompressionType::Patas,
        9 => CompressionType::Alprd,
        10 => CompressionType::ZStd,
        _ => CompressionType::Uncompressed,
    }
}

fn skip_base_statistics(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalType,
) -> io::Result<()> {
    loop {
        match de.next_field()? {
            100 | 101 => {
                let _ = de.read_u8();
            }
            102 => {
                let _ = de.read_varint()?;
            }
            103 => skip_type_statistics(de, logical_type)?,
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected BaseStatistics field {other}"),
                ));
            }
        }
    }
}

fn skip_type_statistics(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalType,
) -> io::Result<()> {
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected type statistics field {other}"),
                ));
            }
        }
    }
}

fn skip_numeric_stat_value(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalType,
) -> io::Result<()> {
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected numeric stats value field {other}"),
                ));
            }
        }
    }
}

fn skip_numeric_value(
    de: &mut BinaryMetadataDeserializer<'_>,
    logical_type: &LogicalType,
) -> io::Result<()> {
    match logical_type.id {
        crate::common::types::LogicalTypeId::Float => de.skip_bytes(4),
        crate::common::types::LogicalTypeId::Double => de.skip_bytes(8),
        _ => {
            let _ = de.read_varint()?;
        }
    }
    Ok(())
}

fn skip_optional_nullable(de: &mut BinaryMetadataDeserializer<'_>) -> io::Result<()> {
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

/// Skip a TableStatistics object which contains:
/// - field 100: column_stats (list of ColumnStatistics objects)
/// - field 101: table_sample (optional nullable)
/// - Additional fields may exist in newer versions
/// - terminator: 0xFFFF
pub fn skip_table_statistics(de: &mut BinaryMetadataDeserializer<'_>) -> io::Result<()> {
    loop {
        let field = de.next_field()?;
        match field {
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            100 => {
                // column_stats: list of ColumnStatistics
                let count = de.read_list_len()?;
                for _ in 0..count {
                    skip_nested_object_recursive(de)?;
                }
            }
            101 => {
                skip_optional_blocking_sample(de)?;
            }
            // Handle additional fields gracefully by skipping them
            other => {
                // Try to skip as a varint - if it fails, we need to handle differently
                let _ = de.read_varint()?;
                let _ = other; // suppress unused variable warning
            }
        }
    }
}

pub fn skip_optional_blocking_sample(de: &mut BinaryMetadataDeserializer<'_>) -> io::Result<()> {
    if de.read_u8() == 0 {
        return Ok(());
    }
    skip_blocking_sample(de)
}

fn skip_blocking_sample(de: &mut BinaryMetadataDeserializer<'_>) -> io::Result<()> {
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
                    let _ = de.read_f64();
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected BlockingSample field {other}"),
                ));
            }
        }
    }
}

fn skip_optional_base_reservoir_sampling(
    de: &mut BinaryMetadataDeserializer<'_>,
) -> io::Result<()> {
    if de.read_u8() == 0 {
        return Ok(());
    }
    loop {
        match de.next_field()? {
            100 | 102 | 103 | 104 => {
                let _ = de.read_varint()?;
            }
            101 => {
                let _ = de.read_f64();
            }
            105 => skip_priority_queue_of_weight_pairs(de)?,
            MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected BaseReservoirSampling field {other}"),
                ));
            }
        }
    }
}

fn skip_priority_queue_of_weight_pairs(de: &mut BinaryMetadataDeserializer<'_>) -> io::Result<()> {
    let count = de.read_list_len()?;
    for _ in 0..count {
        loop {
            match de.next_field()? {
                0 => {
                    let _ = de.read_f64();
                }
                1 => {
                    let _ = de.read_varint()?;
                }
                MESSAGE_TERMINATOR_FIELD_ID => break,
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected reservoir weight pair field {other}"),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn skip_optional_nested_object(de: &mut BinaryMetadataDeserializer<'_>) -> io::Result<()> {
    if de.read_u8() == 0 {
        return Ok(());
    }
    skip_nested_object_recursive(de)
}

/// Skip a nested object recursively by tracking depth
fn skip_nested_object_recursive(de: &mut BinaryMetadataDeserializer<'_>) -> io::Result<()> {
    let mut depth = 1usize;
    while depth > 0 {
        match de.next_field()? {
            MESSAGE_TERMINATOR_FIELD_ID => {
                depth -= 1;
            }
            _ => {
                // We need to skip the value, but we don't know its type
                // Try to detect based on the next byte pattern
                skip_unknown_value(de, &mut depth)?;
            }
        }
    }
    Ok(())
}

/// Skip an unknown value - try to handle common patterns
fn skip_unknown_value(
    de: &mut BinaryMetadataDeserializer<'_>,
    depth: &mut usize,
) -> io::Result<()> {
    // Read first byte to detect type
    let first_byte = de.stream.read_u8();

    // Check if it looks like a varint (most common case)
    // Varints have continuation bits: 0x80 means more bytes follow
    if first_byte & 0x80 == 0 {
        // Single byte varint, already consumed
        return Ok(());
    }

    // Multi-byte varint or something else
    // Read rest of varint
    let mut b = first_byte;
    while b & 0x80 != 0 {
        b = de.stream.read_u8();
    }

    Ok(())
}
