use crate::common::serializer::BinarySerializer;
use crate::common::types::LogicalTypeId;
use crate::storage::data_table::ColumnDefinition;
use crate::storage::metadata::{MetaBlockPointer, MetadataWriter, WriteStream};
use crate::storage::table::row_group::RowGroupPointer;
use crate::storage::table::types::{CompressionType, Idx};

pub fn write_minimal_table_statistics(
    writer: &mut MetadataWriter<'_>,
    columns: &[ColumnDefinition],
    total_rows: Idx,
) {
    let mut serializer = BinarySerializer::new(writer as &mut dyn WriteStream);
    serializer.begin_list(100, columns.len());
    for column in columns {
        serializer.list_write_nullable_object(true, |s| {
            s.write_field_id(100);
            write_minimal_base_statistics(s, &column.logical_type, total_rows);
            s.end_object();
        });
    }
    serializer.end_list();
    serializer.write_nullable_field(101, false);
    serializer.end_object();
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
            serializer.write_bytes(200, &[0u8; 8]);
            serializer.write_bytes(201, &[0xFFu8; 8]);
            serializer.write_bool(202, false);
            serializer.write_bool(203, true);
            serializer.write_varint(204, 0);
        }
        _ => {}
    }
    serializer.end_object();
}

pub fn write_row_group_pointer(serializer: &mut BinarySerializer<'_>, pointer: &RowGroupPointer) {
    serializer.write_varint(100, pointer.row_start);
    serializer.write_varint(101, pointer.tuple_count);
    serializer.begin_list(102, pointer.column_pointers.len());
    for column_pointer in &pointer.column_pointers {
        serializer.list_write_object(|s| write_meta_block_pointer(s, column_pointer));
    }
    serializer.end_list();
    serializer.begin_list(103, 0);
    serializer.end_list();
}

pub fn write_data_pointer(
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

pub fn write_data_pointer_varchar(
    serializer: &mut BinarySerializer<'_>,
    tuple_count: Idx,
    block_id: i64,
    offset: u32,
) {
    const STR_STATS_SIZE: usize = 8;

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
    serializer.write_bytes(200, &[0u8; STR_STATS_SIZE]);
    serializer.write_bytes(201, &[0xFFu8; STR_STATS_SIZE]);
    serializer.write_bool(202, false);
    serializer.write_bool(203, true);
    serializer.write_varint(204, 0);
    serializer.end_object();
    serializer.end_object();
}

pub fn write_meta_block_pointer(serializer: &mut BinarySerializer<'_>, ptr: &MetaBlockPointer) {
    // `MetaBlockPointer { block_pointer: 0, offset: 0 }` is a valid pointer:
    // block_id=0, block_index=0. DuckDB serializes it explicitly. Omitting
    // field 100 turns a valid pointer into the default "invalid" value during
    // deserialization.
    serializer.write_varint(100, ptr.block_pointer);
    if ptr.offset != 0 {
        serializer.write_varint(101, ptr.offset as u64);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::serializer::BinaryMetadataDeserializer;
    use crate::storage::metadata::{MetaBlockPointer, ReadStream, WriteStream};

    #[derive(Default)]
    struct MemStream {
        buf: Vec<u8>,
        pos: usize,
    }

    impl WriteStream for MemStream {
        fn write_data(&mut self, buf: &[u8]) {
            self.buf.extend_from_slice(buf);
        }
    }

    impl ReadStream for MemStream {
        fn read_data(&mut self, buf: &mut [u8]) {
            let end = self.pos + buf.len();
            buf.copy_from_slice(&self.buf[self.pos..end]);
            self.pos = end;
        }
    }

    #[test]
    fn meta_block_pointer_zero_roundtrip() {
        let ptr = MetaBlockPointer {
            block_pointer: 0,
            offset: 0,
        };
        let mut stream = MemStream::default();
        {
            let mut serializer = BinarySerializer::new(&mut stream as &mut dyn WriteStream);
            serializer.begin_root_object();
            write_meta_block_pointer(&mut serializer, &ptr);
            serializer.end_object();
        }
        let mut de = BinaryMetadataDeserializer::new(&mut stream);
        let decoded = de.read_meta_block_pointer().expect("pointer should deserialize");
        assert_eq!(decoded, ptr);
        assert!(decoded.is_valid(), "pointer with block_pointer=0 must remain valid");
    }
}
