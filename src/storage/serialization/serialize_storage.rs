use crate::common::serializer::BinarySerializer;
use crate::common::types::LogicalTypeId;
use crate::storage::data_table::ColumnDefinition;
use crate::storage::metadata::{MetaBlockPointer, MetadataWriter, WriteStream};
use crate::storage::statistics::BaseStatistics;
use crate::storage::table::row_group::RowGroupPointer;
use crate::storage::table::types::{CompressionType, DataPointer, Idx};

pub fn write_minimal_table_statistics(
    writer: &mut MetadataWriter<'_>,
    columns: &[ColumnDefinition],
    total_rows: Idx,
) {
    let mut serializer = BinarySerializer::new(writer as &mut dyn WriteStream);
    serializer.begin_list(100, columns.len());
    for column in columns {
        serializer.list_write_nullable_object(true, |s| {
            let mut stats = BaseStatistics::create_empty(column.logical_type.clone());
            if total_rows > 0 {
                stats.set_has_no_null();
            }
            stats.set_distinct_count(0);
            s.begin_object(100);
            stats.serialize_checkpoint(s);
            s.end_object();
            s.write_nullable_field(101, false);
        });
    }
    serializer.end_list();
    serializer.write_nullable_field(101, false);
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

pub fn write_data_pointer(serializer: &mut BinarySerializer<'_>, pointer: &DataPointer) {
    if pointer.row_start != 0 {
        serializer.write_varint(100, pointer.row_start);
    }
    serializer.write_varint(101, pointer.tuple_count);
    serializer.begin_object(102);
    serializer.write_signed_varint(100, pointer.block_id);
    serializer.write_varint(101, pointer.offset as u64);
    serializer.end_object();
    serializer.write_u8(103, compression_tag(pointer.compression_type));
    serializer.begin_object(104);
    pointer.statistics.serialize_checkpoint(serializer);
    serializer.end_object();
}

pub fn write_data_pointer_varchar(
    serializer: &mut BinarySerializer<'_>,
    pointer: &DataPointer,
) {
    write_data_pointer(serializer, pointer);
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
        CompressionType::Dictionary => 4,
        CompressionType::PforDelta => 5,
        CompressionType::BitPacking => 6,
        CompressionType::Fsst => 7,
        CompressionType::Chimp => 8,
        CompressionType::Patas => 9,
        CompressionType::Alp => 10,
        CompressionType::Alprd => 11,
        CompressionType::ZStd => 12,
        CompressionType::Roaring => 13,
        CompressionType::Empty => 14,
        CompressionType::DictFSST => 15,
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
        let decoded = de
            .read_meta_block_pointer()
            .expect("pointer should deserialize");
        assert_eq!(decoded, ptr);
        assert!(
            decoded.is_valid(),
            "pointer with block_pointer=0 must remain valid"
        );
    }
}
