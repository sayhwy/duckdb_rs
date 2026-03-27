//! Binary Serializer - 序列化为 DuckDB 二进制格式
//!
//! 对应 C++: duckdb/common/serializer/binary_serializer.hpp
//!
//! # 格式说明
//!
//! - 每个字段以 field_id (u16) 开头
//! - 对象结束标记: 0xFFFF
//! - 列表长度使用 varint 编码
//! - 整数使用 varint 编码（节省空间）
//! - 字符串: 长度(varint) + 数据

use crate::storage::metadata::{MetaBlockPointer, WriteStream};

/// 对应 C++ MESSAGE_TERMINATOR_FIELD_ID
pub const MESSAGE_TERMINATOR_FIELD_ID: u16 = 0xFFFF;

/// BinarySerializer - 将结构化数据序列化为 DuckDB 二进制格式
pub struct BinarySerializer<'a, W: WriteStream + ?Sized> {
    stream: &'a mut W,
}

impl<'a, W: WriteStream + ?Sized> BinarySerializer<'a, W> {
    pub fn new(stream: &'a mut W) -> Self {
        Self { stream }
    }

    /// 开始序列化一个对象
    pub fn begin_object(&mut self) {
        // 对象开始不需要特殊标记
    }

    /// 结束序列化一个对象，写入终止符
    pub fn end_object(&mut self) {
        self.stream.write_data(&MESSAGE_TERMINATOR_FIELD_ID.to_le_bytes());
    }

    /// 写入字段 ID
    pub fn write_field_id(&mut self, field_id: u16) {
        self.stream.write_data(&field_id.to_le_bytes());
    }

    /// 写入属性（field_id + value）
    pub fn write_property_u64(&mut self, field_id: u16, value: u64) {
        self.write_field_id(field_id);
        self.write_varint(value);
    }

    pub fn write_property_i64(&mut self, field_id: u16, value: i64) {
        self.write_field_id(field_id);
        self.write_varint_signed(value);
    }

    pub fn write_property_u32(&mut self, field_id: u16, value: u32) {
        self.write_field_id(field_id);
        self.write_varint(value as u64);
    }

    pub fn write_property_bool(&mut self, field_id: u16, value: bool) {
        self.write_field_id(field_id);
        self.stream.write_data(&[if value { 1u8 } else { 0u8 }]);
    }

    pub fn write_property_string(&mut self, field_id: u16, value: &str) {
        self.write_field_id(field_id);
        let bytes = value.as_bytes();
        self.write_varint(bytes.len() as u64);
        self.stream.write_data(bytes);
    }

    /// 写入 MetaBlockPointer
    pub fn write_property_meta_block_pointer(&mut self, field_id: u16, ptr: MetaBlockPointer) {
        self.write_field_id(field_id);
        self.begin_object();
        self.write_property_u64(100, ptr.block_pointer);
        self.write_property_u32(101, ptr.offset);
        self.end_object();
    }

    /// 开始写入列表
    pub fn begin_list(&mut self, count: usize) {
        self.write_varint(count as u64);
    }

    /// 结束写入列表（不需要特殊标记）
    pub fn end_list(&mut self) {
        // 列表结束不需要特殊标记
    }

    /// 写入 varint 编码的 u64
    pub fn write_varint(&mut self, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80; // 设置继续位
            }
            self.stream.write_data(&[byte]);
            if value == 0 {
                break;
            }
        }
    }

    /// 写入有符号 varint (zigzag 编码)
    pub fn write_varint_signed(&mut self, value: i64) {
        // ZigZag 编码: (n << 1) ^ (n >> 63)
        let zigzag = ((value << 1) ^ (value >> 63)) as u64;
        self.write_varint(zigzag);
    }

    /// 写入原始字节数据
    pub fn write_data(&mut self, data: &[u8]) {
        self.stream.write_data(data);
    }

    /// 序列化 RowGroupPointer
    pub fn write_row_group_pointer(&mut self, ptr: &crate::storage::data_pointer::RowGroupPointer) {
        self.begin_object();

        // field 100: row_start
        self.write_property_u64(100, ptr.row_start);

        // field 101: tuple_count
        self.write_property_u64(101, ptr.tuple_count);

        // field 102: column_pointers (list of MetaBlockPointer)
        self.write_field_id(102);
        self.begin_list(ptr.data_pointers.len());
        for col_ptr in &ptr.data_pointers {
            self.begin_object();
            self.write_property_u64(100, col_ptr.block_pointer);
            self.write_property_u32(101, col_ptr.offset);
            self.end_object();
        }

        // field 103: deletes_pointers (list of MetaBlockPointer)
        self.write_field_id(103);
        self.begin_list(ptr.deletes_pointers.len());
        for del_ptr in &ptr.deletes_pointers {
            self.begin_object();
            self.write_property_u64(100, del_ptr.block_pointer);
            self.write_property_u32(101, del_ptr.offset);
            self.end_object();
        }

        self.end_object();
    }
}

/// 辅助结构：CatalogEntry 序列化
pub struct CatalogSerializer;

impl CatalogSerializer {
    /// 写入 SCHEMA_ENTRY
    pub fn write_schema<W: WriteStream + ?Sized>(
        serializer: &mut BinarySerializer<'_, W>,
        schema_name: &str,
    ) {
        serializer.begin_object();
        // field 100: schema info
        serializer.write_property_string(100, schema_name);
        serializer.end_object();
    }

    /// 写入 TABLE_ENTRY
    pub fn write_table<W: WriteStream + ?Sized>(
        serializer: &mut BinarySerializer<'_, W>,
        table_name: &str,
        schema_name: &str,
        columns: &[(String, crate::common::types::LogicalType)],
        table_pointer: MetaBlockPointer,
        total_rows: u64,
    ) {
        serializer.begin_object();

        // field 100: table metadata
        serializer.write_field_id(100);
        serializer.begin_object();
        serializer.write_property_string(100, table_name);
        serializer.write_property_string(101, schema_name);

        // columns list
        serializer.write_field_id(102);
        serializer.begin_list(columns.len());
        for (col_name, _col_type) in columns {
            serializer.begin_object();
            serializer.write_property_string(100, col_name);
            serializer.write_property_u64(101, 1); // LogicalType::INTEGER type id
            serializer.end_object();
        }
        serializer.end_list();
        serializer.end_object();

        // field 101: table_pointer
        serializer.write_property_meta_block_pointer(101, table_pointer);

        // field 102: total_rows
        serializer.write_property_u64(102, total_rows);

        // field 103: index_pointers (empty vector for backwards compatibility)
        // This is written as: field_id(103) + list_count(0)
        serializer.write_field_id(103);
        serializer.begin_list(0);

        // field 104: index_storage_infos (empty vector)
        // NOTE: WritePropertyWithDefault in C++ may skip this if empty and serialize_default_values=false
        // However, for compatibility, we write it explicitly as an empty list
        serializer.write_field_id(104);
        serializer.begin_list(0);

        // End the table entry object
        serializer.end_object();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::metadata::MetaBlockPointer;

    struct TestStream {
        data: Vec<u8>,
    }

    impl WriteStream for TestStream {
        fn write_data(&mut self, buf: &[u8]) {
            self.data.extend_from_slice(buf);
        }
    }

    #[test]
    fn test_varint_encoding() {
        let mut stream = TestStream { data: Vec::new() };
        {
            let mut serializer = BinarySerializer::new(&mut stream);

            // 小值：单字节
            serializer.write_varint(0);
        }
        assert_eq!(stream.data.last(), Some(&0));

        stream.data.clear();
        {
            let mut serializer = BinarySerializer::new(&mut stream);
            serializer.write_varint(127);
        }
        assert_eq!(stream.data.last(), Some(&127));

        // 大值：多字节
        stream.data.clear();
        {
            let mut serializer = BinarySerializer::new(&mut stream);
            serializer.write_varint(128);
        }
        assert!(stream.data.len() > 1);
    }

    #[test]
    fn test_object_serialization() {
        let mut stream = TestStream { data: Vec::new() };
        {
            let mut serializer = BinarySerializer::new(&mut stream);

            serializer.begin_object();
            serializer.write_property_u64(100, 42);
            serializer.write_property_string(101, "test");
            serializer.end_object();
        }

        // 检查终止符
        let len = stream.data.len();
        assert!(len >= 2);
        let terminator = u16::from_le_bytes([stream.data[len - 2], stream.data[len - 1]]);
        assert_eq!(terminator, MESSAGE_TERMINATOR_FIELD_ID);
    }
}