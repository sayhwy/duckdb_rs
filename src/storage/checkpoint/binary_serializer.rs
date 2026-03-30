// ============================================================
// binary_serializer.rs — 二进制序列化器
// 对应 C++: duckdb/common/serializer/binary_serializer.hpp/.cpp
// ============================================================
//
// DuckDB 使用字段 ID (field id) 来标识每个序列化的字段，
// 这样可以在反序列化时跳过未知字段（向前兼容）。
//
// 格式：
//   每个字段: [field_id: u16 LE][value]
//   对象结束: [0xFF 0xFF] (u16 terminator)
//
// 值类型编码（对应 C++ BinarySerializer::WriteValue）：
//   - bool: 1 字节 (Write<uint8_t>)
//   - u8/i8/u16/i16/u32/i32/u64/i64: varint 编码 (VarIntEncode)
//   - float/double: 4/8 字节小端 (Write<T>)
//   - string: [len: varint][bytes]
//   - 嵌套对象: [field_id][fields...][terminator]
//   - 列表: [field_id][count: varint][items...]

use crate::storage::metadata::WriteStream;

/// 消息终止符字段 ID
pub const MESSAGE_TERMINATOR_FIELD_ID: u16 = 0xFFFF;

/// 二进制序列化器
pub struct BinarySerializer<'a> {
    stream: &'a mut dyn WriteStream,
}

impl<'a> BinarySerializer<'a> {
    pub fn new(stream: &'a mut dyn WriteStream) -> Self {
        Self { stream }
    }

    // ─── 原始写入方法 ────────────────────────────────────────

    /// 写入字段 ID（2 字节，小端）
    pub fn write_field_id(&mut self, field_id: u16) {
        self.stream.write_u16(field_id);
    }

    /// 写入终止符 (0xFFFF)
    pub fn write_terminator(&mut self) {
        self.stream.write_u16(MESSAGE_TERMINATOR_FIELD_ID);
    }

    /// 写入 varint 编码的整数（不带字段 ID）
    pub fn write_varint_raw(&mut self, mut value: u64) {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.stream.write_u8(byte);
            if value == 0 {
                break;
            }
        }
    }

    /// 写入有符号 varint（用于负数）
    pub fn write_signed_varint_raw(&mut self, value: i64) {
        let mut value = value;
        loop {
            let byte = (value as u8) & 0x7F;
            let sign_bit_set = (byte & 0x40) != 0;
            value >>= 7;
            let done = (value == 0 && !sign_bit_set) || (value == -1 && sign_bit_set);
            self.stream.write_u8(if done { byte } else { byte | 0x80 });
            if done {
                break;
            }
        }
    }

    // ─── WriteProperty 方法 ───────────────────────────────────

    /// 写入 bool 属性
    pub fn write_bool(&mut self, field_id: u16, value: bool) {
        self.write_field_id(field_id);
        self.stream.write_u8(if value { 1 } else { 0 });
    }

    /// 写入 u8 属性（使用 varint 编码，与 DuckDB C++ 一致）
    /// 对应 C++ BinarySerializer::WriteValue(uint8_t) -> VarIntEncode
    pub fn write_u8(&mut self, field_id: u16, value: u8) {
        self.write_field_id(field_id);
        self.write_varint_raw(value as u64); // DuckDB uses varint for u8!
    }

    /// 写入 varint 属性
    pub fn write_varint(&mut self, field_id: u16, value: u64) {
        self.write_field_id(field_id);
        self.write_varint_raw(value);
    }

    /// 写入有符号 varint 属性
    pub fn write_signed_varint(&mut self, field_id: u16, value: i64) {
        self.write_field_id(field_id);
        self.write_signed_varint_raw(value);
    }

    /// 写入字符串属性
    pub fn write_string(&mut self, field_id: u16, value: &str) {
        self.write_field_id(field_id);
        let bytes = value.as_bytes();
        self.write_varint_raw(bytes.len() as u64);
        self.stream.write_data(bytes);
    }

    /// 写入定长字节数组（blob）属性。
    ///
    /// 对应 C++ `BinarySerializer::WriteProperty(field_id, "name", data_ptr_t, count)`。
    /// DuckDB 在反序列化时使用 `ReadDataPtr(ptr, count)` 验证长度，因此必须写入
    /// 恰好 `bytes.len()` 字节并以 varint 编码长度。
    pub fn write_bytes(&mut self, field_id: u16, bytes: &[u8]) {
        self.write_field_id(field_id);
        self.write_varint_raw(bytes.len() as u64);
        self.stream.write_data(bytes);
    }

    /// 写入可选字符串属性（如果为空则不写入）
    pub fn write_optional_string(&mut self, field_id: u16, value: Option<&str>) {
        if let Some(s) = value {
            self.write_string(field_id, s);
        }
        // 如果为 None，不写入任何内容
    }

    /// 写入 nullable 字段（带 present 标记）
    /// 对应 C++ WritePropertyWithDefault 对于 unique_ptr/shared_ptr：
    ///   - 写入 field_id
    ///   - 写入 present 标记（1 字节）
    ///   - 如果 present=true，写入对象体
    pub fn write_nullable_field(&mut self, field_id: u16, present: bool) {
        self.write_field_id(field_id);
        self.stream.write_u8(if present { 1 } else { 0 });
    }

    // ─── 嵌套结构方法 ────────────────────────────────────────

    /// 开始一个嵌套对象（写入字段 ID）
    /// 对应 C++ WriteObject: OnPropertyBegin -> OnObjectBegin
    pub fn begin_object(&mut self, field_id: u16) {
        self.write_field_id(field_id);
        // OnObjectBegin 在 BinarySerializer 中不写任何东西
    }

    /// 开始一个可空嵌套对象（写入字段 ID + nullable 标志）
    /// 对应 C++ WriteProperty(field_id, &ptr) 对于非空指针:
    ///   OnPropertyBegin -> OnNullableBegin(true) -> WriteValue -> OnNullableEnd
    pub fn begin_nullable_object(&mut self, field_id: u16) {
        self.write_field_id(field_id);
        // OnNullableBegin(true) 写入 1 字节 (0x01)
        self.stream.write_u8(1);
        // OnObjectBegin 在 BinarySerializer 中不写任何东西
    }

    /// 开始一个根对象（不写字段 ID）
    /// 对应 C++ OnObjectBegin: 无操作，仅标记对象开始
    pub fn begin_root_object(&mut self) {
        // OnObjectBegin 不写任何东西
    }

    /// 结束一个嵌套对象（写入终止符）
    /// 对应 C++ OnObjectEnd: Write<field_id_t>(MESSAGE_TERMINATOR_FIELD_ID)
    pub fn end_object(&mut self) {
        self.write_terminator();
    }

    /// 结束一个可空嵌套对象（不写入任何东西）
    /// 对应 C++ OnNullableEnd: 无操作
    pub fn end_nullable_object(&mut self) {
        // OnNullableEnd() 什么都不做
    }

    /// 开始一个列表（写入字段 ID 和数量）
    /// 对应 C++ WriteList: OnPropertyBegin -> OnListBegin
    pub fn begin_list(&mut self, field_id: u16, count: usize) {
        self.write_field_id(field_id);
        self.write_varint_raw(count as u64);
    }

    /// 结束一个列表（无操作）
    /// 对应 C++ OnListEnd: 空实现
    pub fn end_list(&mut self) {
        // OnListEnd 在 BinarySerializer 中不写任何东西
    }

    // ─── List 内部方法（不写字段 ID）────────────────────────

    /// 在列表内写入一个对象
    /// 对应 C++ List::WriteObject: OnObjectBegin -> f() -> OnObjectEnd
    pub fn list_write_object<F: FnOnce(&mut Self)>(&mut self, f: F) {
        // OnObjectBegin: 无操作
        f(self);
        // OnObjectEnd: 写入终止符
        self.write_terminator();
    }

    /// 在列表内写入一个 nullable object。
    ///
    /// 对应 C++ 列表元素为 `shared_ptr<T>` / `unique_ptr<T>` 时的编码：
    /// 先写 1 字节 present 标记，若 present=true 再写对象体和终止符。
    pub fn list_write_nullable_object<F: FnOnce(&mut Self)>(&mut self, present: bool, f: F) {
        self.stream.write_u8(if present { 1 } else { 0 });
        if present {
            f(self);
            self.write_terminator();
        }
    }

    /// 在列表内写入一个值（varint）
    pub fn list_write_varint(&mut self, value: u64) {
        self.write_varint_raw(value);
    }

    /// 在列表内写入一个字符串
    pub fn list_write_string(&mut self, value: &str) {
        let bytes = value.as_bytes();
        self.write_varint_raw(bytes.len() as u64);
        self.stream.write_data(bytes);
    }
}

/// 序列化辅助 trait
pub trait Serialize {
    fn serialize(&self, serializer: &mut BinarySerializer<'_>);
}
