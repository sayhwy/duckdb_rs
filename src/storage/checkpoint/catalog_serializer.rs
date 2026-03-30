// ============================================================
// catalog_serializer.rs — Catalog 序列化
// 对应 C++: duckdb/storage/checkpoint_manager.cpp
// ============================================================

use super::binary_serializer::BinarySerializer;
use crate::catalog::{ColumnDefinition, LogicalType, TableCatalogEntry};
use crate::storage::metadata::{MetaBlockPointer, WriteStream};

// ─── DuckDB 枚举值（与 C++ 源码一致）────────────────────────

/// Catalog 类型枚举（对应 C++ CatalogType）
pub mod catalog_type {
    pub const TABLE_ENTRY: u8 = 1;
    pub const SCHEMA_ENTRY: u8 = 2;
    pub const VIEW_ENTRY: u8 = 3;
    pub const INDEX_ENTRY: u8 = 4;
    pub const SEQUENCE_ENTRY: u8 = 6;
    pub const TYPE_ENTRY: u8 = 8;
    pub const MACRO_ENTRY: u8 = 30;
    pub const TABLE_MACRO_ENTRY: u8 = 31;
}

/// LogicalType ID 枚举（对应 C++ LogicalTypeId）
pub mod logical_type_id {
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

/// TableColumnType 枚举
pub mod table_column_type {
    pub const STANDARD: u8 = 0;
    pub const GENERATED: u8 = 1;
}

/// OnCreateConflict 枚举
pub mod on_create_conflict {
    pub const ERROR_ON_CONFLICT: u8 = 0;
    pub const IGNORE_ON_CONFLICT: u8 = 1;
    pub const REPLACE_ON_CONFLICT: u8 = 2;
    pub const ALTER_ON_CONFLICT: u8 = 3;
}

// ─── 序列化函数 ─────────────────────────────────────────────

/// 写入完整的 catalog
/// 对应 C++ CheckpointWriter::WriteCatalog
///
/// 重要：DuckDB 要求先写入 SCHEMA_ENTRY，然后才能写入 TABLE_ENTRY
///
/// 序列化格式：
///   Begin() -> 根对象开始
///   WriteList(100, "catalog_entries", count) -> 列表
///   End() -> 根对象结束（写入终止符）
pub fn write_catalog<W: WriteStream>(
    stream: &mut W,
    entries: &[(&TableCatalogEntry, Option<MetaBlockPointer>, u64)],
) {
    let mut serializer = BinarySerializer::new(stream);

    // 收集所有需要的 schema
    let mut schemas: Vec<(String, String)> = Vec::new();
    for (entry, _, _) in entries {
        let schema_name = entry.base.parent_schema().to_string();
        let catalog_name = entry.base.parent_catalog().to_string();
        if !schemas.iter().any(|(s, _)| s == &schema_name) {
            schemas.push((schema_name, catalog_name));
        }
    }

    // 总 entry 数 = schemas + tables
    let total_entries = schemas.len() + entries.len();

    // 对应 C++: serializer.Begin() -> OnObjectBegin
    // 这会开始一个根对象，让 ReadList 在一个对象内部
    // 注意：OnObjectBegin 在 BinarySerializer 中不写任何东西
    // 但 OnObjectEnd 会写入终止符

    // WriteList(100, "catalog_entries", count)
    // 这会写入: field_id(100) + varint(count)
    serializer.begin_list(100, total_entries);

    // 1. 先写入所有 SCHEMA_ENTRY
    for (schema_name, catalog_name) in &schemas {
        serializer.list_write_object(|s| {
            write_schema_entry(s, catalog_name, schema_name);
        });
    }

    // 2. 然后写入所有 TABLE_ENTRY
    for (entry, table_pointer, total_rows) in entries {
        // List::WriteObject - 在列表内写入一个对象
        serializer.list_write_object(|s| {
            write_catalog_entry(s, entry, *table_pointer, *total_rows);
        });
    }

    serializer.end_list();

    // 对应 C++: serializer.End() -> OnObjectEnd -> 写入终止符
    // 这结束了根对象
    serializer.end_object();
}

/// 写入 SchemaCatalogEntry
/// 对应 C++ CheckpointWriter::WriteSchema
fn write_schema_entry(
    serializer: &mut BinarySerializer<'_>,
    catalog_name: &str,
    schema_name: &str,
) {
    // 字段 99: catalog_type (u8)
    serializer.write_u8(99, catalog_type::SCHEMA_ENTRY);

    // 字段 100: "schema" - CreateSchemaInfo 对象
    // C++ WriteProperty(100, "schema", &schema) 流程:
    //   OnPropertyBegin(100) -> write field_id
    //   WriteValue(&schema):
    //     OnNullableBegin(true) -> write present flag
    //     WriteValue(schema):
    //       OnObjectBegin() -> nothing
    //       schema.Serialize() -> write fields
    //       OnObjectEnd() -> write terminator
    //     OnNullableEnd() -> nothing
    //   OnPropertyEnd() -> nothing
    serializer.begin_nullable_object(100); // OnPropertyBegin + OnNullableBegin
    write_create_schema_info(serializer, catalog_name, schema_name); // OnObjectBegin + Serialize
    serializer.end_object(); // OnObjectEnd - terminator for CreateSchemaInfo
    serializer.end_nullable_object(); // OnNullableEnd - nothing

    // 注意：终止符由 list_write_object 写入（结束整个 schema entry）
}

/// 写入 CreateSchemaInfo
/// 对应 C++ CreateSchemaInfo::Serialize (不写终止符)
fn write_create_schema_info(
    serializer: &mut BinarySerializer<'_>,
    catalog_name: &str,
    schema_name: &str,
) {
    // 对应 C++ OnObjectBegin（BinarySerializer 中不写任何东西）
    serializer.begin_root_object();

    // CreateInfo 基类字段
    // 字段 100: type (CatalogType) - 总是写入
    serializer.write_u8(100, catalog_type::SCHEMA_ENTRY);

    // 字段 101: catalog - DuckDB 的 schema checkpoint 不写该字段
    let _ = catalog_name;

    // 字段 102: schema - 仅当非空时写入
    if !schema_name.is_empty() {
        serializer.write_string(102, schema_name);
    }

    // 字段 103: temporary - 默认 false，不写入
    // 字段 104: internal - 默认 false，不写入

    // 字段 105: on_conflict - 总是写入 (WriteProperty, not WritePropertyWithDefault)
    serializer.write_u8(105, on_create_conflict::ERROR_ON_CONFLICT);

    // 注意：CreateSchemaInfo::Serialize 不写终止符
    // 终止符由外层 WriteValue 的 OnObjectEnd 写入（在 write_schema_entry 中调用 end_object）
}

/// 写入单个 catalog entry
/// 对应 C++ CheckpointWriter::WriteEntry
fn write_catalog_entry(
    serializer: &mut BinarySerializer<'_>,
    entry: &TableCatalogEntry,
    table_pointer: Option<MetaBlockPointer>,
    total_rows: u64,
) {
    // 字段 99: catalog_type (u8)
    serializer.write_u8(99, catalog_type::TABLE_ENTRY);

    // 写入表信息
    write_table(serializer, entry, table_pointer, total_rows);

    // 注意：终止符由 list_write_object 写入
}

/// 写入表信息
/// 对应 C++ CheckpointWriter::WriteTable
fn write_table(
    serializer: &mut BinarySerializer<'_>,
    entry: &TableCatalogEntry,
    table_pointer: Option<MetaBlockPointer>,
    total_rows: u64,
) {
    // 字段 100: "table" - CreateTableInfo 对象
    // 注意：C++ 使用 WriteProperty(100, "table", &info)，这会通过指针序列化
    // 指针序列化会调用 OnNullableBegin(true)，写入 1 字节的 nullable 标志
    // 然后调用 WriteValue(*ptr): OnObjectBegin -> Serialize -> OnObjectEnd
    serializer.begin_nullable_object(100);
    write_create_table_info(serializer, entry);
    // 写入 CreateTableInfo 的终止符（对应 WriteValue 的 OnObjectEnd）
    serializer.end_object();
    serializer.end_nullable_object();

    // 写入 table storage info (fields 101-104)
    // 这些字段与 field 100 在同一层级
    if let Some(ptr) = table_pointer {
        write_table_storage_info(serializer, ptr, total_rows);
    }
}

/// 写入 CreateTableInfo
/// 对应 C++ CreateTableInfo::Serialize
fn write_create_table_info(serializer: &mut BinarySerializer<'_>, entry: &TableCatalogEntry) {
    // 对应 C++ Serializer::WriteValue(const T* ptr) 中的 OnObjectBegin
    // 当写入非空指针时，会调用 OnNullableBegin(true)，然后调用 WriteValue(*ptr)
    // WriteValue 对于 has_serialize 的类型会调用 OnObjectBegin() -> value.Serialize() -> OnObjectEnd()
    serializer.begin_root_object();

    // 先写 CreateInfo 基类字段
    // 字段 100: type (CatalogType) - 总是写入
    serializer.write_u8(100, catalog_type::TABLE_ENTRY);

    // 字段 101: catalog - 仅当非空时写入（WritePropertyWithDefault）
    let catalog = entry.base.parent_catalog();
    if !catalog.is_empty() {
        serializer.write_string(101, catalog);
    }

    // 字段 102: schema - 仅当非空时写入
    let schema = entry.base.parent_schema();
    if !schema.is_empty() {
        serializer.write_string(102, schema);
    }

    // 字段 103: temporary - 默认 false，不写入
    // 字段 104: internal - 默认 false，不写入

    // 字段 105: on_conflict - 总是写入 (WriteProperty, not WritePropertyWithDefault)
    serializer.write_u8(105, on_create_conflict::ERROR_ON_CONFLICT);

    // 字段 106: sql - 默认空，不写入
    // 字段 107: comment - 默认空，不写入
    // 字段 108: tags - 默认空，不写入

    // CreateTableInfo 字段
    // 字段 200: table name - 总是写入
    serializer.write_string(200, &entry.base.fields().name);

    // 字段 201: columns (ColumnList)
    // C++ WriteProperty(201, "columns", columns) 会:
    //   1. OnPropertyBegin(201) -> 写 field_id
    //   2. WriteValue(columns):
    //      - OnObjectBegin() -> 无操作
    //      - columns.Serialize() -> 写列数据
    //      - OnObjectEnd() -> 写终止符!
    //   3. OnPropertyEnd() -> 无操作
    serializer.write_field_id(201);
    write_column_list(serializer, &entry.columns);
    // 写入 ColumnList 对象的终止符（对应 WriteValue 的 OnObjectEnd）
    serializer.end_object();

    // 字段 202: constraints - 默认空，不写入
    // 字段 203: query - 默认空，不写入

    // 注意：CreateTableInfo 的终止符由 write_table 中的 end_object 写入
}

/// 写入 ColumnList
/// 对应 C++ ColumnList::Serialize
///
/// 注意：调用者已经写入了字段 ID（如 201）
/// ColumnList::Serialize 只写字段 100 (columns vector)
/// 终止符由调用者写入（对应 WriteValue 的 OnObjectEnd）
fn write_column_list(serializer: &mut BinarySerializer<'_>, columns: &crate::catalog::ColumnList) {
    // 字段 100: columns - vector<ColumnDefinition>
    // 对应 C++ WritePropertyWithDefault<vector<ColumnDefinition>>(100, "columns", columns)
    serializer.begin_list(100, columns.columns.len());

    for col in &columns.columns {
        // List::WriteObject - 每列是一个对象
        serializer.list_write_object(|s| {
            write_column_definition(s, col);
        });
    }

    serializer.end_list();
}

/// 写入单个 ColumnDefinition
/// 对应 C++ ColumnDefinition::Serialize
fn write_column_definition(serializer: &mut BinarySerializer<'_>, col: &ColumnDefinition) {
    // 字段 100: name
    serializer.write_string(100, &col.name);

    // 字段 101: type (LogicalType)
    // C++ WriteProperty<LogicalType>(101, "type", type) 会写入 field_id 101，然后调用 LogicalType::Serialize
    serializer.write_field_id(101);
    write_logical_type(serializer, &col.logical_type);

    // 字段 102: expression - 跳过（生成列表达式，可选）

    // 字段 103: category (TableColumnType)
    serializer.write_u8(
        103,
        if col.is_generated() {
            table_column_type::GENERATED
        } else {
            table_column_type::STANDARD
        },
    );

    // 字段 104: compression_type
    // DuckDB 在 ColumnDefinition checkpoint 中会显式写出该字段。
    // AUTO = 0，可兼容基础列类型。
    serializer.write_u8(104, 0);

    // 字段 105: comment - 跳过（可选）
    // 字段 106: tags - 跳过（可选）

    // 注意：不在这里调用 end_object()
    // 终止符由 list_write_object 写入（调用者）
}

/// 写入 LogicalType
/// 对应 C++ LogicalType::Serialize
fn write_logical_type(serializer: &mut BinarySerializer<'_>, ty: &LogicalType) {
    // 注意：调用者（write_column_definition）已经通过 write_field_id(101) 开始了字段
    // 这里只需要写入 LogicalType 的内部字段

    // 对应 C++ OnObjectBegin
    serializer.begin_root_object();

    // 字段 100: id (LogicalTypeId)
    let type_id = logical_type_id_from(ty);
    serializer.write_u8(100, type_id);

    // 字段 101: type_info - 跳过（对于基本类型不需要）

    // LogicalType 对象结束（对应 C++ OnObjectEnd -> 写入终止符）
    serializer.end_object();
}

/// 将 LogicalType 转换为 LogicalTypeId
fn logical_type_id_from(ty: &LogicalType) -> u8 {
    use crate::catalog::LogicalTypeId;
    match ty.id {
        LogicalTypeId::Boolean => logical_type_id::BOOLEAN,
        LogicalTypeId::TinyInt => logical_type_id::TINYINT,
        LogicalTypeId::SmallInt => logical_type_id::SMALLINT,
        LogicalTypeId::Integer => logical_type_id::INTEGER,
        LogicalTypeId::BigInt => logical_type_id::BIGINT,
        LogicalTypeId::Float => logical_type_id::FLOAT,
        LogicalTypeId::Double => logical_type_id::DOUBLE,
        LogicalTypeId::Varchar => logical_type_id::VARCHAR,
        LogicalTypeId::Date => logical_type_id::DATE,
        LogicalTypeId::Time => logical_type_id::TIME,
        LogicalTypeId::Timestamp => logical_type_id::TIMESTAMP,
        LogicalTypeId::Blob => logical_type_id::BLOB,
        LogicalTypeId::Decimal => logical_type_id::DECIMAL,
        _ => 0, // INVALID
    }
}

/// 写入表的存储信息
/// 对应 C++ SingleFileTableDataWriter::FinalizeTable
fn write_table_storage_info(
    serializer: &mut BinarySerializer<'_>,
    table_pointer: MetaBlockPointer,
    total_rows: u64,
) {
    // 字段 101: table_pointer (MetaBlockPointer)
    // WriteProperty<MetaBlockPointer> does:
    //   OnPropertyBegin(101) -> writes field_id
    //   WriteValue(pointer) -> OnObjectBegin (nothing) + Serialize + OnObjectEnd (terminator)
    serializer.write_field_id(101);
    write_meta_block_pointer(serializer, &table_pointer);
    serializer.write_terminator(); // OnObjectEnd

    // 字段 102: total_rows
    serializer.write_varint(102, total_rows);

    // 字段 103: index_pointers - 空列表（向后兼容）
    serializer.begin_list(103, 0);
    serializer.end_list();

    // 字段 104: index_storage_infos - 空列表
    // DuckDB 使用 ReadPropertyWithExplicitDefault，但仍然期望字段存在
    serializer.begin_list(104, 0);
    serializer.end_list();
}

/// 写入 MetaBlockPointer
/// 对应 C++ MetaBlockPointer::Serialize
fn write_meta_block_pointer(serializer: &mut BinarySerializer<'_>, ptr: &MetaBlockPointer) {
    // field 100: block_pointer - 仅当非默认时写入 (WritePropertyWithDefault)
    if ptr.block_pointer != 0 {
        serializer.write_varint(100, ptr.block_pointer);
    }
    // field 101: offset - 仅当非默认时写入 (WritePropertyWithDefault)
    if ptr.offset != 0 {
        serializer.write_varint(101, ptr.offset as u64);
    }
}
