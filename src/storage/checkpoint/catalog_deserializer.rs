use std::io;

use crate::common::serializer::MESSAGE_TERMINATOR_FIELD_ID;
use crate::storage::metadata::MetaBlockPointer;
use crate::storage::metadata::ReadStream;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_id: u32,
    pub type_name: String,
}

#[derive(Debug, Clone)]
pub struct CatalogEntry {
    /// 1 = TABLE, 2 = SCHEMA, others possible
    pub catalog_type: u32,
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub total_rows: u64,
    pub table_pointer: Option<MetaBlockPointer>,
}

struct BinaryDeserializer<'a> {
    stream: &'a mut dyn ReadStream,
}

impl<'a> BinaryDeserializer<'a> {
    fn new(stream: &'a mut dyn ReadStream) -> Self {
        Self { stream }
    }

    fn read_u8(&mut self) -> u8 {
        self.stream.read_u8()
    }

    fn read_u16_le(&mut self) -> u16 {
        let mut buf = [0u8; 2];
        self.stream.read_data(&mut buf);
        u16::from_le_bytes(buf)
    }

    fn read_varint(&mut self) -> io::Result<u64> {
        let mut result = 0u64;
        let mut shift = 0u32;
        loop {
            // 若流已 EOF，直接返回 0（避免无限读 0xFF 循环）
            if self.stream.is_eof() {
                return Ok(0);
            }
            let b = self.read_u8() as u64;
            result |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varint overflow",
                ));
            }
        }
        Ok(result)
    }

    fn read_string(&mut self) -> io::Result<String> {
        let len = self.read_varint()? as usize;
        if len == 0 {
            return Ok(String::new());
        }
        let mut buf = vec![0u8; len];
        self.stream.read_data(&mut buf);
        // Use lossy conversion to handle non-UTF8 strings gracefully
        // DuckDB stores raw bytes and validates UTF-8 downstream
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    fn read_field_id(&mut self) -> u16 {
        self.read_u16_le()
    }

    /// Skip an object whose values are only VarInt-encoded scalars or nested
    /// varint-only objects that we do not need to inspect.
    fn skip_object_varint_only(&mut self) -> io::Result<()> {
        loop {
            let fid = self.read_field_id();
            if fid == 0xFFFF {
                return Ok(());
            }
            let _ = self.read_varint()?;
        }
    }
}

fn type_id_to_name(id: u32) -> &'static str {
    match id {
        0 => "INVALID",
        1 => "SQLNULL",
        2 => "UNKNOWN",
        3 => "ANY",
        4 => "USER",
        10 => "BOOLEAN",
        11 => "TINYINT",
        12 => "SMALLINT",
        13 => "INTEGER",
        14 => "BIGINT",
        15 => "DATE",
        16 => "TIME",
        17 => "TIMESTAMP_SEC",
        18 => "TIMESTAMP_MS",
        19 => "TIMESTAMP",
        20 => "TIMESTAMP_NS",
        21 => "DECIMAL",
        22 => "FLOAT",
        23 => "DOUBLE",
        24 => "CHAR",
        25 => "VARCHAR",
        26 => "BLOB",
        27 => "INTERVAL",
        28 => "UTINYINT",
        29 => "USMALLINT",
        30 => "UINTEGER",
        31 => "UBIGINT",
        32 => "TIMESTAMP_TZ",
        33 => "TIME_TZ",
        34 => "JSON",
        35 => "ENUM",
        36 => "LIST",
        37 => "STRUCT",
        38 => "MAP",
        39 => "UNION",
        40 => "BITSTRING",
        41 => "HUGEINT",
        42 => "POINTER",
        43 => "VALIDITY",
        44 => "UUID",
        45 => "UHUGEINT",
        46 => "ARRAY",
        47 => "VARINT",
        _ => "UNKNOWN",
    }
}

fn read_logical_type(r: &mut BinaryDeserializer<'_>) -> io::Result<u32> {
    let mut type_id = 0u32;
    loop {
        let fid = r.read_field_id();
        if fid == 0xFFFF {
            break;
        }
        match fid {
            100 => {
                type_id = r.read_varint()? as u32;
            }
            101 => {
                if r.read_u8() == 1 {
                    r.skip_object_varint_only()?;
                }
            }
            102 => {
                let _ = r.read_varint()?;
            }
            103 => {
                if r.read_u8() == 1 {
                    let count = r.read_varint()? as usize;
                    for _ in 0..count {
                        let _ = read_logical_type(r)?;
                    }
                }
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }
    Ok(type_id)
}

fn read_column_definition(r: &mut BinaryDeserializer<'_>) -> io::Result<ColumnInfo> {
    let mut name = String::new();
    let mut type_id = 0u32;

    loop {
        let fid = r.read_field_id();
        if fid == 0xFFFF {
            break;
        }
        match fid {
            100 => name = r.read_string()?,
            101 => type_id = read_logical_type(r)?,
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }

    Ok(ColumnInfo {
        name,
        type_id,
        type_name: type_id_to_name(type_id).to_string(),
    })
}

fn read_column_list(r: &mut BinaryDeserializer<'_>) -> io::Result<Vec<ColumnInfo>> {
    let mut columns = Vec::new();

    loop {
        let fid = r.read_field_id();
        if fid == 0xFFFF {
            break;
        }
        match fid {
            100 => {
                let count = r.read_varint()? as usize;
                for _ in 0..count {
                    columns.push(read_column_definition(r)?);
                }
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }

    Ok(columns)
}

fn skip_catalog_entry_info(r: &mut BinaryDeserializer<'_>) -> io::Result<()> {
    loop {
        let fid = r.read_field_id();
        if fid == MESSAGE_TERMINATOR_FIELD_ID {
            return Ok(());
        }
        match fid {
            100 => {
                let _ = r.read_varint()?;
            }
            101 | 102 => {
                let _ = r.read_string()?;
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }
}

fn skip_logical_dependency(r: &mut BinaryDeserializer<'_>) -> io::Result<()> {
    loop {
        let fid = r.read_field_id();
        if fid == MESSAGE_TERMINATOR_FIELD_ID {
            return Ok(());
        }
        match fid {
            100 => skip_catalog_entry_info(r)?,
            101 => {
                let _ = r.read_string()?;
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }
}

fn skip_logical_dependency_list(r: &mut BinaryDeserializer<'_>) -> io::Result<()> {
    loop {
        let fid = r.read_field_id();
        if fid == MESSAGE_TERMINATOR_FIELD_ID {
            return Ok(());
        }
        match fid {
            100 => {
                let count = r.read_varint()? as usize;
                for _ in 0..count {
                    skip_logical_dependency(r)?;
                }
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }
}

fn skip_constraints(r: &mut BinaryDeserializer<'_>) -> io::Result<()> {
    let count = r.read_varint()? as usize;
    for _ in 0..count {
        let _ = r.read_u8();
        r.skip_object_varint_only()?;
    }
    Ok(())
}

fn read_create_table_info(
    r: &mut BinaryDeserializer<'_>,
) -> io::Result<(String, String, String, Vec<ColumnInfo>)> {
    let mut catalog = String::new();
    let mut schema = String::new();
    let mut table_name = String::new();
    let mut columns = Vec::new();

    loop {
        let fid = r.read_field_id();
        if fid == 0xFFFF {
            break;
        }
        match fid {
            // CreateInfo fields
            100 => {
                // type (CatalogType) - just read and discard
                let _ = r.read_varint()?;
            }
            101 => catalog = r.read_string()?,
            102 => schema = r.read_string()?,
            103 | 104 => {
                // temporary, internal (bool)
                let _ = r.read_u8();
            }
            105 => {
                // on_conflict - skip varint
                let _ = r.read_varint()?;
            }
            106 => {
                // sql - skip string
                let _ = r.read_string()?;
            }
            107 => {
                // comment - skip nested object
                r.skip_object_varint_only()?;
            }
            108 => {
                // tags - skip map
                let count = r.read_varint()? as usize;
                for _ in 0..count {
                    let _ = r.read_string()?; // key
                    let _ = r.read_string()?; // value
                }
            }
            109 => {
                skip_logical_dependency_list(r)?;
            }
            110 => {
                let _ = r.read_string()?;
            }
            // CreateTableInfo fields
            200 => table_name = r.read_string()?,
            201 => columns = read_column_list(r)?,
            202 => skip_constraints(r)?,
            203 => {
                // query - optional nested object
                r.skip_object_varint_only()?;
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }

    Ok((catalog, schema, table_name, columns))
}

fn read_catalog_entry_object(r: &mut BinaryDeserializer<'_>) -> io::Result<Option<CatalogEntry>> {
    let mut catalog_type = 0u32;
    let mut catalog = String::new();
    let mut schema = String::new();
    let mut name = String::new();
    let mut columns = Vec::new();
    let mut total_rows = 0u64;
    let mut table_pointer = None;

    loop {
        let fid = r.read_field_id();
        if fid == 0xFFFF {
            break;
        }
        match fid {
            99 => {
                catalog_type = r.read_varint()? as u32;
            }
            100 => {
                // Field 100 is the CreateInfo serialized as a nullable pointer
                // First read the present byte (1 = present, 0 = null)
                let present = r.read_u8();
                if present == 1 {
                    let (cat, sch, tbl, cols) = read_create_table_info(r)?;
                    catalog = cat;
                    schema = sch;
                    name = tbl;
                    columns = cols;
                }
            }
            101 => {
                // Table storage info - a nested object containing MetaBlockPointer
                table_pointer = read_meta_block_pointer_nested(r)?;
            }
            102 => {
                total_rows = r.read_varint()?;
            }
            103 | 104 => {
                // Index pointers - skip as nested list
                let count = r.read_varint()? as usize;
                for _ in 0..count {
                    r.skip_object_varint_only()?;
                }
            }
            _ => {
                if r.stream.is_eof() {
                    break;
                }
                let _ = r.read_varint()?;
            }
        }
    }

    // Return the entry if we have a name and columns
    if !name.is_empty() && !columns.is_empty() {
        Ok(Some(CatalogEntry {
            catalog_type,
            catalog,
            schema,
            name,
            columns,
            total_rows,
            table_pointer,
        }))
    } else {
        Ok(None)
    }
}

fn read_meta_block_pointer_nested(
    r: &mut BinaryDeserializer<'_>,
) -> io::Result<Option<MetaBlockPointer>> {
    // MetaBlockPointer is serialized directly as an object (not as a nullable pointer):
    // field 100: block_pointer (varint)
    // field 101: offset (varint) - optional
    // terminator: 0xFFFF
    let mut block_pointer = u64::MAX;
    let mut offset = 0u32;

    loop {
        let fid = r.read_field_id();
        if fid == MESSAGE_TERMINATOR_FIELD_ID {
            return Ok(Some(MetaBlockPointer {
                block_pointer,
                offset,
            }));
        }
        match fid {
            100 => {
                block_pointer = r.read_varint()?;
            }
            101 => {
                offset = r.read_varint()? as u32;
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }
}

fn read_table_storage_info(r: &mut BinaryDeserializer<'_>) -> io::Result<Option<MetaBlockPointer>> {
    let table_pointer = None;
    loop {
        if r.stream.is_eof() {
            return Ok(table_pointer);
        }
        let fid = r.read_field_id();
        if fid == MESSAGE_TERMINATOR_FIELD_ID {
            return Ok(table_pointer);
        }
        match fid {
            // fid=100：MetaBlockPointer 直接存储为打包的 VarInt（C++ StorageIndex::root）
            // 读到就立即返回，不能继续循环（fid=101 是简单 varint，不是嵌套对象）
            100 => {
                let raw_ptr = r.read_varint()?;
                return Ok(Some(MetaBlockPointer {
                    block_pointer: raw_ptr,
                    offset: 0,
                }));
            }
            101 => {
                // fid=101 是简单 varint（如 row_group_count），跳过
                let _ = r.read_varint()?;
            }
            102 => {
                let _ = r.read_varint()?;
            }
            103 => {
                let count = r.read_varint()? as usize;
                for _ in 0..count {
                    skip_block_pointer(r)?;
                }
            }
            104 => {
                let count = r.read_varint()? as usize;
                for _ in 0..count {
                    r.skip_object_varint_only()?;
                }
            }
            _ => {
                if r.stream.is_eof() {
                    return Ok(table_pointer);
                }
                let _ = r.read_varint()?;
            }
        }
    }
}

fn read_meta_block_pointer(r: &mut BinaryDeserializer<'_>) -> io::Result<MetaBlockPointer> {
    let mut block_pointer = u64::MAX;
    let mut offset = 0u32;
    loop {
        let fid = r.read_field_id();
        if fid == MESSAGE_TERMINATOR_FIELD_ID {
            return Ok(MetaBlockPointer {
                block_pointer,
                offset,
            });
        }
        match fid {
            100 => block_pointer = r.read_varint()?,
            101 => offset = r.read_varint()? as u32,
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }
}

fn skip_block_pointer(r: &mut BinaryDeserializer<'_>) -> io::Result<()> {
    loop {
        let fid = r.read_field_id();
        if fid == MESSAGE_TERMINATOR_FIELD_ID {
            return Ok(());
        }
        let _ = r.read_varint()?;
    }
}

pub fn read_catalog(stream: &mut dyn ReadStream) -> io::Result<Vec<CatalogEntry>> {
    let mut reader = BinaryDeserializer::new(stream);
    let mut entries = Vec::new();

    // The catalog is serialized as:
    // field 100: list of catalog entries (schemas, tables, views, etc.)
    // Each entry is an object with:
    //   field 99: catalog_type (1=TABLE, 2=SCHEMA, etc.)
    //   field 100: CreateInfo (CreateTableInfo, CreateSchemaInfo, etc.)
    //   field 101+: type-specific data (e.g., table storage info)

    loop {
        let fid = reader.read_field_id();
        if fid == 0xFFFF {
            break;
        }
        if fid == 100 {
            // This is a list of all catalog entries
            let entry_count = reader.read_varint()? as usize;
            for _ in 0..entry_count {
                if reader.stream.is_eof() {
                    break;
                }
                if let Some(entry) = read_catalog_entry_object(&mut reader)? {
                    entries.push(entry);
                }
            }
        } else {
            let _ = reader.read_varint()?;
        }
    }

    Ok(entries)
}

// These functions are no longer needed - all entries are in the top-level list

fn skip_schema_info(r: &mut BinaryDeserializer<'_>) -> io::Result<()> {
    loop {
        let fid = r.read_field_id();
        if fid == 0xFFFF {
            return Ok(());
        }
        match fid {
            100 | 101 | 102 => {
                let _ = r.read_string()?;
            }
            _ => {
                let _ = r.read_varint()?;
            }
        }
    }
}
