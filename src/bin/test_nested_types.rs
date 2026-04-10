//! 嵌套类型现状验证：
//!
//! - LIST: 按 DuckDB `list_entry_t + child vector` 语义构造输入，验证当前写入/扫描链路
//! - ARRAY: 按 DuckDB `ArrayVector::GetEntry(vector)` 语义构造输入，验证当前写入/扫描链路
//! - STRUCT: 按 DuckDB `StructVector::GetEntries()` 语义构造输入并验证写入/扫描链路
//!
//! 该用例的目标是“如实反映当前 duckdb_rs 与 DuckDB C++ 的对齐程度”，
//! 不对未实现路径做 workaround。

use std::path::Path;
use std::panic::{AssertUnwindSafe, catch_unwind};

use duckdb_rs::common::types::{DataChunk, LogicalType, Vector};
use duckdb_rs::db::DuckEngine;

fn main() {
    println!();
    println!("=== Nested Type Status Check ===");
    println!("DuckDB source: /Users/liang/Documents/code/duckdb");
    println!("DB files: nested_list_test.db / nested_array_test.db");
    println!();

    let list_status = verify_list_case();
    let list_null_status = verify_list_null_case();
    let array_status = verify_array_case();
    let array_null_status = verify_array_null_case();
    let struct_status = verify_struct_case();
    let struct_null_status = verify_struct_null_case();

    println!();
    println!("=== Summary ===");
    println!("LIST        : {}", list_status);
    println!("LIST NULL   : {}", list_null_status);
    println!("ARRAY       : {}", array_status);
    println!("ARRAY NULL  : {}", array_null_status);
    println!("STRUCT      : {}", struct_status);
    println!("STRUCT NULL : {}", struct_null_status);
    println!();
}

fn verify_list_case() -> &'static str {
    println!("-- LIST case");
    let db_path = "nested_list_test.db";
    cleanup_db_files(db_path);

    let list_type = LogicalType::list(LogicalType::integer());
    let expected = vec![vec![10, 11], vec![20], vec![], vec![30, 31, 32]];
    let mut chunk = make_list_i32_chunk(&list_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let mut conn = engine.connect();
    conn.create_table(
        "main",
        "list_t",
        vec![("v".to_string(), list_type.clone())],
    )
    .expect("创建 list_t 失败");
    conn.begin_transaction().expect("LIST begin_transaction 失败");
    conn.insert("list_t", &mut chunk).expect("LIST insert 失败");
    conn.commit().expect("LIST commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("list_t", None).expect("LIST scan 失败");
        collect_list_i32(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("LIST memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("LIST checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2.scan("list_t", None).expect("LIST reopen scan 失败");
        collect_list_i32(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("LIST reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_list_null_case() -> &'static str {
    println!("-- LIST NULL case");
    let db_path = "nested_list_null_test.db";
    cleanup_db_files(db_path);

    let list_type = LogicalType::list(LogicalType::integer());
    let expected = vec![Some(vec![10, 11]), None, Some(vec![]), Some(vec![30])];
    let mut chunk = make_nullable_list_i32_chunk(&list_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let mut conn = engine.connect();
    conn.create_table("main", "list_null_t", vec![("v".to_string(), list_type.clone())])
        .expect("创建 list_null_t 失败");
    conn.begin_transaction()
        .expect("LIST NULL begin_transaction 失败");
    conn.insert("list_null_t", &mut chunk)
        .expect("LIST NULL insert 失败");
    conn.commit().expect("LIST NULL commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("list_null_t", None).expect("LIST NULL scan 失败");
        collect_nullable_list_i32(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("LIST NULL memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("LIST NULL checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2
            .scan("list_null_t", None)
            .expect("LIST NULL reopen scan 失败");
        collect_nullable_list_i32(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("LIST NULL reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_array_case() -> &'static str {
    println!("-- ARRAY case");
    let db_path = "nested_array_test.db";
    cleanup_db_files(db_path);

    let array_type = LogicalType::array(LogicalType::integer(), 3);
    let expected = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];
    let mut chunk = make_array_i32_chunk(&array_type, 3, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let mut conn = engine.connect();
    conn.create_table(
        "main",
        "array_t",
        vec![("v".to_string(), array_type.clone())],
    )
    .expect("创建 array_t 失败");
    conn.begin_transaction().expect("ARRAY begin_transaction 失败");
    conn.insert("array_t", &mut chunk).expect("ARRAY insert 失败");
    conn.commit().expect("ARRAY commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("array_t", None).expect("ARRAY scan 失败");
        collect_array_i32(&scanned, 0, 3)
    }))
    .unwrap_or_else(|_| Err("ARRAY memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("ARRAY checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2.scan("array_t", None).expect("ARRAY reopen scan 失败");
        collect_array_i32(&scanned2, 0, 3)
    }))
    .unwrap_or_else(|_| Err("ARRAY reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_array_null_case() -> &'static str {
    println!("-- ARRAY NULL case");
    let db_path = "nested_array_null_test.db";
    cleanup_db_files(db_path);

    let array_type = LogicalType::array(LogicalType::integer(), 3);
    let expected = vec![Some(vec![1, 2, 3]), None, Some(vec![7, 8, 9])];
    let mut chunk = make_nullable_array_i32_chunk(&array_type, 3, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let mut conn = engine.connect();
    conn.create_table("main", "array_null_t", vec![("v".to_string(), array_type.clone())])
        .expect("创建 array_null_t 失败");
    conn.begin_transaction()
        .expect("ARRAY NULL begin_transaction 失败");
    conn.insert("array_null_t", &mut chunk)
        .expect("ARRAY NULL insert 失败");
    conn.commit().expect("ARRAY NULL commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("array_null_t", None).expect("ARRAY NULL scan 失败");
        collect_nullable_array_i32(&scanned, 0, 3)
    }))
    .unwrap_or_else(|_| Err("ARRAY NULL memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("ARRAY NULL checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2
            .scan("array_null_t", None)
            .expect("ARRAY NULL reopen scan 失败");
        collect_nullable_array_i32(&scanned2, 0, 3)
    }))
    .unwrap_or_else(|_| Err("ARRAY NULL reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_struct_case() -> &'static str {
    println!("-- STRUCT case");
    let db_path = "nested_struct_test.db";
    cleanup_db_files(db_path);

    let struct_type = LogicalType::struct_type(vec![
        ("id".to_string(), LogicalType::integer()),
        ("score".to_string(), LogicalType::integer()),
    ]);
    let expected = vec![(1, 90), (2, 80), (3, 70)];
    let mut chunk = make_struct_i32_chunk(&struct_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let mut conn = engine.connect();
    conn.create_table(
        "main",
        "struct_t",
        vec![("v".to_string(), struct_type.clone())],
    )
    .expect("创建 struct_t 失败");
    conn.begin_transaction().expect("STRUCT begin_transaction 失败");
    conn.insert("struct_t", &mut chunk).expect("STRUCT insert 失败");
    conn.commit().expect("STRUCT commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("struct_t", None).expect("STRUCT scan 失败");
        collect_struct_i32(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("STRUCT checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2.scan("struct_t", None).expect("STRUCT reopen scan 失败");
        collect_struct_i32(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_struct_null_case() -> &'static str {
    println!("-- STRUCT NULL case");
    let db_path = "nested_struct_null_test.db";
    cleanup_db_files(db_path);

    let struct_type = LogicalType::struct_type(vec![
        ("id".to_string(), LogicalType::integer()),
        ("score".to_string(), LogicalType::integer()),
    ]);
    let expected = vec![Some((1, 90)), None, Some((3, 70))];
    let mut chunk = make_nullable_struct_i32_chunk(&struct_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let mut conn = engine.connect();
    conn.create_table(
        "main",
        "struct_null_t",
        vec![("v".to_string(), struct_type.clone())],
    )
    .expect("创建 struct_null_t 失败");
    conn.begin_transaction()
        .expect("STRUCT NULL begin_transaction 失败");
    conn.insert("struct_null_t", &mut chunk)
        .expect("STRUCT NULL insert 失败");
    conn.commit().expect("STRUCT NULL commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("struct_null_t", None).expect("STRUCT NULL scan 失败");
        collect_nullable_struct_i32(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT NULL memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("STRUCT NULL checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2
            .scan("struct_null_t", None)
            .expect("STRUCT NULL reopen scan 失败");
        collect_nullable_struct_i32(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT NULL reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn cleanup_db_files(db_path: &str) {
    let wal_path = format!("{}.wal", db_path);
    for path in [db_path, wal_path.as_str()] {
        if Path::new(path).exists() {
            std::fs::remove_file(path).unwrap_or_else(|e| panic!("删除 {} 失败: {}", path, e));
        }
    }
}

fn make_list_i32_chunk(list_type: &LogicalType, rows: &[Vec<i32>]) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(list_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    let mut child = Vector::with_capacity(
        list_type
            .get_child_type()
            .expect("LIST 缺少 child type")
            .clone(),
        rows.iter().map(|r| r.len()).sum(),
    );

    let mut child_offset = 0usize;
    for (row_idx, row) in rows.iter().enumerate() {
        write_list_entry(vector, row_idx, child_offset as u32, row.len() as u32);
        vector.validity.set_valid(row_idx);
        for (i, value) in row.iter().enumerate() {
            write_i32(&mut child, child_offset + i, *value);
        }
        child_offset += row.len();
    }
    vector.set_child(child);
    chunk
}

fn make_nullable_list_i32_chunk(list_type: &LogicalType, rows: &[Option<Vec<i32>>]) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(list_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    let child_capacity: usize = rows
        .iter()
        .filter_map(|row| row.as_ref().map(Vec::len))
        .sum();
    let mut child = Vector::with_capacity(
        list_type
            .get_child_type()
            .expect("LIST 缺少 child type")
            .clone(),
        child_capacity,
    );

    let mut child_offset = 0usize;
    for (row_idx, row) in rows.iter().enumerate() {
        match row {
            Some(values) => {
                write_list_entry(vector, row_idx, child_offset as u32, values.len() as u32);
                vector.validity.set_valid(row_idx);
                for (i, value) in values.iter().enumerate() {
                    write_i32(&mut child, child_offset + i, *value);
                }
                child_offset += values.len();
            }
            None => {
                write_list_entry(vector, row_idx, child_offset as u32, 0);
                vector.validity.set_invalid(row_idx);
            }
        }
    }
    vector.set_child(child);
    chunk
}

fn make_array_i32_chunk(array_type: &LogicalType, array_size: usize, rows: &[Vec<i32>]) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(array_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    let mut child = Vector::with_capacity(
        array_type
            .get_child_type()
            .expect("ARRAY 缺少 child type")
            .clone(),
        rows.len() * array_size,
    );

    for (row_idx, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), array_size, "ARRAY 输入长度必须固定");
        vector.validity.set_valid(row_idx);
        for (i, value) in row.iter().enumerate() {
            write_i32(&mut child, row_idx * array_size + i, *value);
        }
    }
    vector.set_child(child);
    chunk
}

fn make_nullable_array_i32_chunk(
    array_type: &LogicalType,
    array_size: usize,
    rows: &[Option<Vec<i32>>],
) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(array_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    let mut child = Vector::with_capacity(
        array_type
            .get_child_type()
            .expect("ARRAY 缺少 child type")
            .clone(),
        rows.len() * array_size,
    );

    for (row_idx, row) in rows.iter().enumerate() {
        let base = row_idx * array_size;
        match row {
            Some(values) => {
                assert_eq!(values.len(), array_size, "ARRAY 输入长度必须固定");
                vector.validity.set_valid(row_idx);
                for (i, value) in values.iter().enumerate() {
                    write_i32(&mut child, base + i, *value);
                }
            }
            None => {
                vector.validity.set_invalid(row_idx);
                for i in 0..array_size {
                    write_i32(&mut child, base + i, 0);
                }
            }
        }
    }
    vector.set_child(child);
    chunk
}

fn make_struct_i32_chunk(struct_type: &LogicalType, rows: &[(i32, i32)]) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(struct_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    for (row_idx, (id, score)) in rows.iter().enumerate() {
        vector.validity.set_valid(row_idx);
        let children = vector.get_children_mut();
        assert_eq!(children.len(), 2, "STRUCT children count mismatch");
        write_i32(&mut children[0], row_idx, *id);
        write_i32(&mut children[1], row_idx, *score);
    }
    chunk
}

fn make_nullable_struct_i32_chunk(
    struct_type: &LogicalType,
    rows: &[Option<(i32, i32)>],
) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(struct_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    for (row_idx, row) in rows.iter().enumerate() {
        match row {
            Some((id, score)) => {
                vector.validity.set_valid(row_idx);
                let children = vector.get_children_mut();
                assert_eq!(children.len(), 2, "STRUCT children count mismatch");
                write_i32(&mut children[0], row_idx, *id);
                write_i32(&mut children[1], row_idx, *score);
            }
            None => {
                vector.validity.set_invalid(row_idx);
                let children = vector.get_children_mut();
                assert_eq!(children.len(), 2, "STRUCT children count mismatch");
                write_i32(&mut children[0], row_idx, 0);
                write_i32(&mut children[1], row_idx, 0);
            }
        }
    }
    chunk
}

fn collect_list_i32(chunks: &[DataChunk], col_idx: usize) -> Result<Vec<Vec<i32>>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let Some(child) = vector.get_child() else {
            return Err("LIST scan result 缺少 child vector".to_string());
        };
        for row_idx in 0..chunk.size() {
            let (offset, len) = read_list_entry(vector, row_idx);
            let mut row = Vec::new();
            for i in 0..len as usize {
                row.push(read_i32(child, offset as usize + i));
            }
            result.push(row);
        }
    }
    Ok(result)
}

fn collect_nullable_list_i32(
    chunks: &[DataChunk],
    col_idx: usize,
) -> Result<Vec<Option<Vec<i32>>>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let Some(child) = vector.get_child() else {
            return Err("LIST scan result 缺少 child vector".to_string());
        };
        for row_idx in 0..chunk.size() {
            if !vector.validity.row_is_valid(row_idx) {
                result.push(None);
                continue;
            }
            let (offset, len) = read_list_entry(vector, row_idx);
            let mut row = Vec::new();
            for i in 0..len as usize {
                row.push(read_i32(child, offset as usize + i));
            }
            result.push(Some(row));
        }
    }
    Ok(result)
}

fn collect_array_i32(
    chunks: &[DataChunk],
    col_idx: usize,
    array_size: usize,
) -> Result<Vec<Vec<i32>>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let Some(child) = vector.get_child() else {
            return Err("ARRAY scan result 缺少 child vector".to_string());
        };
        for row_idx in 0..chunk.size() {
            let mut row = Vec::with_capacity(array_size);
            let base = row_idx * array_size;
            for i in 0..array_size {
                row.push(read_i32(child, base + i));
            }
            result.push(row);
        }
    }
    Ok(result)
}

fn collect_nullable_array_i32(
    chunks: &[DataChunk],
    col_idx: usize,
    array_size: usize,
) -> Result<Vec<Option<Vec<i32>>>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let Some(child) = vector.get_child() else {
            return Err("ARRAY scan result 缺少 child vector".to_string());
        };
        for row_idx in 0..chunk.size() {
            if !vector.validity.row_is_valid(row_idx) {
                result.push(None);
                continue;
            }
            let mut row = Vec::with_capacity(array_size);
            let base = row_idx * array_size;
            for i in 0..array_size {
                row.push(read_i32(child, base + i));
            }
            result.push(Some(row));
        }
    }
    Ok(result)
}

fn collect_struct_i32(chunks: &[DataChunk], col_idx: usize) -> Result<Vec<(i32, i32)>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let children = vector.get_children();
        if children.len() != 2 {
            return Err("STRUCT scan result 缺少子向量".to_string());
        }
        for row_idx in 0..chunk.size() {
            result.push((read_i32(&children[0], row_idx), read_i32(&children[1], row_idx)));
        }
    }
    Ok(result)
}

fn collect_nullable_struct_i32(
    chunks: &[DataChunk],
    col_idx: usize,
) -> Result<Vec<Option<(i32, i32)>>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let children = vector.get_children();
        if children.len() != 2 {
            return Err("STRUCT scan result 缺少子向量".to_string());
        }
        for row_idx in 0..chunk.size() {
            if !vector.validity.row_is_valid(row_idx) {
                result.push(None);
                continue;
            }
            result.push(Some((
                read_i32(&children[0], row_idx),
                read_i32(&children[1], row_idx),
            )));
        }
    }
    Ok(result)
}

fn write_i32(vector: &mut Vector, row_idx: usize, value: i32) {
    let offset = row_idx * 4;
    vector.raw_data_mut()[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    vector.validity.set_valid(row_idx);
}

fn read_i32(vector: &Vector, row_idx: usize) -> i32 {
    let offset = row_idx * 4;
    i32::from_le_bytes(
        vector.raw_data()[offset..offset + 4]
            .try_into()
            .expect("读取 i32 失败"),
    )
}

fn write_list_entry(vector: &mut Vector, row_idx: usize, offset: u32, len: u32) {
    let base = row_idx * 8;
    vector.raw_data_mut()[base..base + 4].copy_from_slice(&offset.to_le_bytes());
    vector.raw_data_mut()[base + 4..base + 8].copy_from_slice(&len.to_le_bytes());
}

fn read_list_entry(vector: &Vector, row_idx: usize) -> (u32, u32) {
    let base = row_idx * 8;
    let offset = u32::from_le_bytes(
        vector.raw_data()[base..base + 4]
            .try_into()
            .expect("读取 list offset 失败"),
    );
    let len = u32::from_le_bytes(
        vector.raw_data()[base + 4..base + 8]
            .try_into()
            .expect("读取 list length 失败"),
    );
    (offset, len)
}
