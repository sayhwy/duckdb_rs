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

type StructPair = (i32, i32);
type ListStructRows = Vec<Vec<StructPair>>;
type StructListArrayRow = (Vec<i32>, Vec<i32>);
type NestedListRows = Vec<Vec<Vec<i32>>>;
type StructStructRow = ((i32, i32), i32);

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
    let list_struct_status = verify_list_struct_case();
    let struct_nested_status = verify_struct_nested_case();
    let list_list_status = verify_list_list_case();
    let struct_struct_status = verify_struct_struct_case();

    println!();
    println!("=== Summary ===");
    println!("LIST        : {}", list_status);
    println!("LIST NULL   : {}", list_null_status);
    println!("ARRAY       : {}", array_status);
    println!("ARRAY NULL  : {}", array_null_status);
    println!("STRUCT      : {}", struct_status);
    println!("STRUCT NULL : {}", struct_null_status);
    println!("LIST<STRUCT>: {}", list_struct_status);
    println!("STRUCT<N..> : {}", struct_nested_status);
    println!("LIST<LIST>  : {}", list_list_status);
    println!("STRUCT<S..> : {}", struct_struct_status);
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
    let conn = engine.connect();
    conn.create_table("list_t", vec![("v".to_string(), list_type.clone())])
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
    let conn = engine.connect();
    conn.create_table("list_null_t", vec![("v".to_string(), list_type.clone())])
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
    let conn = engine.connect();
    conn.create_table("array_t", vec![("v".to_string(), array_type.clone())])
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
    let conn = engine.connect();
    conn.create_table("array_null_t", vec![("v".to_string(), array_type.clone())])
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
    let conn = engine.connect();
    conn.create_table("struct_t", vec![("v".to_string(), struct_type.clone())])
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
    let conn = engine.connect();
    conn.create_table("struct_null_t", vec![("v".to_string(), struct_type.clone())])
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

fn verify_list_struct_case() -> &'static str {
    println!("-- LIST<STRUCT> case");
    let db_path = "nested_list_struct_test.db";
    cleanup_db_files(db_path);

    let struct_type = LogicalType::struct_type(vec![
        ("id".to_string(), LogicalType::integer()),
        ("score".to_string(), LogicalType::integer()),
    ]);
    let list_type = LogicalType::list(struct_type.clone());
    let expected: ListStructRows = vec![
        vec![(1, 10), (2, 20)],
        vec![],
        vec![(3, 30)],
    ];
    let mut chunk = make_list_struct_chunk(&list_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let conn = engine.connect();
    conn.create_table("list_struct_t", vec![("v".to_string(), list_type.clone())])
    .expect("创建 list_struct_t 失败");
    conn.begin_transaction()
        .expect("LIST<STRUCT> begin_transaction 失败");
    conn.insert("list_struct_t", &mut chunk)
        .expect("LIST<STRUCT> insert 失败");
    conn.commit().expect("LIST<STRUCT> commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("list_struct_t", None).expect("LIST<STRUCT> scan 失败");
        collect_list_struct(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("LIST<STRUCT> memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("LIST<STRUCT> checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2
            .scan("list_struct_t", None)
            .expect("LIST<STRUCT> reopen scan 失败");
        collect_list_struct(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("LIST<STRUCT> reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_struct_nested_case() -> &'static str {
    println!("-- STRUCT<LIST,ARRAY> case");
    let db_path = "nested_struct_nested_test.db";
    cleanup_db_files(db_path);

    let struct_type = LogicalType::struct_type(vec![
        ("items".to_string(), LogicalType::list(LogicalType::integer())),
        ("fixed".to_string(), LogicalType::array(LogicalType::integer(), 2)),
    ]);
    let expected: Vec<StructListArrayRow> = vec![
        (vec![1, 2], vec![10, 11]),
        (vec![], vec![20, 21]),
        (vec![3], vec![30, 31]),
    ];
    let mut chunk = make_struct_list_array_chunk(&struct_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let conn = engine.connect();
    conn.create_table("struct_nested_t", vec![("v".to_string(), struct_type.clone())])
    .expect("创建 struct_nested_t 失败");
    conn.begin_transaction()
        .expect("STRUCT<LIST,ARRAY> begin_transaction 失败");
    conn.insert("struct_nested_t", &mut chunk)
        .expect("STRUCT<LIST,ARRAY> insert 失败");
    conn.commit().expect("STRUCT<LIST,ARRAY> commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn
            .scan("struct_nested_t", None)
            .expect("STRUCT<LIST,ARRAY> scan 失败");
        collect_struct_list_array(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT<LIST,ARRAY> memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine
            .checkpoint()
            .expect("STRUCT<LIST,ARRAY> checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2
            .scan("struct_nested_t", None)
            .expect("STRUCT<LIST,ARRAY> reopen scan 失败");
        collect_struct_list_array(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT<LIST,ARRAY> reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_list_list_case() -> &'static str {
    println!("-- LIST<LIST> case");
    let db_path = "nested_list_list_test.db";
    cleanup_db_files(db_path);

    let list_type = LogicalType::list(LogicalType::list(LogicalType::integer()));
    let expected: NestedListRows = vec![
        vec![vec![1, 2], vec![]],
        vec![],
        vec![vec![3], vec![4, 5]],
    ];
    let mut chunk = make_list_list_i32_chunk(&list_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let conn = engine.connect();
    conn.create_table("list_list_t", vec![("v".to_string(), list_type.clone())])
    .expect("创建 list_list_t 失败");
    conn.begin_transaction()
        .expect("LIST<LIST> begin_transaction 失败");
    conn.insert("list_list_t", &mut chunk)
        .expect("LIST<LIST> insert 失败");
    conn.commit().expect("LIST<LIST> commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn.scan("list_list_t", None).expect("LIST<LIST> scan 失败");
        collect_list_list_i32(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("LIST<LIST> memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine.checkpoint().expect("LIST<LIST> checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2
            .scan("list_list_t", None)
            .expect("LIST<LIST> reopen scan 失败");
        collect_list_list_i32(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("LIST<LIST> reopen scan panic".to_string()));
    println!("  actual(reopen scan):                 {:?}", actual_disk);

    if actual_mem.as_ref().ok() == Some(&expected) && actual_disk.as_ref().ok() == Some(&expected) {
        println!("  result: PASS");
        "PASS"
    } else {
        println!("  result: EXPECTED GAP");
        "EXPECTED GAP"
    }
}

fn verify_struct_struct_case() -> &'static str {
    println!("-- STRUCT<STRUCT> case");
    let db_path = "nested_struct_struct_test.db";
    cleanup_db_files(db_path);

    let inner_type = LogicalType::struct_type(vec![
        ("id".to_string(), LogicalType::integer()),
        ("score".to_string(), LogicalType::integer()),
    ]);
    let struct_type = LogicalType::struct_type(vec![
        ("person".to_string(), inner_type),
        ("level".to_string(), LogicalType::integer()),
    ]);
    let expected: Vec<StructStructRow> = vec![((1, 10), 100), ((2, 20), 200), ((3, 30), 300)];
    let mut chunk = make_struct_struct_chunk(&struct_type, &expected);

    let engine = DuckEngine::open(db_path).expect("打开数据库失败");
    let conn = engine.connect();
    conn.create_table("struct_struct_t", vec![("v".to_string(), struct_type.clone())])
    .expect("创建 struct_struct_t 失败");
    conn.begin_transaction()
        .expect("STRUCT<STRUCT> begin_transaction 失败");
    conn.insert("struct_struct_t", &mut chunk)
        .expect("STRUCT<STRUCT> insert 失败");
    conn.commit().expect("STRUCT<STRUCT> commit 失败");

    let actual_mem = catch_unwind(AssertUnwindSafe(|| {
        let scanned = conn
            .scan("struct_struct_t", None)
            .expect("STRUCT<STRUCT> scan 失败");
        collect_struct_struct(&scanned, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT<STRUCT> memory scan panic".to_string()));
    println!("  expected(in DuckDB vector semantics): {:?}", expected);
    println!("  actual(memory scan):                 {:?}", actual_mem);

    let actual_disk = catch_unwind(AssertUnwindSafe(|| {
        engine
            .checkpoint()
            .expect("STRUCT<STRUCT> checkpoint 失败");
        drop(conn);
        drop(engine);

        let engine2 = DuckEngine::open(db_path).expect("重新打开数据库失败");
        let conn2 = engine2.connect();
        let scanned2 = conn2
            .scan("struct_struct_t", None)
            .expect("STRUCT<STRUCT> reopen scan 失败");
        collect_struct_struct(&scanned2, 0)
    }))
    .unwrap_or_else(|_| Err("STRUCT<STRUCT> reopen scan panic".to_string()));
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

fn make_list_struct_chunk(list_type: &LogicalType, rows: &[Vec<StructPair>]) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(list_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    let child_capacity: usize = rows.iter().map(Vec::len).sum();
    let child_type = list_type
        .get_child_type()
        .expect("LIST<STRUCT> 缺少 child type")
        .clone();
    let mut child = Vector::with_capacity(child_type, child_capacity);

    let mut child_offset = 0usize;
    for (row_idx, row) in rows.iter().enumerate() {
        write_list_entry(vector, row_idx, child_offset as u32, row.len() as u32);
        vector.validity.set_valid(row_idx);
        for (i, (id, score)) in row.iter().enumerate() {
            let dst = child_offset + i;
            child.validity.set_valid(dst);
            let children = child.get_children_mut();
            assert_eq!(children.len(), 2, "LIST<STRUCT> child fields mismatch");
            write_i32(&mut children[0], dst, *id);
            write_i32(&mut children[1], dst, *score);
        }
        child_offset += row.len();
    }

    vector.set_child(child);
    chunk
}

fn make_struct_list_array_chunk(
    struct_type: &LogicalType,
    rows: &[StructListArrayRow],
) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(struct_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    let total_list_child_count: usize = rows.iter().map(|(items, _)| items.len()).sum();

    for row_idx in 0..rows.len() {
        vector.validity.set_valid(row_idx);
    }

    {
        let children = vector.get_children_mut();
        assert_eq!(children.len(), 2, "STRUCT<LIST,ARRAY> children mismatch");
        let (list_children, array_children) = children.split_at_mut(1);
        let list_vector = &mut list_children[0];
        let array_vector = &mut array_children[0];
        let list_child_type = list_vector
            .logical_type
            .get_child_type()
            .expect("STRUCT field LIST 缺少 child type")
            .clone();
        let mut list_child = Vector::with_capacity(list_child_type, total_list_child_count);
        if array_vector.get_child().is_none() {
            let array_child_type = array_vector
                .logical_type
                .get_child_type()
                .expect("STRUCT field ARRAY 缺少 child type")
                .clone();
            array_vector.set_child(Vector::with_capacity(array_child_type, rows.len() * 2));
        }
        let mut list_child_offset = 0usize;

        for (row_idx, (items, fixed)) in rows.iter().enumerate() {
            write_list_entry(list_vector, row_idx, list_child_offset as u32, items.len() as u32);
            list_vector.validity.set_valid(row_idx);
            for (i, value) in items.iter().enumerate() {
                write_i32(&mut list_child, list_child_offset + i, *value);
            }
            list_child_offset += items.len();

            assert_eq!(fixed.len(), 2, "STRUCT field ARRAY 输入长度必须固定");
            array_vector.validity.set_valid(row_idx);
            let array_child = array_vector
                .get_child_mut()
                .expect("STRUCT field ARRAY 缺少 child vector");
            write_i32(array_child, row_idx * 2, fixed[0]);
            write_i32(array_child, row_idx * 2 + 1, fixed[1]);
        }

        list_vector.set_child(list_child);
    }

    chunk
}

fn make_list_list_i32_chunk(list_type: &LogicalType, rows: &[Vec<Vec<i32>>]) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(list_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    let outer_child_count: usize = rows.iter().map(Vec::len).sum();
    let total_inner_values: usize = rows.iter().flat_map(|row| row.iter()).map(Vec::len).sum();

    let outer_child_type = list_type
        .get_child_type()
        .expect("LIST<LIST> outer child type missing")
        .clone();
    let mut outer_child = Vector::with_capacity(outer_child_type, outer_child_count);
    let inner_child_type = outer_child
        .logical_type
        .get_child_type()
        .expect("LIST<LIST> inner child type missing")
        .clone();
    let mut inner_child = Vector::with_capacity(inner_child_type, total_inner_values);

    let mut outer_offset = 0usize;
    let mut inner_offset = 0usize;
    for (row_idx, row) in rows.iter().enumerate() {
        write_list_entry(vector, row_idx, outer_offset as u32, row.len() as u32);
        vector.validity.set_valid(row_idx);
        for (nested_idx, nested) in row.iter().enumerate() {
            let dst = outer_offset + nested_idx;
            write_list_entry(&mut outer_child, dst, inner_offset as u32, nested.len() as u32);
            outer_child.validity.set_valid(dst);
            for (i, value) in nested.iter().enumerate() {
                write_i32(&mut inner_child, inner_offset + i, *value);
            }
            inner_offset += nested.len();
        }
        outer_offset += row.len();
    }

    outer_child.set_child(inner_child);
    vector.set_child(outer_child);
    chunk
}

fn make_struct_struct_chunk(struct_type: &LogicalType, rows: &[StructStructRow]) -> DataChunk {
    let mut chunk = DataChunk::new();
    chunk.initialize(std::slice::from_ref(struct_type), rows.len());
    chunk.set_cardinality(rows.len());

    let vector = &mut chunk.data[0];
    for (row_idx, ((id, score), level)) in rows.iter().enumerate() {
        vector.validity.set_valid(row_idx);
        let children = vector.get_children_mut();
        assert_eq!(children.len(), 2, "STRUCT<STRUCT> children mismatch");

        let inner_struct = &mut children[0];
        inner_struct.validity.set_valid(row_idx);
        {
            let inner_children = inner_struct.get_children_mut();
            assert_eq!(inner_children.len(), 2, "STRUCT<STRUCT> inner children mismatch");
            write_i32(&mut inner_children[0], row_idx, *id);
            write_i32(&mut inner_children[1], row_idx, *score);
        }
        write_i32(&mut children[1], row_idx, *level);
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

fn collect_list_struct(chunks: &[DataChunk], col_idx: usize) -> Result<ListStructRows, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let Some(child) = vector.get_child() else {
            return Err("LIST<STRUCT> scan result 缺少 child vector".to_string());
        };
        let struct_children = child.get_children();
        if struct_children.len() != 2 {
            return Err("LIST<STRUCT> child struct 缺少子向量".to_string());
        }
        for row_idx in 0..chunk.size() {
            let (offset, len) = read_list_entry(vector, row_idx);
            let mut row = Vec::new();
            for i in 0..len as usize {
                let idx = offset as usize + i;
                row.push((read_i32(&struct_children[0], idx), read_i32(&struct_children[1], idx)));
            }
            result.push(row);
        }
    }
    Ok(result)
}

fn collect_struct_list_array(
    chunks: &[DataChunk],
    col_idx: usize,
) -> Result<Vec<StructListArrayRow>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let children = vector.get_children();
        if children.len() != 2 {
            return Err("STRUCT<LIST,ARRAY> scan result 缺少子向量".to_string());
        }

        let list_vector = &children[0];
        let Some(list_child) = list_vector.get_child() else {
            return Err("STRUCT<LIST,ARRAY> list field 缺少 child vector".to_string());
        };
        let array_vector = &children[1];
        let Some(array_child) = array_vector.get_child() else {
            return Err("STRUCT<LIST,ARRAY> array field 缺少 child vector".to_string());
        };

        for row_idx in 0..chunk.size() {
            let (offset, len) = read_list_entry(list_vector, row_idx);
            let mut list_values = Vec::new();
            for i in 0..len as usize {
                list_values.push(read_i32(list_child, offset as usize + i));
            }

            let base = row_idx * 2;
            let array_values = vec![read_i32(array_child, base), read_i32(array_child, base + 1)];
            result.push((list_values, array_values));
        }
    }
    Ok(result)
}

fn collect_list_list_i32(chunks: &[DataChunk], col_idx: usize) -> Result<NestedListRows, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let Some(outer_child) = vector.get_child() else {
            return Err("LIST<LIST> scan result 缺少 outer child vector".to_string());
        };
        let Some(inner_child) = outer_child.get_child() else {
            return Err("LIST<LIST> outer child 缺少 inner child vector".to_string());
        };
        for row_idx in 0..chunk.size() {
            let (outer_offset, outer_len) = read_list_entry(vector, row_idx);
            let mut outer_row = Vec::new();
            for i in 0..outer_len as usize {
                let (inner_offset, inner_len) = read_list_entry(outer_child, outer_offset as usize + i);
                let mut inner_row = Vec::new();
                for j in 0..inner_len as usize {
                    inner_row.push(read_i32(inner_child, inner_offset as usize + j));
                }
                outer_row.push(inner_row);
            }
            result.push(outer_row);
        }
    }
    Ok(result)
}

fn collect_struct_struct(chunks: &[DataChunk], col_idx: usize) -> Result<Vec<StructStructRow>, String> {
    let mut result = Vec::new();
    for chunk in chunks {
        let vector = &chunk.data[col_idx];
        let children = vector.get_children();
        if children.len() != 2 {
            return Err("STRUCT<STRUCT> scan result 缺少子向量".to_string());
        }
        let inner_struct = &children[0];
        let inner_children = inner_struct.get_children();
        if inner_children.len() != 2 {
            return Err("STRUCT<STRUCT> inner struct 缺少子向量".to_string());
        }
        for row_idx in 0..chunk.size() {
            result.push((
                (read_i32(&inner_children[0], row_idx), read_i32(&inner_children[1], row_idx)),
                read_i32(&children[1], row_idx),
            ));
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
