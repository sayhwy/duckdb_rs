//! 200 万学生嵌套类型持久化测试
//!
//! 按 DuckDB 的向量语义构造 LIST/ARRAY/STRUCT 数据，插入 200 万学生记录，
//! 关闭数据库并重新打开后扫描读取，并用 `DataChunk::pretty_print()` 打印首个 chunk。

use std::path::Path;
use std::process::Command;
use std::time::Instant;

use duckdb_rs::common::types::{DataChunk, LogicalType, Vector};
use duckdb_rs::db::DuckEngine;

const DB_PATH: &str = "million_students_nested.db";
const DUCKDB_EXE: &str = "/Users/liang/Documents/code/duckdb/bin/duckdb";
const TOTAL_STUDENTS: usize = 2_000_000;
const BATCH_SIZE: usize = 2048;
const FIXED_SCORE_COUNT: usize = 3;

fn main() {
    println!();
    println!("=== DuckDB Rust Nested Student Test ===");
    println!("DuckDB source: /Users/liang/Documents/code/duckdb");
    println!("DB path: {}", DB_PATH);
    println!("rows: {}", TOTAL_STUDENTS);
    println!();

    cleanup_db_files(DB_PATH);

    let student_types = student_types();
    let engine = DuckEngine::open(DB_PATH).expect("打开数据库失败");
    let conn = engine.connect();

    conn.create_table(
        "students_nested",
        vec![
            ("id".to_string(), student_types[0].clone()),
            ("course_ids".to_string(), student_types[1].clone()),
            ("scores".to_string(), student_types[2].clone()),
            ("profile".to_string(), student_types[3].clone()),
        ],
    )
    .expect("创建 students_nested 失败");

    println!("step 1/4: insert 2,000,000 rows");
    let insert_start = Instant::now();
    let total_batches = TOTAL_STUDENTS.div_ceil(BATCH_SIZE);
    let mut inserted = 0usize;
    for batch_idx in 0..total_batches {
        let start_id = batch_idx * BATCH_SIZE + 1;
        let count = (TOTAL_STUDENTS - batch_idx * BATCH_SIZE).min(BATCH_SIZE);
        let mut chunk = build_student_chunk(start_id, count);

        conn.begin_transaction()
            .unwrap_or_else(|e| panic!("batch {} begin_transaction 失败: {:?}", batch_idx + 1, e));
        conn.insert("students_nested", &mut chunk)
            .unwrap_or_else(|e| panic!("batch {} insert 失败: {:?}", batch_idx + 1, e));
        conn.commit()
            .unwrap_or_else(|e| panic!("batch {} commit 失败: {:?}", batch_idx + 1, e));

        inserted += count;
        if batch_idx % 100 == 0 || batch_idx == total_batches - 1 {
            println!("  inserted {}/{}", inserted, TOTAL_STUDENTS);
        }
    }
    println!("  insert finished in {:.3}s", insert_start.elapsed().as_secs_f64());
    println!();
    drop(conn);
    drop(engine);
    println!("step 2/4: close without explicit checkpoint");
    println!("  engine closed");
    println!();

    println!("step 3/4: reopen and scan");
    let reopen_start = Instant::now();
    let engine2 = DuckEngine::open(DB_PATH).expect("重新打开数据库失败");
    let conn2 = engine2.connect();
    conn2.begin_transaction().expect("scan 事务创建失败");
    let chunks = conn2.scan("students_nested", None).expect("scan 失败");
    conn2.commit().expect("scan 事务提交失败");
    println!(
        "  reopened and scanned {} chunks in {:.3}s",
        chunks.len(),
        reopen_start.elapsed().as_secs_f64()
    );
    println!();

    println!("step 4/4: verify row count and pretty print");
    let total_rows: usize = chunks.iter().map(|chunk| chunk.size()).sum();
    assert_eq!(
        total_rows, TOTAL_STUDENTS,
        "reopen 后行数不匹配，预期 {} 实际 {}",
        TOTAL_STUDENTS, total_rows
    );

    let first_chunk = chunks.first().expect("scan 结果为空");
    assert!(first_chunk.size() > 0, "首个 chunk 不能为空");
    assert_eq!(read_i32(&first_chunk.data[0], 0), 1, "首行 id 应为 1");

    println!("  reopen row count verified: {}", total_rows);
    println!("  first chunk pretty print:");
    first_chunk.pretty_print();

    println!();
    println!("step 5/5: verify current db with DuckDB CLI");
    verify_with_duckdb_cli();
    println!();
}

fn student_types() -> Vec<LogicalType> {
    vec![
        LogicalType::integer(),
        LogicalType::list(LogicalType::integer()),
        LogicalType::array(LogicalType::integer(), FIXED_SCORE_COUNT),
        LogicalType::struct_type(vec![
            ("age".to_string(), LogicalType::integer()),
            ("level".to_string(), LogicalType::integer()),
        ]),
    ]
}

fn build_student_chunk(start_id: usize, count: usize) -> DataChunk {
    let types = student_types();
    let mut chunk = DataChunk::new();
    chunk.initialize(&types, count);
    chunk.set_cardinality(count);

    build_id_column(&mut chunk.data[0], start_id, count);
    build_course_ids_column(&mut chunk.data[1], start_id, count);
    build_scores_column(&mut chunk.data[2], start_id, count);
    build_profile_column(&mut chunk.data[3], start_id, count);

    chunk
}

fn build_id_column(vector: &mut Vector, start_id: usize, count: usize) {
    for row_idx in 0..count {
        let id = (start_id + row_idx) as i32;
        write_i32(vector, row_idx, id);
    }
}

fn build_course_ids_column(vector: &mut Vector, start_id: usize, count: usize) {
    let child_capacity: usize = (0..count)
        .map(|row_idx| course_count((start_id + row_idx) as i32))
        .sum();
    let child_type = vector
        .logical_type
        .get_child_type()
        .expect("LIST 缺少 child type")
        .clone();
    let mut child = Vector::with_capacity(child_type, child_capacity);

    let mut child_offset = 0usize;
    for row_idx in 0..count {
        let id = (start_id + row_idx) as i32;
        let len = course_count(id);
        write_list_entry(vector, row_idx, child_offset as u32, len as u32);
        vector.validity.set_valid(row_idx);
        for i in 0..len {
            let course_id = 1000 + ((id + i as i32) % 200);
            write_i32(&mut child, child_offset + i, course_id);
        }
        child_offset += len;
    }
    vector.set_child(child);
}

fn build_scores_column(vector: &mut Vector, start_id: usize, count: usize) {
    let child_type = vector
        .logical_type
        .get_child_type()
        .expect("ARRAY 缺少 child type")
        .clone();
    let mut child = Vector::with_capacity(child_type, count * FIXED_SCORE_COUNT);

    for row_idx in 0..count {
        let id = (start_id + row_idx) as i32;
        vector.validity.set_valid(row_idx);
        let base = row_idx * FIXED_SCORE_COUNT;
        write_i32(&mut child, base, 60 + (id % 40));
        write_i32(&mut child, base + 1, 70 + (id % 25));
        write_i32(&mut child, base + 2, 80 + (id % 15));
    }
    vector.set_child(child);
}

fn build_profile_column(vector: &mut Vector, start_id: usize, count: usize) {
    for row_idx in 0..count {
        let id = (start_id + row_idx) as i32;
        vector.validity.set_valid(row_idx);
        let children = vector.get_children_mut();
        assert_eq!(children.len(), 2, "STRUCT children count mismatch");
        write_i32(&mut children[0], row_idx, 18 + (id % 10));
        write_i32(&mut children[1], row_idx, 1 + (id % 4));
    }
}

fn course_count(id: i32) -> usize {
    ((id % 3) + 1) as usize
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

fn cleanup_db_files(db_path: &str) {
    let wal_path = format!("{}.wal", db_path);
    for path in [db_path, wal_path.as_str()] {
        if Path::new(path).exists() {
            std::fs::remove_file(path).unwrap_or_else(|e| panic!("删除 {} 失败: {}", path, e));
        }
    }
}

fn verify_with_duckdb_cli() {
    if !Path::new(DUCKDB_EXE).exists() {
        panic!("DuckDB CLI 不存在: {}", DUCKDB_EXE);
    }

    let queries = [
        (
            "count(*)",
            "SELECT count(*) FROM students_nested;",
            vec![TOTAL_STUDENTS.to_string()],
        ),
        (
            "LIST",
            "SELECT id, array_to_string(course_ids, '|'), list_count(course_ids) FROM students_nested WHERE id IN (1, 2, 3) ORDER BY id;",
            vec![
                "1\t1001|1002\t2".to_string(),
                "2\t1002|1003|1004\t3".to_string(),
                "3\t1003\t1".to_string(),
            ],
        ),
        (
            "ARRAY",
            "SELECT id, scores[1], scores[2], scores[3] FROM students_nested WHERE id IN (1, 2, 3) ORDER BY id;",
            vec![
                "1\t61\t71\t81".to_string(),
                "2\t62\t72\t82".to_string(),
                "3\t63\t73\t83".to_string(),
            ],
        ),
        (
            "STRUCT",
            "SELECT id, struct_extract(profile, 'age'), struct_extract(profile, 'level') FROM students_nested WHERE id IN (1, 2, 3) ORDER BY id;",
            vec![
                "1\t19\t2".to_string(),
                "2\t20\t3".to_string(),
                "3\t21\t4".to_string(),
            ],
        ),
    ];

    for (label, sql, expected_lines) in queries {
        println!("  DuckDB verify {}:", label);
        let actual_lines = run_duckdb_query(sql);
        for line in &actual_lines {
            println!("    {}", line);
        }
        assert_eq!(
            actual_lines, expected_lines,
            "DuckDB CLI 验证 {} 失败\nSQL: {}\nexpected: {:?}\nactual: {:?}",
            label, sql, expected_lines, actual_lines
        );
    }
}

fn run_duckdb_query(sql: &str) -> Vec<String> {
    let output = Command::new(DUCKDB_EXE)
        .arg(DB_PATH)
        .arg("-csv")
        .arg("-c")
        .arg(sql)
        .output()
        .unwrap_or_else(|e| panic!("执行 DuckDB CLI 失败: {}", e));

    if !output.status.success() {
        panic!(
            "DuckDB CLI 查询失败\nSQL: {}\nstderr: {}",
            sql,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .skip(1)
        .map(|line| line.replace(',', "\t"))
        .filter(|line| !line.trim().is_empty())
        .collect()
}
