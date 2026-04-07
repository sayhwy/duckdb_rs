//! 混合类型学生数据测试
//!
//! 插入 100 条学生记录，字段覆盖：
//!   - id             INTEGER  (i32)
//!   - name           VARCHAR  (string_t, inline<=12)
//!   - gpa            FLOAT    (f32)
//!   - age            INTEGER  (i32)
//!   - is_active      BOOLEAN  (bool)
//!   - enrollment_day DATE     (days since 1970-01-01, int32)
//!
//! 运行:
//!   cargo run --bin student_mixed_types
//!
//! 验证:
//!   D:/duckdb.exe student_mixed_types.db -c "SELECT * FROM students LIMIT 10;"

use std::path::Path;
use std::process::Command;

use duckdb_rs::common::types::{DataChunk, LogicalType};
use duckdb_rs::db::DuckEngine;

const DUCKDB_EXE: &str = "/Users/liang/Documents/code/duckdb/bin/duckdb";
const DB_PATH: &str = "student_mixed_types.db";
const ROW_COUNT: usize = 100;

fn main() {
    println!("=== 混合类型学生数据测试 ===\n");

    // 清除旧数据库
    if Path::new(DB_PATH).exists() {
        std::fs::remove_file(DB_PATH).expect("删除旧数据库失败");
    }

    // 打开数据库
    let engine = DuckEngine::open(DB_PATH).expect("打开数据库失败");
    let mut conn = engine.connect();

    // 创建学生表（"main" schema 已预注册）
    conn.create_table(
        "main",
        "students",
        vec![
            ("id".to_string(), LogicalType::integer()),
            ("name".to_string(), LogicalType::varchar()),
            ("gpa".to_string(), LogicalType::float()),
            ("age".to_string(), LogicalType::integer()),
            ("is_active".to_string(), LogicalType::boolean()),
            ("enrollment_day".to_string(), LogicalType::date()),
        ],
    )
    .expect("创建表失败");
    println!("创建表 students 成功");

    // 生成 100 条学生数据
    let students = generate_students(ROW_COUNT);

    // 构建 DataChunk
    let mut chunk = DataChunk::new();
    chunk.initialize(
        &[
            LogicalType::integer(),
            LogicalType::varchar(),
            LogicalType::float(),
            LogicalType::integer(),
            LogicalType::boolean(),
            LogicalType::date(),
        ],
        ROW_COUNT,
    );

    for (i, s) in students.iter().enumerate() {
        let (id, name, age, gpa, is_active, enrollment_day) = *s;

        // col 0: id (INTEGER, 4 bytes)
        let off = i * 4;
        chunk.data[0].raw_data_mut()[off..off + 4].copy_from_slice(&id.to_le_bytes());

        // col 1: name (VARCHAR, string_t, 16 bytes)
        let off = i * 16;
        let dst = &mut chunk.data[1].raw_data_mut()[off..off + 16];
        write_varchar_inline_string_t(dst, name);

        // col 2: gpa (FLOAT, 4 bytes)
        let off = i * 4;
        chunk.data[2].raw_data_mut()[off..off + 4].copy_from_slice(&gpa.to_le_bytes());

        // col 3: age (INTEGER, 4 bytes)
        let off = i * 4;
        chunk.data[3].raw_data_mut()[off..off + 4].copy_from_slice(&age.to_le_bytes());

        // col 4: is_active (BOOLEAN, 1 byte)
        chunk.data[4].raw_data_mut()[i] = if is_active { 1 } else { 0 };

        // col 5: enrollment_day (DATE, 4 bytes — i32 days since 1970-01-01)
        let off = i * 4;
        chunk.data[5].raw_data_mut()[off..off + 4].copy_from_slice(&enrollment_day.to_le_bytes());
    }

    chunk.set_cardinality(ROW_COUNT);

    // 插入数据（显式事务）
    conn.begin_transaction().expect("begin_transaction 失败");
    conn.insert("students", &mut chunk).expect("插入数据失败");
    conn.commit().expect("commit 失败");
    println!("插入 {} 条记录成功", ROW_COUNT);

    // Checkpoint 到磁盘
    engine.checkpoint().expect("checkpoint 失败");
    println!("Checkpoint 完成\n");

    let file_size = std::fs::metadata(DB_PATH).map(|m| m.len()).unwrap_or(0);
    println!("数据库文件: {} ({} 字节)\n", DB_PATH, file_size);

    // 用 duckdb.exe 验证
    println!("--- DuckDB 验证 ---");
    verify_with_duckdb();
}

/// 生成 100 条学生数据。
///
/// 返回: (id, name, age, gpa, is_active, enrollment_day)
fn generate_students(n: usize) -> Vec<(i32, &'static str, i32, f32, bool, i32)> {
    let names: [&'static str; 10] = [
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack",
    ];
    let mut students = Vec::with_capacity(n);
    for i in 0..n {
        let id = (i + 1) as i32;
        let name = names[i % names.len()];
        let age = 18 + (i % 10) as i32;
        let gpa = 2.0 + (i as f32 % 20.0) * 0.1;
        let is_active = i % 3 != 0;
        let enrollment_day: i32 = 10957 + (i as i32) * 30;
        students.push((id, name, age, gpa, is_active, enrollment_day));
    }
    students
}

fn write_varchar_inline_string_t(dst: &mut [u8], s: &str) {
    assert_eq!(dst.len(), 16, "string_t buffer must be 16 bytes");
    let bytes = s.as_bytes();
    assert!(
        bytes.len() <= 12,
        "inline varchar encoding only supports <= 12 bytes, got {}",
        bytes.len()
    );
    let len_u32 = bytes.len() as u32;
    dst[..4].copy_from_slice(&len_u32.to_le_bytes());
    dst[4..4 + bytes.len()].copy_from_slice(bytes);
    dst[4 + bytes.len()..].fill(0);
}

fn verify_with_duckdb() {
    run_duckdb_query(
        "SELECT id, concat(name, '') AS name, gpa, age, is_active, enrollment_day FROM students ORDER BY id LIMIT 10;",
    );
    run_duckdb_query(
        "SELECT COUNT(*) AS total, AVG(age) AS avg_age, AVG(gpa) AS avg_gpa, \
         SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_count FROM students;",
    );
    run_duckdb_query(
        "SELECT id, concat(name, '') AS name, enrollment_day FROM students \
         WHERE enrollment_day BETWEEN (DATE '1970-01-01' + 10957) AND (DATE '1970-01-01' + 11500) \
         ORDER BY id LIMIT 5;",
    );
}

fn run_duckdb_query(sql: &str) {
    println!("\nSQL: {}", sql);
    let output = Command::new(DUCKDB_EXE)
        .arg(DB_PATH)
        .arg("-c")
        .arg(sql)
        .output()
        .expect("无法执行 DuckDB");

    if output.status.success() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    } else {
        eprintln!("错误: {}", String::from_utf8_lossy(&output.stderr));
    }
}
