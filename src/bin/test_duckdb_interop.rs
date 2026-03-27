//! DuckDB 与 Rust 互操作性测试
//!
//! 测试目标：
//!   1. DuckDB CLI 创建学生表 -> Rust 读取验证
//!   2. Rust 创建学生表 -> DuckDB CLI 读取验证
//!
//! 运行：
//!   cargo run --bin test_duckdb_interop

use std::path::Path;
use std::process::Command;

use duckdb_rs::common::types::{DataChunk, LogicalType};
use duckdb_rs::db::DB;

// ─── 配置 ─────────────────────────────────────────────────────────────────────

const DUCKDB_EXE: &str = "/Users/liang/Documents/code/duckdb/cmake-build-debug/duckdb";
const TEST_DB_RUST: &str = "student_rust.db";
const TEST_DB_DUCKDB: &str = "student_duckdb.db";

// ─── 主函数 ───────────────────────────────────────────────────────────────────

fn main() {
    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║        DuckDB ↔ Rust 互操作性测试                    ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();

    // ═══════════════════════════════════════════════════════════════════════════
    // 测试 1: DuckDB 创建学生表，Rust 读取
    // ═══════════════════════════════════════════════════════════════════════════
    println!("┌─ 测试 1: DuckDB 创建 -> Rust 读取 ────────────────────┐");
    println!("│");

    // 删除旧文件
    if Path::new(TEST_DB_DUCKDB).exists() {
        std::fs::remove_file(TEST_DB_DUCKDB).expect("删除旧文件失败");
    }

    // 用 DuckDB 创建学生表
    let create_sql = r#"
SET force_compression='uncompressed';
CREATE TABLE students (
    id INTEGER PRIMARY KEY,
    age INTEGER,
    score DOUBLE,
    class_id BIGINT
);

INSERT INTO students VALUES
    (1, 20, 85.5, 1001),
    (2, 21, 92.0, 1001),
    (3, 19, 78.5, 1002),
    (4, 22, 95.0, 1002),
    (5, 20, 88.0, 1003);
"#;

    let output = Command::new(DUCKDB_EXE)
        .arg(TEST_DB_DUCKDB)
        .arg("-c")
        .arg(create_sql)
        .output()
        .expect("无法执行 DuckDB");

    if output.status.success() {
        println!("│  ✓ DuckDB 创建学生表成功");
    } else {
        println!("│  ✗ DuckDB 创建表失败: {}", String::from_utf8_lossy(&output.stderr));
    }

    // 检查文件
    if Path::new(TEST_DB_DUCKDB).exists() {
        let size = std::fs::metadata(TEST_DB_DUCKDB).map(|m| m.len()).unwrap_or(0);
        println!("│  ✓ 数据库文件已创建: {} 字节", size);

        // 尝试用 Rust 读取
        println!("│  尝试用 Rust 读取...");
        match test_rust_read_duckdb_file() {
            Ok(tables) => {
                println!("│  ✓ Rust 成功读取，发现表: {:?}", tables);
            }
            Err(e) => {
                println!("│  ✗ Rust 读取失败: {}", e);
            }
        }
    } else {
        println!("│  ✗ 数据库文件未创建");
    }

    println!("└────────────────────────────────────────────────────────┘");

    // ═══════════════════════════════════════════════════════════════════════════
    // 测试 2: Rust 创建学生表，DuckDB 读取
    // ═══════════════════════════════════════════════════════════════════════════
    println!("\n┌─ 测试 2: Rust 创建 -> DuckDB 读取 ────────────────────┐");
    println!("│");

    // 删除旧文件
    if Path::new(TEST_DB_RUST).exists() {
        std::fs::remove_file(TEST_DB_RUST).expect("删除旧文件失败");
    }

    // 用 Rust 创建学生表
    match test_rust_create_student_table() {
        Ok(()) => {
            println!("│  ✓ Rust 创建学生表成功");

            // 检查文件
            if Path::new(TEST_DB_RUST).exists() {
                let size = std::fs::metadata(TEST_DB_RUST).map(|m| m.len()).unwrap_or(0);
                println!("│  ✓ 数据库文件已创建: {} 字节", size);

                // 用 DuckDB 读取
                println!("│  尝试用 DuckDB 读取...");
                let output = Command::new(DUCKDB_EXE)
                    .arg(TEST_DB_RUST)
                    .arg("-c")
                    .arg("SELECT * FROM students ORDER BY id desc;")
                    .output()
                    .expect("无法执行 DuckDB");

                if output.status.success() {
                    println!("│  ✓ DuckDB 成功读取:");
                    for line in String::from_utf8_lossy(&output.stdout).lines().take(10) {
                        println!("│    {}", line);
                    }
                } else {
                    println!("│  ✗ DuckDB 读取失败: {}", String::from_utf8_lossy(&output.stderr));
                }
            } else {
                println!("│  ✗ 数据库文件未创建");
            }
        }
        Err(e) => {
            println!("│  ✗ Rust 创建失败: {}", e);
        }
    }

    println!("└────────────────────────────────────────────────────────┘");

    // ═══════════════════════════════════════════════════════════════════════════
    // 总结
    // ═══════════════════════════════════════════════════════════════════════════
    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║                    测试完成                          ║");
    println!("╠══════════════════════════════════════════════════════╣");
    println!("║  文件位置:                                           ║");
    println!("║    DuckDB 创建: {}  ║", TEST_DB_DUCKDB);
    println!("║    Rust 创建:   {}    ║", TEST_DB_RUST);
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
}

// ─── Rust 读取 DuckDB 文件 ─────────────────────────────────────────────────────

fn test_rust_read_duckdb_file() -> Result<Vec<String>, String> {
    let db = std::panic::catch_unwind(|| DB::open(TEST_DB_DUCKDB))
        .map_err(|_| "打开 DuckDB 文件时发生 panic".to_string())?
        .map_err(|e| format!("无法打开数据库: {:?}", e))?;

    // 获取表列表
    let tables = db.tables();

    // 尝试读取数据
    if tables.contains(&"students".to_string()) {
        match db.scan_chunks("students", None) {
            Ok(chunks) => {
                let total_rows: usize = chunks.iter().map(|c| c.size()).sum();
                println!("│    读取到 {} 行数据", total_rows);
                let rows = decode_student_rows(&chunks)?;
                let expected = expected_students();
                if rows != expected {
                    return Err(format!("读取结果不匹配: actual={rows:?}, expected={expected:?}"));
                }
                println!("│    ✓ 数据校验通过");

                // 打印前几行数据
                for (i, chunk) in chunks.iter().take(3).enumerate() {
                    println!("│    Chunk {}: {} 行", i, chunk.size());
                }
            }
            Err(e) => {
                println!("│    读取数据失败: {:?}", e);
            }
        }
    }

    Ok(tables)
}

// ─── Rust 创建学生表 ───────────────────────────────────────────────────────────

fn test_rust_create_student_table() -> Result<(), String> {
    // 创建内存数据库
    let mut db = DB::open(TEST_DB_RUST)
        .map_err(|e| format!("无法创建数据库: {:?}", e))?;

    // 创建学生表
    db.create_table(
        "main",
        "students",
        vec![
            ("id".to_string(), LogicalType::integer()),
            ("age".to_string(), LogicalType::integer()),
            ("score".to_string(), LogicalType::double()),
            ("class_id".to_string(), LogicalType::bigint()),
        ],
    );

    // 准备学生数据
    let students = expected_students();

    // 创建 DataChunk
    let mut chunk = DataChunk::new();
    chunk.initialize(
        &[
            LogicalType::integer(),
            LogicalType::integer(),
            LogicalType::double(),
            LogicalType::bigint(),
        ],
        students.len(),
    );

    // 填充数据
    for (i, &(id, age, score, class_id)) in students.iter().enumerate() {
        // id (INTEGER)
        let id_offset = i * 4;
        chunk.data[0].raw_data_mut()[id_offset..id_offset + 4].copy_from_slice(&id.to_le_bytes());

        // age (INTEGER)
        let age_offset = i * 4;
        chunk.data[1].raw_data_mut()[age_offset..age_offset + 4].copy_from_slice(&age.to_le_bytes());

        // score (DOUBLE)
        let score_offset = i * 8;
        chunk.data[2].raw_data_mut()[score_offset..score_offset + 8].copy_from_slice(&score.to_le_bytes());

        // class_id (BIGINT)
        let class_offset = i * 8;
        chunk.data[3].raw_data_mut()[class_offset..class_offset + 8]
            .copy_from_slice(&class_id.to_le_bytes());
    }
    chunk.set_cardinality(students.len());

    // 插入数据
    db.insert_chunk("students", &mut chunk)
        .map_err(|e| format!("插入数据失败: {:?}", e))?;

    // 验证插入
    let results = db.scan_chunks("students", None)
        .map_err(|e| format!("查询失败: {:?}", e))?;
    let total_rows: usize = results.iter().map(|c| c.size()).sum();
    println!("│  已插入 {} 行数据", total_rows);
    let rows = decode_student_rows(&results)?;
    if rows != students {
        return Err(format!("checkpoint 前查询结果不匹配: actual={rows:?}, expected={students:?}"));
    }

    // 执行 checkpoint 将数据写入磁盘
    println!("│  执行 checkpoint...");
    db.checkpoint()
        .map_err(|e| format!("Checkpoint 失败: {:?}", e))?;
    println!("│  ✓ Checkpoint 完成");

    Ok(())
}

fn expected_students() -> Vec<(i32, i32, f64, i64)> {
    vec![
        (1, 20, 85.5, 1001),
        (2, 21, 92.0, 1001),
        (3, 19, 78.5, 1002),
        (4, 22, 95.0, 1002),
        (5, 20, 88.0, 1003),
    ]
}

fn decode_student_rows(chunks: &[DataChunk]) -> Result<Vec<(i32, i32, f64, i64)>, String> {
    let mut rows = Vec::new();
    for chunk in chunks {
        for row_idx in 0..chunk.size() {
            let id = read_i32(chunk, 0, row_idx)?;
            let age = read_i32(chunk, 1, row_idx)?;
            let score = read_f64(chunk, 2, row_idx)?;
            let class_id = read_i64(chunk, 3, row_idx)?;
            rows.push((id, age, score, class_id));
        }
    }
    Ok(rows)
}

fn read_i32(chunk: &DataChunk, col: usize, row: usize) -> Result<i32, String> {
    let raw = chunk.data[col].raw_data();
    let offset = row * 4;
    let bytes = raw.get(offset..offset + 4).ok_or_else(|| format!("列 {col} 第 {row} 行超界"))?;
    Ok(i32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_i64(chunk: &DataChunk, col: usize, row: usize) -> Result<i64, String> {
    let raw = chunk.data[col].raw_data();
    let offset = row * 8;
    let bytes = raw.get(offset..offset + 8).ok_or_else(|| format!("列 {col} 第 {row} 行超界"))?;
    Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_f64(chunk: &DataChunk, col: usize, row: usize) -> Result<f64, String> {
    let raw = chunk.data[col].raw_data();
    let offset = row * 8;
    let bytes = raw.get(offset..offset + 8).ok_or_else(|| format!("列 {col} 第 {row} 行超界"))?;
    Ok(f64::from_le_bytes(bytes.try_into().unwrap()))
}
