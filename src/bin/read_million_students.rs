//! 读取已有数据库文件的测试用例
//!
//! 读取 million_students_txn.db 中的 students 表数据（通过 DuckEngine）

use duckdb_rs::common::types::DataChunk;
use duckdb_rs::db::DuckEngine;

/// 数据库文件路径
const DB_PATH: &str = "million_students_txn.db";

/// 从 DataChunk 的指定列读取 i32 值列表
fn read_i32_column(chunk: &DataChunk, col: usize) -> Vec<i32> {
    let raw = chunk.data[col].raw_data();
    raw.chunks_exact(4)
        .take(chunk.size())
        .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
        .collect()
}

fn main() {
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         读取 million_students.db 测试用例 (DuckEngine)       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    if !std::path::Path::new(DB_PATH).exists() {
        println!("错误: 数据库文件不存在: {}", DB_PATH);
        println!("请先运行 million_student_txn 来创建数据库:");
        println!("  cargo run --bin million_student_txn --release");
        return;
    }

    // ─── 步骤 1：打开数据库 ────────────────────────────────────────────────────
    println!("步骤 1：打开数据库");
    println!("  数据库路径: {}", DB_PATH);
    let engine = DuckEngine::open(DB_PATH).expect("打开数据库失败");
    let conn = engine.connect();
    println!("  ✓ 数据库已打开");
    println!();

    // ─── 步骤 2：查看表列表 ────────────────────────────────────────────────────
    println!("步骤 2：查看表列表");
    println!("  表列表: {:?}", engine.tables());
    println!();

    // ─── 步骤 3：扫描 students 表（全列）─────────────────────────────────────
    println!("步骤 3：扫描 students 表（全列）");
    let txn = conn.begin_transaction().expect("begin_transaction 失败");
    let chunks = conn.scan(&txn, "students", None).expect("scan 失败");
    conn.commit(txn).expect("commit 失败");

    let total_rows: usize = chunks.iter().map(|c| c.size()).sum();
    println!("  ✓ 读取完成");
    println!("    Chunk 数量: {}", chunks.len());
    println!("    总行数: {}", total_rows);
    println!();

    // ─── 步骤 4：验证数据正确性 ───────────────────────────────────────────────
    println!("步骤 4：验证数据正确性");

    let mut all_ids: Vec<i32> = Vec::new();
    for chunk in &chunks {
        all_ids.extend(read_i32_column(chunk, 0));
    }
    all_ids.sort_unstable();

    println!("  id 列最小值: {}", all_ids.first().unwrap_or(&0));
    println!("  id 列最大值: {}", all_ids.last().unwrap_or(&0));
    println!("  id 列数量: {}", all_ids.len());

    let expected_max = *all_ids.last().unwrap_or(&0);
    assert_eq!(*all_ids.first().unwrap(), 1, "最小 id 应该是 1");
    assert_eq!(
        *all_ids.last().unwrap(),
        expected_max,
        "最大 id 应与总行数一致"
    );
    assert_eq!(all_ids.len(), total_rows, "id 数量应与总行数一致");
    println!("  ✓ id 列验证通过");

    let mut all_ages: Vec<i32> = Vec::new();
    for chunk in &chunks {
        all_ages.extend(read_i32_column(chunk, 1));
    }
    println!(
        "  age 列范围: {} - {}",
        all_ages.iter().min().unwrap_or(&0),
        all_ages.iter().max().unwrap_or(&0)
    );
    assert!(*all_ages.iter().min().unwrap() >= 18, "age 最小值应该 >= 18");
    assert!(*all_ages.iter().max().unwrap() <= 27, "age 最大值应该 <= 27");
    println!("  ✓ age 列验证通过");

    let mut all_scores: Vec<i32> = Vec::new();
    for chunk in &chunks {
        all_scores.extend(read_i32_column(chunk, 2));
    }
    println!(
        "  score 列范围: {} - {}",
        all_scores.iter().min().unwrap_or(&0),
        all_scores.iter().max().unwrap_or(&0)
    );
    assert!(
        *all_scores.iter().min().unwrap() >= 50,
        "score 最小值应该 >= 50"
    );
    assert!(
        *all_scores.iter().max().unwrap() <= 99,
        "score 最大值应该 <= 99"
    );
    println!("  ✓ score 列验证通过");
    println!();

    // ─── 步骤 5：只读取指定列 ─────────────────────────────────────────────────
    println!("步骤 5：只读取 id 和 score 列");
    let txn2 = conn.begin_transaction().expect("begin_transaction 失败");
    let chunks_partial = conn
        .scan(&txn2, "students", Some(vec![0, 2]))
        .expect("scan 失败");
    conn.commit(txn2).expect("commit 失败");

    let partial_rows: usize = chunks_partial.iter().map(|c| c.size()).sum();
    println!("  读取到 {} 行 (只包含 id 和 score)", partial_rows);

    let mut partial_ids: Vec<i32> = Vec::new();
    let mut partial_scores: Vec<i32> = Vec::new();
    for chunk in &chunks_partial {
        partial_ids.extend(read_i32_column(chunk, 0));
        partial_scores.extend(read_i32_column(chunk, 1));
    }
    assert_eq!(partial_ids.len(), total_rows, "部分列读取行数应与总行数一致");
    assert_eq!(partial_scores.len(), total_rows, "score 列数量应该匹配");
    println!("  ✓ 部分列读取验证通过");
    println!();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    测试全部通过!                            ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  数据库文件:   {:<42} ║", DB_PATH);
    println!("║  表名:         students                                  ║");
    println!("║  总行数:       {:<42} ║", format!("{}", total_rows));
    println!("║  Chunk 数量:   {:<42} ║", format!("{}", chunks.len()));
    println!("╚══════════════════════════════════════════════════════════════╝");
}
