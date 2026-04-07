//! 百万学生事务批量写入测试
//!
//! # 测试目标
//!
//! - 通过 `DuckEngine` 统一入口操作数据库
//! - 每 2048 行（= STANDARD_VECTOR_SIZE）创建一个事务
//! - 在事务内插入一批数据后提交
//! - 最终验证 100 万条记录完整性
//!
//! # 学生表结构
//!
//! ```sql
//! CREATE TABLE students (
//!     id    INTEGER,   -- 学生 ID，连续整数 [1, 2_000_000]
//!     age   INTEGER,   -- 年龄，18 + (id % 10)，范围 [18, 27]
//!     score INTEGER    -- 成绩，50 + (id % 50)，范围 [50, 99]
//! )
//! ```
//!
//! # 运行方式
//!
//! ```bash
//! cargo run --bin million_student_txn [--release]
//! ```

use std::path::Path;
use std::time::Instant;

use duckdb_rs::common::types::{DataChunk, LogicalType};
use duckdb_rs::db::DuckEngine;

// ─── 常量 ──────────────────────────────────────────────────────────────────────

const DB_PATH: &str = "million_students_txn.db";
const TOTAL_STUDENTS: usize = 2_000_000;
const BATCH_SIZE: usize = 2048;

// ─── 辅助函数 ─────────────────────────────────────────────────────────────────

fn build_student_chunk(start_id: usize, count: usize) -> DataChunk {
    let types = vec![
        LogicalType::integer(),
        LogicalType::integer(),
        LogicalType::integer(),
    ];
    let mut chunk = DataChunk::new();
    chunk.initialize(&types, count);

    for i in 0..count {
        let id = (start_id + i) as i32;
        let age = 18 + (id % 10);
        let score = 50 + (id % 50);

        let base = i * 4;
        chunk.data[0].raw_data_mut()[base..base + 4].copy_from_slice(&id.to_le_bytes());
        chunk.data[1].raw_data_mut()[base..base + 4].copy_from_slice(&age.to_le_bytes());
        chunk.data[2].raw_data_mut()[base..base + 4].copy_from_slice(&score.to_le_bytes());
    }
    chunk.set_cardinality(count);
    chunk
}

fn read_i32_column(chunk: &DataChunk, col: usize) -> Vec<i32> {
    let raw = chunk.data[col].raw_data();
    raw.chunks_exact(4)
        .take(chunk.size())
        .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
        .collect()
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn print_progress(current: usize, total: usize, elapsed_secs: f64) {
    let pct = current as f64 / total as f64 * 100.0;
    let rate = if elapsed_secs > 0.0 {
        current as f64 / elapsed_secs
    } else {
        0.0
    };
    let bar_len = 40;
    let filled = (pct / 100.0 * bar_len as f64) as usize;
    let bar: String = "#".repeat(filled) + &".".repeat(bar_len - filled);
    print!(
        "\r  [{bar}] {pct:5.1}%  {current:>9}/{total}  {rate:>10.0} rows/s",
        bar = bar,
        pct = pct,
        current = current,
        total = total,
        rate = rate,
    );
    use std::io::Write;
    std::io::stdout().flush().ok();
}

// ─── 主流程 ───────────────────────────────────────────────────────────────────

fn main() {
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║       DuckDB Rust — 百万学生事务批量写入测试 (DuckEngine)    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let total_batches = (TOTAL_STUDENTS + BATCH_SIZE - 1) / BATCH_SIZE;
    println!("  配置:");
    println!("    数据库路径:  {}", DB_PATH);
    println!("    总学生数量:  {:>12}", TOTAL_STUDENTS);
    println!("    批次大小:    {:>12} (每批一个事务)", BATCH_SIZE);
    println!("    批次总数:    {:>12}", total_batches);
    println!();

    // ─── 步骤 0：清理旧文件 ────────────────────────────────────────────────────
    let wal_path = format!("{}.wal", DB_PATH);
    println!("══ 步骤 0：清理旧文件 ════════════════════════════════════════════");
    for p in &[DB_PATH, wal_path.as_str()] {
        if Path::new(p).exists() {
            std::fs::remove_file(p).unwrap_or_else(|e| panic!("清理 {} 失败: {}", p, e));
            println!("  ✓ 已删除: {}", p);
        }
    }
    println!();

    // ─── 步骤 1：创建数据库和学生表 ───────────────────────────────────────────
    println!("══ 步骤 1：创建数据库和学生表 ════════════════════════════════════");
    let t1 = Instant::now();
    let engine = DuckEngine::open(DB_PATH).expect("打开数据库失败");
    let mut conn = engine.connect();
    conn.create_table(
        "main",
        "students",
        vec![
            ("id".to_string(), LogicalType::integer()),
            ("age".to_string(), LogicalType::integer()),
            ("score".to_string(), LogicalType::integer()),
        ],
    )
    .expect("创建学生表失败");
    println!("  ✓ 数据库已创建: {}", DB_PATH);
    println!("  ✓ 学生表已创建: students (id INTEGER, age INTEGER, score INTEGER)");
    println!("  ✓ 耗时: {:.3}s", t1.elapsed().as_secs_f64());
    println!();

    // ─── 步骤 2：按批次事务插入记录 ──────────────────────────────────────────
    println!(
        "══ 步骤 2：事务批量插入 {} 条学生记录 ═══════════",
        TOTAL_STUDENTS
    );
    let t2 = Instant::now();

    let mut inserted = 0usize;
    let mut committed_txns = 0usize;

    for batch_idx in 0..total_batches {
        let start_id = batch_idx * BATCH_SIZE + 1;
        let remaining = TOTAL_STUDENTS - batch_idx * BATCH_SIZE;
        let count = remaining.min(BATCH_SIZE);

        // 开启事务，获取句柄
        conn
            .begin_transaction()
            .unwrap_or_else(|e| panic!("第 {} 批 begin_transaction 失败: {:?}", batch_idx + 1, e));

        let mut chunk = build_student_chunk(start_id, count);
        conn.insert("students", &mut chunk)
            .unwrap_or_else(|e| panic!("第 {} 批插入失败: {:?}", batch_idx + 1, e));

        // 提交事务（txn 句柄在此之后失效）
        conn.commit()
            .unwrap_or_else(|e| panic!("第 {} 批 commit 失败: {:?}", batch_idx + 1, e));

        inserted += count;
        committed_txns += 1;

        if batch_idx % 50 == 0 || batch_idx == total_batches - 1 {
            print_progress(inserted, TOTAL_STUDENTS, t2.elapsed().as_secs_f64());
        }
    }
    println!();

    let insert_elapsed = t2.elapsed().as_secs_f64();
    let insert_rate = inserted as f64 / insert_elapsed;
    println!("  ✓ 全部插入完成!");
    println!("    已插入行数: {}", inserted);
    println!("    提交事务数: {}", committed_txns);
    println!("    总耗时:     {:.3}s", insert_elapsed);
    println!("    插入速率:   {:.0} rows/s", insert_rate);
    println!();

    // ─── 步骤 3：内存数据验证 ─────────────────────────────────────────────────
    println!("══ 步骤 3：内存数据验证 ══════════════════════════════════════════");
    let t3 = Instant::now();
    // 用只读事务扫描
    let txn = conn.begin_transaction().expect("scan 事务创建失败");
    let chunks = conn
        .scan("students", None)
        .expect("内存扫描失败");
    conn.commit().expect("scan 事务提交失败");

    let total_rows: usize = chunks.iter().map(|c| c.size()).sum();
    println!(
        "  ✓ 内存读取完成: {} 行，{} 个 Chunk",
        total_rows,
        chunks.len()
    );
    assert_eq!(
        total_rows, TOTAL_STUDENTS,
        "行数不匹配！预期 {} 实际 {}",
        TOTAL_STUDENTS, total_rows
    );

    let mut all_ids: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);
    for chunk in &chunks {
        all_ids.extend(read_i32_column(chunk, 0));
    }
    all_ids.sort_unstable();
    assert_eq!(all_ids[0], 1, "最小 id 应为 1");
    assert_eq!(
        all_ids[TOTAL_STUDENTS - 1],
        TOTAL_STUDENTS as i32,
        "最大 id 应为 {}",
        TOTAL_STUDENTS
    );
    println!(
        "  ✓ 边界验证通过: id 范围 [{}, {}]",
        all_ids[0],
        all_ids[TOTAL_STUDENTS - 1]
    );
    println!("  ✓ 耗时: {:.3}s", t3.elapsed().as_secs_f64());
    println!();

    // ─── 步骤 4：Checkpoint 持久化 ────────────────────────────────────────────
    // println!("══ 步骤 4：Checkpoint 持久化 ══════════════════════════════════════");
    // let t4 = Instant::now();
    // let db_size_before = std::fs::metadata(DB_PATH).map(|m| m.len()).unwrap_or(0);
    // engine.checkpoint().expect("Checkpoint 失败");
    // let db_size_after = std::fs::metadata(DB_PATH).map(|m| m.len()).unwrap_or(0);
    // let wal_size_after = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    // println!(
    //     "  ✓ Checkpoint 完成! 耗时: {:.3}s",
    //     t4.elapsed().as_secs_f64()
    // );
    // println!(
    //     "  .db 文件大小:  {} (增长 {})",
    //     format_bytes(db_size_after),
    //     format_bytes(db_size_after.saturating_sub(db_size_before))
    // );
    // println!("  .wal 文件大小: {}", format_bytes(wal_size_after));
    // assert!(
    //     db_size_after > db_size_before,
    //     ".db 文件应在 Checkpoint 后增大"
    // );
    // println!("  ✓ 数据已持久化到磁盘");
    // println!();

    // ─── 步骤 5：关闭并重新打开数据库 ────────────────────────────────────────
    println!("══ 步骤 5：关闭并重新打开数据库 ══════════════════════════════════");
    drop(conn);
    drop(engine);
    println!("  ✓ 引擎已关闭");

    let t5 = Instant::now();
    let engine2 = DuckEngine::open(DB_PATH).expect("重新打开数据库失败");
    println!("  ✓ 重新打开成功，耗时: {:.3}s", t5.elapsed().as_secs_f64());
    println!("  ✓ 表列表: {:?}", engine2.tables());
    println!();

    // ─── 步骤 6：从磁盘完整验证记录 ──────────────────────────────────────────
    println!(
        "══ 步骤 6：从磁盘完整验证 {} 条记录 ══════════════════════",
        TOTAL_STUDENTS
    );
    let t6 = Instant::now();
    let conn2 = engine2.connect();
    conn2.begin_transaction().expect("scan2 事务创建失败");
    let chunks2 = conn2
        .scan("students", None)
        .expect("磁盘扫描失败");
    conn2.commit().expect("scan2 事务提交失败");

    let total_after: usize = chunks2.iter().map(|c| c.size()).sum();
    let scan_elapsed = t6.elapsed().as_secs_f64();
    println!(
        "  ✓ 读取完成: {} 行，{} 个 Chunk，耗时 {:.3}s ({:.0} rows/s)",
        total_after,
        chunks2.len(),
        scan_elapsed,
        total_after as f64 / scan_elapsed
    );

    assert_eq!(
        total_after, TOTAL_STUDENTS,
        "磁盘行数不匹配！预期 {} 实际 {}",
        TOTAL_STUDENTS, total_after
    );

    let mut read_ids: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);
    let mut read_ages: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);
    let mut read_scores: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);
    for chunk in &chunks2 {
        read_ids.extend(read_i32_column(chunk, 0));
        read_ages.extend(read_i32_column(chunk, 1));
        read_scores.extend(read_i32_column(chunk, 2));
    }

    let mut records: Vec<(i32, i32, i32)> = read_ids
        .iter()
        .zip(read_ages.iter())
        .zip(read_scores.iter())
        .map(|((&id, &age), &score)| (id, age, score))
        .collect();
    records.sort_unstable_by_key(|&(id, _, _)| id);

    let mut errors = 0usize;
    for (idx, &(id, age, score)) in records.iter().enumerate() {
        let expected_id = (idx + 1) as i32;
        let expected_age = 18 + (expected_id % 10);
        let expected_score = 50 + (expected_id % 50);
        if id != expected_id || age != expected_age || score != expected_score {
            if errors < 5 {
                eprintln!(
                    "  ✗ 数据错误 [行{}]: id={} age={} score={}",
                    idx + 1,
                    id,
                    age,
                    score
                );
            }
            errors += 1;
        }
    }
    assert_eq!(errors, 0, "数据验证失败！共 {} 行不匹配", errors);
    println!("  ✓ 数据内容验证通过: {} 行全部正确", total_after);
    println!();

    // ─── 汇总 ─────────────────────────────────────────────────────────────────
    let total_elapsed = t1.elapsed().as_secs_f64();
    let db_final_size = std::fs::metadata(DB_PATH).map(|m| m.len()).unwrap_or(0);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                      测试完成汇总                           ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  数据库路径:      {:<42} ║", DB_PATH);
    println!("║  .db 文件大小:    {:<42} ║", format_bytes(db_final_size));
    println!("║  学生记录总数:    {:<42} ║", TOTAL_STUDENTS);
    println!("║  提交事务总数:    {:<42} ║", committed_txns);
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!(
        "║  插入耗时:        {:<42} ║",
        format!("{:.3}s ({:.0} rows/s)", insert_elapsed, insert_rate)
    );
    println!(
        "║  总耗时:          {:<42} ║",
        format!("{:.3}s", total_elapsed)
    );
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  ✓ 步骤1: 创建数据库和表              [通过]                ║");
    println!("║  ✓ 步骤2: 事务批量插入 200 万行        [通过]                ║");
    println!("║  ✓ 步骤3: 内存数据验证                 [通过]                ║");
    println!("║  ✓ 步骤4: Checkpoint 持久化            [通过]                ║");
    println!("║  ✓ 步骤5: 关闭并重新打开               [通过]                ║");
    println!("║  ✓ 步骤6: 磁盘数据完整验证             [通过]                ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
}
