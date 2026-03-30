//! 百万学生数据持久化测试
//!
//! # 测试目标
//!
//! 1. 大数据量写入：插入 1,000,000 条学生记录（批次写入）
//! 2. Checkpoint 持久化验证：写入后执行 Checkpoint，确保数据写入 .db 文件
//! 3. WAL 机制验证：验证 WAL 文件在 Checkpoint 前后的状态变化
//! 4. 重新打开数据库：关闭后重新 DB::open()，从磁盘读取数据
//! 5. 数据一致性验证：验证读取的 100 万条记录与写入完全一致
//!
//! # 学生表结构
//!
//! ```sql
//! CREATE TABLE students (
//!     id    INTEGER,   -- 学生 ID，连续整数 [1, 1_000_000]
//!     age   INTEGER,   -- 年龄，18 + (id % 10)，范围 [18, 27]
//!     score INTEGER    -- 成绩，50 + (id % 50)，范围 [50, 99]
//! )
//! ```
//!
//! # 插入策略
//!
//! - 每批 BATCH_SIZE = 2048 行（= STANDARD_VECTOR_SIZE）
//! - 共 489 批（最后一批可能不足 2048 行）
//!
//! # 运行方式
//!
//! ```bash
//! cargo run --bin million_student_test [--release]
//! ```
//!
//! 建议使用 --release 模式，避免 debug 模式下太慢。

use std::path::Path;
use std::time::Instant;

use duckdb_rs::common::types::{DataChunk, LogicalType};
use duckdb_rs::db::DB;
use duckdb_rs::storage::storage_manager::StorageManager;

// ─── 常量 ──────────────────────────────────────────────────────────────────────

/// 数据库文件路径（临时路径，测试完成后可删除）
const DB_PATH: &str = "/tmp/million_students.db";

/// 总学生数量：100 万
const TOTAL_STUDENTS: usize = 1_000_000;

/// 每批插入行数（= STANDARD_VECTOR_SIZE，与 DuckDB 向量大小对齐）
const BATCH_SIZE: usize = 2048;

// ─── 辅助函数 ─────────────────────────────────────────────────────────────────

/// 构建学生数据块（id, age, score 均为 INTEGER）。
///
/// - id:    连续整数，从 `start_id` 开始
/// - age:   18 + (id % 10)，范围 [18, 27]
/// - score: 50 + (id % 50)，范围 [50, 99]
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

/// 从 DataChunk 的指定列读取 i32 值列表。
fn read_i32_column(chunk: &DataChunk, col: usize) -> Vec<i32> {
    let raw = chunk.data[col].raw_data();
    raw.chunks_exact(4)
        .take(chunk.size())
        .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
        .collect()
}

/// 格式化字节大小（B / KB / MB / GB）。
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

/// 打印进度条。
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
    println!("║          DuckDB Rust — 百万学生数据持久化验证测试            ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  配置:");
    println!("    数据库路径:  {}", DB_PATH);
    println!("    总学生数量:  {:>12}", TOTAL_STUDENTS);
    println!("    批次大小:    {:>12}", BATCH_SIZE);
    println!(
        "    批次总数:    {:>12}",
        (TOTAL_STUDENTS + BATCH_SIZE - 1) / BATCH_SIZE
    );
    println!();

    // ─── 步骤 0：清理旧文件 ────────────────────────────────────────────────────
    let wal_path = format!("{}.wal", DB_PATH);
    println!("══ 步骤 0：清理旧文件 ════════════════════════════════════════════");
    if Path::new(DB_PATH).exists() {
        std::fs::remove_file(DB_PATH).expect("清理旧 .db 文件失败");
        println!("  ✓ 已删除: {}", DB_PATH);
    }
    if Path::new(&wal_path).exists() {
        std::fs::remove_file(&wal_path).expect("清理旧 .wal 文件失败");
        println!("  ✓ 已删除: {}", wal_path);
    }
    println!();

    // ─── 步骤 1：创建数据库和学生表 ───────────────────────────────────────────
    println!("══ 步骤 1：创建数据库和学生表 ════════════════════════════════════");
    let t1 = Instant::now();
    let mut db = DB::open(DB_PATH).expect("打开数据库失败");
    println!("  ✓ 数据库已创建: {}", db.storage_manager().db_path());
    println!("  ✓ WAL 路径:     {}", db.storage_manager().wal_path());

    db.create_table(
        "main",
        "students",
        vec![
            ("id".to_string(), LogicalType::integer()),
            ("age".to_string(), LogicalType::integer()),
            ("score".to_string(), LogicalType::integer()),
        ],
    );
    println!("  ✓ 学生表已创建: students (id INTEGER, age INTEGER, score INTEGER)");
    println!("  ✓ 耗时: {:.3}s", t1.elapsed().as_secs_f64());
    println!();

    // ─── 步骤 2：批量插入 100 万条学生记录 ────────────────────────────────────
    println!(
        "══ 步骤 2：批量插入 {} 条学生记录 ════════════════════════",
        TOTAL_STUDENTS
    );
    let t2 = Instant::now();
    let total_batches = (TOTAL_STUDENTS + BATCH_SIZE - 1) / BATCH_SIZE;
    let mut inserted = 0usize;

    for batch_idx in 0..total_batches {
        let start_id = batch_idx * BATCH_SIZE + 1; // id 从 1 开始
        let remaining = TOTAL_STUDENTS - batch_idx * BATCH_SIZE;
        let count = remaining.min(BATCH_SIZE);

        let mut chunk = build_student_chunk(start_id, count);
        db.insert_chunk("students", &mut chunk)
            .unwrap_or_else(|e| panic!("第 {} 批插入失败: {:?}", batch_idx + 1, e));

        inserted += count;

        // 每 50 批或最后一批打印进度
        if batch_idx % 50 == 0 || batch_idx == total_batches - 1 {
            print_progress(inserted, TOTAL_STUDENTS, t2.elapsed().as_secs_f64());
        }
    }
    println!(); // 换行（进度条后）

    let insert_elapsed = t2.elapsed().as_secs_f64();
    let insert_rate = inserted as f64 / insert_elapsed;
    println!("  ✓ 全部插入完成!");
    println!("    已插入行数: {}", inserted);
    println!("    总耗时:     {:.3}s", insert_elapsed);
    println!("    插入速率:   {:.0} rows/s", insert_rate);
    println!();

    // ─── 步骤 3：插入后立即验证（内存数据）────────────────────────────────────
    println!("══ 步骤 3：插入后内存数据验证 ════════════════════════════════════");
    let t3 = Instant::now();
    println!("  正在扫描内存中的学生数据（静默模式，不打印行）...");
    let chunks_before = db
        .scan_chunks_silent("students", None)
        .expect("内存数据扫描失败");
    let total_before: usize = chunks_before.iter().map(|c| c.size()).sum();
    println!(
        "  ✓ 内存读取完成: {} 行，{} 个 Chunk",
        total_before,
        chunks_before.len()
    );
    assert_eq!(
        total_before, TOTAL_STUDENTS,
        "内存数据行数不匹配！预期 {} 行，实际 {} 行",
        TOTAL_STUDENTS, total_before
    );

    // 验证边界值
    let mut all_ids: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);
    for chunk in &chunks_before {
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

    // ─── 步骤 4：检查 WAL 状态（Checkpoint 前）───────────────────────────────
    println!("══ 步骤 4：WAL 状态检查（Checkpoint 前）══════════════════════════");
    let wal_size_before = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    let db_size_before = std::fs::metadata(DB_PATH).map(|m| m.len()).unwrap_or(0);
    println!("  .db 文件大小:  {}", format_bytes(db_size_before));
    println!(
        "  .wal 文件大小: {} ({})",
        format_bytes(wal_size_before),
        if wal_size_before > 0 {
            "存在"
        } else {
            "不存在/为空"
        }
    );
    println!(
        "  WAL 计数器:    {} (存储管理器跟踪)",
        db.storage_manager().wal_size()
    );

    // WAL 自动 Checkpoint 阈值检测
    let auto_ckpt = db.storage_manager().automatic_checkpoint(0);
    println!(
        "  自动 Checkpoint 阈值已达到: {}",
        if auto_ckpt {
            "是（将在下次提交时触发）"
        } else {
            "否（WAL < 16MB）"
        }
    );
    println!();

    // ─── 步骤 5：执行 Checkpoint（持久化到 .db）──────────────────────────────
    println!("══ 步骤 5：执行 Checkpoint（将数据持久化到 .db 文件）══════════════");
    let t5 = Instant::now();
    println!("  正在执行 Checkpoint...");
    db.checkpoint().expect("Checkpoint 失败");
    let checkpoint_elapsed = t5.elapsed().as_secs_f64();
    println!("  ✓ Checkpoint 完成! 耗时: {:.3}s", checkpoint_elapsed);

    // 检查 Checkpoint 后的文件状态
    let db_size_after = std::fs::metadata(DB_PATH).map(|m| m.len()).unwrap_or(0);
    let wal_size_after = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    println!(
        "  .db 文件大小:  {} (增长 {})",
        format_bytes(db_size_after),
        format_bytes(db_size_after.saturating_sub(db_size_before))
    );
    println!(
        "  .wal 文件大小: {} (Checkpoint 后)",
        format_bytes(wal_size_after)
    );

    // Checkpoint 后 .db 文件应该比之前大（包含了 100 万行数据）
    assert!(
        db_size_after > db_size_before,
        ".db 文件在 Checkpoint 后应该增大! before={} after={}",
        db_size_before,
        db_size_after
    );
    println!("  ✓ .db 文件已成功增长，数据已写入磁盘");
    println!();

    // ─── 步骤 6：关闭数据库（销毁内存中的 DB 对象）──────────────────────────
    println!("══ 步骤 6：关闭数据库（释放内存中的数据）══════════════════════════");
    drop(db); // 显式释放 DB，清除所有内存中的数据
    println!("  ✓ 数据库已关闭，内存数据已释放");
    println!();

    // ─── 步骤 7：重新打开数据库（从磁盘恢复）────────────────────────────────
    println!("══ 步骤 7：重新打开数据库（从 .db 文件恢复）══════════════════════");
    let t7 = Instant::now();
    let db2 = DB::open(DB_PATH).expect("重新打开数据库失败");
    println!("  ✓ 数据库重新打开成功");
    println!("  ✓ 表列表: {:?}", db2.tables());
    println!("  ✓ 耗时: {:.3}s", t7.elapsed().as_secs_f64());
    println!();

    // ─── 步骤 8：从磁盘读取并完整验证 100 万条记录 ───────────────────────────
    println!(
        "══ 步骤 8：从磁盘读取并完整验证 {} 条记录 ══════════════════",
        TOTAL_STUDENTS
    );
    let t8 = Instant::now();
    println!("  正在读取所有学生数据（静默模式，不打印行）...");
    let chunks_after = db2
        .scan_chunks_silent("students", None)
        .expect("磁盘数据扫描失败");
    let total_after: usize = chunks_after.iter().map(|c| c.size()).sum();
    let scan_elapsed = t8.elapsed().as_secs_f64();
    let scan_rate = total_after as f64 / scan_elapsed;

    println!("  ✓ 读取完成!");
    println!("    读取行数: {}", total_after);
    println!("    Chunk 数: {}", chunks_after.len());
    println!("    读取耗时: {:.3}s", scan_elapsed);
    println!("    读取速率: {:.0} rows/s", scan_rate);

    // 行数验证
    assert_eq!(
        total_after, TOTAL_STUDENTS,
        "从磁盘读取的行数不匹配！预期 {} 行，实际 {} 行",
        TOTAL_STUDENTS, total_after
    );
    println!("  ✓ 行数验证通过: {} 行", total_after);

    // 数据内容验证
    println!("  正在验证数据内容...");
    let t_verify = Instant::now();
    let mut read_ids: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);
    let mut read_ages: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);
    let mut read_scores: Vec<i32> = Vec::with_capacity(TOTAL_STUDENTS);

    for chunk in &chunks_after {
        read_ids.extend(read_i32_column(chunk, 0));
        read_ages.extend(read_i32_column(chunk, 1));
        read_scores.extend(read_i32_column(chunk, 2));
    }

    // 排序后验证（数据顺序可能因存储方式不同而改变）
    // 构建 (id, age, score) 三元组后排序
    let mut records: Vec<(i32, i32, i32)> = read_ids
        .iter()
        .zip(read_ages.iter())
        .zip(read_scores.iter())
        .map(|((&id, &age), &score)| (id, age, score))
        .collect();
    records.sort_unstable_by_key(|&(id, _, _)| id);

    // 逐记录验证
    let mut errors = 0usize;
    for (idx, &(id, age, score)) in records.iter().enumerate() {
        let expected_id = (idx + 1) as i32;
        let expected_age = 18 + (expected_id % 10);
        let expected_score = 50 + (expected_id % 50);

        if id != expected_id || age != expected_age || score != expected_score {
            if errors < 5 {
                eprintln!(
                    "  ✗ 数据错误 [行{}]: id={} (预期{}), age={} (预期{}), score={} (预期{})",
                    idx + 1,
                    id,
                    expected_id,
                    age,
                    expected_age,
                    score,
                    expected_score
                );
            }
            errors += 1;
        }
    }

    assert_eq!(errors, 0, "数据验证失败！共 {} 行数据不匹配", errors);

    let verify_elapsed = t_verify.elapsed().as_secs_f64();
    println!("  ✓ 数据内容验证通过! ({} 行全部正确)", total_after);
    println!("    验证耗时: {:.3}s", verify_elapsed);
    println!();

    // ─── 步骤 9：统计汇总 ─────────────────────────────────────────────────────
    let total_elapsed = t1.elapsed().as_secs_f64();
    let db_final_size = std::fs::metadata(DB_PATH).map(|m| m.len()).unwrap_or(0);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                      测试完成汇总                           ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  数据库路径:      {:<42} ║", DB_PATH);
    println!("║  .db 文件大小:    {:<42} ║", format_bytes(db_final_size));
    println!(
        "║  学生记录总数:    {:<42} ║",
        format!("{}", TOTAL_STUDENTS)
    );
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  [写入阶段]                                                  ║");
    println!(
        "║    插入耗时:      {:<42} ║",
        format!("{:.3}s", insert_elapsed)
    );
    println!(
        "║    插入速率:      {:<42} ║",
        format!("{:.0} rows/s", insert_rate)
    );
    println!(
        "║    Checkpoint:    {:<42} ║",
        format!("{:.3}s", checkpoint_elapsed)
    );
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  [读取阶段]                                                  ║");
    println!(
        "║    读取耗时:      {:<42} ║",
        format!("{:.3}s", scan_elapsed)
    );
    println!(
        "║    读取速率:      {:<42} ║",
        format!("{:.0} rows/s", scan_rate)
    );
    println!(
        "║    验证耗时:      {:<42} ║",
        format!("{:.3}s", verify_elapsed)
    );
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!(
        "║  总耗时:          {:<42} ║",
        format!("{:.3}s", total_elapsed)
    );
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  ✓ 步骤1: 创建数据库和表              [通过]                ║");
    println!("║  ✓ 步骤2: 批量插入 100 万行            [通过]                ║");
    println!("║  ✓ 步骤3: 内存数据验证                 [通过]                ║");
    println!("║  ✓ 步骤4: WAL 状态检查                 [通过]                ║");
    println!("║  ✓ 步骤5: Checkpoint 持久化            [通过]                ║");
    println!("║  ✓ 步骤6: 关闭数据库                   [通过]                ║");
    println!("║  ✓ 步骤7: 重新打开（磁盘恢复）         [通过]                ║");
    println!("║  ✓ 步骤8: 从磁盘读取并完整验证         [通过]                ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
}
