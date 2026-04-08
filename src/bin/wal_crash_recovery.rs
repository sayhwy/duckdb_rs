use std::fs;
use std::path::Path;

use duckdb_rs::common::errors::{Result, anyhow};
use duckdb_rs::common::types::{DataChunk, LogicalType, STANDARD_VECTOR_SIZE};
use duckdb_rs::db::DuckEngine;

const SMALL_DB_PATH: &str = "wal_recovery_students_10k.db";
const LARGE_DB_PATH: &str = "wal_recovery_students_2m.db";
const SMALL_ROWS: usize = 10_000;
const LARGE_ROWS: usize = 2_000_000;

#[derive(Debug, Clone, Copy)]
struct FileState {
    exists: bool,
    len: u64,
}

fn main() {
    println!("=== WAL 崩溃恢复验证 ===");
    println!("场景 1: 10000 行，关闭前不手动 checkpoint，重启后检查是否自动 checkpoint");
    println!("场景 2: 2000000 行，WAL 超过 16 MiB 触发自动 checkpoint，重启后检查恢复结果");
    println!();

    let mut failures = Vec::new();

    if let Err(err) = run_scenario(
        "场景1",
        SMALL_DB_PATH,
        SMALL_ROWS,
        ScenarioExpectation::RecoverWalOnRestart,
    ) {
        failures.push(err);
    }
    println!();
    if let Err(err) = run_scenario(
        "场景2",
        LARGE_DB_PATH,
        LARGE_ROWS,
        ScenarioExpectation::CheckpointBeforeClose,
    ) {
        failures.push(err);
    }

    println!();
    println!("=== 汇总 ===");
    if failures.is_empty() {
        println!("两个场景都满足当前测试预期。");
    } else {
        for failure in &failures {
            println!("- {}", failure);
        }
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, Copy)]
enum ScenarioExpectation {
    RecoverWalOnRestart,
    CheckpointBeforeClose,
}

fn run_scenario(
    name: &str,
    db_path: &str,
    total_rows: usize,
    expectation: ScenarioExpectation,
) -> Result<()> {
    cleanup(db_path);

    let wal_path = format!("{}.wal", db_path);
    let checkpoint_wal_path = format!("{}.wal.checkpoint", db_path);

    println!("=== {} ===", name);
    println!("数据库文件: {}", db_path);
    println!("目标行数: {}", total_rows);

    let before_reopen = {
        let engine =
            DuckEngine::open(db_path).map_err(|e| anyhow!("{name} 打开数据库失败: {e}"))?;
        let mut conn = engine.connect();
        create_students_table(&mut conn);

        // 先把表结构持久化，避免后续观测混入 CREATE TABLE 的 WAL。
        engine
            .checkpoint()
            .map_err(|e| anyhow!("{name} 创建表后的 checkpoint 失败: {e}"))?;

        conn.begin_transaction()
            .map_err(|e| anyhow!("{name} begin_transaction 失败: {e}"))?;
        insert_students(&conn, total_rows);
        conn.commit()
            .map_err(|e| anyhow!("{name} commit 失败: {e}"))?;

        let visible_rows = count_rows(&engine);
        if visible_rows != total_rows {
            return Err(anyhow!(
                "{} 关闭前可见行数不符合预期: expected {}, got {}",
                name, total_rows, visible_rows
            ));
        }

        let db_state = file_state(db_path);
        let wal_state = file_state(&wal_path);
        let checkpoint_wal_state = file_state(&checkpoint_wal_path);

        println!("关闭前文件状态:");
        print_file_state("db", db_state);
        print_file_state("wal", wal_state);
        print_file_state("checkpoint wal", checkpoint_wal_state);
        println!("关闭前读到 {} 行", visible_rows);

        drop(conn);
        drop(engine);

        Snapshot {
            db: db_state,
            wal: wal_state,
            checkpoint_wal: checkpoint_wal_state,
            rows: visible_rows,
        }
    };

    let after_reopen = {
        let engine =
            DuckEngine::open(db_path).map_err(|e| anyhow!("{name} 重启数据库失败: {e}"))?;
        let visible_rows = count_rows(&engine);

        let db_state = file_state(db_path);
        let wal_state = file_state(&wal_path);
        let checkpoint_wal_state = file_state(&checkpoint_wal_path);

        println!("重启后文件状态:");
        print_file_state("db", db_state);
        print_file_state("wal", wal_state);
        print_file_state("checkpoint wal", checkpoint_wal_state);
        println!("重启后读到 {} 行", visible_rows);

        drop(engine);

        Snapshot {
            db: db_state,
            wal: wal_state,
            checkpoint_wal: checkpoint_wal_state,
            rows: visible_rows,
        }
    };

    match expectation {
        ScenarioExpectation::RecoverWalOnRestart => {
            if after_reopen.rows != total_rows {
                return Err(anyhow!(
                    "{} 重启后行数异常: expected {}, got {}",
                    name, total_rows, after_reopen.rows
                ));
            }
            if !(before_reopen.wal.exists && before_reopen.wal.len > 0) {
                return Err(anyhow!("{} 关闭前应该存在非空 WAL", name));
            }
            if !(after_reopen.db.exists && after_reopen.db.len >= before_reopen.db.len) {
                return Err(anyhow!("{} 重启后 db 文件应包含 WAL 回放后的数据", name));
            }
            if after_reopen.wal.exists {
                return Err(anyhow!("{} 重启后 WAL 应已被 checkpoint 并清理", name));
            }
            if after_reopen.checkpoint_wal.exists {
                return Err(anyhow!("{} 重启后不应遗留 checkpoint WAL", name));
            }
            println!("结论: 场景1重启时会从 WAL 完整恢复数据，并将 WAL checkpoint 回 db 后清理。");
        }
        ScenarioExpectation::CheckpointBeforeClose => {
            if after_reopen.rows != total_rows {
                return Err(anyhow!(
                    "{} 重启后行数异常: expected {}, got {}",
                    name, total_rows, after_reopen.rows
                ));
            }
            if !(before_reopen.db.exists && before_reopen.db.len > 0) {
                return Err(anyhow!("{} 关闭前 db 文件应已有持久化数据", name));
            }
            if before_reopen.checkpoint_wal.exists {
                return Err(anyhow!("{} 关闭前不应遗留 checkpoint WAL", name));
            }
            if before_reopen.wal.exists && before_reopen.wal.len >= 16 * 1024 * 1024 {
                return Err(anyhow!(
                    "{} 自动 checkpoint 后 WAL 仍然大于等于 16 MiB",
                    name
                ));
            }
            if !(after_reopen.db.exists && after_reopen.db.len >= before_reopen.db.len) {
                return Err(anyhow!("{} 重启后 db 文件不应回退", name));
            }
            if after_reopen.checkpoint_wal.exists {
                return Err(anyhow!("{} 重启后不应遗留 checkpoint WAL", name));
            }
            if after_reopen.wal.exists {
                return Err(anyhow!("{} 重启后不应遗留 WAL", name));
            }
            println!("结论: 场景2在关闭前已经触发自动 checkpoint，重启后可直接读到全部数据。");
        }
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct Snapshot {
    db: FileState,
    wal: FileState,
    checkpoint_wal: FileState,
    rows: usize,
}

fn create_students_table(conn: &mut duckdb_rs::db::DuckConnection) {
    conn.create_table(
        "main",
        "students",
        vec![
            ("id".to_string(), LogicalType::integer()),
            ("age".to_string(), LogicalType::integer()),
            ("score".to_string(), LogicalType::integer()),
        ],
    )
    .expect("创建 students 表失败");
}

fn insert_students(conn: &duckdb_rs::db::DuckConnection, total_rows: usize) {
    for start in (0..total_rows).step_by(STANDARD_VECTOR_SIZE) {
        let count = (total_rows - start).min(STANDARD_VECTOR_SIZE);
        let mut chunk = build_student_chunk(start + 1, count);
        conn.insert("students", &mut chunk)
            .unwrap_or_else(|e| panic!("插入 students 失败: {}", e));
    }
}

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
        let score = 60 + (id % 40);
        let offset = i * 4;

        chunk.data[0].raw_data_mut()[offset..offset + 4].copy_from_slice(&id.to_le_bytes());
        chunk.data[1].raw_data_mut()[offset..offset + 4].copy_from_slice(&age.to_le_bytes());
        chunk.data[2].raw_data_mut()[offset..offset + 4].copy_from_slice(&score.to_le_bytes());
    }

    chunk.set_cardinality(count);
    chunk
}

fn count_rows(engine: &DuckEngine) -> usize {
    let conn = engine.connect();
    conn.begin_transaction()
        .expect("count begin_transaction 失败");
    let chunks = conn.scan("students", None).expect("scan students 失败");
    conn.commit().expect("count commit 失败");
    chunks.iter().map(|chunk| chunk.size()).sum()
}

fn cleanup(db_path: &str) {
    for suffix in ["", ".wal", ".wal.checkpoint", ".wal.recovery"] {
        let path = format!("{}{}", db_path, suffix);
        if Path::new(&path).exists() {
            fs::remove_file(&path).unwrap_or_else(|e| panic!("删除 {} 失败: {}", path, e));
        }
    }
}

fn file_state(path: &str) -> FileState {
    match fs::metadata(path) {
        Ok(meta) => FileState {
            exists: true,
            len: meta.len(),
        },
        Err(_) => FileState {
            exists: false,
            len: 0,
        },
    }
}

fn print_file_state(name: &str, state: FileState) {
    if state.exists {
        println!("  {:<15} exists, size={} bytes", name, state.len);
    } else {
        println!("  {:<15} missing", name);
    }
}
