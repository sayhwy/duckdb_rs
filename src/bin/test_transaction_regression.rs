use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use duckdb_rs::common::errors::{Result, anyhow};
use duckdb_rs::common::types::{DataChunk, DataChunkBuilder, LogicalType};
use duckdb_rs::db::DuckEngine;

const DB_PATH: &str = "transaction_regression.db";
const TABLE_ACCOUNTS: &str = "accounts";
const TABLE_CONCURRENT: &str = "concurrent_accounts";

fn main() -> Result<()> {
    cleanup(DB_PATH);

    let engine = Arc::new(DuckEngine::open(DB_PATH)?);
    println!("=== 事务回归测试 ===");
    println!("数据库文件: {}", DB_PATH);

    let mut ddl_conn = engine.connect();
    ddl_conn.create_table(
        "main",
        TABLE_ACCOUNTS,
        vec![
            ("id".to_string(), LogicalType::integer()),
            ("balance".to_string(), LogicalType::integer()),
            ("active".to_string(), LogicalType::boolean()),
        ],
    )?;
    ddl_conn.create_table(
        "main",
        TABLE_CONCURRENT,
        vec![
            ("worker_id".to_string(), LogicalType::integer()),
            ("seq".to_string(), LogicalType::integer()),
        ],
    )?;

    let mut failures = Vec::new();
    run_case("提交插入事务", &mut failures, || test_insert_commit(&engine));
    run_case("单连接禁止多事务", &mut failures, || {
        test_single_connection_single_active_transaction(&engine)
    });
    run_case("回滚插入事务", &mut failures, || test_insert_rollback(&engine));
    run_case("更新事务", &mut failures, || test_update_commit(&engine));
    run_case("删除事务", &mut failures, || test_delete_commit(&engine));
    run_case("并发事务", &mut failures, || test_concurrent_transactions(&engine));

    println!("\n=== 汇总 ===");
    if failures.is_empty() {
        println!("所有事务场景验证通过。");
        Ok(())
    } else {
        for failure in &failures {
            println!("- {}", failure);
        }
        Err(anyhow!("事务回归测试存在 {} 个失败场景", failures.len()))
    }
}

fn run_case<F>(name: &str, failures: &mut Vec<String>, f: F)
where
    F: FnOnce() -> Result<()>,
{
    if let Err(err) = f() {
        failures.push(format!("{}: {}", name, err));
    }
}

fn test_insert_commit(engine: &DuckEngine) -> Result<()> {
    println!("\n[1] 提交插入事务");
    let conn = engine.connect();
    conn.begin_transaction()?;
    let mut chunk = build_accounts_chunk(&[
        (1, 100, true),
        (2, 200, true),
        (3, 300, true),
        (4, 400, true),
        (5, 500, true),
    ])?;
    conn.insert(TABLE_ACCOUNTS, &mut chunk)?;
    conn.commit()?;

    let rows = load_accounts(engine)?;
    ensure_account_count(&rows, 5)?;
    ensure_account_balance(&rows, 1, 100)?;
    ensure_account_balance(&rows, 5, 500)?;
    println!("  已提交 5 行初始数据");
    Ok(())
}

fn test_insert_rollback(engine: &DuckEngine) -> Result<()> {
    println!("\n[2] 回滚插入事务");
    let conn = engine.connect();
    conn.begin_transaction()?;
    let mut chunk = build_accounts_chunk(&[(101, 9100, false), (102, 9200, false)])?;
    conn.insert(TABLE_ACCOUNTS, &mut chunk)?;

    let visible_in_txn = load_accounts_from_conn(&conn)?;
    ensure_account_count(&visible_in_txn, 7)?;
    conn.rollback()?;

    let rows = load_accounts(engine)?;
    ensure_account_count(&rows, 5)?;
    if rows.contains_key(&101) || rows.contains_key(&102) {
        return Err(anyhow!("rollback 后不应看到未提交插入"));
    }
    println!("  回滚后未提交插入不可见");
    Ok(())
}

fn test_single_connection_single_active_transaction(engine: &DuckEngine) -> Result<()> {
    println!("\n[1.5] 单连接禁止多事务");
    let conn = engine.connect();
    conn.begin_transaction()?;

    let second_begin = conn.begin_transaction();
    if second_begin.is_ok() {
        return Err(anyhow!("同一连接不应允许重复 begin_transaction"));
    }
    let err = second_begin.err().unwrap().to_string();
    if !err.contains("already in transaction") {
        return Err(anyhow!("重复 begin_transaction 的错误信息不符合预期: {}", err));
    }

    let conn2 = engine.connect();
    conn2.begin_transaction()?;
    conn2.rollback()?;
    conn.rollback()?;
    println!("  同一连接第二次 BEGIN 被拒绝，不同连接可独立开启事务");
    Ok(())
}

fn test_update_commit(engine: &DuckEngine) -> Result<()> {
    println!("\n[3] 更新事务");
    let conn = engine.connect();
    conn.begin_transaction()?;

    let mut updates = DataChunkBuilder::new(&[LogicalType::integer(), LogicalType::boolean()], 2);
    updates.set_i32(0, 0, 250)?;
    updates.set_bool(1, 0, true)?;
    updates.set_i32(0, 1, 450)?;
    updates.set_bool(1, 1, false)?;
    let mut updates = updates.finish();

    conn.update(TABLE_ACCOUNTS, &[1, 3], &[1, 2], &mut updates)?;

    let visible_in_txn = load_accounts_from_conn(&conn)?;
    ensure_account_balance(&visible_in_txn, 2, 250)?;
    ensure_account_active(&visible_in_txn, 4, false)?;

    conn.commit()?;

    let rows = load_accounts(engine)?;
    ensure_account_balance(&rows, 2, 250)?;
    ensure_account_active(&rows, 4, false)?;
    println!("  已更新 row_id=1 和 row_id=3 对应记录");
    Ok(())
}

fn test_delete_commit(engine: &DuckEngine) -> Result<()> {
    println!("\n[4] 删除事务");
    let conn = engine.connect();
    conn.begin_transaction()?;
    let deleted = conn.delete(TABLE_ACCOUNTS, &[0, 4])?;
    if deleted != 2 {
        return Err(anyhow!("预期删除 2 行，实际 {}", deleted));
    }
    conn.commit()?;

    let rows = load_accounts(engine)?;
    ensure_account_count(&rows, 3)?;
    if rows.contains_key(&1) || rows.contains_key(&5) {
        return Err(anyhow!("删除后仍然读到了已删除记录"));
    }
    println!("  已删除两行，剩余 {} 行", rows.len());
    Ok(())
}

fn test_concurrent_transactions(engine: &Arc<DuckEngine>) -> Result<()> {
    println!("\n[5] 并发事务");
    let workers = 4usize;
    let rows_per_worker = 25usize;
    let barrier = Arc::new(Barrier::new(workers));

    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let engine = Arc::clone(engine);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || -> Result<()> {
            let conn = engine.connect();
            conn.begin_transaction()?;
            barrier.wait();

            let start = worker_id * rows_per_worker;
            let rows: Vec<(i32, i32)> = (0..rows_per_worker)
                .map(|seq| (worker_id as i32, (start + seq) as i32))
                .collect();
            let mut chunk = build_worker_chunk(&rows)?;
            conn.insert(TABLE_CONCURRENT, &mut chunk)?;

            if worker_id == workers - 1 {
                conn.rollback()?;
            } else {
                conn.commit()?;
            }
            Ok(())
        }));
    }

    for handle in handles {
        handle
            .join()
            .map_err(|_| anyhow!("并发线程 panic"))??;
    }

    let conn = engine.connect();
    let chunks = conn.scan(TABLE_CONCURRENT, None)?;
    let rows = decode_worker_rows(&chunks)?;
    let expected = (workers - 1) * rows_per_worker;
    if rows.len() != expected {
        return Err(anyhow!(
            "并发插入结果不符合预期，expected {}，actual {}",
            expected,
            rows.len()
        ));
    }
    if rows.iter().any(|(worker_id, _)| *worker_id == (workers as i32 - 1)) {
        return Err(anyhow!("回滚线程的数据不应写入最终结果"));
    }
    println!(
        "  {} 个线程并发写入，其中 1 个回滚，最终保留 {} 行",
        workers, expected
    );
    Ok(())
}

fn build_accounts_chunk(rows: &[(i32, i32, bool)]) -> Result<DataChunk> {
    let mut builder = DataChunkBuilder::new(
        &[
            LogicalType::integer(),
            LogicalType::integer(),
            LogicalType::boolean(),
        ],
        rows.len(),
    );
    for (row_idx, &(id, balance, active)) in rows.iter().enumerate() {
        builder.set_i32(0, row_idx, id)?;
        builder.set_i32(1, row_idx, balance)?;
        builder.set_bool(2, row_idx, active)?;
    }
    Ok(builder.finish())
}

fn build_worker_chunk(rows: &[(i32, i32)]) -> Result<DataChunk> {
    let mut builder =
        DataChunkBuilder::new(&[LogicalType::integer(), LogicalType::integer()], rows.len());
    for (row_idx, &(worker_id, seq)) in rows.iter().enumerate() {
        builder.set_i32(0, row_idx, worker_id)?;
        builder.set_i32(1, row_idx, seq)?;
    }
    Ok(builder.finish())
}

fn load_accounts(engine: &DuckEngine) -> Result<BTreeMap<i32, (i32, bool)>> {
    let conn = engine.connect();
    load_accounts_from_conn(&conn)
}

fn load_accounts_from_conn(
    conn: &duckdb_rs::db::DuckConnection,
) -> Result<BTreeMap<i32, (i32, bool)>> {
    let chunks = conn.scan(TABLE_ACCOUNTS, None)?;
    let mut rows = BTreeMap::new();
    for chunk in &chunks {
        for row_idx in 0..chunk.size() {
            let id = read_i32(chunk, 0, row_idx)?;
            let balance = read_i32(chunk, 1, row_idx)?;
            let active = read_bool(chunk, 2, row_idx)?;
            rows.insert(id, (balance, active));
        }
    }
    Ok(rows)
}

fn decode_worker_rows(chunks: &[DataChunk]) -> Result<Vec<(i32, i32)>> {
    let mut rows = Vec::new();
    for chunk in chunks {
        for row_idx in 0..chunk.size() {
            rows.push((read_i32(chunk, 0, row_idx)?, read_i32(chunk, 1, row_idx)?));
        }
    }
    Ok(rows)
}

fn read_i32(chunk: &DataChunk, col: usize, row: usize) -> Result<i32> {
    if !chunk.data[col].validity.row_is_valid(row) {
        return Err(anyhow!("列 {} 行 {} 为 NULL", col, row));
    }
    let offset = row * 4;
    Ok(i32::from_le_bytes(
        chunk.data[col].raw_data()[offset..offset + 4]
            .try_into()
            .map_err(|_| anyhow!("读取 i32 失败"))?,
    ))
}

fn read_bool(chunk: &DataChunk, col: usize, row: usize) -> Result<bool> {
    if !chunk.data[col].validity.row_is_valid(row) {
        return Err(anyhow!("列 {} 行 {} 为 NULL", col, row));
    }
    Ok(chunk.data[col].raw_data()[row] != 0)
}

fn ensure_account_count(rows: &BTreeMap<i32, (i32, bool)>, expected: usize) -> Result<()> {
    if rows.len() != expected {
        return Err(anyhow!("预期 {} 行，实际 {}", expected, rows.len()));
    }
    Ok(())
}

fn ensure_account_balance(
    rows: &BTreeMap<i32, (i32, bool)>,
    id: i32,
    expected: i32,
) -> Result<()> {
    let actual = rows
        .get(&id)
        .ok_or_else(|| anyhow!("未找到 id={}", id))?
        .0;
    if actual != expected {
        return Err(anyhow!(
            "id={} 的 balance 不匹配，expected {}，actual {}",
            id,
            expected,
            actual
        ));
    }
    Ok(())
}

fn ensure_account_active(
    rows: &BTreeMap<i32, (i32, bool)>,
    id: i32,
    expected: bool,
) -> Result<()> {
    let actual = rows
        .get(&id)
        .ok_or_else(|| anyhow!("未找到 id={}", id))?
        .1;
    if actual != expected {
        return Err(anyhow!(
            "id={} 的 active 不匹配，expected {}，actual {}",
            id,
            expected,
            actual
        ));
    }
    Ok(())
}

fn cleanup(db_path: &str) {
    for suffix in ["", ".wal", ".wal.checkpoint", ".wal.recovery"] {
        let path = format!("{}{}", db_path, suffix);
        if Path::new(&path).exists() {
            fs::remove_file(&path).unwrap_or_else(|e| panic!("删除 {} 失败: {}", path, e));
        }
    }
}
