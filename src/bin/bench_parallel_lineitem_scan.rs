use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use duckdb_rs::common::types::{DataChunk, STANDARD_VECTOR_SIZE};
use duckdb_rs::db::{DuckEngine, TableScanRequest};
use duckdb_rs::storage::data_table::StorageIndex;

const DB_PATH: &str = "./data/tpch-sf1.db";
const TABLE_NAME: &str = "lineitem";
const EXPECTED_ROWS: usize = 6_001_215;
const THREAD_COUNTS: [usize; 3] = [2, 8, 16];

#[derive(Default, Debug, Clone, Copy)]
struct WorkerStats {
    chunks_read: usize,
    rows_read: usize,
}

#[derive(Debug, Clone, Copy)]
struct BenchResult {
    threads: usize,
    elapsed: Duration,
    rows: usize,
    chunks: usize,
}

fn main() -> Result<()> {
    if !Path::new(DB_PATH).exists() {
        return Err(anyhow!("数据库文件不存在: {}", DB_PATH));
    }

    println!("并发扫描 benchmark: table={}, db={}", TABLE_NAME, DB_PATH);
    println!("预期总行数: {}", EXPECTED_ROWS);
    println!();

    let mut results = Vec::new();
    for threads in THREAD_COUNTS {
        let result = run_benchmark(threads)
            .with_context(|| format!("{} 线程 benchmark 失败", threads))?;
        println!(
            "threads={:>2} elapsed={:.3}s rows={} chunks={} rows/s={:.0}",
            result.threads,
            result.elapsed.as_secs_f64(),
            result.rows,
            result.chunks,
            result.rows as f64 / result.elapsed.as_secs_f64()
        );
        results.push(result);
    }

    println!();
    println!("加速比（相对 2 线程）:");
    let baseline = results
        .first()
        .ok_or_else(|| anyhow!("缺少 baseline benchmark 结果"))?;
    for result in &results {
        println!(
            "threads={:>2} speedup={:.3}x",
            result.threads,
            baseline.elapsed.as_secs_f64() / result.elapsed.as_secs_f64()
        );
    }

    Ok(())
}

fn run_benchmark(threads: usize) -> Result<BenchResult> {
    let engine = DuckEngine::open(DB_PATH).context("打开数据库失败")?;
    let conn = engine.connect();
    let request = TableScanRequest::new(Vec::<StorageIndex>::new());
    let global_scan = Arc::new(
        conn.duck_table_scan_init_global(TABLE_NAME, request)
            .context("duck_table_scan_init_global 失败")?,
    );
    let result_types = Arc::new(global_scan.result_types().to_vec());
    let start_barrier = Arc::new(Barrier::new(threads + 1));

    let mut handles = Vec::with_capacity(threads);
    for worker_id in 0..threads {
        let global_scan = Arc::clone(&global_scan);
        let result_types = Arc::clone(&result_types);
        let start_barrier = Arc::clone(&start_barrier);
        handles.push(thread::spawn(move || -> Result<(usize, WorkerStats)> {
            let mut stats = WorkerStats::default();
            let mut local_state = global_scan.init_local_state();
            start_barrier.wait();
            loop {
                let mut chunk = DataChunk::new();
                chunk.initialize(&result_types, STANDARD_VECTOR_SIZE);
                let has_data = global_scan
                    .table_scan(&mut local_state, &mut chunk)
                    .map_err(|e| anyhow!("worker {} table_scan 失败: {}", worker_id, e))?;
                if !has_data {
                    break;
                }
                stats.chunks_read += 1;
                stats.rows_read += chunk.size();
            }
            Ok((worker_id, stats))
        }));
    }

    start_barrier.wait();
    let start = Instant::now();

    let mut total_rows = 0usize;
    let mut total_chunks = 0usize;
    for handle in handles {
        let (_worker_id, stats) = handle.join().map_err(|_| anyhow!("worker panic"))??;
        total_rows += stats.rows_read;
        total_chunks += stats.chunks_read;
    }
    let elapsed = start.elapsed();

    if total_rows != EXPECTED_ROWS {
        return Err(anyhow!(
            "扫描结果不正确: threads={}, expected_rows={}, actual_rows={}",
            threads,
            EXPECTED_ROWS,
            total_rows
        ));
    }

    Ok(BenchResult {
        threads,
        elapsed,
        rows: total_rows,
        chunks: total_chunks,
    })
}
