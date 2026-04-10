use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use duckdb_rs::common::types::{DataChunk, STANDARD_VECTOR_SIZE};
use duckdb_rs::db::{DuckEngine, TableScanRequest};
use duckdb_rs::storage::data_table::StorageIndex;
use duckdb_rs::storage::table::segment_base::SegmentBase;
use tokio::sync::Mutex;

const DB_PATH: &str = "./data/tpch-sf1.db";
const TABLE_NAME: &str = "lineitem";
const TASK_COUNT: usize = 4;

#[derive(Default, Debug, Clone, Copy)]
struct WorkerStats {
    tasks_claimed: usize,
    chunks_read: usize,
    rows_read: usize,
}

#[derive(Debug, Clone, Copy)]
struct ClaimedTask {
    row_group_start: u64,
    row_group_rows: u64,
    chunks_read: usize,
    rows_read: usize,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║   DuckDB Rust — tokio 并行扫描 mock 执行器 (lineitem / 4 tasks)  ║");
    println!("╚════════════════════════════════════════════════════════════════════╝");
    println!();

    if !Path::new(DB_PATH).exists() {
        return Err(anyhow!("数据库文件不存在: {}", DB_PATH));
    }

    println!("步骤 1：打开数据库");
    println!("  路径: {}", DB_PATH);
    let engine = DuckEngine::open(DB_PATH).context("打开数据库失败")?;
    let conn = engine.connect();
    println!("  表列表: {:?}", engine.tables());
    println!();

    println!("步骤 2：执行器主线程创建 pipeline/source global state");
    let request = TableScanRequest::new(Vec::<StorageIndex>::new());
    let parallel_scan = conn
        .begin_parallel_scan(TABLE_NAME, request)
        .context("begin_parallel_scan 失败")?;
    let result_types = parallel_scan.result_types().to_vec();
    let shared_scan = Arc::new(Mutex::new(parallel_scan));
    println!("  已调用 DuckConnection::begin_parallel_scan");
    println!("  内部链路: DuckTableScanInitGlobal -> DataTable::InitializeParallelScan");
    println!("  输出列数: {}", result_types.len());
    println!();

    println!("步骤 3：启动 4 个 tokio worker task");
    let mut handles = Vec::with_capacity(TASK_COUNT);
    for worker_id in 0..TASK_COUNT {
        let shared_scan = Arc::clone(&shared_scan);
        let result_types = result_types.clone();
        handles.push(tokio::spawn(async move {
            let mut stats = WorkerStats::default();
            let mut claimed_tasks = Vec::new();
            loop {
                let next_task = {
                    let mut guard = shared_scan.lock().await;
                    guard
                        .next_task()
                        .map_err(|e| anyhow!("worker {} next_task 失败: {}", worker_id, e))?
                };

                let Some(mut task) = next_task else {
                    break;
                };

                stats.tasks_claimed += 1;
                let current_row_group = task
                    .local_state()
                    .scan_state
                    .table_state
                    .current_row_group
                    .clone()
                    .ok_or_else(|| anyhow!("worker {} 领取任务后缺少 current_row_group", worker_id))?;
                let mut claimed_task = ClaimedTask {
                    row_group_start: current_row_group.row_start,
                    row_group_rows: current_row_group.row_group.count(),
                    chunks_read: 0,
                    rows_read: 0,
                };

                loop {
                    let mut chunk = DataChunk::new();
                    chunk.initialize(&result_types, STANDARD_VECTOR_SIZE);
                    let has_data = task
                        .next_chunk(&mut chunk)
                        .map_err(|e| anyhow!("worker {} next_chunk 失败: {}", worker_id, e))?;
                    if !has_data {
                        break;
                    }
                    stats.chunks_read += 1;
                    stats.rows_read += chunk.size();
                    claimed_task.chunks_read += 1;
                    claimed_task.rows_read += chunk.size();
                }
                claimed_tasks.push(claimed_task);
            }
            Ok::<(usize, WorkerStats, Vec<ClaimedTask>), anyhow::Error>((
                worker_id,
                stats,
                claimed_tasks,
            ))
        }));
    }

    println!("步骤 4：每个 worker 按 DuckDB 并行 scan 流程工作");
    println!("  local init: local_scan_state.Initialize(...)");
    println!("  task claim: DataTable::NextParallelScan(global_parallel_state, local_scan_state)");
    println!("  chunk loop: DataTable::Scan(tx, chunk, local_scan_state)");
    println!();

    let mut total_tasks = 0usize;
    let mut total_chunks = 0usize;
    let mut total_rows = 0usize;

    for handle in handles {
        let (worker_id, stats, claimed_tasks) = handle
            .await
            .map_err(|e| anyhow!("tokio worker join 失败: {}", e))??;
        total_tasks += stats.tasks_claimed;
        total_chunks += stats.chunks_read;
        total_rows += stats.rows_read;
        println!(
            "worker {:>2}: tasks={}, chunks={}, rows={}",
            worker_id, stats.tasks_claimed, stats.chunks_read, stats.rows_read
        );
        for (task_idx, claimed_task) in claimed_tasks.iter().enumerate() {
            println!(
                "  task {:>2}: row_group_start={}, row_group_rows={}, chunks={}, rows={}",
                task_idx,
                claimed_task.row_group_start,
                claimed_task.row_group_rows,
                claimed_task.chunks_read,
                claimed_task.rows_read
            );
        }
    }

    println!();
    println!("汇总：");
    println!("  总任务数: {}", total_tasks);
    println!("  总 chunk 数: {}", total_chunks);
    println!("  {} 总行数: {}", TABLE_NAME, total_rows);
    println!();

    if total_rows == 0 {
        return Err(anyhow!("并行扫描结果为空，期望至少读取到 {} 表数据", TABLE_NAME));
    }

    Ok(())
}
