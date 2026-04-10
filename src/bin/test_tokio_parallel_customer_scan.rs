use std::path::Path;
use std::sync::Arc;
use anyhow::{Context, Result, anyhow};
use duckdb_rs::common::types::{DataChunk, STANDARD_VECTOR_SIZE};
use duckdb_rs::db::{DuckEngine, TableScanRequest};
use duckdb_rs::storage::data_table::StorageIndex;

const DB_PATH: &str = "./data/tpch-sf1.db";
const TABLE_NAME: &str = "lineitem";

#[derive(Default, Debug, Clone, Copy)]
struct WorkerStats {
    chunks_read: usize,
    rows_read: usize,
}

#[derive(Debug, Default)]
struct WorkerResult {
    stats: WorkerStats,
    sample_chunk: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let display_task_count = 15usize;

    println!();
    println!("╔════════════════════════════════════════════════════════════════════╗");
    println!("║    DuckDB Rust — tokio 并行扫描 mock 执行器 (lineitem scan)     ║");
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
    let global_scan = Arc::new(
        conn.duck_table_scan_init_global(TABLE_NAME, request)
            .context("duck_table_scan_init_global 失败")?,
    );
    let result_types = global_scan.result_types().to_vec();
    println!("  已调用 DuckConnection::duck_table_scan_init_global");
    println!("  内部链路: DuckTableScanInitGlobal -> DataTable::InitializeParallelScan -> DataTable::MaxThreads");
    println!("  输出列数: {}", result_types.len());
    println!("  source max_threads: {}", global_scan.max_threads());
    println!("  本次用于打印演示的 task 数: {}", display_task_count);
    println!();

    println!("步骤 3：按建议任务数启动 tokio worker task");
    let mut handles = Vec::with_capacity(display_task_count);
    for worker_id in 0..display_task_count {
        let global_scan = Arc::clone(&global_scan);
        let result_types = result_types.clone();
        handles.push(tokio::spawn(async move {
            let mut result = WorkerResult::default();
            let mut local_state = global_scan.init_local_state();
            loop {
                let mut chunk = DataChunk::new();
                chunk.initialize(&result_types, STANDARD_VECTOR_SIZE);
                let has_data = global_scan
                    .table_scan(&mut local_state, &mut chunk)
                    .map_err(|e| anyhow!("worker {} table_scan 失败: {}", worker_id, e))?;
                if !has_data {
                    break;
                }
                result.stats.chunks_read += 1;
                result.stats.rows_read += chunk.size();
            }
            Ok::<(usize, WorkerResult), anyhow::Error>((worker_id, result))
        }));
    }

    println!("步骤 4：每个 worker 按 DuckDB 并行 scan 流程工作");
    println!("  local init: DuckTableScanState::init_local_state()");
    println!("  scan loop: DuckTableScanState::table_scan(local_state, chunk)");
    println!("  内部链路: InitLocalState -> DataTable::NextParallelScan(...); TableScanFunc -> DataTable::Scan(...) / DataTable::NextParallelScan(...)");
    println!();

    let mut total_chunks = 0usize;
    let mut total_rows = 0usize;

    for handle in handles {
        let (worker_id, result) = handle
            .await
            .map_err(|e| anyhow!("tokio worker join 失败: {}", e))??;
        if let Some(sample_chunk) = result.sample_chunk.as_ref() {
            println!("worker {:>2} sample chunk:", worker_id);
            print!("{sample_chunk}");
        }
        let stats = result.stats;
        total_chunks += stats.chunks_read;
        total_rows += stats.rows_read;
        println!(
            "worker {:>2}: chunks={}, rows={}",
            worker_id, stats.chunks_read, stats.rows_read
        );
    }

    println!();
    println!("汇总：");
    println!("  总 chunk 数: {}", total_chunks);
    println!("  {} 总行数: {}", TABLE_NAME, total_rows);
    println!();

    if total_rows == 0 {
        return Err(anyhow!("并行扫描结果为空，期望至少读取到 {} 表数据", TABLE_NAME));
    }

    Ok(())
}
