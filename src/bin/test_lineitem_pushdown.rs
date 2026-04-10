use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use duckdb_rs::common::enums::ExpressionType;
use duckdb_rs::common::types::{DataChunk, STANDARD_VECTOR_SIZE};
use duckdb_rs::db::{DuckConnection, DuckEngine, TableScanRequest};
use duckdb_rs::storage::data_table::StorageIndex;
use duckdb_rs::{ConstantFilter, Value};

const DB_PATH: &str = "./data/tpch-sf1.db";
const TABLE_NAME: &str = "lineitem";
const L_ORDERKEY: u64 = 0;
const L_RETURNFLAG: u64 = 8;
const TARGET_ORDERKEY: i64 = 1;
const SAMPLE_ROWS: usize = 32;

fn main() -> Result<()> {
    if !Path::new(DB_PATH).exists() {
        return Err(anyhow!("数据库文件不存在: {}", DB_PATH));
    }

    println!("=== lineitem 下推验证 ===");
    println!("数据库: {}", DB_PATH);
    println!("表: {}", TABLE_NAME);

    let engine = DuckEngine::open(DB_PATH).context("打开数据库失败")?;
    let conn = engine.connect();

    test_projection_pushdown(&conn)?;
    test_predicate_pushdown_with_hidden_filter_column(&conn)?;

    println!("所有下推验证通过。");
    Ok(())
}

fn test_projection_pushdown(conn: &DuckConnection) -> Result<()> {
    println!("\n[1] 投影下推");

    let mut all_rows = 0usize;
    let mut all_pairs = Vec::new();
    let all_result_types = scan_request(
        conn,
        TableScanRequest::new(Vec::new()),
        |chunk| {
            let remaining = SAMPLE_ROWS.saturating_sub(all_pairs.len());
            let take_count = remaining.min(chunk.size());
            for row_idx in 0..take_count {
                all_pairs.push((
                    read_i64(chunk, L_ORDERKEY as usize, row_idx)?,
                    read_string(chunk, L_RETURNFLAG as usize, row_idx)?,
                ));
            }
            all_rows += chunk.size();
            Ok(())
        },
    )?;

    let projected_request =
        TableScanRequest::new(vec![StorageIndex(L_ORDERKEY), StorageIndex(L_RETURNFLAG)]);
    let mut projected_rows = 0usize;
    let mut projected_pairs = Vec::new();
    let projected_result_types = scan_request(conn, projected_request, |chunk| {
        if chunk.column_count() != 2 {
            return Err(anyhow!("投影下推返回列数错误，期望 2，实际 {}", chunk.column_count()));
        }
        let remaining = SAMPLE_ROWS.saturating_sub(projected_pairs.len());
        let take_count = remaining.min(chunk.size());
        for row_idx in 0..take_count {
            projected_pairs.push((read_i64(chunk, 0, row_idx)?, read_string(chunk, 1, row_idx)?));
        }
        projected_rows += chunk.size();
        Ok(())
    })?;

    if all_result_types != 16 {
        return Err(anyhow!("lineitem 全列扫描列数异常，期望 16，实际 {}", all_result_types));
    }
    if projected_result_types != 2 {
        return Err(anyhow!(
            "投影下推 result_types 列数错误，期望 2，实际 {}",
            projected_result_types
        ));
    }
    if projected_rows != all_rows {
        return Err(anyhow!(
            "投影下推行数错误，期望 {}，实际 {}",
            all_rows,
            projected_rows
        ));
    }
    if projected_pairs != all_pairs {
        return Err(anyhow!("投影下推结果与全列扫描投影结果不一致"));
    }

    println!("  全列行数: {}", all_rows);
    println!("  投影列: [l_orderkey, l_returnflag]");
    println!("  样本比对行数: {}", projected_pairs.len());
    Ok(())
}

fn test_predicate_pushdown_with_hidden_filter_column(conn: &DuckConnection) -> Result<()> {
    println!("\n[2] 谓词下推");

    let expected_request =
        TableScanRequest::new(vec![StorageIndex(L_ORDERKEY), StorageIndex(L_RETURNFLAG)]);
    let mut expected_flags = Vec::new();
    scan_request(conn, expected_request, |chunk| {
        for row_idx in 0..chunk.size() {
            if read_i64(chunk, 0, row_idx)? == TARGET_ORDERKEY {
                expected_flags.push(read_string(chunk, 1, row_idx)?);
            }
        }
        Ok(())
    })?;

    if expected_flags.is_empty() {
        return Err(anyhow!(
            "基线扫描未命中 l_orderkey = {}，无法验证谓词下推",
            TARGET_ORDERKEY
        ));
    }

    let mut visible_request =
        TableScanRequest::new(vec![StorageIndex(L_ORDERKEY), StorageIndex(L_RETURNFLAG)]);
    visible_request.push_filter(
        StorageIndex(L_ORDERKEY),
        Arc::new(ConstantFilter::new(
            ExpressionType::CompareEqual,
            Value::Integer(TARGET_ORDERKEY),
        )),
    );
    let mut visible_rows = 0usize;
    let visible_result_types = scan_request(conn, visible_request, |chunk| {
        visible_rows += chunk.size();
        Ok(())
    })?;
    if visible_result_types != 2 {
        return Err(anyhow!(
            "同列谓词下推 result_types 列数错误，期望 2，实际 {}",
            visible_result_types
        ));
    }
    if visible_rows != expected_flags.len() {
        return Err(anyhow!(
            "同列谓词下推结果错误，期望 {} 行，实际 {} 行",
            expected_flags.len(),
            visible_rows
        ));
    }

    let mut pushed_request = TableScanRequest::new(vec![StorageIndex(L_RETURNFLAG)]);
    pushed_request.push_filter(
        StorageIndex(L_ORDERKEY),
        Arc::new(ConstantFilter::new(
            ExpressionType::CompareEqual,
            Value::Integer(TARGET_ORDERKEY),
        )),
    );

    let mut pushed_flags = Vec::new();
    let pushed_result_types = scan_request(conn, pushed_request, |chunk| {
        if chunk.column_count() != 1 {
            return Err(anyhow!(
                "谓词下推返回列数错误，期望 1，实际 {}",
                chunk.column_count()
            ));
        }
        for row_idx in 0..chunk.size() {
            pushed_flags.push(read_string(chunk, 0, row_idx)?);
        }
        Ok(())
    })?;

    if pushed_result_types != 1 {
        return Err(anyhow!(
            "谓词下推 result_types 列数错误，期望 1，实际 {}",
            pushed_result_types
        ));
    }
    if pushed_flags != expected_flags {
        return Err(anyhow!(
            "谓词下推结果与基线结果不一致，期望 {:?}，实际 {:?}",
            expected_flags,
            pushed_flags
        ));
    }

    println!(
        "  谓词: l_orderkey = {}，命中行数: {}",
        TARGET_ORDERKEY,
        pushed_flags.len()
    );
    println!("  输出列: [l_returnflag]，过滤列通过隐藏扫描列补入");
    Ok(())
}

fn scan_request<F>(conn: &DuckConnection, request: TableScanRequest, mut f: F) -> Result<usize>
where
    F: FnMut(&DataChunk) -> Result<()>,
{
    let global_state = conn
        .duck_table_scan_init_global(TABLE_NAME, request)
        .context("duck_table_scan_init_global 失败")?;
    let result_types = global_state.result_types().to_vec();
    let result_type_count = result_types.len();
    let mut local_state = global_state.init_local_state();

    loop {
        let mut chunk = DataChunk::new();
        chunk.initialize(&result_types, STANDARD_VECTOR_SIZE);
        let has_data = global_state
            .table_scan(&mut local_state, &mut chunk)
            .context("table_scan 失败")?;
        if !has_data {
            break;
        }
        chunk.flatten();
        f(&chunk)?;
    }
    Ok(result_type_count)
}

fn read_i64(chunk: &DataChunk, col: usize, row: usize) -> Result<i64> {
    let raw = chunk.data[col].raw_data();
    let width = chunk.data[col].logical_type.physical_size();
    let offset = row * width;
    match width {
        1 => {
            let bytes = raw
                .get(offset..offset + 1)
                .ok_or_else(|| anyhow!("列 {col} 第 {row} 行超界"))?;
            Ok(i8::from_le_bytes(bytes.try_into().unwrap()) as i64)
        }
        2 => {
            let bytes = raw
                .get(offset..offset + 2)
                .ok_or_else(|| anyhow!("列 {col} 第 {row} 行超界"))?;
            Ok(i16::from_le_bytes(bytes.try_into().unwrap()) as i64)
        }
        4 => {
            let bytes = raw
                .get(offset..offset + 4)
                .ok_or_else(|| anyhow!("列 {col} 第 {row} 行超界"))?;
            Ok(i32::from_le_bytes(bytes.try_into().unwrap()) as i64)
        }
        8 => {
            let bytes = raw
                .get(offset..offset + 8)
                .ok_or_else(|| anyhow!("列 {col} 第 {row} 行超界"))?;
            Ok(i64::from_le_bytes(bytes.try_into().unwrap()))
        }
        other => Err(anyhow!("列 {col} 不支持按整数读取，物理宽度 {}", other)),
    }
}

fn read_string(chunk: &DataChunk, col: usize, row: usize) -> Result<String> {
    if !chunk.data[col].validity.row_is_valid(row) {
        return Err(anyhow!("列 {col} 第 {row} 行为 NULL"));
    }
    Ok(String::from_utf8_lossy(&chunk.data[col].read_varchar_bytes(row)).into_owned())
}
