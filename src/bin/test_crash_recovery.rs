use duckdb_rs::common::types::{DataChunk, LogicalType};
use duckdb_rs::db::DB;

fn main() -> Result<(), String> {
    let db_path_main = format!(
        "/tmp/duckdb_rs_crash_recovery_main_{}.db",
        std::process::id()
    );

    cleanup_db_files(&db_path_main);

    scenario_main_wal_recovery(&db_path_main)?;

    cleanup_db_files(&db_path_main);

    println!("crash recovery scenarios passed");
    Ok(())
}

fn cleanup_db_files(db_path: &str) {
    let wal_path = format!("{}.wal", db_path);
    let checkpoint_wal_path = format!("{}.checkpoint.wal", db_path);
    let recovery_wal_path = format!("{}.recovery", wal_path);
    for path in [db_path, wal_path.as_str(), checkpoint_wal_path.as_str(), recovery_wal_path.as_str()] {
        let _ = std::fs::remove_file(path);
    }
}

fn scenario_main_wal_recovery(db_path: &str) -> Result<(), String> {
    {
        let mut db = DB::open(db_path).map_err(|e| format!("{e:?}"))?;
        db.create_table(
            "main",
            "items",
            vec![("id".to_string(), LogicalType::integer())],
        );
        insert_i32_rows(&db, "items", &[1, 2, 3])?;
        db.checkpoint().map_err(|e| format!("{e:?}"))?;
        insert_i32_rows(&db, "items", &[10, 11])?;
    }

    let reopened = DB::open(db_path).map_err(|e| format!("{e:?}"))?;
    let values = scan_i32_values(&reopened, "items")?;
    assert_eq!(&values[..3], &[1, 2, 3], "checkpointed rows changed unexpectedly");
    assert_eq!(values.len(), 5, "main WAL recovery row count mismatch");
    Ok(())
}

fn insert_i32_rows(db: &DB, table: &str, values: &[i32]) -> Result<(), String> {
    let mut chunk = DataChunk::new();
    chunk.initialize(&[LogicalType::integer()], values.len());
    for (idx, value) in values.iter().enumerate() {
        let start = idx * 4;
        chunk.data[0].raw_data_mut()[start..start + 4].copy_from_slice(&value.to_le_bytes());
    }
    chunk.set_cardinality(values.len());
    db.insert_chunk(table, &mut chunk)
        .map_err(|e| format!("{e:?}"))?;
    Ok(())
}

fn scan_i32_values(db: &DB, table: &str) -> Result<Vec<i32>, String> {
    let chunks = db.scan_chunks(table, None).map_err(|e| format!("{e:?}"))?;
    let mut values = Vec::new();
    for chunk in chunks {
        let raw = chunk.data[0].raw_data();
        for idx in 0..chunk.size() {
            let start = idx * 4;
            values.push(i32::from_le_bytes(raw[start..start + 4].try_into().unwrap()));
        }
    }
    Ok(values)
}
