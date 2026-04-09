use duckdb_rs::db::DuckEngine;

/// 数据库文件路径
const DB_PATH: &str = "./data/tpch-sf1.db";

fn main() {
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         读取 tpch-sf1.db 测试用例 (DuckEngine)       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    if !std::path::Path::new(DB_PATH).exists() {
        println!("错误: 数据库文件不存在: {}", DB_PATH);
        return;
    }

    // ─── 步骤 1：打开数据库 ────────────────────────────────────────────────────
    println!("步骤 1：打开数据库");
    println!("  数据库路径: {}", DB_PATH);
    let engine = DuckEngine::open(DB_PATH).expect("打开数据库失败");
    println!("  ✓ 数据库已打开");
    println!();

    // ─── 步骤 2：查看表列表 ────────────────────────────────────────────────────
    println!("步骤 2：查看表列表");
    println!("  表列表: {:?}", engine.tables());
    println!();

    // ─── 步骤 3：扫描 TPCH 全表全列 ──────────────────────────────────────────
    println!("步骤 3：扫描 TPCH 全表全列");
    let conn = engine.connect();
    for table in engine.tables() {
        println!("  扫描表: {}", table);
        let chunks = conn.scan(&table, None).unwrap_or_else(|err| {
            panic!("scan 表 {} 失败: {}", table, err);
        });
        let total_rows: usize = chunks.iter().map(|c| c.size()).sum();
        println!("    ✓ 读取完成");
        println!("      Chunk 数量: {}", chunks.len());
        println!("      总行数: {}", total_rows);
    }
    println!();
}
