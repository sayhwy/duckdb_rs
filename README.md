# duckdb_rs
duckdb存储的Rust实现





## TODO

* D:\code\duckdb_rs\src\bin\million_student_txn.rs生成的本地D:\code\duckdb_rs\million_students_txn.db，执行d:\duckdb.exe                                                                                                           D:\code\duckdb_rs\million_students_txn.db，duckdb只能读取前面的256行数据，后面全部是NULL，请参考duckdb的源码和duckdb_rs的源码比较，并去解决  
* 统计信息更新
* 重新定义接口

```
pub trait StorageEngine {
    // Catalog
    fn get_table_schema(&self, name: &str, txn: TxnId) -> Result<Schema>;
    
    // Scan（核心接口，push 模型由执行器驱动）
    fn scan(
        &self,
        table: TableId,
        projection: &[ColumnId],
        filter: Option<&Expr>,
        txn: TxnId,
        sink: &mut dyn RecordBatchSink,  // 执行器注入的 sink
    ) -> Result<()>;
    
    // DML
    fn insert(&self, table: TableId, batch: RecordBatch, txn: TxnId) -> Result<()>;
    fn delete(&self, table: TableId, row_ids: &[RowId], txn: TxnId) -> Result<()>;
    
    // Transaction
    fn begin_txn(&self) -> Result<TxnId>;
    fn commit(&self, txn: TxnId) -> Result<()>;
    fn rollback(&self, txn: TxnId);
}
```

*  PRAGMA storage_info 里 min/max 统计仍显示为 NULL，说明统计信息序列化还没完全对齐官方格式，但它不再影响数据读取正确性。