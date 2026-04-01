# duckdb_rs
duckdb存储的Rust实现





## TODO

- [ ] 统计信息更新
- [ ] 多数据库实例DataBaseInstance、AttachedDatabase以及MetaTransaction
- [ ] 引入第三方库简化代码
- [ ] 重新定义接口

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

- [ ] PRAGMA storage_info 里 min/max 统计仍显示为 NULL，说明统计信息序列化还没完全对齐官方格式，但它不再影响数据读取正确性。