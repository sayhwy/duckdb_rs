
duckdb_rs 在启动时已经会把“表级元数据”加载到内存，但不会把 row group、column、data block 全量读入；它采用了分层懒加
载。scan 持久化数据时 BufferPool 会参与缓存，而且这部分思路和 DuckDB C++ 是一致的。
但“启动流程是否与 DuckDB 源码实现一致”，答案是：总体策略一致，启动编排不一致，而且当前 duckdb_rs 还有一个关键架构偏
差。

启动时实际加载了什么

- DatabaseInstance::open 先调 storage_manager.initialize()，这里只创建运行时 BlockManager/BufferPool/MetadataManager
  和 WAL 壳，不负责把 checkpoint catalog/table 装进运行时对象里，见 src/storage/storage_manager.rs:923。
- 然后 open 又单独调用 read_catalog_from_db 读取 checkpoint catalog，给每张表建 TableHandle，再 read_table_data 读取
  PersistentTableData，见 src/db/mod.rs:57 和 src/db/mod.rs:1794。
- TableDataReader 目前只把这些表级信息读进来：
    - table_stats
    - row_group_count
    - block_pointer
    - base_table_pointer
      见 src/storage/checkpoint/table_data_reader.rs:25。
- DataTable::new 收到 PersistentTableData 后，只把 RowGroupCollection 设成“可从 metadata 懒展开”的状态：total_rows、
  metadata pointer、table stats、lazy RowGroupSegmentTree，见 src/storage/data_table.rs:193 和 src/storage/table/
  row_group_collection.rs:120。

哪些是懒加载

- row group 本身是懒加载的：RowGroupSegmentTree::load_segment 扫描到需要时才从 metadata 里读一个 RowGroupPointer 并
  构造 RowGroup，见 src/storage/table/row_group_segment_tree.rs:82。
- row group 里的列也是懒加载的：RowGroup::get_column -> load_column 才反序列化 PersistentColumnData 并创建
  ColumnData，见 src/storage/table/row_group.rs:233。
- 列段对应的数据块不是加载列时就读出来，而是在 scan 初始化 segment 时才 pin block，见 src/storage/table/
  column_segment.rs:338 和 src/storage/compression/uncompressed.rs:221。

scan 时 BufferPool 是否参与

- 会参与，但仅限持久化 segment。
- 链路是：
    1. DuckTableScanInitGlobal / DataTable::scan 进入 scan，见 src/db/duck_engine.rs:227 和 src/storage/
       data_table.rs:443
    2. RowGroup::initialize_scan
    3. ColumnData::begin_scan_vector_internal
    4. ColumnSegment::initialize_scan
    5. initialize_scan_buffer
    6. StandardBufferManager::pin
    7. BlockHandle::load_guarded
- 对应代码分别在 src/storage/table/row_group.rs:334、src/storage/table/column_data.rs:312、src/storage/table/
  column_segment.rs:685、src/storage/standard_buffer_manager.rs:153、src/storage/buffer/block_handle.rs:339。
- register_block 会复用同一个 block_id 的 BlockHandle，reader 归零后 block 进入 eviction queue，所以它确实是缓存，不
  是每次 scan 都强制重读，见 src/storage/single_file_block_manager.rs:697 和 src/storage/buffer/buffer_pool.rs:210。
- 纯内存/transaction-local 数据不走这条路径，它们直接扫内存结构。

与 DuckDB C++ 是否一致

- 一致的部分：
    - DuckDB 也是启动时只读表统计、row_group_count 和 block_pointer，不立即展开 row groups，见 /Users/liang/
      Documents/code/duckdb/src/storage/checkpoint/table_data_reader.cpp:16。
    - DuckDB 也是 RowGroupCollection::Initialize -> RowGroupSegmentTree::Initialize 挂上 lazy loader，见 /Users/
      liang/Documents/code/duckdb/src/storage/table/row_group_collection.cpp:112 和 /Users/liang/Documents/code/
      duckdb/src/storage/table/row_group_collection.cpp:34。
    - DuckDB 也是 RowGroup::GetColumn -> LoadColumn 懒加载列，见 /Users/liang/Documents/code/duckdb/src/storage/
      table/row_group.cpp:132。
    - DuckDB 也是 scan 时 FixedSizeInitScan -> BufferManager::Pin 进入 buffer cache，见 /Users/liang/Documents/code/
      duckdb/src/storage/compression/fixed_size_uncompressed.cpp:145 和 /Users/liang/Documents/code/duckdb/src/
      storage/standard_buffer_manager.cpp:308。
- 不一致的部分：
    - DuckDB 把“加载已有数据库文件、checkpoint、WAL replay”都放在 SingleFileStorageManager::LoadDatabase 里完成，
      见 /Users/liang/Documents/code/duckdb/src/storage/storage_manager.cpp:366。
      duckdb_rs 则拆成了 storage_manager.initialize() + read_catalog_from_db() + recover_database() 三段，见 src/db/
      mod.rs:57。
    - 更关键：duckdb_rs 读取 checkpoint catalog 时重新造了一套“只读 BlockManager/BufferPool/TableIOManager”，然后把
      这套 IO manager 绑定到了已加载表上，见 src/db/mod.rs:1806 到 src/db/mod.rs:1846。
      这和 DuckDB 不一致。DuckDB 的已加载表使用的是 StorageManager::LoadDatabase 建出来的那一套运行时 block manager/
      table io manager，不会再额外分叉一套只读缓存体系。
    - 所以现在 duckdb_rs 虽然“有 BufferPool 缓存”，但已加载持久化表走的是那套临时读 checkpoint 时创建的 BufferPool/
      BlockManager，不是 storage_manager.initialize() 内主运行时那套。这不是 DuckDB 的结构。

如果你要我继续，我下一步可以把这部分整理成一张“DuckDB C++ 调用链 vs duckdb_rs 调用链”的逐层对照表，直接列到函数级
别。