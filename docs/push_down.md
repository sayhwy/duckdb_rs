定义
在 DuckDB 里，“下推”分两层，不要混在一起看：

1. 逻辑/优化器层的 filter pushdown
   把上层 WHERE/join 产生的谓词，尽量压到 LogicalGet.table_filters 里，表示“这些过滤条件可以在 scan 内部
   执行”，而不是先全量读出来再走上层 LogicalFilter。
2. 存储层 scan 内的 pruning/filtering
   table_filters 进入 table scan 后，又分成几步：

- row group 级别：先看 row group statistics/zonemap，能整组跳过就整组跳过
- segment 级别：扫描某个 row group 时，再看当前 column segment 的统计，能跳过整段/若干 vector 就跳
- vector 级别：真正读取 filter 列，生成 SelectionVector
- fetch/select 级别：只把 surviving rows 对应的数据取出来

DuckDB 的“下推”不是一个单点方法，而是一整条链。

———

核心结构
关键定义在这些地方：

- /Users/liang/Documents/code/duckdb/src/include/duckdb/planner/table_filter.hpp
- /Users/liang/Documents/code/duckdb/src/include/duckdb/planner/table_filter_set.hpp
- /Users/liang/Documents/code/duckdb/src/include/duckdb/planner/table_filter_state.hpp

TableFilterType 里最重要的几类是：

- CONSTANT_COMPARISON
- IS_NULL
- IS_NOT_NULL
- IN_FILTER
- CONJUNCTION_AND
- CONJUNCTION_OR
- STRUCT_EXTRACT
- OPTIONAL_FILTER
- EXPRESSION_FILTER
- BLOOM_FILTER
- PERFECT_HASH_JOIN_FILTER
- PREFIX_RANGE_FILTER

TableFilterSet 的语义是：

- 只保存“单列可执行”的 filter
- key 是 ProjectionIndex
- 像 A = 2 OR B = 4 这种跨列 OR，不能直接作为普通 TableFilterSet 下推

DuckDB 在注释里写得很明确，见 /Users/liang/Documents/code/duckdb/src/include/duckdb/planner/
table_filter_set.hpp。

———

整个下推流程
按 DuckDB 实际调用链看，是这条：

1. FilterPushdown 收集上层 filter
   入口在 /Users/liang/Documents/code/duckdb/src/optimizer/filter_pushdown.cpp
2. 到 LogicalGet 时尝试把表达式变成 TableFilterSet
   入口在 /Users/liang/Documents/code/duckdb/src/optimizer/pushdown/pushdown_get.cpp

核心逻辑：

- combiner.GenerateTableScanFilters(column_ids, pushdown_results)
- 成功的 filter 放进 get.table_filters
- 没法专门下推的，可能继续尝试 TryPushdownGenericExpression
- 剩下的保留成上层 LogicalFilter

3. FilterCombiner 把表达式改写成具体 TableFilter
   主要在 /Users/liang/Documents/code/duckdb/src/optimizer/filter_combiner.cpp

它会做这些事：

- 常量比较改成 ConstantFilter
- IN (...) 改成 InFilter
- IS NULL / IS NOT NULL 改成对应 null filter
- LIKE 'prefix%' 改成前缀范围 filter
- struct_extract(...) 外挂成 StructFilter
- 某些不能完全下推的，包成 OptionalFilter
- 任意单列表达式，可能退化成 ExpressionFilter

4. 逻辑计划转物理计划时，把 table_filters 挂到 PhysicalTableScan
   在 /Users/liang/Documents/code/duckdb/src/execution/physical_plan/plan_get.cpp

也就是：

- LogicalGet.table_filters
- MoveTableFilters(...)
- PhysicalTableScan(..., std::move(table_filters), ...)

5. 执行时，PhysicalTableScan 把 filters 放进 TableFunctionInitInput
   在 /Users/liang/Documents/code/duckdb/src/execution/operator/scan/physical_table_scan.cpp

这里会构造：

- TableFunctionInitInput(bind_data, column_ids, projection_ids, filters, ...)

6. 对 base table scan，进入 DuckTableScanInitGlobal / InitLocalState / TableScanFunc
   在 /Users/liang/Documents/code/duckdb/src/function/table/table_scan.cpp

也就是：

- DuckTableScanInitGlobal
- DuckTableScanState::InitLocalState
- DuckTableScanState::TableScanFunc

7. TableScanState::Initialize(...) 把 TableFilterSet 变成运行时 ScanFilterInfo
   在 /Users/liang/Documents/code/duckdb/src/storage/table/scan_state.cpp

这里会做：

- filters.Initialize(*context, *table_filters, column_ids)
- 每个 filter 生成一个 ScanFilter
- 每个 ScanFilter 持有 TableFilterState::Initialize(context, filter)

8. 后面 scan 真正进入 storage 层
   调用链是：

- /Users/liang/Documents/code/duckdb/src/function/table/table_scan.cpp
- /Users/liang/Documents/code/duckdb/src/storage/data_table.cpp
- /Users/liang/Documents/code/duckdb/src/storage/table/scan_state.cpp
- /Users/liang/Documents/code/duckdb/src/storage/table/row_group.cpp

———

zone filter / zonemap 是怎么做的
DuckDB 里你说的 “zone filter” 实际上就是基于 statistics 的 zonemap pruning。

分两层：

1. row group 级别 zonemap
   在 /Users/liang/Documents/code/duckdb/src/storage/table/row_group.cpp

逻辑是：

- 遍历当前 scan 的 filter_list
- 对每个 filter，调用
  /Users/liang/Documents/code/duckdb/src/storage/table/column_data.cpp
- 底层再调用 filter.CheckStatistics(stats)

结果有三种：

- FILTER_ALWAYS_FALSE：整个 row group 直接跳过
- FILTER_ALWAYS_TRUE：这个 filter 不用再执行了
- NO_PRUNING_POSSIBLE：不能靠 zonemap 判定，后面继续跑真正 filter

2. segment 级别 zonemap
   在 /Users/liang/Documents/code/duckdb/src/storage/table/row_group.cpp

它不是跳整个 row group，而是：

- 看当前 filter 所在列当前 segment 的 stats
- 如果某个 segment 一定不满足 filter
- 计算可以跳到哪个 vector_index
- 然后循环 NextVector(state) 跳过去

所以 DuckDB 的 zonemap 不是只做 row group pruning，也做 segment/vector skip。

具体统计判断来自各个 filter 的 CheckStatistics(...)：

- /Users/liang/Documents/code/duckdb/src/planner/filter/constant_filter.cpp
- /Users/liang/Documents/code/duckdb/src/planner/filter/in_filter.cpp
- /Users/liang/Documents/code/duckdb/src/planner/filter/null_filter.cpp
- /Users/liang/Documents/code/duckdb/src/planner/filter/conjunction_filter.cpp
- /Users/liang/Documents/code/duckdb/src/planner/filter/struct_filter.cpp

再往下，数值/字符串的 zonemap 判断在：

- /Users/liang/Documents/code/duckdb/src/include/duckdb/storage/statistics/numeric_stats.hpp
- /Users/liang/Documents/code/duckdb/src/include/duckdb/storage/statistics/string_stats.hpp

———

skip 是怎么实现的
skip 的语义是：当前这列/当前 vector 不需要真的解码，把 scan state 往前推进。

调用链：

- row group 里跳整 vector：
  /Users/liang/Documents/code/duckdb/src/storage/table/row_group.cpp
- 对每一列调用：
  GetColumn(column).Skip(state.column_scans[i])
- 再到：
  /Users/liang/Documents/code/duckdb/src/storage/table/column_data.cpp
- 本质就是：
  state.Next(s_count)

也就是说 DuckDB 的 Skip 不是“过滤掉数据后保留结果”，而是“这列根本不读，直接推进 scan 指针”。

这个在两种场景里很关键：

- 全 row group / 全 segment 已经被 zonemap 剪掉
- 某个 vector 经过 filter 后 approved_tuple_count == 0，未参与 filter 的列全部 Skip

———

filter 是怎么实现的
DuckDB 的 filter 是“先读 filter 列，再更新 selection vector”。

主路径在 /Users/liang/Documents/code/duckdb/src/storage/table/row_group.cpp：

1. 先拿版本可见性 selection

- GetSelVector(...)

2. 如果有 table filters

- 遍历 filter_list
- 对 filter 列调用
  col_data.Filter(...)

3. ColumnData::Filter(...)
   在 /Users/liang/Documents/code/duckdb/src/storage/table/column_data.cpp
   它会：

- 先 Scan(...) 读当前 filter 列向量
- result.ToUnifiedFormat(...)
- 然后调用
  /Users/liang/Documents/code/duckdb/src/storage/table/column_segment.cpp

4. ColumnSegment::FilterSelection(...) 是真正执行 filter 的公共核心
   它根据 TableFilterType 分派：

- CONSTANT_COMPARISON：类型化比较
- IS_NULL / IS_NOT_NULL
- CONJUNCTION_AND / OR
- STRUCT_EXTRACT
- OPTIONAL_FILTER
- EXPRESSION_FILTER
- join dynamic filters

这一步会更新 SelectionVector sel 和 approved_tuple_count。

所以 DuckDB 的 filter 执行顺序是：

- 先按 filter 列做筛选
- 后面的非 filter 列只 fetch/select surviving rows

———

select 是怎么实现的
DuckDB 的 select 不是 SQL SELECT，而是“已知 selection vector 后，只把需要的 tuple 取出来”。

调用链：

- 在 /Users/liang/Documents/code/duckdb/src/storage/table/row_group.cpp
  对“没有作为 filter 列提前扫描”的其他列调用：
  col_data.Select(...)
- 到 /Users/liang/Documents/code/duckdb/src/storage/table/column_data.cpp
  它先 Scan(...) 当前 vector
  再 result.Slice(sel, s_count)

如果底层刚好能在单 segment 内直接按 sel 取，还会走更专门的 SelectVector(...)。

所以 select 的语义是：

- 已经有 sel
- 只保留 sel 指向的行

———

fetch 是怎么实现的
fetch 用在 row-id 定位取数，不是顺序 scan。

入口：

- /Users/liang/Documents/code/duckdb/src/storage/data_table.cpp
- /Users/liang/Documents/code/duckdb/src/storage/table/row_group_collection.cpp

逻辑：

1. 传入 row id vector
2. 找每个 row id 属于哪个 row group
3. 做可见性检查：
   current_row_group.Fetch(transaction, offset_in_row_group)
4. 真正取行：
   current_row_group.FetchRow(...)

FetchRow(...) 再对每一列调用：

- /Users/liang/Documents/code/duckdb/src/storage/table/row_group.cpp 内部调列对象
- 最终走 ColumnData::Fetch(...) / ColumnSegment::FetchRow(...)

这个路径主要服务：

- index scan 命中的 row ids 回表
- row id 精确抓取
- 不是顺序 scan 主路径

———

并行 scan 和 filter 的关系
并行只是分配 row group/batch，不改变 filter 语义。

调用链：

- /Users/liang/Documents/code/duckdb/src/function/table/table_scan.cpp
- /Users/liang/Documents/code/duckdb/src/storage/data_table.cpp
- /Users/liang/Documents/code/duckdb/src/storage/table/row_group_collection.cpp

每个 worker 初始化本地状态时：

- /Users/liang/Documents/code/duckdb/src/function/table/table_scan.cpp
- TableScanState::Initialize(...)
- storage.NextParallelScan(...)

真正分任务在：

- /Users/liang/Documents/code/duckdb/src/storage/table/row_group_collection.cpp

它做的事是：

- 共享状态里拿一个 row group 或一个 verify-parallelism vector batch
- 调 InitializeScanInRowGroup(...)
- 本地 CollectionScanState 就只扫这块

之后真正 filter / zonemap / skip / select 仍然都在该 worker 的 RowGroup::Scan 内执行。

———

你最该先在 duckdb_rs 复刻的最小骨架
如果你现在还没做下推，按 DuckDB 复刻，建议顺序就是：

1. 先有 TableFilter / TableFilterSet / TableFilterState
2. 再让 TableScanState::Initialize(...) 持有 ScanFilterInfo
3. 在 RowGroup::CheckZonemap 做 row group 级 pruning
4. 在 RowGroup::CheckZonemapSegments 做 segment/vector skip
5. 在 RowGroup::Scan 里复刻 DuckDB 这段主逻辑：

- 先可见性 GetSelVector
- 再执行 filter 列 Filter
- 再对剩余列 Select
- 空结果时对未扫描列 Skip

6. 最后补 Fetch

这条顺序和 DuckDB 的真实结构最贴。

如果你要，我下一步可以继续按 DuckDB 源码，把这些点对应到 duckdb_rs 现有文件，给你列出“应该新增哪些结构、
哪些方法、调用链怎么接”，严格按 DuckDB 命名来拆。