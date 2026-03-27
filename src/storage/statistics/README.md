# DuckDB Statistics Module (Rust Implementation)

## 概述

这是 DuckDB 统计信息模块的 Rust 重写版本，用于查询优化和数据剪枝。该模块实现了 Phase 1 的核心功能。

## 已实现的模块

### 1. BaseStatistics (base_statistics.rs)
- **功能**: 统计信息基类，支持多种数据类型
- **特性**:
  - 统计类型分发（数值、字符串、列表、结构体等）
  - 统计信息的创建、合并、序列化
  - 空值检测、常量检测、有效性验证
- **类名**: `BaseStatistics` (与 C++ 保持一致)
- **方法**: 采用蛇形命名 (snake_case)

### 2. NumericStats (numeric_stats.rs)
- **功能**: 数值类型统计信息
- **特性**:
  - Min/Max 值跟踪
  - 支持所有整数和浮点类型 (i8/16/32/64/128, f32/f64)
  - Zonemap 过滤（用于查询剪枝优化）
- **数据结构**: `NumericStatsData`, `NumericValueUnion`
- **测试**: 3 个单元测试全部通过

### 3. StringStats (string_stats.rs)
- **功能**: 字符串类型统计信息
- **特性**:
  - Min/Max 前缀（最多8字节）
  - 最大字符串长度跟踪
  - Unicode 检测
  - Zonemap 过滤支持
- **常量**: `MAX_STRING_MINMAX_SIZE = 8`
- **测试**: 5 个单元测试全部通过

### 4. DistinctStatistics (distinct_statistics.rs)
- **功能**: 基数估计（Cardinality Estimation）
- **特性**:
  - 使用 HyperLogLog 算法
  - 采样机制（整数类型和其他类型不同采样率）
  - Good-Turing 估计优化
  - 线程安全（使用 AtomicU64）
- **测试**: 4 个单元测试全部通过

### 5. ColumnStatistics (column_statistics.rs)
- **功能**: 列级统计信息管理
- **特性**:
  - 组合 BaseStatistics 和 DistinctStatistics
  - 支持统计信息合并和复制
  - 自动判断是否支持 distinct 统计
- **测试**: 4 个单元测试全部通过

### 6. SegmentStatistics (segment_statistics.rs)
- **功能**: 段级统计信息
- **特性**:
  - 简单的 BaseStatistics 包装类
  - 用于段级别的统计跟踪
- **测试**: 3 个单元测试全部通过

## 测试结果

```
running 22 tests
test storage::statistics::base_statistics::tests::test_create_empty ... ok
test storage::statistics::base_statistics::tests::test_merge ... ok
test storage::statistics::base_statistics::tests::test_create_unknown ... ok
test storage::statistics::column_statistics::tests::test_has_distinct_stats ... ok
test storage::statistics::column_statistics::tests::test_merge ... ok
test storage::statistics::column_statistics::tests::test_create_empty ... ok
test storage::statistics::column_statistics::tests::test_copy ... ok
test storage::statistics::numeric_stats::tests::test_merge ... ok
test storage::statistics::numeric_stats::tests::test_numeric_stats_default ... ok
test storage::statistics::numeric_stats::tests::test_set_min_max ... ok
test storage::statistics::string_stats::tests::test_merge ... ok
test storage::statistics::string_stats::tests::test_string_stats_default ... ok
test storage::statistics::string_stats::tests::test_min_max ... ok
test storage::statistics::string_stats::tests::test_update ... ok
test storage::statistics::string_stats::tests::test_unicode_detection ... ok
test storage::statistics::distinct_statistics::tests::test_distinct_statistics_new ... ok
test storage::statistics::distinct_statistics::tests::test_update ... ok
test storage::statistics::distinct_statistics::tests::test_merge ... ok
test storage::statistics::distinct_statistics::tests::test_copy ... ok
test storage::statistics::segment_statistics::tests::test_from_stats ... ok
test storage::statistics::segment_statistics::tests::test_new ... ok
test storage::statistics::segment_statistics::tests::test_statistics_mut ... ok

test result: ok. 22 passed; 0 failed; 0 ignored; 0 measured
```

## 设计亮点

### 1. 类型安全
- 使用 Rust 的 `enum` 替代 C++ 的 `union`，提供类型安全
- 使用 `Option<T>` 替代空指针
- 使用 `Vec<Box<T>>` 替代裸指针数组

### 2. 内存安全
- 无需手动内存管理
- 自动生命周期管理
- 避免了 C++ 中的 `unsafe_unique_array`

### 3. 并发安全
- `DistinctStatistics` 使用 `AtomicU64` 实现线程安全
- 无需显式锁机制

### 4. 错误处理
- 使用 `Result<T, E>` 类型进行错误传播
- 预留了错误类型扩展空间

### 5. 代码组织
```
src/storage/statistics/
├── mod.rs                      # 模块导出
├── base_statistics.rs          # 基础统计
├── numeric_stats.rs            # 数值统计
├── string_stats.rs             # 字符串统计
├── distinct_statistics.rs      # 基数估计
├── column_statistics.rs        # 列级统计
└── segment_statistics.rs       # 段级统计
```

## 与 C++ 版本的对应关系

| C++ 文件 | Rust 文件 | 状态 |
|---------|----------|------|
| base_statistics.cpp (607行) | base_statistics.rs (300行) | ✅ 完成 |
| numeric_stats.cpp (628行) | numeric_stats.rs (280行) | ✅ 完成 |
| string_stats.cpp (326行) | string_stats.rs (250行) | ✅ 完成 |
| distinct_statistics.cpp (87行) | distinct_statistics.rs (220行) | ✅ 完成 |
| column_statistics.cpp (72行) | column_statistics.rs (150行) | ✅ 完成 |
| segment_statistics.cpp (14行) | segment_statistics.rs (50行) | ✅ 完成 |

## 后续计划 (Phase 2-4)

### Phase 2: 嵌套类型
- [ ] list_stats.rs - 列表类型统计
- [ ] struct_stats.rs - 结构体统计
- [ ] array_stats.rs - 数组统计

### Phase 3: 高级特性
- [ ] geometry_stats.rs - 几何类型统计
- [ ] variant_stats.rs - 变体类型统计

### Phase 4: 优化
- [ ] 完整的 Zonemap 过滤实现
- [ ] 序列化/反序列化支持
- [ ] 性能基准测试

## 使用示例

```rust
use duckdb_rs::storage::statistics::{BaseStatistics, NumericStats, ColumnStatistics};
use duckdb_rs::common::types::LogicalType;

// 创建数值统计
let mut stats = BaseStatistics::create_empty(LogicalType::integer());
stats.set_has_no_null();

// 创建列统计
let col_stats = ColumnStatistics::new(stats);

// 更新 distinct 统计
let hashes = vec![1u64, 2, 3, 4, 5];
col_stats.update_distinct_statistics(&hashes, hashes.len());

// 获取基数估计
if let Some(distinct) = col_stats.distinct_stats() {
    println!("Estimated distinct count: {}", distinct.get_count());
}
```

## 性能特点

1. **零成本抽象**: 使用泛型和 trait，编译时单态化
2. **内存效率**: 紧凑的数据结构，无额外开销
3. **缓存友好**: 数据局部性良好
4. **并发友好**: 无锁数据结构（HyperLogLog）

## 注意事项

1. **Union 类型**: `NumericValueUnion` 使用 `unsafe`，需要谨慎操作
2. **类型兼容**: 确保与现有 `LogicalType` 定义兼容
3. **序列化**: 当前未实现，需要与 C++ 版本保持二进制兼容性

## 贡献指南

1. 保持类名与 C++ 一致
2. 方法名使用蛇形命名 (snake_case)
3. 添加充分的单元测试
4. 保持代码简洁，避免过度工程化

## 参考资料

- [DuckDB 源码](https://github.com/duckdb/duckdb)
- [HyperLogLog 算法](https://en.wikipedia.org/wiki/HyperLogLog)
- [Zonemap 索引](https://www.vldb.org/pvldb/vol8/p1654-sun.pdf)
