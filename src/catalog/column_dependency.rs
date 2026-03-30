//! 列依赖关系管理器（用于生成列）。
//!
//! 对应 C++: `duckdb/catalog/catalog_entry/column_dependency_manager.hpp`
//!
//! # 设计说明
//!
//! 管理表内生成列对普通列的依赖关系，用于：
//! - 确定生成列的绑定（求值）顺序
//! - 在删除普通列时检查是否有生成列依赖
//!
//! | C++ | Rust |
//! |-----|------|
//! | `logical_index_map_t<logical_index_set_t> dependencies_map` | `HashMap<usize, HashSet<usize>>` |
//! | `logical_index_map_t<logical_index_set_t> dependents_map` | `HashMap<usize, HashSet<usize>>` |
//! | `logical_index_map_t<logical_index_set_t> direct_dependencies` | `HashMap<usize, HashSet<usize>>` |
//! | `logical_index_set_t deleted_columns` | `HashSet<usize>` |

use super::error::CatalogError;
use super::types::ColumnList;
use std::collections::{HashMap, HashSet, VecDeque};

// ─── ColumnDependencyManager ──────────────────────────────────────────────────

/// 表内列依赖关系管理（C++: `ColumnDependencyManager`）。
///
/// 用于追踪生成列（generated columns）对普通列的依赖关系。
#[derive(Debug, Clone, Default)]
pub struct ColumnDependencyManager {
    /// 列的传递依赖（列 → 它所依赖的所有列）（C++: `dependencies_map`）。
    dependencies_map: HashMap<usize, HashSet<usize>>,
    /// 列的传递被依赖关系（列 → 依赖它的所有列）（C++: `dependents_map`）。
    dependents_map: HashMap<usize, HashSet<usize>>,
    /// 列的直接依赖（列 → 它直接依赖的列）（C++: `direct_dependencies`）。
    direct_dependencies: HashMap<usize, HashSet<usize>>,
    /// 已被删除的列索引（用于列删除操作中的调整）（C++: `deleted_columns`）。
    deleted_columns: HashSet<usize>,
}

impl ColumnDependencyManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// 添加生成列及其依赖（C++: `AddGeneratedColumn(LogicalIndex, vector<LogicalIndex>, bool)`）。
    ///
    /// - `col_idx`：生成列的逻辑索引。
    /// - `deps`：该生成列直接依赖的列索引列表。
    /// - `root`：是否为顶层调用（用于递归展开传递依赖时区分）。
    pub fn add_generated_column(&mut self, col_idx: usize, deps: Vec<usize>, root: bool) {
        // 记录直接依赖
        self.direct_dependencies
            .entry(col_idx)
            .or_default()
            .extend(deps.iter().copied());

        // 更新传递依赖和被依赖
        for &dep in &deps {
            // col_idx 依赖 dep
            self.dependencies_map
                .entry(col_idx)
                .or_default()
                .insert(dep);
            // dep 被 col_idx 依赖
            self.dependents_map.entry(dep).or_default().insert(col_idx);

            // 传递：如果 dep 也依赖其他列，col_idx 也间接依赖那些列
            let transitive: Vec<usize> = self
                .dependencies_map
                .get(&dep)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
            for t_dep in transitive {
                self.dependencies_map
                    .entry(col_idx)
                    .or_default()
                    .insert(t_dep);
                self.dependents_map
                    .entry(t_dep)
                    .or_default()
                    .insert(col_idx);
            }
        }
    }

    /// 从列定义列表添加所有生成列（C++: `AddGeneratedColumn(ColumnDefinition&, ColumnList&)`）。
    pub fn add_all_generated_columns(&mut self, columns: &ColumnList) {
        for (idx, col) in columns.columns.iter().enumerate() {
            if col.is_generated() {
                // 简化：从表达式中解析依赖列名（实际实现需要表达式解析器）
                // 此处通过扫描表达式字符串来近似识别列名
                if let Some(expr) = &col.generated_expression {
                    let mut deps = Vec::new();
                    for (dep_idx, dep_col) in columns.columns.iter().enumerate() {
                        if dep_idx != idx && expr.contains(&dep_col.name) {
                            deps.push(dep_idx);
                        }
                    }
                    self.add_generated_column(idx, deps, true);
                }
            }
        }
    }

    /// 获取生成列的绑定顺序（拓扑排序）（C++: `GetBindOrder`）。
    ///
    /// 返回列索引的栈，顶部优先绑定（依赖在前，被依赖在后）。
    pub fn get_bind_order(&self, columns: &ColumnList) -> VecDeque<usize> {
        let n = columns.columns.len();
        let mut in_degree: HashMap<usize, usize> = HashMap::new();
        let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();

        // 建图：gen_col → deps (gen_col 依赖 deps，所以 deps 先处理)
        for (&gen_col, deps) in &self.direct_dependencies {
            for &dep in deps {
                adj.entry(dep).or_default().push(gen_col);
                *in_degree.entry(gen_col).or_insert(0) += 1;
            }
        }

        // 拓扑排序（Kahn 算法）
        let mut queue: VecDeque<usize> = (0..n)
            .filter(|i| *in_degree.get(i).unwrap_or(&0) == 0)
            .collect();
        let mut order = VecDeque::new();

        while let Some(node) = queue.pop_front() {
            order.push_back(node);
            if let Some(neighbors) = adj.get(&node) {
                for &next in neighbors {
                    let deg = in_degree.entry(next).or_insert(1);
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(next);
                    }
                }
            }
        }

        order
    }

    /// 删除列时更新依赖关系（C++: `RemoveColumn`）。
    ///
    /// 返回受影响的生成列索引列表（需要一并删除或重新生成）。
    pub fn remove_column(&mut self, col_idx: usize, total_columns: usize) -> Vec<usize> {
        // 找出依赖被删除列的所有生成列
        let dependents = self.dependents_map.remove(&col_idx).unwrap_or_default();
        let affected: Vec<usize> = dependents.into_iter().collect();

        // 从所有依赖映射中移除对该列的引用
        for deps in self.dependencies_map.values_mut() {
            deps.remove(&col_idx);
        }
        for deps in self.direct_dependencies.values_mut() {
            deps.remove(&col_idx);
        }
        self.dependencies_map.remove(&col_idx);
        self.direct_dependencies.remove(&col_idx);
        self.deleted_columns.insert(col_idx);

        affected
    }

    /// 判断列 a 是否依赖列 b（C++: `IsDependencyOf`）。
    pub fn is_dependency_of(&self, a: usize, b: usize) -> bool {
        self.dependencies_map
            .get(&a)
            .map(|deps| deps.contains(&b))
            .unwrap_or(false)
    }

    /// 判断列是否有依赖（C++: `HasDependencies`）。
    pub fn has_dependencies(&self, col_idx: usize) -> bool {
        self.dependencies_map
            .get(&col_idx)
            .map(|deps| !deps.is_empty())
            .unwrap_or(false)
    }

    /// 获取列的所有依赖（C++: `GetDependencies`）。
    pub fn get_dependencies(&self, col_idx: usize) -> &HashSet<usize> {
        static EMPTY: std::sync::OnceLock<HashSet<usize>> = std::sync::OnceLock::new();
        self.dependencies_map
            .get(&col_idx)
            .unwrap_or_else(|| EMPTY.get_or_init(HashSet::new))
    }

    /// 判断列是否有被依赖关系（C++: `HasDependents`）。
    pub fn has_dependents(&self, col_idx: usize) -> bool {
        self.dependents_map
            .get(&col_idx)
            .map(|deps| !deps.is_empty())
            .unwrap_or(false)
    }

    /// 获取所有依赖该列的列（C++: `GetDependents`）。
    pub fn get_dependents(&self, col_idx: usize) -> &HashSet<usize> {
        static EMPTY: std::sync::OnceLock<HashSet<usize>> = std::sync::OnceLock::new();
        self.dependents_map
            .get(&col_idx)
            .unwrap_or_else(|| EMPTY.get_or_init(HashSet::new))
    }

    /// 验证不存在循环依赖。
    pub fn verify_no_cycles(&self) -> Result<(), CatalogError> {
        // 检测图中是否存在环（DFS 着色）
        let mut color: HashMap<usize, u8> = HashMap::new();
        let all_nodes: Vec<usize> = self
            .direct_dependencies
            .keys()
            .chain(self.direct_dependencies.values().flatten())
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        fn dfs(
            node: usize,
            adj: &HashMap<usize, HashSet<usize>>,
            color: &mut HashMap<usize, u8>,
        ) -> bool {
            color.insert(node, 1); // 灰色（正在处理）
            if let Some(deps) = adj.get(&node) {
                for &next in deps {
                    match color.get(&next) {
                        Some(&1) => return true, // 找到环
                        Some(&2) => {}           // 已处理
                        _ => {
                            if dfs(next, adj, color) {
                                return true;
                            }
                        }
                    }
                }
            }
            color.insert(node, 2); // 黑色（已完成）
            false
        }

        for &node in &all_nodes {
            if !color.contains_key(&node) {
                if dfs(node, &self.direct_dependencies, &mut color) {
                    return Err(CatalogError::invalid(
                        "Circular dependency detected among generated columns",
                    ));
                }
            }
        }
        Ok(())
    }
}
