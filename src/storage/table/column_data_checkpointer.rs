//! 列数据压缩分析与 Checkpoint 执行器。
//!
//! 对应 C++: `duckdb/storage/table/column_data_checkpointer.hpp`
//!
//! # 职责
//!
//! `ColumnDataCheckpointer` 负责对一组列（通常是同一 RowGroup 的所有列）执行 checkpoint：
//! 1. 扫描所有列段，分析最优压缩算法（`DetectBestCompressionMethod`）。
//! 2. 将数据用选定的压缩算法写入磁盘（`WriteToDisk`）。
//! 3. 完成 checkpoint，释放旧段（`FinalizeCheckpoint`）。
//!
//! `ColumnDataCheckpointData` 是单列的上下文聚合（可选字段版本），
//! 供 checkpoint 函数使用。
//!
//! # C++ → Rust 映射
//!
//! | C++ | Rust |
//! |-----|------|
//! | `optional_ptr<ColumnCheckpointState>` | `Option<Arc<Mutex<ColumnCheckpointState>>>` |
//! | `vector<reference<ColumnCheckpointState>> &states` | `Vec<Arc<Mutex<ColumnCheckpointState>>>` |
//! | `Vector intermediate` | `Vec<u8>` 原始缓冲 |
//! | `vector<vector<optional_ptr<CompressionFunction>>>` | `Vec<Vec<&'static CompressionFunction>>` |

use parking_lot::Mutex;
use std::sync::Arc;

use super::column_checkpoint_state::ColumnCheckpointState;
use super::column_data::ScanVectorType;
use super::scan_state::ColumnScanState;
use super::types::{Idx, LogicalType, PhysicalType};
use crate::function::compression_config::get_compression_functions;
use crate::function::compression_function::{AnalyzeState, CompressionFunction, CompressionInfo};
use crate::storage::buffer::{DEFAULT_BLOCK_ALLOC_SIZE, DEFAULT_BLOCK_HEADER_SIZE};
use crate::common::types::{LogicalTypeId, Vector};

// ─── CheckpointAnalyzeResult ──────────────────────────────────────────────────

/// 单列的压缩分析结果（C++: `struct CheckpointAnalyzeResult`）。
///
/// 若为默认构造（`analyze_state = None`），表示该列无需 checkpoint。
pub struct CheckpointAnalyzeResult {
    /// 选定压缩算法的分析状态（C++: `unique_ptr<AnalyzeState> analyze_state`）。
    pub analyze_state: Option<Box<dyn AnalyzeState>>,
    /// 选定的压缩函数（C++: `optional_ptr<CompressionFunction> function`）。
    pub function: Option<&'static CompressionFunction>,
}

impl CheckpointAnalyzeResult {
    /// 默认构造（无需 checkpoint 的列）（C++: 默认构造函数）。
    pub fn none() -> Self {
        Self {
            analyze_state: None,
            function: None,
        }
    }

    /// 有压缩结果。
    pub fn new(
        analyze_state: Box<dyn AnalyzeState>,
        function: &'static CompressionFunction,
    ) -> Self {
        Self {
            analyze_state: Some(analyze_state),
            function: Some(function),
        }
    }
}

// ─── ColumnCheckpointInfo ──────────────────────────────────────────────────────

/// Checkpoint 全局配置（C++: `ColumnCheckpointInfo`）。
pub struct ColumnCheckpointInfo {
    /// 首选压缩类型（C++: `CompressionType compression`）。
    pub compression_type: super::types::CompressionType,
}

// ─── ColumnDataCheckpointData ─────────────────────────────────────────────────

/// 单列 checkpoint 上下文聚合（C++: `struct ColumnDataCheckpointData`）。
///
/// 所有字段均为可选，默认构造时表示"该列不需要 checkpoint"。
pub struct ColumnDataCheckpointData {
    /// 对应的 checkpoint 状态（C++: `optional_ptr<ColumnCheckpointState>`）。
    pub checkpoint_state: Option<Arc<Mutex<ColumnCheckpointState>>>,
    /// 原始列数据（C++: `optional_ptr<ColumnData> col_data`）。
    pub col_data: Option<Arc<super::column_data::ColumnData>>,
    /// RowGroup ID（C++: `optional_ptr<const RowGroup> row_group`，当前仅保留 ID）。
    pub row_group_id: Option<u64>,
}

impl ColumnDataCheckpointData {
    /// 默认构造（列不需要 checkpoint）。
    pub fn none() -> Self {
        Self {
            checkpoint_state: None,
            col_data: None,
            row_group_id: None,
        }
    }

    pub fn new(
        checkpoint_state: Arc<Mutex<ColumnCheckpointState>>,
        col_data: Arc<super::column_data::ColumnData>,
        row_group_id: u64,
    ) -> Self {
        Self {
            checkpoint_state: Some(checkpoint_state),
            col_data: Some(col_data),
            row_group_id: Some(row_group_id),
        }
    }

    /// 获取逻辑类型（C++: `GetType()`）。
    pub fn get_type(&self) -> super::types::LogicalType {
        self.col_data
            .as_ref()
            .map(|column| column.base.logical_type.clone())
            .expect("col_data not set")
    }

    /// 获取 checkpoint 状态（C++: `GetCheckpointState()`）。
    pub fn get_checkpoint_state(&self) -> Arc<Mutex<ColumnCheckpointState>> {
        self.checkpoint_state
            .clone()
            .expect("checkpoint_state not set")
    }
}

// ─── ColumnDataCheckpointer ───────────────────────────────────────────────────

/// 列数据压缩分析与写盘执行器（C++: `class ColumnDataCheckpointer`）。
///
/// 同时处理一个 RowGroup 的所有列，并行分析压缩算法，
/// 选出最优方案后批量写入。
pub struct ColumnDataCheckpointer {
    /// 各列的 checkpoint 状态（C++: `vector<reference<ColumnCheckpointState>> &states`）。
    checkpoint_states: Vec<Arc<Mutex<ColumnCheckpointState>>>,

    /// RowGroup ID（C++: `const RowGroup &row_group`）。
    row_group_id: u64,

    /// 全局 checkpoint 配置（C++: `ColumnCheckpointInfo &checkpoint_info`）。
    checkpoint_info: ColumnCheckpointInfo,

    /// 是否检测到任何变化（C++: `bool has_changes`）。
    has_changes: bool,

    /// 每列候选压缩函数列表（C++: `vector<vector<optional_ptr<CompressionFunction>>>`）。
    compression_functions: Vec<Vec<&'static CompressionFunction>>,

    /// 每列每个候选函数的分析状态（C++: `vector<vector<unique_ptr<AnalyzeState>>>`）。
    analyze_states: Vec<Vec<Option<Box<dyn AnalyzeState>>>>,
}

impl ColumnDataCheckpointer {
    /// 构造（C++: `ColumnDataCheckpointer(states, storage_manager, row_group, checkpoint_info)`）。
    pub fn new(
        checkpoint_states: Vec<Arc<Mutex<ColumnCheckpointState>>>,
        row_group_id: u64,
        checkpoint_info: ColumnCheckpointInfo,
    ) -> Self {
        let mut compression_functions = Vec::with_capacity(checkpoint_states.len());
        for checkpoint_state in &checkpoint_states {
            let state = checkpoint_state.lock();
            let physical_type = logical_type_to_physical(state.global_stats.get_type());
            compression_functions.push(get_compression_functions(physical_type).iter().collect());
        }
        Self {
            checkpoint_states,
            row_group_id,
            checkpoint_info,
            has_changes: false,
            compression_functions,
            analyze_states: Vec::new(),
        }
    }

    /// 执行 checkpoint（C++: `Checkpoint()`）。
    ///
    /// 流程：分析 → 写盘 → (由调用方) FinalizeCheckpoint。
    pub fn checkpoint(&mut self) {
        self.has_changes = self
            .checkpoint_states
            .iter()
            .any(|state| state.lock().get_original_column().has_any_changes());
        if self.has_changes {
            self.write_to_disk();
        }
    }

    /// 完成 checkpoint，释放旧段内存（C++: `FinalizeCheckpoint()`）。
    pub fn finalize_checkpoint(&mut self) {
        if self.has_changes {
            return;
        }
        for checkpoint_state in &self.checkpoint_states {
            let mut state = checkpoint_state.lock();
            self.write_persistent_segments(&mut state);
        }
    }

    // ── 私有辅助 ───────────────────────────────────────────────────────────────

    fn scan_segments<F>(
        checkpoint_states: &[Arc<Mutex<ColumnCheckpointState>>],
        mut callback: F,
    )
    where
        F: FnMut(&Vector, Idx),
    {
        let first_state = checkpoint_states
            .first()
            .expect("checkpoint_states must not be empty")
            .lock();
        let column = first_state.get_original_column();
        drop(first_state);

        let mut scan_state = ColumnScanState::new();
        column.initialize_scan(&mut scan_state);

        let vector_type = create_intermediate_type(&column.base.logical_type);
        let mut scan_vector = Vector::with_capacity(vector_type, crate::storage::table::types::STANDARD_VECTOR_SIZE as usize);

        let mut scanned = 0;
        while scanned < column.count() {
            let count = (column.count() - scanned).min(crate::storage::table::types::STANDARD_VECTOR_SIZE);
            scan_vector = Vector::with_capacity(scan_vector.get_type().clone(), crate::storage::table::types::STANDARD_VECTOR_SIZE as usize);
            let scan_type = column.base.get_vector_scan_type(&scan_state, count);
            column.base.scan_vector(
                &mut scan_state,
                &mut scan_vector,
                count,
                scan_type,
                0,
            );
            callback(&scan_vector, count);
            scanned += count;
        }
    }

    fn detect_best_compression_method(&mut self) -> Vec<CheckpointAnalyzeResult> {
        let forced_methods = self.force_compression();
        self.init_analyze();

        let compression_functions = &self.compression_functions;
        let analyze_states = &mut self.analyze_states;
        Self::scan_segments(&self.checkpoint_states, |scan_vector, count| {
            for (functions, states) in compression_functions.iter().zip(analyze_states.iter_mut()) {
                for (idx, function) in functions.iter().enumerate() {
                    let Some(state) = states[idx].as_mut() else {
                        continue;
                    };
                    let analyze = function.analyze.expect("compression analyze hook missing");
                    if !analyze(state.as_mut(), scan_vector, count) {
                        states[idx] = None;
                    }
                }
            }
        });

        let mut result = Vec::with_capacity(self.checkpoint_states.len());
        for ((functions, states), forced_method) in self
            .compression_functions
            .iter()
            .zip(self.analyze_states.iter_mut())
            .zip(forced_methods.iter())
        {
            let mut best_score = Idx::MAX;
            let mut best_idx = None;
            for (idx, (function, state)) in functions.iter().zip(states.iter_mut()).enumerate() {
                let Some(state) = state.as_mut() else {
                    continue;
                };
                let final_analyze = function
                    .final_analyze
                    .expect("compression final_analyze hook missing");
                let score = final_analyze(state.as_mut());
                if score == Idx::MAX {
                    continue;
                }
                let forced_match = function.type_ == *forced_method;
                if score < best_score || forced_match {
                    best_score = score;
                    best_idx = Some(idx);
                }
                if forced_match {
                    break;
                }
            }

            let Some(best_idx) = best_idx else {
                panic!("no suitable compression/storage method found");
            };
            let function = functions[best_idx];
            let analyze_state = states[best_idx]
                .take()
                .expect("chosen compression missing analyze state");
            result.push(CheckpointAnalyzeResult::new(analyze_state, function));
        }
        result
    }

    fn write_to_disk(&mut self) {
        let analyze_result = self.detect_best_compression_method();
        let compression_info = CompressionInfo::new(
            (DEFAULT_BLOCK_ALLOC_SIZE - DEFAULT_BLOCK_HEADER_SIZE) as Idx,
            DEFAULT_BLOCK_HEADER_SIZE as Idx,
        );
        let mut compression_states = Vec::with_capacity(analyze_result.len());
        for (idx, result) in analyze_result.into_iter().enumerate() {
            let function = result.function.expect("chosen compression missing function");
            let analyze_state = result
                .analyze_state
                .expect("chosen compression missing analyze state");
            let checkpoint_state = self.checkpoint_states[idx].clone();
            let init = function
                .init_compression
                .expect("compression init_compression hook missing");
            compression_states.push((function, init(&compression_info, checkpoint_state, analyze_state)));
        }

        Self::scan_segments(&self.checkpoint_states, |scan_vector, count| {
            for (function, state) in &mut compression_states {
                let compress = function
                    .compress
                    .expect("compression compress hook missing");
                compress(state.as_mut(), scan_vector, count);
            }
        });

        for (function, state) in &mut compression_states {
            let finalize = function
                .compress_finalize
                .expect("compression compress_finalize hook missing");
            finalize(state.as_mut());
        }
    }

    fn has_changes_in_column(&self, col_data_id: u64) -> bool {
        self.checkpoint_states.iter().any(|state| {
            let state = state.lock();
            state
                .original_column
                .as_ref()
                .map(|column| Arc::as_ptr(column) as usize as u64 == col_data_id && column.has_any_changes())
                .unwrap_or(false)
        })
    }

    fn init_analyze(&mut self) {
        let compression_info = CompressionInfo::new(
            (DEFAULT_BLOCK_ALLOC_SIZE - DEFAULT_BLOCK_HEADER_SIZE) as Idx,
            DEFAULT_BLOCK_HEADER_SIZE as Idx,
        );
        self.analyze_states.clear();
        self.analyze_states.reserve(self.compression_functions.len());
        for functions in &self.compression_functions {
            let mut states = Vec::with_capacity(functions.len());
            for function in functions {
                let state = function.init_analyze.map(|init| init(&compression_info));
                states.push(state);
            }
            self.analyze_states.push(states);
        }
    }

    fn force_compression(&self) -> Vec<super::types::CompressionType> {
        let mut forced_methods = vec![super::types::CompressionType::Auto; self.checkpoint_states.len()];
        if self.checkpoint_info.compression_type == super::types::CompressionType::Auto {
            return forced_methods;
        }
        for (column_idx, functions) in self.compression_functions.iter().enumerate() {
            if functions
                .iter()
                .any(|function| function.type_ == self.checkpoint_info.compression_type)
            {
                forced_methods[column_idx] = self.checkpoint_info.compression_type;
            }
        }
        forced_methods
    }

    fn write_persistent_segments(&self, state: &mut ColumnCheckpointState) {
        let column = state.get_original_column();
        state.data_pointers.clear();
        for pointer in column.base.get_data_pointers() {
            state.data_pointers.push(pointer);
        }
    }
}

fn logical_type_to_physical(logical_type: &LogicalType) -> PhysicalType {
    match logical_type.id {
        LogicalTypeId::Boolean => PhysicalType::Bool,
        LogicalTypeId::Validity => PhysicalType::Bit,
        LogicalTypeId::TinyInt => PhysicalType::Int8,
        LogicalTypeId::SmallInt => PhysicalType::Int16,
        LogicalTypeId::Integer | LogicalTypeId::Date => PhysicalType::Int32,
        LogicalTypeId::BigInt | LogicalTypeId::Time | LogicalTypeId::Timestamp => PhysicalType::Int64,
        LogicalTypeId::HugeInt => PhysicalType::Int128,
        LogicalTypeId::Float => PhysicalType::Float,
        LogicalTypeId::Double => PhysicalType::Double,
        LogicalTypeId::Varchar => PhysicalType::VarChar,
        LogicalTypeId::List => PhysicalType::List,
        LogicalTypeId::Struct => PhysicalType::Struct,
        LogicalTypeId::Array => PhysicalType::Array,
        LogicalTypeId::Decimal => {
            if logical_type.width <= 4 {
                PhysicalType::Int16
            } else if logical_type.width <= 9 {
                PhysicalType::Int32
            } else if logical_type.width <= 18 {
                PhysicalType::Int64
            } else {
                PhysicalType::Int128
            }
        }
        _ => PhysicalType::Invalid,
    }
}

fn create_intermediate_type(logical_type: &LogicalType) -> LogicalType {
    if logical_type.id == LogicalTypeId::Validity {
        return LogicalType::boolean();
    }
    if logical_type_to_physical(logical_type) == PhysicalType::List {
        return LogicalType::ubigint();
    }
    logical_type.clone()
}
