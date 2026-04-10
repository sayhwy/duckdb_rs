
use crate::common::types::LogicalType;
use crate::storage::data_table::{StorageIndex, TableFilterSet};

// ─── Error Type ───────────────────────────────────────────────────────────────

/// 引擎/连接操作的错误类型。
pub type EngineError = crate::common::errors::Error;

// ─── Schema Types ─────────────────────────────────────────────────────────────

/// Schema 下单张表的信息。
pub struct SchemaTableInfo {
    /// 表名。
    pub name: String,
    /// 列定义列表：`(列名, 逻辑类型)`。
    pub columns: Vec<(String, LogicalType)>,
}

/// Schema 信息（命名空间 + 其下所有表的定义）。
pub struct SchemaInfo {
    /// Schema 名称（如 `"main"`）。
    pub name: String,
    /// 该 Schema 下的所有表。
    pub tables: Vec<SchemaTableInfo>,
}

/// 存储层可识别的表扫描请求。
///
/// 对齐 DuckDB 的 `column_ids + table_filters` 入口，
/// 不直接承载执行器表达式树。
#[derive(Clone, Default)]
pub struct TableScanRequest {
    pub column_ids: Vec<StorageIndex>,
    pub filters: Option<TableFilterSet>,
}

impl TableScanRequest {
    pub fn new(column_ids: Vec<StorageIndex>) -> Self {
        Self {
            column_ids,
            filters: None,
        }
    }
}

/// Table scan 绑定数据。
///
/// 对齐 DuckDB 的 `TableScanBindData`：
/// 保存绑定阶段解析好的列列表和输出类型。
pub struct TableScanBindData {
    pub request: TableScanRequest,
    pub result_types: Vec<LogicalType>,
}
