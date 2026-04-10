use crate::planner::{TableFilter, TableFilterType};
use crate::storage::statistics::{BaseStatistics, FilterPropagateResult};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StructFilter {
    pub child_idx: usize,
    pub child_name: String,
    pub child_filter: Arc<dyn TableFilter>,
}

impl StructFilter {
    pub const TYPE: TableFilterType = TableFilterType::StructExtract;

    pub fn new(child_idx: usize, child_name: String, child_filter: Arc<dyn TableFilter>) -> Self {
        Self {
            child_idx,
            child_name,
            child_filter,
        }
    }
}

impl TableFilter for StructFilter {
    fn filter_type(&self) -> TableFilterType {
        Self::TYPE
    }

    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult {
        let Some(child_stats) = stats.child_stats.get_mut(self.child_idx) else {
            return FilterPropagateResult::NoPruningPossible;
        };
        self.child_filter.check_statistics(child_stats)
    }

    fn to_string(&self, column_name: &str) -> String {
        if !self.child_name.is_empty() {
            return self
                .child_filter
                .to_string(&format!("{column_name}.{}", self.child_name));
        }
        self.child_filter
            .to_string(&format!("struct_extract_at({column_name},{})", self.child_idx + 1))
    }

    fn copy(&self) -> Arc<dyn TableFilter> {
        Arc::new(Self {
            child_idx: self.child_idx,
            child_name: self.child_name.clone(),
            child_filter: self.child_filter.copy(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &dyn TableFilter) -> bool {
        let Some(other) = other.as_any().downcast_ref::<StructFilter>() else {
            return false;
        };
        self.child_idx == other.child_idx && self.child_filter.equals(other.child_filter.as_ref())
    }

    fn to_expression_string(&self, column_name: &str) -> String {
        self.to_string(column_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Value;
    use crate::common::enums::ExpressionType;
    use crate::common::types::LogicalType;
    use crate::planner::{ConstantFilter, TableFilterState};
    use crate::storage::statistics::{BaseStatistics, NumericStats};
    use std::sync::Arc;

    #[test]
    fn struct_filter_statistics() {
        let mut stats = BaseStatistics::new(LogicalType::struct_type(vec![(
            "a".to_string(),
            LogicalType::integer(),
        )]));
        let mut child = BaseStatistics::new(LogicalType::integer());
        child.set_has_no_null();
        let logical_type = child.get_type().clone();
        let data = child.get_stats_data_mut();
        let crate::storage::statistics::StatsData::Numeric(data) = data else {
            panic!("expected numeric stats");
        };
        NumericStats::set_min(data, 10i32, &logical_type);
        NumericStats::set_max(data, 20i32, &logical_type);
        stats.child_stats.push(Box::new(child));

        let filter = StructFilter::new(
            0,
            "a".to_string(),
            Arc::new(ConstantFilter::new(ExpressionType::CompareEqual, Value::Integer(30))),
        );
        assert_eq!(
            filter.check_statistics(&mut stats),
            FilterPropagateResult::FilterAlwaysFalse
        );
    }

    #[test]
    fn struct_filter_state_initialization_uses_child_filter() {
        let context = crate::db::conn::ClientContext::new(
            crate::db::conn::DatabaseInstance::open(":memory:").expect("open in-memory db"),
        );
        let filter = StructFilter::new(
            0,
            "a".to_string(),
            Arc::new(ConstantFilter::new(ExpressionType::CompareEqual, Value::Integer(30))),
        );
        let state = <dyn TableFilterState>::initialize(&context, &filter);
        assert!(state.as_any().is::<crate::planner::table_filter_state::BaseTableFilterState>());
    }
}
