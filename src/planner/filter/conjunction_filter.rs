use crate::planner::{TableFilter, TableFilterState, TableFilterType};
use crate::storage::statistics::{BaseStatistics, FilterPropagateResult};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ConjunctionFilter {
    pub filter_type: TableFilterType,
    pub child_filters: Vec<Arc<dyn TableFilter>>,
}

impl ConjunctionFilter {
    pub fn new(filter_type: TableFilterType) -> Self {
        Self {
            filter_type,
            child_filters: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConjunctionOrFilter {
    pub child_filters: Vec<Arc<dyn TableFilter>>,
}

impl ConjunctionOrFilter {
    pub const TYPE: TableFilterType = TableFilterType::ConjunctionOr;

    pub fn new() -> Self {
        Self {
            child_filters: Vec::new(),
        }
    }
}

impl Default for ConjunctionOrFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl TableFilter for ConjunctionOrFilter {
    fn filter_type(&self) -> TableFilterType {
        Self::TYPE
    }

    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult {
        assert!(!self.child_filters.is_empty());
        for filter in &self.child_filters {
            let prune_result = filter.check_statistics(stats);
            if prune_result == FilterPropagateResult::NoPruningPossible {
                return FilterPropagateResult::NoPruningPossible;
            }
            if prune_result == FilterPropagateResult::FilterAlwaysTrue {
                return FilterPropagateResult::FilterAlwaysTrue;
            }
        }
        FilterPropagateResult::FilterAlwaysFalse
    }

    fn to_string(&self, column_name: &str) -> String {
        self.child_filters
            .iter()
            .map(|filter| filter.to_string(column_name))
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    fn copy(&self) -> Arc<dyn TableFilter> {
        Arc::new(Self {
            child_filters: self.child_filters.iter().map(|filter| filter.copy()).collect(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &dyn TableFilter) -> bool {
        let Some(other) = other.as_any().downcast_ref::<ConjunctionOrFilter>() else {
            return false;
        };
        if self.child_filters.len() != other.child_filters.len() {
            return false;
        }
        for (left, right) in self.child_filters.iter().zip(&other.child_filters) {
            if !left.equals(right.as_ref()) {
                return false;
            }
        }
        true
    }

    fn to_expression_string(&self, column_name: &str) -> String {
        self.to_string(column_name)
    }
}

#[derive(Debug, Clone)]
pub struct ConjunctionAndFilter {
    pub child_filters: Vec<Arc<dyn TableFilter>>,
}

impl ConjunctionAndFilter {
    pub const TYPE: TableFilterType = TableFilterType::ConjunctionAnd;

    pub fn new() -> Self {
        Self {
            child_filters: Vec::new(),
        }
    }
}

impl Default for ConjunctionAndFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl TableFilter for ConjunctionAndFilter {
    fn filter_type(&self) -> TableFilterType {
        Self::TYPE
    }

    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult {
        assert!(!self.child_filters.is_empty());
        let mut result = FilterPropagateResult::FilterAlwaysTrue;
        for filter in &self.child_filters {
            let prune_result = filter.check_statistics(stats);
            if prune_result == FilterPropagateResult::FilterAlwaysFalse {
                return FilterPropagateResult::FilterAlwaysFalse;
            }
            if prune_result != result {
                result = FilterPropagateResult::NoPruningPossible;
            }
        }
        result
    }

    fn to_string(&self, column_name: &str) -> String {
        self.child_filters
            .iter()
            .map(|filter| filter.to_string(column_name))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn copy(&self) -> Arc<dyn TableFilter> {
        Arc::new(Self {
            child_filters: self.child_filters.iter().map(|filter| filter.copy()).collect(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &dyn TableFilter) -> bool {
        let Some(other) = other.as_any().downcast_ref::<ConjunctionAndFilter>() else {
            return false;
        };
        if self.child_filters.len() != other.child_filters.len() {
            return false;
        }
        for (left, right) in self.child_filters.iter().zip(&other.child_filters) {
            if !left.equals(right.as_ref()) {
                return false;
            }
        }
        true
    }

    fn to_expression_string(&self, column_name: &str) -> String {
        self.to_string(column_name)
    }
}

#[derive(Debug, Default)]
pub struct ConjunctionOrFilterState {
    pub child_states: Vec<Arc<dyn TableFilterState>>,
}

impl TableFilterState for ConjunctionOrFilterState {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct ConjunctionAndFilterState {
    pub child_states: Vec<Arc<dyn TableFilterState>>,
}

impl TableFilterState for ConjunctionAndFilterState {
    fn as_any(&self) -> &dyn Any {
        self
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
    fn conjunction_and_filter_statistics() {
        let mut stats = BaseStatistics::new(LogicalType::integer());
        stats.set_has_no_null();
        let logical_type = stats.get_type().clone();
        let data = stats.get_stats_data_mut();
        let crate::storage::statistics::StatsData::Numeric(data) = data else {
            panic!("expected numeric stats");
        };
        NumericStats::set_min(data, 10i32, &logical_type);
        NumericStats::set_max(data, 20i32, &logical_type);

        let mut filter = ConjunctionAndFilter::new();
        filter
            .child_filters
            .push(Arc::new(ConstantFilter::new(ExpressionType::CompareGreaterThan, Value::Integer(5))));
        filter
            .child_filters
            .push(Arc::new(ConstantFilter::new(ExpressionType::CompareLessThan, Value::Integer(15))));

        assert_eq!(
            filter.check_statistics(&mut stats),
            FilterPropagateResult::NoPruningPossible
        );
    }

    #[test]
    fn conjunction_or_filter_state_initialization() {
        let context = crate::db::conn::ClientContext::new(
            crate::db::conn::DatabaseInstance::open(":memory:").expect("open in-memory db"),
        );
        let mut filter = ConjunctionOrFilter::new();
        filter
            .child_filters
            .push(Arc::new(ConstantFilter::new(ExpressionType::CompareEqual, Value::Integer(1))));
        filter.child_filters.push(Arc::new(crate::planner::IsNullFilter::new()));

        let state = <dyn TableFilterState>::initialize(&context, &filter);
        let state = state
            .as_any()
            .downcast_ref::<ConjunctionOrFilterState>()
            .expect("expected conjunction or state");
        assert_eq!(state.child_states.len(), 2);
    }
}
