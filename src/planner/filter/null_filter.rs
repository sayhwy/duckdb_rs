use crate::planner::{TableFilter, TableFilterType};
use crate::storage::statistics::{BaseStatistics, FilterPropagateResult};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct IsNullFilter;

impl IsNullFilter {
    pub const TYPE: TableFilterType = TableFilterType::IsNull;

    pub fn new() -> Self {
        Self
    }
}

impl TableFilter for IsNullFilter {
    fn filter_type(&self) -> TableFilterType {
        Self::TYPE
    }

    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult {
        if !stats.can_have_null() {
            return FilterPropagateResult::FilterAlwaysFalse;
        }
        if !stats.can_have_no_null() {
            return FilterPropagateResult::FilterAlwaysTrue;
        }
        FilterPropagateResult::NoPruningPossible
    }

    fn to_string(&self, column_name: &str) -> String {
        format!("{column_name} IS NULL")
    }

    fn copy(&self) -> Arc<dyn TableFilter> {
        Arc::new(Self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_expression_string(&self, column_name: &str) -> String {
        self.to_string(column_name)
    }
}

#[derive(Debug, Clone, Default)]
pub struct IsNotNullFilter;

impl IsNotNullFilter {
    pub const TYPE: TableFilterType = TableFilterType::IsNotNull;

    pub fn new() -> Self {
        Self
    }
}

impl TableFilter for IsNotNullFilter {
    fn filter_type(&self) -> TableFilterType {
        Self::TYPE
    }

    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult {
        if !stats.can_have_no_null() {
            return FilterPropagateResult::FilterAlwaysFalse;
        }
        if !stats.can_have_null() {
            return FilterPropagateResult::FilterAlwaysTrue;
        }
        FilterPropagateResult::NoPruningPossible
    }

    fn to_string(&self, column_name: &str) -> String {
        format!("{column_name} IS NOT NULL")
    }

    fn copy(&self) -> Arc<dyn TableFilter> {
        Arc::new(Self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_expression_string(&self, column_name: &str) -> String {
        self.to_string(column_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::LogicalType;
    use crate::storage::statistics::BaseStatistics;

    #[test]
    fn is_null_filter_statistics() {
        let mut stats = BaseStatistics::new(LogicalType::integer());
        stats.set_has_null();

        assert_eq!(
            IsNullFilter::new().check_statistics(&mut stats),
            FilterPropagateResult::FilterAlwaysTrue
        );
    }

    #[test]
    fn is_not_null_filter_statistics() {
        let mut stats = BaseStatistics::new(LogicalType::integer());
        stats.set_has_no_null();

        assert_eq!(
            IsNotNullFilter::new().check_statistics(&mut stats),
            FilterPropagateResult::FilterAlwaysTrue
        );
    }
}
