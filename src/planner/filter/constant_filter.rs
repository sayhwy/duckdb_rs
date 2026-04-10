use crate::catalog::Value;
use crate::common::enums::{expression_type_to_operator, ExpressionType};
use crate::planner::{compare_value, TableFilter, TableFilterType};
use crate::storage::statistics::{BaseStatistics, FilterPropagateResult, NumericStats, StringStats};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ConstantFilter {
    pub comparison_type: ExpressionType,
    pub constant: Value,
}

impl ConstantFilter {
    pub const TYPE: TableFilterType = TableFilterType::ConstantComparison;

    pub fn new(comparison_type: ExpressionType, constant: Value) -> Self {
        if constant.is_null() {
            panic!("ConstantFilter constant cannot be NULL - use IsNullFilter instead");
        }
        Self {
            comparison_type,
            constant,
        }
    }

    pub fn compare(&self, value: &Value) -> bool {
        compare_value(value, self.comparison_type, &self.constant)
    }
}

impl TableFilter for ConstantFilter {
    fn filter_type(&self) -> TableFilterType {
        Self::TYPE
    }

    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult {
        if !stats.can_have_no_null() {
            return FilterPropagateResult::FilterAlwaysFalse;
        }
        let result = match &self.constant {
            Value::Boolean(_) | Value::Integer(_) | Value::Float(_) => {
                NumericStats::check_zonemap_stats(stats, self.comparison_type, std::slice::from_ref(&self.constant))
            }
            Value::Text(_) => {
                StringStats::check_zonemap_stats(stats, self.comparison_type, std::slice::from_ref(&self.constant))
            }
            Value::Null | Value::Blob(_) => FilterPropagateResult::NoPruningPossible,
        };
        if result == FilterPropagateResult::FilterAlwaysTrue && stats.can_have_null() {
            return FilterPropagateResult::NoPruningPossible;
        }
        result
    }

    fn to_string(&self, column_name: &str) -> String {
        format!(
            "{}{}{}",
            column_name,
            expression_type_to_operator(self.comparison_type),
            self.constant.to_sql_string()
        )
    }

    fn copy(&self) -> Arc<dyn TableFilter> {
        Arc::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &dyn TableFilter) -> bool {
        let Some(other) = other.as_any().downcast_ref::<ConstantFilter>() else {
            return false;
        };
        self.comparison_type == other.comparison_type && self.constant == other.constant
    }

    fn to_expression_string(&self, column_name: &str) -> String {
        self.to_string(column_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::LogicalType;
    use crate::storage::statistics::{BaseStatistics, NumericStats, StringStats};

    #[test]
    fn constant_filter_numeric_statistics() {
        let mut stats = BaseStatistics::new(LogicalType::integer());
        stats.set_has_no_null();
        let logical_type = stats.get_type().clone();
        let data = stats.get_stats_data_mut();
        let crate::storage::statistics::StatsData::Numeric(data) = data else {
            panic!("expected numeric stats");
        };
        NumericStats::set_min(data, 10i32, &logical_type);
        NumericStats::set_max(data, 20i32, &logical_type);

        let filter = ConstantFilter::new(ExpressionType::CompareEqual, Value::Integer(30));
        assert_eq!(
            filter.check_statistics(&mut stats),
            FilterPropagateResult::FilterAlwaysFalse
        );
    }

    #[test]
    fn constant_filter_string_statistics() {
        let mut stats = BaseStatistics::new(LogicalType::varchar());
        stats.set_has_no_null();
        let data = stats.get_stats_data_mut();
        let crate::storage::statistics::StatsData::String(data) = data else {
            panic!("expected string stats");
        };
        StringStats::set_min(data, "apple");
        StringStats::set_max(data, "banana");
        StringStats::set_max_string_length(data, 6);

        let filter = ConstantFilter::new(ExpressionType::CompareEqual, Value::Text("carrot".to_string()));
        assert_eq!(
            filter.check_statistics(&mut stats),
            FilterPropagateResult::FilterAlwaysFalse
        );
    }
}
