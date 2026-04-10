use crate::catalog::Value;
use crate::common::enums::ExpressionType;
use crate::planner::{TableFilter, TableFilterType};
use crate::storage::statistics::{BaseStatistics, FilterPropagateResult, NumericStats, StringStats};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct InFilter {
    pub values: Vec<Value>,
}

impl InFilter {
    pub const TYPE: TableFilterType = TableFilterType::InFilter;

    pub fn new(values: Vec<Value>) -> Self {
        if values.is_empty() {
            panic!("InFilter constants cannot be empty");
        }
        for value in &values {
            if value.is_null() {
                panic!("InFilter constant cannot be NULL - use IsNullFilter instead");
            }
        }
        let first_tag = std::mem::discriminant(&values[0]);
        for value in values.iter().skip(1) {
            if std::mem::discriminant(value) != first_tag {
                panic!("InFilter constants must all have the same type");
            }
        }
        Self { values }
    }
}

impl TableFilter for InFilter {
    fn filter_type(&self) -> TableFilterType {
        Self::TYPE
    }

    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult {
        if !stats.can_have_no_null() {
            return FilterPropagateResult::FilterAlwaysFalse;
        }
        match &self.values[0] {
            Value::Boolean(_) | Value::Integer(_) | Value::Float(_) => {
                NumericStats::check_zonemap_stats(stats, ExpressionType::CompareEqual, &self.values)
            }
            Value::Text(_) => StringStats::check_zonemap_stats(stats, ExpressionType::CompareEqual, &self.values),
            Value::Null | Value::Blob(_) => FilterPropagateResult::NoPruningPossible,
        }
    }

    fn to_string(&self, column_name: &str) -> String {
        let in_list = self
            .values
            .iter()
            .map(Value::to_sql_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!("{column_name} IN ({in_list})")
    }

    fn copy(&self) -> Arc<dyn TableFilter> {
        Arc::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, other: &dyn TableFilter) -> bool {
        let Some(other) = other.as_any().downcast_ref::<InFilter>() else {
            return false;
        };
        self.values == other.values
    }

    fn to_expression_string(&self, column_name: &str) -> String {
        self.to_string(column_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::LogicalType;
    use crate::storage::statistics::{BaseStatistics, NumericStats};

    #[test]
    fn in_filter_all_values_outside_zone_map() {
        let mut stats = BaseStatistics::new(LogicalType::integer());
        stats.set_has_no_null();
        let logical_type = stats.get_type().clone();
        let data = stats.get_stats_data_mut();
        let crate::storage::statistics::StatsData::Numeric(data) = data else {
            panic!("expected numeric stats");
        };
        NumericStats::set_min(data, 10i32, &logical_type);
        NumericStats::set_max(data, 20i32, &logical_type);

        let filter = InFilter::new(vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)]);
        assert_eq!(
            filter.check_statistics(&mut stats),
            FilterPropagateResult::FilterAlwaysFalse
        );
    }
}
