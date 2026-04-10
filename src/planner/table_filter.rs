use crate::common::enums::ExpressionType;
use crate::catalog::Value;
use crate::storage::statistics::{BaseStatistics, FilterPropagateResult};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TableFilterType {
    ConstantComparison = 0,
    IsNull = 1,
    IsNotNull = 2,
    ConjunctionOr = 3,
    ConjunctionAnd = 4,
    StructExtract = 5,
    OptionalFilter = 6,
    InFilter = 7,
    DynamicFilter = 8,
    ExpressionFilter = 9,
    BloomFilter = 10,
    PerfectHashJoinFilter = 11,
    PrefixRangeFilter = 12,
}

pub trait TableFilter: Debug + Send + Sync {
    fn filter_type(&self) -> TableFilterType;
    fn check_statistics(&self, stats: &mut BaseStatistics) -> FilterPropagateResult;
    fn to_string(&self, column_name: &str) -> String;
    fn copy(&self) -> Arc<dyn TableFilter>;
    fn as_any(&self) -> &dyn Any;
    fn to_expression_string(&self, column_name: &str) -> String;

    fn equals(&self, other: &dyn TableFilter) -> bool {
        self.filter_type() == other.filter_type()
    }

    fn is_only_for_zone_map_filtering(&self) -> bool {
        false
    }

    fn debug_to_string(&self) -> String {
        self.to_string("?")
    }
}

fn value_partial_cmp(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Boolean(l), Value::Boolean(r)) => l.partial_cmp(r),
        (Value::Integer(l), Value::Integer(r)) => l.partial_cmp(r),
        (Value::Float(l), Value::Float(r)) => l.partial_cmp(r),
        (Value::Integer(l), Value::Float(r)) => (*l as f64).partial_cmp(r),
        (Value::Float(l), Value::Integer(r)) => l.partial_cmp(&(*r as f64)),
        (Value::Text(l), Value::Text(r)) => l.partial_cmp(r),
        (Value::Blob(l), Value::Blob(r)) => l.partial_cmp(r),
        _ => None,
    }
}

pub fn compare_value(left: &Value, comparison_type: ExpressionType, right: &Value) -> bool {
    use std::cmp::Ordering;

    match comparison_type {
        ExpressionType::CompareEqual => left == right,
        ExpressionType::CompareNotEqual => left != right,
        ExpressionType::CompareGreaterThan => {
            value_partial_cmp(left, right).is_some_and(|ord| ord == Ordering::Greater)
        }
        ExpressionType::CompareGreaterThanOrEqualTo => value_partial_cmp(left, right)
            .is_some_and(|ord| ord == Ordering::Greater || ord == Ordering::Equal),
        ExpressionType::CompareLessThan => {
            value_partial_cmp(left, right).is_some_and(|ord| ord == Ordering::Less)
        }
        ExpressionType::CompareLessThanOrEqualTo => value_partial_cmp(left, right)
            .is_some_and(|ord| ord == Ordering::Less || ord == Ordering::Equal),
        _ => panic!("unsupported comparison type for compare_value"),
    }
}
