#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExpressionType {
    CompareEqual = 0,
    CompareNotEqual = 1,
    CompareGreaterThan = 2,
    CompareGreaterThanOrEqualTo = 3,
    CompareLessThan = 4,
    CompareLessThanOrEqualTo = 5,
    CompareIn = 6,
    ConjunctionOr = 7,
    ConjunctionAnd = 8,
    OperatorIsNull = 9,
    OperatorIsNotNull = 10,
}

pub fn expression_type_to_operator(expression_type: ExpressionType) -> &'static str {
    match expression_type {
        ExpressionType::CompareEqual => "=",
        ExpressionType::CompareNotEqual => "!=",
        ExpressionType::CompareGreaterThan => ">",
        ExpressionType::CompareGreaterThanOrEqualTo => ">=",
        ExpressionType::CompareLessThan => "<",
        ExpressionType::CompareLessThanOrEqualTo => "<=",
        ExpressionType::CompareIn => " IN ",
        ExpressionType::ConjunctionOr => " OR ",
        ExpressionType::ConjunctionAnd => " AND ",
        ExpressionType::OperatorIsNull => " IS NULL",
        ExpressionType::OperatorIsNotNull => " IS NOT NULL",
    }
}
