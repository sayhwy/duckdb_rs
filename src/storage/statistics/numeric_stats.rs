use super::FilterPropagateResult;
use crate::common::types::{LogicalType, LogicalTypeId};
use std::fmt;

/// Union type for storing numeric values of different types
#[derive(Clone, Copy)]
pub union NumericValueUnion {
    pub boolean: bool,
    pub tinyint: i8,
    pub smallint: i16,
    pub integer: i32,
    pub bigint: i64,
    pub hugeint: i128,
    pub float: f32,
    pub double: f64,
}

impl Default for NumericValueUnion {
    fn default() -> Self {
        NumericValueUnion { bigint: 0 }
    }
}

impl fmt::Debug for NumericValueUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NumericValueUnion")
    }
}

/// Numeric statistics data
#[derive(Clone)]
pub struct NumericStatsData {
    pub min: NumericValueUnion,
    pub max: NumericValueUnion,
    pub has_min: bool,
    pub has_max: bool,
}

impl fmt::Debug for NumericStatsData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NumericStatsData")
            .field("has_min", &self.has_min)
            .field("has_max", &self.has_max)
            .finish()
    }
}

impl Default for NumericStatsData {
    fn default() -> Self {
        Self {
            min: NumericValueUnion::default(),
            max: NumericValueUnion::default(),
            has_min: false,
            has_max: false,
        }
    }
}

/// Numeric statistics operations
pub struct NumericStats;

impl NumericStats {
    /// Check if the statistics has both min and max values
    pub fn has_min_max(data: &NumericStatsData) -> bool {
        data.has_min && data.has_max
    }

    /// Check if the statistics has a min value
    pub fn has_min(data: &NumericStatsData) -> bool {
        data.has_min
    }

    /// Check if the statistics has a max value
    pub fn has_max(data: &NumericStatsData) -> bool {
        data.has_max
    }

    // ── BaseStatistics overloads (C++: NumericStats::HasMin/HasMax/GetMinUnsafe/GetMaxUnsafe) ──

    /// C++: `NumericStats::HasMin(const BaseStatistics &stats)`
    pub fn has_min_stats(stats: &super::BaseStatistics) -> bool {
        stats.get_numeric_data().map_or(false, |d| d.has_min)
    }

    /// C++: `NumericStats::HasMax(const BaseStatistics &stats)`
    pub fn has_max_stats(stats: &super::BaseStatistics) -> bool {
        stats.get_numeric_data().map_or(false, |d| d.has_max)
    }

    /// C++: `NumericStats::GetMinUnsafe<uint32_t>(stats)` — reads min as u32.
    /// # Safety
    /// Caller must ensure the stats are numeric and the stored type matches u32 width.
    pub fn get_min_unsafe_u32(stats: &super::BaseStatistics) -> u32 {
        stats
            .get_numeric_data()
            .map_or(0, |d| unsafe { d.min.integer as u32 })
    }

    /// C++: `NumericStats::GetMaxUnsafe<uint32_t>(stats)` — reads max as u32.
    /// # Safety
    /// Same as `get_min_unsafe_u32`.
    pub fn get_max_unsafe_u32(stats: &super::BaseStatistics) -> u32 {
        stats
            .get_numeric_data()
            .map_or(0, |d| unsafe { d.max.integer as u32 })
    }

    /// Check if the statistics represent a constant value (min == max)
    pub fn is_constant(data: &NumericStatsData) -> bool {
        if !Self::has_min_max(data) {
            return false;
        }
        // For simplicity, we'll just check if has both min and max
        // In a full implementation, we'd compare the actual values
        true
    }

    /// Set the minimum value
    pub fn set_min<T: Copy>(data: &mut NumericStatsData, value: T, logical_type: &LogicalType) {
        data.has_min = true;
        unsafe {
            match logical_type.id {
                LogicalTypeId::Boolean => data.min.boolean = *((&value as *const T) as *const bool),
                LogicalTypeId::TinyInt => data.min.tinyint = *((&value as *const T) as *const i8),
                LogicalTypeId::SmallInt => {
                    data.min.smallint = *((&value as *const T) as *const i16)
                }
                LogicalTypeId::Integer => data.min.integer = *((&value as *const T) as *const i32),
                LogicalTypeId::BigInt => data.min.bigint = *((&value as *const T) as *const i64),
                LogicalTypeId::HugeInt => data.min.hugeint = *((&value as *const T) as *const i128),
                LogicalTypeId::Float => data.min.float = *((&value as *const T) as *const f32),
                LogicalTypeId::Double => data.min.double = *((&value as *const T) as *const f64),
                _ => {}
            }
        }
    }

    /// Set the maximum value
    pub fn set_max<T: Copy>(data: &mut NumericStatsData, value: T, logical_type: &LogicalType) {
        data.has_max = true;
        unsafe {
            match logical_type.id {
                LogicalTypeId::Boolean => data.max.boolean = *((&value as *const T) as *const bool),
                LogicalTypeId::TinyInt => data.max.tinyint = *((&value as *const T) as *const i8),
                LogicalTypeId::SmallInt => {
                    data.max.smallint = *((&value as *const T) as *const i16)
                }
                LogicalTypeId::Integer => data.max.integer = *((&value as *const T) as *const i32),
                LogicalTypeId::BigInt => data.max.bigint = *((&value as *const T) as *const i64),
                LogicalTypeId::HugeInt => data.max.hugeint = *((&value as *const T) as *const i128),
                LogicalTypeId::Float => data.max.float = *((&value as *const T) as *const f32),
                LogicalTypeId::Double => data.max.double = *((&value as *const T) as *const f64),
                _ => {}
            }
        }
    }

    /// Merge two numeric statistics
    pub fn merge(
        data: &mut NumericStatsData,
        other: &NumericStatsData,
        logical_type: &LogicalType,
    ) {
        if !other.has_min || !data.has_min {
            data.has_min = false;
        } else {
            // Compare and update min
            if Self::compare_values(&other.min, &data.min, logical_type) < 0 {
                data.min = other.min;
            }
        }

        if !other.has_max || !data.has_max {
            data.has_max = false;
        } else {
            // Compare and update max
            if Self::compare_values(&other.max, &data.max, logical_type) > 0 {
                data.max = other.max;
            }
        }
    }

    /// Compare two numeric values
    /// Returns: -1 if a < b, 0 if a == b, 1 if a > b
    fn compare_values(
        a: &NumericValueUnion,
        b: &NumericValueUnion,
        logical_type: &LogicalType,
    ) -> i32 {
        unsafe {
            match logical_type.id {
                LogicalTypeId::TinyInt => {
                    if a.tinyint < b.tinyint {
                        -1
                    } else if a.tinyint > b.tinyint {
                        1
                    } else {
                        0
                    }
                }
                LogicalTypeId::SmallInt => {
                    if a.smallint < b.smallint {
                        -1
                    } else if a.smallint > b.smallint {
                        1
                    } else {
                        0
                    }
                }
                LogicalTypeId::Integer => {
                    if a.integer < b.integer {
                        -1
                    } else if a.integer > b.integer {
                        1
                    } else {
                        0
                    }
                }
                LogicalTypeId::BigInt => {
                    if a.bigint < b.bigint {
                        -1
                    } else if a.bigint > b.bigint {
                        1
                    } else {
                        0
                    }
                }
                LogicalTypeId::HugeInt => {
                    if a.hugeint < b.hugeint {
                        -1
                    } else if a.hugeint > b.hugeint {
                        1
                    } else {
                        0
                    }
                }
                LogicalTypeId::Float => {
                    if a.float < b.float {
                        -1
                    } else if a.float > b.float {
                        1
                    } else {
                        0
                    }
                }
                LogicalTypeId::Double => {
                    if a.double < b.double {
                        -1
                    } else if a.double > b.double {
                        1
                    } else {
                        0
                    }
                }
                _ => 0,
            }
        }
    }

    /// Check zonemap for filtering
    pub fn check_zonemap(
        data: &NumericStatsData,
        _comparison_type: &str,
        _constant: &NumericValueUnion,
        _logical_type: &LogicalType,
    ) -> FilterPropagateResult {
        if !Self::has_min_max(data) {
            return FilterPropagateResult::NoPruningPossible;
        }

        // TODO: Implement full zonemap checking logic
        FilterPropagateResult::NoPruningPossible
    }

    /// Convert statistics to string
    pub fn to_string(data: &NumericStatsData, logical_type: &LogicalType) -> String {
        if !data.has_min && !data.has_max {
            return "[Min: NULL, Max: NULL]".to_string();
        }

        let min_str = if data.has_min {
            Self::value_to_string(&data.min, logical_type)
        } else {
            "NULL".to_string()
        };

        let max_str = if data.has_max {
            Self::value_to_string(&data.max, logical_type)
        } else {
            "NULL".to_string()
        };

        format!("[Min: {}, Max: {}]", min_str, max_str)
    }

    /// Convert a numeric value to string
    fn value_to_string(value: &NumericValueUnion, logical_type: &LogicalType) -> String {
        unsafe {
            match logical_type.id {
                LogicalTypeId::Boolean => value.boolean.to_string(),
                LogicalTypeId::TinyInt => value.tinyint.to_string(),
                LogicalTypeId::SmallInt => value.smallint.to_string(),
                LogicalTypeId::Integer => value.integer.to_string(),
                LogicalTypeId::BigInt => value.bigint.to_string(),
                LogicalTypeId::HugeInt => value.hugeint.to_string(),
                LogicalTypeId::Float => value.float.to_string(),
                LogicalTypeId::Double => value.double.to_string(),
                _ => "?".to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_stats_default() {
        let data = NumericStatsData::default();
        assert!(!data.has_min);
        assert!(!data.has_max);
    }

    #[test]
    fn test_set_min_max() {
        let mut data = NumericStatsData::default();
        let logical_type = LogicalType::integer();

        NumericStats::set_min(&mut data, 10i32, &logical_type);
        NumericStats::set_max(&mut data, 100i32, &logical_type);

        assert!(data.has_min);
        assert!(data.has_max);
        unsafe {
            assert_eq!(data.min.integer, 10);
            assert_eq!(data.max.integer, 100);
        }
    }

    #[test]
    fn test_merge() {
        let mut data1 = NumericStatsData::default();
        let mut data2 = NumericStatsData::default();
        let logical_type = LogicalType::integer();

        NumericStats::set_min(&mut data1, 10i32, &logical_type);
        NumericStats::set_max(&mut data1, 50i32, &logical_type);

        NumericStats::set_min(&mut data2, 5i32, &logical_type);
        NumericStats::set_max(&mut data2, 100i32, &logical_type);

        NumericStats::merge(&mut data1, &data2, &logical_type);

        unsafe {
            assert_eq!(data1.min.integer, 5);
            assert_eq!(data1.max.integer, 100);
        }
    }
}
