use super::FilterPropagateResult;
use crate::catalog::Value;
use crate::common::enums::ExpressionType;
use std::cmp::Ordering;

/// Maximum size for min/max string prefix storage (8 bytes)
pub const MAX_STRING_MINMAX_SIZE: usize = 8;

/// String statistics data
#[derive(Debug, Clone)]
pub struct StringStatsData {
    /// Minimum string prefix (up to 8 bytes)
    pub min: [u8; MAX_STRING_MINMAX_SIZE],
    /// Maximum string prefix (up to 8 bytes)
    pub max: [u8; MAX_STRING_MINMAX_SIZE],
    /// Maximum string length encountered
    pub max_string_length: u32,
    /// Whether max_string_length is valid
    pub has_max_string_length: bool,
    /// Whether the data contains unicode characters
    pub has_unicode: bool,
}

impl Default for StringStatsData {
    fn default() -> Self {
        Self {
            min: [0xFF; MAX_STRING_MINMAX_SIZE], // Initialize to max for proper min comparison
            max: [0; MAX_STRING_MINMAX_SIZE],    // Initialize to min for proper max comparison
            max_string_length: 0,
            has_max_string_length: true,
            has_unicode: false,
        }
    }
}

/// String statistics operations
pub struct StringStats;

impl StringStats {
    /// Create unknown string statistics
    pub fn create_unknown() -> StringStatsData {
        StringStatsData {
            min: [0; MAX_STRING_MINMAX_SIZE],
            max: [0xFF; MAX_STRING_MINMAX_SIZE],
            max_string_length: 0,
            has_max_string_length: false,
            has_unicode: true,
        }
    }

    /// Create empty string statistics
    pub fn create_empty() -> StringStatsData {
        StringStatsData::default()
    }

    /// Check if statistics has max string length
    pub fn has_max_string_length(data: &StringStatsData) -> bool {
        data.has_max_string_length
    }

    /// Get the max string length
    pub fn max_string_length(data: &StringStatsData) -> u32 {
        data.max_string_length
    }

    /// Check if data can contain unicode
    pub fn can_contain_unicode(data: &StringStatsData) -> bool {
        data.has_unicode
    }

    /// Get the minimum string prefix
    pub fn min(data: &StringStatsData) -> String {
        Self::get_string_value(&data.min)
    }

    /// Get the maximum string prefix
    pub fn max(data: &StringStatsData) -> String {
        Self::get_string_value(&data.max)
    }

    /// Extract string from byte array (stops at first null byte)
    fn get_string_value(bytes: &[u8; MAX_STRING_MINMAX_SIZE]) -> String {
        let len = bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(MAX_STRING_MINMAX_SIZE);
        String::from_utf8_lossy(&bytes[..len]).to_string()
    }

    /// Update statistics with a new string value
    pub fn update(data: &mut StringStatsData, value: &str) {
        let bytes = value.as_bytes();
        let size = bytes.len();

        // Construct the value (up to MAX_STRING_MINMAX_SIZE bytes)
        let mut target = [0u8; MAX_STRING_MINMAX_SIZE];
        let copy_size = size.min(MAX_STRING_MINMAX_SIZE);
        target[..copy_size].copy_from_slice(&bytes[..copy_size]);

        // Update min
        if Self::compare_bytes(&target, &data.min) < 0 {
            data.min = target;
        }

        // Update max
        if Self::compare_bytes(&target, &data.max) > 0 {
            data.max = target;
        }

        // Update max string length
        if size > data.max_string_length as usize {
            data.max_string_length = size as u32;
        }

        // Check for unicode (simple check: any byte > 127)
        if !data.has_unicode && bytes.iter().any(|&b| b > 127) {
            data.has_unicode = true;
        }
    }

    /// Set the minimum string value
    pub fn set_min(data: &mut StringStatsData, value: &str) {
        let bytes = value.as_bytes();
        let copy_size = bytes.len().min(MAX_STRING_MINMAX_SIZE);
        data.min[..copy_size].copy_from_slice(&bytes[..copy_size]);
        if copy_size < MAX_STRING_MINMAX_SIZE {
            data.min[copy_size..].fill(0);
        }
    }

    /// Set the maximum string value
    pub fn set_max(data: &mut StringStatsData, value: &str) {
        let bytes = value.as_bytes();
        let copy_size = bytes.len().min(MAX_STRING_MINMAX_SIZE);
        data.max[..copy_size].copy_from_slice(&bytes[..copy_size]);
        if copy_size < MAX_STRING_MINMAX_SIZE {
            data.max[copy_size..].fill(0);
        }
    }

    /// Reset max string length tracking
    pub fn reset_max_string_length(data: &mut StringStatsData) {
        data.has_max_string_length = false;
    }

    /// Set max string length
    pub fn set_max_string_length(data: &mut StringStatsData, length: u32) {
        data.has_max_string_length = true;
        data.max_string_length = length;
    }

    /// Mark that the data contains unicode
    pub fn set_contains_unicode(data: &mut StringStatsData) {
        data.has_unicode = true;
    }

    /// Merge two string statistics
    pub fn merge(data: &mut StringStatsData, other: &StringStatsData) {
        // Merge min
        if Self::compare_bytes(&other.min, &data.min) < 0 {
            data.min = other.min;
        }

        // Merge max
        if Self::compare_bytes(&other.max, &data.max) > 0 {
            data.max = other.max;
        }

        // Merge unicode flag
        data.has_unicode = data.has_unicode || other.has_unicode;

        // Merge max string length
        data.has_max_string_length = data.has_max_string_length && other.has_max_string_length;
        data.max_string_length = data.max_string_length.max(other.max_string_length);
    }

    /// Compare two byte arrays
    /// Returns: -1 if a < b, 0 if a == b, 1 if a > b
    fn compare_bytes(a: &[u8; MAX_STRING_MINMAX_SIZE], b: &[u8; MAX_STRING_MINMAX_SIZE]) -> i32 {
        match a.cmp(b) {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }
    }

    /// Check zonemap for filtering
    pub fn check_zonemap(
        data: &StringStatsData,
        comparison_type: ExpressionType,
        constant: &str,
    ) -> FilterPropagateResult {
        let constant_bytes = constant.as_bytes();
        let constant_size = constant_bytes.len();

        let mut constant_prefix = [0u8; MAX_STRING_MINMAX_SIZE];
        let copy_size = constant_size.min(MAX_STRING_MINMAX_SIZE);
        constant_prefix[..copy_size].copy_from_slice(&constant_bytes[..copy_size]);

        let min_cmp = Self::compare_bytes(&constant_prefix, &data.min);
        let max_cmp = Self::compare_bytes(&constant_prefix, &data.max);

        match comparison_type {
            ExpressionType::CompareEqual => {
                // Equal: constant must be within [min, max]
                if min_cmp >= 0 && max_cmp <= 0 {
                    if min_cmp == 0
                        && max_cmp == 0
                        && data.has_max_string_length
                        && data.max_string_length as usize == constant_size
                    {
                        FilterPropagateResult::FilterAlwaysTrue
                    } else {
                        FilterPropagateResult::NoPruningPossible
                    }
                } else {
                    FilterPropagateResult::FilterAlwaysFalse
                }
            }
            ExpressionType::CompareNotEqual => {
                // Not equal: if constant is outside [min, max], always true
                if min_cmp < 0 || max_cmp > 0 {
                    FilterPropagateResult::FilterAlwaysTrue
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ExpressionType::CompareGreaterThan => {
                if max_cmp <= 0 {
                    FilterPropagateResult::FilterAlwaysFalse
                } else if min_cmp > 0 {
                    FilterPropagateResult::FilterAlwaysTrue
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ExpressionType::CompareGreaterThanOrEqualTo => {
                if max_cmp < 0 {
                    FilterPropagateResult::FilterAlwaysFalse
                } else if min_cmp >= 0 {
                    FilterPropagateResult::FilterAlwaysTrue
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ExpressionType::CompareLessThan => {
                if min_cmp >= 0 {
                    FilterPropagateResult::FilterAlwaysFalse
                } else if max_cmp < 0 {
                    FilterPropagateResult::FilterAlwaysTrue
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            ExpressionType::CompareLessThanOrEqualTo => {
                if min_cmp > 0 {
                    FilterPropagateResult::FilterAlwaysFalse
                } else if max_cmp <= 0 {
                    FilterPropagateResult::FilterAlwaysTrue
                } else {
                    FilterPropagateResult::NoPruningPossible
                }
            }
            _ => FilterPropagateResult::NoPruningPossible,
        }
    }

    pub fn check_zonemap_stats(
        stats: &super::BaseStatistics,
        comparison_type: ExpressionType,
        constants: &[Value],
    ) -> FilterPropagateResult {
        let super::StatsData::String(data) = stats.get_stats_data() else {
            return FilterPropagateResult::NoPruningPossible;
        };
        match comparison_type {
            ExpressionType::CompareEqual => {
                let mut found_unknown = false;
                for constant in constants {
                    let Value::Text(constant) = constant else {
                        return FilterPropagateResult::NoPruningPossible;
                    };
                    let result = Self::check_zonemap(data, comparison_type, constant);
                    if result == FilterPropagateResult::FilterAlwaysTrue {
                        return FilterPropagateResult::FilterAlwaysTrue;
                    }
                    if result == FilterPropagateResult::NoPruningPossible {
                        found_unknown = true;
                    }
                }
                if found_unknown {
                    FilterPropagateResult::NoPruningPossible
                } else {
                    FilterPropagateResult::FilterAlwaysFalse
                }
            }
            _ => {
                let Some(Value::Text(constant)) = constants.first() else {
                    return FilterPropagateResult::NoPruningPossible;
                };
                Self::check_zonemap(data, comparison_type, constant)
            }
        }
    }

    /// Convert statistics to string
    pub fn to_string(data: &StringStatsData) -> String {
        let min_str = Self::get_string_value(&data.min);
        let max_str = Self::get_string_value(&data.max);
        let max_len_str = if data.has_max_string_length {
            data.max_string_length.to_string()
        } else {
            "?".to_string()
        };

        format!(
            "[Min: {:?}, Max: {:?}, Has Unicode: {}, Max String Length: {}]",
            min_str, max_str, data.has_unicode, max_len_str
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_stats_default() {
        let data = StringStatsData::default();
        assert!(data.has_max_string_length);
        assert!(!data.has_unicode);
        assert_eq!(data.max_string_length, 0);
    }

    #[test]
    fn test_update() {
        let mut data = StringStatsData::default();

        StringStats::update(&mut data, "hello");
        assert_eq!(data.max_string_length, 5);

        StringStats::update(&mut data, "world");
        assert_eq!(data.max_string_length, 5);

        StringStats::update(&mut data, "longer string");
        assert_eq!(data.max_string_length, 13);
    }

    #[test]
    fn test_min_max() {
        let mut data = StringStatsData::default();

        StringStats::update(&mut data, "banana");
        StringStats::update(&mut data, "apple");
        StringStats::update(&mut data, "cherry");

        let min = StringStats::min(&data);
        let max = StringStats::max(&data);

        assert_eq!(min, "apple");
        assert_eq!(max, "cherry");
    }

    #[test]
    fn test_unicode_detection() {
        let mut data = StringStatsData::default();
        assert!(!data.has_unicode);

        StringStats::update(&mut data, "hello");
        assert!(!data.has_unicode);

        StringStats::update(&mut data, "你好");
        assert!(data.has_unicode);
    }

    #[test]
    fn test_merge() {
        let mut data1 = StringStatsData::default();
        let mut data2 = StringStatsData::default();

        StringStats::update(&mut data1, "apple");
        StringStats::update(&mut data1, "banana");

        StringStats::update(&mut data2, "cherry");
        StringStats::update(&mut data2, "date");

        StringStats::merge(&mut data1, &data2);

        let min = StringStats::min(&data1);
        let max = StringStats::max(&data1);

        assert_eq!(min, "apple");
        assert_eq!(max, "date");
    }
}
