use super::BaseStatistics;
use crate::common::types::LogicalType;

/// Segment-level statistics
/// Simple wrapper around BaseStatistics for segment-level tracking
#[derive(Debug, Clone)]
pub struct SegmentStatistics {
    /// Base statistics for this segment
    statistics: BaseStatistics,
}

impl SegmentStatistics {
    /// Create new segment statistics with a logical type
    pub fn new(logical_type: LogicalType) -> Self {
        Self {
            statistics: BaseStatistics::create_empty(logical_type),
        }
    }

    /// Create new segment statistics from existing base statistics
    pub fn from_stats(stats: BaseStatistics) -> Self {
        Self { statistics: stats }
    }

    /// Get reference to the base statistics
    pub fn statistics(&self) -> &BaseStatistics {
        &self.statistics
    }

    /// Get mutable reference to the base statistics
    pub fn statistics_mut(&mut self) -> &mut BaseStatistics {
        &mut self.statistics
    }

    /// Merge another segment statistics into this one
    pub fn merge(&mut self, other: &SegmentStatistics) {
        self.statistics.merge(other.statistics());
    }

    /// Check if segment has null values
    pub fn has_null(&self) -> bool {
        self.statistics.can_have_null()
    }

    /// Check if segment has non-null values
    pub fn has_no_null(&self) -> bool {
        self.statistics.can_have_no_null()
    }

    /// Set has_null flag
    pub fn set_has_null(&mut self) {
        self.statistics.set_has_null();
    }

    /// Set has_no_null flag
    pub fn set_has_no_null(&mut self) {
        self.statistics.set_has_no_null();
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        self.statistics.to_string()
    }
}

impl Default for SegmentStatistics {
    fn default() -> Self {
        Self {
            statistics: BaseStatistics::create_empty(crate::common::types::LogicalType::integer()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let stats = SegmentStatistics::new(crate::common::types::LogicalType::integer());
        assert!(!stats.statistics().can_have_null());
        assert!(!stats.statistics().can_have_no_null());
    }

    #[test]
    fn test_from_stats() {
        let base_stats =
            BaseStatistics::create_unknown(crate::common::types::LogicalType::integer());
        let seg_stats = SegmentStatistics::from_stats(base_stats);

        assert!(seg_stats.statistics().can_have_null());
        assert!(seg_stats.statistics().can_have_no_null());
    }

    #[test]
    fn test_statistics_mut() {
        let mut stats = SegmentStatistics::new(crate::common::types::LogicalType::integer());
        stats.statistics_mut().set_has_null();

        assert!(stats.statistics().can_have_null());
    }

    #[test]
    fn test_merge() {
        let mut stats1 = SegmentStatistics::new(crate::common::types::LogicalType::integer());
        let mut stats2 = SegmentStatistics::new(crate::common::types::LogicalType::integer());

        stats1.set_has_null();
        stats2.set_has_no_null();

        stats1.merge(&stats2);

        assert!(stats1.has_null());
        assert!(stats1.has_no_null());
    }

    #[test]
    fn test_default() {
        let stats = SegmentStatistics::default();
        assert!(!stats.has_null());
        assert!(!stats.has_no_null());
    }
}
