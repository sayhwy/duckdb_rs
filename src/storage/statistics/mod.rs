// Statistics module for DuckDB storage engine
// Provides statistical information for query optimization and data pruning

mod base_statistics;
mod column_statistics;
mod distinct_statistics;
mod numeric_stats;
mod segment_statistics;
mod string_stats;

pub use base_statistics::{BaseStatistics, FilterPropagateResult, StatisticsType, StatsInfo};
pub use column_statistics::ColumnStatistics;
pub use distinct_statistics::DistinctStatistics;
pub use numeric_stats::{NumericStats, NumericStatsData, NumericValueUnion};
pub use segment_statistics::SegmentStatistics;
pub use string_stats::{StringStats, StringStatsData};
