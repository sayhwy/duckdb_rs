// Statistics module for DuckDB storage engine
// Provides statistical information for query optimization and data pruning

mod base_statistics;
mod numeric_stats;
mod string_stats;
mod distinct_statistics;
mod column_statistics;
mod segment_statistics;

pub use base_statistics::{BaseStatistics, StatisticsType, StatsInfo, FilterPropagateResult};
pub use numeric_stats::{NumericStats, NumericStatsData, NumericValueUnion};
pub use string_stats::{StringStats, StringStatsData};
pub use distinct_statistics::DistinctStatistics;
pub use column_statistics::ColumnStatistics;
pub use segment_statistics::SegmentStatistics;
