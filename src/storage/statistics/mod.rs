// Statistics module for DuckDB storage engine
// Provides statistical information for query optimization and data pruning

mod array_stats;
mod base_statistics;
mod column_statistics;
mod distinct_statistics;
mod geometry_stats;
mod list_stats;
mod numeric_stats;
mod segment_statistics;
mod string_stats;
mod struct_stats;
mod variant_stats;

pub use base_statistics::{
    BaseStatistics, FilterPropagateResult, StatisticsType, StatsData, StatsInfo,
};
pub use array_stats::ArrayStats;
pub use column_statistics::ColumnStatistics;
pub use distinct_statistics::DistinctStatistics;
pub use list_stats::ListStats;
pub use numeric_stats::{NumericStats, NumericStatsData, NumericValueUnion};
pub use segment_statistics::SegmentStatistics;
pub use string_stats::{StringStats, StringStatsData};
pub use struct_stats::StructStats;
pub use variant_stats::{VariantShreddedStats, VariantStats};
