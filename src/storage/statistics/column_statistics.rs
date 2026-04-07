use super::{BaseStatistics, DistinctStatistics};
use crate::common::serializer::BinarySerializer;
use crate::common::serializer::{BinaryMetadataDeserializer, MESSAGE_TERMINATOR_FIELD_ID};
use crate::common::types::LogicalType;
use std::io;
use std::sync::Arc;

/// Column-level statistics
/// Combines base statistics with distinct count estimation
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Base statistics (min/max, null info, etc.)
    stats: BaseStatistics,
    /// Distinct statistics (cardinality estimation)
    distinct_stats: Option<DistinctStatistics>,
}

impl ColumnStatistics {
    /// Create new column statistics with base stats
    pub fn new(stats: BaseStatistics) -> Self {
        let distinct_stats = if DistinctStatistics::type_is_supported(&stats.get_type().id) {
            Some(DistinctStatistics::new())
        } else {
            None
        };

        Self {
            stats,
            distinct_stats,
        }
    }

    /// Create new column statistics with both base and distinct stats
    pub fn with_distinct(
        stats: BaseStatistics,
        distinct_stats: Option<DistinctStatistics>,
    ) -> Self {
        Self {
            stats,
            distinct_stats,
        }
    }

    /// Create empty statistics for a given type
    pub fn create_empty(logical_type: LogicalType) -> Arc<Self> {
        Arc::new(Self::new(BaseStatistics::create_empty(logical_type)))
    }

    /// Merge another column statistics into this one
    pub fn merge(&mut self, other: &ColumnStatistics) {
        self.stats.merge(&other.stats);

        if let (Some(dist), Some(other_dist)) = (&mut self.distinct_stats, &other.distinct_stats) {
            dist.merge(other_dist);
        }
    }

    /// Get reference to base statistics
    pub fn statistics(&self) -> &BaseStatistics {
        &self.stats
    }

    /// Get mutable reference to base statistics
    pub fn statistics_mut(&mut self) -> &mut BaseStatistics {
        &mut self.stats
    }

    /// Check if distinct statistics are available
    pub fn has_distinct_stats(&self) -> bool {
        self.distinct_stats.is_some()
    }

    /// Get reference to distinct statistics
    pub fn distinct_stats(&self) -> Option<&DistinctStatistics> {
        self.distinct_stats.as_ref()
    }

    /// Get mutable reference to distinct statistics
    pub fn distinct_stats_mut(&mut self) -> Option<&mut DistinctStatistics> {
        self.distinct_stats.as_mut()
    }

    /// Set distinct statistics
    pub fn set_distinct(&mut self, distinct: Option<DistinctStatistics>) {
        self.distinct_stats = distinct;
    }

    /// Update distinct statistics with new data
    /// Uses sampling for performance
    pub fn update_distinct_statistics(&mut self, hashes: &[u64], count: usize) {
        if let Some(ref mut distinct) = self.distinct_stats {
            let is_integral = matches!(
                self.stats.get_type().id,
                crate::common::types::LogicalTypeId::TinyInt
                    | crate::common::types::LogicalTypeId::SmallInt
                    | crate::common::types::LogicalTypeId::Integer
                    | crate::common::types::LogicalTypeId::BigInt
                    | crate::common::types::LogicalTypeId::HugeInt
            );
            distinct.update_sample(hashes, count, is_integral);
        }
    }

    /// Copy the column statistics
    pub fn copy(&self) -> Self {
        Self {
            stats: self.stats.clone(),
            distinct_stats: self.distinct_stats.as_ref().map(|d| d.copy()),
        }
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        let mut result = self.stats.to_string();
        if let Some(ref distinct) = self.distinct_stats {
            result.push_str(&format!(" {}", distinct.to_string()));
        }
        result
    }

    pub fn serialize_checkpoint(&self, serializer: &mut BinarySerializer<'_>) {
        serializer.begin_object(100);
        self.stats.serialize_checkpoint(serializer);
        serializer.end_object();
        serializer.write_nullable_field(101, self.distinct_stats.is_some());
        if let Some(distinct) = &self.distinct_stats {
            distinct.serialize_checkpoint(serializer);
            serializer.end_object();
            serializer.end_nullable_object();
        }
    }

    pub fn deserialize_checkpoint(
        de: &mut BinaryMetadataDeserializer<'_>,
        logical_type: LogicalType,
    ) -> io::Result<Option<Self>> {
        let present = de.read_u8();
        if present == 0 {
            return Ok(None);
        }
        let mut stats = None;
        let mut distinct = None;
        loop {
            let field = de.next_field()?;
            match field {
                100 => {
                    stats = Some(BaseStatistics::deserialize_checkpoint(
                        de,
                        logical_type.clone(),
                    )?);
                    let next = de.peek_field()?;
                    if next != 101 && next != MESSAGE_TERMINATOR_FIELD_ID {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "after BaseStatistics expected field 101 or terminator, got {next}"
                            ),
                        ));
                    }
                }
                101 => {
                    let distinct_present = de.read_u8();
                    if distinct_present != 0 {
                        distinct = Some(DistinctStatistics::deserialize_checkpoint(de)?);
                        let next = de.peek_field()?;
                        if next != MESSAGE_TERMINATOR_FIELD_ID {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("after DistinctStatistics expected terminator, got {next}"),
                            ));
                        }
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "ColumnStatistics distinct field present but nullable flag is false",
                        ));
                    }
                }
                MESSAGE_TERMINATOR_FIELD_ID => {
                    return Ok(Some(ColumnStatistics::with_distinct(
                        stats.unwrap_or_else(|| BaseStatistics::create_empty(logical_type)),
                        distinct,
                    )));
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected ColumnStatistics field {other}"),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_empty() {
        let stats = ColumnStatistics::create_empty(crate::common::types::LogicalType::integer());
        assert!(!stats.statistics().can_have_null());
        assert!(!stats.statistics().can_have_no_null());
    }

    #[test]
    fn test_has_distinct_stats() {
        let base_stats = BaseStatistics::create_empty(crate::common::types::LogicalType::integer());
        let col_stats = ColumnStatistics::new(base_stats);
        assert!(col_stats.has_distinct_stats());
    }

    #[test]
    fn test_merge() {
        let mut stats1 = ColumnStatistics::new(BaseStatistics::create_empty(
            crate::common::types::LogicalType::integer(),
        ));
        let stats2 = ColumnStatistics::new(BaseStatistics::create_empty(
            crate::common::types::LogicalType::integer(),
        ));

        stats1.statistics_mut().set_has_null();
        stats1.merge(&stats2);

        assert!(stats1.statistics().can_have_null());
    }

    #[test]
    fn test_copy() {
        let stats = ColumnStatistics::new(BaseStatistics::create_empty(
            crate::common::types::LogicalType::integer(),
        ));
        let copied = stats.copy();

        assert_eq!(
            stats.statistics().can_have_null(),
            copied.statistics().can_have_null()
        );
    }
}
