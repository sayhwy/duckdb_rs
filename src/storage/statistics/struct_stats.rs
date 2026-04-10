//! Statistics for STRUCT types.
//!
//! Mirrors `duckdb/storage/statistics/struct_stats.cpp`.

use crate::common::types::{LogicalType, LogicalTypeId, SelectionVector, Vector};

use super::base_statistics::{BaseStatistics, StatisticsType};

pub struct StructStats;

impl StructStats {
    pub fn construct(stats: &mut BaseStatistics) {
        let child_types: Vec<LogicalType> = stats
            .get_type()
            .get_struct_fields()
            .iter()
            .map(|(_, child_type)| child_type.clone())
            .collect();
        stats.child_stats.clear();
        for child_type in child_types {
            stats
                .child_stats
                .push(Box::new(BaseStatistics::new(child_type)));
        }
    }

    pub fn create_unknown(logical_type: LogicalType) -> BaseStatistics {
        let child_types: Vec<LogicalType> = logical_type
            .get_struct_fields()
            .iter()
            .map(|(_, child_type)| child_type.clone())
            .collect();
        let mut result = BaseStatistics::new(logical_type);
        result.initialize_unknown();
        for child_type in child_types {
            result
                .child_stats
                .push(Box::new(BaseStatistics::create_unknown(child_type)));
        }
        result
    }

    pub fn create_empty(logical_type: LogicalType) -> BaseStatistics {
        let child_types: Vec<LogicalType> = logical_type
            .get_struct_fields()
            .iter()
            .map(|(_, child_type)| child_type.clone())
            .collect();
        let mut result = BaseStatistics::new(logical_type);
        result.initialize_empty();
        for child_type in child_types {
            result
                .child_stats
                .push(Box::new(BaseStatistics::create_empty(child_type)));
        }
        result
    }

    pub fn get_child_stats_ptr(stats: &BaseStatistics) -> &[Box<BaseStatistics>] {
        if stats.get_stats_type() != StatisticsType::StructStats {
            panic!("Calling StructStats::GetChildStats on stats that is not a struct");
        }
        &stats.child_stats
    }

    pub fn get_child_stats(stats: &BaseStatistics, i: usize) -> &BaseStatistics {
        debug_assert!(stats.get_stats_type() == StatisticsType::StructStats);
        if i >= stats.get_type().get_struct_child_count() {
            panic!("Calling StructStats::GetChildStats but there are no stats for this index");
        }
        &stats.child_stats[i]
    }

    pub fn get_child_stats_mut(stats: &mut BaseStatistics, i: usize) -> &mut BaseStatistics {
        debug_assert!(stats.get_stats_type() == StatisticsType::StructStats);
        if i >= stats.get_type().get_struct_child_count() {
            panic!("Calling StructStats::GetChildStats but there are no stats for this index");
        }
        &mut stats.child_stats[i]
    }

    pub fn set_child_stats_from_ref(
        stats: &mut BaseStatistics,
        i: usize,
        new_stats: &BaseStatistics,
    ) {
        debug_assert!(stats.get_stats_type() == StatisticsType::StructStats);
        debug_assert!(i < stats.get_type().get_struct_child_count());
        *stats.child_stats[i] = new_stats.clone();
    }

    pub fn set_child_stats(
        stats: &mut BaseStatistics,
        i: usize,
        new_stats: Option<BaseStatistics>,
    ) {
        debug_assert!(stats.get_stats_type() == StatisticsType::StructStats);
        let replacement = match new_stats {
            Some(new_stats) => new_stats,
            None => BaseStatistics::create_unknown(
                stats
                    .get_type()
                    .get_struct_child_type(i)
                    .cloned()
                    .unwrap_or_else(LogicalType::new_invalid),
            ),
        };
        if i >= stats.child_stats.len() {
            stats.child_stats.push(Box::new(replacement));
        } else {
            *stats.child_stats[i] = replacement;
        }
    }

    pub fn copy(stats: &mut BaseStatistics, other: &BaseStatistics) {
        let count = stats.get_type().get_struct_child_count();
        for i in 0..count {
            *stats.child_stats[i] = (*other.child_stats[i]).clone();
        }
    }

    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        if other.get_type().id == LogicalTypeId::Validity {
            return;
        }
        debug_assert!(stats.get_type().id == other.get_type().id);
        debug_assert!(
            stats.get_type().get_struct_child_count() == other.get_type().get_struct_child_count()
        );
        let child_count = stats.get_type().get_struct_child_count();
        let other_children: Vec<BaseStatistics> = (0..child_count)
            .map(|i| Self::get_child_stats(other, i).clone())
            .collect();
        for (i, other_child) in other_children.iter().enumerate() {
            stats.child_stats[i].merge(other_child);
        }
    }

    pub fn serialize<W: crate::common::serializer::WriteStream>(
        stats: &BaseStatistics,
        _serializer: &mut W,
    ) {
        let _child_stats = Self::get_child_stats_ptr(stats);
        let _child_count = stats.get_type().get_struct_child_count();
        unimplemented!("StructStats::serialize requires a property-based serializer")
    }

    pub fn deserialize<R: crate::common::serializer::ReadStream>(
        _deserializer: &mut R,
        base: &mut BaseStatistics,
    ) {
        debug_assert!(base.get_type().id == LogicalTypeId::Struct);
        let _child_types = base.get_type().get_struct_fields();
        unimplemented!("StructStats::deserialize requires a property-based deserializer")
    }

    pub fn to_string(stats: &BaseStatistics) -> String {
        let mut result = String::from(" {");
        let child_types = stats.get_type().get_struct_fields();
        for (i, (name, _)) in child_types.iter().enumerate() {
            if i > 0 {
                result.push_str(", ");
            }
            result.push_str(name);
            result.push_str(": ");
            result.push_str(&stats.child_stats[i].to_string());
        }
        result.push('}');
        result
    }

    pub fn verify(stats: &BaseStatistics, vector: &Vector, sel: &SelectionVector, count: usize) {
        for (i, child_entry) in vector
            .get_children()
            .iter()
            .enumerate()
            .take(stats.child_stats.len())
        {
            let _ = stats.child_stats[i].verify(child_entry, sel, count);
        }
    }
}
