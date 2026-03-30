//! Statistics for variable-length LIST types.
//!
//! Mirrors `duckdb/storage/statistics/list_stats.cpp`.

use crate::common::types::{LogicalType, LogicalTypeId, SelectionVector, Vector};

use super::base_statistics::{BaseStatistics, StatisticsType};

pub struct ListStats;

impl ListStats {
    pub fn construct(stats: &mut BaseStatistics) {
        let child_type = stats
            .get_type()
            .get_child_type()
            .cloned()
            .unwrap_or_else(LogicalType::new_invalid);
        stats.child_stats.clear();
        stats
            .child_stats
            .push(Box::new(BaseStatistics::new(child_type)));
    }

    pub fn create_unknown(logical_type: LogicalType) -> BaseStatistics {
        let child_type = logical_type
            .get_child_type()
            .cloned()
            .unwrap_or_else(LogicalType::new_invalid);
        let mut result = BaseStatistics::new(logical_type);
        result.initialize_unknown();
        result
            .child_stats
            .push(Box::new(BaseStatistics::create_unknown(child_type)));
        result
    }

    pub fn create_empty(logical_type: LogicalType) -> BaseStatistics {
        let child_type = logical_type
            .get_child_type()
            .cloned()
            .unwrap_or_else(LogicalType::new_invalid);
        let mut result = BaseStatistics::new(logical_type);
        result.initialize_empty();
        result
            .child_stats
            .push(Box::new(BaseStatistics::create_empty(child_type)));
        result
    }

    pub fn copy(stats: &mut BaseStatistics, other: &BaseStatistics) {
        debug_assert!(!stats.child_stats.is_empty());
        debug_assert!(!other.child_stats.is_empty());
        *stats.child_stats[0] = (*other.child_stats[0]).clone();
    }

    pub fn get_child_stats(stats: &BaseStatistics) -> &BaseStatistics {
        if stats.get_stats_type() != StatisticsType::ListStats {
            panic!("ListStats::GetChildStats called on stats that is not a list");
        }
        debug_assert!(!stats.child_stats.is_empty());
        &stats.child_stats[0]
    }

    pub fn get_child_stats_mut(stats: &mut BaseStatistics) -> &mut BaseStatistics {
        if stats.get_stats_type() != StatisticsType::ListStats {
            panic!("ListStats::GetChildStats called on stats that is not a list");
        }
        debug_assert!(!stats.child_stats.is_empty());
        &mut stats.child_stats[0]
    }

    pub fn set_child_stats(stats: &mut BaseStatistics, new_stats: Option<BaseStatistics>) {
        let replacement = match new_stats {
            Some(new_stats) => new_stats,
            None => BaseStatistics::create_unknown(
                stats
                    .get_type()
                    .get_child_type()
                    .cloned()
                    .unwrap_or_else(LogicalType::new_invalid),
            ),
        };
        if stats.child_stats.is_empty() {
            stats.child_stats.push(Box::new(replacement));
        } else {
            *stats.child_stats[0] = replacement;
        }
    }

    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        if other.get_type().id == LogicalTypeId::Validity {
            return;
        }
        let other_child_stats = Self::get_child_stats(other).clone();
        Self::get_child_stats_mut(stats).merge(&other_child_stats);
    }

    pub fn serialize<W: crate::common::serializer::WriteStream>(
        stats: &BaseStatistics,
        _serializer: &mut W,
    ) {
        let _child_stats = Self::get_child_stats(stats);
        unimplemented!("ListStats::serialize requires a property-based serializer")
    }

    pub fn deserialize<R: crate::common::serializer::ReadStream>(
        _deserializer: &mut R,
        base: &mut BaseStatistics,
    ) {
        debug_assert!(base.get_type().id == LogicalTypeId::List);
        let _child_type = base
            .get_type()
            .get_child_type()
            .cloned()
            .unwrap_or_else(LogicalType::new_invalid);
        unimplemented!("ListStats::deserialize requires a property-based deserializer")
    }

    pub fn to_string(stats: &BaseStatistics) -> String {
        format!("[{}]", Self::get_child_stats(stats))
    }

    pub fn verify(stats: &BaseStatistics, vector: &Vector, sel: &SelectionVector, count: usize) {
        let child_stats = Self::get_child_stats(stats);
        let child_entry = match vector.get_child() {
            Some(child_entry) => child_entry,
            None => return,
        };

        let data = vector.raw_data();
        let mut total_list_count = 0usize;
        for i in 0..count {
            let index = sel.get_index(i);
            if vector.validity.row_is_valid(index) {
                if let Some((_, length)) = read_list_entry(data, index) {
                    total_list_count += length;
                }
            }
        }

        let mut list_sel = Vec::with_capacity(total_list_count);
        for i in 0..count {
            let index = sel.get_index(i);
            if vector.validity.row_is_valid(index) {
                if let Some((offset, length)) = read_list_entry(data, index) {
                    for list_idx in 0..length {
                        list_sel.push((offset + list_idx) as u32);
                    }
                }
            }
        }

        let list_count = list_sel.len();
        let list_sel = SelectionVector { indices: list_sel };
        let _ = child_stats.verify(child_entry, &list_sel, list_count);
    }
}

fn read_list_entry(data: &[u8], idx: usize) -> Option<(usize, usize)> {
    let byte_offset = idx.checked_mul(8)?;
    let bytes = data.get(byte_offset..byte_offset + 8)?;
    let offset = u32::from_le_bytes(bytes[0..4].try_into().ok()?) as usize;
    let length = u32::from_le_bytes(bytes[4..8].try_into().ok()?) as usize;
    Some((offset, length))
}
