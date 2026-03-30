//! Statistics for VARIANT types.
//!
//! Mirrors `duckdb/storage/statistics/variant_stats.cpp`.

use std::collections::HashMap;

use crate::common::types::{LogicalType, LogicalTypeId, SelectionVector, Vector};

use super::base_statistics::{BaseStatistics, VariantStatsData, VariantStatsShreddingState};
use super::{ListStats, NumericStats, StructStats};

pub struct VariantStats;

pub struct VariantShreddedStats;

impl VariantStats {
    pub fn construct(stats: &mut BaseStatistics) {
        stats.child_stats.clear();
        stats
            .child_stats
            .push(Box::new(BaseStatistics::new(LogicalType::new_invalid())));
        stats
            .child_stats
            .push(Box::new(BaseStatistics::new(LogicalType::new_invalid())));
        Self::get_data_unsafe_mut(stats).shredding_state =
            VariantStatsShreddingState::Uninitialized;
        Self::create_unshredded_stats(stats);
    }

    pub fn create_unknown(logical_type: LogicalType) -> BaseStatistics {
        let mut result = BaseStatistics::new(logical_type);
        result.initialize_unknown();
        result
            .child_stats
            .push(Box::new(BaseStatistics::create_unknown(
                get_unshredded_type(),
            )));
        result
            .child_stats
            .push(Box::new(BaseStatistics::new(LogicalType::new_invalid())));
        Self::get_data_unsafe_mut(&mut result).shredding_state =
            VariantStatsShreddingState::Inconsistent;
        result
    }

    pub fn create_empty(logical_type: LogicalType) -> BaseStatistics {
        let mut result = BaseStatistics::new(logical_type);
        result.initialize_empty();
        result.child_stats.push(Box::new(
            BaseStatistics::create_empty(get_unshredded_type()),
        ));
        result
            .child_stats
            .push(Box::new(BaseStatistics::new(LogicalType::new_invalid())));
        Self::get_data_unsafe_mut(&mut result).shredding_state =
            VariantStatsShreddingState::Uninitialized;
        result
    }

    pub fn create_unshredded_stats(stats: &mut BaseStatistics) {
        ensure_child_slot(stats, 0);
        *stats.child_stats[0] = BaseStatistics::new(get_unshredded_type());
    }

    pub fn get_unshredded_stats(stats: &BaseStatistics) -> &BaseStatistics {
        assert_variant(stats);
        &stats.child_stats[0]
    }

    pub fn get_unshredded_stats_mut(stats: &mut BaseStatistics) -> &mut BaseStatistics {
        assert_variant(stats);
        &mut stats.child_stats[0]
    }

    pub fn set_unshredded_stats(stats: &mut BaseStatistics, new_stats: &BaseStatistics) {
        assert_variant(stats);
        ensure_child_slot(stats, 0);
        *stats.child_stats[0] = new_stats.clone();
    }

    pub fn set_unshredded_stats_owned(
        stats: &mut BaseStatistics,
        new_stats: Option<BaseStatistics>,
    ) {
        assert_variant(stats);
        match new_stats {
            Some(new_stats) => Self::set_unshredded_stats(stats, &new_stats),
            None => Self::create_unshredded_stats(stats),
        }
    }

    pub fn mark_as_not_shredded(stats: &mut BaseStatistics) {
        debug_assert!(!Self::is_shredded(stats));
        Self::get_data_unsafe_mut(stats).shredding_state = VariantStatsShreddingState::NotShredded;
    }

    pub fn get_shredded_structured_type(stats: &BaseStatistics) -> LogicalType {
        debug_assert!(Self::is_shredded(stats));
        to_structured_type(Self::get_shredded_stats(stats).get_type())
    }

    pub fn create_shredded_stats(stats: &mut BaseStatistics, shredded_type: &LogicalType) {
        ensure_child_slot(stats, 1);
        *stats.child_stats[1] = BaseStatistics::new(shredded_type.clone());
        Self::get_data_unsafe_mut(stats).shredding_state = VariantStatsShreddingState::Shredded;
    }

    pub fn is_shredded(stats: &BaseStatistics) -> bool {
        Self::get_data_unsafe(stats).shredding_state == VariantStatsShreddingState::Shredded
    }

    pub fn create_shredded(shredded_type: &LogicalType) -> BaseStatistics {
        let mut result = Self::create_empty(LogicalType::variant());
        Self::create_shredded_stats(&mut result, shredded_type);
        *result.child_stats[0] = BaseStatistics::create_empty(get_unshredded_type());
        *result.child_stats[1] = BaseStatistics::create_empty(shredded_type.clone());
        result
    }

    pub fn get_shredded_stats(stats: &BaseStatistics) -> &BaseStatistics {
        assert_variant(stats);
        debug_assert!(Self::is_shredded(stats));
        &stats.child_stats[1]
    }

    pub fn get_shredded_stats_mut(stats: &mut BaseStatistics) -> &mut BaseStatistics {
        assert_variant(stats);
        debug_assert!(Self::is_shredded(stats));
        &mut stats.child_stats[1]
    }

    pub fn set_shredded_stats(stats: &mut BaseStatistics, new_stats: &BaseStatistics) {
        if !Self::is_shredded(stats) {
            ensure_child_slot(stats, 1);
            *stats.child_stats[1] = BaseStatistics::new(new_stats.get_type().clone());
            debug_assert!(
                Self::get_data_unsafe(stats).shredding_state
                    != VariantStatsShreddingState::Inconsistent
            );
            Self::get_data_unsafe_mut(stats).shredding_state = VariantStatsShreddingState::Shredded;
        }
        *stats.child_stats[1] = new_stats.clone();
    }

    pub fn set_shredded_stats_owned(stats: &mut BaseStatistics, new_stats: Option<BaseStatistics>) {
        assert_variant(stats);
        let new_stats = new_stats.expect("VariantStats::SetShreddedStats requires non-null stats");
        Self::set_shredded_stats(stats, &new_stats);
    }

    pub fn serialize<W: crate::common::serializer::WriteStream>(
        stats: &BaseStatistics,
        _serializer: &mut W,
    ) {
        let _data = Self::get_data_unsafe(stats);
        let _unshredded_stats = Self::get_unshredded_stats(stats);
        if Self::is_shredded(stats) {
            let _shredded_stats = Self::get_shredded_stats(stats);
        }
        unimplemented!("VariantStats::serialize requires a property-based serializer")
    }

    pub fn deserialize<R: crate::common::serializer::ReadStream>(
        _deserializer: &mut R,
        base: &mut BaseStatistics,
    ) {
        debug_assert!(base.get_type().id == LogicalTypeId::Variant);
        let _unshredded_type = get_unshredded_type();
        unimplemented!("VariantStats::deserialize requires a property-based deserializer")
    }

    pub fn to_string(stats: &BaseStatistics) -> String {
        let mut result = format!(
            "shredding_state: {}",
            shredding_state_to_string(Self::get_data_unsafe(stats).shredding_state)
        );
        if Self::is_shredded(stats) {
            result.push_str(", shredding: {");
            result.push_str(&format!(
                "typed_value_type: {}, ",
                Self::get_shredded_structured_type(stats).name()
            ));
            result.push_str(&format!(
                "stats: {{{}}}",
                to_string_internal(Self::get_shredded_stats(stats))
            ));
            result.push('}');
        }
        result
    }

    pub fn merge_shredding(
        stats: &mut BaseStatistics,
        other: &BaseStatistics,
        new_stats: &mut BaseStatistics,
    ) -> bool {
        debug_assert!(stats.get_type().id == LogicalTypeId::Struct);
        debug_assert!(other.get_type().id == LogicalTypeId::Struct);

        let stats_children = stats.get_type().get_struct_fields().to_vec();
        let other_children = other.get_type().get_struct_fields().to_vec();
        debug_assert!(stats_children.len() == 2);
        debug_assert!(other_children.len() == 2);

        let stats_typed_value_type = stats_children[1].1.clone();
        let other_typed_value_type = other_children[1].1.clone();

        let mut untyped_value_index = StructStats::get_child_stats(stats, 0).clone();
        untyped_value_index.merge(StructStats::get_child_stats(other, 0));

        let stats_typed_value = StructStats::get_child_stats(stats, 1).clone();
        let other_typed_value = StructStats::get_child_stats(other, 1).clone();

        if stats_typed_value_type.id == LogicalTypeId::Struct {
            if other_typed_value_type.id != LogicalTypeId::Struct {
                return false;
            }

            let stats_object_children = stats_typed_value_type.get_struct_fields().to_vec();
            let other_object_children = other_typed_value_type.get_struct_fields().to_vec();
            let mut key_to_index = HashMap::new();
            for (i, (name, _)) in other_object_children.iter().enumerate() {
                key_to_index.insert(name.to_ascii_lowercase(), i);
            }

            let mut new_children = Vec::new();
            let mut new_child_stats = Vec::new();

            for (i, (name, child_type)) in stats_object_children.iter().enumerate() {
                let Some(other_index) = key_to_index.get(&name.to_ascii_lowercase()).copied()
                else {
                    continue;
                };
                let (_, other_child_type) = &other_object_children[other_index];
                if other_child_type.id != child_type.id {
                    continue;
                }

                let mut lhs_child = StructStats::get_child_stats(&stats_typed_value, i).clone();
                let rhs_child =
                    StructStats::get_child_stats(&other_typed_value, other_index).clone();
                let mut merged_child = BaseStatistics::new(LogicalType::new_invalid());
                if !Self::merge_shredding(&mut lhs_child, &rhs_child, &mut merged_child) {
                    continue;
                }
                new_children.push((name.clone(), merged_child.get_type().clone()));
                new_child_stats.push(merged_child);
            }
            if new_children.is_empty() {
                return false;
            }

            let mut new_typed_value =
                StructStats::create_empty(LogicalType::struct_type(new_children));
            for (i, child_stats) in new_child_stats.into_iter().enumerate() {
                StructStats::set_child_stats(&mut new_typed_value, i, Some(child_stats));
            }
            new_typed_value.combine_validity(&stats_typed_value, &other_typed_value);
            *new_stats = wrap_typed_value(&untyped_value_index, &new_typed_value);
            true
        } else if stats_typed_value_type.id == LogicalTypeId::List {
            if other_typed_value_type.id != LogicalTypeId::List {
                return false;
            }

            let mut lhs_child = ListStats::get_child_stats(&stats_typed_value).clone();
            let rhs_child = ListStats::get_child_stats(&other_typed_value).clone();
            let mut merged_child = BaseStatistics::new(LogicalType::new_invalid());
            if !Self::merge_shredding(&mut lhs_child, &rhs_child, &mut merged_child) {
                return false;
            }

            let mut new_typed_value =
                ListStats::create_empty(LogicalType::list(merged_child.get_type().clone()));
            new_typed_value.combine_validity(&stats_typed_value, &other_typed_value);
            ListStats::set_child_stats(&mut new_typed_value, Some(merged_child));
            *new_stats = wrap_typed_value(&untyped_value_index, &new_typed_value);
            true
        } else {
            if stats_typed_value_type.id != other_typed_value_type.id {
                return false;
            }
            let mut merged = stats.clone();
            merged.merge(other);
            *new_stats = merged;
            true
        }
    }

    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        if other.get_type().id == LogicalTypeId::Validity {
            return;
        }

        ensure_child_slot(stats, 0);
        ensure_child_slot(stats, 1);

        stats.child_stats[0].merge(&other.child_stats[0]);
        let other_shredding_state = Self::get_data_unsafe(other).shredding_state;
        let shredding_state = Self::get_data_unsafe(stats).shredding_state;

        if other_shredding_state == VariantStatsShreddingState::Uninitialized {
            return;
        }

        match shredding_state {
            VariantStatsShreddingState::Inconsistent => {}
            VariantStatsShreddingState::Uninitialized => {
                if other_shredding_state == VariantStatsShreddingState::Shredded {
                    *stats.child_stats[1] =
                        BaseStatistics::create_unknown(other.child_stats[1].get_type().clone());
                    *stats.child_stats[1] = (*other.child_stats[1]).clone();
                }
                Self::get_data_unsafe_mut(stats).shredding_state = other_shredding_state;
            }
            VariantStatsShreddingState::NotShredded => {
                if other_shredding_state != VariantStatsShreddingState::NotShredded {
                    Self::get_data_unsafe_mut(stats).shredding_state =
                        VariantStatsShreddingState::Inconsistent;
                    stats.child_stats[1].set_type(LogicalType::new_invalid());
                }
            }
            VariantStatsShreddingState::Shredded => match other_shredding_state {
                VariantStatsShreddingState::Shredded => {
                    let mut merged_shredding_stats =
                        BaseStatistics::new(LogicalType::new_invalid());
                    if !Self::merge_shredding(
                        stats.child_stats[1].as_mut(),
                        other.child_stats[1].as_ref(),
                        &mut merged_shredding_stats,
                    ) {
                        Self::get_data_unsafe_mut(stats).shredding_state =
                            VariantStatsShreddingState::Inconsistent;
                        stats.child_stats[1].set_type(LogicalType::new_invalid());
                    } else {
                        *stats.child_stats[1] = BaseStatistics::create_unknown(
                            merged_shredding_stats.get_type().clone(),
                        );
                        *stats.child_stats[1] = merged_shredding_stats;
                    }
                }
                _ => {
                    Self::get_data_unsafe_mut(stats).shredding_state =
                        VariantStatsShreddingState::Inconsistent;
                    stats.child_stats[1].set_type(LogicalType::new_invalid());
                }
            },
        }
    }

    pub fn copy(stats: &mut BaseStatistics, other: &BaseStatistics) {
        let other_data = Self::get_data_unsafe(other);
        let data = Self::get_data_unsafe_mut(stats);
        data.shredding_state = other_data.shredding_state;

        ensure_child_slot(stats, 0);
        ensure_child_slot(stats, 1);

        *stats.child_stats[0] = (*other.child_stats[0]).clone();
        if Self::is_shredded(other) {
            *stats.child_stats[1] =
                BaseStatistics::create_unknown(other.child_stats[1].get_type().clone());
            *stats.child_stats[1] = (*other.child_stats[1]).clone();
        } else {
            stats.child_stats[1].set_type(LogicalType::new_invalid());
        }
    }

    pub fn verify(
        _stats: &BaseStatistics,
        _vector: &Vector,
        _sel: &SelectionVector,
        _count: usize,
    ) {
    }

    pub fn get_data_unsafe(stats: &BaseStatistics) -> &VariantStatsData {
        assert_variant(stats);
        stats.get_variant_data()
    }

    pub fn get_data_unsafe_mut(stats: &mut BaseStatistics) -> &mut VariantStatsData {
        assert_variant(stats);
        stats.get_variant_data_mut()
    }
}

impl VariantShreddedStats {
    pub fn is_fully_shredded(stats: &BaseStatistics) -> bool {
        assert_shredded_stats(stats);

        let untyped_value_index_stats = StructStats::get_child_stats(stats, 0);
        let typed_value_stats = StructStats::get_child_stats(stats, 1);

        if !typed_value_stats.can_have_null() {
            return true;
        }
        if !untyped_value_index_stats.can_have_no_null() {
            return false;
        }
        if !NumericStats::has_min_stats(untyped_value_index_stats)
            || !NumericStats::has_max_stats(untyped_value_index_stats)
        {
            return false;
        }
        let min_value = NumericStats::get_min_unsafe_u32(untyped_value_index_stats);
        let max_value = NumericStats::get_max_unsafe_u32(untyped_value_index_stats);
        if min_value != max_value {
            return false;
        }
        min_value == 0
    }
}

fn assert_variant(stats: &BaseStatistics) {
    if stats.get_type().id != LogicalTypeId::Variant {
        panic!("Calling a VariantStats method on BaseStatistics that are not of type VARIANT");
    }
}

fn assert_shredded_stats(stats: &BaseStatistics) {
    if stats.get_type().id != LogicalTypeId::Struct {
        panic!("Shredded stats should be of type STRUCT");
    }
    let struct_children = stats.get_type().get_struct_fields();
    if struct_children.len() != 2 {
        panic!("Shredded stats need to consist of 2 children");
    }
    if struct_children[0].1.id != LogicalTypeId::UInteger {
        panic!("Shredded stats 'untyped_value_index' should be of type UINTEGER");
    }
}

fn get_unshredded_type() -> LogicalType {
    LogicalType::variant()
}

fn to_structured_type(shredding: &LogicalType) -> LogicalType {
    debug_assert!(shredding.id == LogicalTypeId::Struct);
    let child_types = shredding.get_struct_fields();
    debug_assert!(child_types.len() == 2);

    let typed_value = &child_types[1].1;
    if typed_value.id == LogicalTypeId::Struct {
        let structured_children = typed_value
            .get_struct_fields()
            .iter()
            .map(|(name, child)| (name.clone(), to_structured_type(child)))
            .collect();
        LogicalType::struct_type(structured_children)
    } else if typed_value.id == LogicalTypeId::List {
        let child_type = typed_value
            .get_child_type()
            .unwrap_or_else(|| panic!("LIST typed_value missing child type"));
        LogicalType::list(to_structured_type(child_type))
    } else {
        typed_value.clone()
    }
}

fn to_string_internal(stats: &BaseStatistics) -> String {
    let mut result = format!(
        "fully_shredded: {}",
        if VariantShreddedStats::is_fully_shredded(stats) {
            "true"
        } else {
            "false"
        }
    );

    let typed_value = StructStats::get_child_stats(stats, 1);
    let type_id = typed_value.get_type().id;
    if type_id == LogicalTypeId::List {
        result.push_str(", child: ");
        result.push_str(&to_string_internal(ListStats::get_child_stats(typed_value)));
    } else if type_id == LogicalTypeId::Struct {
        result.push_str(", children: {");
        let fields = typed_value.get_type().get_struct_fields();
        for (i, (name, _)) in fields.iter().enumerate() {
            if i > 0 {
                result.push_str(", ");
            }
            let child_stats = StructStats::get_child_stats(typed_value, i);
            result.push_str(&format!("{}: {}", name, to_string_internal(child_stats)));
        }
        result.push('}');
    }
    result
}

fn wrap_typed_value(
    untyped_value_index: &BaseStatistics,
    typed_value: &BaseStatistics,
) -> BaseStatistics {
    let mut shredded = StructStats::create_empty(LogicalType::struct_type(vec![
        (
            "untyped_value_index".to_string(),
            untyped_value_index.get_type().clone(),
        ),
        ("typed_value".to_string(), typed_value.get_type().clone()),
    ]));
    StructStats::set_child_stats_from_ref(&mut shredded, 0, untyped_value_index);
    StructStats::set_child_stats_from_ref(&mut shredded, 1, typed_value);
    shredded
}

fn shredding_state_to_string(state: VariantStatsShreddingState) -> &'static str {
    match state {
        VariantStatsShreddingState::Uninitialized => "UNINITIALIZED",
        VariantStatsShreddingState::NotShredded => "NOT_SHREDDED",
        VariantStatsShreddingState::Shredded => "SHREDDED",
        VariantStatsShreddingState::Inconsistent => "INCONSISTENT",
    }
}

fn ensure_child_slot(stats: &mut BaseStatistics, index: usize) {
    while stats.child_stats.len() <= index {
        stats
            .child_stats
            .push(Box::new(BaseStatistics::new(LogicalType::new_invalid())));
    }
}
