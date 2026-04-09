use crate::common::serializer::BinarySerializer;
use crate::common::serializer::{BinaryMetadataDeserializer, MESSAGE_TERMINATOR_FIELD_ID};
use crate::common::errors::Result;
use crate::common::types::{LogicalType, LogicalTypeId, SelectionVector, Vector};
use std::fmt;
use std::io;

// ─────────────────────────────────────────────────────────────────────────────
// VariantStats auxiliary types (defined here to avoid circular deps)
// Mirrors variant_stats.hpp: VariantStatsShreddingState / VariantStatsData
// ─────────────────────────────────────────────────────────────────────────────

/// Whether a VARIANT column uses shredding and its state.
/// C++: `enum class VariantStatsShreddingState : uint8_t`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantStatsShreddingState {
    /// Initial state – not yet classified.
    Uninitialized,
    /// Column is stored without shredding.
    NotShredded,
    /// Column is stored with a consistent shredding schema.
    Shredded,
    /// Merged from incompatible shredding schemas.
    Inconsistent,
}

/// Extra data stored inside `BaseStatistics` for VARIANT columns.
/// C++: `struct VariantStatsData { VariantStatsShreddingState shredding_state; }`
#[derive(Debug, Clone, Copy)]
pub struct VariantStatsData {
    pub shredding_state: VariantStatsShreddingState,
}

/// Statistics type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsType {
    BaseStats,
    NumericStats,
    StringStats,
    ListStats,
    StructStats,
    ArrayStats,
    GeometryStats,
    VariantStats,
}

/// Statistics information flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsInfo {
    CanHaveNullValues,
    CannotHaveNullValues,
    CanHaveValidValues,
    CannotHaveValidValues,
    CanHaveNullAndValidValues,
}

/// Filter propagation result for zonemap filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterPropagateResult {
    /// No pruning is possible
    NoPruningPossible,
    /// Filter will always be true
    FilterAlwaysTrue,
    /// Filter will always be false
    FilterAlwaysFalse,
}

/// Union type for storing different statistics data
#[derive(Debug, Clone)]
pub enum StatsData {
    Numeric(super::NumericStatsData),
    String(super::StringStatsData),
    Variant(VariantStatsData),
    None,
}

/// Base statistics structure
/// Tracks statistical information about data for query optimization
#[derive(Debug, Clone)]
pub struct BaseStatistics {
    /// Logical type of the data
    logical_type: LogicalType,

    /// Whether the data can contain NULL values
    has_null: bool,

    /// Whether the data can contain non-NULL values
    has_no_null: bool,

    /// Approximate distinct count (cardinality estimate)
    distinct_count: u64,

    /// Type-specific statistics data
    stats_data: StatsData,

    /// Child statistics for nested types (List, Struct, Array)
    /// C++: `unsafe_unique_array<BaseStatistics> child_stats`
    pub child_stats: Vec<Box<BaseStatistics>>,
}

impl BaseStatistics {
    fn decimal_uses_smallint(logical_type: &LogicalType) -> bool {
        logical_type.id == LogicalTypeId::Decimal && logical_type.width <= 4
    }

    fn decimal_uses_integer(logical_type: &LogicalType) -> bool {
        logical_type.id == LogicalTypeId::Decimal && logical_type.width > 4 && logical_type.width <= 9
    }

    fn decimal_uses_bigint(logical_type: &LogicalType) -> bool {
        logical_type.id == LogicalTypeId::Decimal && logical_type.width > 9 && logical_type.width <= 18
    }

    fn decimal_uses_hugeint(logical_type: &LogicalType) -> bool {
        logical_type.id == LogicalTypeId::Decimal && logical_type.width > 18 && logical_type.width <= 38
    }

    /// Create a new BaseStatistics with the given logical type
    pub fn new(logical_type: LogicalType) -> Self {
        let stats_type = Self::get_stats_type_from_logical(&logical_type);
        let stats_data = match stats_type {
            StatisticsType::NumericStats => StatsData::Numeric(Default::default()),
            StatisticsType::StringStats => StatsData::String(Default::default()),
            StatisticsType::VariantStats => StatsData::Variant(VariantStatsData {
                shredding_state: VariantStatsShreddingState::Uninitialized,
            }),
            _ => StatsData::None,
        };

        Self {
            logical_type,
            has_null: false,
            has_no_null: false,
            distinct_count: 0,
            stats_data,
            child_stats: Vec::new(),
        }
    }

    /// Determine the statistics type based on logical type
    pub fn get_stats_type_from_logical(logical_type: &LogicalType) -> StatisticsType {
        match logical_type.id {
            LogicalTypeId::Boolean
            | LogicalTypeId::TinyInt
            | LogicalTypeId::SmallInt
            | LogicalTypeId::Integer
            | LogicalTypeId::Date
            | LogicalTypeId::BigInt
            | LogicalTypeId::Time
            | LogicalTypeId::Timestamp
            | LogicalTypeId::HugeInt
            | LogicalTypeId::Float
            | LogicalTypeId::Double
            | LogicalTypeId::Decimal => StatisticsType::NumericStats,

            LogicalTypeId::Varchar => StatisticsType::StringStats,

            LogicalTypeId::List => StatisticsType::ListStats,
            LogicalTypeId::Struct => StatisticsType::StructStats,
            LogicalTypeId::Array => StatisticsType::ArrayStats,
            LogicalTypeId::Variant => StatisticsType::VariantStats,

            _ => StatisticsType::BaseStats,
        }
    }

    /// Create statistics representing unknown data
    pub fn create_unknown(logical_type: LogicalType) -> Self {
        let mut stats = Self::new(logical_type);
        stats.initialize_unknown();
        stats
    }

    /// Create statistics representing empty data
    pub fn create_empty(logical_type: LogicalType) -> Self {
        let mut stats = Self::new(logical_type);
        stats.initialize_empty();
        stats
    }

    /// Initialize as unknown (can have both NULL and non-NULL values)
    pub fn initialize_unknown(&mut self) {
        self.has_null = true;
        self.has_no_null = true;
    }

    /// Initialize as empty (no values at all)
    pub fn initialize_empty(&mut self) {
        self.has_null = false;
        self.has_no_null = false;
    }

    /// Check if the data can contain NULL values
    pub fn can_have_null(&self) -> bool {
        self.has_null
    }

    /// Check if the data can contain non-NULL values
    pub fn can_have_no_null(&self) -> bool {
        self.has_no_null
    }

    /// Check if the statistics represent a constant value
    pub fn is_constant(&self) -> bool {
        match Self::get_stats_type_from_logical(&self.logical_type) {
            StatisticsType::NumericStats => {
                if let StatsData::Numeric(ref data) = self.stats_data {
                    return super::NumericStats::is_constant(data);
                }
            }
            _ => {}
        }

        false
    }

    /// Merge another statistics object into this one
    pub fn merge(&mut self, other: &BaseStatistics) {
        self.has_null = self.has_null || other.has_null;
        self.has_no_null = self.has_no_null || other.has_no_null;

        match Self::get_stats_type_from_logical(&self.logical_type) {
            StatisticsType::NumericStats => {
                if let (StatsData::Numeric(data), StatsData::Numeric(other_data)) =
                    (&mut self.stats_data, &other.stats_data)
                {
                    super::NumericStats::merge(data, other_data, &self.logical_type);
                }
            }
            StatisticsType::StringStats => {
                if let (StatsData::String(data), StatsData::String(other_data)) =
                    (&mut self.stats_data, &other.stats_data)
                {
                    super::StringStats::merge(data, other_data);
                }
            }
            _ => {}
        }
    }

    /// Get the distinct count estimate
    pub fn get_distinct_count(&self) -> u64 {
        self.distinct_count
    }

    /// Set the distinct count estimate
    pub fn set_distinct_count(&mut self, count: u64) {
        self.distinct_count = count;
    }

    /// Set statistics information flags
    pub fn set(&mut self, info: StatsInfo) {
        match info {
            StatsInfo::CanHaveNullValues => self.set_has_null(),
            StatsInfo::CannotHaveNullValues => self.has_null = false,
            StatsInfo::CanHaveValidValues => self.set_has_no_null(),
            StatsInfo::CannotHaveValidValues => self.has_no_null = false,
            StatsInfo::CanHaveNullAndValidValues => {
                self.set_has_null();
                self.set_has_no_null();
            }
        }
    }

    /// Mark that the data can contain NULL values
    pub fn set_has_null(&mut self) {
        self.has_null = true;
    }

    /// Mark that the data can contain non-NULL values
    pub fn set_has_no_null(&mut self) {
        self.has_no_null = true;
    }

    /// Copy validity information from another statistics object
    pub fn copy_validity(&mut self, other: &BaseStatistics) {
        self.has_null = other.has_null;
        self.has_no_null = other.has_no_null;
    }

    /// Combine validity information from two statistics objects
    pub fn combine_validity(&mut self, left: &BaseStatistics, right: &BaseStatistics) {
        self.has_null = left.has_null || right.has_null;
        self.has_no_null = left.has_no_null || right.has_no_null;
    }

    /// Get the logical type
    pub fn get_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Set the logical type (used by VariantStats when resetting child type to INVALID).
    pub fn set_type(&mut self, t: LogicalType) {
        self.logical_type = t;
    }

    /// Consume self and return a boxed copy (C++: `BaseStatistics::ToUnique()`).
    pub fn to_unique(self) -> Box<BaseStatistics> {
        Box::new(self)
    }

    /// Immutable access to the VARIANT extra data.
    /// C++: `VariantStats::GetDataUnsafe(const BaseStatistics &)`
    pub fn get_variant_data(&self) -> &VariantStatsData {
        match &self.stats_data {
            StatsData::Variant(d) => d,
            _ => panic!("get_variant_data called on non-variant stats"),
        }
    }

    /// Mutable access to the VARIANT extra data.
    /// C++: `VariantStats::GetDataUnsafe(BaseStatistics &)`
    pub fn get_variant_data_mut(&mut self) -> &mut VariantStatsData {
        match &mut self.stats_data {
            StatsData::Variant(d) => d,
            _ => panic!("get_variant_data_mut called on non-variant stats"),
        }
    }

    /// Get the statistics type
    pub fn get_stats_type(&self) -> StatisticsType {
        Self::get_stats_type_from_logical(&self.logical_type)
    }

    /// Get mutable reference to stats data
    pub fn get_stats_data_mut(&mut self) -> &mut StatsData {
        &mut self.stats_data
    }

    /// Get reference to stats data
    pub fn get_stats_data(&self) -> &StatsData {
        &self.stats_data
    }

    /// Get numeric data if this is a numeric stats object.
    pub fn get_numeric_data(&self) -> Option<&super::NumericStatsData> {
        match &self.stats_data {
            StatsData::Numeric(d) => Some(d),
            _ => None,
        }
    }

    /// Verify that a vector matches the statistics
    pub fn verify(
        &self,
        _vector: &Vector,
        _sel: &SelectionVector,
        _count: usize,
    ) -> Result<()> {
        // TODO: Implement verification logic
        Ok(())
    }

    pub fn serialize_checkpoint(&self, serializer: &mut BinarySerializer<'_>) {
        serializer.write_bool(100, self.has_null);
        serializer.write_bool(101, self.has_no_null);
        serializer.write_varint(102, self.distinct_count);
        serializer.begin_object(103);
        match &self.stats_data {
            StatsData::Numeric(data) => {
                Self::serialize_numeric_value(
                    serializer,
                    200,
                    data.has_min,
                    data.min,
                    &self.logical_type,
                );
                Self::serialize_numeric_value(
                    serializer,
                    201,
                    data.has_max,
                    data.max,
                    &self.logical_type,
                );
            }
            StatsData::String(data) => {
                serializer.write_bytes(200, &data.min);
                serializer.write_bytes(201, &data.max);
                serializer.write_bool(202, data.has_unicode);
                serializer.write_bool(203, data.has_max_string_length);
                serializer.write_varint(204, data.max_string_length as u64);
            }
            StatsData::Variant(data) => {
                serializer.write_u8(204, data.shredding_state as u8);
            }
            StatsData::None => {}
        }
        serializer.end_object();
    }

    pub fn deserialize_checkpoint(
        de: &mut BinaryMetadataDeserializer<'_>,
        logical_type: LogicalType,
    ) -> io::Result<Self> {
        let mut result = BaseStatistics::new(logical_type.clone());
        loop {
            match de.next_field()? {
                100 => result.has_null = de.read_u8() != 0,
                101 => result.has_no_null = de.read_u8() != 0,
                102 => result.distinct_count = de.read_varint()?,
                103 => result.stats_data = Self::deserialize_type_stats(de, &logical_type)?,
                MESSAGE_TERMINATOR_FIELD_ID => return Ok(result),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected BaseStatistics field {other}"),
                    ));
                }
            }
        }
    }

    fn serialize_numeric_value(
        serializer: &mut BinarySerializer<'_>,
        field_id: u16,
        present: bool,
        value: super::NumericValueUnion,
        logical_type: &LogicalType,
    ) {
        serializer.begin_object(field_id);
        serializer.write_bool(100, present);
        if present {
            match logical_type.id {
                LogicalTypeId::Float => serializer.write_field_id(101),
                LogicalTypeId::Double => serializer.write_field_id(101),
                _ => serializer.write_field_id(101),
            }
            match logical_type.id {
                LogicalTypeId::Boolean => {
                    serializer.write_varint_raw(unsafe { value.boolean as u64 })
                }
                LogicalTypeId::TinyInt => {
                    serializer.write_signed_varint_raw(unsafe { value.tinyint as i64 })
                }
                LogicalTypeId::SmallInt if !Self::decimal_uses_smallint(logical_type) => {
                    serializer.write_signed_varint_raw(unsafe { value.smallint as i64 })
                }
                LogicalTypeId::Integer | LogicalTypeId::Date
                    if !Self::decimal_uses_integer(logical_type) =>
                {
                    serializer.write_signed_varint_raw(unsafe { value.integer as i64 })
                }
                LogicalTypeId::BigInt | LogicalTypeId::Time | LogicalTypeId::Timestamp
                    if !Self::decimal_uses_bigint(logical_type) =>
                {
                    serializer.write_signed_varint_raw(unsafe { value.bigint })
                }
                LogicalTypeId::HugeInt if !Self::decimal_uses_hugeint(logical_type) => {
                    serializer.write_signed_varint_i128_raw(unsafe { value.hugeint })
                }
                LogicalTypeId::Decimal if Self::decimal_uses_smallint(logical_type) => {
                    serializer.write_signed_varint_raw(unsafe { value.smallint as i64 })
                }
                LogicalTypeId::Decimal if Self::decimal_uses_integer(logical_type) => {
                    serializer.write_signed_varint_raw(unsafe { value.integer as i64 })
                }
                LogicalTypeId::Decimal if Self::decimal_uses_bigint(logical_type) => {
                    serializer.write_signed_varint_raw(unsafe { value.bigint })
                }
                LogicalTypeId::Decimal if Self::decimal_uses_hugeint(logical_type) => {
                    serializer.write_signed_varint_i128_raw(unsafe { value.hugeint })
                }
                LogicalTypeId::Float => {
                    serializer.write_f32_raw(unsafe { value.float });
                }
                LogicalTypeId::Double => {
                    serializer.write_f64_raw(unsafe { value.double });
                }
                _ => serializer.write_varint_raw(0),
            }
        }
        serializer.end_object();
    }

    fn deserialize_type_stats(
        de: &mut BinaryMetadataDeserializer<'_>,
        logical_type: &LogicalType,
    ) -> io::Result<StatsData> {
        let mut result = match Self::get_stats_type_from_logical(logical_type) {
            StatisticsType::NumericStats => StatsData::Numeric(Default::default()),
            StatisticsType::StringStats => StatsData::String(Default::default()),
            StatisticsType::VariantStats => StatsData::Variant(VariantStatsData {
                shredding_state: VariantStatsShreddingState::Uninitialized,
            }),
            _ => StatsData::None,
        };
        loop {
            match de.next_field()? {
                200 if matches!(result, StatsData::Numeric(_)) => {
                    if let StatsData::Numeric(ref mut data) = result {
                        Self::deserialize_numeric_value(de, logical_type, true, data)?;
                    }
                }
                201 if matches!(result, StatsData::Numeric(_)) => {
                    if let StatsData::Numeric(ref mut data) = result {
                        Self::deserialize_numeric_value(de, logical_type, false, data)?;
                    }
                }
                200 if matches!(result, StatsData::String(_)) => {
                    if let StatsData::String(ref mut data) = result {
                        let bytes = de.read_sized_bytes(8)?;
                        data.min.copy_from_slice(&bytes);
                    }
                }
                201 if matches!(result, StatsData::String(_)) => {
                    if let StatsData::String(ref mut data) = result {
                        let bytes = de.read_sized_bytes(8)?;
                        data.max.copy_from_slice(&bytes);
                    }
                }
                202 if matches!(result, StatsData::String(_)) => {
                    if let StatsData::String(ref mut data) = result {
                        data.has_unicode = de.read_u8() != 0;
                    }
                }
                203 if matches!(result, StatsData::String(_)) => {
                    if let StatsData::String(ref mut data) = result {
                        data.has_max_string_length = de.read_u8() != 0;
                    }
                }
                204 if matches!(result, StatsData::String(_)) => {
                    if let StatsData::String(ref mut data) = result {
                        data.max_string_length = de.read_varint()? as u32;
                    }
                }
                204 if matches!(result, StatsData::Variant(_)) => {
                    if let StatsData::Variant(ref mut data) = result {
                        data.shredding_state = match de.read_u8() {
                            1 => VariantStatsShreddingState::NotShredded,
                            2 => VariantStatsShreddingState::Shredded,
                            3 => VariantStatsShreddingState::Inconsistent,
                            _ => VariantStatsShreddingState::Uninitialized,
                        };
                    }
                }
                MESSAGE_TERMINATOR_FIELD_ID => return Ok(result),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected type statistics field {other}"),
                    ));
                }
            }
        }
    }

    fn deserialize_numeric_value(
        de: &mut BinaryMetadataDeserializer<'_>,
        logical_type: &LogicalType,
        is_min: bool,
        data: &mut super::NumericStatsData,
    ) -> io::Result<()> {
        let mut has_value = false;
        loop {
            match de.next_field()? {
                100 => has_value = de.read_u8() != 0,
                101 => {
                    if has_value {
                        match logical_type.id {
                            LogicalTypeId::Boolean => {
                                let value = de.read_varint()? != 0;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::TinyInt => {
                                let value = de.read_i64_varint()? as i8;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::SmallInt if !Self::decimal_uses_smallint(logical_type) => {
                                let value = de.read_i64_varint()? as i16;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::Integer | LogicalTypeId::Date
                                if !Self::decimal_uses_integer(logical_type) =>
                            {
                                let value = de.read_i64_varint()? as i32;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::BigInt
                            | LogicalTypeId::Time
                            | LogicalTypeId::Timestamp
                                if !Self::decimal_uses_bigint(logical_type) =>
                            {
                                let value = de.read_i64_varint()?;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::HugeInt if !Self::decimal_uses_hugeint(logical_type) => {
                                let value = de.read_i128_varint()?;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::Decimal if Self::decimal_uses_smallint(logical_type) => {
                                let value = de.read_i64_varint()? as i16;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::Decimal if Self::decimal_uses_integer(logical_type) => {
                                let value = de.read_i64_varint()? as i32;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::Decimal if Self::decimal_uses_bigint(logical_type) => {
                                let value = de.read_i64_varint()?;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::Decimal if Self::decimal_uses_hugeint(logical_type) => {
                                let value = de.read_i128_varint()?;
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::Float => {
                                let value = de.read_f32();
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            LogicalTypeId::Double => {
                                let value = de.read_f64();
                                if is_min {
                                    super::NumericStats::set_min(data, value, logical_type);
                                } else {
                                    super::NumericStats::set_max(data, value, logical_type);
                                }
                            }
                            _ => {
                                let _ = de.read_varint()?;
                            }
                        }
                    }
                }
                MESSAGE_TERMINATOR_FIELD_ID => return Ok(()),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected numeric stats value field {other}"),
                    ));
                }
            }
        }
    }
}

impl fmt::Display for BaseStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[Has Null: {}, Has No Null: {}]",
            self.has_null, self.has_no_null
        )?;

        if self.distinct_count > 0 {
            write!(f, "[Approx Unique: {}]", self.distinct_count)?;
        }

        match &self.stats_data {
            StatsData::Numeric(data) => {
                write!(
                    f,
                    "{}",
                    super::NumericStats::to_string(data, &self.logical_type)
                )?;
            }
            StatsData::String(data) => {
                write!(f, "{}", super::StringStats::to_string(data))?;
            }
            StatsData::Variant(_) => {}
            StatsData::None => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_unknown() {
        let stats = BaseStatistics::create_unknown(LogicalType::integer());
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
    }

    #[test]
    fn test_create_empty() {
        let stats = BaseStatistics::create_empty(LogicalType::integer());
        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
    }

    #[test]
    fn test_merge() {
        let mut stats1 = BaseStatistics::create_empty(LogicalType::integer());
        stats1.set_has_no_null();

        let mut stats2 = BaseStatistics::create_empty(LogicalType::integer());
        stats2.set_has_null();

        stats1.merge(&stats2);
        assert!(stats1.can_have_null());
        assert!(stats1.can_have_no_null());
    }
}
