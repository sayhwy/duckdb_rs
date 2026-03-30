//! Statistics for fixed-size ARRAY types.
//!
//! Mirrors `duckdb/storage/statistics/array_stats.cpp`.
//!
//! An ARRAY type has exactly one child statistics object tracking the
//! statistics of the element type.

use super::base_statistics::{BaseStatistics, StatisticsType};
use crate::common::types::{LogicalType, LogicalTypeId, SelectionVector, Vector};

/// Statistics helper for fixed-size ARRAY types.
///
/// All methods are static, mirroring the C++ `ArrayStats` class.
pub struct ArrayStats;

impl ArrayStats {
    // ── Construct ──────────────────────────────────────────────────────────────

    /// Initialise `stats.child_stats` for an array statistics object.
    ///
    /// Mirrors `void ArrayStats::Construct(BaseStatistics &stats)`.
    ///
    /// Pushes one default-constructed child `BaseStatistics` whose type is
    /// the element type of `stats.get_type()`.
    pub fn construct(stats: &mut BaseStatistics) {
        let child_type = stats
            .get_type()
            .get_child_type()
            .cloned()
            .unwrap_or_else(LogicalType::new_invalid);
        let child = Box::new(BaseStatistics::new(child_type));
        stats.child_stats.push(child);
    }

    // ── CreateUnknown / CreateEmpty ────────────────────────────────────────────

    /// Create statistics representing *unknown* data for an array type.
    ///
    /// Mirrors `BaseStatistics ArrayStats::CreateUnknown(LogicalType type)`.
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

    /// Create statistics representing *empty* (no data) for an array type.
    ///
    /// Mirrors `BaseStatistics ArrayStats::CreateEmpty(LogicalType type)`.
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

    // ── Copy ──────────────────────────────────────────────────────────────────

    /// Copy child statistics from `other` into `stats`.
    ///
    /// Mirrors `void ArrayStats::Copy(BaseStatistics &stats, const BaseStatistics &other)`.
    pub fn copy(stats: &mut BaseStatistics, other: &BaseStatistics) {
        debug_assert!(
            !stats.child_stats.is_empty(),
            "child_stats must be initialised"
        );
        debug_assert!(
            !other.child_stats.is_empty(),
            "other.child_stats must be initialised"
        );
        *stats.child_stats[0] = (*other.child_stats[0]).clone();
    }

    // ── GetChildStats ──────────────────────────────────────────────────────────

    /// Return an immutable reference to the single child statistics.
    ///
    /// Mirrors `const BaseStatistics &ArrayStats::GetChildStats(const BaseStatistics &stats)`.
    ///
    /// # Panics
    /// Panics if `stats` is not of statistics type `ArrayStats`.
    pub fn get_child_stats(stats: &BaseStatistics) -> &BaseStatistics {
        if stats.get_stats_type() != StatisticsType::ArrayStats {
            panic!("ArrayStats::get_child_stats called on stats that is not an array");
        }
        debug_assert!(
            !stats.child_stats.is_empty(),
            "child_stats must be initialised"
        );
        &stats.child_stats[0]
    }

    /// Return a mutable reference to the single child statistics.
    ///
    /// Mirrors `BaseStatistics &ArrayStats::GetChildStats(BaseStatistics &stats)`.
    ///
    /// # Panics
    /// Panics if `stats` is not of statistics type `ArrayStats`.
    pub fn get_child_stats_mut(stats: &mut BaseStatistics) -> &mut BaseStatistics {
        if stats.get_stats_type() != StatisticsType::ArrayStats {
            panic!("ArrayStats::get_child_stats_mut called on stats that is not an array");
        }
        debug_assert!(
            !stats.child_stats.is_empty(),
            "child_stats must be initialised"
        );
        &mut stats.child_stats[0]
    }

    // ── SetChildStats ──────────────────────────────────────────────────────────

    /// Replace the child statistics, resetting to *unknown* when `new_stats` is `None`.
    ///
    /// Mirrors `void ArrayStats::SetChildStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats)`.
    pub fn set_child_stats(stats: &mut BaseStatistics, new_stats: Option<BaseStatistics>) {
        let replacement = match new_stats {
            None => {
                let child_type = stats
                    .get_type()
                    .get_child_type()
                    .cloned()
                    .unwrap_or_else(LogicalType::new_invalid);
                BaseStatistics::create_unknown(child_type)
            }
            Some(s) => s,
        };
        *stats.child_stats[0] = replacement;
    }

    // ── Merge ─────────────────────────────────────────────────────────────────

    /// Merge `other` into `stats` by merging their child statistics.
    ///
    /// Mirrors `void ArrayStats::Merge(BaseStatistics &stats, const BaseStatistics &other)`.
    pub fn merge(stats: &mut BaseStatistics, other: &BaseStatistics) {
        // Skip validity pseudo-type (C++: `if (other.GetType().id() == LogicalTypeId::VALIDITY) return`)
        if other.get_type().id == LogicalTypeId::Validity {
            return;
        }

        // Clone to avoid simultaneous mutable/immutable borrow of stats
        let other_child = (*Self::get_child_stats(other)).clone();
        Self::get_child_stats_mut(stats).merge(&other_child);
    }

    // ── Serialize / Deserialize ────────────────────────────────────────────────

    /// Serialize child statistics.
    ///
    /// Mirrors `void ArrayStats::Serialize(const BaseStatistics &stats, Serializer &serializer)`.
    ///
    /// C++ writes property tag 200 (`"child_stats"`) via the abstract `Serializer`.
    /// A full implementation requires a property-based serializer (not yet available
    /// in this Rust port); call sites should use the checkpoint serializer instead.
    pub fn serialize<W: crate::common::serializer::WriteStream>(
        stats: &BaseStatistics,
        _serializer: &mut W,
    ) {
        // C++: serializer.WriteProperty(200, "child_stats", child_stats)
        let _child = Self::get_child_stats(stats);
        unimplemented!("ArrayStats::serialize requires a property-based serializer")
    }

    /// Deserialize child statistics.
    ///
    /// Mirrors `void ArrayStats::Deserialize(Deserializer &deserializer, BaseStatistics &base)`.
    ///
    /// C++ reads property tag 200 (`"child_stats"`) and pushes the logical type
    /// of the child into the deserialization context.
    pub fn deserialize<R: crate::common::serializer::ReadStream>(
        _deserializer: &mut R,
        base: &mut BaseStatistics,
    ) {
        debug_assert!(
            base.get_type().id == LogicalTypeId::Array,
            "ArrayStats::deserialize called on non-array stats"
        );
        // C++:
        //   deserializer.Set<const LogicalType &>(child_type);
        //   base.child_stats[0].Copy(deserializer.ReadProperty<BaseStatistics>(200, "child_stats"));
        //   deserializer.Unset<LogicalType>();
        unimplemented!("ArrayStats::deserialize requires a property-based deserializer")
    }

    // ── ToString ──────────────────────────────────────────────────────────────

    /// Return a human-readable string for the array statistics.
    ///
    /// Mirrors `string ArrayStats::ToString(const BaseStatistics &stats)`.
    ///
    /// Format: `"[<child_stats>]"`
    pub fn to_string(stats: &BaseStatistics) -> String {
        let child = Self::get_child_stats(stats);
        format!("[{}]", child)
    }

    // ── Verify ────────────────────────────────────────────────────────────────

    /// Verify that `vector` matches the array statistics.
    ///
    /// Mirrors `void ArrayStats::Verify(const BaseStatistics &stats, Vector &vector,
    ///                                   const SelectionVector &sel, idx_t count)`.
    ///
    /// Algorithm (matching C++):
    /// 1. Count valid (non-NULL) arrays.
    /// 2. Build a flat child selection vector that covers all elements of those
    ///    valid arrays (`valid_count × array_size` entries).
    /// 3. Verify child statistics against the child vector.
    ///
    /// Note: This implementation handles flat vectors. Dictionary/constant
    /// vectors require `Vector::to_unified_format()` which is not yet implemented.
    pub fn verify(stats: &BaseStatistics, vector: &Vector, sel: &SelectionVector, count: usize) {
        let child_stats = Self::get_child_stats(stats);
        let array_size = vector.get_type().get_array_size();

        // ── Step 1: count valid arrays ─────────────────────────────────────────
        // C++: UnifiedVectorFormat vdata; vector.ToUnifiedFormat(count, vdata);
        //      for each i: index = vdata.sel->get_index(sel.get_index(i));
        //                  if vdata.validity.RowIsValid(index) → valid_count++
        //
        // Simplified for flat vectors: vdata.sel is the identity mapping.
        let mut valid_count: usize = 0;
        for i in 0..count {
            let idx = sel.get_index(i);
            if vector.validity.row_is_valid(idx) {
                valid_count += 1;
            }
        }

        // ── Step 2: build child selection vector ───────────────────────────────
        // C++: SelectionVector element_sel(valid_count * array_size);
        //      for each valid array at index `index`:
        //          offset = index * array_size
        //          for elem_idx in 0..array_size: element_sel[element_count++] = offset + elem_idx
        let total_elements = valid_count * array_size;
        let mut element_indices: Vec<u32> = Vec::with_capacity(total_elements);
        for i in 0..count {
            let idx = sel.get_index(i);
            let offset = idx * array_size;
            if vector.validity.row_is_valid(idx) {
                for elem_idx in 0..array_size {
                    element_indices.push((offset + elem_idx) as u32);
                }
            }
        }
        let element_sel = SelectionVector {
            indices: element_indices,
        };
        let element_count = element_sel.len();

        // ── Step 3: verify child stats ─────────────────────────────────────────
        // C++: child_stats.Verify(ArrayVector::GetEntry(vector), element_sel, element_count)
        if let Some(child_entry) = vector.get_child() {
            let _ = child_stats.verify(child_entry, &element_sel, element_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{LogicalType, LogicalTypeId};

    fn array_type() -> LogicalType {
        LogicalType::array(LogicalType::integer(), 4)
    }

    #[test]
    fn test_create_unknown() {
        let stats = ArrayStats::create_unknown(array_type());
        assert!(stats.can_have_null());
        assert!(stats.can_have_no_null());
        assert_eq!(stats.child_stats.len(), 1);
        let child = ArrayStats::get_child_stats(&stats);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_create_empty() {
        let stats = ArrayStats::create_empty(array_type());
        assert!(!stats.can_have_null());
        assert!(!stats.can_have_no_null());
        assert_eq!(stats.child_stats.len(), 1);
        let child = ArrayStats::get_child_stats(&stats);
        assert!(!child.can_have_null());
        assert!(!child.can_have_no_null());
    }

    #[test]
    fn test_construct() {
        let mut stats = BaseStatistics::new(array_type());
        assert!(stats.child_stats.is_empty());
        ArrayStats::construct(&mut stats);
        assert_eq!(stats.child_stats.len(), 1);
    }

    #[test]
    fn test_copy() {
        let mut dst = ArrayStats::create_empty(array_type());
        let src = ArrayStats::create_unknown(array_type());
        ArrayStats::copy(&mut dst, &src);
        let child = ArrayStats::get_child_stats(&dst);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_set_child_stats_some() {
        let mut stats = ArrayStats::create_empty(array_type());
        let new_child = BaseStatistics::create_unknown(LogicalType::integer());
        ArrayStats::set_child_stats(&mut stats, Some(new_child));
        let child = ArrayStats::get_child_stats(&stats);
        assert!(child.can_have_null());
    }

    #[test]
    fn test_set_child_stats_none_resets_to_unknown() {
        let mut stats = ArrayStats::create_empty(array_type());
        ArrayStats::set_child_stats(&mut stats, None);
        let child = ArrayStats::get_child_stats(&stats);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_merge_skips_validity() {
        let mut stats = ArrayStats::create_empty(array_type());
        let validity_stats = BaseStatistics::create_empty(LogicalType::validity());
        // Should not panic
        ArrayStats::merge(&mut stats, &validity_stats);
    }

    #[test]
    fn test_merge() {
        let mut stats = ArrayStats::create_empty(array_type());
        {
            let child = ArrayStats::get_child_stats_mut(&mut stats);
            child.set_has_no_null();
        }
        let mut other = ArrayStats::create_empty(array_type());
        {
            let child = ArrayStats::get_child_stats_mut(&mut other);
            child.set_has_null();
        }
        ArrayStats::merge(&mut stats, &other);
        let child = ArrayStats::get_child_stats(&stats);
        assert!(child.can_have_null());
        assert!(child.can_have_no_null());
    }

    #[test]
    fn test_to_string() {
        let stats = ArrayStats::create_empty(array_type());
        let s = ArrayStats::to_string(&stats);
        assert!(s.starts_with('[') && s.ends_with(']'));
    }

    #[test]
    #[should_panic(expected = "not an array")]
    fn test_get_child_stats_panics_on_non_array() {
        let stats = BaseStatistics::create_empty(LogicalType::integer());
        ArrayStats::get_child_stats(&stats);
    }
}
