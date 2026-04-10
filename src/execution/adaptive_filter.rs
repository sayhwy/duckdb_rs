use std::time::Instant;

use crate::catalog::Value;
use crate::planner::{
    ConjunctionAndFilter, ConjunctionOrFilter, ConstantFilter, StructFilter, TableFilter,
    TableFilterSet, TableFilterType,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct AdaptiveFilterState {
    pub start_time: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct AdaptiveFilter {
    pub permutation: Vec<usize>,
    disable_permutations: bool,
    iteration_count: usize,
    swap_idx: usize,
    right_random_border: usize,
    observe_interval: usize,
    execute_interval: usize,
    runtime_sum: f64,
    prev_mean: f64,
    observe: bool,
    warmup: bool,
    swap_likeliness: Vec<usize>,
    random_state: u64,
}

impl AdaptiveFilter {
    pub fn new(table_filters: &TableFilterSet) -> Self {
        let permutation = get_initial_order(table_filters);
        let filter_count = table_filters.filter_count();
        let mut swap_likeliness = Vec::new();
        for _ in 1..filter_count {
            swap_likeliness.push(100);
        }
        Self {
            permutation,
            disable_permutations: false,
            iteration_count: 0,
            swap_idx: 0,
            right_random_border: 100usize.saturating_mul(filter_count.saturating_sub(1)),
            observe_interval: 10,
            execute_interval: 20,
            runtime_sum: 0.0,
            prev_mean: 0.0,
            observe: false,
            warmup: true,
            swap_likeliness,
            random_state: 0x9e3779b97f4a7c15,
        }
    }

    pub fn begin_filter(&self) -> AdaptiveFilterState {
        if self.permutation.len() <= 1 || self.disable_permutations {
            return AdaptiveFilterState::default();
        }
        AdaptiveFilterState {
            start_time: Some(Instant::now()),
        }
    }

    pub fn end_filter(&mut self, state: AdaptiveFilterState) {
        if self.permutation.len() <= 1 || self.disable_permutations {
            return;
        }
        let duration = state
            .start_time
            .map(|start_time| start_time.elapsed().as_secs_f64())
            .unwrap_or(0.0);
        self.adapt_runtime_statistics(duration);
    }

    fn adapt_runtime_statistics(&mut self, duration: f64) {
        self.iteration_count += 1;
        self.runtime_sum += duration;

        if !self.warmup {
            if self.observe && self.iteration_count == self.observe_interval {
                if self.prev_mean - (self.runtime_sum / self.iteration_count as f64) <= 0.0 {
                    self.permutation.swap(self.swap_idx, self.swap_idx + 1);
                    if self.swap_likeliness[self.swap_idx] > 1 {
                        self.swap_likeliness[self.swap_idx] /= 2;
                    }
                } else {
                    self.swap_likeliness[self.swap_idx] = 100;
                }
                self.observe = false;
                self.iteration_count = 0;
                self.runtime_sum = 0.0;
            } else if !self.observe && self.iteration_count == self.execute_interval {
                self.prev_mean = self.runtime_sum / self.iteration_count as f64;
                if self.right_random_border == 0 {
                    self.iteration_count = 0;
                    self.runtime_sum = 0.0;
                    return;
                }

                let random_number = self.next_random_integer(1, self.right_random_border as u32) as usize;
                self.swap_idx = random_number / 100;
                let likeliness = random_number - 100 * self.swap_idx;
                if self.swap_likeliness[self.swap_idx] > likeliness {
                    self.permutation.swap(self.swap_idx, self.swap_idx + 1);
                    self.observe = true;
                }
                self.iteration_count = 0;
                self.runtime_sum = 0.0;
            }
        } else if self.iteration_count == 5 {
            self.iteration_count = 0;
            self.runtime_sum = 0.0;
            self.observe = false;
            self.warmup = false;
        }
    }

    fn next_random_integer(&mut self, min: u32, max: u32) -> u32 {
        debug_assert!(min <= max);
        if min == max {
            return min;
        }
        self.random_state ^= self.random_state >> 12;
        self.random_state ^= self.random_state << 25;
        self.random_state ^= self.random_state >> 27;
        let value = self.random_state.wrapping_mul(2685821657736338717);
        let unit = ((value >> 11) as f64) * (1.0 / ((1u64 << 53) as f64));
        min + (unit * f64::from(max - min)) as u32
    }
}

fn get_initial_order(table_filters: &TableFilterSet) -> Vec<usize> {
    let mut filter_costs = Vec::new();
    for (filter_index, (_, filter)) in table_filters.iter().enumerate() {
        filter_costs.push((filter_index, filter_cost(filter.as_ref())));
    }
    filter_costs.sort_by_key(|(_, cost)| *cost);
    filter_costs.into_iter().map(|(idx, _)| idx).collect()
}

fn filter_cost(filter: &dyn TableFilter) -> usize {
    match filter.filter_type() {
        TableFilterType::DynamicFilter | TableFilterType::OptionalFilter => 0,
        TableFilterType::ConjunctionOr => {
            let filter = filter
                .as_any()
                .downcast_ref::<ConjunctionOrFilter>()
                .expect("conjunction or filter type mismatch");
            5 + filter
                .child_filters
                .iter()
                .map(|child| filter_cost(child.as_ref()))
                .sum::<usize>()
        }
        TableFilterType::ConjunctionAnd => {
            let filter = filter
                .as_any()
                .downcast_ref::<ConjunctionAndFilter>()
                .expect("conjunction and filter type mismatch");
            5 + filter
                .child_filters
                .iter()
                .map(|child| filter_cost(child.as_ref()))
                .sum::<usize>()
        }
        TableFilterType::ConstantComparison => {
            let filter = filter
                .as_any()
                .downcast_ref::<ConstantFilter>()
                .expect("constant filter type mismatch");
            value_cost(&filter.constant)
        }
        TableFilterType::IsNull | TableFilterType::IsNotNull => 5,
        TableFilterType::StructExtract => {
            let filter = filter
                .as_any()
                .downcast_ref::<StructFilter>()
                .expect("struct filter type mismatch");
            filter_cost(filter.child_filter.as_ref())
        }
        _ => 1000,
    }
}

fn physical_type_cost(width: usize, multiplier: usize) -> usize {
    match width {
        16 => 5 * multiplier,
        4 | 8 => 2 * multiplier,
        _ => multiplier,
    }
}

fn value_cost(value: &Value) -> usize {
    match value {
        Value::Text(_) | Value::Blob(_) => 5,
        Value::Float(_) => 2,
        Value::Integer(_) | Value::Boolean(_) | Value::Null => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::catalog::Value;
    use crate::common::enums::ExpressionType;
    use crate::planner::{ConstantFilter, TableFilterSet};

    use super::AdaptiveFilter;

    #[test]
    fn begin_filter_tracks_time_only_when_permutable() {
        let mut filters = TableFilterSet::default();
        filters.push_filter(
            0,
            Arc::new(ConstantFilter::new(
                ExpressionType::CompareEqual,
                Value::Integer(1),
            )),
        );
        let adaptive = AdaptiveFilter::new(&filters);
        assert!(adaptive.begin_filter().start_time.is_none());

        filters.push_filter(
            1,
            Arc::new(ConstantFilter::new(
                ExpressionType::CompareEqual,
                Value::Integer(2),
            )),
        );
        let adaptive = AdaptiveFilter::new(&filters);
        assert!(adaptive.begin_filter().start_time.is_some());
    }

    #[test]
    fn next_random_integer_keeps_upper_bound_exclusive() {
        let filters = TableFilterSet::default();
        let mut adaptive = AdaptiveFilter::new(&filters);
        for _ in 0..1024 {
            let value = adaptive.next_random_integer(1, 100);
            assert!((1..100).contains(&value));
        }
        assert_eq!(adaptive.next_random_integer(7, 7), 7);
    }
}
