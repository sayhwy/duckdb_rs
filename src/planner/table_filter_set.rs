use crate::planner::{ConjunctionAndFilter, ProjectionIndex, TableFilter, TableFilterType};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct TableFilterSet {
    filters: BTreeMap<ProjectionIndex, Arc<dyn TableFilter>>,
}

impl Clone for TableFilterSet {
    fn clone(&self) -> Self {
        self.copy()
    }
}

impl TableFilterSet {
    pub fn push_filter(&mut self, col_idx: ProjectionIndex, filter: Arc<dyn TableFilter>) {
        match self.filters.get_mut(&col_idx) {
            None => {
                self.filters.insert(col_idx, filter);
            }
            Some(existing) => {
                if existing.filter_type() == TableFilterType::ConjunctionAnd {
                    let and_filter = existing
                        .as_any()
                        .downcast_ref::<ConjunctionAndFilter>()
                        .expect("ConjunctionAnd filter type mismatch");
                    let mut new_filter = and_filter.clone();
                    new_filter.child_filters.push(filter);
                    *existing = Arc::new(new_filter);
                } else {
                    let mut and_filter = ConjunctionAndFilter::new();
                    and_filter.child_filters.push(existing.copy());
                    and_filter.child_filters.push(filter);
                    *existing = Arc::new(and_filter);
                }
            }
        }
    }

    pub fn has_filters(&self) -> bool {
        !self.filters.is_empty()
    }

    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    pub fn has_filter(&self, col_idx: ProjectionIndex) -> bool {
        self.filters.contains_key(&col_idx)
    }

    pub fn get_filter_by_column_index(&self, col_idx: ProjectionIndex) -> Arc<dyn TableFilter> {
        self.filters
            .get(&col_idx)
            .cloned()
            .unwrap_or_else(|| panic!("TableFilterSet::get_filter_by_column_index: missing filter"))
    }

    pub fn try_get_filter_by_column_index(
        &self,
        col_idx: ProjectionIndex,
    ) -> Option<Arc<dyn TableFilter>> {
        self.filters.get(&col_idx).cloned()
    }

    pub fn set_filter_by_column_index(&mut self, col_idx: ProjectionIndex, filter: Arc<dyn TableFilter>) {
        self.filters.insert(col_idx, filter);
    }

    pub fn remove_filter_by_column_index(&mut self, col_idx: ProjectionIndex) {
        self.filters.remove(&col_idx);
    }

    pub fn clear_filters(&mut self) {
        self.filters.clear();
    }

    pub fn equals(&self, other: &TableFilterSet) -> bool {
        if self.filters.len() != other.filters.len() {
            return false;
        }
        for (idx, filter) in &self.filters {
            let Some(other_filter) = other.filters.get(idx) else {
                return false;
            };
            if !filter.equals(other_filter.as_ref()) {
                return false;
            }
        }
        true
    }

    pub fn copy(&self) -> TableFilterSet {
        let mut result = TableFilterSet::default();
        for (idx, filter) in &self.filters {
            result.filters.insert(*idx, filter.copy());
        }
        result
    }

    pub fn iter(&self) -> impl Iterator<Item = (ProjectionIndex, &Arc<dyn TableFilter>)> {
        self.filters.iter().map(|(idx, filter)| (*idx, filter))
    }
}

#[cfg(test)]
mod tests {
    use super::TableFilterSet;
    use crate::catalog::Value;
    use crate::common::enums::ExpressionType;
    use crate::planner::{ConjunctionAndFilter, ConstantFilter};
    use std::sync::Arc;

    #[test]
    fn push_filter_ands_same_column() {
        let mut filters = TableFilterSet::default();
        filters.push_filter(
            0,
            Arc::new(ConstantFilter::new(
                ExpressionType::CompareGreaterThan,
                Value::Integer(10),
            )),
        );
        filters.push_filter(
            0,
            Arc::new(ConstantFilter::new(
                ExpressionType::CompareLessThan,
                Value::Integer(20),
            )),
        );

        let filter = filters.get_filter_by_column_index(0);
        let and_filter = filter
            .as_any()
            .downcast_ref::<ConjunctionAndFilter>()
            .expect("expected conjunction and filter");
        assert_eq!(and_filter.child_filters.len(), 2);
    }
}
