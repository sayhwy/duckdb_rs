use crate::db::conn::ClientContext;
use crate::planner::{
    ConjunctionAndFilter, ConjunctionAndFilterState, ConjunctionOrFilter, ConjunctionOrFilterState,
    StructFilter, TableFilter, TableFilterType,
};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

pub trait TableFilterState: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Default)]
pub struct BaseTableFilterState;

impl TableFilterState for BaseTableFilterState {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl dyn TableFilterState {
    pub fn initialize(context: &ClientContext, filter: &dyn TableFilter) -> Arc<dyn TableFilterState> {
        match filter.filter_type() {
            TableFilterType::StructExtract => {
                let struct_filter = filter
                    .as_any()
                    .downcast_ref::<StructFilter>()
                    .expect("StructExtract filter type mismatch");
                Self::initialize(context, struct_filter.child_filter.as_ref())
            }
            TableFilterType::ConjunctionOr => {
                let conjunction_filter = filter
                    .as_any()
                    .downcast_ref::<ConjunctionOrFilter>()
                    .expect("ConjunctionOr filter type mismatch");
                let mut result = ConjunctionOrFilterState::default();
                for child_filter in &conjunction_filter.child_filters {
                    result
                        .child_states
                        .push(Self::initialize(context, child_filter.as_ref()));
                }
                Arc::new(result)
            }
            TableFilterType::ConjunctionAnd => {
                let conjunction_filter = filter
                    .as_any()
                    .downcast_ref::<ConjunctionAndFilter>()
                    .expect("ConjunctionAnd filter type mismatch");
                let mut result = ConjunctionAndFilterState::default();
                for child_filter in &conjunction_filter.child_filters {
                    result
                        .child_states
                        .push(Self::initialize(context, child_filter.as_ref()));
                }
                Arc::new(result)
            }
            TableFilterType::ConstantComparison
            | TableFilterType::IsNull
            | TableFilterType::IsNotNull
            | TableFilterType::InFilter => Arc::new(BaseTableFilterState),
            _ => panic!("Unsupported filter type for TableFilterState::initialize"),
        }
    }
}
