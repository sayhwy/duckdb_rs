use crate::function::compression_function::CompressionFunction;
use crate::storage::table::types::{CompressionType, PhysicalType};

pub struct EmptyValidityCompressionFun;

impl EmptyValidityCompressionFun {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        CompressionFunction::new(
            CompressionType::Empty,
            data_type,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub fn type_is_supported(physical_type: PhysicalType) -> bool {
        physical_type == PhysicalType::Bool
    }
}
