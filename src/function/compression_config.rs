use std::sync::OnceLock;

use crate::function::compression_function::{CompressionFunction, CompressionFunctionSet};
use crate::storage::compression::bitpacking::BitpackingFun;
use crate::storage::compression::dictionary_compression::DictionaryCompressionFun;
use crate::storage::compression::empty_validity::EmptyValidityCompressionFun;
use crate::storage::compression::fsst::FsstFun;
use crate::storage::compression::numeric_constant::ConstantFun;
use crate::storage::compression::uncompressed::UncompressedFun;
use crate::storage::table::types::{CompressionType, PhysicalType};

static COMPRESSION_FUNCTION_SET: OnceLock<CompressionFunctionSet> = OnceLock::new();

fn build_function_set() -> CompressionFunctionSet {
    let mut function_set = CompressionFunctionSet::new();
    let physical_types = [
        PhysicalType::Bool,
        PhysicalType::Bit,
        PhysicalType::Int8,
        PhysicalType::Int16,
        PhysicalType::Int32,
        PhysicalType::Int64,
        PhysicalType::Int128,
        PhysicalType::Uint8,
        PhysicalType::Uint16,
        PhysicalType::Uint32,
        PhysicalType::Uint64,
        PhysicalType::Float,
        PhysicalType::Double,
        PhysicalType::VarChar,
        PhysicalType::List,
        PhysicalType::Struct,
        PhysicalType::Array,
    ];

    for physical_type in physical_types {
        if ConstantFun::type_is_supported(physical_type) {
            function_set.register_function(ConstantFun::get_function(physical_type));
        }
        if UncompressedFun::type_is_supported(physical_type) {
            function_set.register_function(UncompressedFun::get_function(physical_type));
        }
        if BitpackingFun::type_is_supported(physical_type) {
            function_set.register_function(BitpackingFun::get_function(physical_type));
        }
        if DictionaryCompressionFun::type_is_supported(physical_type) {
            function_set.register_function(DictionaryCompressionFun::get_function(physical_type));
        }
        if FsstFun::type_is_supported(physical_type) {
            function_set.register_function(FsstFun::get_function(physical_type));
        }
        if EmptyValidityCompressionFun::type_is_supported(physical_type) {
            function_set.register_function(EmptyValidityCompressionFun::get_function(physical_type));
        }
    }
    function_set
}

pub fn get_compression_functions(physical_type: PhysicalType) -> &'static [CompressionFunction] {
    COMPRESSION_FUNCTION_SET
        .get_or_init(build_function_set)
        .get_compression_functions(physical_type)
}

pub fn get_compression_function(
    compression_type: CompressionType,
    physical_type: PhysicalType,
) -> &'static CompressionFunction {
    COMPRESSION_FUNCTION_SET
        .get_or_init(build_function_set)
        .get_compression_function(compression_type, physical_type)
        .unwrap_or_else(|| {
            panic!(
                "could not find compression function {:?} for physical type {:?}",
                compression_type, physical_type
            )
        })
}
