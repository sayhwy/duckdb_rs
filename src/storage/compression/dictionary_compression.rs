use crate::function::compression_function::CompressionFunction;
use crate::storage::compression::dictionary::decompression::{
    string_fetch_row, string_init_scan, string_scan, string_scan_partial, string_select,
};
use crate::storage::table::column_segment::ColumnSegment;
use crate::storage::table::scan_state::ColumnScanState;
use crate::storage::table::types::{CompressionType, Idx, PhysicalType};

fn empty_skip(_segment: &ColumnSegment, _state: &mut ColumnScanState, _skip_count: Idx) {}

pub struct DictionaryCompressionFun;

impl DictionaryCompressionFun {
    pub fn get_function(data_type: PhysicalType) -> CompressionFunction {
        CompressionFunction::new(
            CompressionType::Dictionary,
            data_type,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(string_init_scan),
            Some(string_scan),
            Some(string_scan_partial),
            Some(string_fetch_row),
            Some(empty_skip),
            None,
            None,
            None,
            None,
            Some(string_select),
        )
    }

    pub fn type_is_supported(physical_type: PhysicalType) -> bool {
        physical_type == PhysicalType::VarChar
    }
}
