#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DuckdbFsstDecoder {
    pub version: u64,
    pub zero_terminated: u8,
    pub len: [u8; 255],
    pub symbol: [u64; 255],
}

impl Default for DuckdbFsstDecoder {
    fn default() -> Self {
        Self {
            version: 0,
            zero_terminated: 0,
            len: [0; 255],
            symbol: [0; 255],
        }
    }
}

unsafe extern "C" {
    fn duckdb_rs_fsst_import(decoder: *mut DuckdbFsstDecoder, buf: *mut u8) -> u32;
    fn duckdb_rs_fsst_decompress(
        decoder: *mut DuckdbFsstDecoder,
        len_in: usize,
        str_in: *const u8,
        out_size: usize,
        output: *mut u8,
    ) -> usize;
}

pub fn import_symbol_table(symbol_table: &mut DuckdbFsstDecoder, bytes: &mut [u8]) -> u32 {
    unsafe { duckdb_rs_fsst_import(symbol_table as *mut _, bytes.as_mut_ptr()) }
}

pub fn decompress_value(
    decoder: &mut DuckdbFsstDecoder,
    compressed: &[u8],
    output: &mut [u8],
) -> usize {
    unsafe {
        duckdb_rs_fsst_decompress(
            decoder as *mut _,
            compressed.len(),
            compressed.as_ptr(),
            output.len(),
            output.as_mut_ptr(),
        )
    }
}
