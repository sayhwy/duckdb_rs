// ============================================================
// compression/rle.rs — RLE 压缩读取器（桩实现）
// 对应 C++: duckdb/storage/compression/rle.hpp
// ============================================================

/// RLE（Run-Length Encoding）压缩读取器桩
///
/// 实际 RLE 解压需要配合 ColumnSegment 上下文，此处仅提供类型占位。
pub struct RleReader;

impl RleReader {
    pub fn new() -> Self {
        Self
    }
}
