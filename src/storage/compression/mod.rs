// ============================================================
// compression/mod.rs — 压缩类型枚举及子模块
// 对应 C++: duckdb/storage/compression/compression.hpp
// ============================================================

mod rle;
mod uncompressed;

pub use uncompressed::UncompressedReader;

/// 压缩类型（对应 C++ CompressionType 枚举）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    /// 未压缩（直接存储）
    Uncompressed = 0,
    /// 常量压缩（所有值相同）
    Constant = 1,
    /// RLE 压缩（Run-Length Encoding）
    Rle = 2,
    /// Dictionary 压缩
    Dictionary = 3,
    /// Patas 压缩（浮点专用）
    Patas = 4,
    /// BitPacking 压缩（整数专用）
    BitPacking = 5,
    /// FSST 压缩（字符串专用）
    Fsst = 6,
    /// Chimp 压缩（浮点专用）
    Chimp = 7,
    /// ALP 压缩（浮点专用）
    Alp = 8,
    /// ZSTD 压缩
    Zstd = 9,
    /// AlpRd 压缩
    AlpRd = 10,
    /// 未知压缩类型
    Unknown = 255,
}

impl From<u8> for CompressionType {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Uncompressed,
            1 => Self::Constant,
            2 => Self::Rle,
            3 => Self::Dictionary,
            4 => Self::Patas,
            5 => Self::BitPacking,
            6 => Self::Fsst,
            7 => Self::Chimp,
            8 => Self::Alp,
            9 => Self::Zstd,
            10 => Self::AlpRd,
            _ => Self::Unknown,
        }
    }
}

impl From<u32> for CompressionType {
    fn from(v: u32) -> Self {
        Self::from(v as u8)
    }
}
