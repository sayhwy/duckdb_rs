// ============================================================
// compression/uncompressed.rs — 未压缩列数据读取器
// 对应 C++: duckdb/storage/compression/uncompressed.hpp
// ============================================================

/// 未压缩数据读取器（直接从字节切片中读取原始数据）
pub struct UncompressedReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> UncompressedReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn read_bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return None;
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Some(slice)
    }

    pub fn read_u8(&mut self) -> Option<u8> {
        let b = self.data.get(self.pos).copied();
        if b.is_some() {
            self.pos += 1;
        }
        b
    }

    pub fn read_u16_le(&mut self) -> Option<u16> {
        let bytes = self.read_bytes(2)?;
        Some(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn read_u32_le(&mut self) -> Option<u32> {
        let bytes = self.read_bytes(4)?;
        Some(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn read_u64_le(&mut self) -> Option<u64> {
        let bytes = self.read_bytes(8)?;
        Some(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn position(&self) -> usize {
        self.pos
    }
}
