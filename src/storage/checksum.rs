// ============================================================
// checksum.rs — DuckDB checksum 算法
// 对应 C++: duckdb/common/checksum.hpp / checksum.cpp
// ============================================================

/// 对应 C++ Checksum(uint64_t x)
fn checksum_u64(x: u64) -> u64 {
    x.wrapping_mul(0xbf58476d1ce4e5b9)
}

/// 对应 C++ ChecksumRemainder(void *ptr, size_t len)
/// 使用类似 robin_hood hash 的算法处理剩余字节
fn checksum_remainder(data: &[u8]) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const SEED: u64 = 0xe17a1465;
    const R: u32 = 47;

    let len = data.len();
    let n_blocks = len / 8;

    let mut h = SEED ^ ((len as u64).wrapping_mul(M));

    // 处理完整的 8 字节块
    for i in 0..n_blocks {
        let mut k = u64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());

        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h ^= k;
        h = h.wrapping_mul(M);
    }

    // 处理剩余字节
    let remainder = &data[n_blocks * 8..];
    for (i, &byte) in remainder.iter().enumerate() {
        let shift = (i * 8) as u64;
        h ^= (byte as u64) << shift;
        if i == remainder.len() - 1 {
            h = h.wrapping_mul(M);
        }
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;

    h
}

/// 对应 C++ Checksum(uint8_t *buffer, size_t size)
///
/// DuckDB 的 checksum 算法：
/// 1. 初始化 result = 5381
/// 2. 对每个完整的 u64 进行 XOR checksum
/// 3. 对剩余字节使用 remainder hash
pub fn checksum(buffer: &[u8]) -> u64 {
    let size = buffer.len();
    let n_u64 = size / 8;

    let mut result: u64 = 5381;

    // 处理完整的 8 字节块
    for i in 0..n_u64 {
        let val = u64::from_le_bytes(buffer[i * 8..i * 8 + 8].try_into().unwrap());
        result ^= checksum_u64(val);
    }

    // 处理剩余字节
    let remainder = size - n_u64 * 8;
    if remainder > 0 {
        result ^= checksum_remainder(&buffer[n_u64 * 8..]);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_basic() {
        // 空数据
        assert_eq!(checksum(&[]), 5381);

        // 单个字节
        let data = [0u8; 8];
        let c = checksum(&data);
        assert_ne!(c, 0);
    }

    #[test]
    fn test_checksum_deterministic() {
        let data: Vec<u8> = (0..=255).collect();
        let c1 = checksum(&data);
        let c2 = checksum(&data);
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_checksum_different() {
        let data1 = [1u8; 256];
        let data2 = [2u8; 256];
        assert_ne!(checksum(&data1), checksum(&data2));
    }
}