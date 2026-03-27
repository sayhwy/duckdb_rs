use std::sync::atomic::{AtomicU64, Ordering};

/// Sample rate for integral types (higher sampling rate)
const INTEGRAL_SAMPLE_RATE: f64 = 0.1;

/// Base sample rate for other types
const BASE_SAMPLE_RATE: f64 = 0.01;

/// Standard vector size (from DuckDB)
const STANDARD_VECTOR_SIZE: usize = 2048;

/// HyperLogLog implementation for cardinality estimation
/// This is a simplified version - in production, use a proper HLL implementation
#[derive(Debug, Clone)]
struct HyperLogLog {
    registers: Vec<u8>,
    precision: u8,
}

impl HyperLogLog {
    fn new() -> Self {
        let precision = 14; // 2^14 = 16384 registers
        let m = 1 << precision;
        Self {
            registers: vec![0; m],
            precision,
        }
    }

    fn add(&mut self, hash: u64) {
        let m = 1 << self.precision;
        let j = (hash & ((m - 1) as u64)) as usize;
        let w = hash >> self.precision;
        let leading_zeros = w.leading_zeros() as u8 + 1;
        self.registers[j] = self.registers[j].max(leading_zeros);
    }

    fn count(&self) -> u64 {
        let m = self.registers.len() as f64;
        let alpha = match self.precision {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        let raw_estimate = {
            let sum: f64 = self.registers.iter().map(|&r| 2.0_f64.powi(-(r as i32))).sum();
            alpha * m * m / sum
        };

        // Small range correction
        if raw_estimate <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros != 0 {
                return (m * (m / zeros as f64).ln()) as u64;
            }
        }

        // Large range correction
        if raw_estimate > (1_u64 << 32) as f64 / 30.0 {
            let two_32 = (1_u64 << 32) as f64;
            return (-two_32 * (1.0 - raw_estimate / two_32).ln()) as u64;
        }

        raw_estimate as u64
    }

    fn merge(&mut self, other: &HyperLogLog) {
        for (i, &other_val) in other.registers.iter().enumerate() {
            self.registers[i] = self.registers[i].max(other_val);
        }
    }

    fn copy(&self) -> HyperLogLog {
        HyperLogLog {
            registers: self.registers.clone(),
            precision: self.precision,
        }
    }
}

/// Distinct statistics using HyperLogLog for cardinality estimation
#[derive(Debug)]
pub struct DistinctStatistics {
    /// HyperLogLog data structure
    log: HyperLogLog,
    /// Number of sampled values
    sample_count: AtomicU64,
    /// Total number of values seen
    total_count: AtomicU64,
}

impl DistinctStatistics {
    /// Create a new DistinctStatistics
    pub fn new() -> Self {
        Self {
            log: HyperLogLog::new(),
            sample_count: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
        }
    }

    /// Copy the statistics
    pub fn copy(&self) -> Self {
        Self {
            log: self.log.copy(),
            sample_count: AtomicU64::new(self.sample_count.load(Ordering::Relaxed)),
            total_count: AtomicU64::new(self.total_count.load(Ordering::Relaxed)),
        }
    }

    /// Merge another DistinctStatistics into this one
    pub fn merge(&mut self, other: &DistinctStatistics) {
        self.log.merge(&other.log);
        self.sample_count.fetch_add(other.sample_count.load(Ordering::Relaxed), Ordering::Relaxed);
        self.total_count.fetch_add(other.total_count.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    /// Update with a sample of new data
    pub fn update_sample(&mut self, hashes: &[u64], count: usize, is_integral: bool) {
        self.total_count.fetch_add(count as u64, Ordering::Relaxed);

        let sample_rate = if is_integral {
            INTEGRAL_SAMPLE_RATE
        } else {
            BASE_SAMPLE_RATE
        };

        // Sample up to sample_rate * STANDARD_VECTOR_SIZE (at least 1)
        let sample_size = ((sample_rate * STANDARD_VECTOR_SIZE as f64) as usize)
            .max(1)
            .min(count);

        self.update_internal(&hashes[..sample_size]);
    }

    /// Update with all data (no sampling)
    pub fn update(&mut self, hashes: &[u64], count: usize) {
        self.total_count.fetch_add(count as u64, Ordering::Relaxed);
        self.update_internal(&hashes[..count]);
    }

    /// Internal update method
    fn update_internal(&mut self, hashes: &[u64]) {
        self.sample_count.fetch_add(hashes.len() as u64, Ordering::Relaxed);
        for &hash in hashes {
            self.log.add(hash);
        }
    }

    /// Get the estimated distinct count
    pub fn get_count(&self) -> u64 {
        let sample_count = self.sample_count.load(Ordering::Relaxed);
        let total_count = self.total_count.load(Ordering::Relaxed);

        if sample_count == 0 || total_count == 0 {
            return 0;
        }

        let u = self.log.count().min(sample_count) as f64;
        let s = sample_count as f64;
        let n = total_count as f64;

        // Assume this proportion of the sampled values occurred only once
        let u1 = (u / s).powi(2) * u;

        // Estimate total uniques using Good-Turing Estimation
        let estimate = (u + u1 / s * (n - s)) as u64;
        estimate.min(total_count)
    }

    /// Check if the type is supported for distinct statistics
    pub fn type_is_supported(type_id: &crate::common::types::LogicalTypeId) -> bool {
        use crate::common::types::LogicalTypeId;

        match type_id {
            // Not supported: nested types
            LogicalTypeId::List | LogicalTypeId::Struct | LogicalTypeId::Map => false,
            // Not supported: doesn't make sense
            LogicalTypeId::Boolean => false,
            // Supported: all other types
            _ => true,
        }
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        format!("[Approx Unique: {}]", self.get_count())
    }
}

impl Default for DistinctStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for DistinctStatistics {
    fn clone(&self) -> Self {
        self.copy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distinct_statistics_new() {
        let stats = DistinctStatistics::new();
        assert_eq!(stats.get_count(), 0);
    }

    #[test]
    fn test_update() {
        let mut stats = DistinctStatistics::new();

        // Simulate hashes for values 1, 2, 3, 1, 2, 3
        let hashes = vec![1u64, 2, 3, 1, 2, 3];
        stats.update(&hashes, hashes.len());

        let count = stats.get_count();
        // Should estimate around 3 distinct values
        assert!(count > 0);
    }

    #[test]
    fn test_merge() {
        let mut stats1 = DistinctStatistics::new();
        let mut stats2 = DistinctStatistics::new();

        let hashes1 = vec![1u64, 2, 3];
        let hashes2 = vec![4u64, 5, 6];

        stats1.update(&hashes1, hashes1.len());
        stats2.update(&hashes2, hashes2.len());

        stats1.merge(&stats2);

        let count = stats1.get_count();
        // Should estimate around 6 distinct values
        assert!(count > 0);
    }

    #[test]
    fn test_copy() {
        let mut stats = DistinctStatistics::new();
        let hashes = vec![1u64, 2, 3];
        stats.update(&hashes, hashes.len());

        let copied = stats.copy();
        assert_eq!(stats.get_count(), copied.get_count());
    }
}
