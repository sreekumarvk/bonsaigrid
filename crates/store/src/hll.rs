//! Dense HyperLogLog for the `CardinalityEstimator` data structure: p=14 (16384
//! registers), 64-bit hashes, the classic bias-corrected estimator with linear
//! counting for small cardinalities. ~0.81% standard error. One byte per register
//! (rank 0..=51), so the state is a flat 16 KiB — cheap to serialize, merge,
//! persist (via `aux_state`) and WAN-replicate.

const P: u32 = 14;
const M: usize = 1 << P; // 16384 registers

#[derive(Clone)]
pub struct Hll {
    regs: Vec<u8>,
}

impl Default for Hll {
    fn default() -> Self {
        Hll::new()
    }
}

impl Hll {
    pub fn new() -> Hll {
        Hll { regs: vec![0u8; M] }
    }

    /// Fold a 64-bit hash into the sketch.
    pub fn add(&mut self, hash: u64) {
        let idx = (hash >> (64 - P)) as usize; // top p bits → register index
        // remaining (64-p) bits sit in the high end of `w`; the guard bit bounds
        // the rank when they are all zero.
        let w = (hash << P) | (1u64 << (P - 1));
        let rank = (w.leading_zeros() + 1) as u8; // leftmost 1-bit position (1-based)
        if rank > self.regs[idx] {
            self.regs[idx] = rank;
        }
    }

    /// Estimated distinct count.
    pub fn estimate(&self) -> u64 {
        let m = M as f64;
        let mut sum = 0.0f64;
        let mut zeros = 0usize;
        for &r in &self.regs {
            sum += 2f64.powi(-(r as i32));
            if r == 0 {
                zeros += 1;
            }
        }
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let mut e = alpha * m * m / sum;
        if e <= 2.5 * m && zeros != 0 {
            // Small-range correction: linear counting over empty registers.
            e = m * (m / zeros as f64).ln();
        }
        e.round() as u64
    }

    /// Register-wise max union — used for merge/aggregation.
    pub fn merge(&mut self, other: &Hll) {
        for i in 0..M {
            if other.regs[i] > self.regs[i] {
                self.regs[i] = other.regs[i];
            }
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.regs.clone()
    }

    pub fn from_bytes(b: &[u8]) -> Hll {
        let mut regs = vec![0u8; M];
        let n = b.len().min(M);
        regs[..n].copy_from_slice(&b[..n]);
        Hll { regs }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // splitmix64: a good 64-bit mix so distinct inputs spread across registers.
    fn mix(i: u64) -> u64 {
        let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[test]
    fn estimate_within_error_bound() {
        let n = 100_000u64;
        let mut h = Hll::new();
        for i in 0..n {
            h.add(mix(i));
        }
        let est = h.estimate() as f64;
        let err = (est - n as f64).abs() / n as f64;
        assert!(err < 0.02, "HLL relative error {err:.4} for n={n} (est {est})");
    }

    #[test]
    fn small_cardinality_is_close() {
        let mut h = Hll::new();
        for i in 0..100u64 {
            h.add(mix(i));
        }
        let e = h.estimate();
        assert!((95..=105).contains(&e), "small-n estimate {e} (want ~100)");
        // duplicates do not inflate the count
        for i in 0..100u64 {
            h.add(mix(i));
        }
        assert!((95..=105).contains(&h.estimate()));
    }

    #[test]
    fn merge_unions_distinct_sets() {
        let mut a = Hll::new();
        let mut b = Hll::new();
        for i in 0..50_000u64 {
            a.add(mix(i));
        }
        for i in 25_000..75_000u64 {
            b.add(mix(i)); // overlap 25k..50k; union is 0..75k
        }
        a.merge(&b);
        let est = a.estimate() as f64;
        assert!((est - 75_000.0).abs() / 75_000.0 < 0.02, "merged estimate {est}");
    }

    #[test]
    fn bytes_roundtrip() {
        let mut h = Hll::new();
        for i in 0..1000u64 {
            h.add(mix(i));
        }
        let h2 = Hll::from_bytes(&h.to_bytes());
        assert_eq!(h.estimate(), h2.estimate());
        assert_eq!(h.to_bytes().len(), M);
    }
}
