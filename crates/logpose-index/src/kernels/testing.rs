//! Deterministic helpers shared by the kernel and quantizer tests.

/// Lengths that straddle every lane width (4, 8, 16) and unroll factor.
pub(crate) const TEST_LENGTHS: &[usize] = &[
    0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129, 255, 384,
    767, 768, 769, 1024, 1536, 1537,
];

/// SplitMix64: tiny, fast, and reproducible across platforms.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub(crate) fn unit(&mut self) -> f32 {
        // 24 random mantissa bits map exactly onto f32.
        (self.next_u64() >> 40) as f32 / (1_u64 << 24) as f32
    }

    /// Uniform in `[-1, 1)`.
    pub(crate) fn signed(&mut self) -> f32 {
        self.unit() * 2.0 - 1.0
    }

    /// Approximately standard normal (sum of uniforms).
    pub(crate) fn gaussian(&mut self) -> f32 {
        (0..4).map(|_| self.signed()).sum::<f32>() * 0.866
    }

    pub(crate) fn vector(&mut self, len: usize) -> Vec<f32> {
        (0..len).map(|_| self.signed()).collect()
    }

    /// Small integers in `[-4, 4]`; their dot products and distances are exact
    /// in f32 regardless of summation order.
    pub(crate) fn integer_vector(&mut self, len: usize) -> Vec<f32> {
        (0..len)
            .map(|_| (self.next_u64() % 9) as f32 - 4.0)
            .collect()
    }
}

/// Asserts `actual` is within reordering error of `expected`.
///
/// `magnitude` is the sum of the absolute values of the summed terms, the
/// natural scale of rounding error for a reduction. Random-signed reductions
/// accumulate error roughly with `sqrt(len)`; the factor leaves headroom.
/// Exactness of tail handling is covered separately with integer inputs.
pub(crate) fn assert_close(actual: f32, expected: f32, magnitude: f32, len: usize) {
    let tolerance = 8.0 * (len.max(1) as f32).sqrt() * f32::EPSILON * magnitude;
    assert!(
        (actual - expected).abs() <= tolerance,
        "len {len}: actual {actual} expected {expected} (tolerance {tolerance})"
    );
}
