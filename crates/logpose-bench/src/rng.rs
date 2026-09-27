//! Small deterministic random number generator.
//!
//! The harness implements SplitMix64 locally instead of depending on `rand` so
//! that a seed produces byte-identical datasets across dependency upgrades.
//! Baselines committed today must stay reproducible for later phases.

/// SplitMix64 generator (Steele, Lea, and Flood 2014).
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

impl SplitMix64 {
    /// Create a generator from a seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Derive an independent generator for a named stream of this seed.
    ///
    /// Streams let the dataset keep cluster centers, base rows, and queries
    /// stable when an unrelated size parameter changes.
    #[must_use]
    pub fn stream(seed: u64, stream: u64) -> Self {
        let mut mixer = Self::new(seed ^ stream.wrapping_mul(GOLDEN_GAMMA).rotate_left(17));
        Self::new(mixer.next_u64())
    }

    /// Return the next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN_GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Return a uniform value in `[0, 1)` with 53 bits of precision.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    /// Return a uniform integer in `[0, bound)`. Returns 0 when `bound` is 0.
    pub fn below(&mut self, bound: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(bound)) >> 64) as u64
    }

    /// Return a standard normal sample using the Box-Muller transform.
    pub fn gaussian(&mut self) -> f64 {
        // 1 - u keeps the logarithm argument in (0, 1].
        let radius = (-2.0 * (1.0 - self.next_f64()).ln()).sqrt();
        let angle = std::f64::consts::TAU * self.next_f64();
        radius * angle.cos()
    }

    /// Shuffle a slice in place with Fisher-Yates.
    pub fn shuffle<T>(&mut self, values: &mut [T]) {
        for index in (1..values.len()).rev() {
            let other = self.below(index as u64 + 1) as usize;
            values.swap(index, other);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SplitMix64;

    #[test]
    fn same_seed_produces_same_sequence() {
        let mut left = SplitMix64::new(42);
        let mut right = SplitMix64::new(42);
        for _ in 0..64 {
            assert_eq!(left.next_u64(), right.next_u64());
        }
    }

    #[test]
    fn matches_reference_splitmix64_output() {
        // Reference values for seed 0 from the published SplitMix64 algorithm.
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
    }

    #[test]
    fn streams_are_distinct() {
        let mut first = SplitMix64::stream(7, 1);
        let mut second = SplitMix64::stream(7, 2);
        assert_ne!(first.next_u64(), second.next_u64());
    }

    #[test]
    fn below_stays_in_range_and_shuffle_is_permutation() {
        let mut rng = SplitMix64::new(3);
        for bound in [1_u64, 2, 7, 1000] {
            for _ in 0..100 {
                assert!(rng.below(bound) < bound);
            }
        }
        let mut values = (0..100_u32).collect::<Vec<_>>();
        rng.shuffle(&mut values);
        let mut sorted = values.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..100_u32).collect::<Vec<_>>());
        assert_ne!(values, sorted);
    }

    #[test]
    fn gaussian_has_plausible_moments() {
        let mut rng = SplitMix64::new(11);
        let samples = (0..20_000).map(|_| rng.gaussian()).collect::<Vec<_>>();
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance = samples
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / samples.len() as f64;
        assert!(mean.abs() < 0.05, "mean {mean}");
        assert!((variance - 1.0).abs() < 0.05, "variance {variance}");
    }
}
