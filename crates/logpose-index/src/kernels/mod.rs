//! Runtime-dispatched SIMD distance kernels.
//!
//! Every kernel picks the widest instruction set the CPU supports at runtime
//! (AVX-512, AVX2 with FMA, NEON, or a scalar fallback) through [`pulp`]'s safe
//! API, so the crate keeps `unsafe_code = "forbid"`. Inputs of any length are
//! handled; the ragged tail is processed with a masked partial load.
//!
//! The SIMD kernels reorder floating-point additions (several independent
//! accumulators, lane-wise partial sums, fused multiply-add), so results differ
//! from the sequential [`scalar`] reference kernels by rounding error only.
//!
//! Metric conventions:
//!
//! - [`dot`] is a similarity: larger is closer.
//! - [`l2_squared`] is a distance: smaller is closer. Ranking by squared
//!   distance is equivalent to ranking by distance and skips the square root.
//! - Cosine similarity is [`dot`] over vectors normalized with
//!   [`normalize_in_place`].

pub mod scalar;

#[cfg(test)]
pub(crate) mod testing;

use pulp::{Arch, Simd, WithSimd};
use std::sync::LazyLock;

static ARCH: LazyLock<Arch> = LazyLock::new(Arch::new);

/// Returns the detected instruction-set dispatcher.
#[inline]
pub(crate) fn arch() -> Arch {
    *ARCH
}

/// Name of the instruction set the kernels dispatch to on this machine.
#[must_use]
pub fn simd_level() -> &'static str {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        match arch() {
            Arch::V4(_) => "x86-64-v4 (AVX-512)",
            Arch::V3(_) => "x86-64-v3 (AVX2+FMA)",
            _ => "scalar",
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        match arch() {
            Arch::Scalar => "scalar",
            _ => "aarch64 (NEON)",
        }
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
    {
        "portable"
    }
}

/// Inner product of `a` and `b`.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
#[must_use]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "dot: length mismatch");
    arch().dispatch(Dot { a, b })
}

/// Squared Euclidean distance between `a` and `b`.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
#[must_use]
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "l2_squared: length mismatch");
    arch().dispatch(L2Squared { a, b })
}

/// Euclidean norm of `a`.
#[inline]
#[must_use]
pub fn norm(a: &[f32]) -> f32 {
    dot(a, a).sqrt()
}

/// Scales `a` to unit length in place.
///
/// Returns `false` and leaves `a` unchanged when its norm is zero or not
/// finite (the vector contains NaN or infinity, or its squared norm
/// overflows).
pub fn normalize_in_place(a: &mut [f32]) -> bool {
    let length = norm(a);
    if length == 0.0 || !length.is_finite() {
        return false;
    }
    let inverse = 1.0 / length;
    arch().dispatch(Scale {
        values: a,
        factor: inverse,
    });
    true
}

/// Inner product of `query` with each `dims`-wide row of the row-major
/// `matrix`, written to `out` (one slot per row).
///
/// # Panics
///
/// Panics if `query.len() != dims` or `matrix.len() != dims * out.len()`.
pub fn dot_many(query: &[f32], matrix: &[f32], dims: usize, out: &mut [f32]) {
    scalar::check_many_shape(query, matrix, dims, out.len());
    if dims == 0 {
        out.fill(0.0);
        return;
    }
    arch().dispatch(DotMany {
        query,
        matrix,
        dims,
        out,
    });
}

/// Squared Euclidean distance from `query` to each `dims`-wide row of the
/// row-major `matrix`, written to `out` (one slot per row).
///
/// # Panics
///
/// Panics if `query.len() != dims` or `matrix.len() != dims * out.len()`.
pub fn l2_squared_many(query: &[f32], matrix: &[f32], dims: usize, out: &mut [f32]) {
    scalar::check_many_shape(query, matrix, dims, out.len());
    if dims == 0 {
        out.fill(0.0);
        return;
    }
    arch().dispatch(L2SquaredMany {
        query,
        matrix,
        dims,
        out,
    });
}

struct Dot<'a> {
    a: &'a [f32],
    b: &'a [f32],
}

impl WithSimd for Dot<'_> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> f32 {
        dot_simd(simd, self.a, self.b)
    }
}

struct L2Squared<'a> {
    a: &'a [f32],
    b: &'a [f32],
}

impl WithSimd for L2Squared<'_> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> f32 {
        l2_squared_simd(simd, self.a, self.b)
    }
}

struct DotMany<'a> {
    query: &'a [f32],
    matrix: &'a [f32],
    dims: usize,
    out: &'a mut [f32],
}

impl WithSimd for DotMany<'_> {
    type Output = ();

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) {
        for (row, slot) in self.matrix.chunks_exact(self.dims).zip(self.out) {
            *slot = dot_simd(simd, self.query, row);
        }
    }
}

struct L2SquaredMany<'a> {
    query: &'a [f32],
    matrix: &'a [f32],
    dims: usize,
    out: &'a mut [f32],
}

impl WithSimd for L2SquaredMany<'_> {
    type Output = ();

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) {
        for (row, slot) in self.matrix.chunks_exact(self.dims).zip(self.out) {
            *slot = l2_squared_simd(simd, self.query, row);
        }
    }
}

struct Scale<'a> {
    values: &'a mut [f32],
    factor: f32,
}

impl WithSimd for Scale<'_> {
    type Output = ();

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) {
        let factor = simd.splat_f32s(self.factor);
        let (head, tail) = S::as_mut_simd_f32s(self.values);
        for value in head {
            *value = simd.mul_f32s(*value, factor);
        }
        for value in tail {
            *value *= self.factor;
        }
    }
}

/// Four independent accumulators hide FMA latency on every target.
pub(crate) const UNROLL: usize = 4;

#[inline(always)]
fn dot_simd<S: Simd>(simd: S, a: &[f32], b: &[f32]) -> f32 {
    let (a_head, a_tail) = S::as_simd_f32s(a);
    let (b_head, b_tail) = S::as_simd_f32s(b);
    let mut acc = [simd.splat_f32s(0.0); UNROLL];

    let (a_blocks, a_rest) = a_head.as_chunks::<UNROLL>();
    let (b_blocks, b_rest) = b_head.as_chunks::<UNROLL>();
    for (x, y) in a_blocks.iter().zip(b_blocks) {
        for lane in 0..UNROLL {
            acc[lane] = simd.mul_add_e_f32s(x[lane], y[lane], acc[lane]);
        }
    }
    for (slot, (x, y)) in acc.iter_mut().zip(a_rest.iter().zip(b_rest)) {
        *slot = simd.mul_add_e_f32s(*x, *y, *slot);
    }
    if !a_tail.is_empty() {
        let x = simd.partial_load_f32s(a_tail);
        let y = simd.partial_load_f32s(b_tail);
        acc[3] = simd.mul_add_e_f32s(x, y, acc[3]);
    }

    reduce(simd, acc)
}

#[inline(always)]
fn l2_squared_simd<S: Simd>(simd: S, a: &[f32], b: &[f32]) -> f32 {
    let (a_head, a_tail) = S::as_simd_f32s(a);
    let (b_head, b_tail) = S::as_simd_f32s(b);
    let mut acc = [simd.splat_f32s(0.0); UNROLL];

    let (a_blocks, a_rest) = a_head.as_chunks::<UNROLL>();
    let (b_blocks, b_rest) = b_head.as_chunks::<UNROLL>();
    for (x, y) in a_blocks.iter().zip(b_blocks) {
        for lane in 0..UNROLL {
            let delta = simd.sub_f32s(x[lane], y[lane]);
            acc[lane] = simd.mul_add_e_f32s(delta, delta, acc[lane]);
        }
    }
    for (slot, (x, y)) in acc.iter_mut().zip(a_rest.iter().zip(b_rest)) {
        let delta = simd.sub_f32s(*x, *y);
        *slot = simd.mul_add_e_f32s(delta, delta, *slot);
    }
    if !a_tail.is_empty() {
        let delta = simd.sub_f32s(
            simd.partial_load_f32s(a_tail),
            simd.partial_load_f32s(b_tail),
        );
        acc[3] = simd.mul_add_e_f32s(delta, delta, acc[3]);
    }

    reduce(simd, acc)
}

#[inline(always)]
pub(crate) fn reduce<S: Simd>(simd: S, acc: [S::f32s; UNROLL]) -> f32 {
    let low = simd.add_f32s(acc[0], acc[1]);
    let high = simd.add_f32s(acc[2], acc[3]);
    simd.reduce_sum_f32s(simd.add_f32s(low, high))
}

#[cfg(test)]
mod tests {
    use super::testing::{Rng, TEST_LENGTHS, assert_close};
    use super::*;

    #[test]
    fn dot_matches_scalar_reference_over_many_lengths() {
        let mut rng = Rng::new(0x05ee_dd07);
        for &len in TEST_LENGTHS {
            for _ in 0..8 {
                let a = rng.vector(len);
                let b = rng.vector(len);
                let magnitude: f32 = a.iter().zip(&b).map(|(x, y)| (x * y).abs()).sum();
                assert_close(dot(&a, &b), scalar::dot(&a, &b), magnitude, len);
            }
        }
    }

    #[test]
    fn l2_squared_matches_scalar_reference_over_many_lengths() {
        let mut rng = Rng::new(0x005e_ed12);
        for &len in TEST_LENGTHS {
            for _ in 0..8 {
                let a = rng.vector(len);
                let b = rng.vector(len);
                let expected = scalar::l2_squared(&a, &b);
                assert_close(l2_squared(&a, &b), expected, expected, len);
            }
        }
    }

    #[test]
    fn norm_matches_scalar_reference_over_many_lengths() {
        let mut rng = Rng::new(0x5eed_4042);
        for &len in TEST_LENGTHS {
            let a = rng.vector(len);
            let expected = scalar::norm(&a);
            assert_close(norm(&a), expected, expected, len);
        }
    }

    #[test]
    fn integer_inputs_match_scalar_reference_exactly() {
        // Small integers make every partial sum exact, so any dropped,
        // duplicated or misaligned tail element shows up as a mismatch.
        let mut rng = Rng::new(0x1a7e);
        for &len in TEST_LENGTHS {
            let a = rng.integer_vector(len);
            let b = rng.integer_vector(len);
            assert_eq!(dot(&a, &b), scalar::dot(&a, &b), "dot len {len}");
            assert_eq!(
                l2_squared(&a, &b),
                scalar::l2_squared(&a, &b),
                "l2 len {len}"
            );
        }
    }

    #[test]
    fn identical_vectors_have_zero_l2_distance() {
        let mut rng = Rng::new(7);
        for &len in TEST_LENGTHS {
            let a = rng.vector(len);
            assert_eq!(l2_squared(&a, &a), 0.0);
        }
    }

    #[test]
    fn empty_inputs_yield_zero() {
        assert_eq!(dot(&[], &[]), 0.0);
        assert_eq!(l2_squared(&[], &[]), 0.0);
        assert_eq!(norm(&[]), 0.0);
    }

    #[test]
    fn small_exact_values_are_exact() {
        assert_eq!(dot(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]), 32.0);
        assert_eq!(l2_squared(&[1.0, 2.0, 3.0], &[4.0, 6.0, 3.0]), 25.0);
        assert_eq!(norm(&[3.0, 4.0]), 5.0);
    }

    #[test]
    fn normalize_in_place_produces_unit_vectors() {
        let mut rng = Rng::new(0x0123_4567);
        for &len in TEST_LENGTHS.iter().filter(|len| **len > 0) {
            let mut a = rng.vector(len);
            let mut reference = a.clone();
            assert!(normalize_in_place(&mut a));
            assert!(scalar::normalize_in_place(&mut reference));
            assert!((norm(&a) - 1.0).abs() < 1e-5, "len {len}: {}", norm(&a));
            for (value, expected) in a.iter().zip(&reference) {
                assert!((value - expected).abs() <= 1e-5, "len {len}");
            }
        }
    }

    #[test]
    fn normalize_in_place_rejects_zero_and_non_finite_vectors() {
        let mut empty: [f32; 0] = [];
        assert!(!normalize_in_place(&mut empty));

        let mut zeros = vec![0.0; 17];
        assert!(!normalize_in_place(&mut zeros));
        assert!(zeros.iter().all(|value| *value == 0.0));

        let mut with_nan = vec![1.0, f32::NAN, 2.0];
        assert!(!normalize_in_place(&mut with_nan));
        assert_eq!(with_nan[0], 1.0);

        let mut with_inf = vec![1.0, f32::INFINITY];
        assert!(!normalize_in_place(&mut with_inf));
    }

    #[test]
    fn batched_kernels_match_single_kernels() {
        let mut rng = Rng::new(0xba7c);
        for &dims in TEST_LENGTHS {
            for rows in [0_usize, 1, 5, 33] {
                let query = rng.vector(dims);
                let matrix = rng.vector(dims * rows);
                let mut dots = vec![f32::NAN; rows];
                let mut dists = vec![f32::NAN; rows];
                let mut scalar_dots = vec![f32::NAN; rows];
                let mut scalar_dists = vec![f32::NAN; rows];
                dot_many(&query, &matrix, dims, &mut dots);
                l2_squared_many(&query, &matrix, dims, &mut dists);
                scalar::dot_many(&query, &matrix, dims, &mut scalar_dots);
                scalar::l2_squared_many(&query, &matrix, dims, &mut scalar_dists);
                for row in 0..rows {
                    let candidate = &matrix[row * dims..(row + 1) * dims];
                    assert_eq!(dots[row].to_bits(), dot(&query, candidate).to_bits());
                    assert_eq!(
                        dists[row].to_bits(),
                        l2_squared(&query, candidate).to_bits()
                    );
                    let magnitude: f32 = query
                        .iter()
                        .zip(candidate)
                        .map(|(x, y)| (x * y).abs())
                        .sum();
                    assert_close(dots[row], scalar_dots[row], magnitude, dims);
                    assert_close(dists[row], scalar_dists[row], scalar_dists[row], dims);
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "length mismatch")]
    fn dot_rejects_length_mismatch() {
        let _ = dot(&[1.0, 2.0], &[1.0]);
    }

    #[test]
    #[should_panic(expected = "matrix length")]
    fn dot_many_rejects_ragged_matrix() {
        let mut out = [0.0; 2];
        dot_many(&[1.0, 2.0], &[1.0, 2.0, 3.0], 2, &mut out);
    }

    #[test]
    fn simd_level_is_reported() {
        assert!(!simd_level().is_empty());
    }
}
