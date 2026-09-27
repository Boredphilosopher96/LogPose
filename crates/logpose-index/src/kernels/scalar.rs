//! Scalar reference kernels.
//!
//! These are the plain sequential loops the SIMD kernels in
//! [`crate::kernels`] must agree with (within floating-point reordering
//! tolerance). They exist as a test oracle and as the benchmark baseline, and
//! are not meant for hot paths.

/// Inner product of `a` and `b`, summed left to right.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[must_use]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "dot: length mismatch");
    let mut sum = 0.0_f32;
    for (lhs, rhs) in a.iter().zip(b) {
        sum += lhs * rhs;
    }
    sum
}

/// Squared Euclidean distance between `a` and `b`, summed left to right.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[must_use]
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "l2_squared: length mismatch");
    let mut sum = 0.0_f32;
    for (lhs, rhs) in a.iter().zip(b) {
        let delta = lhs - rhs;
        sum += delta * delta;
    }
    sum
}

/// Euclidean norm of `a`.
#[must_use]
pub fn norm(a: &[f32]) -> f32 {
    dot(a, a).sqrt()
}

/// Scales `a` to unit length.
///
/// The norm is computed in f64, so every finite non-zero vector is
/// normalized. Returns `false` and leaves `a` unchanged when every component
/// is zero or any component is NaN or infinite.
pub fn normalize_in_place(a: &mut [f32]) -> bool {
    let squared: f64 = a.iter().map(|value| f64::from(*value).powi(2)).sum();
    if squared == 0.0 || !squared.is_finite() {
        return false;
    }
    let inverse = 1.0 / squared.sqrt();
    for value in a.iter_mut() {
        *value = (f64::from(*value) * inverse) as f32;
    }
    true
}

/// Inner product of `query` with each `dims`-wide row of `matrix`.
///
/// # Panics
///
/// Panics if `query.len() != dims` or `matrix.len() != dims * out.len()`.
pub fn dot_many(query: &[f32], matrix: &[f32], dims: usize, out: &mut [f32]) {
    check_many_shape(query, matrix, dims, out.len());
    for (index, slot) in out.iter_mut().enumerate() {
        *slot = dot(query, &matrix[index * dims..(index + 1) * dims]);
    }
}

/// Squared Euclidean distance from `query` to each `dims`-wide row of
/// `matrix`.
///
/// # Panics
///
/// Panics if `query.len() != dims` or `matrix.len() != dims * out.len()`.
pub fn l2_squared_many(query: &[f32], matrix: &[f32], dims: usize, out: &mut [f32]) {
    check_many_shape(query, matrix, dims, out.len());
    for (index, slot) in out.iter_mut().enumerate() {
        *slot = l2_squared(query, &matrix[index * dims..(index + 1) * dims]);
    }
}

pub(crate) fn check_many_shape(query: &[f32], matrix: &[f32], dims: usize, rows: usize) {
    assert_eq!(query.len(), dims, "query length must equal dims");
    assert_eq!(
        Some(matrix.len()),
        dims.checked_mul(rows),
        "matrix length must equal dims * out.len()"
    );
}
