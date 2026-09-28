//! SQ8 scalar quantization: one byte per dimension.
//!
//! Each dimension `i` is mapped linearly from its trained range
//! `[min_i, max_i]` onto the 256 codes `0..=255`:
//!
//! ```text
//! step_i   = (max_i - min_i) / 255
//! code_i   = round((x_i - min_i) / step_i)      clamped to 0..=255
//! decoded  = min_i + step_i * code_i
//! ```
//!
//! Values inside the trained range decode to within half a step of the
//! original. Values outside it are clamped to the nearest end of the range.
//! A constant dimension (`max_i == min_i`) always encodes to code `0` and
//! decodes exactly to `min_i`.
//!
//! Distances are asymmetric: the query stays in f32 and only the stored
//! vectors are quantized. [`Sq8Params::query`] folds the per-dimension range
//! into query-dependent terms once, so the per-code inner loop is a widening
//! byte-to-float conversion plus one fused multiply-add per dimension for
//! inner product, or two for squared L2:
//!
//! ```text
//! dot(q, x^)  = sum_i q_i * min_i  +  sum_i (q_i * step_i) * code_i
//! |q - x^|^2  = sum_i ((q_i - min_i) - step_i * code_i)^2
//! ```
//!
//! Both inner loops run on the same runtime-dispatched SIMD backend as
//! [`crate::kernels`].

use crate::kernels::{self, UNROLL, reduce};
use pulp::{Simd, WithSimd};
use std::fmt;

/// Number of quantization levels above zero.
const LEVELS: f32 = 255.0;
/// Serialized-params magic.
const MAGIC: [u8; 4] = *b"LPQ8";
/// Serialized-params format version.
const VERSION: u16 = 1;
/// Magic, version and dimension count.
const HEADER_LEN: usize = 4 + 2 + 4;
/// Trailing CRC32.
const CHECKSUM_LEN: usize = 4;
/// Codes widened to f32 per SIMD block.
const BLOCK: usize = 64;

/// Error returned by SQ8 training, encoding, querying, and deserialization.
#[derive(Clone, Debug, PartialEq)]
pub enum Sq8Error {
    /// The dimension count is zero.
    ZeroDimensions,
    /// Training received no vectors.
    EmptyTrainingSet,
    /// The training buffer length is not a multiple of the dimension count.
    RaggedTrainingSet {
        /// Length of the training buffer.
        len: usize,
        /// Declared dimension count.
        dims: usize,
    },
    /// The dimension count does not fit the serialized format.
    TooManyDimensions(usize),
    /// A vector or bound has the wrong number of dimensions.
    DimensionMismatch {
        /// Dimensions the params were trained with.
        expected: usize,
        /// Dimensions of the rejected input.
        found: usize,
    },
    /// An input value is NaN or infinite.
    NonFinite {
        /// Index of the first offending value in the input slice.
        index: usize,
    },
    /// A dimension's range is inverted (`min > max`).
    InvertedRange {
        /// Offending dimension.
        dim: usize,
    },
    /// A dimension's range is too wide to represent in f32.
    RangeTooLarge {
        /// Offending dimension.
        dim: usize,
    },
    /// Serialized params are truncated, oversized, or structurally invalid.
    Corrupt(&'static str),
    /// Serialized params use an unknown format version.
    UnsupportedVersion(u16),
    /// Serialized params failed their CRC32 check.
    ChecksumMismatch {
        /// Checksum stored in the payload.
        stored: u32,
        /// Checksum computed over the payload.
        computed: u32,
    },
}

impl fmt::Display for Sq8Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroDimensions => f.write_str("sq8 requires at least one dimension"),
            Self::EmptyTrainingSet => f.write_str("sq8 training set is empty"),
            Self::RaggedTrainingSet { len, dims } => write!(
                f,
                "sq8 training buffer of {len} values is not a multiple of {dims} dimensions"
            ),
            Self::TooManyDimensions(dims) => write!(f, "sq8 dimension count {dims} is too large"),
            Self::DimensionMismatch { expected, found } => {
                write!(f, "sq8 expected {expected} dimensions but found {found}")
            }
            Self::NonFinite { index } => {
                write!(f, "sq8 input value at index {index} is NaN or infinite")
            }
            Self::InvertedRange { dim } => write!(f, "sq8 dimension {dim} has min > max"),
            Self::RangeTooLarge { dim } => {
                write!(f, "sq8 dimension {dim} has a range too wide for f32")
            }
            Self::Corrupt(reason) => write!(f, "corrupt sq8 params: {reason}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported sq8 params version {version}")
            }
            Self::ChecksumMismatch { stored, computed } => write!(
                f,
                "sq8 params checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
        }
    }
}

impl std::error::Error for Sq8Error {}

/// Trained per-dimension ranges for SQ8 encoding.
#[derive(Clone, Debug, PartialEq)]
pub struct Sq8Params {
    min: Vec<f32>,
    max: Vec<f32>,
    step: Vec<f32>,
    inv_step: Vec<f32>,
}

impl Sq8Params {
    /// Learns per-dimension min and max over `rows`, a row-major buffer of
    /// vectors with `dims` dimensions each.
    ///
    /// # Errors
    ///
    /// Fails when `dims` is zero, `rows` is empty or not a multiple of `dims`,
    /// any value is NaN or infinite, or a dimension's range is too wide for
    /// f32.
    pub fn train(rows: &[f32], dims: usize) -> Result<Self, Sq8Error> {
        if dims == 0 {
            return Err(Sq8Error::ZeroDimensions);
        }
        if rows.is_empty() {
            return Err(Sq8Error::EmptyTrainingSet);
        }
        if !rows.len().is_multiple_of(dims) {
            return Err(Sq8Error::RaggedTrainingSet {
                len: rows.len(),
                dims,
            });
        }
        check_finite(rows)?;

        let mut min = rows[..dims].to_vec();
        let mut max = min.clone();
        for row in rows.chunks_exact(dims).skip(1) {
            for ((low, high), value) in min.iter_mut().zip(max.iter_mut()).zip(row) {
                *low = low.min(*value);
                *high = high.max(*value);
            }
        }
        Self::from_bounds(min, max)
    }

    /// Builds params from explicit per-dimension bounds.
    ///
    /// # Errors
    ///
    /// Fails when the bounds are empty, differ in length, contain NaN or
    /// infinity, have `min > max` for some dimension, or span a range too wide
    /// for f32.
    pub fn from_bounds(min: Vec<f32>, max: Vec<f32>) -> Result<Self, Sq8Error> {
        let dims = min.len();
        if dims == 0 {
            return Err(Sq8Error::ZeroDimensions);
        }
        if u32::try_from(dims).is_err() {
            return Err(Sq8Error::TooManyDimensions(dims));
        }
        if max.len() != dims {
            return Err(Sq8Error::DimensionMismatch {
                expected: dims,
                found: max.len(),
            });
        }
        check_finite(&min)?;
        check_finite(&max)?;

        let mut step = Vec::with_capacity(dims);
        let mut inv_step = Vec::with_capacity(dims);
        for (dim, (low, high)) in min.iter().zip(&max).enumerate() {
            if low > high {
                return Err(Sq8Error::InvertedRange { dim });
            }
            // Compute in f64 so that a range near f32::MAX does not overflow
            // before the division.
            let width = f64::from(*high) - f64::from(*low);
            let raw_step = (width / f64::from(LEVELS)) as f32;
            if !(high - low).is_finite() || !(low + raw_step * LEVELS).is_finite() {
                return Err(Sq8Error::RangeTooLarge { dim });
            }
            // A zero or subnormal step would make its reciprocal overflow; such
            // a dimension is effectively constant.
            if raw_step.is_normal() && (1.0 / raw_step).is_finite() {
                step.push(raw_step);
                inv_step.push(1.0 / raw_step);
            } else {
                step.push(0.0);
                inv_step.push(0.0);
            }
        }

        Ok(Self {
            min,
            max,
            step,
            inv_step,
        })
    }

    /// Number of dimensions, which is also the code length in bytes.
    #[must_use]
    pub fn dims(&self) -> usize {
        self.min.len()
    }

    /// Per-dimension lower bounds.
    #[must_use]
    pub fn min(&self) -> &[f32] {
        &self.min
    }

    /// Per-dimension upper bounds.
    #[must_use]
    pub fn max(&self) -> &[f32] {
        &self.max
    }

    /// Per-dimension quantization step, `(max - min) / 255`; zero for constant
    /// dimensions.
    #[must_use]
    pub fn step(&self) -> &[f32] {
        &self.step
    }

    /// Encodes `vector` into a new code of [`Self::dims`] bytes.
    ///
    /// # Errors
    ///
    /// Fails on a dimension mismatch or a NaN or infinite value.
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>, Sq8Error> {
        let mut code = vec![0; self.dims()];
        self.encode_into(vector, &mut code)?;
        Ok(code)
    }

    /// Encodes `vector` into `code`, which must hold [`Self::dims`] bytes.
    ///
    /// Values outside the trained range are clamped to it.
    ///
    /// # Errors
    ///
    /// Fails on a dimension mismatch (of `vector` or `code`) or a NaN or
    /// infinite value. `code` is left unchanged on error.
    pub fn encode_into(&self, vector: &[f32], code: &mut [u8]) -> Result<(), Sq8Error> {
        self.check_dims(vector.len())?;
        self.check_dims(code.len())?;
        check_finite(vector)?;
        for (((slot, value), low), inv) in code
            .iter_mut()
            .zip(vector)
            .zip(&self.min)
            .zip(&self.inv_step)
        {
            // `as u8` saturates: negatives (below min) become 0, values above
            // max become 255, and the NaN from `inf * 0` on a constant
            // dimension becomes 0.
            *slot = ((value - low) * inv).round() as u8;
        }
        Ok(())
    }

    /// Decodes `code` back to an approximate f32 vector.
    ///
    /// # Errors
    ///
    /// Fails when `code` does not have [`Self::dims`] bytes.
    pub fn decode(&self, code: &[u8]) -> Result<Vec<f32>, Sq8Error> {
        self.check_dims(code.len())?;
        Ok(code
            .iter()
            .zip(&self.min)
            .zip(&self.step)
            .map(|((code, low), step)| step.mul_add(f32::from(*code), *low))
            .collect())
    }

    /// Prepares an f32 `query` for asymmetric distance estimates against codes
    /// produced by these params.
    ///
    /// # Errors
    ///
    /// Fails on a dimension mismatch or a NaN or infinite value.
    pub fn query(&self, metric: Sq8Metric, query: &[f32]) -> Result<Sq8Query, Sq8Error> {
        self.check_dims(query.len())?;
        check_finite(query)?;
        Ok(match metric {
            Sq8Metric::Dot => Sq8Query {
                metric,
                bias: kernels::dot(query, &self.min),
                scale: query
                    .iter()
                    .zip(&self.step)
                    .map(|(value, step)| value * step)
                    .collect(),
                offset: Vec::new(),
            },
            Sq8Metric::L2Squared => Sq8Query {
                metric,
                bias: 0.0,
                scale: self.step.clone(),
                offset: query
                    .iter()
                    .zip(&self.min)
                    .map(|(value, low)| value - low)
                    .collect(),
            },
        })
    }

    /// Serializes the params deterministically: magic `LPQ8`, a little-endian
    /// `u16` version, a `u32` dimension count, the `min` and `max` bounds as
    /// little-endian f32, and a trailing CRC32 over every preceding byte.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let dims = self.dims();
        let mut bytes = Vec::with_capacity(HEADER_LEN + dims * 8 + CHECKSUM_LEN);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        // Construction guarantees dims fits in u32.
        bytes.extend_from_slice(&u32::try_from(dims).unwrap_or(u32::MAX).to_le_bytes());
        for value in self.min.iter().chain(&self.max) {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Parses params written by [`Self::to_bytes`].
    ///
    /// # Errors
    ///
    /// Fails on a wrong magic, unknown version, length mismatch, checksum
    /// mismatch, or bounds that [`Self::from_bounds`] rejects.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Sq8Error> {
        let Some((payload, stored)) = bytes.split_last_chunk::<CHECKSUM_LEN>() else {
            return Err(Sq8Error::Corrupt("truncated payload"));
        };
        let Some((header, body)) = payload.split_first_chunk::<HEADER_LEN>() else {
            return Err(Sq8Error::Corrupt("truncated header"));
        };
        if header[..4] != MAGIC {
            return Err(Sq8Error::Corrupt("bad magic"));
        }
        let stored = u32::from_le_bytes(*stored);
        let computed = crc32fast::hash(payload);
        if stored != computed {
            return Err(Sq8Error::ChecksumMismatch { stored, computed });
        }
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != VERSION {
            return Err(Sq8Error::UnsupportedVersion(version));
        }
        let dims = u32::from_le_bytes([header[6], header[7], header[8], header[9]]) as usize;
        if Some(body.len()) != dims.checked_mul(8) {
            return Err(Sq8Error::Corrupt("body length does not match dimensions"));
        }

        let values: Vec<f32> = body
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| f32::from_le_bytes(*chunk))
            .collect();
        let (min, max) = values.split_at(dims);
        Self::from_bounds(min.to_vec(), max.to_vec())
    }

    fn check_dims(&self, found: usize) -> Result<(), Sq8Error> {
        if found == self.dims() {
            Ok(())
        } else {
            Err(Sq8Error::DimensionMismatch {
                expected: self.dims(),
                found,
            })
        }
    }
}

/// Metric estimated by an [`Sq8Query`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Sq8Metric {
    /// Inner product; larger is closer. Use for cosine over vectors that were
    /// normalized before encoding.
    Dot,
    /// Squared Euclidean distance; smaller is closer.
    L2Squared,
}

/// An f32 query prepared against [`Sq8Params`] for asymmetric distance
/// estimates.
///
/// Estimates equal the exact metric between the query and the *decoded* code,
/// up to floating-point rounding.
#[derive(Clone, Debug, PartialEq)]
pub struct Sq8Query {
    metric: Sq8Metric,
    /// `sum q_i * min_i` for [`Sq8Metric::Dot`]; zero otherwise.
    bias: f32,
    /// `q_i * step_i` for [`Sq8Metric::Dot`]; `step_i` for L2.
    scale: Vec<f32>,
    /// `q_i - min_i` for [`Sq8Metric::L2Squared`]; empty for dot.
    offset: Vec<f32>,
}

impl Sq8Query {
    /// Metric this query estimates.
    #[must_use]
    pub fn metric(&self) -> Sq8Metric {
        self.metric
    }

    /// Number of dimensions (code length in bytes).
    #[must_use]
    pub fn dims(&self) -> usize {
        self.scale.len()
    }

    /// Estimates the metric between the query and one code.
    ///
    /// # Panics
    ///
    /// Panics if `code.len() != self.dims()`.
    #[must_use]
    pub fn estimate(&self, code: &[u8]) -> f32 {
        assert_eq!(code.len(), self.dims(), "sq8 code length mismatch");
        kernels::arch().dispatch(Estimate { query: self, code })
    }

    /// Estimates the metric between the query and each code in `codes`, a
    /// contiguous buffer of [`Self::dims`]-byte codes, writing one value per
    /// code to `out`.
    ///
    /// # Panics
    ///
    /// Panics if `codes.len() != self.dims() * out.len()`.
    pub fn estimate_many(&self, codes: &[u8], out: &mut [f32]) {
        assert_eq!(
            Some(codes.len()),
            self.dims().checked_mul(out.len()),
            "sq8 codes length must equal dims * out.len()"
        );
        kernels::arch().dispatch(EstimateMany {
            query: self,
            codes,
            out,
        });
    }
}

struct Estimate<'a> {
    query: &'a Sq8Query,
    code: &'a [u8],
}

impl WithSimd for Estimate<'_> {
    type Output = f32;

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) -> f32 {
        estimate_with(simd, self.query, self.code)
    }
}

struct EstimateMany<'a> {
    query: &'a Sq8Query,
    codes: &'a [u8],
    out: &'a mut [f32],
}

impl WithSimd for EstimateMany<'_> {
    type Output = ();

    #[inline(always)]
    fn with_simd<S: Simd>(self, simd: S) {
        let dims = self.query.dims();
        for (code, slot) in self.codes.chunks_exact(dims).zip(self.out) {
            *slot = estimate_with(simd, self.query, code);
        }
    }
}

#[inline(always)]
fn estimate_with<S: Simd>(simd: S, query: &Sq8Query, code: &[u8]) -> f32 {
    match query.metric {
        Sq8Metric::Dot => query.bias + dot_codes(simd, &query.scale, code),
        Sq8Metric::L2Squared => l2_codes(simd, &query.offset, &query.scale, code),
    }
}

#[inline(always)]
fn widen(codes: &[u8; BLOCK]) -> [f32; BLOCK] {
    let mut out = [0.0; BLOCK];
    for (slot, code) in out.iter_mut().zip(codes) {
        *slot = f32::from(*code);
    }
    out
}

/// Zero-pads a ragged tail up to one block. Zero scale and offset make the
/// padded lanes contribute nothing to either metric.
#[inline(always)]
fn pad<T: Copy + Default, const N: usize>(tail: &[T]) -> [T; N] {
    let mut block = [T::default(); N];
    block[..tail.len()].copy_from_slice(tail);
    block
}

#[inline(always)]
fn dot_codes<S: Simd>(simd: S, scale: &[f32], codes: &[u8]) -> f32 {
    let mut acc = [simd.splat_f32s(0.0); UNROLL];
    let (code_blocks, code_tail) = codes.as_chunks::<BLOCK>();
    let (scale_blocks, scale_tail) = scale.as_chunks::<BLOCK>();
    for (code, weights) in code_blocks.iter().zip(scale_blocks) {
        dot_block(simd, &mut acc, weights, &widen(code));
    }
    if !code_tail.is_empty() {
        dot_block(simd, &mut acc, &pad(scale_tail), &widen(&pad(code_tail)));
    }
    reduce(simd, acc)
}

#[inline(always)]
fn dot_block<S: Simd>(
    simd: S,
    acc: &mut [S::f32s; UNROLL],
    weights: &[f32; BLOCK],
    values: &[f32; BLOCK],
) {
    let (weights, _) = S::as_simd_f32s(weights);
    let (values, _) = S::as_simd_f32s(values);
    for (index, (weight, value)) in weights.iter().zip(values).enumerate() {
        let lane = index % UNROLL;
        acc[lane] = simd.mul_add_e_f32s(*weight, *value, acc[lane]);
    }
}

#[inline(always)]
fn l2_codes<S: Simd>(simd: S, offset: &[f32], scale: &[f32], codes: &[u8]) -> f32 {
    let mut acc = [simd.splat_f32s(0.0); UNROLL];
    let (code_blocks, code_tail) = codes.as_chunks::<BLOCK>();
    let (offset_blocks, offset_tail) = offset.as_chunks::<BLOCK>();
    let (scale_blocks, scale_tail) = scale.as_chunks::<BLOCK>();
    for ((code, offsets), scales) in code_blocks.iter().zip(offset_blocks).zip(scale_blocks) {
        l2_block(simd, &mut acc, offsets, scales, &widen(code));
    }
    if !code_tail.is_empty() {
        l2_block(
            simd,
            &mut acc,
            &pad(offset_tail),
            &pad(scale_tail),
            &widen(&pad(code_tail)),
        );
    }
    reduce(simd, acc)
}

#[inline(always)]
fn l2_block<S: Simd>(
    simd: S,
    acc: &mut [S::f32s; UNROLL],
    offsets: &[f32; BLOCK],
    scales: &[f32; BLOCK],
    values: &[f32; BLOCK],
) {
    let (offsets, _) = S::as_simd_f32s(offsets);
    let (scales, _) = S::as_simd_f32s(scales);
    let (values, _) = S::as_simd_f32s(values);
    for (index, ((offset, scale), value)) in offsets.iter().zip(scales).zip(values).enumerate() {
        let lane = index % UNROLL;
        // offset - scale * code
        let delta = simd.negate_mul_add_e_f32s(*scale, *value, *offset);
        acc[lane] = simd.mul_add_e_f32s(delta, delta, acc[lane]);
    }
}

/// Magic of an SQ8 codes section ([`write_codes_section`]).
const SECTION_MAGIC: [u8; 8] = *b"LPS8CODE";
/// Format version of an SQ8 codes section.
const SECTION_VERSION: u32 = 1;
/// Fixed header of an SQ8 codes section.
const SECTION_HEADER_LEN: usize = 32;

/// Serializes one segment's SQ8 section: the trained `params` and the codes
/// of `rows` rows, `rows * params.dims()` bytes in row order (rows without a
/// vector hold zero codes).
///
/// ```text
/// offset size field
///      0    8 magic "LPS8CODE"
///      8    4 version (1)
///     12    4 dims
///     16    8 rows
///     24    4 params_len
///     28    4 reserved, zero
///     32    n params (Sq8Params::to_bytes), zero padding to 8
///      .    . codes, rows * dims bytes
/// ```
///
/// Integers are little-endian. The params carry their own CRC; the codes
/// are covered by the storage section's CRC.
///
/// # Errors
///
/// [`Sq8Error::Corrupt`] when `codes` is not `rows * dims` bytes.
pub fn write_codes_section(
    params: &Sq8Params,
    rows: u64,
    codes: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), Sq8Error> {
    let dims = params.dims();
    let expected = usize::try_from(rows)
        .ok()
        .and_then(|rows| rows.checked_mul(dims));
    if expected != Some(codes.len()) {
        return Err(Sq8Error::Corrupt("codes length does not match rows * dims"));
    }
    let params_bytes = params.to_bytes();
    out.extend_from_slice(&SECTION_MAGIC);
    out.extend_from_slice(&SECTION_VERSION.to_le_bytes());
    out.extend_from_slice(&u32::try_from(dims).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(&rows.to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(params_bytes.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&params_bytes);
    out.resize(out.len() + padding_to_8(params_bytes.len()), 0);
    out.extend_from_slice(codes);
    Ok(())
}

/// A parsed SQ8 codes section: its params and where the codes start.
#[derive(Clone, Debug, PartialEq)]
pub struct Sq8Section {
    params: Sq8Params,
    rows: usize,
    codes_offset: usize,
}

impl Sq8Section {
    /// Parses and validates a section written by [`write_codes_section`]:
    /// the header, the params (with their CRC), and that the codes fill the
    /// rest of `bytes` exactly. The codes stay in `bytes`; read them with
    /// [`Self::codes`].
    ///
    /// # Errors
    ///
    /// [`Sq8Error::Corrupt`] or a params error for malformed input.
    pub fn parse(bytes: &[u8]) -> Result<Self, Sq8Error> {
        let header = bytes
            .get(..SECTION_HEADER_LEN)
            .ok_or(Sq8Error::Corrupt("truncated codes header"))?;
        if header[..8] != SECTION_MAGIC {
            return Err(Sq8Error::Corrupt("bad codes magic"));
        }
        let word = |at: usize| {
            u32::from_le_bytes([header[at], header[at + 1], header[at + 2], header[at + 3]])
        };
        let version = word(8);
        if version != SECTION_VERSION {
            return Err(Sq8Error::UnsupportedVersion(
                u16::try_from(version).unwrap_or(u16::MAX),
            ));
        }
        let dims = word(12) as usize;
        let mut rows_bytes = [0; 8];
        rows_bytes.copy_from_slice(&header[16..24]);
        let rows = usize::try_from(u64::from_le_bytes(rows_bytes))
            .map_err(|_| Sq8Error::Corrupt("row count does not fit in memory"))?;
        let params_len = word(24) as usize;
        if word(28) != 0 {
            return Err(Sq8Error::Corrupt("reserved codes header bits are set"));
        }
        let params_end = SECTION_HEADER_LEN
            .checked_add(params_len)
            .ok_or(Sq8Error::Corrupt("params length overflows"))?;
        let params = Sq8Params::from_bytes(
            bytes
                .get(SECTION_HEADER_LEN..params_end)
                .ok_or(Sq8Error::Corrupt("truncated params"))?,
        )?;
        if params.dims() != dims {
            return Err(Sq8Error::Corrupt(
                "params dimensions differ from the header",
            ));
        }
        let codes_offset = params_end + padding_to_8(params_len);
        let codes_len = rows
            .checked_mul(dims)
            .ok_or(Sq8Error::Corrupt("codes length overflows"))?;
        if codes_offset.checked_add(codes_len) != Some(bytes.len()) {
            return Err(Sq8Error::Corrupt("codes do not fill the section"));
        }
        Ok(Self {
            params,
            rows,
            codes_offset,
        })
    }

    /// The trained params.
    #[must_use]
    pub fn params(&self) -> &Sq8Params {
        &self.params
    }

    /// Number of rows with a code.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// The codes, `rows * dims` bytes, from the same `bytes` [`Self::parse`]
    /// validated. Empty if `bytes` is shorter than that.
    #[must_use]
    pub fn codes<'a>(&self, bytes: &'a [u8]) -> &'a [u8] {
        bytes.get(self.codes_offset..).unwrap_or(&[])
    }
}

fn padding_to_8(len: usize) -> usize {
    (8 - len % 8) % 8
}

fn check_finite(values: &[f32]) -> Result<(), Sq8Error> {
    match values.iter().position(|value| !value.is_finite()) {
        Some(index) => Err(Sq8Error::NonFinite { index }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernels::scalar;
    use crate::kernels::testing::{Rng, TEST_LENGTHS, assert_close};

    fn random_rows(rng: &mut Rng, rows: usize, dims: usize) -> Vec<f32> {
        (0..rows * dims).map(|_| rng.signed() * 3.0).collect()
    }

    #[test]
    fn train_learns_per_dimension_bounds() -> Result<(), Sq8Error> {
        let rows = [1.0, -2.0, 5.0, 3.0, 4.0, 5.0, -1.0, 0.0, 5.0];
        let params = Sq8Params::train(&rows, 3)?;
        assert_eq!(params.dims(), 3);
        assert_eq!(params.min(), &[-1.0, -2.0, 5.0]);
        assert_eq!(params.max(), &[3.0, 4.0, 5.0]);
        assert_eq!(params.step(), &[4.0 / 255.0, 6.0 / 255.0, 0.0]);
        Ok(())
    }

    #[test]
    fn train_rejects_invalid_input() {
        assert_eq!(Sq8Params::train(&[1.0], 0), Err(Sq8Error::ZeroDimensions));
        assert_eq!(Sq8Params::train(&[], 4), Err(Sq8Error::EmptyTrainingSet));
        assert_eq!(
            Sq8Params::train(&[1.0, 2.0, 3.0], 2),
            Err(Sq8Error::RaggedTrainingSet { len: 3, dims: 2 })
        );
        assert_eq!(
            Sq8Params::train(&[1.0, f32::NAN], 2),
            Err(Sq8Error::NonFinite { index: 1 })
        );
        assert_eq!(
            Sq8Params::train(&[f32::NEG_INFINITY, 0.0], 1),
            Err(Sq8Error::NonFinite { index: 0 })
        );
        assert_eq!(
            Sq8Params::train(&[-f32::MAX, f32::MAX], 1),
            Err(Sq8Error::RangeTooLarge { dim: 0 })
        );
        assert_eq!(
            Sq8Params::from_bounds(vec![1.0], vec![0.0]),
            Err(Sq8Error::InvertedRange { dim: 0 })
        );
    }

    #[test]
    fn round_trip_error_is_within_half_a_step() -> Result<(), Sq8Error> {
        let mut rng = Rng::new(0x5a8);
        for &dims in TEST_LENGTHS.iter().filter(|dims| **dims > 0) {
            let rows = random_rows(&mut rng, 64, dims);
            let params = Sq8Params::train(&rows, dims)?;
            for row in rows.chunks_exact(dims) {
                let decoded = params.decode(&params.encode(row)?)?;
                for (dim, (value, approx)) in row.iter().zip(&decoded).enumerate() {
                    let step = params.step()[dim];
                    let bound = 0.5 * step * (1.0 + 1e-3) + 1e-6;
                    assert!(
                        (value - approx).abs() <= bound,
                        "dims {dims} dim {dim}: {value} vs {approx} (step {step})"
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn range_endpoints_map_to_extreme_codes() -> Result<(), Sq8Error> {
        let params = Sq8Params::from_bounds(vec![-1.0, 0.0], vec![1.0, 10.0])?;
        assert_eq!(params.encode(&[-1.0, 0.0])?, vec![0, 0]);
        assert_eq!(params.encode(&[1.0, 10.0])?, vec![255, 255]);
        Ok(())
    }

    #[test]
    fn out_of_range_values_are_clamped() -> Result<(), Sq8Error> {
        let params = Sq8Params::from_bounds(vec![-1.0, 0.0], vec![1.0, 10.0])?;
        assert_eq!(params.encode(&[-50.0, 1e30])?, vec![0, 255]);
        assert_eq!(params.encode(&[-f32::MAX, f32::MAX])?, vec![0, 255]);
        Ok(())
    }

    #[test]
    fn constant_dimensions_round_trip_exactly() -> Result<(), Sq8Error> {
        let rows = [2.5, 1.0, -7.0, 2.5, 3.0, -7.0, 2.5, -4.0, -7.0];
        let params = Sq8Params::train(&rows, 3)?;
        assert_eq!(params.step()[0], 0.0);
        assert_eq!(params.step()[2], 0.0);
        let code = params.encode(&[2.5, 0.0, -7.0])?;
        assert_eq!((code[0], code[2]), (0, 0));
        let decoded = params.decode(&code)?;
        assert_eq!((decoded[0], decoded[2]), (2.5, -7.0));

        // Off-range values on a constant dimension still decode to the constant.
        let code = params.encode(&[f32::MAX, 0.0, -f32::MAX])?;
        let decoded = params.decode(&code)?;
        assert_eq!((decoded[0], decoded[2]), (2.5, -7.0));

        let query = [1.0, 2.0, 3.0];
        for metric in [Sq8Metric::Dot, Sq8Metric::L2Squared] {
            let estimate = params.query(metric, &query)?.estimate(&code);
            assert!(estimate.is_finite());
        }
        Ok(())
    }

    #[test]
    fn non_finite_inputs_are_rejected() -> Result<(), Sq8Error> {
        let params = Sq8Params::from_bounds(vec![0.0; 3], vec![1.0; 3])?;
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let vector = [0.5, bad, 0.5];
            let expected = Err(Sq8Error::NonFinite { index: 1 });
            assert_eq!(params.encode(&vector), expected);
            let mut code = [7_u8; 3];
            assert_eq!(params.encode_into(&vector, &mut code), expected.map(|_| ()));
            assert_eq!(code, [7, 7, 7]);
            for metric in [Sq8Metric::Dot, Sq8Metric::L2Squared] {
                assert_eq!(
                    params.query(metric, &vector).err(),
                    Some(Sq8Error::NonFinite { index: 1 })
                );
            }
            assert_eq!(
                Sq8Params::from_bounds(vec![0.0, bad, 0.0], vec![1.0; 3]),
                Err(Sq8Error::NonFinite { index: 1 })
            );
        }
        Ok(())
    }

    #[test]
    fn dimension_mismatches_are_rejected() -> Result<(), Sq8Error> {
        let params = Sq8Params::from_bounds(vec![0.0; 3], vec![1.0; 3])?;
        let mismatch = Sq8Error::DimensionMismatch {
            expected: 3,
            found: 2,
        };
        assert_eq!(params.encode(&[0.0, 1.0]), Err(mismatch.clone()));
        assert_eq!(params.decode(&[0, 1]), Err(mismatch.clone()));
        assert_eq!(
            params.encode_into(&[0.0; 3], &mut [0; 2]),
            Err(mismatch.clone())
        );
        assert_eq!(
            params.query(Sq8Metric::Dot, &[0.0; 2]).err(),
            Some(mismatch)
        );
        Ok(())
    }

    #[test]
    fn estimates_match_exact_metric_on_decoded_codes() -> Result<(), Sq8Error> {
        let mut rng = Rng::new(0xe57);
        for &dims in TEST_LENGTHS.iter().filter(|dims| **dims > 0) {
            let rows = random_rows(&mut rng, 16, dims);
            let params = Sq8Params::train(&rows, dims)?;
            let query = rng.vector(dims);
            let dot_query = params.query(Sq8Metric::Dot, &query)?;
            let l2_query = params.query(Sq8Metric::L2Squared, &query)?;
            for row in rows.chunks_exact(dims) {
                let code = params.encode(row)?;
                let decoded = params.decode(&code)?;

                let expected = scalar::dot(&query, &decoded);
                let magnitude: f32 = query
                    .iter()
                    .zip(&decoded)
                    .map(|(x, y)| (x * y).abs())
                    .sum::<f32>()
                    + query
                        .iter()
                        .zip(params.min())
                        .map(|(x, y)| (x * y).abs())
                        .sum::<f32>();
                assert_close(dot_query.estimate(&code), expected, magnitude, dims);

                let expected = scalar::l2_squared(&query, &decoded);
                let magnitude = expected
                    + query.iter().map(|x| x * x).sum::<f32>()
                    + decoded.iter().map(|x| x * x).sum::<f32>();
                assert_close(l2_query.estimate(&code), expected, magnitude, dims);
            }
        }
        Ok(())
    }

    #[test]
    fn estimates_are_within_quantization_error_of_exact_metric() -> Result<(), Sq8Error> {
        let mut rng = Rng::new(0xb0b);
        for dims in [8_usize, 100, 768] {
            let rows = random_rows(&mut rng, 200, dims);
            let params = Sq8Params::train(&rows, dims)?;
            let query = rng.vector(dims);
            let dot_query = params.query(Sq8Metric::Dot, &query)?;
            let l2_query = params.query(Sq8Metric::L2Squared, &query)?;
            // |x - decode(encode(x))|_i <= step_i / 2.
            let dot_bound: f32 = query
                .iter()
                .zip(params.step())
                .map(|(q, step)| q.abs() * step * 0.5)
                .sum();
            let l2_bound = params
                .step()
                .iter()
                .map(|step| (step * 0.5).powi(2))
                .sum::<f32>()
                .sqrt();
            for row in rows.chunks_exact(dims) {
                let code = params.encode(row)?;
                let exact_dot = scalar::dot(&query, row);
                let approx_dot = dot_query.estimate(&code);
                assert!(
                    (exact_dot - approx_dot).abs() <= dot_bound * 1.01 + 1e-4,
                    "dims {dims}: dot {exact_dot} vs {approx_dot} (bound {dot_bound})"
                );
                // Triangle inequality in Euclidean distance.
                let exact = scalar::l2_squared(&query, row).sqrt();
                let approx = l2_query.estimate(&code).max(0.0).sqrt();
                assert!(
                    (exact - approx).abs() <= l2_bound * 1.01 + 1e-4,
                    "dims {dims}: l2 {exact} vs {approx} (bound {l2_bound})"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn estimate_many_matches_single_estimates() -> Result<(), Sq8Error> {
        let mut rng = Rng::new(0x3a7);
        for dims in [1_usize, 7, 64, 65, 300] {
            let rows = random_rows(&mut rng, 23, dims);
            let params = Sq8Params::train(&rows, dims)?;
            let mut codes = Vec::new();
            for row in rows.chunks_exact(dims) {
                codes.extend(params.encode(row)?);
            }
            let query = rng.vector(dims);
            for metric in [Sq8Metric::Dot, Sq8Metric::L2Squared] {
                let prepared = params.query(metric, &query)?;
                let mut out = vec![f32::NAN; 23];
                prepared.estimate_many(&codes, &mut out);
                for (code, value) in codes.chunks_exact(dims).zip(&out) {
                    assert_eq!(value.to_bits(), prepared.estimate(code).to_bits());
                }
            }
        }
        Ok(())
    }

    /// Unit steps from zero with small-integer queries and codes make every
    /// estimate exact, so a dropped or misread tail lane shows up as a
    /// mismatch on the backend under test.
    fn check_backend<S: Simd>(simd: S, backend: &str) -> Result<(), Sq8Error> {
        let mut rng = Rng::new(0x5a8_bac);
        for &dims in TEST_LENGTHS.iter().filter(|dims| **dims > 0) {
            let params = Sq8Params::from_bounds(vec![0.0; dims], vec![255.0; dims])?;
            assert!(params.step().iter().all(|step| *step == 1.0));
            let query = rng.integer_vector(dims);
            let code: Vec<u8> = (0..dims).map(|_| (rng.next_u64() % 9) as u8).collect();
            let decoded = params.decode(&code)?;
            for (metric, expected) in [
                (Sq8Metric::Dot, scalar::dot(&query, &decoded)),
                (Sq8Metric::L2Squared, scalar::l2_squared(&query, &decoded)),
            ] {
                let prepared = params.query(metric, &query)?;
                let estimate = simd.vectorize(Estimate {
                    query: &prepared,
                    code: &code,
                });
                assert_eq!(estimate, expected, "{backend} {metric:?} dims {dims}");
            }
        }
        Ok(())
    }

    #[test]
    fn every_simd_backend_estimates_every_tail_length_exactly() -> Result<(), Sq8Error> {
        check_backend(pulp::Scalar::new(), "scalar")?;
        check_backend(pulp::Scalar128b, "scalar128")?;
        check_backend(pulp::Scalar256b, "scalar256")?;
        check_backend(pulp::Scalar512b, "scalar512")?;
        #[cfg(target_arch = "x86_64")]
        {
            if let Some(simd) = pulp::x86::V3::try_new() {
                check_backend(simd, "x86-64-v3")?;
            }
            if let Some(simd) = pulp::x86::V4::try_new() {
                check_backend(simd, "x86-64-v4")?;
            }
        }
        #[cfg(target_arch = "aarch64")]
        if let Some(simd) = pulp::aarch64::Neon::try_new() {
            check_backend(simd, "neon")?;
        }
        Ok(())
    }

    #[test]
    fn reranked_sq8_candidates_recover_exact_top_10() -> Result<(), Sq8Error> {
        const DIMS: usize = 96;
        const CLUSTERS: usize = 24;
        const ROWS: usize = 3_000;
        const QUERIES: usize = 40;
        const K: usize = 10;
        const CANDIDATES: usize = 40;

        let mut rng = Rng::new(0xc105);
        let centers: Vec<Vec<f32>> = (0..CLUSTERS)
            .map(|_| (0..DIMS).map(|_| rng.gaussian()).collect())
            .collect();
        let sample = |rng: &mut Rng| -> Vec<f32> {
            let center = &centers[(rng.next_u64() % CLUSTERS as u64) as usize];
            center.iter().map(|c| c + 0.35 * rng.gaussian()).collect()
        };
        let rows: Vec<f32> = (0..ROWS).flat_map(|_| sample(&mut rng)).collect();
        let queries: Vec<Vec<f32>> = (0..QUERIES).map(|_| sample(&mut rng)).collect();

        let params = Sq8Params::train(&rows, DIMS)?;
        let mut codes = Vec::with_capacity(ROWS * DIMS);
        for row in rows.chunks_exact(DIMS) {
            codes.extend(params.encode(row)?);
        }

        for metric in [Sq8Metric::Dot, Sq8Metric::L2Squared] {
            // Orient every score so that larger is better.
            let exact_score = |query: &[f32], row: &[f32]| match metric {
                Sq8Metric::Dot => scalar::dot(query, row),
                Sq8Metric::L2Squared => -scalar::l2_squared(query, row),
            };
            let sign = match metric {
                Sq8Metric::Dot => 1.0,
                Sq8Metric::L2Squared => -1.0,
            };
            let top = |scores: Vec<(usize, f32)>, n: usize| -> Vec<usize> {
                let mut scores = scores;
                scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                scores.into_iter().take(n).map(|(id, _)| id).collect()
            };

            let mut hits = 0;
            let mut raw_hits = 0;
            let mut estimates = vec![0.0; ROWS];
            for query in &queries {
                let truth = top(
                    rows.chunks_exact(DIMS)
                        .map(|row| exact_score(query, row))
                        .enumerate()
                        .collect(),
                    K,
                );
                params
                    .query(metric, query)?
                    .estimate_many(&codes, &mut estimates);
                let candidates = top(
                    estimates
                        .iter()
                        .map(|value| sign * value)
                        .enumerate()
                        .collect(),
                    CANDIDATES,
                );
                raw_hits += candidates[..K]
                    .iter()
                    .filter(|id| truth.contains(id))
                    .count();
                let reranked = top(
                    candidates
                        .iter()
                        .map(|&id| (id, exact_score(query, &rows[id * DIMS..(id + 1) * DIMS])))
                        .collect(),
                    K,
                );
                hits += reranked.iter().filter(|id| truth.contains(id)).count();
            }
            let recall = hits as f32 / (QUERIES * K) as f32;
            let raw_recall = raw_hits as f32 / (QUERIES * K) as f32;
            assert!(recall >= 0.95, "{metric:?}: reranked recall@10 {recall}");
            assert!(raw_recall >= 0.9, "{metric:?}: raw recall@10 {raw_recall}");
        }
        Ok(())
    }

    #[test]
    fn serialization_is_deterministic_and_round_trips() -> Result<(), Sq8Error> {
        let mut rng = Rng::new(0x5e71);
        let rows = random_rows(&mut rng, 32, 37);
        let params = Sq8Params::train(&rows, 37)?;
        let bytes = params.to_bytes();
        assert_eq!(bytes, params.clone().to_bytes());
        assert_eq!(bytes.len(), HEADER_LEN + 37 * 8 + CHECKSUM_LEN);
        assert_eq!(&bytes[..4], b"LPQ8");
        let parsed = Sq8Params::from_bytes(&bytes)?;
        assert_eq!(parsed, params);
        assert_eq!(parsed.to_bytes(), bytes);
        Ok(())
    }

    #[test]
    fn serialization_rejects_corruption() -> Result<(), Sq8Error> {
        let params = Sq8Params::from_bounds(vec![0.0, -1.0, 2.0], vec![1.0, 1.0, 2.0])?;
        let bytes = params.to_bytes();

        for index in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[index] ^= 0x10;
            assert!(Sq8Params::from_bytes(&corrupt).is_err(), "flip at {index}");
        }
        let mut flipped = bytes.clone();
        flipped[HEADER_LEN] ^= 1;
        assert!(matches!(
            Sq8Params::from_bytes(&flipped),
            Err(Sq8Error::ChecksumMismatch { .. })
        ));
        for len in 0..bytes.len() {
            assert!(
                Sq8Params::from_bytes(&bytes[..len]).is_err(),
                "truncated to {len}"
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(Sq8Params::from_bytes(&trailing).is_err());

        let mut future = bytes[..bytes.len() - CHECKSUM_LEN].to_vec();
        future[4] = 2;
        let checksum = crc32fast::hash(&future);
        future.extend_from_slice(&checksum.to_le_bytes());
        assert_eq!(
            Sq8Params::from_bytes(&future),
            Err(Sq8Error::UnsupportedVersion(2))
        );
        Ok(())
    }

    #[test]
    fn codes_section_round_trips_and_rejects_damage() -> Result<(), Sq8Error> {
        let rows = [0.0_f32, 1.0, -1.0, 0.5, 2.0, -2.0];
        let params = Sq8Params::train(&rows, 2)?;
        let mut codes = Vec::new();
        for row in rows.chunks_exact(2) {
            codes.extend(params.encode(row)?);
        }
        let mut bytes = Vec::new();
        write_codes_section(&params, 3, &codes, &mut bytes)?;
        assert_eq!(
            (bytes.len() - codes.len()) % 8,
            0,
            "codes start 8-byte aligned"
        );
        let section = Sq8Section::parse(&bytes)?;
        assert_eq!(section.rows(), 3);
        assert_eq!(section.params(), &params);
        assert_eq!(section.codes(&bytes), codes.as_slice());

        assert!(write_codes_section(&params, 4, &codes, &mut Vec::new()).is_err());
        assert!(Sq8Section::parse(&bytes[..bytes.len() - 1]).is_err());
        let mut bad = bytes.clone();
        bad[0] ^= 1;
        assert!(Sq8Section::parse(&bad).is_err());
        let mut bad = bytes;
        bad[40] ^= 1;
        assert!(Sq8Section::parse(&bad).is_err());
        Ok(())
    }
}
