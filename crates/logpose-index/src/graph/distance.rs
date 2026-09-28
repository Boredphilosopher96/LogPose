//! Distance sources the graph builds and searches over.

use super::GraphError;

/// Row-to-row distances used while building a graph.
///
/// Rows are dense ids `0..len()`. Lower distance means closer. The source is
/// shared across build threads, so it must be `Sync`.
pub trait VectorSource: Sync {
    /// Number of rows in the source.
    fn len(&self) -> usize;

    /// Returns `true` when the source has no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Distance between rows `a` and `b`. Both must be below [`Self::len`].
    fn distance_between(&self, a: u32, b: u32) -> f32;
}

/// Query-to-row distances used while searching.
///
/// One value is created per query, typically holding the query vector (or
/// its encoded form) and a reference to the row storage.
pub trait QueryDistance {
    /// Distance from the query to `row`. Lower means closer.
    fn distance(&self, row: u32) -> f32;

    /// Distances from the query to each of `rows`, written to `out` (of the same length).
    /// A walk scores an expanded node's new neighbors with one call, so an implementation
    /// whose per-call setup is significant (a SIMD dispatch) can score them together; the
    /// default scores them one by one.
    fn distances(&self, rows: &[u32], out: &mut [f32]) {
        for (row, slot) in rows.iter().zip(out) {
            *slot = self.distance(*row);
        }
    }
}

/// Distance function for [`F32Vectors`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum F32Metric {
    /// Squared Euclidean distance.
    L2Squared,
    /// Negated inner product, so that a larger dot product ranks closer.
    NegativeDot,
}

impl F32Metric {
    /// Distance between two equal-length vectors.
    pub fn distance(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Self::L2Squared => l2_squared(a, b),
            Self::NegativeDot => -dot(a, b),
        }
    }
}

const LANES: usize = 8;

fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0_f32; LANES];
    let a_chunks = a.chunks_exact(LANES);
    let b_chunks = b.chunks_exact(LANES);
    let mut tail = 0.0_f32;
    for (x, y) in a_chunks.remainder().iter().zip(b_chunks.remainder()) {
        let diff = x - y;
        tail += diff * diff;
    }
    for (x, y) in a_chunks.zip(b_chunks) {
        for ((sum, xv), yv) in acc.iter_mut().zip(x).zip(y) {
            let diff = xv - yv;
            *sum += diff * diff;
        }
    }
    acc.iter().sum::<f32>() + tail
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0_f32; LANES];
    let a_chunks = a.chunks_exact(LANES);
    let b_chunks = b.chunks_exact(LANES);
    let mut tail = 0.0_f32;
    for (x, y) in a_chunks.remainder().iter().zip(b_chunks.remainder()) {
        tail += x * y;
    }
    for (x, y) in a_chunks.zip(b_chunks) {
        for ((sum, xv), yv) in acc.iter_mut().zip(x).zip(y) {
            *sum += xv * yv;
        }
    }
    acc.iter().sum::<f32>() + tail
}

/// Row-major f32 vectors, the reference [`VectorSource`].
#[derive(Clone, Debug, PartialEq)]
pub struct F32Vectors {
    dim: usize,
    data: Vec<f32>,
    metric: F32Metric,
}

impl F32Vectors {
    /// Wraps row-major `data` with `dim` values per row.
    pub fn new(dim: usize, data: Vec<f32>, metric: F32Metric) -> Result<Self, GraphError> {
        if dim == 0 {
            return Err(GraphError::InvalidVectors(
                "dimension must be positive".to_owned(),
            ));
        }
        if !data.len().is_multiple_of(dim) {
            return Err(GraphError::InvalidVectors(format!(
                "{} values do not divide into rows of dimension {dim}",
                data.len()
            )));
        }
        if data.len() / dim >= u32::MAX as usize {
            return Err(GraphError::TooLarge);
        }
        Ok(Self { dim, data, metric })
    }

    /// Values per row.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Distance function.
    pub fn metric(&self) -> F32Metric {
        self.metric
    }

    /// The vector stored at `row`.
    ///
    /// # Panics
    ///
    /// Panics when `row` is out of range.
    pub fn row(&self, row: u32) -> &[f32] {
        let start = row as usize * self.dim;
        &self.data[start..start + self.dim]
    }

    /// Builds a per-query distance for `query`, which must have [`Self::dim`]
    /// values.
    pub fn query<'a>(&'a self, query: &'a [f32]) -> Result<F32Query<'a>, GraphError> {
        if query.len() != self.dim {
            return Err(GraphError::InvalidVectors(format!(
                "query has {} values, expected {}",
                query.len(),
                self.dim
            )));
        }
        Ok(F32Query {
            vectors: self,
            query,
        })
    }
}

impl VectorSource for F32Vectors {
    fn len(&self) -> usize {
        self.data.len() / self.dim
    }

    fn distance_between(&self, a: u32, b: u32) -> f32 {
        self.metric.distance(self.row(a), self.row(b))
    }
}

/// A query against [`F32Vectors`].
#[derive(Clone, Copy, Debug)]
pub struct F32Query<'a> {
    vectors: &'a F32Vectors,
    query: &'a [f32],
}

impl QueryDistance for F32Query<'_> {
    fn distance(&self, row: u32) -> f32 {
        self.vectors
            .metric
            .distance(self.query, self.vectors.row(row))
    }
}

/// Adapts a [`VectorSource`] row into a [`QueryDistance`] for insertion.
pub(super) struct RowQuery<'a, V: ?Sized> {
    pub(super) source: &'a V,
    pub(super) row: u32,
}

impl<V: VectorSource + ?Sized> QueryDistance for RowQuery<'_, V> {
    fn distance(&self, row: u32) -> f32 {
        self.source.distance_between(self.row, row)
    }
}
