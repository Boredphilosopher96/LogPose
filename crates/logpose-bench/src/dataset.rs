//! Benchmark datasets: a deterministic clustered generator and SIFT-format loaders.

use crate::rng::SplitMix64;
use anyhow::{Context, Result, bail, ensure};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, ErrorKind, Read},
    path::{Path, PathBuf},
};

/// Similarity metric, owned by the harness so it does not depend on an engine's types.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    /// Euclidean distance; smaller is closer.
    L2,
    /// Cosine similarity; larger is closer.
    Cosine,
    /// Inner product; larger is closer.
    Dot,
}

/// Where a dataset came from, recorded in the report.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DatasetSource {
    /// Clustered synthetic vectors.
    Synthetic(SyntheticSpec),
    /// Synthetic vectors with low intrinsic dimension, shaped like text embeddings.
    EmbeddingLike(EmbeddingLikeSpec),
    /// Vectors loaded from `.fvecs` files.
    Fvecs {
        /// Base vectors file.
        base: PathBuf,
        /// Query vectors file, or `None` when queries are held out from the base file.
        queries: Option<PathBuf>,
        /// Optional reference ground truth file.
        ground_truth: Option<PathBuf>,
    },
}

/// Parameters for the clustered synthetic generator.
#[derive(Clone, Debug, Serialize)]
pub struct SyntheticSpec {
    /// Number of base rows.
    pub n: usize,
    /// Number of queries.
    pub queries: usize,
    /// Vector dimensionality.
    pub dims: usize,
    /// Number of Gaussian clusters.
    pub clusters: usize,
    /// Fraction of clusters that queries are drawn from ("hot" clusters).
    ///
    /// Concentrating queries makes anti-correlated filters meaningful: rows in
    /// cold clusters are far from every query.
    pub query_cluster_fraction: f64,
    /// Standard deviation of each point around its cluster center, relative to
    /// the unit standard deviation of the center coordinates.
    pub spread: f64,
    /// Seed for every random stream.
    pub seed: u64,
}

/// An in-memory dataset with row-major base and query vectors.
#[derive(Clone, Debug)]
pub struct Dataset {
    /// Vector dimensionality.
    pub dims: usize,
    /// Metric the collection is searched with.
    pub metric: Metric,
    /// Base vectors, `n * dims` values.
    pub base: Vec<f32>,
    /// Query vectors, `queries * dims` values.
    pub queries: Vec<f32>,
    /// Provenance recorded in the report.
    pub source: DatasetSource,
    /// Reference neighbor ids from an `.ivecs` file, when provided and valid.
    pub reference_ground_truth: Option<Vec<Vec<u64>>>,
}

impl Dataset {
    /// Number of base rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.base.len() / self.dims.max(1)
    }

    /// Whether the dataset has no base rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of queries.
    #[must_use]
    pub fn query_count(&self) -> usize {
        self.queries.len() / self.dims.max(1)
    }

    /// Base row `index`.
    #[must_use]
    pub fn row(&self, index: usize) -> &[f32] {
        &self.base[index * self.dims..(index + 1) * self.dims]
    }

    /// Query `index`.
    #[must_use]
    pub fn query(&self, index: usize) -> &[f32] {
        &self.queries[index * self.dims..(index + 1) * self.dims]
    }
}

const STREAM_CENTERS: u64 = 1;
const STREAM_BASE: u64 = 2;
const STREAM_QUERIES: u64 = 3;

/// Generate a clustered Gaussian dataset.
///
/// Centers, base rows, and queries use separate random streams, so changing
/// `n` leaves the centers and the queries unchanged.
pub fn generate_synthetic(spec: &SyntheticSpec, metric: Metric) -> Result<Dataset> {
    ensure!(spec.dims > 0, "dims must be positive");
    ensure!(spec.clusters > 0, "clusters must be positive");
    ensure!(
        spec.query_cluster_fraction > 0.0 && spec.query_cluster_fraction <= 1.0,
        "query cluster fraction must be in (0, 1]"
    );

    let mut center_rng = SplitMix64::stream(spec.seed, STREAM_CENTERS);
    let centers = (0..spec.clusters * spec.dims)
        .map(|_| center_rng.gaussian())
        .collect::<Vec<_>>();

    let sample = |rng: &mut SplitMix64, cluster_count: usize, out: &mut Vec<f32>| {
        let cluster = rng.below(cluster_count as u64) as usize;
        let center = &centers[cluster * spec.dims..(cluster + 1) * spec.dims];
        out.extend(
            center
                .iter()
                .map(|value| (value + spec.spread * rng.gaussian()) as f32),
        );
    };

    let mut base_rng = SplitMix64::stream(spec.seed, STREAM_BASE);
    let mut base = Vec::with_capacity(spec.n * spec.dims);
    for _ in 0..spec.n {
        sample(&mut base_rng, spec.clusters, &mut base);
    }

    let hot_clusters = ((spec.clusters as f64 * spec.query_cluster_fraction).ceil() as usize)
        .clamp(1, spec.clusters);
    let mut query_rng = SplitMix64::stream(spec.seed, STREAM_QUERIES);
    let mut queries = Vec::with_capacity(spec.queries * spec.dims);
    for _ in 0..spec.queries {
        sample(&mut query_rng, hot_clusters, &mut queries);
    }

    Ok(Dataset {
        dims: spec.dims,
        metric,
        base,
        queries,
        source: DatasetSource::Synthetic(spec.clone()),
        reference_ground_truth: None,
    })
}

/// Parameters for the embedding-like synthetic generator.
///
/// Real text embeddings have far fewer degrees of freedom than dimensions, and an
/// isotropic Gaussian in hundreds of dimensions is a pathological case for graph
/// indexes (every point is nearly equidistant from every other). This generator
/// draws a Gaussian mixture in `latent_dims` dimensions, maps it to `dims`
/// dimensions with a fixed random linear projection, and adds a little isotropic
/// noise, so nearest neighbors are well defined the way they are for real
/// embeddings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingLikeSpec {
    /// Number of base rows.
    pub n: usize,
    /// Number of queries, drawn from the same distribution as the base rows.
    pub queries: usize,
    /// Output dimensionality.
    pub dims: usize,
    /// Dimensionality of the latent space.
    pub latent_dims: usize,
    /// Gaussian clusters in the latent space.
    pub clusters: usize,
    /// Standard deviation of latent points around their cluster center, relative to the
    /// unit standard deviation of the centers.
    pub spread: f64,
    /// Standard deviation of the isotropic noise added to each output component.
    pub noise: f64,
    /// Seed for every random stream.
    pub seed: u64,
}

const STREAM_LATENT_CENTERS: u64 = 11;
const STREAM_PROJECTION: u64 = 12;
const STREAM_LATENT_BASE: u64 = 13;
const STREAM_LATENT_QUERIES: u64 = 14;

/// Generate an embedding-like dataset (see [`EmbeddingLikeSpec`]).
///
/// Centers, the projection, base rows, and queries use separate random streams, so
/// changing `n` leaves the queries and the leading rows unchanged.
pub fn generate_embedding_like(spec: &EmbeddingLikeSpec, metric: Metric) -> Result<Dataset> {
    ensure!(spec.dims > 0, "dims must be positive");
    ensure!(spec.latent_dims > 0, "latent dims must be positive");
    ensure!(spec.clusters > 0, "clusters must be positive");

    let latent = spec.latent_dims;
    let mut center_rng = SplitMix64::stream(spec.seed, STREAM_LATENT_CENTERS);
    let centers = (0..spec.clusters * latent)
        .map(|_| center_rng.gaussian())
        .collect::<Vec<_>>();
    let scale = 1.0 / (latent as f64).sqrt();
    let mut projection_rng = SplitMix64::stream(spec.seed, STREAM_PROJECTION);
    // Row-major `latent x dims`.
    let projection = (0..latent * spec.dims)
        .map(|_| projection_rng.gaussian() * scale)
        .collect::<Vec<_>>();

    let sample = |rng: &mut SplitMix64, point: &mut Vec<f64>, out: &mut Vec<f32>| {
        let cluster = rng.below(spec.clusters as u64) as usize;
        let center = &centers[cluster * latent..(cluster + 1) * latent];
        point.clear();
        point.extend(
            center
                .iter()
                .map(|value| value + spec.spread * rng.gaussian()),
        );
        let mut row = vec![0.0_f64; spec.dims];
        for (weight, basis) in point.iter().zip(projection.chunks_exact(spec.dims)) {
            for (component, value) in row.iter_mut().zip(basis) {
                *component += weight * value;
            }
        }
        out.extend(
            row.into_iter()
                .map(|value| (value + spec.noise * rng.gaussian()) as f32),
        );
    };

    let mut point = Vec::with_capacity(latent);
    let mut base_rng = SplitMix64::stream(spec.seed, STREAM_LATENT_BASE);
    let mut base = Vec::with_capacity(spec.n * spec.dims);
    for _ in 0..spec.n {
        sample(&mut base_rng, &mut point, &mut base);
    }
    let mut query_rng = SplitMix64::stream(spec.seed, STREAM_LATENT_QUERIES);
    let mut queries = Vec::with_capacity(spec.queries * spec.dims);
    for _ in 0..spec.queries {
        sample(&mut query_rng, &mut point, &mut queries);
    }

    Ok(Dataset {
        dims: spec.dims,
        metric,
        base,
        queries,
        source: DatasetSource::EmbeddingLike(spec.clone()),
        reference_ground_truth: None,
    })
}

/// Paths and limits for loading an `.fvecs` dataset.
#[derive(Clone, Debug)]
pub struct FvecsSpec {
    /// Base vectors file.
    pub base: PathBuf,
    /// Query vectors file; when absent, the last `queries` base rows are held out.
    pub queries: Option<PathBuf>,
    /// Optional reference ground truth `.ivecs` file.
    pub ground_truth: Option<PathBuf>,
    /// Maximum number of base rows to use.
    pub limit_n: Option<usize>,
    /// Number of queries to use.
    pub query_count: usize,
}

/// Load a SIFT-format dataset (`.fvecs` base and queries, optional `.ivecs` ground truth).
///
/// Reference ground truth is kept only when the base set is not truncated,
/// because truncation changes the true neighbors.
pub fn load_fvecs_dataset(spec: &FvecsSpec, metric: Metric) -> Result<Dataset> {
    let (dims, mut base) = match &spec.queries {
        Some(_) => read_fvecs(&spec.base, spec.limit_n)?,
        None => read_fvecs(
            &spec.base,
            spec.limit_n.map(|limit| limit + spec.query_count),
        )?,
    };
    let (queries, truncated) = match &spec.queries {
        Some(path) => {
            let (query_dims, queries) = read_fvecs(path, Some(spec.query_count))?;
            ensure!(
                query_dims == dims,
                "query dimensionality {query_dims} does not match base dimensionality {dims}"
            );
            let full_len = count_vectors(&spec.base, dims)?;
            (queries, base.len() / dims < full_len)
        }
        None => {
            let rows = base.len() / dims;
            ensure!(
                rows > spec.query_count,
                "base file has {rows} rows, not enough to hold out {} queries",
                spec.query_count
            );
            let split = (rows - spec.query_count) * dims;
            let queries = base.split_off(split);
            (queries, true)
        }
    };

    let reference_ground_truth = match (&spec.ground_truth, truncated) {
        (Some(path), false) => Some(read_ivecs(path, Some(queries.len() / dims))?),
        _ => None,
    };

    Ok(Dataset {
        dims,
        metric,
        base,
        queries,
        source: DatasetSource::Fvecs {
            base: spec.base.clone(),
            queries: spec.queries.clone(),
            ground_truth: spec.ground_truth.clone(),
        },
        reference_ground_truth,
    })
}

fn count_vectors(path: &Path, dims: usize) -> Result<usize> {
    let bytes = std::fs::metadata(path)
        .with_context(|| format!("reading metadata for {}", path.display()))?
        .len();
    Ok((bytes / (4 + 4 * dims as u64)) as usize)
}

/// Read up to `limit` vectors from an `.fvecs` file.
///
/// Each record is a little-endian `i32` dimension followed by that many `f32` values.
pub fn read_fvecs(path: &Path, limit: Option<usize>) -> Result<(usize, Vec<f32>)> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    read_vecs(BufReader::new(file), limit, f32::from_le_bytes)
        .with_context(|| format!("reading {}", path.display()))
}

/// Read up to `limit` rows from an `.ivecs` file as neighbor ids.
pub fn read_ivecs(path: &Path, limit: Option<usize>) -> Result<Vec<Vec<u64>>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let (dims, values) = read_vecs(BufReader::new(file), limit, i32::from_le_bytes)
        .with_context(|| format!("reading {}", path.display()))?;
    values
        .chunks(dims.max(1))
        .map(|row| {
            row.iter()
                .map(|value| u64::try_from(*value).context("negative neighbor id in ivecs"))
                .collect()
        })
        .collect()
}

/// Decode `.fvecs`-style records from a reader.
pub fn read_vecs<R, T>(
    mut reader: R,
    limit: Option<usize>,
    decode: fn([u8; 4]) -> T,
) -> Result<(usize, Vec<T>)>
where
    R: Read,
{
    let mut dims = None;
    let mut values = Vec::new();
    let mut rows = 0_usize;
    let mut header = [0_u8; 4];
    while limit.is_none_or(|limit| rows < limit) {
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
        let row_dims = usize::try_from(i32::from_le_bytes(header))
            .context("negative dimension in vecs header")?;
        if row_dims == 0 {
            bail!("zero dimension in vecs header at row {rows}");
        }
        match dims {
            None => dims = Some(row_dims),
            Some(expected) if expected != row_dims => {
                bail!("row {rows} has dimension {row_dims}, expected {expected}")
            }
            Some(_) => {}
        }
        let mut row = vec![0_u8; row_dims * 4];
        reader
            .read_exact(&mut row)
            .with_context(|| format!("truncated vecs row {rows}"))?;
        values.extend(
            row.chunks_exact(4)
                .map(|chunk| decode([chunk[0], chunk[1], chunk[2], chunk[3]])),
        );
        rows += 1;
    }
    let Some(dims) = dims else {
        bail!("vecs input contains no rows");
    };
    Ok((dims, values))
}

#[cfg(test)]
mod tests {
    use super::{
        EmbeddingLikeSpec, Metric, SyntheticSpec, generate_embedding_like, generate_synthetic,
        read_vecs,
    };

    fn spec(n: usize, seed: u64) -> SyntheticSpec {
        SyntheticSpec {
            n,
            queries: 8,
            dims: 6,
            clusters: 4,
            query_cluster_fraction: 0.5,
            spread: 0.2,
            seed,
        }
    }

    #[test]
    fn generator_is_deterministic_for_a_seed() -> anyhow::Result<()> {
        let first = generate_synthetic(&spec(100, 9), Metric::L2)?;
        let second = generate_synthetic(&spec(100, 9), Metric::L2)?;
        assert_eq!(first.base, second.base);
        assert_eq!(first.queries, second.queries);
        assert_eq!(first.len(), 100);
        assert_eq!(first.query_count(), 8);

        let other = generate_synthetic(&spec(100, 10), Metric::L2)?;
        assert_ne!(first.base, other.base);
        Ok(())
    }

    #[test]
    fn queries_and_prefix_rows_do_not_depend_on_n() -> anyhow::Result<()> {
        let small = generate_synthetic(&spec(50, 9), Metric::L2)?;
        let large = generate_synthetic(&spec(200, 9), Metric::L2)?;
        assert_eq!(small.queries, large.queries);
        assert_eq!(small.base[..], large.base[..small.base.len()]);
        Ok(())
    }

    fn embedding_spec(n: usize) -> EmbeddingLikeSpec {
        EmbeddingLikeSpec {
            n,
            queries: 6,
            dims: 24,
            latent_dims: 4,
            clusters: 3,
            spread: 1.0,
            noise: 0.05,
            seed: 7,
        }
    }

    #[test]
    fn embedding_like_generator_is_deterministic_and_prefix_stable() -> anyhow::Result<()> {
        let small = generate_embedding_like(&embedding_spec(40), Metric::Cosine)?;
        let again = generate_embedding_like(&embedding_spec(40), Metric::Cosine)?;
        let large = generate_embedding_like(&embedding_spec(90), Metric::Cosine)?;
        assert_eq!(small.base, again.base);
        assert_eq!(small.len(), 40);
        assert_eq!(small.query_count(), 6);
        assert_eq!(small.queries, large.queries);
        assert_eq!(small.base[..], large.base[..small.base.len()]);
        assert!(small.base.iter().all(|value| value.is_finite()));
        Ok(())
    }

    #[test]
    fn embedding_like_rows_live_near_a_low_dimensional_subspace() -> anyhow::Result<()> {
        // With tiny noise, every row is a combination of `latent_dims` basis vectors, so the
        // residual after projecting on the span of the first rows is small.
        let spec = EmbeddingLikeSpec {
            noise: 0.0,
            ..embedding_spec(12)
        };
        let dataset = generate_embedding_like(&spec, Metric::L2)?;
        // Gram-Schmidt over the first rows; the rank never exceeds `latent_dims`.
        let mut basis: Vec<Vec<f64>> = Vec::new();
        for row in 0..dataset.len() {
            let mut residual = dataset
                .row(row)
                .iter()
                .map(|v| f64::from(*v))
                .collect::<Vec<_>>();
            for vector in &basis {
                let dot = residual.iter().zip(vector).map(|(a, b)| a * b).sum::<f64>();
                for (value, basis_value) in residual.iter_mut().zip(vector) {
                    *value -= dot * basis_value;
                }
            }
            let norm = residual.iter().map(|v| v * v).sum::<f64>().sqrt();
            if norm > 1e-3 {
                basis.push(residual.iter().map(|v| v / norm).collect());
            }
        }
        assert!(basis.len() <= spec.latent_dims, "rank {}", basis.len());
        Ok(())
    }

    #[test]
    fn reads_fvecs_records_and_respects_limit() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        for row in [[1.0_f32, 2.0], [3.0, 4.0], [5.0, 6.0]] {
            bytes.extend(2_i32.to_le_bytes());
            for value in row {
                bytes.extend(value.to_le_bytes());
            }
        }
        let (dims, values) = read_vecs(bytes.as_slice(), None, f32::from_le_bytes)?;
        assert_eq!(dims, 2);
        assert_eq!(values, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);

        let (_, limited) = read_vecs(bytes.as_slice(), Some(1), f32::from_le_bytes)?;
        assert_eq!(limited, vec![1.0, 2.0]);

        assert!(read_vecs(&bytes[..10], None, f32::from_le_bytes).is_err());
        Ok(())
    }
}
