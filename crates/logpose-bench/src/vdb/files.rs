//! Prepared dataset directories shared by every benchmark driver.
//!
//! A prepared directory holds everything a driver needs to load and query a
//! system and to score its answers, in formats any language can read:
//!
//! | File | Contents |
//! | --- | --- |
//! | `dataset.json` | the [`Manifest`]: shape, generator parameters, cases |
//! | `base.fvecs` | base vectors, row `i` has primary key `i` |
//! | `queries.fvecs` | query vectors |
//! | `rank.i64` | the `rank` scalar of every base row, little-endian `i64` |
//! | `gt-<case>.ivecs` | exact top-k row ids per query for each case, best first |
//!
//! `.fvecs` and `.ivecs` are the SIFT formats: each row is a little-endian `i32`
//! length followed by that many `f32` or `i32` values.
//!
//! The manifest is written last, so a directory with a manifest is complete. A
//! directory whose manifest matches the requested shape is reused as is.

use crate::{
    dataset::{
        Dataset, DatasetSource, EmbeddingLikeSpec, Metric, generate_embedding_like, read_fvecs,
        read_vecs,
    },
    filter::format_percent,
    oracle::ground_truth,
    rng::SplitMix64,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};

/// Version of the directory layout; bump when a file changes meaning.
pub const FORMAT_VERSION: u32 = 1;
/// Manifest file name.
pub const MANIFEST_FILE: &str = "dataset.json";
/// Name of the integer scalar field filters read.
pub const SCALAR_FIELD: &str = "rank";

const BASE_FILE: &str = "base.fvecs";
const QUERIES_FILE: &str = "queries.fvecs";
const SCALAR_FILE: &str = "rank.i64";
/// Random stream for the `rank` permutation.
const STREAM_RANK: u64 = 301;

/// Description of a prepared dataset directory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Directory layout version.
    pub format: u32,
    /// Shape name, such as `cohere-100k`.
    pub name: String,
    /// Whether the vectors are synthetic. Always true today; recorded so that a
    /// report can never present synthetic data as a public dataset.
    pub synthetic: bool,
    /// Public dataset this shape mirrors (rows, dimensions, metric), if any.
    pub mirrors: Option<String>,
    /// Generator parameters.
    pub generator: EmbeddingLikeSpec,
    /// Search metric.
    pub metric: Metric,
    /// Base rows.
    pub n: usize,
    /// Vector dimensionality.
    pub dims: usize,
    /// Number of queries.
    pub queries: usize,
    /// Neighbors per query in the ground truth.
    pub k: usize,
    /// Integer scalar field filters read. Its values are a seeded random
    /// permutation of `0..n`, so `rank < t` matches exactly `t` rows, uncorrelated
    /// with the vectors.
    pub scalar_field: String,
    /// Base vectors file.
    pub base_file: String,
    /// Query vectors file.
    pub queries_file: String,
    /// Scalar column file.
    pub scalar_file: String,
    /// Search cases with their ground truth.
    pub cases: Vec<CaseSpec>,
    /// Seconds spent generating vectors.
    pub generate_seconds: f64,
    /// Seconds spent computing ground truth.
    pub oracle_seconds: f64,
}

/// One search case of a prepared dataset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CaseSpec {
    /// Case name: `unfiltered` or `filter-<percent>pct`.
    pub name: String,
    /// Requested fraction of rows the filter matches; `None` when unfiltered.
    pub selectivity: Option<f64>,
    /// The filter is `rank < filter_lt`; `None` when unfiltered.
    pub filter_lt: Option<i64>,
    /// Rows the filter matches.
    pub matching_rows: usize,
    /// Ground truth file.
    pub ground_truth_file: String,
}

/// A prepared dataset loaded into memory.
#[derive(Clone, Debug)]
pub struct Prepared {
    /// Directory it was loaded from.
    pub dir: PathBuf,
    /// Its manifest.
    pub manifest: Manifest,
    /// Base and query vectors.
    pub dataset: Dataset,
    /// The `rank` value of every base row.
    pub ranks: Vec<i64>,
    /// Ground truth per case, in manifest order: per query, row ids best first.
    pub truths: Vec<Vec<Vec<u64>>>,
}

/// What to prepare.
#[derive(Clone, Debug)]
pub struct PrepareRequest {
    /// Shape name, used as the directory name.
    pub name: String,
    /// Public dataset the shape mirrors.
    pub mirrors: Option<String>,
    /// Generator parameters.
    pub generator: EmbeddingLikeSpec,
    /// Search metric.
    pub metric: Metric,
    /// Neighbors per query.
    pub k: usize,
    /// Filter selectivities, as fractions of rows.
    pub selectivities: Vec<f64>,
}

impl PrepareRequest {
    fn cases(&self) -> Result<Vec<CaseSpec>> {
        let n = self.generator.n;
        let mut cases = vec![CaseSpec {
            name: "unfiltered".to_owned(),
            selectivity: None,
            filter_lt: None,
            matching_rows: n,
            ground_truth_file: "gt-unfiltered.ivecs".to_owned(),
        }];
        for selectivity in &self.selectivities {
            ensure!(
                *selectivity > 0.0 && *selectivity <= 1.0,
                "selectivity {selectivity} must be in (0, 1]"
            );
            let matching_rows = ((selectivity * n as f64).ceil() as usize).clamp(1, n.max(1));
            let name = format!(
                "filter-{}pct",
                format_percent(*selectivity).replace('.', "_")
            );
            ensure!(
                cases.iter().all(|case| case.name != name),
                "selectivity {selectivity} repeats case {name}"
            );
            cases.push(CaseSpec {
                ground_truth_file: format!("gt-{name}.ivecs"),
                name,
                selectivity: Some(*selectivity),
                filter_lt: Some(i64::try_from(matching_rows)?),
                matching_rows,
            });
        }
        Ok(cases)
    }

    /// Whether an existing manifest describes exactly this request.
    fn matches(&self, manifest: &Manifest) -> Result<bool> {
        Ok(manifest.format == FORMAT_VERSION
            && manifest.name == self.name
            && manifest.generator == self.generator
            && manifest.metric == self.metric
            && manifest.k == self.k
            && manifest.cases.len() == self.selectivities.len() + 1
            && manifest
                .cases
                .iter()
                .zip(self.cases()?)
                .all(|(have, want)| {
                    have.name == want.name
                        && have.filter_lt == want.filter_lt
                        && have.ground_truth_file == want.ground_truth_file
                }))
    }
}

fn log(message: impl AsRef<str>) {
    eprintln!("[logpose-bench vdb] {}", message.as_ref());
}

/// Prepare `request` under `root/<name>`, reusing a matching directory unless `force`.
pub fn prepare(root: &Path, request: &PrepareRequest, force: bool) -> Result<Prepared> {
    let dir = root.join(&request.name);
    let manifest_path = dir.join(MANIFEST_FILE);
    if !force && manifest_path.exists() {
        let manifest = read_manifest(&dir)?;
        if request.matches(&manifest)? {
            log(format!("reusing prepared dataset {}", dir.display()));
            return load(&dir);
        }
        log(format!(
            "{} was prepared with other parameters; regenerating",
            dir.display()
        ));
    }
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    // Remove the manifest first so a crash mid-write never leaves a stale match.
    if manifest_path.exists() {
        std::fs::remove_file(&manifest_path)
            .with_context(|| format!("removing {}", manifest_path.display()))?;
    }

    log(format!(
        "generating {} ({} rows, {} dims, {} queries)",
        request.name, request.generator.n, request.generator.dims, request.generator.queries
    ));
    let started = Instant::now();
    let dataset = generate_embedding_like(&request.generator, request.metric)?;
    let ranks = rank_column(dataset.len(), request.generator.seed);
    let generate_seconds = started.elapsed().as_secs_f64();
    write_vecs(&dir.join(BASE_FILE), dataset.dims, &dataset.base, |v| {
        v.to_le_bytes()
    })?;
    write_vecs(
        &dir.join(QUERIES_FILE),
        dataset.dims,
        &dataset.queries,
        |v| v.to_le_bytes(),
    )?;
    write_i64(&dir.join(SCALAR_FILE), &ranks)?;

    let cases = request.cases()?;
    let started = Instant::now();
    let oracle_input = oracle_view(&dataset);
    let mut truths = Vec::with_capacity(cases.len());
    for case in &cases {
        log(format!("computing exact ground truth for {}", case.name));
        let truth = match case.filter_lt {
            None => ground_truth(&oracle_input, request.k, |_| true),
            Some(limit) => ground_truth(&oracle_input, request.k, |row| ranks[row] < limit),
        };
        write_ground_truth(&dir.join(&case.ground_truth_file), request.k, &truth)?;
        truths.push(truth);
    }
    let oracle_seconds = started.elapsed().as_secs_f64();

    let manifest = Manifest {
        format: FORMAT_VERSION,
        name: request.name.clone(),
        synthetic: true,
        mirrors: request.mirrors.clone(),
        generator: request.generator.clone(),
        metric: request.metric,
        n: dataset.len(),
        dims: dataset.dims,
        queries: dataset.query_count(),
        k: request.k,
        scalar_field: SCALAR_FIELD.to_owned(),
        base_file: BASE_FILE.to_owned(),
        queries_file: QUERIES_FILE.to_owned(),
        scalar_file: SCALAR_FILE.to_owned(),
        cases,
        generate_seconds,
        oracle_seconds,
    };
    let mut json = serde_json::to_string_pretty(&manifest)?;
    json.push('\n');
    let staged = dir.join(format!("{MANIFEST_FILE}.tmp"));
    std::fs::write(&staged, json).with_context(|| format!("writing {}", staged.display()))?;
    std::fs::rename(&staged, &manifest_path)
        .with_context(|| format!("publishing {}", manifest_path.display()))?;
    log(format!(
        "prepared {} in {:.1} s (ground truth {:.1} s)",
        dir.display(),
        generate_seconds + oracle_seconds,
        oracle_seconds
    ));
    Ok(Prepared {
        dir,
        manifest,
        dataset,
        ranks,
        truths,
    })
}

fn read_manifest(dir: &Path) -> Result<Manifest> {
    let path = dir.join(MANIFEST_FILE);
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Load a prepared directory.
pub fn load(dir: &Path) -> Result<Prepared> {
    let manifest = read_manifest(dir)?;
    ensure!(
        manifest.format == FORMAT_VERSION,
        "{} has format {}, expected {FORMAT_VERSION}; prepare it again",
        dir.display(),
        manifest.format
    );
    let (dims, base) = read_fvecs(&dir.join(&manifest.base_file), None)?;
    let (query_dims, queries) = read_fvecs(&dir.join(&manifest.queries_file), None)?;
    ensure!(
        dims == manifest.dims && query_dims == manifest.dims,
        "vector files do not match the manifest's {} dimensions",
        manifest.dims
    );
    let dataset = Dataset {
        dims,
        metric: manifest.metric,
        base,
        queries,
        source: DatasetSource::EmbeddingLike(manifest.generator.clone()),
        reference_ground_truth: None,
    };
    ensure!(
        dataset.len() == manifest.n && dataset.query_count() == manifest.queries,
        "vector files hold {} rows and {} queries, the manifest says {} and {}",
        dataset.len(),
        dataset.query_count(),
        manifest.n,
        manifest.queries
    );
    let ranks = read_i64(&dir.join(&manifest.scalar_file))?;
    ensure!(
        ranks.len() == manifest.n,
        "scalar file holds {} values, expected {}",
        ranks.len(),
        manifest.n
    );
    let truths = manifest
        .cases
        .iter()
        .map(|case| {
            let truth = read_ground_truth(&dir.join(&case.ground_truth_file))?;
            ensure!(
                truth.len() == manifest.queries,
                "{} holds {} queries, expected {}",
                case.ground_truth_file,
                truth.len(),
                manifest.queries
            );
            Ok(truth)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Prepared {
        dir: dir.to_path_buf(),
        manifest,
        dataset,
        ranks,
        truths,
    })
}

/// A seeded random permutation of `0..n`.
#[must_use]
pub fn rank_column(n: usize, seed: u64) -> Vec<i64> {
    let mut ranks = (0..n as i64).collect::<Vec<_>>();
    SplitMix64::stream(seed, STREAM_RANK).shuffle(&mut ranks);
    ranks
}

/// The dataset the oracle scores: cosine vectors normalized once and scored by dot
/// product, which ranks identically and avoids recomputing norms per pair.
fn oracle_view(dataset: &Dataset) -> Dataset {
    if dataset.metric != Metric::Cosine {
        return dataset.clone();
    }
    let normalize = |values: &[f32]| {
        let mut out = Vec::with_capacity(values.len());
        for row in values.chunks_exact(dataset.dims) {
            let norm = row.iter().map(|v| v * v).sum::<f32>().sqrt();
            let scale = if norm > 0.0 { 1.0 / norm } else { 0.0 };
            out.extend(row.iter().map(|v| v * scale));
        }
        out
    };
    Dataset {
        metric: Metric::Dot,
        base: normalize(&dataset.base),
        queries: normalize(&dataset.queries),
        ..dataset.clone()
    }
}

fn write_vecs<T: Copy>(
    path: &Path,
    dims: usize,
    values: &[T],
    encode: fn(T) -> [u8; 4],
) -> Result<()> {
    let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut out = BufWriter::new(file);
    let header = i32::try_from(dims)?.to_le_bytes();
    for row in values.chunks(dims.max(1)) {
        out.write_all(&header)?;
        for value in row {
            out.write_all(&encode(*value))?;
        }
    }
    out.flush()
        .with_context(|| format!("writing {}", path.display()))
}

fn write_ground_truth(path: &Path, k: usize, truth: &[Vec<u64>]) -> Result<()> {
    // Rows shorter than k (a filter matching fewer rows) are padded with -1.
    let mut values = Vec::with_capacity(truth.len() * k);
    for row in truth {
        ensure!(row.len() <= k, "ground truth row longer than k");
        for id in row {
            values.push(i32::try_from(*id).context("row id does not fit ivecs")?);
        }
        values.extend(std::iter::repeat_n(-1, k - row.len()));
    }
    write_vecs(path, k, &values, i32::to_le_bytes)
}

fn read_ground_truth(path: &Path) -> Result<Vec<Vec<u64>>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let (k, values) = read_vecs(BufReader::new(file), None, i32::from_le_bytes)
        .with_context(|| format!("reading {}", path.display()))?;
    Ok(values
        .chunks(k)
        .map(|row| {
            row.iter()
                .filter_map(|id| u64::try_from(*id).ok())
                .collect::<Vec<_>>()
        })
        .collect())
}

fn write_i64(path: &Path, values: &[i64]) -> Result<()> {
    let mut bytes = Vec::with_capacity(values.len() * 8);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))
}

fn read_i64(path: &Path) -> Result<Vec<i64>> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if bytes.len() % 8 != 0 {
        bail!("{} is not a whole number of i64 values", path.display());
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|chunk| {
            let mut word = [0_u8; 8];
            word.copy_from_slice(chunk);
            i64::from_le_bytes(word)
        })
        .collect())
}

/// A fresh temp directory named `logpose-vdb-{label}-…` for a test, removed when the returned
/// guard drops, also when the test panics.
#[cfg(test)]
pub(crate) fn scratch_dir(label: &str) -> std::io::Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix(&format!("logpose-vdb-{label}-"))
        .tempdir()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{PrepareRequest, load, prepare, rank_column, scratch_dir};
    use crate::dataset::{EmbeddingLikeSpec, Metric};

    pub(crate) fn request() -> PrepareRequest {
        PrepareRequest {
            name: "tiny".to_owned(),
            mirrors: None,
            generator: EmbeddingLikeSpec {
                n: 300,
                queries: 7,
                dims: 12,
                latent_dims: 4,
                clusters: 5,
                spread: 1.0,
                noise: 0.1,
                seed: 3,
            },
            metric: Metric::Cosine,
            k: 5,
            selectivities: vec![0.01, 0.99],
        }
    }

    #[test]
    fn rank_column_is_a_permutation() {
        let mut ranks = rank_column(1_000, 9);
        assert_ne!(ranks, (0..1_000).collect::<Vec<_>>());
        ranks.sort_unstable();
        assert_eq!(ranks, (0..1_000).collect::<Vec<_>>());
    }

    #[test]
    fn prepares_round_trips_and_reuses_a_directory() -> anyhow::Result<()> {
        let root_dir = scratch_dir("prepare")?;
        let root = root_dir.path().to_path_buf();
        let request = request();
        let first = prepare(&root, &request, false)?;
        assert_eq!(first.manifest.cases.len(), 3);
        assert_eq!(first.manifest.cases[1].name, "filter-1pct");
        assert_eq!(first.manifest.cases[1].filter_lt, Some(3));
        assert_eq!(first.manifest.cases[2].filter_lt, Some(297));
        // The 1% filter matches three rows, so its truth is three long.
        assert!(first.truths[1].iter().all(|row| row.len() == 3));
        for row in &first.truths[1] {
            assert!(row.iter().all(|id| first.ranks[*id as usize] < 3));
        }

        let loaded = load(&first.dir)?;
        assert_eq!(loaded.manifest, first.manifest);
        assert_eq!(loaded.dataset.base, first.dataset.base);
        assert_eq!(loaded.dataset.queries, first.dataset.queries);
        assert_eq!(loaded.ranks, first.ranks);
        assert_eq!(loaded.truths, first.truths);

        // A matching request reuses the directory; the manifest is not rewritten.
        let manifest_path = first.dir.join(super::MANIFEST_FILE);
        let written = std::fs::metadata(&manifest_path)?.modified()?;
        let again = prepare(&root, &request, false)?;
        assert_eq!(again.manifest, first.manifest);
        assert_eq!(std::fs::metadata(&manifest_path)?.modified()?, written);

        // Different parameters regenerate.
        let other = PrepareRequest {
            k: 3,
            ..request.clone()
        };
        let regenerated = prepare(&root, &other, false)?;
        assert_eq!(regenerated.manifest.k, 3);
        assert!(regenerated.truths[0].iter().all(|row| row.len() == 3));
        Ok(())
    }
}
