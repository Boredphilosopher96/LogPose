//! Exact and ANN index sidecars for immutable units.

pub mod graph;
pub mod kernels;
pub mod scalar;
pub mod sq8;

#[cfg(test)]
use criterion as _;

use logpose_types::{DistanceMetric, RecordId, ScalarFieldStats, ScalarMetadataValue, SeqNo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap, HashSet},
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    io,
    path::Path,
};

mod durable;

/// Index family available for a queryable unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    /// Hierarchical navigable small world graph.
    Hnsw,
    /// Inverted file with product quantization.
    IvfPq,
    /// Brute-force exact search path.
    Flat,
}

impl IndexKind {
    /// Render the index kind as a stable string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Hnsw => "hnsw",
            Self::IvfPq => "ivf_pq",
            Self::Flat => "flat",
        }
    }
}

/// File-backed exact sidecar for immutable flat retrieval.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlatIndexSidecar {
    /// Sidecar version.
    pub version: u16,
    /// Segment identifier this sidecar belongs to.
    pub segment_id: String,
    /// Index family represented by the sidecar.
    pub index_kind: IndexKind,
    /// Total segment entry count, including tombstones.
    pub entry_count: usize,
    /// Number of put entries represented by the sidecar.
    pub put_count: usize,
    /// Number of delete entries represented by the sidecar.
    pub delete_count: usize,
    /// Stable offsets into the segment payload sections.
    pub entry_offsets: Vec<FlatIndexOffset>,
    /// Precomputed vector norms for put entries.
    pub vector_norms: Vec<Option<f32>>,
    /// Scalar field summaries over top-level metadata fields.
    pub scalar_fields: BTreeMap<String, ScalarFieldStats>,
}

/// Offsets into the immutable segment payload sections.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FlatIndexOffset {
    /// Offset into the record id section.
    pub record_id_offset: u64,
    /// Offset into the vector section.
    pub vector_offset: u64,
    /// Offset into the metadata section.
    pub metadata_offset: u64,
}

/// Builder input for one segment entry.
#[derive(Clone, Debug)]
pub struct FlatIndexEntrySource {
    /// Whether the segment entry is a put.
    pub is_put: bool,
    /// Offset into the record id section.
    pub record_id_offset: u64,
    /// Offset into the vector section.
    pub vector_offset: u64,
    /// Offset into the metadata section.
    pub metadata_offset: u64,
    /// Raw vector for put entries.
    pub vector: Option<Vec<f32>>,
    /// Top-level metadata JSON for put entries.
    pub metadata: Option<Value>,
}

/// Build a persisted flat exact sidecar from segment entries.
#[must_use]
pub fn build_flat_index(
    segment_id: impl Into<String>,
    entries: &[FlatIndexEntrySource],
) -> FlatIndexSidecar {
    let mut put_count = 0usize;
    let mut delete_count = 0usize;
    let mut vector_norms = Vec::with_capacity(entries.len());
    let mut entry_offsets = Vec::with_capacity(entries.len());
    let mut scalar_fields = BTreeMap::<String, ScalarFieldStats>::new();

    for entry in entries {
        entry_offsets.push(FlatIndexOffset {
            record_id_offset: entry.record_id_offset,
            vector_offset: entry.vector_offset,
            metadata_offset: entry.metadata_offset,
        });

        if entry.is_put {
            put_count += 1;
            vector_norms.push(entry.vector.as_deref().map(kernels::norm));
            if let Some(metadata) = &entry.metadata {
                update_scalar_field_stats(&mut scalar_fields, metadata);
            }
        } else {
            delete_count += 1;
            vector_norms.push(None);
        }
    }

    FlatIndexSidecar {
        version: 1,
        segment_id: segment_id.into(),
        index_kind: IndexKind::Flat,
        entry_count: entries.len(),
        put_count,
        delete_count,
        entry_offsets,
        vector_norms,
        scalar_fields,
    }
}

/// Durably persist a flat exact sidecar to disk.
///
/// The write is atomic: readers see either the previous file or the complete new one, and the
/// contents and directory entry are fsynced before this returns.
pub fn write_flat_index(path: &Path, sidecar: &FlatIndexSidecar) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(sidecar)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    durable::write_atomic(path, &bytes)
}

/// Load a flat exact sidecar from disk.
pub fn read_flat_index(path: &Path) -> io::Result<FlatIndexSidecar> {
    serde_json::from_slice(&fs::read(path)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

const HNSW_MAGIC: &[u8; 4] = b"LPH1";
/// Sidecar layout and graph-semantics version.
///
/// Version 2 draws node levels with `mL = 1 / ln(M)`, bounds layer 0 at `2 * M` neighbors, and
/// picks neighbors with the diversity heuristic. Version 1 graphs were built with a different
/// level distribution and degree bound, so they are rejected rather than reinterpreted.
const HNSW_VERSION: u16 = 2;
/// Upper bound on node levels. With `M >= 2`, a level this high has probability at most
/// `2^-16` per node, and clamping the rare outlier only costs it a little extra reach.
const MAX_HNSW_LEVEL: u8 = 16;

/// Deterministic build parameters for the persisted HNSW sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HnswBuildParams {
    /// Maximum neighbors kept per node on layers above 0 (`M`).
    ///
    /// Layer 0 keeps up to `2 * M` neighbors, and node levels are drawn with `mL = 1 / ln(M)`.
    /// Must be at least 2.
    pub max_neighbors: usize,
    /// Search breadth used while linking new nodes. Must be at least 1.
    pub ef_construction: usize,
    /// Default search breadth used at query time. Must be at least 1.
    pub ef_search: usize,
}

impl Default for HnswBuildParams {
    fn default() -> Self {
        Self {
            max_neighbors: 16,
            ef_construction: 128,
            ef_search: 64,
        }
    }
}

impl HnswBuildParams {
    /// Maximum neighbor count allowed on `layer`: `2 * M` on layer 0 and `M` above it.
    #[must_use]
    pub fn max_neighbors_for_layer(&self, layer: usize) -> usize {
        if layer == 0 {
            self.max_neighbors.saturating_mul(2)
        } else {
            self.max_neighbors
        }
    }

    /// Level normalization factor `mL = 1 / ln(M)`, which makes each layer hold about `1 / M`
    /// of the nodes of the layer below it.
    fn level_multiplier(&self) -> f64 {
        1.0 / (self.max_neighbors as f64).ln()
    }

    fn validate(&self) -> io::Result<()> {
        if self.max_neighbors < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "hnsw max_neighbors must be at least 2 but was {}",
                    self.max_neighbors
                ),
            ));
        }
        if self.ef_construction == 0 || self.ef_search == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "hnsw ef_construction and ef_search must be at least 1 but were {} and {}",
                    self.ef_construction, self.ef_search
                ),
            ));
        }
        Ok(())
    }
}

/// Builder input for one HNSW-visible immutable put entry.
#[derive(Clone, Debug, PartialEq)]
pub struct HnswIndexEntrySource {
    /// Stable offset index into the flat sidecar entry table.
    pub entry_offset_index: usize,
    /// External record identifier.
    pub record_id: RecordId,
    /// Sequence number represented by the immutable put.
    pub seq_no: SeqNo,
    /// Raw vector payload duplicated for ANN traversal.
    pub vector: Vec<f32>,
    /// Metadata payload used for filtered traversal hooks and rerank fetches.
    pub metadata: Value,
}

/// Persisted record payload for one HNSW node.
#[derive(Clone, Debug, PartialEq)]
pub struct HnswStoredRecord {
    /// Stable offset index into the flat sidecar entry table.
    pub entry_offset_index: usize,
    /// External record identifier.
    pub record_id: RecordId,
    /// Sequence number represented by the immutable put.
    pub seq_no: SeqNo,
    /// Raw vector payload duplicated for ANN traversal.
    pub vector: Vec<f32>,
    /// Metadata payload used for filtered traversal hooks and rerank fetches.
    pub metadata: Value,
}

/// One HNSW node persisted inside the sidecar.
#[derive(Clone, Debug, PartialEq)]
pub struct HnswNode {
    /// Stored record carried by this node.
    pub record: HnswStoredRecord,
    /// Highest layer reachable by the node.
    pub level: u8,
    /// Neighbor lists for every layer from 0..=level.
    pub neighbors_by_level: Vec<Vec<u32>>,
}

/// Binary HNSW sidecar persisted for immutable ANN search.
#[derive(Clone, Debug, PartialEq)]
pub struct HnswIndexSidecar {
    /// Sidecar version.
    pub version: u16,
    /// Segment identifier this sidecar belongs to.
    pub segment_id: String,
    /// Index family represented by the sidecar.
    pub index_kind: IndexKind,
    /// Distance metric this graph was built for.
    pub metric: DistanceMetric,
    /// Vector dimensionality for every node.
    pub dimensions: usize,
    /// Build parameters that produced the graph.
    pub params: HnswBuildParams,
    /// Current graph entry point, if any nodes exist.
    pub entry_point: Option<u32>,
    /// Highest level present in the graph.
    pub max_level: u8,
    /// Persisted nodes in insertion order.
    pub nodes: Vec<HnswNode>,
}

/// Final candidate returned by HNSW search.
#[derive(Clone, Debug, PartialEq)]
pub struct HnswSearchCandidate {
    /// Stable offset index into the flat sidecar entry table.
    pub entry_offset_index: usize,
    /// External record identifier.
    pub record_id: RecordId,
    /// Sequence number represented by the immutable put.
    pub seq_no: SeqNo,
    /// Raw vector payload duplicated for ANN traversal.
    pub vector: Vec<f32>,
    /// Metadata payload associated with the candidate.
    pub metadata: Value,
    /// Raw metric value for the candidate.
    pub value: f32,
}

/// Internal search accounting for ANN explain and verification surfaces.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HnswSearchStats {
    /// Distinct nodes visited while traversing the graph.
    pub visited_nodes: usize,
    /// Candidate count before any metadata filter is applied.
    pub candidate_count: usize,
    /// Candidate count rejected by the metadata filter hook.
    pub filtered_out_count: usize,
}

/// Search output returned from an HNSW sidecar query.
#[derive(Clone, Debug, PartialEq)]
pub struct HnswSearchResult {
    /// Final ranked candidates after optional filtering.
    pub candidates: Vec<HnswSearchCandidate>,
    /// Traversal and filter accounting.
    pub stats: HnswSearchStats,
}

/// A node scored against a query or base vector.
///
/// `Ord` ranks better nodes as greater: a higher similarity for cosine and dot, a smaller
/// distance for L2. Ties break toward the lower node index so builds and searches stay
/// deterministic.
#[derive(Clone, Copy, Debug)]
struct ScoredNode {
    index: usize,
    value: f32,
    goodness: f32,
}

impl ScoredNode {
    fn new(metric: DistanceMetric, index: usize, value: f32) -> Self {
        let goodness = match metric {
            DistanceMetric::Cosine | DistanceMetric::Dot => value,
            DistanceMetric::L2 => -value,
        };
        Self {
            index,
            value,
            goodness: if goodness.is_nan() {
                f32::NEG_INFINITY
            } else {
                goodness
            },
        }
    }
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ScoredNode {}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.goodness
            .total_cmp(&other.goodness)
            .then_with(|| other.index.cmp(&self.index))
    }
}

/// Build a deterministic HNSW sidecar from immutable visible put entries.
pub fn build_hnsw_index(
    segment_id: impl Into<String>,
    metric: DistanceMetric,
    params: HnswBuildParams,
    entries: &[HnswIndexEntrySource],
) -> io::Result<HnswIndexSidecar> {
    params.validate()?;
    let dimensions = entries
        .first()
        .map(|entry| entry.vector.len())
        .unwrap_or_default();
    for entry in entries {
        if entry.vector.len() != dimensions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "hnsw entry '{}' expected {} dimensions but found {}",
                    entry.record_id,
                    dimensions,
                    entry.vector.len()
                ),
            ));
        }
    }

    let mut index = HnswIndexSidecar {
        version: HNSW_VERSION,
        segment_id: segment_id.into(),
        index_kind: IndexKind::Hnsw,
        metric,
        dimensions,
        params,
        entry_point: None,
        max_level: 0,
        nodes: Vec::with_capacity(entries.len()),
    };

    let level_multiplier = index.params.level_multiplier();
    for entry in entries {
        insert_hnsw_entry(&mut index, entry, level_multiplier)?;
    }

    Ok(index)
}

/// Durably persist an HNSW sidecar to disk as a binary artifact.
///
/// The write is atomic: readers see either the previous file or the complete new one, and the
/// contents and directory entry are fsynced before this returns.
pub fn write_hnsw_index(path: &Path, sidecar: &HnswIndexSidecar) -> io::Result<()> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(HNSW_MAGIC);
    write_u16(&mut bytes, sidecar.version);
    write_string(&mut bytes, &sidecar.segment_id)?;
    bytes.push(match sidecar.index_kind {
        IndexKind::Hnsw => 1,
        IndexKind::IvfPq => 2,
        IndexKind::Flat => 3,
    });
    bytes.push(match sidecar.metric {
        DistanceMetric::Cosine => 1,
        DistanceMetric::Dot => 2,
        DistanceMetric::L2 => 3,
    });
    write_u32(&mut bytes, sidecar.dimensions)?;
    write_u32(&mut bytes, sidecar.params.max_neighbors)?;
    write_u32(&mut bytes, sidecar.params.ef_construction)?;
    write_u32(&mut bytes, sidecar.params.ef_search)?;
    bytes.push(sidecar.max_level);
    write_optional_u32(&mut bytes, sidecar.entry_point);
    write_u32(&mut bytes, sidecar.nodes.len())?;
    for node in &sidecar.nodes {
        bytes.push(node.level);
        write_u32(&mut bytes, node.record.entry_offset_index)?;
        write_u64(&mut bytes, node.record.seq_no);
        write_string(&mut bytes, node.record.record_id.as_str())?;
        write_f32_slice(&mut bytes, &node.record.vector)?;
        write_string(
            &mut bytes,
            &serde_json::to_string(&node.record.metadata)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?,
        )?;
        write_u32(&mut bytes, node.neighbors_by_level.len())?;
        for neighbors in &node.neighbors_by_level {
            write_u32(&mut bytes, neighbors.len())?;
            for neighbor in neighbors {
                write_u32(&mut bytes, *neighbor as usize)?;
            }
        }
    }
    durable::write_atomic(path, &bytes)
}

/// Load an HNSW sidecar from disk.
pub fn read_hnsw_index(path: &Path) -> io::Result<HnswIndexSidecar> {
    let bytes = fs::read(path)?;
    let mut cursor = 0usize;
    if read_bytes(&bytes, &mut cursor, 4)? != HNSW_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid hnsw magic in '{}'", path.display()),
        ));
    }

    let version = read_u16(&bytes, &mut cursor)?;
    if version != HNSW_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported hnsw version {version} in '{}' (expected {HNSW_VERSION})",
                path.display()
            ),
        ));
    }
    let segment_id = read_string(&bytes, &mut cursor)?;
    let index_kind = match read_u8(&bytes, &mut cursor)? {
        1 => IndexKind::Hnsw,
        2 => IndexKind::IvfPq,
        3 => IndexKind::Flat,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown index kind tag {other}"),
            ));
        }
    };
    let metric = match read_u8(&bytes, &mut cursor)? {
        1 => DistanceMetric::Cosine,
        2 => DistanceMetric::Dot,
        3 => DistanceMetric::L2,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown metric tag {other}"),
            ));
        }
    };
    let dimensions = read_u32(&bytes, &mut cursor)? as usize;
    let params = HnswBuildParams {
        max_neighbors: read_u32(&bytes, &mut cursor)? as usize,
        ef_construction: read_u32(&bytes, &mut cursor)? as usize,
        ef_search: read_u32(&bytes, &mut cursor)? as usize,
    };
    let max_level = read_u8(&bytes, &mut cursor)?;
    let entry_point = read_optional_u32(&bytes, &mut cursor)?;
    let node_count = read_u32(&bytes, &mut cursor)? as usize;
    let mut nodes = Vec::with_capacity(node_count.min(bytes.len()));
    for _ in 0..node_count {
        let level = read_u8(&bytes, &mut cursor)?;
        let entry_offset_index = read_u32(&bytes, &mut cursor)? as usize;
        let seq_no = read_u64(&bytes, &mut cursor)?;
        let record_id = RecordId::new(read_string(&bytes, &mut cursor)?);
        let vector = read_f32_slice(&bytes, &mut cursor)?;
        let metadata = serde_json::from_str::<Value>(&read_string(&bytes, &mut cursor)?)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        if vector.len() != dimensions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stored vector '{}' expected {} dimensions but found {}",
                    record_id,
                    dimensions,
                    vector.len()
                ),
            ));
        }
        let level_count = read_u32(&bytes, &mut cursor)? as usize;
        let mut neighbors_by_level = Vec::with_capacity(level_count.min(bytes.len()));
        for _ in 0..level_count {
            let neighbor_count = read_u32(&bytes, &mut cursor)? as usize;
            let mut neighbors = Vec::with_capacity(neighbor_count.min(bytes.len()));
            for _ in 0..neighbor_count {
                neighbors.push(read_u32(&bytes, &mut cursor)?);
            }
            neighbors_by_level.push(neighbors);
        }
        nodes.push(HnswNode {
            record: HnswStoredRecord {
                entry_offset_index,
                record_id,
                seq_no,
                vector,
                metadata,
            },
            level,
            neighbors_by_level,
        });
    }

    let sidecar = HnswIndexSidecar {
        version,
        segment_id,
        index_kind,
        metric,
        dimensions,
        params,
        entry_point,
        max_level,
        nodes,
    };
    validate_hnsw_index(&sidecar)?;
    if cursor != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unexpected trailing bytes in hnsw sidecar '{}' ({} unread)",
                path.display(),
                bytes.len() - cursor
            ),
        ));
    }
    Ok(sidecar)
}

/// Search the persisted HNSW graph with an optional metadata filter hook.
pub fn search_hnsw(
    sidecar: &HnswIndexSidecar,
    query: &[f32],
    top_k: usize,
    filter: Option<&(dyn for<'a> Fn(&'a Value) -> bool + Send + Sync)>,
) -> io::Result<HnswSearchResult> {
    if top_k == 0 || sidecar.nodes.is_empty() {
        return Ok(HnswSearchResult {
            candidates: Vec::new(),
            stats: HnswSearchStats::default(),
        });
    }
    if query.len() != sidecar.dimensions {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "query expected {} dimensions but found {}",
                sidecar.dimensions,
                query.len()
            ),
        ));
    }

    let mut entry = sidecar.entry_point.unwrap_or_default() as usize;
    for layer in (1..=sidecar.max_level).rev() {
        entry = greedy_descent(sidecar, query, layer, entry)?;
    }
    let mut ef = sidecar.params.ef_search.max(top_k);
    let mut stats;
    let mut candidates = Vec::with_capacity(top_k.min(sidecar.nodes.len()));
    loop {
        let (scored, visited_nodes) = search_layer(sidecar, query, 0, &[entry], ef, None)?;
        stats = HnswSearchStats {
            visited_nodes,
            candidate_count: scored.len(),
            filtered_out_count: 0,
        };
        candidates.clear();
        for scored_node in scored {
            let node = &sidecar.nodes[scored_node.index];
            if filter.is_some_and(|predicate| !predicate(&node.record.metadata)) {
                stats.filtered_out_count += 1;
                continue;
            }
            candidates.push(HnswSearchCandidate {
                entry_offset_index: node.record.entry_offset_index,
                record_id: node.record.record_id.clone(),
                seq_no: node.record.seq_no,
                vector: node.record.vector.clone(),
                metadata: node.record.metadata.clone(),
                value: scored_node.value,
            });
            if candidates.len() == top_k {
                break;
            }
        }
        if filter.is_none() || candidates.len() == top_k || ef >= sidecar.nodes.len() {
            break;
        }
        ef = ef.saturating_mul(2).min(sidecar.nodes.len());
    }

    Ok(HnswSearchResult { candidates, stats })
}

fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn validate_hnsw_index(sidecar: &HnswIndexSidecar) -> io::Result<()> {
    if sidecar.index_kind != IndexKind::Hnsw {
        return Err(invalid_data(format!(
            "hnsw sidecar has index kind '{}'",
            sidecar.index_kind.as_str()
        )));
    }
    sidecar
        .params
        .validate()
        .map_err(|error| invalid_data(error.to_string()))?;
    if sidecar.max_level > MAX_HNSW_LEVEL {
        return Err(invalid_data(format!(
            "max level {} exceeds the limit of {MAX_HNSW_LEVEL}",
            sidecar.max_level
        )));
    }

    match sidecar.entry_point {
        None if !sidecar.nodes.is_empty() => {
            return Err(invalid_data(
                "non-empty hnsw sidecar is missing an entry point".to_owned(),
            ));
        }
        None if sidecar.max_level != 0 => {
            return Err(invalid_data(format!(
                "empty hnsw sidecar has max level {}",
                sidecar.max_level
            )));
        }
        None => {}
        Some(entry_point) => {
            let Some(entry_node) = sidecar.nodes.get(entry_point as usize) else {
                return Err(invalid_data(format!(
                    "entry point {entry_point} is out of range for {} nodes",
                    sidecar.nodes.len()
                )));
            };
            if entry_node.level != sidecar.max_level {
                return Err(invalid_data(format!(
                    "entry point level {} does not match max level {}",
                    entry_node.level, sidecar.max_level
                )));
            }
        }
    }

    let mut seen = Vec::new();
    for (node_index, node) in sidecar.nodes.iter().enumerate() {
        if node.level > sidecar.max_level {
            return Err(invalid_data(format!(
                "node {node_index} level {} exceeds max level {}",
                node.level, sidecar.max_level
            )));
        }
        if node.neighbors_by_level.len() != usize::from(node.level) + 1 {
            return Err(invalid_data(format!(
                "node {node_index} level {} expected {} neighbor lists but found {}",
                node.level,
                usize::from(node.level) + 1,
                node.neighbors_by_level.len()
            )));
        }
        for (layer, neighbors) in node.neighbors_by_level.iter().enumerate() {
            let max_degree = sidecar.params.max_neighbors_for_layer(layer);
            if neighbors.len() > max_degree {
                return Err(invalid_data(format!(
                    "node {node_index} layer {layer} has {} neighbors but at most {max_degree} are allowed",
                    neighbors.len()
                )));
            }
            for neighbor in neighbors {
                let neighbor_index = *neighbor as usize;
                let Some(neighbor_node) = sidecar.nodes.get(neighbor_index) else {
                    return Err(invalid_data(format!(
                        "node {node_index} layer {layer} references out-of-range neighbor {neighbor_index}"
                    )));
                };
                if neighbor_index == node_index {
                    return Err(invalid_data(format!(
                        "node {node_index} layer {layer} references itself"
                    )));
                }
                if usize::from(neighbor_node.level) < layer {
                    return Err(invalid_data(format!(
                        "node {node_index} layer {layer} references neighbor {neighbor_index} below that layer"
                    )));
                }
            }
            seen.clear();
            seen.extend_from_slice(neighbors);
            seen.sort_unstable();
            if seen.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(invalid_data(format!(
                    "node {node_index} layer {layer} lists a neighbor more than once"
                )));
            }
        }
    }

    Ok(())
}

/// Insert one entry following Malkov & Yashunin, Algorithm 1.
fn insert_hnsw_entry(
    sidecar: &mut HnswIndexSidecar,
    entry: &HnswIndexEntrySource,
    level_multiplier: f64,
) -> io::Result<()> {
    let level = deterministic_level(&entry.record_id, entry.seq_no, level_multiplier);
    let new_index = sidecar.nodes.len();
    sidecar.nodes.push(HnswNode {
        record: HnswStoredRecord {
            entry_offset_index: entry.entry_offset_index,
            record_id: entry.record_id.clone(),
            seq_no: entry.seq_no,
            vector: entry.vector.clone(),
            metadata: entry.metadata.clone(),
        },
        level,
        neighbors_by_level: vec![Vec::new(); usize::from(level) + 1],
    });

    let Some(entry_point) = sidecar.entry_point else {
        sidecar.entry_point = Some(new_index as u32);
        sidecar.max_level = level;
        return Ok(());
    };

    let mut current_entry = entry_point as usize;
    for layer in ((level.saturating_add(1))..=sidecar.max_level).rev() {
        current_entry = greedy_descent(
            sidecar,
            &sidecar.nodes[new_index].record.vector,
            layer,
            current_entry,
        )?;
    }

    let mut entry_points = vec![current_entry];
    let ef_construction = sidecar
        .params
        .ef_construction
        .max(sidecar.params.max_neighbors);
    for layer in (0..=sidecar.max_level.min(level)).rev() {
        let (candidates, _) = search_layer(
            sidecar,
            &sidecar.nodes[new_index].record.vector,
            layer,
            &entry_points,
            ef_construction,
            Some(new_index),
        )?;
        let selected = select_neighbors_heuristic(
            sidecar.metric,
            &sidecar.nodes,
            &candidates,
            sidecar.params.max_neighbors,
        )?;
        connect_node(sidecar, new_index, usize::from(layer), &selected)?;
        entry_points = candidates.iter().map(|candidate| candidate.index).collect();
    }

    if level > sidecar.max_level {
        sidecar.max_level = level;
        sidecar.entry_point = Some(new_index as u32);
    }
    Ok(())
}

/// Draw a node level from the geometric distribution `floor(-ln(U) * mL)`.
///
/// `U` comes from a hash of the record id and sequence number rather than a random generator,
/// so rebuilding a segment from the same entries yields the same graph.
fn deterministic_level(record_id: &RecordId, seq_no: SeqNo, level_multiplier: f64) -> u8 {
    let mut hasher = DefaultHasher::new();
    record_id.hash(&mut hasher);
    seq_no.hash(&mut hasher);
    // The top 53 bits give a uniform value in (0, 1] that an f64 represents exactly.
    let uniform = ((hasher.finish() >> 11) + 1) as f64 / (1u64 << 53) as f64;
    let level = (-uniform.ln() * level_multiplier).floor();
    if level >= f64::from(MAX_HNSW_LEVEL) {
        MAX_HNSW_LEVEL
    } else {
        level as u8
    }
}

fn greedy_descent(
    sidecar: &HnswIndexSidecar,
    query: &[f32],
    layer: u8,
    mut current: usize,
) -> io::Result<usize> {
    let metric = sidecar.metric;
    let mut best = ScoredNode::new(
        metric,
        current,
        metric_value(metric, query, &sidecar.nodes[current].record.vector)?,
    );
    loop {
        for &neighbor in neighbor_slice(sidecar, current, layer) {
            let neighbor = neighbor as usize;
            let scored = ScoredNode::new(
                metric,
                neighbor,
                metric_value(metric, query, &sidecar.nodes[neighbor].record.vector)?,
            );
            if scored.goodness > best.goodness {
                best = scored;
            }
        }
        if best.index == current {
            return Ok(current);
        }
        current = best.index;
    }
}

/// Beam search over one layer (Malkov & Yashunin, Algorithm 2).
///
/// Returns up to `ef` nodes ordered best first, plus the number of distinct nodes visited.
fn search_layer(
    sidecar: &HnswIndexSidecar,
    query: &[f32],
    layer: u8,
    entry_points: &[usize],
    ef: usize,
    exclude_index: Option<usize>,
) -> io::Result<(Vec<ScoredNode>, usize)> {
    let metric = sidecar.metric;
    // A layer never yields more than every node, and the clamp keeps `ef + 1` from overflowing.
    let ef = ef.clamp(1, sidecar.nodes.len().max(1));
    let mut visited = HashSet::new();
    // Max-heap: the best unexpanded candidate is on top.
    let mut candidates = BinaryHeap::<ScoredNode>::new();
    // Min-heap: the worst kept result is on top.
    let mut results = BinaryHeap::<Reverse<ScoredNode>>::with_capacity(ef + 1);

    for &entry_point in entry_points {
        if Some(entry_point) == exclude_index
            || entry_point >= sidecar.nodes.len()
            || !visited.insert(entry_point)
        {
            continue;
        }
        let scored = ScoredNode::new(
            metric,
            entry_point,
            metric_value(metric, query, &sidecar.nodes[entry_point].record.vector)?,
        );
        candidates.push(scored);
        results.push(Reverse(scored));
        if results.len() > ef {
            results.pop();
        }
    }

    while let Some(candidate) = candidates.pop() {
        if results.len() >= ef
            && results
                .peek()
                .is_some_and(|Reverse(worst)| candidate.goodness < worst.goodness)
        {
            break;
        }

        for &neighbor in neighbor_slice(sidecar, candidate.index, layer) {
            let neighbor = neighbor as usize;
            if Some(neighbor) == exclude_index || !visited.insert(neighbor) {
                continue;
            }
            let scored = ScoredNode::new(
                metric,
                neighbor,
                metric_value(metric, query, &sidecar.nodes[neighbor].record.vector)?,
            );
            if results.len() < ef || results.peek().is_some_and(|Reverse(worst)| scored > *worst) {
                candidates.push(scored);
                results.push(Reverse(scored));
                if results.len() > ef {
                    results.pop();
                }
            }
        }
    }

    let mut ranked = results
        .into_iter()
        .map(|Reverse(scored)| scored)
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|left, right| right.cmp(left));
    Ok((ranked, visited.len()))
}

/// Pick up to `limit` neighbors from `candidates` with the diversity heuristic
/// (Malkov & Yashunin, Algorithm 4, with `keepPrunedConnections`).
///
/// `candidates` must be ordered best first and scored against the node being linked. A
/// candidate is kept only if it is closer to that node than to every neighbor already kept,
/// which preserves the long edges that bridge clusters. Remaining slots are then filled with
/// the closest pruned candidates.
fn select_neighbors_heuristic(
    metric: DistanceMetric,
    nodes: &[HnswNode],
    candidates: &[ScoredNode],
    limit: usize,
) -> io::Result<Vec<usize>> {
    let mut selected = Vec::<usize>::with_capacity(limit);
    let mut pruned = Vec::<usize>::new();
    for candidate in candidates {
        if selected.len() >= limit {
            break;
        }
        let candidate_vector = &nodes[candidate.index].record.vector;
        let mut diverse = true;
        for &kept in &selected {
            let between = ScoredNode::new(
                metric,
                kept,
                metric_value(metric, candidate_vector, &nodes[kept].record.vector)?,
            );
            if between.goodness > candidate.goodness {
                diverse = false;
                break;
            }
        }
        if diverse {
            selected.push(candidate.index);
        } else {
            pruned.push(candidate.index);
        }
    }
    let open_slots = limit.saturating_sub(selected.len());
    selected.extend(pruned.into_iter().take(open_slots));
    Ok(selected)
}

/// Link `node_index` to `neighbors` on `layer` and add the reverse edges, shrinking any
/// neighbor list that grows past the layer's degree bound.
fn connect_node(
    sidecar: &mut HnswIndexSidecar,
    node_index: usize,
    layer: usize,
    neighbors: &[usize],
) -> io::Result<()> {
    let metric = sidecar.metric;
    let max_degree = sidecar.params.max_neighbors_for_layer(layer);
    let node_id = node_index as u32;
    sidecar.nodes[node_index].neighbors_by_level[layer] =
        neighbors.iter().map(|&neighbor| neighbor as u32).collect();

    for &neighbor in neighbors {
        let Some(list) = sidecar.nodes[neighbor].neighbors_by_level.get_mut(layer) else {
            continue;
        };
        if list.contains(&node_id) {
            continue;
        }
        list.push(node_id);
        if list.len() > max_degree {
            let shrunk = shrink_neighbors(metric, &sidecar.nodes, neighbor, layer, max_degree)?;
            sidecar.nodes[neighbor].neighbors_by_level[layer] = shrunk;
        }
    }
    Ok(())
}

/// Re-select an overfull neighbor list with the same heuristic used for new nodes.
fn shrink_neighbors(
    metric: DistanceMetric,
    nodes: &[HnswNode],
    node_index: usize,
    layer: usize,
    limit: usize,
) -> io::Result<Vec<u32>> {
    let base = &nodes[node_index].record.vector;
    let mut scored = nodes[node_index].neighbors_by_level[layer]
        .iter()
        .map(|&neighbor| {
            let neighbor = neighbor as usize;
            metric_value(metric, base, &nodes[neighbor].record.vector)
                .map(|value| ScoredNode::new(metric, neighbor, value))
        })
        .collect::<io::Result<Vec<_>>>()?;
    scored.sort_unstable_by(|left, right| right.cmp(left));
    Ok(select_neighbors_heuristic(metric, nodes, &scored, limit)?
        .into_iter()
        .map(|neighbor| neighbor as u32)
        .collect())
}

fn neighbor_slice(sidecar: &HnswIndexSidecar, node_index: usize, layer: u8) -> &[u32] {
    sidecar.nodes[node_index]
        .neighbors_by_level
        .get(usize::from(layer))
        .map_or(&[], Vec::as_slice)
}

fn metric_value(metric: DistanceMetric, query: &[f32], candidate: &[f32]) -> io::Result<f32> {
    if query.len() != candidate.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "vector expected {} dimensions but found {}",
                query.len(),
                candidate.len()
            ),
        ));
    }

    Ok(match metric {
        DistanceMetric::Dot => kernels::dot(query, candidate),
        DistanceMetric::Cosine => {
            let query_norm = kernels::norm(query);
            let candidate_norm = kernels::norm(candidate);
            if query_norm == 0.0 || candidate_norm == 0.0 {
                0.0
            } else {
                kernels::dot(query, candidate) / (query_norm * candidate_norm)
            }
        }
        DistanceMetric::L2 => kernels::l2_squared(query, candidate).sqrt(),
    })
}

fn write_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut Vec<u8>, value: usize) -> io::Result<()> {
    let value = u32::try_from(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "value does not fit in u32"))?;
    bytes.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_optional_u32(bytes: &mut Vec<u8>, value: Option<u32>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
}

fn write_string(bytes: &mut Vec<u8>, value: &str) -> io::Result<()> {
    write_u32(bytes, value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_f32_slice(bytes: &mut Vec<u8>, values: &[f32]) -> io::Result<()> {
    write_u32(bytes, values.len())?;
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> io::Result<u8> {
    Ok(*read_bytes(bytes, cursor, 1)?
        .first()
        .expect("one byte slice should not be empty"))
}

fn read_u16(bytes: &[u8], cursor: &mut usize) -> io::Result<u16> {
    Ok(u16::from_le_bytes(
        read_bytes(bytes, cursor, 2)?
            .try_into()
            .expect("u16 slice should be exact"),
    ))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    Ok(u32::from_le_bytes(
        read_bytes(bytes, cursor, 4)?
            .try_into()
            .expect("u32 slice should be exact"),
    ))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    Ok(u64::from_le_bytes(
        read_bytes(bytes, cursor, 8)?
            .try_into()
            .expect("u64 slice should be exact"),
    ))
}

fn read_optional_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<Option<u32>> {
    match read_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(read_u32(bytes, cursor)?)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid optional u32 tag {other}"),
        )),
    }
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> io::Result<String> {
    let len = read_u32(bytes, cursor)? as usize;
    let slice = read_bytes(bytes, cursor, len)?;
    String::from_utf8(slice.to_vec())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn read_f32_slice(bytes: &[u8], cursor: &mut usize) -> io::Result<Vec<f32>> {
    let len = read_u32(bytes, cursor)? as usize;
    let mut values = Vec::with_capacity(len.min(bytes.len()));
    for _ in 0..len {
        values.push(f32::from_le_bytes(
            read_bytes(bytes, cursor, 4)?
                .try_into()
                .expect("f32 slice should be exact"),
        ));
    }
    Ok(values)
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated hnsw payload"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated hnsw payload"))?;
    *cursor = end;
    Ok(slice)
}

fn update_scalar_field_stats(
    scalar_fields: &mut BTreeMap<String, ScalarFieldStats>,
    metadata: &Value,
) {
    let Value::Object(fields) = metadata else {
        return;
    };

    for (field, value) in fields {
        let stats = scalar_fields
            .entry(field.clone())
            .or_insert_with(|| ScalarFieldStats {
                present_count: 0,
                null_count: 0,
                distinct_count: 0,
                min: None,
                max: None,
                value_counts: BTreeMap::new(),
            });

        stats.present_count += 1;
        let Some(scalar) = ScalarMetadataValue::from_json(value) else {
            continue;
        };
        if scalar == ScalarMetadataValue::Null {
            stats.null_count += 1;
        }

        let summary_key = scalar.summary_key();
        let next_count = stats.value_counts.entry(summary_key).or_insert(0);
        *next_count += 1;
        stats.distinct_count = stats.value_counts.len();

        if scalar != ScalarMetadataValue::Null {
            if stats
                .min
                .as_ref()
                .is_none_or(|current| compare_scalars(&scalar, current) == Ordering::Less)
            {
                stats.min = Some(scalar.clone());
            }
            if stats
                .max
                .as_ref()
                .is_none_or(|current| compare_scalars(&scalar, current) == Ordering::Greater)
            {
                stats.max = Some(scalar);
            }
        }
    }
}

fn compare_scalars(left: &ScalarMetadataValue, right: &ScalarMetadataValue) -> Ordering {
    match (left, right) {
        (ScalarMetadataValue::String(left), ScalarMetadataValue::String(right)) => left.cmp(right),
        (ScalarMetadataValue::Bool(left), ScalarMetadataValue::Bool(right)) => left.cmp(right),
        (ScalarMetadataValue::Number(left), ScalarMetadataValue::Number(right)) => {
            compare_numbers(left, right)
        }
        (ScalarMetadataValue::Null, ScalarMetadataValue::Null) => Ordering::Equal,
        (ScalarMetadataValue::Null, _) => Ordering::Less,
        (_, ScalarMetadataValue::Null) => Ordering::Greater,
        (
            ScalarMetadataValue::Bool(_),
            ScalarMetadataValue::Number(_) | ScalarMetadataValue::String(_),
        ) => Ordering::Less,
        (ScalarMetadataValue::Number(_), ScalarMetadataValue::String(_)) => Ordering::Less,
        (
            ScalarMetadataValue::Number(_) | ScalarMetadataValue::String(_),
            ScalarMetadataValue::Bool(_),
        ) => Ordering::Greater,
        (ScalarMetadataValue::String(_), ScalarMetadataValue::Number(_)) => Ordering::Greater,
    }
}

fn compare_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Ordering {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return left.cmp(&right);
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return left.cmp(&right);
    }
    let left = left.as_f64().unwrap_or_default();
    let right = right.as_f64().unwrap_or_default();
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logpose_types::{DistanceMetric, RecordId};
    use serde_json::json;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn build_flat_index_tracks_norms_offsets_and_scalar_stats() {
        let sidecar = build_flat_index(
            "segment-1",
            &[
                FlatIndexEntrySource {
                    is_put: true,
                    record_id_offset: 0,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: Some(vec![3.0, 4.0]),
                    metadata: Some(json!({
                        "topic":"intro",
                        "rank":2,
                        "active":true,
                        "details":{"chapter":1}
                    })),
                },
                FlatIndexEntrySource {
                    is_put: false,
                    record_id_offset: 4,
                    vector_offset: 0,
                    metadata_offset: 0,
                    vector: None,
                    metadata: None,
                },
            ],
        );

        assert_eq!(sidecar.index_kind, IndexKind::Flat);
        assert_eq!(sidecar.put_count, 1);
        assert_eq!(sidecar.delete_count, 1);
        assert_eq!(sidecar.vector_norms, vec![Some(5.0), None]);
        assert_eq!(sidecar.entry_offsets[0].record_id_offset, 0);
        assert_eq!(sidecar.scalar_fields["topic"].present_count, 1);
        assert_eq!(sidecar.scalar_fields["rank"].distinct_count, 1);
        assert_eq!(sidecar.scalar_fields["details"].present_count, 1);
        assert!(sidecar.scalar_fields["details"].value_counts.is_empty());
    }

    #[test]
    fn hnsw_round_trip_preserves_top_candidates() {
        let path = temp_file_path("hnsw-round-trip.bin");
        let index = build_hnsw_index(
            "segment-2",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[
                HnswIndexEntrySource {
                    entry_offset_index: 0,
                    record_id: RecordId::new("alpha"),
                    seq_no: 1,
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 1,
                    record_id: RecordId::new("beta"),
                    seq_no: 2,
                    vector: vec![0.1, 1.0],
                    metadata: json!({"kind":"drop"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 2,
                    record_id: RecordId::new("gamma"),
                    seq_no: 3,
                    vector: vec![0.9, 0.1],
                    metadata: json!({"kind":"keep"}),
                },
            ],
        )
        .expect("index should build");

        write_hnsw_index(&path, &index).expect("index should write");
        let restored = read_hnsw_index(&path).expect("index should read");

        let original = search_hnsw(&index, &[1.0, 0.0], 2, None).expect("search should succeed");
        let round_trip =
            search_hnsw(&restored, &[1.0, 0.0], 2, None).expect("search should succeed");

        assert_eq!(
            original
                .candidates
                .iter()
                .map(|candidate| candidate.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "gamma"]
        );
        assert_eq!(
            original
                .candidates
                .iter()
                .map(|candidate| candidate.record_id.as_str())
                .collect::<Vec<_>>(),
            round_trip
                .candidates
                .iter()
                .map(|candidate| candidate.record_id.as_str())
                .collect::<Vec<_>>()
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn hnsw_filter_hook_excludes_non_matching_candidates() {
        let index = build_hnsw_index(
            "segment-3",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[
                HnswIndexEntrySource {
                    entry_offset_index: 0,
                    record_id: RecordId::new("alpha"),
                    seq_no: 1,
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 1,
                    record_id: RecordId::new("beta"),
                    seq_no: 2,
                    vector: vec![0.95, 0.0],
                    metadata: json!({"kind":"drop"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 2,
                    record_id: RecordId::new("gamma"),
                    seq_no: 3,
                    vector: vec![0.75, 0.0],
                    metadata: json!({"kind":"keep"}),
                },
            ],
        )
        .expect("index should build");

        let result = search_hnsw(
            &index,
            &[1.0, 0.0],
            2,
            Some(&|metadata| metadata["kind"] == "keep"),
        )
        .expect("search should succeed");

        assert_eq!(
            result
                .candidates
                .iter()
                .map(|candidate| candidate.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "gamma"]
        );
        assert!(result.stats.filtered_out_count >= 1);
    }

    #[test]
    fn hnsw_filter_hook_expands_search_until_top_k_is_satisfied() {
        let index = build_hnsw_index(
            "segment-4",
            DistanceMetric::Dot,
            HnswBuildParams {
                ef_search: 2,
                ..HnswBuildParams::default()
            },
            &[
                HnswIndexEntrySource {
                    entry_offset_index: 0,
                    record_id: RecordId::new("drop-a"),
                    seq_no: 1,
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"drop"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 1,
                    record_id: RecordId::new("drop-b"),
                    seq_no: 2,
                    vector: vec![0.99, 0.0],
                    metadata: json!({"kind":"drop"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 2,
                    record_id: RecordId::new("keep-a"),
                    seq_no: 3,
                    vector: vec![0.98, 0.0],
                    metadata: json!({"kind":"keep"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 3,
                    record_id: RecordId::new("keep-b"),
                    seq_no: 4,
                    vector: vec![0.97, 0.0],
                    metadata: json!({"kind":"keep"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 4,
                    record_id: RecordId::new("drop-c"),
                    seq_no: 5,
                    vector: vec![0.96, 0.0],
                    metadata: json!({"kind":"drop"}),
                },
            ],
        )
        .expect("index should build");

        let result = search_hnsw(
            &index,
            &[1.0, 0.0],
            2,
            Some(&|metadata| metadata["kind"] == "keep"),
        )
        .expect("search should succeed");

        assert_eq!(
            result
                .candidates
                .iter()
                .map(|candidate| candidate.record_id.as_str())
                .collect::<Vec<_>>(),
            vec!["keep-a", "keep-b"]
        );
        assert!(result.stats.filtered_out_count >= 2);
    }

    #[test]
    fn read_hnsw_index_rejects_truncated_payload() {
        let path = temp_file_path("hnsw-truncated.bin");
        fs::write(&path, b"LPH1").expect("truncated payload should write");

        let error = read_hnsw_index(&path).expect_err("truncated payload should fail");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_hnsw_index_rejects_out_of_range_entry_points() {
        let path = temp_file_path("hnsw-invalid-entry-point.bin");
        let mut index = build_hnsw_index(
            "segment-invalid-entry",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            }],
        )
        .expect("index should build");
        index.entry_point = Some(9);

        write_hnsw_index(&path, &index).expect("index should write");
        let error = read_hnsw_index(&path).expect_err("invalid entry point should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_hnsw_index_rejects_unsupported_version() {
        let path = temp_file_path("hnsw-unsupported-version.bin");
        let mut index = build_hnsw_index(
            "segment-unsupported-version",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            }],
        )
        .expect("index should build");
        index.version = HNSW_VERSION + 1;

        write_hnsw_index(&path, &index).expect("index should write");
        let error = read_hnsw_index(&path).expect_err("unsupported version should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("unsupported hnsw version"),
            "unexpected error: {error}"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_hnsw_index_rejects_missing_entry_point_for_non_empty_graph() {
        let path = temp_file_path("hnsw-missing-entry-point.bin");
        let mut index = build_hnsw_index(
            "segment-missing-entry",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            }],
        )
        .expect("index should build");
        index.entry_point = None;

        write_hnsw_index(&path, &index).expect("index should write");
        let error = read_hnsw_index(&path).expect_err("missing entry point should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_hnsw_index_rejects_trailing_bytes() {
        let path = temp_file_path("hnsw-trailing-bytes.bin");
        let index = build_hnsw_index(
            "segment-trailing-bytes",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[HnswIndexEntrySource {
                entry_offset_index: 0,
                record_id: RecordId::new("alpha"),
                seq_no: 1,
                vector: vec![1.0, 0.0],
                metadata: json!({"kind":"keep"}),
            }],
        )
        .expect("index should build");

        write_hnsw_index(&path, &index).expect("index should write");
        let mut bytes = fs::read(&path).expect("serialized sidecar should read");
        bytes.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        fs::write(&path, bytes).expect("trailing bytes should write");

        let error = read_hnsw_index(&path).expect_err("trailing bytes should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("unexpected trailing bytes"),
            "unexpected error: {error}"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_hnsw_index_rejects_out_of_range_neighbor_references() {
        let path = temp_file_path("hnsw-invalid-neighbor.bin");
        let mut index = build_hnsw_index(
            "segment-invalid-neighbor",
            DistanceMetric::Dot,
            HnswBuildParams::default(),
            &[
                HnswIndexEntrySource {
                    entry_offset_index: 0,
                    record_id: RecordId::new("alpha"),
                    seq_no: 1,
                    vector: vec![1.0, 0.0],
                    metadata: json!({"kind":"keep"}),
                },
                HnswIndexEntrySource {
                    entry_offset_index: 1,
                    record_id: RecordId::new("beta"),
                    seq_no: 2,
                    vector: vec![0.9, 0.0],
                    metadata: json!({"kind":"keep"}),
                },
            ],
        )
        .expect("index should build");
        index.nodes[0].neighbors_by_level[0].push(99);

        write_hnsw_index(&path, &index).expect("index should write");
        let error = read_hnsw_index(&path).expect_err("invalid neighbor should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_hnsw_index_rejects_overfull_neighbor_lists() {
        let path = temp_file_path("hnsw-overfull-neighbors.bin");
        let entries = clustered_entries(&mut TestRng::new(3), 40, 4, 2, 5.0);
        let mut index = build_hnsw_index(
            "segment-overfull",
            DistanceMetric::L2,
            HnswBuildParams {
                max_neighbors: 2,
                ..HnswBuildParams::default()
            },
            &entries,
        )
        .expect("index should build");
        index.nodes[0].neighbors_by_level[0] = (1..=5).collect();

        write_hnsw_index(&path, &index).expect("index should write");
        let error = read_hnsw_index(&path).expect_err("overfull neighbor list should fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("at most 4 are allowed"),
            "unexpected error: {error}"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn search_hnsw_saturating_top_k_returns_every_node() {
        let entries = clustered_entries(&mut TestRng::new(5), 30, 3, 4, 5.0);
        let index = build_hnsw_index(
            "segment-huge-top-k",
            DistanceMetric::L2,
            HnswBuildParams::default(),
            &entries,
        )
        .expect("index should build");

        let result =
            search_hnsw(&index, &entries[0].vector, usize::MAX, None).expect("search should work");
        assert_eq!(result.candidates.len(), entries.len());
        assert_eq!(result.candidates[0].entry_offset_index, 0);
    }

    #[test]
    fn build_hnsw_index_rejects_degenerate_params() {
        for params in [
            HnswBuildParams {
                max_neighbors: 1,
                ..HnswBuildParams::default()
            },
            HnswBuildParams {
                ef_construction: 0,
                ..HnswBuildParams::default()
            },
            HnswBuildParams {
                ef_search: 0,
                ..HnswBuildParams::default()
            },
        ] {
            let error = build_hnsw_index("segment-bad-params", DistanceMetric::L2, params, &[])
                .expect_err("degenerate params should fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn deterministic_levels_thin_out_by_a_factor_of_m() {
        let params = HnswBuildParams::default();
        let multiplier = params.level_multiplier();
        let total = 20_000u64;
        let mut at_least = [0usize; 3];
        for seq_no in 0..total {
            let level =
                deterministic_level(&RecordId::new(format!("row-{seq_no}")), seq_no, multiplier);
            for (threshold, count) in at_least.iter_mut().enumerate() {
                if usize::from(level) > threshold {
                    *count += 1;
                }
            }
        }
        // P(level >= 1) = 1/16 and P(level >= 2) = 1/256 for M = 16.
        let above_zero = at_least[0] as f64 / total as f64;
        let above_one = at_least[1] as f64 / total as f64;
        assert!(
            (0.05..0.075).contains(&above_zero),
            "level >= 1 fraction {above_zero}"
        );
        assert!(
            (0.002..0.006).contains(&above_one),
            "level >= 2 fraction {above_one}"
        );
        assert_eq!(
            deterministic_level(&RecordId::new("row-7"), 7, multiplier),
            deterministic_level(&RecordId::new("row-7"), 7, multiplier),
            "levels must be reproducible"
        );
    }

    #[test]
    fn hnsw_recall_on_clustered_data_meets_target_at_default_ef() {
        let mut rng = TestRng::new(42);
        let dimensions = 32;
        let entries = clustered_entries(&mut rng, 5_000, 16, dimensions, 5.0);
        let index = build_hnsw_index(
            "segment-recall",
            DistanceMetric::L2,
            HnswBuildParams::default(),
            &entries,
        )
        .expect("index should build");
        validate_hnsw_index(&index).expect("built graph should satisfy sidecar invariants");
        assert_eq!(
            layer0_reachable_from_entry_point(&index),
            entries.len(),
            "every node should be reachable on layer 0"
        );

        let queries = clustered_entries(&mut rng, 100, 16, dimensions, 5.0);
        let recall = mean_recall_at_10(&index, &entries, &queries);
        assert!(recall >= 0.95, "recall@10 was {recall}");
    }

    #[test]
    fn hnsw_recall_on_clustered_cosine_data_meets_target_at_default_ef() {
        let mut rng = TestRng::new(7);
        let entries = clustered_entries(&mut rng, 2_000, 8, 16, 3.0);
        let index = build_hnsw_index(
            "segment-recall-cosine",
            DistanceMetric::Cosine,
            HnswBuildParams::default(),
            &entries,
        )
        .expect("index should build");
        validate_hnsw_index(&index).expect("built graph should satisfy sidecar invariants");

        let queries = clustered_entries(&mut rng, 50, 8, 16, 3.0);
        let recall = mean_recall_at_10(&index, &entries, &queries);
        assert!(recall >= 0.95, "recall@10 was {recall}");
    }

    /// Deterministic SplitMix64 generator so the recall tests need no extra dependency.
    struct TestRng(u64);

    impl TestRng {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut value = self.0;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^ (value >> 31)
        }

        /// Uniform value in (0, 1].
        fn next_unit(&mut self) -> f64 {
            ((self.next_u64() >> 11) + 1) as f64 / (1u64 << 53) as f64
        }

        fn next_gaussian(&mut self) -> f32 {
            let radius = (-2.0 * self.next_unit().ln()).sqrt();
            let angle = std::f64::consts::TAU * self.next_unit();
            (radius * angle.cos()) as f32
        }
    }

    /// Gaussian blobs with unit spread around centers drawn uniformly from
    /// `[-center_range, center_range]` per dimension. The same seed yields the same centers.
    fn clustered_entries(
        rng: &mut TestRng,
        count: usize,
        clusters: usize,
        dimensions: usize,
        center_range: f32,
    ) -> Vec<HnswIndexEntrySource> {
        let mut center_rng = TestRng::new(0x5eed);
        let centers = (0..clusters)
            .map(|_| {
                (0..dimensions)
                    .map(|_| (center_rng.next_unit() as f32 * 2.0 - 1.0) * center_range)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let id_base = rng.next_u64();
        (0..count)
            .map(|index| {
                let center = &centers[(rng.next_u64() % clusters as u64) as usize];
                HnswIndexEntrySource {
                    entry_offset_index: index,
                    record_id: RecordId::new(format!("row-{id_base:x}-{index}")),
                    seq_no: index as u64 + 1,
                    vector: center
                        .iter()
                        .map(|value| value + rng.next_gaussian())
                        .collect(),
                    metadata: json!({}),
                }
            })
            .collect()
    }

    fn mean_recall_at_10(
        index: &HnswIndexSidecar,
        entries: &[HnswIndexEntrySource],
        queries: &[HnswIndexEntrySource],
    ) -> f64 {
        let k = 10;
        let mut total = 0.0;
        for query in queries {
            let mut exact = entries
                .iter()
                .map(|entry| {
                    let value = metric_value(index.metric, &query.vector, &entry.vector)
                        .expect("dimensions should match");
                    (value, entry.entry_offset_index)
                })
                .collect::<Vec<_>>();
            exact.sort_unstable_by(|left, right| {
                let by_value = match index.metric {
                    DistanceMetric::L2 => left.0.total_cmp(&right.0),
                    DistanceMetric::Cosine | DistanceMetric::Dot => right.0.total_cmp(&left.0),
                };
                by_value.then(left.1.cmp(&right.1))
            });
            let truth = exact
                .iter()
                .take(k)
                .map(|(_, index)| *index)
                .collect::<HashSet<_>>();
            let found = search_hnsw(index, &query.vector, k, None)
                .expect("search should succeed")
                .candidates
                .iter()
                .filter(|candidate| truth.contains(&candidate.entry_offset_index))
                .count();
            total += found as f64 / k as f64;
        }
        total / queries.len() as f64
    }

    fn layer0_reachable_from_entry_point(index: &HnswIndexSidecar) -> usize {
        let Some(entry_point) = index.entry_point else {
            return 0;
        };
        let mut seen = HashSet::from([entry_point as usize]);
        let mut stack = vec![entry_point as usize];
        while let Some(node) = stack.pop() {
            for &neighbor in &index.nodes[node].neighbors_by_level[0] {
                if seen.insert(neighbor as usize) {
                    stack.push(neighbor as usize);
                }
            }
        }
        seen.len()
    }

    #[test]
    fn metric_values_keep_cosine_dot_and_l2_semantics() -> io::Result<()> {
        let query = [3.0, 4.0, 0.0];
        let candidate = [4.0, 0.0, 3.0];
        assert_eq!(metric_value(DistanceMetric::Dot, &query, &candidate)?, 12.0);
        assert_eq!(
            metric_value(DistanceMetric::Cosine, &query, &candidate)?,
            12.0 / 25.0
        );
        // L2 reports the Euclidean distance, not its square.
        assert_eq!(
            metric_value(DistanceMetric::L2, &query, &candidate)?,
            26.0_f32.sqrt()
        );
        assert_eq!(
            metric_value(DistanceMetric::Cosine, &query, &[0.0; 3])?,
            0.0
        );
        assert!(metric_value(DistanceMetric::Dot, &query, &[1.0]).is_err());
        Ok(())
    }

    fn temp_file_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        std::env::temp_dir().join(format!("logpose-{unique}-{name}"))
    }
}
