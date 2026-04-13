//! Exact and ANN index sidecars for immutable units.

use logpose_types::{DistanceMetric, RecordId, ScalarFieldStats, ScalarMetadataValue, SeqNo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    io,
    path::Path,
};

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
            vector_norms.push(entry.vector.as_ref().map(|vector| vector_norm(vector)));
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

/// Persist a flat exact sidecar to disk.
pub fn write_flat_index(path: &Path, sidecar: &FlatIndexSidecar) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(sidecar)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?,
    )
}

/// Load a flat exact sidecar from disk.
pub fn read_flat_index(path: &Path) -> io::Result<FlatIndexSidecar> {
    serde_json::from_slice(&fs::read(path)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

const HNSW_MAGIC: &[u8; 4] = b"LPH1";
const HNSW_VERSION: u16 = 1;
const MAX_HNSW_LEVEL: u8 = 4;

/// Deterministic build parameters for the persisted HNSW sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HnswBuildParams {
    /// Maximum bidirectional neighbors kept per layer.
    pub max_neighbors: usize,
    /// Search breadth used while linking new nodes.
    pub ef_construction: usize,
    /// Default search breadth used at query time.
    pub ef_search: usize,
}

impl Default for HnswBuildParams {
    fn default() -> Self {
        Self {
            max_neighbors: 8,
            ef_construction: 32,
            ef_search: 32,
        }
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

#[derive(Clone, Copy, Debug)]
struct ScoredNode {
    index: usize,
    value: f32,
}

/// Build a deterministic HNSW sidecar from immutable visible put entries.
pub fn build_hnsw_index(
    segment_id: impl Into<String>,
    metric: DistanceMetric,
    params: HnswBuildParams,
    entries: &[HnswIndexEntrySource],
) -> io::Result<HnswIndexSidecar> {
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

    for entry in entries {
        insert_hnsw_entry(&mut index, entry)?;
    }

    Ok(index)
}

/// Persist an HNSW sidecar to disk as a binary artifact.
pub fn write_hnsw_index(path: &Path, sidecar: &HnswIndexSidecar) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

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
    fs::write(path, bytes)
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
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        let level = read_u8(&bytes, &mut cursor)?;
        let entry_offset_index = read_u32(&bytes, &mut cursor)? as usize;
        let seq_no = read_u64(&bytes, &mut cursor)?;
        let record_id = RecordId::new(read_string(&bytes, &mut cursor)?);
        let vector = read_f32_slice(&bytes, &mut cursor)?;
        let metadata = serde_json::from_str::<Value>(&read_string(&bytes, &mut cursor)?)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        if dimensions != 0 && vector.len() != dimensions {
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
        let mut neighbors_by_level = Vec::with_capacity(level_count);
        for _ in 0..level_count {
            let neighbor_count = read_u32(&bytes, &mut cursor)? as usize;
            let mut neighbors = Vec::with_capacity(neighbor_count);
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
    let mut candidates = Vec::with_capacity(top_k);
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

fn validate_hnsw_index(sidecar: &HnswIndexSidecar) -> io::Result<()> {
    if !sidecar.nodes.is_empty() && sidecar.entry_point.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "non-empty hnsw sidecar is missing an entry point",
        ));
    }

    if let Some(entry_point) = sidecar.entry_point {
        if entry_point as usize >= sidecar.nodes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "entry point {entry_point} is out of range for {} nodes",
                    sidecar.nodes.len()
                ),
            ));
        }
        if sidecar.nodes[entry_point as usize].level != sidecar.max_level {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "entry point level {} does not match max level {}",
                    sidecar.nodes[entry_point as usize].level, sidecar.max_level
                ),
            ));
        }
    }

    for (node_index, node) in sidecar.nodes.iter().enumerate() {
        if node.neighbors_by_level.len() != usize::from(node.level) + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "node {node_index} level {} expected {} neighbor lists but found {}",
                    node.level,
                    usize::from(node.level) + 1,
                    node.neighbors_by_level.len()
                ),
            ));
        }
        for (layer, neighbors) in node.neighbors_by_level.iter().enumerate() {
            for neighbor in neighbors {
                let neighbor_index = *neighbor as usize;
                let Some(neighbor_node) = sidecar.nodes.get(neighbor_index) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "node {node_index} layer {layer} references out-of-range neighbor {neighbor_index}"
                        ),
                    ));
                };
                if usize::from(neighbor_node.level) < layer {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "node {node_index} layer {layer} references neighbor {neighbor_index} below that layer"
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

fn insert_hnsw_entry(
    sidecar: &mut HnswIndexSidecar,
    entry: &HnswIndexEntrySource,
) -> io::Result<()> {
    let level = deterministic_level(&entry.record_id, entry.seq_no);
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

    if sidecar.entry_point.is_none() {
        sidecar.entry_point = Some(new_index as u32);
        sidecar.max_level = level;
        return Ok(());
    }

    let mut current_entry = sidecar.entry_point.unwrap_or_default() as usize;
    if sidecar.max_level > level {
        for layer in ((level + 1)..=sidecar.max_level).rev() {
            current_entry = greedy_descent(
                sidecar,
                &sidecar.nodes[new_index].record.vector,
                layer,
                current_entry,
            )?;
        }
    }

    let upper_layer = sidecar.max_level.min(level);
    for layer in (0..=upper_layer).rev() {
        let (candidates, _) = search_layer(
            sidecar,
            &sidecar.nodes[new_index].record.vector,
            layer,
            &[current_entry],
            sidecar
                .params
                .ef_construction
                .max(sidecar.params.max_neighbors),
            Some(new_index),
        )?;
        let selected =
            select_best_neighbors(sidecar.metric, candidates, sidecar.params.max_neighbors);
        if let Some(best) = selected.first() {
            current_entry = *best;
        }
        connect_node(sidecar, new_index, layer as usize, &selected);
    }

    if level > sidecar.max_level {
        sidecar.max_level = level;
        sidecar.entry_point = Some(new_index as u32);
    }
    Ok(())
}

fn deterministic_level(record_id: &RecordId, seq_no: SeqNo) -> u8 {
    let mut hasher = DefaultHasher::new();
    record_id.hash(&mut hasher);
    seq_no.hash(&mut hasher);
    let mut value = hasher.finish();
    let mut level = 0u8;
    while level < MAX_HNSW_LEVEL && value & 1 == 0 {
        level += 1;
        value >>= 1;
    }
    level
}

fn greedy_descent(
    sidecar: &HnswIndexSidecar,
    query: &[f32],
    layer: u8,
    mut current: usize,
) -> io::Result<usize> {
    loop {
        let current_value =
            metric_value(sidecar.metric, query, &sidecar.nodes[current].record.vector)?;
        let mut best_index = current;
        let mut best_value = current_value;
        for neighbor in neighbor_indices(sidecar, current, layer) {
            let value = metric_value(
                sidecar.metric,
                query,
                &sidecar.nodes[neighbor].record.vector,
            )?;
            if is_better(sidecar.metric, value, best_value) {
                best_index = neighbor;
                best_value = value;
            }
        }
        if best_index == current {
            return Ok(current);
        }
        current = best_index;
    }
}

fn search_layer(
    sidecar: &HnswIndexSidecar,
    query: &[f32],
    layer: u8,
    entry_points: &[usize],
    ef: usize,
    exclude_index: Option<usize>,
) -> io::Result<(Vec<ScoredNode>, usize)> {
    let mut visited = HashSet::new();
    let mut candidates = Vec::<ScoredNode>::new();
    let mut results = Vec::<ScoredNode>::new();

    for &entry_point in entry_points {
        if Some(entry_point) == exclude_index || entry_point >= sidecar.nodes.len() {
            continue;
        }
        if !visited.insert(entry_point) {
            continue;
        }
        let value = metric_value(
            sidecar.metric,
            query,
            &sidecar.nodes[entry_point].record.vector,
        )?;
        let scored = ScoredNode {
            index: entry_point,
            value,
        };
        candidates.push(scored);
        results.push(scored);
    }

    while !candidates.is_empty() {
        let candidate = pop_best_scored(sidecar.metric, &mut candidates);
        let Some(worst_result) = worst_scored(sidecar.metric, &results) else {
            break;
        };
        if results.len() >= ef
            && is_worse_or_equal(sidecar.metric, candidate.value, worst_result.value)
        {
            break;
        }

        for neighbor in neighbor_indices(sidecar, candidate.index, layer) {
            if Some(neighbor) == exclude_index || !visited.insert(neighbor) {
                continue;
            }
            let value = metric_value(
                sidecar.metric,
                query,
                &sidecar.nodes[neighbor].record.vector,
            )?;
            let scored = ScoredNode {
                index: neighbor,
                value,
            };
            if results.len() < ef
                || worst_scored(sidecar.metric, &results)
                    .is_some_and(|worst| is_better(sidecar.metric, value, worst.value))
            {
                candidates.push(scored);
                results.push(scored);
                if results.len() > ef {
                    drop_worst_scored(sidecar.metric, &mut results);
                }
            }
        }
    }

    sort_scored_best_first(sidecar.metric, &mut results);
    Ok((results, visited.len()))
}

fn select_best_neighbors(
    metric: DistanceMetric,
    mut candidates: Vec<ScoredNode>,
    limit: usize,
) -> Vec<usize> {
    sort_scored_best_first(metric, &mut candidates);
    candidates
        .into_iter()
        .take(limit)
        .map(|candidate| candidate.index)
        .collect()
}

fn connect_node(
    sidecar: &mut HnswIndexSidecar,
    node_index: usize,
    layer_index: usize,
    neighbors: &[usize],
) {
    for &neighbor in neighbors {
        sidecar.nodes[node_index].neighbors_by_level[layer_index].push(neighbor as u32);
        if sidecar.nodes[neighbor].neighbors_by_level.len() <= layer_index {
            continue;
        }
        sidecar.nodes[neighbor].neighbors_by_level[layer_index].push(node_index as u32);
        let trimmed = trimmed_neighbors(
            sidecar.metric,
            &sidecar.nodes[neighbor].neighbors_by_level[layer_index],
            &sidecar.nodes,
            neighbor,
            sidecar.params.max_neighbors,
        );
        sidecar.nodes[neighbor].neighbors_by_level[layer_index] = trimmed;
    }
    let trimmed = trimmed_neighbors(
        sidecar.metric,
        &sidecar.nodes[node_index].neighbors_by_level[layer_index],
        &sidecar.nodes,
        node_index,
        sidecar.params.max_neighbors,
    );
    sidecar.nodes[node_index].neighbors_by_level[layer_index] = trimmed;
}

fn trimmed_neighbors(
    metric: DistanceMetric,
    neighbors: &[u32],
    nodes: &[HnswNode],
    node_index: usize,
    limit: usize,
) -> Vec<u32> {
    let mut unique = neighbors
        .iter()
        .copied()
        .filter(|neighbor| *neighbor as usize != node_index)
        .collect::<Vec<_>>();
    unique.sort_unstable();
    unique.dedup();
    unique.sort_by(|left, right| {
        let left_value = metric_value(
            metric,
            &nodes[node_index].record.vector,
            &nodes[*left as usize].record.vector,
        )
        .unwrap_or_default();
        let right_value = metric_value(
            metric,
            &nodes[node_index].record.vector,
            &nodes[*right as usize].record.vector,
        )
        .unwrap_or_default();
        metric_compare(metric, left_value, right_value).reverse()
    });
    unique.truncate(limit);
    unique
}

fn neighbor_indices(sidecar: &HnswIndexSidecar, node_index: usize, layer: u8) -> Vec<usize> {
    sidecar.nodes[node_index]
        .neighbors_by_level
        .get(layer as usize)
        .into_iter()
        .flatten()
        .map(|neighbor| *neighbor as usize)
        .collect()
}

fn pop_best_scored(metric: DistanceMetric, scored: &mut Vec<ScoredNode>) -> ScoredNode {
    let mut best_index = 0usize;
    for index in 1..scored.len() {
        if is_better(metric, scored[index].value, scored[best_index].value) {
            best_index = index;
        }
    }
    scored.swap_remove(best_index)
}

fn worst_scored(metric: DistanceMetric, scored: &[ScoredNode]) -> Option<ScoredNode> {
    let mut worst = *scored.first()?;
    for candidate in &scored[1..] {
        if is_worse_or_equal(metric, candidate.value, worst.value) {
            worst = *candidate;
        }
    }
    Some(worst)
}

fn drop_worst_scored(metric: DistanceMetric, scored: &mut Vec<ScoredNode>) {
    if scored.is_empty() {
        return;
    }
    let mut worst_index = 0usize;
    for index in 1..scored.len() {
        if is_worse_or_equal(metric, scored[index].value, scored[worst_index].value) {
            worst_index = index;
        }
    }
    scored.swap_remove(worst_index);
}

fn sort_scored_best_first(metric: DistanceMetric, scored: &mut [ScoredNode]) {
    scored.sort_by(|left, right| metric_compare(metric, right.value, left.value));
}

fn is_better(metric: DistanceMetric, left: f32, right: f32) -> bool {
    metric_compare(metric, left, right) == Ordering::Greater
}

fn is_worse_or_equal(metric: DistanceMetric, left: f32, right: f32) -> bool {
    let ordering = metric_compare(metric, left, right);
    ordering == Ordering::Less || ordering == Ordering::Equal
}

fn metric_compare(metric: DistanceMetric, left: f32, right: f32) -> Ordering {
    match metric {
        DistanceMetric::Cosine | DistanceMetric::Dot => {
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }
        DistanceMetric::L2 => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
    }
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
        DistanceMetric::Dot => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| lhs * rhs)
            .sum(),
        DistanceMetric::Cosine => {
            let dot: f32 = query
                .iter()
                .zip(candidate)
                .map(|(lhs, rhs)| lhs * rhs)
                .sum();
            let query_norm = vector_norm(query);
            let candidate_norm = vector_norm(candidate);
            if query_norm == 0.0 || candidate_norm == 0.0 {
                0.0
            } else {
                dot / (query_norm * candidate_norm)
            }
        }
        DistanceMetric::L2 => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| {
                let delta = lhs - rhs;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
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
    let mut values = Vec::with_capacity(len);
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

fn vector_norm(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum::<f32>().sqrt()
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

    fn temp_file_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        std::env::temp_dir().join(format!("logpose-{unique}-{name}"))
    }
}
