//! Index sections: building them from a [`SegmentBuilder`]'s rows, and the
//! decoded forms readers attach to their cached bytes.
//!
//! Flush and compaction both write segments through the one
//! [`SegmentBuilder`]; [`SegmentBuilder::build_index_sections`] adds, per
//! [`IndexPolicy`]:
//!
//! - `ScalarInverted` and `ScalarSorted` for every scalar field whose index
//!   asks for them (`logpose-index` scalar payloads);
//! - `VectorSq8` for a vector field with at least `sq8_min_rows` non-null
//!   vectors (`logpose-index` SQ8 params plus one code per row, zero codes
//!   for null rows).
//!
//! Both are one or two passes over the rows, so a flush stays bounded by its
//! memtable's size. `VectorGraph` sections are not built with the segment:
//! the engine's index-build job reads a written segment's vectors back and
//! builds each field's graph with [`build_graph_section`] into an index
//! sidecar ([`write_index_sidecar`](super::write_index_sidecar)).
//!
//! The graph is built over *distinct* vectors, not rows: exact duplicates
//! would otherwise collapse into mutually unreachable islands (with 100
//! copies of each of 200 vectors, about 60 percent of rows were orphaned).
//! The graph section therefore carries a node map, node to rows, around the
//! `logpose-index` graph bytes:
//!
//! ```text
//! offset size field
//!      0    8 magic "LPVGRAPH"
//!      8    4 version (1)
//!     12    4 flags: bit 0 = identity map (node i is row i)
//!     16    4 node_count
//!     20    4 row_count of the segment
//!     24    4 mapped rows (entries in node_rows)
//!     28    4 reserved, zero
//!     32      without the identity flag: u32 node_offsets[node_count + 1],
//!             then u32 node_rows[mapped rows] (ascending within a node),
//!             zero padding to 8
//!      .    . HnswGraph::to_bytes over nodes 0..node_count
//! ```
//!
//! Rows with a null vector are in no node. Integers are little-endian.

use super::{
    builder::{IndexSection, IndexSectionKind, SegmentBuilder},
    column::{ColumnBuf, ColumnData, ElemBuf},
    error::SegmentError,
    format::SectionKind,
    vector::VectorBuf,
};
use logpose_index::{
    graph::{F32Metric, F32Vectors, GraphError, HnswGraph, HnswParams},
    scalar::{InvertedIndex, KeyKind, ScalarIndexBuilder, ScalarKey, SortedIndex},
    sq8::{Sq8Params, Sq8Section, write_codes_section},
};
use logpose_types::{
    DistanceMetric,
    schema::{ElementType, FieldId, FieldType},
};
use std::collections::HashMap;

/// Encoding code of the index payloads this module writes.
pub const INDEX_ENCODING_V1: u16 = 1;

const GRAPH_MAGIC: [u8; 8] = *b"LPVGRAPH";
const GRAPH_VERSION: u32 = 1;
const GRAPH_HEADER_LEN: usize = 32;
const GRAPH_IDENTITY: u32 = 1;

/// Which index sections a segment build writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexPolicy {
    /// Build a segment's `VectorGraph` sidecar in the background as soon as
    /// a vector field has at least this many non-null vectors (and SQ8
    /// codes). Smaller segments get theirs once the collection is quiet (see
    /// `CompactionConfig::quiet_after`) or an explicit compaction settles it;
    /// until then they are searched by SQ8 scan. Default 20,000.
    pub graph_min_rows: u32,
    /// Write `VectorSq8` codes when a vector field has at least this many
    /// non-null vectors. Default 1,024.
    pub sq8_min_rows: u32,
    /// Graph construction parameters.
    pub hnsw: HnswParams,
    /// Write scalar inverted and sorted indexes. Default on.
    pub scalar: bool,
}

impl Default for IndexPolicy {
    fn default() -> Self {
        Self {
            graph_min_rows: 20_000,
            sq8_min_rows: 1_024,
            hnsw: HnswParams::default(),
            scalar: true,
        }
    }
}

/// What [`SegmentBuilder::build_index_sections`] added.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexBuildReport {
    /// Sections added, by kind.
    pub sections: Vec<(IndexSectionKind, u32)>,
}

impl SegmentBuilder {
    /// Build and attach every index section `policy` asks for, from the rows
    /// pushed so far. Graph builds run on the current `rayon` pool (the
    /// engine installs its maintenance pool).
    ///
    /// # Errors
    ///
    /// [`SegmentError::Encode`] if an index cannot be built or encoded, and
    /// the errors of [`add_index_section`](Self::add_index_section).
    pub fn build_index_sections(
        &mut self,
        policy: &IndexPolicy,
    ) -> Result<IndexBuildReport, SegmentError> {
        let schema = std::sync::Arc::clone(self.schema());
        let rows = self.row_count();
        let mut sections = Vec::new();
        {
            let (vectors, columns) = self.index_inputs();
            if policy.scalar {
                for column in columns {
                    let Some(field) = schema
                        .fields()
                        .iter()
                        .find(|field| field.id == column.field)
                    else {
                        continue;
                    };
                    let (inverted, sorted) = (field.index.has_inverted(), field.index.has_sorted());
                    if !inverted && !sorted {
                        continue;
                    }
                    let Some(builder) = scalar_builder(column)? else {
                        continue;
                    };
                    let (inverted_index, sorted_index) = if sorted {
                        let (inverted_index, sorted_index) = builder.build().map_err(encode)?;
                        (inverted.then_some(inverted_index), Some(sorted_index))
                    } else {
                        (Some(builder.build_inverted().map_err(encode)?), None)
                    };
                    if let Some(index) = inverted_index {
                        let mut payload = Vec::new();
                        index.write_to(&mut payload).map_err(encode)?;
                        sections.push(section(
                            IndexSectionKind::ScalarInverted,
                            column.field,
                            0,
                            u64::from(rows),
                            payload,
                        ));
                    }
                    if let Some(index) = sorted_index {
                        let mut payload = Vec::new();
                        index.write_to(&mut payload).map_err(encode)?;
                        sections.push(section(
                            IndexSectionKind::ScalarSorted,
                            column.field,
                            0,
                            u64::from(rows),
                            payload,
                        ));
                    }
                }
            }
            for vector in vectors {
                if !schema
                    .vectors()
                    .iter()
                    .any(|field| field.id == vector.field)
                {
                    continue;
                }
                sections.extend(sq8_section(vector, policy)?);
            }
        }
        let mut report = IndexBuildReport::default();
        for section in sections {
            report.sections.push((section.kind, section.field.0));
            self.add_index_section(section)?;
        }
        Ok(report)
    }
}

fn encode(error: impl std::fmt::Display) -> SegmentError {
    SegmentError::Encode(error.to_string())
}

fn section(
    kind: IndexSectionKind,
    field: FieldId,
    aux32: u32,
    aux64: u64,
    payload: Vec<u8>,
) -> IndexSection {
    IndexSection {
        kind,
        field,
        encoding: INDEX_ENCODING_V1,
        aux32,
        aux64,
        payload,
    }
}

/// The key kind a field type indexes as; `None` for JSON.
fn key_kind(field_type: FieldType) -> Option<KeyKind> {
    let element = match field_type {
        FieldType::Bool => ElementType::Bool,
        FieldType::Int64 => ElementType::Int64,
        FieldType::Float64 => ElementType::Float64,
        FieldType::String => ElementType::String,
        FieldType::Timestamp => ElementType::Timestamp,
        FieldType::Array(element) => element,
        FieldType::Json => return None,
    };
    Some(match element {
        ElementType::Bool => KeyKind::Bool,
        ElementType::Int64 | ElementType::Timestamp => KeyKind::Int,
        ElementType::Float64 => KeyKind::Float,
        ElementType::String => KeyKind::Str,
    })
}

/// A scalar index builder over one column's rows; `None` for unindexable
/// types.
fn scalar_builder(column: &ColumnBuf) -> Result<Option<ScalarIndexBuilder>, SegmentError> {
    let Some(kind) = key_kind(column.field_type) else {
        return Ok(None);
    };
    let mut builder = ScalarIndexBuilder::new(kind);
    let str_key = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .map(ScalarKey::string)
            .map_err(encode)
    };
    for row in 0..column.rows {
        if column.nulls.contains(row) {
            builder.insert_null(row);
            continue;
        }
        let index = row as usize;
        let mut keys = Vec::new();
        match &column.data {
            ColumnData::Bool(values) => keys.push(ScalarKey::Bool(values[index])),
            ColumnData::Int(values) => keys.push(ScalarKey::Int(values[index])),
            ColumnData::Float(values) => {
                keys.push(ScalarKey::float(values[index]).map_err(encode)?)
            }
            ColumnData::Str(values) => keys.push(str_key(values.get(index))?),
            ColumnData::Json(_) => return Ok(None),
            ColumnData::Array { ends, elements } => {
                let start = if index == 0 { 0 } else { ends[index - 1] };
                for element in start..ends[index] {
                    keys.push(match elements {
                        ElemBuf::Bool(values) => ScalarKey::Bool(values[element]),
                        ElemBuf::Int(values) => ScalarKey::Int(values[element]),
                        ElemBuf::Float(values) => {
                            ScalarKey::float(values[element]).map_err(encode)?
                        }
                        ElemBuf::Str(values) => str_key(values.get(element))?,
                    });
                }
            }
        }
        if keys.is_empty() {
            // An empty array has no value to index, so it indexes as null, as in memtables.
            builder.insert_null(row);
            continue;
        }
        for key in keys {
            builder.insert(row, key).map_err(encode)?;
        }
    }
    Ok(Some(builder))
}

/// The SQ8 section of one vector field, or `None` below `sq8_min_rows` non-null vectors or
/// for a range too wide for SQ8 (searches then use the f32 vectors). The bounds are found in
/// one pass over the builder's bytes and the codes in a second, so the build holds the codes
/// and their encoded payload, not an f32 copy of the vectors.
fn sq8_section(
    vector: &VectorBuf,
    policy: &IndexPolicy,
) -> Result<Option<IndexSection>, SegmentError> {
    let dim = vector.dim as usize;
    let rows = vector.rows;
    let non_null = u64::from(rows).saturating_sub(vector.nulls.len());
    if non_null == 0 || dim == 0 || non_null < u64::from(policy.sq8_min_rows) {
        return Ok(None);
    }
    let stride = dim * 4;
    let live_rows = || (0..rows).filter(|row| !vector.nulls.contains(*row));
    let values = |row: u32| {
        vector.data[row as usize * stride..(row as usize + 1) * stride]
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
    };
    let mut min = vec![f32::INFINITY; dim];
    let mut max = vec![f32::NEG_INFINITY; dim];
    for row in live_rows() {
        for ((low, high), value) in min.iter_mut().zip(max.iter_mut()).zip(values(row)) {
            if !value.is_finite() {
                return Ok(None);
            }
            *low = low.min(value);
            *high = high.max(value);
        }
    }
    // A range too wide for f32 leaves the field without codes.
    let Ok(params) = Sq8Params::from_bounds(min, max) else {
        return Ok(None);
    };
    let mut codes = vec![0_u8; rows as usize * dim];
    let mut row_values = Vec::with_capacity(dim);
    for row in live_rows() {
        row_values.clear();
        row_values.extend(values(row));
        let start = row as usize * dim;
        if params
            .encode_into(&row_values, &mut codes[start..start + dim])
            .is_err()
        {
            return Ok(None);
        }
    }
    let mut payload = Vec::new();
    write_codes_section(&params, u64::from(rows), &codes, &mut payload).map_err(encode)?;
    Ok(Some(section(
        IndexSectionKind::VectorSq8,
        vector.field,
        vector.dim,
        u64::from(rows),
        payload,
    )))
}

/// The input of one field's graph build: every row's vector, row-major, as a segment stores
/// it (zeros for null rows).
#[derive(Debug)]
pub struct GraphInput {
    /// The vector field.
    pub field: FieldId,
    /// Its dimension.
    pub dim: u32,
    /// Its metric.
    pub metric: DistanceMetric,
    /// Rows in the segment.
    pub rows: u32,
    /// `rows * dim` values.
    pub values: Vec<f32>,
    /// Rows without a vector; they are in no graph node.
    pub nulls: roaring::RoaringBitmap,
}

/// Build the `VectorGraph` section of one field of a segment: an HNSW graph over its distinct
/// non-null vectors, wrapped with the node-to-rows map (see the module docs). `None` when the
/// field has fewer than `min_nodes` distinct vectors (at least two). The graph builds on the
/// current `rayon` pool (the engine installs its maintenance pool) and polls `cancelled`
/// before every insert.
///
/// The build holds `input.values` (one f32 copy of the field), a map from each distinct vector
/// to its rows while it deduplicates, and the graph's link lists; with null rows or duplicates it
/// moves the distinct vectors to the front of that buffer in place, never into a second one (the
/// index build's memory reservation charges one f32 copy).
///
/// # Errors
///
/// [`SegmentError::Cancelled`] once `cancelled` returned `true`, and
/// [`SegmentError::Encode`] if the graph cannot be built or encoded.
pub fn build_graph_section(
    input: GraphInput,
    params: HnswParams,
    min_nodes: u32,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<Option<IndexSection>, SegmentError> {
    let GraphInput {
        field,
        dim,
        metric,
        rows,
        values,
        nulls,
    } = input;
    let width = dim as usize;
    if width == 0 || values.len() != rows as usize * width {
        return Err(SegmentError::Encode(format!(
            "graph input of field {field} holds {} values for {rows} rows of dimension {dim}",
            values.len()
        )));
    }
    // One node per distinct vector, in order of first appearance.
    let mut node_rows: Vec<Vec<u32>> = Vec::new();
    {
        let mut node_of: HashMap<&[u8], u32> = HashMap::new();
        let bytes: &[u8] = bytemuck::cast_slice(&values);
        for row in (0..rows).filter(|row| !nulls.contains(*row)) {
            let at = row as usize * width * 4;
            let next = u32::try_from(node_rows.len()).map_err(encode)?;
            let node = *node_of.entry(&bytes[at..at + width * 4]).or_insert(next);
            if node == next {
                node_rows.push(Vec::new());
            }
            node_rows[node as usize].push(row);
        }
    }
    if (node_rows.len() as u64) < u64::from(min_nodes.max(2)) {
        return Ok(None);
    }
    let identity = node_rows.len() == rows as usize
        && node_rows
            .iter()
            .enumerate()
            .all(|(node, rows)| rows.len() == 1 && rows[0] as usize == node);
    let data = if identity {
        values
    } else {
        distinct_in_place(values, width, &node_rows)
    };
    let graph_metric = match metric {
        DistanceMetric::L2 => F32Metric::L2Squared,
        DistanceMetric::Cosine | DistanceMetric::Dot => F32Metric::NegativeDot,
    };
    let source = F32Vectors::new(width, data, graph_metric).map_err(encode)?;
    let graph = match HnswGraph::build_parallel_cancellable(&source, params, cancelled) {
        Ok(graph) => graph,
        Err(GraphError::Cancelled) => return Err(SegmentError::Cancelled),
        Err(error) => return Err(encode(error)),
    };
    drop(source);
    let payload = encode_graph_section(rows, &node_rows, identity, &graph)?;
    Ok(Some(section(
        IndexSectionKind::VectorGraph,
        field,
        dim,
        u64::from(rows),
        payload,
    )))
}

/// `values` (row-major, `width` values a row) with each node's vector, its first row's, moved
/// to position `node` and the rest truncated: the graph's input, in the buffer the rows came in,
/// so the build never holds a second copy of the vectors. Nodes are numbered in order of first
/// appearance, so node `i`'s first row `r_i` is at least `i` and grows with `i`: every copy
/// moves a vector towards the front, onto a slot no later node reads from. Truncated, not
/// shrunk: a shrinking `realloc` may copy, and the spare capacity is within what the index
/// build reserved.
fn distinct_in_place(mut values: Vec<f32>, width: usize, node_rows: &[Vec<u32>]) -> Vec<f32> {
    for (node, rows_of_node) in node_rows.iter().enumerate() {
        let from = rows_of_node[0] as usize * width;
        let to = node * width;
        if from != to {
            values.copy_within(from..from + width, to);
        }
    }
    values.truncate(node_rows.len() * width);
    values
}

fn encode_graph_section(
    rows: u32,
    node_rows: &[Vec<u32>],
    identity: bool,
    graph: &HnswGraph,
) -> Result<Vec<u8>, SegmentError> {
    let nodes = u32::try_from(node_rows.len()).map_err(encode)?;
    let mapped = u32::try_from(node_rows.iter().map(Vec::len).sum::<usize>()).map_err(encode)?;
    let mut out = Vec::new();
    out.extend_from_slice(&GRAPH_MAGIC);
    out.extend_from_slice(&GRAPH_VERSION.to_le_bytes());
    out.extend_from_slice(&(if identity { GRAPH_IDENTITY } else { 0 }).to_le_bytes());
    out.extend_from_slice(&nodes.to_le_bytes());
    out.extend_from_slice(&rows.to_le_bytes());
    out.extend_from_slice(&(if identity { 0 } else { mapped }).to_le_bytes());
    out.extend_from_slice(&[0; 4]);
    if !identity {
        let mut offset = 0_u32;
        out.extend_from_slice(&offset.to_le_bytes());
        for rows_of_node in node_rows {
            offset += rows_of_node.len() as u32;
            out.extend_from_slice(&offset.to_le_bytes());
        }
        for rows_of_node in node_rows {
            for row in rows_of_node {
                out.extend_from_slice(&row.to_le_bytes());
            }
        }
        out.resize(out.len().div_ceil(8) * 8, 0);
    }
    out.extend_from_slice(&graph.to_bytes());
    Ok(out)
}

/// Which rows each graph node stands for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeMap {
    /// Node `i` is row `i`, for every row.
    Identity,
    /// Node `i` is rows `rows[offsets[i]..offsets[i + 1]]`.
    Mapped {
        /// `node_count + 1` offsets into `rows`.
        offsets: Vec<u32>,
        /// Rows, ascending within each node.
        rows: Vec<u32>,
    },
}

impl NodeMap {
    /// The rows of `node`.
    #[must_use]
    pub fn rows(&self, node: u32) -> NodeRows<'_> {
        match self {
            Self::Identity => NodeRows::One(Some(node)),
            Self::Mapped { offsets, rows } => {
                let start = offsets.get(node as usize).copied().unwrap_or(0) as usize;
                let end = offsets.get(node as usize + 1).copied().unwrap_or(0) as usize;
                NodeRows::Many(rows.get(start..end).unwrap_or(&[]).iter())
            }
        }
    }

    /// The first row of `node`, whose vector (and code) the node stands for.
    #[must_use]
    pub fn first_row(&self, node: u32) -> Option<u32> {
        self.rows(node).next()
    }

    /// Whether nodes are rows.
    #[must_use]
    pub fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }
}

/// Iterator over the rows of one node.
#[derive(Clone, Debug)]
pub enum NodeRows<'a> {
    /// A single row.
    One(Option<u32>),
    /// Several rows.
    Many(std::slice::Iter<'a, u32>),
}

impl Iterator for NodeRows<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        match self {
            Self::One(row) => row.take(),
            Self::Many(rows) => rows.next().copied(),
        }
    }
}

/// A decoded `VectorGraph` section: the graph over distinct vectors and the
/// rows each node stands for.
#[derive(Debug)]
pub struct SegmentGraph {
    /// The graph; node ids index `nodes`.
    pub graph: HnswGraph,
    /// Node to rows.
    pub nodes: NodeMap,
}

impl SegmentGraph {
    /// Decode and validate a graph section of a segment of `row_count` rows.
    ///
    /// # Errors
    ///
    /// A description of the defect.
    pub fn decode(bytes: &[u8], row_count: u32) -> Result<Self, String> {
        let header = bytes
            .get(..GRAPH_HEADER_LEN)
            .ok_or("truncated graph header")?;
        if header[..8] != GRAPH_MAGIC {
            return Err("bad graph magic".to_owned());
        }
        let word = |at: usize| {
            u32::from_le_bytes([header[at], header[at + 1], header[at + 2], header[at + 3]])
        };
        if word(8) != GRAPH_VERSION {
            return Err(format!("unsupported graph section version {}", word(8)));
        }
        let flags = word(12);
        let nodes = word(16);
        let rows = word(20);
        let mapped = word(24) as usize;
        if flags & !GRAPH_IDENTITY != 0 || word(28) != 0 {
            return Err("reserved graph header bits are set".to_owned());
        }
        if rows != row_count {
            return Err("graph row count differs from the segment".to_owned());
        }
        let identity = flags & GRAPH_IDENTITY != 0;
        let (map, graph_start) = if identity {
            if nodes != rows || mapped != 0 {
                return Err("an identity graph map must cover every row".to_owned());
            }
            (NodeMap::Identity, GRAPH_HEADER_LEN)
        } else {
            let words = (nodes as usize + 1)
                .checked_add(mapped)
                .ok_or("graph map overflows")?;
            let end = GRAPH_HEADER_LEN
                .checked_add(words.checked_mul(4).ok_or("graph map overflows")?)
                .ok_or("graph map overflows")?;
            let raw = bytes
                .get(GRAPH_HEADER_LEN..end)
                .ok_or("truncated graph map")?;
            let values: Vec<u32> = raw
                .chunks_exact(4)
                .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect();
            let (offsets, node_rows) = values.split_at(nodes as usize + 1);
            if offsets.first() != Some(&0)
                || offsets.last().map(|last| *last as usize) != Some(mapped)
                || offsets.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err("graph node offsets are not strictly increasing".to_owned());
            }
            let mut seen = roaring::RoaringBitmap::new();
            for pair in offsets.windows(2) {
                let rows_of_node = &node_rows[pair[0] as usize..pair[1] as usize];
                if rows_of_node.windows(2).any(|rows| rows[0] >= rows[1]) {
                    return Err("graph node rows are not ascending".to_owned());
                }
                for row in rows_of_node {
                    if *row >= row_count || !seen.insert(*row) {
                        return Err("graph node rows are out of range or repeat".to_owned());
                    }
                }
            }
            (
                NodeMap::Mapped {
                    offsets: offsets.to_vec(),
                    rows: node_rows.to_vec(),
                },
                end.div_ceil(8) * 8,
            )
        };
        let graph_bytes = bytes.get(graph_start..).ok_or("truncated graph")?;
        let graph = HnswGraph::from_bytes(graph_bytes).map_err(|error| error.to_string())?;
        if graph.len() != nodes as usize {
            return Err("graph node count differs from its header".to_owned());
        }
        Ok(Self { graph, nodes: map })
    }

    /// Heap bytes the decoded graph holds.
    #[must_use]
    pub fn heap_bytes(&self) -> u64 {
        let map = match &self.nodes {
            NodeMap::Identity => 0,
            NodeMap::Mapped { offsets, rows } => (offsets.len() + rows.len()) * 4,
        };
        (self.graph.memory_bytes() + map) as u64
    }
}

/// Decoded scalar index section.
#[derive(Debug)]
pub enum DecodedScalarIndex {
    /// An inverted index.
    Inverted(InvertedIndex),
    /// A sorted index.
    Sorted(SortedIndex),
}

/// Decode an index or key section's payload into the form readers attach to
/// it, so the cache charges the decoded form with the bytes. Other kinds are
/// left as they are.
///
/// # Errors
///
/// A description of the defect.
pub(crate) fn attach_decoded(
    entry: &super::format::SectionEntry,
    row_count: u32,
    bytes: &crate::cache::AlignedBytes,
) -> Result<(), String> {
    let rows = row_count as usize;
    match entry.section_kind() {
        // The key sections decode into owned copies of about their size.
        Some(SectionKind::PkColumn) => {
            let column =
                super::PkColumn::decode(bytes, entry.encoding, rows).map_err(|error| error.0)?;
            bytes.attach(column, bytes.len() as u64);
        }
        Some(SectionKind::PkSorted) => {
            let sorted =
                super::PkSorted::decode(bytes, entry.encoding, rows).map_err(|error| error.0)?;
            bytes.attach(sorted, bytes.len() as u64);
        }
        Some(SectionKind::PkFilter) => {
            let filter = super::PkFilter::decode(bytes, entry.encoding).map_err(|error| error.0)?;
            bytes.attach(filter, bytes.len() as u64);
        }
        Some(SectionKind::VectorGraph) => {
            let graph = SegmentGraph::decode(bytes, row_count)?;
            let heap = graph.heap_bytes();
            bytes.attach(graph, heap);
        }
        Some(SectionKind::VectorSq8) => {
            let section = Sq8Section::parse(bytes).map_err(|error| error.to_string())?;
            if section.rows() != row_count as usize {
                return Err("sq8 row count differs from the segment".to_owned());
            }
            let heap = section.params().dims() as u64 * 16;
            bytes.attach(section, heap);
        }
        Some(SectionKind::ScalarInverted) => {
            let index = InvertedIndex::from_bytes(bytes).map_err(|error| error.to_string())?;
            let heap = bytes.len() as u64;
            bytes.attach(DecodedScalarIndex::Inverted(index), heap);
        }
        Some(SectionKind::ScalarSorted) => {
            let index = SortedIndex::from_bytes(bytes).map_err(|error| error.to_string())?;
            let heap = bytes.len() as u64;
            bytes.attach(DecodedScalarIndex::Sorted(index), heap);
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_v2::SegmentIdentity;
    use logpose_types::{
        CollectionId,
        record::Record,
        schema::{CollectionSchema, PrimaryKeySpec, PrimaryKeyType, VectorFieldSpec},
    };
    use logpose_wal::codec::RowImage;
    use std::sync::Arc;

    fn built(vectors: impl Fn(u32) -> Vec<f32>) -> Vec<(IndexSectionKind, u32)> {
        let schema = Arc::new(
            CollectionSchema::new(
                PrimaryKeySpec {
                    name: "id".to_owned(),
                    key_type: PrimaryKeyType::String,
                },
                vec![VectorFieldSpec {
                    name: "v".to_owned(),
                    dimensions: 2,
                    metric: DistanceMetric::L2,
                }],
                Vec::new(),
                false,
            )
            .expect("schema"),
        );
        let mut builder = SegmentBuilder::new(
            Arc::clone(&schema),
            SegmentIdentity {
                collection_id: CollectionId::default(),
                unit_id: 1,
            },
        )
        .expect("builder");
        for row in 0..16 {
            let record = Record::new(format!("k{row:02}")).with_vector("v", vectors(row));
            let image = RowImage::from_record(&schema, record).expect("row image");
            builder
                .push_row_image(u64::from(row) + 1, &image)
                .expect("push");
        }
        let policy = IndexPolicy {
            graph_min_rows: 4,
            sq8_min_rows: 2,
            ..IndexPolicy::default()
        };
        builder
            .build_index_sections(&policy)
            .expect("index sections")
            .sections
    }

    /// A damaged graph section never panics or allocates beyond its bytes: every truncation
    /// and every single-byte flip of a mapped section (duplicate vectors) decodes to an error,
    /// or to a graph whose node map stays within the segment.
    #[test]
    fn damaged_graph_sections_decode_to_errors_never_panics() {
        // 12 distinct vectors, each on two rows, plus 6 null rows: 30 rows.
        let rows = 30_u32;
        let data = (0..12_u16)
            .flat_map(|node| [f32::from(node), f32::from(node % 5)])
            .collect::<Vec<_>>();
        let source = F32Vectors::new(2, data, F32Metric::L2Squared).expect("vectors");
        let graph = HnswGraph::build_parallel(&source, HnswParams::default()).expect("graph");
        let node_rows = (0..12_u32)
            .map(|node| vec![node * 2, node * 2 + 1])
            .collect::<Vec<_>>();
        let bytes = encode_graph_section(rows, &node_rows, false, &graph).expect("encodes");
        let decoded = SegmentGraph::decode(&bytes, rows).expect("decodes");
        assert_eq!(decoded.nodes.rows(3).collect::<Vec<_>>(), [6, 7]);
        assert!(SegmentGraph::decode(&bytes, rows + 1).is_err(), "row count");

        let check = |damaged: &[u8]| {
            if let Ok(graph) = SegmentGraph::decode(damaged, rows) {
                for node in 0..u32::try_from(graph.graph.len()).expect("fits") {
                    assert!(graph.nodes.rows(node).all(|row| row < rows));
                }
            }
        };
        for len in 0..bytes.len() {
            check(&bytes[..len]);
        }
        for at in 0..bytes.len() {
            for flip in [0x01_u8, 0x80, 0xff] {
                let mut damaged = bytes.clone();
                damaged[at] ^= flip;
                check(&damaged);
            }
        }
    }

    /// A segment's own index sections are SQ8 codes and scalar indexes, never a graph (the
    /// index-build job adds that later). A vector field whose range is too wide for SQ8 gets
    /// no codes: walks traverse codes, so its segments are scanned exactly in f32.
    #[test]
    fn segments_get_sq8_codes_but_no_graph_and_wide_fields_get_neither() {
        #[allow(clippy::cast_precision_loss)]
        let ordinary = built(|row| vec![row as f32, (row * row) as f32]);
        let kinds = ordinary.iter().map(|(kind, _)| *kind).collect::<Vec<_>>();
        assert!(kinds.contains(&IndexSectionKind::VectorSq8), "{kinds:?}");
        assert!(!kinds.contains(&IndexSectionKind::VectorGraph), "{kinds:?}");

        #[allow(clippy::cast_precision_loss)]
        let wide = built(|row| {
            let sign = if row % 2 == 0 { 1.0 } else { -1.0 };
            vec![sign * f32::MAX, row as f32]
        });
        assert!(
            wide.iter().all(|(kind, _)| !matches!(
                kind,
                IndexSectionKind::VectorSq8 | IndexSectionKind::VectorGraph
            )),
            "{wide:?}"
        );
    }

    /// The codes are the ones training on an f32 copy produced: the streaming bounds equal
    /// `Sq8Params::train`'s.
    #[test]
    fn streamed_sq8_bounds_match_training() {
        #[allow(clippy::cast_precision_loss)]
        let vector = |row: u32| vec![(row as f32).sin() * 3.0, row as f32 - 7.5];
        let mut buf = VectorBuf::new(FieldId(1), 2);
        let mut training = Vec::new();
        for row in 0..40_u32 {
            let values = vector(row);
            for value in &values {
                buf.data.extend_from_slice(&value.to_le_bytes());
            }
            if row % 7 == 3 {
                buf.nulls.insert(row);
            } else {
                training.extend(values);
            }
            buf.rows += 1;
        }
        let policy = IndexPolicy {
            sq8_min_rows: 2,
            ..IndexPolicy::default()
        };
        let section = sq8_section(&buf, &policy).expect("builds").expect("codes");
        let parsed = Sq8Section::parse(&section.payload).expect("parses");
        let trained = Sq8Params::train(&training, 2).expect("trains");
        assert_eq!(parsed.params().min(), trained.min());
        assert_eq!(parsed.params().max(), trained.max());
    }

    /// Moving the distinct vectors to the front in place gives what copying them out would,
    /// whatever null rows and duplicates come before them.
    #[test]
    fn distinct_vectors_move_to_the_front_in_place() {
        // Rows: null, a, a, null, b, a, c, null, b, d (width 3).
        let vector = |id: u32| {
            let base = f32::from(u16::try_from(id).expect("small"));
            [base, base + 0.5, -base]
        };
        let layout: [Option<u32>; 10] = [
            None,
            Some(1),
            Some(1),
            None,
            Some(2),
            Some(1),
            Some(3),
            None,
            Some(2),
            Some(4),
        ];
        let values = layout
            .iter()
            .flat_map(|id| id.map_or([0.0; 3], vector))
            .collect::<Vec<_>>();
        let mut node_rows: Vec<Vec<u32>> = Vec::new();
        let mut node_of = HashMap::new();
        for (row, id) in (0_u32..).zip(layout) {
            let Some(id) = id else { continue };
            let node = *node_of.entry(id).or_insert(node_rows.len());
            if node == node_rows.len() {
                node_rows.push(Vec::new());
            }
            node_rows[node].push(row);
        }
        let copied = node_rows
            .iter()
            .flat_map(|rows| {
                let at = rows[0] as usize * 3;
                values[at..at + 3].to_vec()
            })
            .collect::<Vec<_>>();
        let moved = distinct_in_place(values, 3, &node_rows);
        assert_eq!(moved, copied);
        assert_eq!(
            moved,
            [1, 2, 3, 4].into_iter().flat_map(vector).collect::<Vec<_>>()
        );
    }

    fn graph_input(values: Vec<f32>, nulls: &[u32]) -> GraphInput {
        let rows = u32::try_from(values.len() / 2).expect("fits");
        GraphInput {
            field: FieldId(1),
            dim: 2,
            metric: DistanceMetric::L2,
            rows,
            values,
            nulls: nulls.iter().copied().collect(),
        }
    }

    /// A graph section covers every non-null row through its node map: duplicates share a
    /// node, null rows are in none, and a field with too few distinct vectors gets no graph.
    #[test]
    fn graph_sections_map_distinct_vectors_to_their_rows() {
        // 50 distinct vectors on rows 0..50, each repeated on row + 50; rows 100 and 101 null.
        #[allow(clippy::cast_precision_loss)]
        let mut values = (0..100_u32)
            .flat_map(|row| {
                let node = row % 50;
                [node as f32, (node * node % 17) as f32]
            })
            .collect::<Vec<_>>();
        values.extend([0.0; 4]);
        let section = build_graph_section(
            graph_input(values.clone(), &[100, 101]),
            HnswParams::default(),
            10,
            &|| false,
        )
        .expect("builds")
        .expect("a graph");
        assert_eq!(section.kind, IndexSectionKind::VectorGraph);
        let graph = SegmentGraph::decode(&section.payload, 102).expect("decodes");
        assert_eq!(graph.graph.len(), 50);
        assert!(!graph.nodes.is_identity());
        let mut covered = (0..50)
            .flat_map(|node| graph.nodes.rows(node))
            .collect::<Vec<_>>();
        covered.sort_unstable();
        assert_eq!(covered, (0..100).collect::<Vec<_>>());

        // Too few distinct vectors for the floor: no graph.
        let none = build_graph_section(
            graph_input(values.clone(), &[100, 101]),
            HnswParams::default(),
            51,
            &|| false,
        )
        .expect("builds");
        assert!(none.is_none());

        // Distinct rows without nulls: an identity map.
        #[allow(clippy::cast_precision_loss)]
        let distinct = (0..64_u32)
            .flat_map(|row| [row as f32, (row % 5) as f32])
            .collect::<Vec<_>>();
        let section = build_graph_section(
            graph_input(distinct, &[]),
            HnswParams::default(),
            2,
            &|| false,
        )
        .expect("builds")
        .expect("a graph");
        let graph = SegmentGraph::decode(&section.payload, 64).expect("decodes");
        assert!(graph.nodes.is_identity());

        // Cancelled: a typed error, nothing built.
        let cancelled = build_graph_section(
            graph_input(values, &[100, 101]),
            HnswParams::default(),
            2,
            &|| true,
        );
        assert!(matches!(cancelled, Err(SegmentError::Cancelled)));
    }
}
