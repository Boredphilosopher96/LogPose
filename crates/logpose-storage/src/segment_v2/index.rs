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
//!   for null rows);
//! - `VectorGraph` for a vector field with at least `graph_min_rows`
//!   distinct non-null vectors.
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
    graph::{F32Metric, F32Vectors, HnswGraph, HnswParams},
    scalar::{InvertedIndex, KeyKind, ScalarIndexBuilder, ScalarKey, SortedIndex},
    sq8::{Sq8Params, Sq8Section, write_codes_section},
};
use logpose_types::{
    DistanceMetric,
    schema::{ElementType, FieldType},
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
    /// Write a `VectorGraph` when a vector field has at least this many
    /// distinct non-null vectors; smaller segments are searched exactly.
    /// Default 20,000.
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
                let Some(field) = schema
                    .vectors()
                    .iter()
                    .find(|field| field.id == vector.field)
                else {
                    continue;
                };
                sections.extend(vector_sections(vector, field.metric, policy)?);
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
    field: logpose_types::schema::FieldId,
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

/// The SQ8 and graph sections of one vector field.
fn vector_sections(
    vector: &VectorBuf,
    metric: DistanceMetric,
    policy: &IndexPolicy,
) -> Result<Vec<IndexSection>, SegmentError> {
    let dim = vector.dim as usize;
    let rows = vector.rows;
    let non_null = u64::from(rows).saturating_sub(vector.nulls.len());
    let mut sections = Vec::new();
    if non_null == 0 || dim == 0 {
        return Ok(sections);
    }
    let stride = dim * 4;
    let row_bytes = |row: u32| &vector.data[row as usize * stride..(row as usize + 1) * stride];
    let row_f32s = |row: u32| -> Vec<f32> {
        row_bytes(row)
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect()
    };
    let live_rows = || (0..rows).filter(|row| !vector.nulls.contains(*row));

    if non_null >= u64::from(policy.sq8_min_rows) {
        let mut training = Vec::with_capacity(non_null as usize * dim);
        for row in live_rows() {
            training.extend(row_f32s(row));
        }
        // A range too wide for f32 (or any other training failure) leaves the field
        // without codes; searches then use the f32 vectors.
        if let Ok(params) = Sq8Params::train(&training, dim) {
            drop(training);
            let mut codes = vec![0_u8; rows as usize * dim];
            let mut ok = true;
            for row in live_rows() {
                let start = row as usize * dim;
                if params
                    .encode_into(&row_f32s(row), &mut codes[start..start + dim])
                    .is_err()
                {
                    ok = false;
                    break;
                }
            }
            if ok {
                let mut payload = Vec::new();
                write_codes_section(&params, u64::from(rows), &codes, &mut payload)
                    .map_err(encode)?;
                sections.push(section(
                    IndexSectionKind::VectorSq8,
                    vector.field,
                    vector.dim,
                    u64::from(rows),
                    payload,
                ));
            }
        }
    }

    if non_null >= u64::from(policy.graph_min_rows) {
        // One node per distinct vector, in order of first appearance.
        let mut node_of: HashMap<&[u8], u32> = HashMap::new();
        let mut node_rows: Vec<Vec<u32>> = Vec::new();
        for row in live_rows() {
            let next = u32::try_from(node_rows.len()).map_err(encode)?;
            let node = *node_of.entry(row_bytes(row)).or_insert(next);
            if node == next {
                node_rows.push(Vec::new());
            }
            node_rows[node as usize].push(row);
        }
        drop(node_of);
        if node_rows.len() as u64 >= u64::from(policy.graph_min_rows) {
            let mut data = Vec::with_capacity(node_rows.len() * dim);
            for rows_of_node in &node_rows {
                data.extend(row_f32s(rows_of_node[0]));
            }
            let graph_metric = match metric {
                DistanceMetric::L2 => F32Metric::L2Squared,
                DistanceMetric::Cosine | DistanceMetric::Dot => F32Metric::NegativeDot,
            };
            let source = F32Vectors::new(dim, data, graph_metric).map_err(encode)?;
            let graph = HnswGraph::build_parallel(&source, policy.hnsw).map_err(encode)?;
            drop(source);
            let identity = node_rows.len() == rows as usize
                && node_rows
                    .iter()
                    .enumerate()
                    .all(|(node, rows)| rows.len() == 1 && rows[0] as usize == node);
            let payload = encode_graph_section(rows, &node_rows, identity, &graph)?;
            sections.push(section(
                IndexSectionKind::VectorGraph,
                vector.field,
                vector.dim,
                u64::from(rows),
                payload,
            ));
        }
    }
    Ok(sections)
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

/// Decode an index section's payload into the form readers attach to it.
/// Returns `Ok(None)` for kinds that have no decoded form.
///
/// # Errors
///
/// A description of the defect.
pub(crate) fn attach_decoded(
    kind: Option<SectionKind>,
    row_count: u32,
    bytes: &crate::cache::AlignedBytes,
) -> Result<(), String> {
    match kind {
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
