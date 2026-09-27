//! Compact binary layout for [`HnswGraph`]; see [`HnswGraph::to_bytes`].

use super::hnsw::MAX_M;
use super::{GraphError, HnswGraph, HnswParams};

const MAGIC: &[u8; 8] = b"LPHNSWG\0";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 40;
const CRC_LEN: usize = 4;
const NO_ENTRY: u32 = u32::MAX;
/// In-memory graphs up to this size load regardless of the input size.
const DECODE_FLOOR_BYTES: usize = 64 << 20;
/// Beyond [`DECODE_FLOOR_BYTES`], the loaded graph may take at most this
/// many times the serialized size. Fixed-size link slots make an edgeless
/// graph cost `4 * (2M + 1)` bytes per row in memory against 3 bytes on
/// disk, so without a bound a forged input could demand thousands of times
/// its size; real graphs sit well below 20x at any `M`.
const MAX_DECODE_AMPLIFICATION: usize = 64;

impl HnswGraph {
    /// Serializes the graph into a versioned, checksummed layout.
    ///
    /// All integers are little-endian.
    ///
    /// ```text
    /// offset  size  field
    /// 0       8     magic "LPHNSWG\0"
    /// 8       2     format version (1)
    /// 10      2     reserved, zero
    /// 12      4     M
    /// 16      4     ef_construction
    /// 20      8     seed
    /// 28      4     row count n
    /// 32      1     max level L
    /// 33      3     reserved, zero
    /// 36      4     entry point (u32::MAX when n = 0)
    /// 40      n     level of each row (u8)
    /// then, when n > 0, one CSR section per level l in 0..=L:
    ///         4     rows on this level (rows with level >= l, ascending id)
    ///         8     edge count e
    ///         2*r   degree of each of those rows (u16; offsets are their prefix sums)
    ///         4*e   concatenated neighbor row ids (u32)
    /// end-4   4     CRC32 (IEEE) of every preceding byte
    /// ```
    ///
    /// Loading verifies the checksum first, then every structural invariant:
    /// parameter ranges, level bounds, the entry point, per-level row counts,
    /// degree caps, neighbor ranges and levels, no self links, and no trailing
    /// bytes. Malformed input yields a [`GraphError`], never a panic, and
    /// input whose in-memory layout would exceed 64 times its size (beyond a
    /// 64 MiB floor) is refused with [`GraphError::TooLarge`].
    pub fn to_bytes(&self) -> Vec<u8> {
        let levels = self.levels();
        let mut bytes = Vec::with_capacity(HEADER_LEN + levels.len() * 4 + CRC_LEN);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        let params = self.params();
        bytes.extend_from_slice(&(params.m as u32).to_le_bytes());
        bytes.extend_from_slice(&(params.ef_construction as u32).to_le_bytes());
        bytes.extend_from_slice(&params.seed.to_le_bytes());
        bytes.extend_from_slice(&(levels.len() as u32).to_le_bytes());
        bytes.push(self.max_level() as u8);
        bytes.extend_from_slice(&[0; 3]);
        bytes.extend_from_slice(&self.entry_point().unwrap_or(NO_ENTRY).to_le_bytes());
        bytes.extend_from_slice(levels);
        if !levels.is_empty() {
            for level in 0..=self.max_level() {
                let rows: Vec<u32> = (0..levels.len() as u32)
                    .filter(|row| self.level(*row).is_some_and(|top| top >= level))
                    .collect();
                let edges: usize = rows
                    .iter()
                    .map(|row| self.neighbors(*row, level).len())
                    .sum();
                bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                bytes.extend_from_slice(&(edges as u64).to_le_bytes());
                for row in &rows {
                    let degree = self.neighbors(*row, level).len() as u16;
                    bytes.extend_from_slice(&degree.to_le_bytes());
                }
                for row in &rows {
                    for neighbor in self.neighbors(*row, level) {
                        bytes.extend_from_slice(&neighbor.to_le_bytes());
                    }
                }
            }
        }
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// Loads a graph written by [`Self::to_bytes`].
    ///
    /// The checksum is verified first, then every structural invariant, so
    /// malformed input yields a [`GraphError`], never a panic.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, GraphError> {
        if bytes.len() < MAGIC.len() {
            return Err(GraphError::Truncated);
        }
        if bytes.get(..MAGIC.len()) != Some(MAGIC.as_slice()) {
            return Err(GraphError::BadMagic);
        }
        if bytes.len() < HEADER_LEN + CRC_LEN {
            return Err(GraphError::Truncated);
        }
        let mut reader = Reader::new(bytes, MAGIC.len());
        let version = reader.u16()?;
        if version != VERSION {
            return Err(GraphError::UnsupportedVersion(version));
        }
        let (body, trailer) = bytes.split_at(bytes.len() - CRC_LEN);
        let stored = u32::from_le_bytes(array(trailer)?);
        let computed = crc32fast::hash(body);
        if stored != computed {
            return Err(GraphError::ChecksumMismatch { stored, computed });
        }
        let mut reader = Reader::new(body, reader.pos);
        decode_body(&mut reader)
    }
}

fn decode_body(reader: &mut Reader<'_>) -> Result<HnswGraph, GraphError> {
    if reader.u16()? != 0 {
        return Err(corrupt("reserved header bits are set"));
    }
    let m = reader.u32()? as usize;
    let ef_construction = reader.u32()? as usize;
    let seed = reader.u64()?;
    let rows = reader.u32()?;
    let max_level = reader.u8()?;
    if reader.take(3)? != [0, 0, 0] {
        return Err(corrupt("reserved header bytes are set"));
    }
    let entry = reader.u32()?;
    if !(2..=MAX_M).contains(&m) {
        return Err(corrupt(format!("m {m} is out of range")));
    }
    let params = HnswParams {
        m,
        ef_construction,
        seed,
    };
    params
        .validate()
        .map_err(|error| corrupt(error.to_string()))?;
    if rows == u32::MAX {
        return Err(corrupt("row count is out of range"));
    }

    let levels = reader.take(rows as usize)?;
    if rows == 0 {
        if entry != NO_ENTRY || max_level != 0 {
            return Err(corrupt("empty graph has an entry point or levels"));
        }
        reader.finish()?;
        return HnswGraph::new(params);
    }
    let observed_max = levels.iter().copied().max().unwrap_or(0);
    if observed_max != max_level {
        return Err(corrupt(format!(
            "max level {max_level} does not match the highest row level {observed_max}"
        )));
    }
    match levels.get(entry as usize) {
        Some(level) if *level == max_level => {}
        Some(_) => return Err(corrupt("entry point is not on the top level")),
        None => return Err(corrupt("entry point is out of range")),
    }

    check_decoded_size(&params, levels, reader.bytes.len())?;
    let mut graph = HnswGraph::with_capacity(params, rows as usize)?;
    for &level in levels {
        graph.push_node(level)?;
    }
    for level in 0..=usize::from(max_level) {
        decode_level(reader, &mut graph, levels, level)?;
    }
    reader.finish()?;
    graph.set_entry(Some(entry), max_level);
    Ok(graph)
}

/// Refuses inputs whose fixed-slot in-memory layout would dwarf them.
fn check_decoded_size(params: &HnswParams, levels: &[u8], input: usize) -> Result<(), GraphError> {
    let upper_slots: usize = levels.iter().map(|level| usize::from(*level)).sum();
    let words = levels
        .len()
        .checked_mul(params.max_links(0) + 1)
        .and_then(|layer0| {
            upper_slots
                .checked_mul(params.m + 1)
                .and_then(|upper| layer0.checked_add(upper))
        })
        .ok_or(GraphError::TooLarge)?;
    let bytes = words
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(levels.len() * 5))
        .ok_or(GraphError::TooLarge)?;
    let limit = DECODE_FLOOR_BYTES.max(input.saturating_mul(MAX_DECODE_AMPLIFICATION));
    if bytes > limit {
        return Err(GraphError::TooLarge);
    }
    Ok(())
}

fn decode_level(
    reader: &mut Reader<'_>,
    graph: &mut HnswGraph,
    levels: &[u8],
    level: usize,
) -> Result<(), GraphError> {
    let expected_rows = levels
        .iter()
        .filter(|row_level| usize::from(**row_level) >= level)
        .count();
    let level_rows = reader.u32()? as usize;
    if level_rows != expected_rows {
        return Err(corrupt(format!(
            "level {level} lists {level_rows} rows, expected {expected_rows}"
        )));
    }
    let edges = usize::try_from(reader.u64()?).map_err(|_| GraphError::Truncated)?;
    let degree_bytes = reader.take(level_rows.checked_mul(2).ok_or(GraphError::Truncated)?)?;
    let neighbor_bytes = reader.take(edges.checked_mul(4).ok_or(GraphError::Truncated)?)?;
    let cap = graph.params().max_links(level);
    let mut degrees = degree_bytes
        .chunks_exact(2)
        .map(|chunk| array(chunk).map(|raw| usize::from(u16::from_le_bytes(raw))));
    let mut neighbors = neighbor_bytes
        .chunks_exact(4)
        .map(|chunk| array(chunk).map(u32::from_le_bytes));
    let mut links = Vec::with_capacity(cap);
    let mut consumed = 0_usize;
    for (row, row_level) in levels.iter().enumerate() {
        if usize::from(*row_level) < level {
            continue;
        }
        let degree = degrees.next().ok_or(GraphError::Truncated)??;
        if degree > cap {
            return Err(corrupt(format!(
                "row {row} has {degree} links on level {level}, above the cap of {cap}"
            )));
        }
        consumed = consumed.checked_add(degree).ok_or(GraphError::TooLarge)?;
        if consumed > edges {
            return Err(corrupt(format!(
                "level {level} degrees exceed its {edges} edges"
            )));
        }
        links.clear();
        for _ in 0..degree {
            let neighbor = neighbors.next().ok_or(GraphError::Truncated)??;
            if neighbor as usize == row {
                return Err(corrupt(format!("row {row} links to itself")));
            }
            match levels.get(neighbor as usize) {
                Some(neighbor_level) if usize::from(*neighbor_level) >= level => {}
                Some(_) => {
                    return Err(corrupt(format!(
                        "row {row} links to row {neighbor}, which is not on level {level}"
                    )));
                }
                None => {
                    return Err(corrupt(format!(
                        "row {row} links to out-of-range row {neighbor}"
                    )));
                }
            }
            links.push(neighbor);
        }
        graph.set_links(row as u32, level, &links);
    }
    if consumed != edges {
        return Err(corrupt(format!(
            "level {level} declares {edges} edges but its degrees sum to {consumed}"
        )));
    }
    Ok(())
}

fn corrupt(message: impl Into<String>) -> GraphError {
    GraphError::Corrupt(message.into())
}

fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], GraphError> {
    bytes.try_into().map_err(|_| GraphError::Truncated)
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], pos: usize) -> Self {
        Self { bytes, pos }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], GraphError> {
        let end = self.pos.checked_add(len).ok_or(GraphError::Truncated)?;
        let slice = self.bytes.get(self.pos..end).ok_or(GraphError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, GraphError> {
        Ok(u8::from_le_bytes(array(self.take(1)?)?))
    }

    fn u16(&mut self) -> Result<u16, GraphError> {
        Ok(u16::from_le_bytes(array(self.take(2)?)?))
    }

    fn u32(&mut self) -> Result<u32, GraphError> {
        Ok(u32::from_le_bytes(array(self.take(4)?)?))
    }

    fn u64(&mut self) -> Result<u64, GraphError> {
        Ok(u64::from_le_bytes(array(self.take(8)?)?))
    }

    fn finish(&self) -> Result<(), GraphError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(corrupt(format!(
                "{} trailing bytes after the last section",
                self.bytes.len() - self.pos
            )))
        }
    }
}
