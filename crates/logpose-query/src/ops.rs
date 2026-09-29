//! Get, count, scroll, and order-by over the read interfaces.
//!
//! - **Get** walks units newest to oldest per key ([`ReadView::get`]).
//! - **Count** sums `|B AND NOT deleted|` over units; without a filter it is the view's live
//!   row counter.
//! - **Scroll by key** merges each unit's ascending key order restricted to its `B`. The first
//!   page of a scroll that has more pins its view under a snapshot token, which every later page
//!   reads, so every live row appears exactly once across pages even under concurrent writes.
//!   A page reads one row past its limit to learn whether more follow, so a scroll that fits in
//!   one page pins nothing and returns no cursor, and the last page of a scroll releases the
//!   snapshot the scroll pinned (not a token the caller passed).
//! - **Order by a field** yields each unit's rows in `(value, key)` order, from the field's
//!   sorted index when the unit has one and from its column otherwise, and merges them. Ties
//!   are broken by key ascending in both directions, and rows without a value come last.
//!
//! A [`Cursor`] travels to clients as opaque text ([`Cursor`]'s `Display` and `FromStr`): the
//! token, the order, a digest of the filter, and the last row's position, with a checksum.

use crate::explain::{Operator, OperatorStats, PlanNode};
use crate::{QueryError, Result, compile::CompiledFilter};
use logpose_storage::{
    CollectionReader, FetchPlan, Projection, ReadOptions, ReadView, RowData, SectionNeed,
    SnapshotToken, TOKEN_BYTES, UnitView, base64url_decode, base64url_encode,
    cache::PinSet,
    checksum,
    read::{Direction, ScalarKey, value_index_keys},
};
use logpose_types::{
    CollectionRef, LogPoseError, RowAddr, RowId, Snapshot, UnitId,
    filter::FilterExpr,
    record::PrimaryKey,
    schema::{CollectionSchema, FieldId, FieldRef, FieldType},
    value::Value,
};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fmt,
    ops::Bound,
    str::FromStr,
    sync::Arc,
    time::Instant,
};

/// The order of a scroll.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScrollOrder {
    /// Primary key ascending.
    Pk,
    /// A declared scalar field (not an array or JSON) in `direction`; ties by key ascending,
    /// rows without a value last.
    Field {
        /// The field name.
        field: String,
        /// The direction.
        direction: Direction,
    },
}

/// Where a scroll page ends: the key of its last row.
#[derive(Clone, Debug, PartialEq)]
pub enum CursorKey {
    /// After this primary key.
    Pk(PrimaryKey),
    /// After this `(value, key)` position; `None` is the null tail.
    Field(Option<Value>, PrimaryKey),
}

/// A scroll position: the pinned snapshot, the order, the filter, and the last row returned.
#[derive(Clone, Debug, PartialEq)]
pub struct Cursor {
    /// The snapshot every page reads.
    pub token: SnapshotToken,
    /// Whether the scroll pinned `token` itself (rather than reading a token the caller
    /// passed), so its last page releases it.
    pub owned: bool,
    /// The scroll's order.
    pub order: ScrollOrder,
    /// [`filter_digest`] of the scroll's filter; a page with another filter is refused.
    pub filter: u32,
    /// The last row returned.
    pub after: CursorKey,
}

/// One scroll request.
#[derive(Clone, Debug, PartialEq)]
pub struct ScrollRequest {
    /// Only rows matching this filter.
    pub filter: Option<FilterExpr>,
    /// The order.
    pub order: ScrollOrder,
    /// Rows per page (at least 1).
    pub limit: u32,
    /// What to read of each row.
    pub projection: Projection,
    /// `None` starts a scroll (pinning a snapshot when rows are left after the page); `Some`
    /// continues one, with the same filter and order.
    pub cursor: Option<Cursor>,
    /// The pinned snapshot a new scroll reads instead of the current state. A cursor carries
    /// its own, so this must be `None` with one.
    pub token: Option<SnapshotToken>,
}

/// One page of a scroll.
#[derive(Clone, Debug, PartialEq)]
pub struct ScrollPage {
    /// The rows, in order.
    pub rows: Vec<RowData>,
    /// Where the next page starts; `None` after the last page.
    pub next: Option<Cursor>,
    /// The state the page read.
    pub snapshot: Snapshot,
}

/// The live rows of `pks` (each `None` when absent), read as `projection` asks.
///
/// # Errors
///
/// As [`CollectionReader::read_view`] and [`ReadView::get`].
pub async fn get(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    pks: &[PrimaryKey],
    projection: Projection,
    options: ReadOptions,
) -> Result<Vec<Option<RowData>>> {
    let view = reader.read_view(collection, options).await?;
    Ok(view.get(pks, projection).await?)
}

/// The number of live rows matching `filter` (every live row without one).
///
/// # Errors
///
/// As [`CollectionReader::read_view`] and [`count_view`].
pub async fn count(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    filter: Option<&FilterExpr>,
    options: ReadOptions,
) -> Result<u64> {
    let view = reader.read_view(collection, options).await?;
    count_view(&view, filter).await
}

/// [`count`] over an open view.
///
/// # Errors
///
/// An invalid filter, I/O, or typed corruption.
pub async fn count_view(view: &ReadView, filter: Option<&FilterExpr>) -> Result<u64> {
    let Some(filter) = filter else {
        return Ok(view.counters().live_rows());
    };
    let rows = resolve_view(view, filter).await?;
    Ok(rows.iter().map(|(_, rows)| rows.len()).sum())
}

/// The live rows of `view` matching `filter`, per unit (units without a match are omitted).
///
/// # Errors
///
/// An invalid filter, I/O, or typed corruption.
pub async fn resolve_view(
    view: &ReadView,
    filter: &FilterExpr,
) -> Result<Vec<(UnitId, RoaringBitmap)>> {
    let filter = Arc::new(CompiledFilter::compile(view.schema(), filter)?);
    let mut plan = FetchPlan::default();
    let mut candidates = Vec::new();
    for unit in view.units() {
        if !filter.may_match(&unit) {
            continue;
        }
        candidates.push(unit.id());
        for need in filter.needs(&unit) {
            plan.push(unit.id(), need);
        }
    }
    let (pins, _) = view.fetch(&plan).await?;
    let rows = view
        .run(move |view| {
            view.units()
                .into_par_iter()
                .filter(|unit| candidates.contains(&unit.id()))
                .map(|unit| filter.evaluate(&unit, &pins).map(|rows| (unit.id(), rows)))
                .collect::<Vec<logpose_types::Result<_>>>()
        })
        .await?
        .into_iter()
        .collect::<logpose_types::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter(|(_, rows)| !rows.is_empty())
        .collect())
}

/// A digest of a scroll's filter, which its cursor carries: CRC-32C of the filter's canonical
/// serialization, 0 without a filter.
#[must_use]
pub fn filter_digest(filter: Option<&FilterExpr>) -> u32 {
    filter.map_or(0, |filter| {
        checksum(&serde_json::to_vec(filter).unwrap_or_default())
    })
}

fn invalid_cursor(message: impl Into<String>) -> QueryError {
    QueryError::Storage(LogPoseError::invalid_field("cursor", message))
}

/// One page of a scroll. A request without a cursor reads the current state (or the snapshot
/// `request.token` pins) and, when rows are left after its page, pins the view it read; the
/// page's cursor carries that token, and later pages read exactly that snapshot. A scroll that
/// fits in one page pins nothing.
///
/// # Errors
///
/// As [`CollectionReader::read_view`] (a cursor whose token expired is `SnapshotExpired`),
/// `InvalidArgument` for a cursor used with another order or filter or together with a token,
/// an invalid filter or order field, I/O, or typed corruption.
pub async fn scroll(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    request: ScrollRequest,
) -> Result<ScrollPage> {
    let view = reader
        .read_view(collection, scroll_options(&request)?)
        .await?;
    scroll_page(&view, request).await
}

/// The view options of a scroll page: its cursor's token, or the request's for a first page.
///
/// # Errors
///
/// `InvalidArgument` for a cursor used with another order or filter, or together with a
/// token.
pub fn scroll_options(request: &ScrollRequest) -> Result<ReadOptions> {
    let Some(cursor) = &request.cursor else {
        return Ok(ReadOptions {
            token: request.token.clone(),
            ..ReadOptions::default()
        });
    };
    if request.token.is_some() {
        return Err(QueryError::Storage(LogPoseError::invalid_field(
            "snapshot_token",
            "a cursor carries its snapshot; a later page takes no snapshot token",
        )));
    }
    if cursor.order != request.order {
        return Err(invalid_cursor(
            "the cursor belongs to a scroll with another order",
        ));
    }
    if cursor.filter != filter_digest(request.filter.as_ref()) {
        return Err(invalid_cursor(
            "the cursor belongs to a scroll with another filter",
        ));
    }
    Ok(ReadOptions {
        token: Some(cursor.token.clone()),
        ..ReadOptions::default()
    })
}

/// One page of a scroll over `view`, which [`scroll_options`] of the same request opened.
///
/// # Errors
///
/// As [`scroll`], apart from opening the view.
pub async fn scroll_page(view: &ReadView, request: ScrollRequest) -> Result<ScrollPage> {
    let digest = filter_digest(request.filter.as_ref());
    let after = request.cursor.as_ref().map(|cursor| cursor.after.clone());
    // A first page read through no token pins the view itself (when a later page follows).
    let owned = request
        .cursor
        .as_ref()
        .map_or(view.token().is_none(), |cursor| cursor.owned);
    let limit = request.limit.max(1) as usize;
    let (mut entries, _) = scroll_entries(
        view,
        request.filter.as_ref(),
        &request.order,
        limit + 1,
        after.as_ref(),
    )
    .await?;
    let more = entries.len() > limit;
    entries.truncate(limit);
    let addrs = entries.iter().map(|entry| entry.addr).collect::<Vec<_>>();
    let rows = view.rows(&addrs, request.projection).await?;
    let next = match entries.last() {
        Some(last) if more => {
            // Pinned only now that a later page will read it: a scroll that fits in one page
            // holds no token.
            let pinned = view.pinned()?;
            let token = pinned.token().cloned().ok_or_else(|| {
                QueryError::Storage(LogPoseError::internal("a pinned view has no token"))
            })?;
            Some(Cursor {
                token,
                owned,
                order: request.order.clone(),
                filter: digest,
                after: cursor_key(last, &request.order),
            })
        }
        _ => {
            // The last page: nothing reads the scroll's own snapshot after it.
            if request.cursor.is_some() && owned {
                view.release();
            }
            None
        }
    };
    Ok(ScrollPage {
        rows,
        next,
        snapshot: view.snapshot(),
    })
}

/// The next `limit` rows of `view` after `after` in `order`, and the key of the last one.
///
/// # Errors
///
/// An invalid filter or order field, I/O, or typed corruption.
pub async fn scroll_view(
    view: &ReadView,
    filter: Option<&FilterExpr>,
    order: &ScrollOrder,
    limit: u32,
    projection: Projection,
    after: Option<&CursorKey>,
) -> Result<(Vec<RowData>, Option<CursorKey>)> {
    let (rows, last, _) =
        scroll_view_explained(view, filter, order, limit, projection, after).await?;
    Ok((rows, last))
}

/// [`scroll_view`], with the plan it ran: `Project` over `Merge` over one `OrderedScan` per
/// unit, each over the unit's filter bitmap.
///
/// # Errors
///
/// As [`scroll_view`].
pub async fn scroll_view_explained(
    view: &ReadView,
    filter: Option<&FilterExpr>,
    order: &ScrollOrder,
    limit: u32,
    projection: Projection,
    after: Option<&CursorKey>,
) -> Result<(Vec<RowData>, Option<CursorKey>, PlanNode)> {
    let limit = limit.max(1) as usize;
    let (entries, merge) = scroll_entries(view, filter, order, limit, after).await?;
    let addrs = entries.iter().map(|entry| entry.addr).collect::<Vec<_>>();
    let rows = view.rows(&addrs, projection).await?;
    let last = entries.last().map(|entry| cursor_key(entry, order));
    let plan = PlanNode::new(Operator::Project, format!("limit={limit}"))
        .with_stats(
            OperatorStats::rows(limit as u64),
            OperatorStats::rows(rows.len() as u64),
        )
        .over(merge);
    Ok((rows, last, plan))
}

/// The cursor key of `entry` in `order`.
fn cursor_key(entry: &Entry, order: &ScrollOrder) -> CursorKey {
    match order {
        ScrollOrder::Pk => CursorKey::Pk(entry.pk.clone()),
        ScrollOrder::Field { .. } => {
            CursorKey::Field(entry.key.as_ref().map(key_value), entry.pk.clone())
        }
    }
}

/// The next `limit` entries of `view` after `after` in `order`, and the plan's merge node.
///
/// Each unit yields at most `limit` entries in `(value, key)` order after the cursor (see
/// [`unit_by_key`] and [`unit_by_field`]); a heap merge of the units' sorted lists takes the
/// first `limit`, which are the first `limit` of the whole view because the order is total.
async fn scroll_entries(
    view: &ReadView,
    filter: Option<&FilterExpr>,
    order: &ScrollOrder,
    limit: usize,
    after: Option<&CursorKey>,
) -> Result<(Vec<Entry>, PlanNode)> {
    let filter = filter
        .map(|filter| CompiledFilter::compile(view.schema(), filter))
        .transpose()?
        .map(Arc::new);
    let ordered_field = match order {
        ScrollOrder::Pk => None,
        ScrollOrder::Field { field, direction } => Some((
            order_field(view.schema(), field, "order_by[0].field")?,
            *direction,
        )),
    };
    let mut plan = FetchPlan::default();
    let mut units = Vec::new();
    let mut pruned = 0;
    for unit in view.units() {
        if filter
            .as_ref()
            .is_some_and(|filter| !filter.may_match(&unit))
        {
            pruned += 1;
            continue;
        }
        units.push(unit.id());
        plan.push(unit.id(), SectionNeed::Pk);
        if let Some(filter) = &filter {
            for need in filter.needs(&unit) {
                plan.push(unit.id(), need);
            }
        }
        if let Some((field, _)) = &ordered_field {
            let need = if unit.has_scalar_index(*field, true) {
                SectionNeed::ScalarIndex(*field)
            } else {
                SectionNeed::Column(*field)
            };
            plan.push(unit.id(), need);
        }
    }
    let (pins, _) = view.fetch(&plan).await?;
    let after_entry = match (after, &ordered_field) {
        (None, _) => None,
        (Some(CursorKey::Pk(pk)), None) => Some(Entry {
            key: None,
            pk: pk.clone(),
            addr: RowAddr {
                unit: UnitId(0),
                row: 0,
            },
        }),
        (Some(CursorKey::Field(value, pk)), Some(_)) => Some(Entry {
            key: value
                .as_ref()
                .and_then(|value| value_index_keys(value).into_iter().next()),
            pk: pk.clone(),
            addr: RowAddr {
                unit: UnitId(0),
                row: 0,
            },
        }),
        _ => {
            return Err(invalid_cursor("the cursor does not fit the scroll's order"));
        }
    };
    let described = filter.clone();
    let scans = view
        .run(move |view| {
            view.units()
                .into_par_iter()
                .filter(|unit| units.contains(&unit.id()))
                .map(|unit| -> logpose_types::Result<(Vec<Entry>, PlanNode)> {
                    let started = Instant::now();
                    let allowed = match &filter {
                        Some(filter) => filter.evaluate(&unit, &pins)?,
                        None => unit.live(),
                    };
                    let probe_micros = started.elapsed().as_secs_f64() * 1e6;
                    let scan_started = Instant::now();
                    let (entries, how) = match ordered_field {
                        None => (
                            unit_by_key(&unit, &pins, &allowed, after_entry.as_ref(), limit)?,
                            "key order".to_owned(),
                        ),
                        Some((field, direction)) => unit_by_field(
                            &unit,
                            &pins,
                            &allowed,
                            field,
                            direction,
                            after_entry.as_ref(),
                            limit,
                        )?,
                    };
                    let node = ordered_node(
                        &unit,
                        described.as_deref(),
                        &allowed,
                        &how,
                        limit,
                        entries.len(),
                        probe_micros,
                        scan_started.elapsed().as_secs_f64() * 1e6,
                    );
                    Ok((entries, node))
                })
                .collect::<Vec<_>>()
        })
        .await?;
    let direction = ordered_field.map(|(_, direction)| direction);
    let mut lists = Vec::with_capacity(scans.len());
    let mut nodes = Vec::with_capacity(scans.len());
    for scan in scans {
        let (entries, node) = scan?;
        lists.push(entries);
        nodes.push(node);
    }
    let merge_started = Instant::now();
    let merged = merge_entries(lists, direction, limit);
    let mut merge = PlanNode::new(
        Operator::Merge,
        format!("units={} pruned={pruned} limit={limit}", nodes.len()),
    )
    .with_stats(
        OperatorStats::rows(limit as u64),
        OperatorStats {
            rows: merged.len() as u64,
            micros: merge_started.elapsed().as_secs_f64() * 1e6,
            ..OperatorStats::default()
        },
    );
    merge.children = nodes;
    Ok((merged, merge))
}

/// A unit's `OrderedScan` over its filter nodes.
#[allow(clippy::too_many_arguments)]
fn ordered_node(
    unit: &UnitView<'_>,
    filter: Option<&CompiledFilter>,
    allowed: &RoaringBitmap,
    how: &str,
    limit: usize,
    produced: usize,
    probe_micros: f64,
    scan_micros: f64,
) -> PlanNode {
    let live = u64::from(unit.live_count());
    let matched = allowed.len();
    let kind = if unit.is_memtable() {
        "memtable"
    } else {
        "segment"
    };
    let mut node = PlanNode::new(
        Operator::SegmentSource,
        format!("unit={:08x} {kind} rows={}", unit.id().0, unit.row_count()),
    )
    .with_stats(OperatorStats::rows(live), OperatorStats::rows(live));
    if let Some(filter) = filter {
        node = PlanNode::new(Operator::BitmapProbe, filter.describe(unit))
            .with_stats(
                OperatorStats::rows(matched),
                OperatorStats {
                    rows: matched,
                    micros: probe_micros,
                    ..OperatorStats::default()
                },
            )
            .over(node);
    }
    let deleted = u64::from(unit.row_count()).saturating_sub(live);
    node = PlanNode::new(Operator::MaskDeletes, format!("deleted={deleted}"))
        .with_stats(OperatorStats::rows(matched), OperatorStats::rows(matched))
        .over(node);
    PlanNode::new(
        Operator::OrderedScan,
        format!("unit={:08x} {how} limit={limit}", unit.id().0),
    )
    .with_stats(
        OperatorStats::rows(matched.min(limit as u64)),
        OperatorStats {
            rows: produced as u64,
            micros: scan_micros,
            ..OperatorStats::default()
        },
    )
    .over(node)
}

/// An entry with the scroll's direction, ordered as the scroll orders rows (then by address,
/// which only equal keys from different units could need).
struct Ordered(Entry, Option<Direction>);

impl PartialEq for Ordered {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Ordered {}

impl PartialOrd for Ordered {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ordered {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_entries(&self.0, &other.0, self.1)
            .then(self.0.addr.unit.cmp(&other.0.addr.unit))
            .then(self.0.addr.row.cmp(&other.0.addr.row))
    }
}

/// A k-way heap merge of sorted entry lists to their first `limit`.
fn merge_entries(lists: Vec<Vec<Entry>>, direction: Option<Direction>, limit: usize) -> Vec<Entry> {
    let mut iters = lists
        .into_iter()
        .map(std::iter::IntoIterator::into_iter)
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(iters.len());
    for (index, iter) in iters.iter_mut().enumerate() {
        if let Some(first) = iter.next() {
            heap.push(Reverse((Ordered(first, direction), index)));
        }
    }
    let mut out = Vec::with_capacity(limit.min(4_096));
    while out.len() < limit {
        let Some(Reverse((Ordered(entry, _), index))) = heap.pop() else {
            break;
        };
        if let Some(next) = iters[index].next() {
            heap.push(Reverse((Ordered(next, direction), index)));
        }
        out.push(entry);
    }
    out
}

/// The field a scroll or query orders by: a declared scalar field that is not an array or JSON.
/// `path` names the order in the request for the error.
///
/// # Errors
///
/// `InvalidArgument` at `path` for any other name.
pub fn order_field(schema: &CollectionSchema, name: &str, path: &str) -> Result<FieldId> {
    match schema.field(name) {
        Some(FieldRef::Scalar(field))
            if !matches!(field.field_type, FieldType::Array(_) | FieldType::Json) =>
        {
            Ok(field.id)
        }
        Some(FieldRef::PrimaryKey(_)) => Err(QueryError::Storage(LogPoseError::invalid_field(
            path,
            format!(
                "'{name}' is the primary key; primary key order is the default, so leave order_by out"
            ),
        ))),
        _ => Err(QueryError::Storage(LogPoseError::invalid_field(
            path,
            format!("'{name}' is not a scalar field that can be ordered (arrays and json cannot)"),
        ))),
    }
}

/// The order key of a value, as scrolls compare them: `None` for null.
#[must_use]
pub fn order_key(value: Option<&Value>) -> Option<ScalarKey> {
    value.and_then(|value| value_index_keys(value).into_iter().next())
}

/// Compare two rows by `(key, pk)` in `direction`, rows without a key last and ties by key
/// ascending: the order of [`ScrollOrder::Field`].
#[must_use]
pub fn compare_ordered(
    left: (Option<&ScalarKey>, &PrimaryKey),
    right: (Option<&ScalarKey>, &PrimaryKey),
    direction: Direction,
) -> Ordering {
    match (left.0, right.0) {
        (Some(left_key), Some(right_key)) => {
            let order = left_key.cmp(right_key);
            let order = match direction {
                Direction::Ascending => order,
                Direction::Descending => order.reverse(),
            };
            order.then_with(|| left.1.cmp(right.1))
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.1.cmp(right.1),
    }
}

/// Why a cursor's text does not parse.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidCursor;

impl fmt::Display for InvalidCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("not a scroll cursor")
    }
}

impl std::error::Error for InvalidCursor {}

/// Version byte of the cursor encoding.
const CURSOR_VERSION: u8 = 2;

impl fmt::Display for Cursor {
    /// Unpadded base64url of: a version byte, the token's bytes, whether the scroll owns the
    /// token, the filter digest, the order, the last row's position, and a CRC-32C of
    /// everything before it.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = vec![CURSOR_VERSION];
        bytes.extend_from_slice(&self.token.to_bytes());
        bytes.push(u8::from(self.owned));
        bytes.extend_from_slice(&self.filter.to_le_bytes());
        match &self.order {
            ScrollOrder::Pk => bytes.push(0),
            ScrollOrder::Field { field, direction } => {
                bytes.push(match direction {
                    Direction::Ascending => 1,
                    Direction::Descending => 2,
                });
                put_bytes(&mut bytes, field.as_bytes());
            }
        }
        match &self.after {
            CursorKey::Pk(pk) => {
                bytes.push(0);
                put_pk(&mut bytes, pk);
            }
            CursorKey::Field(None, pk) => {
                bytes.push(1);
                put_pk(&mut bytes, pk);
            }
            CursorKey::Field(Some(value), pk) => {
                bytes.push(2);
                put_value(&mut bytes, value);
                put_pk(&mut bytes, pk);
            }
        }
        let crc = checksum(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        formatter.write_str(&base64url_encode(&bytes))
    }
}

impl FromStr for Cursor {
    type Err = InvalidCursor;

    fn from_str(text: &str) -> std::result::Result<Self, Self::Err> {
        let bytes = base64url_decode(text).ok_or(InvalidCursor)?;
        let (body, crc) = bytes.split_last_chunk::<4>().ok_or(InvalidCursor)?;
        if checksum(body) != u32::from_le_bytes(*crc) {
            return Err(InvalidCursor);
        }
        let mut reader = Reader { bytes: body };
        if reader.byte()? != CURSOR_VERSION {
            return Err(InvalidCursor);
        }
        let token = SnapshotToken::from_bytes(reader.take(TOKEN_BYTES)?).ok_or(InvalidCursor)?;
        let owned = match reader.byte()? {
            0 => false,
            1 => true,
            _ => return Err(InvalidCursor),
        };
        let filter = u32::from_le_bytes(reader.array()?);
        let order = match reader.byte()? {
            0 => ScrollOrder::Pk,
            tag @ (1 | 2) => ScrollOrder::Field {
                field: reader.string()?,
                direction: if tag == 1 {
                    Direction::Ascending
                } else {
                    Direction::Descending
                },
            },
            _ => return Err(InvalidCursor),
        };
        let after = match reader.byte()? {
            0 => CursorKey::Pk(reader.pk()?),
            1 => CursorKey::Field(None, reader.pk()?),
            2 => {
                let value = reader.value()?;
                CursorKey::Field(Some(value), reader.pk()?)
            }
            _ => return Err(InvalidCursor),
        };
        if !reader.bytes.is_empty() {
            return Err(InvalidCursor);
        }
        Ok(Self {
            token,
            owned,
            order,
            filter,
            after,
        })
    }
}

fn put_bytes(bytes: &mut Vec<u8>, data: &[u8]) {
    let len = u32::try_from(data.len()).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(data);
}

fn put_pk(bytes: &mut Vec<u8>, pk: &PrimaryKey) {
    match pk {
        PrimaryKey::Int64(value) => {
            bytes.push(0);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        PrimaryKey::String(value) => {
            bytes.push(1);
            put_bytes(bytes, value.as_bytes());
        }
    }
}

fn put_value(bytes: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Bool(value) => {
            bytes.push(0);
            bytes.push(u8::from(*value));
        }
        Value::Float64(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Value::String(value) => {
            bytes.push(3);
            put_bytes(bytes, value.as_bytes());
        }
        Value::Timestamp(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.as_micros().to_le_bytes());
        }
        Value::Int64(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        // Order keys are never null, arrays, or JSON; encode them as the null tail.
        Value::Null | Value::Array(_) | Value::Json(_) => bytes.push(4),
    }
}

/// Reads a cursor's fields in order.
struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> std::result::Result<&'a [u8], InvalidCursor> {
        if self.bytes.len() < len {
            return Err(InvalidCursor);
        }
        let (head, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> std::result::Result<[u8; N], InvalidCursor> {
        self.take(N)?.try_into().map_err(|_| InvalidCursor)
    }

    fn byte(&mut self) -> std::result::Result<u8, InvalidCursor> {
        Ok(self.array::<1>()?[0])
    }

    fn string(&mut self) -> std::result::Result<String, InvalidCursor> {
        let len = u32::from_le_bytes(self.array()?) as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| InvalidCursor)
    }

    fn pk(&mut self) -> std::result::Result<PrimaryKey, InvalidCursor> {
        match self.byte()? {
            0 => Ok(PrimaryKey::Int64(i64::from_le_bytes(self.array()?))),
            1 => Ok(PrimaryKey::String(self.string()?)),
            _ => Err(InvalidCursor),
        }
    }

    fn value(&mut self) -> std::result::Result<Value, InvalidCursor> {
        match self.byte()? {
            0 => Ok(Value::Bool(self.byte()? != 0)),
            1 => Ok(Value::Int64(i64::from_le_bytes(self.array()?))),
            2 => {
                let value = f64::from_bits(u64::from_le_bytes(self.array()?));
                Value::float64(value).map_err(|_| InvalidCursor)
            }
            3 => Ok(Value::String(self.string()?)),
            4 => Ok(Value::Null),
            _ => Err(InvalidCursor),
        }
    }
}

/// One row in a scroll's order.
#[derive(Clone, Debug)]
struct Entry {
    key: Option<ScalarKey>,
    pk: PrimaryKey,
    addr: RowAddr,
}

fn compare_entries(left: &Entry, right: &Entry, direction: Option<Direction>) -> Ordering {
    let Some(direction) = direction else {
        return left.pk.cmp(&right.pk);
    };
    compare_ordered(
        (left.key.as_ref(), &left.pk),
        (right.key.as_ref(), &right.pk),
        direction,
    )
}

/// A scalar key back as a value (timestamps come back as integers).
fn key_value(key: &ScalarKey) -> Value {
    match key {
        ScalarKey::Bool(value) => Value::Bool(*value),
        ScalarKey::Int(value) => Value::Int64(*value),
        ScalarKey::Float(value) => Value::Float64(value.get()),
        ScalarKey::Str(value) => Value::String(value.to_string()),
    }
}

/// Up to `limit` rows of `unit` in key order after `after`: a binary search in the unit's
/// key order, then rows until `limit` of them are allowed.
fn unit_by_key(
    unit: &UnitView<'_>,
    pins: &PinSet,
    allowed: &RoaringBitmap,
    after: Option<&Entry>,
    limit: usize,
) -> logpose_types::Result<Vec<Entry>> {
    let pks = unit.pks(pins)?;
    Ok(pks
        .ascending_after(after.map(|entry| &entry.pk))
        .filter(|(_, row)| allowed.contains(*row))
        .take(limit)
        .map(|(pk, row)| Entry {
            key: None,
            pk,
            addr: RowAddr {
                unit: unit.id(),
                row,
            },
        })
        .collect())
}

/// Up to `limit` rows of `unit` in `(value, key)` order after `after`, and how they were read.
///
/// With a sorted index, the scan seeks to the cursor's value and continues from there, one
/// group of equal values at a time; the cursor's own group (and the null tail) seeks by key
/// ([`group_after`]), so a page never re-reads the rows of a large tie group that earlier pages
/// returned. Without one, a bounded heap of `limit` entries over the column keeps the page's
/// rows: one pass over the allowed rows, no sort of all of them.
fn unit_by_field(
    unit: &UnitView<'_>,
    pins: &PinSet,
    allowed: &RoaringBitmap,
    field: FieldId,
    direction: Direction,
    after: Option<&Entry>,
    limit: usize,
) -> logpose_types::Result<(Vec<Entry>, String)> {
    let pks = unit.pks(pins)?;
    let entry = |key: Option<ScalarKey>, pk: PrimaryKey, row: RowId| Entry {
        key,
        pk,
        addr: RowAddr {
            unit: unit.id(),
            row,
        },
    };
    let pk_of = |row: RowId| -> logpose_types::Result<PrimaryKey> {
        pks.pk_at(row).ok_or_else(|| {
            LogPoseError::internal(format!("row {row} of unit {} has no key", unit.id()))
        })
    };
    let Some(index) = unit.scalar_index(field, true, pins)? else {
        // No sorted index: a bounded heap over the column.
        let column = unit.column(field, pins)?;
        let mut heap: BinaryHeap<Ordered> = BinaryHeap::with_capacity(limit.min(4_096) + 1);
        for row in allowed {
            let key = column
                .value(row)?
                .and_then(|value| value_index_keys(&value).into_iter().next());
            // Compare by value first: a row whose value is past the heap's worst never needs
            // its key read.
            if heap.len() >= limit
                && heap.peek().is_some_and(|worst| {
                    compare_ordered(
                        (key.as_ref(), &worst.0.pk),
                        (worst.0.key.as_ref(), &worst.0.pk),
                        direction,
                    ) == Ordering::Greater
                })
            {
                continue;
            }
            let candidate = entry(key, pk_of(row)?, row);
            if after.is_some_and(|after| {
                compare_entries(&candidate, after, Some(direction)) != Ordering::Greater
            }) {
                continue;
            }
            let candidate = Ordered(candidate, Some(direction));
            if heap.len() < limit {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                heap.pop();
                heap.push(candidate);
            }
        }
        let entries = heap
            .into_sorted_vec()
            .into_iter()
            .map(|ordered| ordered.0)
            .collect();
        return Ok((entries, "column heap".to_owned()));
    };
    let mut out: Vec<Entry> = Vec::new();
    let in_null_tail = after.is_some_and(|after| after.key.is_none());
    if !in_null_tail {
        // The cursor's own group first, seeking by key within it.
        let mut start = Bound::Unbounded;
        if let Some(after) = after
            && let Some(key) = &after.key
        {
            let group = index.equals(key) & allowed;
            for (pk, row) in group_after(&pks, &group, Some(&after.pk), limit, unit.row_count()) {
                out.push(entry(Some(key.clone()), pk, row));
            }
            start = Bound::Excluded(key);
        }
        // Then later groups in value order, each ordered by key; a group larger than what
        // the page still needs keeps only its smallest keys.
        if out.len() < limit
            && let Some(ordered) = index.ordered(start, direction)
        {
            let mut group = RoaringBitmap::new();
            let mut group_key: Option<ScalarKey> = None;
            let flush = |group: &mut RoaringBitmap,
                         key: Option<ScalarKey>,
                         out: &mut Vec<Entry>|
             -> logpose_types::Result<()> {
                let wanted = limit - out.len();
                for (pk, row) in group_after(&pks, group, None, wanted, unit.row_count()) {
                    out.push(entry(key.clone(), pk, row));
                }
                group.clear();
                Ok(())
            };
            for (key, row) in ordered {
                if group_key.as_ref() != Some(&key) {
                    if !group.is_empty() {
                        flush(&mut group, group_key.take(), &mut out)?;
                        if out.len() >= limit {
                            break;
                        }
                    }
                    group_key = Some(key);
                }
                if allowed.contains(row) {
                    group.insert(row);
                }
            }
            if out.len() < limit && !group.is_empty() {
                flush(&mut group, group_key.take(), &mut out)?;
            }
        }
    }
    if out.len() < limit {
        // Rows without a value come last, by key.
        let nulls = index.nulls() & allowed;
        let after_pk = after.filter(|_| in_null_tail).map(|after| &after.pk);
        for (pk, row) in group_after(&pks, &nulls, after_pk, limit - out.len(), unit.row_count()) {
            out.push(entry(None, pk, row));
        }
    }
    out.truncate(limit);
    Ok((out, "sorted index".to_owned()))
}

/// The first `limit` rows of `group` by key, strictly after `after`. Two ways, whichever
/// reads fewer rows: walk the unit's key order from `after` testing membership (about
/// `limit * rows / |group|` rows for a large group), or collect the group's keys into a
/// bounded heap (`|group|` rows).
fn group_after(
    pks: &logpose_storage::read::PkRef<'_>,
    group: &RoaringBitmap,
    after: Option<&PrimaryKey>,
    limit: usize,
    rows: u32,
) -> Vec<(PrimaryKey, RowId)> {
    let size = group.len();
    if size == 0 || limit == 0 {
        return Vec::new();
    }
    let walk = (limit as u64).saturating_mul(u64::from(rows)) / size;
    if walk < size {
        return pks
            .ascending_after(after)
            .filter(|(_, row)| group.contains(*row))
            .take(limit)
            .collect();
    }
    let mut heap: BinaryHeap<(PrimaryKey, RowId)> = BinaryHeap::with_capacity(limit.min(4_096) + 1);
    for row in group {
        let Some(pk) = pks.pk_at(row) else {
            continue;
        };
        if after.is_some_and(|after| &pk <= after) {
            continue;
        }
        if heap.len() < limit {
            heap.push((pk, row));
        } else if heap.peek().is_some_and(|worst| pk < worst.0) {
            heap.pop();
            heap.push((pk, row));
        }
    }
    heap.into_sorted_vec()
}
