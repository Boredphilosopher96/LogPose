//! Get, count, scroll, and order-by over the read interfaces.
//!
//! - **Get** walks units newest to oldest per key ([`ReadView::get`]).
//! - **Count** sums `|B AND NOT deleted|` over units; without a filter it is the view's live
//!   row counter.
//! - **Scroll by key** merges each unit's ascending key order restricted to its `B`. The first
//!   page of a scroll that has more pins its view under a snapshot token, which every later page
//!   reads, so every live row appears exactly once across pages even under concurrent writes.
//!   A scroll that fits in one page pins nothing.
//! - **Order by a field** yields each unit's rows in `(value, key)` order, from the field's
//!   sorted index when the unit has one and from its column otherwise, and merges them. Ties
//!   are broken by key ascending in both directions, and rows without a value come last.

use crate::{QueryError, Result, compile::CompiledFilter};
use logpose_storage::{
    CollectionReader, FetchPlan, Projection, ReadOptions, ReadView, RowData, SectionNeed,
    SnapshotToken, UnitView,
    cache::PinSet,
    read::{Direction, ScalarKey, value_index_keys},
};
use logpose_types::{
    CollectionRef, LogPoseError, RowAddr, RowId, UnitId,
    filter::FilterExpr,
    record::PrimaryKey,
    schema::{FieldId, FieldRef, FieldType},
    value::Value,
};
use rayon::prelude::*;
use roaring::RoaringBitmap;
use std::{cmp::Ordering, ops::Bound, sync::Arc};

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

/// A scroll position: the pinned snapshot, the order, and the last row returned.
#[derive(Clone, Debug, PartialEq)]
pub struct Cursor {
    /// The snapshot every page reads.
    pub token: SnapshotToken,
    /// The scroll's order.
    pub order: ScrollOrder,
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
    /// continues one.
    pub cursor: Option<Cursor>,
}

/// One page of a scroll.
#[derive(Clone, Debug, PartialEq)]
pub struct ScrollPage {
    /// The rows, in order.
    pub rows: Vec<RowData>,
    /// Where the next page starts; `None` after the last page.
    pub next: Option<Cursor>,
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

/// One page of a scroll. A request without a cursor reads the current state and, when rows are
/// left after its page, pins the view it read; the page's cursor carries that token, and later
/// pages read exactly that snapshot. A scroll that fits in one page pins nothing.
///
/// # Errors
///
/// As [`CollectionReader::read_view`] (a cursor whose token expired is `SnapshotExpired`),
/// an invalid filter or order field, I/O, or typed corruption.
pub async fn scroll(
    reader: &dyn CollectionReader,
    collection: &CollectionRef,
    request: ScrollRequest,
) -> Result<ScrollPage> {
    let (options, after) = match &request.cursor {
        Some(cursor) => {
            if cursor.order != request.order {
                return Err(QueryError::Storage(LogPoseError::invalid_field(
                    "cursor",
                    "the cursor belongs to a scroll with another order",
                )));
            }
            (
                ReadOptions {
                    token: Some(cursor.token.clone()),
                    ..ReadOptions::default()
                },
                Some(cursor.after.clone()),
            )
        }
        None => (ReadOptions::default(), None),
    };
    let view = reader.read_view(collection, options).await?;
    let (rows, last) = scroll_view(
        &view,
        request.filter.as_ref(),
        &request.order,
        request.limit,
        request.projection,
        after.as_ref(),
    )
    .await?;
    let next = match last {
        Some(after) if rows.len() >= request.limit.max(1) as usize => {
            // Pinned only now that a later page will read it: a scroll that fits in one page
            // holds no token.
            let pinned = view.pinned()?;
            let token = pinned.token().cloned().ok_or_else(|| {
                QueryError::Storage(LogPoseError::internal("a pinned view has no token"))
            })?;
            Some(Cursor {
                token,
                order: request.order.clone(),
                after,
            })
        }
        _ => None,
    };
    Ok(ScrollPage { rows, next })
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
    let limit = limit.max(1) as usize;
    let filter = filter
        .map(|filter| CompiledFilter::compile(view.schema(), filter))
        .transpose()?
        .map(Arc::new);
    let ordered_field = match order {
        ScrollOrder::Pk => None,
        ScrollOrder::Field { field, direction } => Some((order_field(view, field)?, *direction)),
    };
    let mut plan = FetchPlan::default();
    let mut units = Vec::new();
    for unit in view.units() {
        if filter
            .as_ref()
            .is_some_and(|filter| !filter.may_match(&unit))
        {
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
            return Err(QueryError::Storage(LogPoseError::invalid_field(
                "cursor",
                "the cursor does not fit the scroll's order",
            )));
        }
    };
    let entries = view
        .run(move |view| {
            view.units()
                .into_par_iter()
                .filter(|unit| units.contains(&unit.id()))
                .map(|unit| {
                    let allowed = match &filter {
                        Some(filter) => filter.evaluate(&unit, &pins)?,
                        None => unit.live(),
                    };
                    match ordered_field {
                        None => unit_by_key(&unit, &pins, &allowed, after_entry.as_ref(), limit),
                        Some((field, direction)) => unit_by_field(
                            &unit,
                            &pins,
                            &allowed,
                            field,
                            direction,
                            after_entry.as_ref(),
                            limit,
                        ),
                    }
                })
                .collect::<Vec<logpose_types::Result<Vec<Entry>>>>()
        })
        .await?;
    let direction = ordered_field.map(|(_, direction)| direction);
    let mut merged = Vec::new();
    for entries in entries {
        merged.extend(entries?);
    }
    merged.sort_by(|left, right| compare_entries(left, right, direction));
    merged.truncate(limit);
    let addrs = merged.iter().map(|entry| entry.addr).collect::<Vec<_>>();
    let rows = view.rows(&addrs, projection).await?;
    let last = merged.last().map(|entry| match &ordered_field {
        None => CursorKey::Pk(entry.pk.clone()),
        Some(_) => CursorKey::Field(entry.key.as_ref().map(key_value), entry.pk.clone()),
    });
    Ok((rows, last))
}

/// The field a scroll orders by: a declared scalar field that is not an array or JSON.
fn order_field(view: &ReadView, name: &str) -> Result<FieldId> {
    match view.schema().field(name) {
        Some(FieldRef::Scalar(field))
            if !matches!(field.field_type, FieldType::Array(_) | FieldType::Json) =>
        {
            Ok(field.id)
        }
        _ => Err(QueryError::Storage(LogPoseError::invalid_field(
            "order_by",
            format!("'{name}' is not a scalar field that can be ordered"),
        ))),
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
    match (&left.key, &right.key) {
        (Some(left_key), Some(right_key)) => {
            let order = left_key.cmp(right_key);
            let order = match direction {
                Direction::Ascending => order,
                Direction::Descending => order.reverse(),
            };
            order.then_with(|| left.pk.cmp(&right.pk))
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.pk.cmp(&right.pk),
    }
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

/// Up to `limit` rows of `unit` in key order after `after`.
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

/// Up to `limit` rows of `unit` in `(value, key)` order after `after` (plus the rest of the
/// last value's ties, so the merge sees every tie).
fn unit_by_field(
    unit: &UnitView<'_>,
    pins: &PinSet,
    allowed: &RoaringBitmap,
    field: FieldId,
    direction: Direction,
    after: Option<&Entry>,
    limit: usize,
) -> logpose_types::Result<Vec<Entry>> {
    let pks = unit.pks(pins)?;
    let entry = |key: Option<ScalarKey>, row: RowId| -> logpose_types::Result<Entry> {
        Ok(Entry {
            key,
            pk: pks.pk_at(row).ok_or_else(|| {
                LogPoseError::internal(format!("row {row} of unit {} has no key", unit.id()))
            })?,
            addr: RowAddr {
                unit: unit.id(),
                row,
            },
        })
    };
    let is_after = |candidate: &Entry| {
        after.is_none_or(|after| {
            compare_entries(candidate, after, Some(direction)) == Ordering::Greater
        })
    };
    let mut out = Vec::new();
    if let Some(index) = unit.scalar_index(field, true, pins)? {
        let in_null_tail = after.is_some_and(|after| after.key.is_none());
        if !in_null_tail {
            let start = after
                .and_then(|after| after.key.as_ref())
                .map_or(Bound::Unbounded, Bound::Included);
            if let Some(ordered) = index.ordered(start, direction) {
                let mut group: Vec<Entry> = Vec::new();
                let mut group_key: Option<ScalarKey> = None;
                for (key, row) in ordered {
                    if !allowed.contains(row) {
                        continue;
                    }
                    if group_key.as_ref() != Some(&key) {
                        flush_group(&mut group, &mut out, &is_after);
                        if out.len() >= limit {
                            break;
                        }
                        group_key = Some(key.clone());
                    }
                    group.push(entry(Some(key), row)?);
                }
                flush_group(&mut group, &mut out, &is_after);
            }
        }
        if out.len() < limit {
            let mut nulls = Vec::new();
            for row in &(index.nulls() & allowed) {
                let candidate = entry(None, row)?;
                if is_after(&candidate) {
                    nulls.push(candidate);
                }
            }
            nulls.sort_by(|left, right| left.pk.cmp(&right.pk));
            out.extend(nulls);
        }
    } else {
        let column = unit.column(field, pins)?;
        for row in allowed {
            let key = column
                .value(row)?
                .and_then(|value| value_index_keys(&value).into_iter().next());
            let candidate = entry(key, row)?;
            if is_after(&candidate) {
                out.push(candidate);
            }
        }
        out.sort_by(|left, right| compare_entries(left, right, Some(direction)));
    }
    let keep = out
        .get(limit.saturating_sub(1))
        .map(|last| last.key.clone());
    if let Some(last_key) = keep {
        // Keep the whole tie group of the last row kept.
        let mut end = limit;
        while end < out.len() && out[end].key == last_key {
            end += 1;
        }
        out.truncate(end);
    }
    Ok(out)
}

/// Move a group of equal-valued rows, ordered by key, past the cursor into `out`.
fn flush_group(group: &mut Vec<Entry>, out: &mut Vec<Entry>, is_after: &impl Fn(&Entry) -> bool) {
    group.sort_by(|left, right| left.pk.cmp(&right.pk));
    out.extend(group.drain(..).filter(|entry| is_after(entry)));
}
