//! The model of one collection: its logical state as a map from primary key to row, plus the
//! schema and the last sequence number. It knows nothing about memtables, segments, deletion
//! vectors, or files; every engine outcome is checked against what it predicts.
//!
//! Rows are stored the way a write stores them (typed values by `FieldId`, the raw `$extra`
//! object) and read the way a reader reads them (names from the current schema, shadowed
//! `$extra` keys hidden), so schema changes, dynamic-field shadowing, and partial updates of
//! rows written under an older schema follow the same rules as the engine without the model
//! sharing any of its code beyond the schema type itself.

use logpose_query::{FilterComparison, FilterExpr, FilterOperator, ScalarMetadataValue};
use logpose_storage::SchemaChange;
use logpose_types::{
    DistanceMetric, SeqNo,
    record::{ClientOp, PartialUpdate, PrimaryKey, Record},
    schema::{CollectionSchema, FieldId, FieldRef, FieldType},
    value::Value,
};
use serde_json::{Map, Value as Json};
use std::{cmp::Ordering, collections::BTreeMap};

/// One live row as a write stored it.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub vector: Vec<f32>,
    /// Typed scalar values by field id; nulls are not stored.
    pub typed: BTreeMap<FieldId, Value>,
    /// The `$extra` object as written, shadowed keys included.
    pub extra: Map<String, Json>,
    /// The sequence numbers the row's last write may carry: exact for a client batch, the
    /// batch's range for a filter write (whose key order the engine picks).
    pub seq: (SeqNo, SeqNo),
}

/// Why the model refuses a batch.
#[derive(Clone, Debug, PartialEq)]
pub enum Refusal {
    /// A partial update of a key without a live row.
    NotFound(PrimaryKey),
    /// A request the schema rejects.
    Invalid(String),
}

/// The logical state of a collection.
#[derive(Clone, Debug, PartialEq)]
pub struct Model {
    pub schema: CollectionSchema,
    pub rows: BTreeMap<PrimaryKey, Row>,
    pub visible_seq_no: SeqNo,
}

impl Model {
    pub fn new(schema: CollectionSchema) -> Self {
        Self {
            schema,
            rows: BTreeMap::new(),
            visible_seq_no: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// The name of the (only) vector field.
    pub fn vector_name(&self) -> String {
        self.schema
            .vectors()
            .first()
            .map(|field| field.name.clone())
            .unwrap_or_default()
    }

    pub fn metric(&self) -> DistanceMetric {
        self.schema
            .vectors()
            .first()
            .map_or(DistanceMetric::Dot, |field| field.metric)
    }

    /// Declared scalar fields as `(name, type)`, in declaration order.
    pub fn scalar_fields(&self) -> Vec<(String, FieldType)> {
        self.schema
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.field_type))
            .collect()
    }

    /// The record a reader of the current schema sees for `row`.
    pub fn visible(&self, pk: &PrimaryKey, row: &Row) -> Record {
        let mut record = Record::new(pk.clone());
        record
            .vectors
            .insert(self.vector_name(), row.vector.clone());
        for (id, value) in &row.typed {
            if let Some(FieldRef::Scalar(field)) = self.schema.field_by_id(*id) {
                record.fields.insert(field.name.clone(), value.clone());
            }
        }
        let mut extra = row.extra.clone();
        self.schema.retain_visible_dynamic(&mut extra);
        record.extra = extra;
        record
    }

    pub fn record(&self, pk: &PrimaryKey) -> Option<Record> {
        self.rows.get(pk).map(|row| self.visible(pk, row))
    }

    /// Every live record in key order.
    pub fn records(&self) -> Vec<Record> {
        self.rows
            .iter()
            .map(|(pk, row)| self.visible(pk, row))
            .collect()
    }

    /// The row a write of `record` stores, with sequence range `seq`.
    fn store(&self, record: Record, seq: (SeqNo, SeqNo)) -> Result<Row, Refusal> {
        let record = self
            .schema
            .validate_record(record)
            .map_err(|error| Refusal::Invalid(error.to_string()))?;
        let vector = record
            .vectors
            .get(&self.vector_name())
            .cloned()
            .ok_or_else(|| Refusal::Invalid("the record has no vector".to_owned()))?;
        let mut typed = BTreeMap::new();
        for (name, value) in record.fields {
            let field = self
                .schema
                .scalar_field(&name)
                .ok_or_else(|| Refusal::Invalid(format!("'{name}' is not declared")))?;
            if !value.is_null() {
                typed.insert(field.id, value);
            }
        }
        Ok(Row {
            vector,
            typed,
            extra: record.extra,
            seq,
        })
    }

    /// Apply a client batch: every operation or none. Each operation takes one sequence number.
    pub fn apply_batch(&self, ops: &[ClientOp]) -> Result<Self, Refusal> {
        let mut next = self.clone();
        let first = self.visible_seq_no + 1;
        for (index, op) in ops.iter().enumerate() {
            let seq = first + index as SeqNo;
            match op {
                ClientOp::Upsert(record) => {
                    let row = next.store(record.clone(), (seq, seq))?;
                    next.rows.insert(record.pk.clone(), row);
                }
                ClientOp::Update(update) => {
                    let row = next.updated(update, (seq, seq))?;
                    next.rows.insert(update.pk.clone(), row);
                }
                ClientOp::Delete(pk) => {
                    next.rows.remove(pk);
                }
            }
        }
        next.visible_seq_no = self.visible_seq_no + ops.len() as SeqNo;
        Ok(next)
    }

    /// The row a partial update of a live key leaves: the old row read with the current schema,
    /// the patch applied, and the result stored again.
    fn updated(&self, update: &PartialUpdate, seq: (SeqNo, SeqNo)) -> Result<Row, Refusal> {
        let update = self
            .schema
            .validate_update(update.clone())
            .map_err(|error| Refusal::Invalid(error.to_string()))?;
        let Some(old) = self.record(&update.pk) else {
            return Err(Refusal::NotFound(update.pk));
        };
        let mut record = old;
        update
            .apply_to(&mut record)
            .map_err(|error| Refusal::Invalid(error.to_string()))?;
        self.store(record, seq)
    }

    /// Apply a schema change, which takes one sequence number.
    pub fn apply_schema(&self, change: &SchemaChange) -> Result<Self, Refusal> {
        let mut next = self.clone();
        change
            .apply_to(&mut next.schema)
            .map_err(|error| Refusal::Invalid(error.to_string()))?;
        next.visible_seq_no += 1;
        Ok(next)
    }

    /// Keys of the live rows matching `filter`, ascending.
    pub fn matching(&self, filter: Option<&FilterExpr>) -> Vec<PrimaryKey> {
        self.rows
            .iter()
            .filter(|(pk, row)| filter.is_none_or(|filter| self.matches(filter, pk, row)))
            .map(|(pk, _)| pk.clone())
            .collect()
    }

    /// Delete every live row matching `filter` as one batch. Returns the new model and the
    /// number of rows deleted; none matching takes no sequence number.
    pub fn delete_by_filter(&self, filter: &FilterExpr) -> (Self, usize) {
        let keys = self.matching(Some(filter));
        let mut next = self.clone();
        for key in &keys {
            next.rows.remove(key);
        }
        next.visible_seq_no += keys.len() as SeqNo;
        (next, keys.len())
    }

    /// Apply `patch` to every live row matching `filter` as one batch.
    pub fn update_by_filter(
        &self,
        filter: &FilterExpr,
        patch: &PartialUpdate,
    ) -> Result<(Self, usize), Refusal> {
        let keys = self.matching(Some(filter));
        let mut next = self.clone();
        let range = (
            self.visible_seq_no + 1,
            self.visible_seq_no + keys.len() as SeqNo,
        );
        for key in &keys {
            let mut update = patch.clone();
            update.pk = key.clone();
            let row = self.updated(&update, range)?;
            next.rows.insert(key.clone(), row);
        }
        next.visible_seq_no += keys.len() as SeqNo;
        Ok((next, keys.len()))
    }

    /// The typed value of the declared scalar field `name` in `row`, if the row has one.
    fn typed_value<'a>(&self, name: &str, row: &'a Row) -> Option<&'a Value> {
        let field = self.schema.scalar_field(name)?;
        row.typed.get(&field.id)
    }

    /// Whether `row` matches `filter`: declared fields compare by type (integers as integers,
    /// strings bytewise); `ne` needs a value other than the operand; `not` is the complement
    /// over live rows, so it matches rows without a value too.
    pub fn matches(&self, filter: &FilterExpr, pk: &PrimaryKey, row: &Row) -> bool {
        match filter {
            FilterExpr::And { children } => children.iter().all(|c| self.matches(c, pk, row)),
            FilterExpr::Or { children } => children.iter().any(|c| self.matches(c, pk, row)),
            FilterExpr::Not { child } => !self.matches(child, pk, row),
            FilterExpr::Comparison(comparison) => self.compare(comparison, row),
        }
    }

    fn compare(&self, comparison: &FilterComparison, row: &Row) -> bool {
        let value = self.typed_value(&comparison.field, row);
        let ordering = match (value, comparison.value.as_ref()) {
            (Some(Value::Int64(value)), Some(ScalarMetadataValue::Number(number))) => {
                number.as_i64().map(|wanted| value.cmp(&wanted))
            }
            (Some(Value::String(value)), Some(ScalarMetadataValue::String(wanted))) => {
                Some(value.as_bytes().cmp(wanted.as_bytes()))
            }
            _ => None,
        };
        match comparison.operator {
            FilterOperator::Exists => value.is_some(),
            FilterOperator::IsNull => value.is_none(),
            FilterOperator::Eq => ordering == Some(Ordering::Equal),
            FilterOperator::Ne => ordering.is_some_and(Ordering::is_ne),
            FilterOperator::Lt => ordering == Some(Ordering::Less),
            FilterOperator::Lte => ordering.is_some_and(Ordering::is_le),
            FilterOperator::Gt => ordering == Some(Ordering::Greater),
            FilterOperator::Gte => ordering.is_some_and(Ordering::is_ge),
        }
    }

    /// The exact top `k` of the rows matching `filter` for `query`: `(key, metric value)`,
    /// best first, ties by key.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: Option<&FilterExpr>,
    ) -> Vec<(PrimaryKey, f32)> {
        let metric = self.metric();
        let mut scored = self
            .rows
            .iter()
            .filter(|(pk, row)| filter.is_none_or(|filter| self.matches(filter, pk, row)))
            .map(|(pk, row)| (pk.clone(), metric_value(metric, query, &row.vector)))
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            better(metric, left.1, right.1).then_with(|| left.0.cmp(&right.0))
        });
        scored.truncate(k);
        scored
    }

    /// Keys of the rows matching `filter` in `(value, key)` order by the declared field
    /// `field`, `descending` or not; rows without a value come last either way, by key.
    pub fn order_by(
        &self,
        field: &str,
        descending: bool,
        filter: Option<&FilterExpr>,
    ) -> Vec<PrimaryKey> {
        let mut rows = self
            .rows
            .iter()
            .filter(|(pk, row)| filter.is_none_or(|filter| self.matches(filter, pk, row)))
            .map(|(pk, row)| (self.typed_value(field, row).cloned(), pk.clone()))
            .collect::<Vec<_>>();
        rows.sort_by(|(left, left_pk), (right, right_pk)| {
            let by_value = match (left, right) {
                (Some(left), Some(right)) => {
                    let ordering = compare_values(left, right);
                    if descending {
                        ordering.reverse()
                    } else {
                        ordering
                    }
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            by_value.then_with(|| left_pk.cmp(right_pk))
        });
        rows.into_iter().map(|(_, pk)| pk).collect()
    }
}

/// Integers numerically, strings bytewise.
fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::String(left), Value::String(right)) => left.as_bytes().cmp(right.as_bytes()),
        _ => Ordering::Equal,
    }
}

/// Order metric values best first.
fn better(metric: DistanceMetric, left: f32, right: f32) -> Ordering {
    match metric {
        DistanceMetric::L2 => left.total_cmp(&right),
        DistanceMetric::Dot | DistanceMetric::Cosine => right.total_cmp(&left),
    }
}

/// The value the API reports: similarity for dot, Euclidean distance for L2. The harness uses
/// small integer components, so every value is exact in `f32` whatever the summation order.
pub fn metric_value(metric: DistanceMetric, query: &[f32], vector: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => query
            .iter()
            .zip(vector)
            .map(|(left, right)| (left - right) * (left - right))
            .sum::<f32>()
            .sqrt(),
        DistanceMetric::Dot | DistanceMetric::Cosine => query
            .iter()
            .zip(vector)
            .map(|(left, right)| left * right)
            .sum(),
    }
}
