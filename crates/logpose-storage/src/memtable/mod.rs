//! The memtable: rows written since the last flush, in append-only slots.
//!
//! Every structure is persistent (`imbl`, `Arc`, and the two-tier [`CowBitmap`]), so the writer
//! mutates its private copy in place and a published `Version` holds an O(fields) clone that
//! later writes never change. An upsert of an existing key appends a new slot and the writer
//! sets the old slot's bit in the unit's deletion vector (which lives in `Version::deletes`, not
//! here); slots are never reused or removed, so a slot id keeps its meaning (I10).
//!
//! - [`VectorArena`]: one per vector field, contiguous `f32` in blocks of [`BLOCK_ROWS`] rows.
//! - [`MemColumn`]: one typed column per scalar field, starting at the slot the field was added.
//! - [`MemScalarIndex`]: inverted and sorted postings over slots, per indexed field.
//! - `$extra`: one object node in the binary value codec per slot.

mod arena;
mod column;
mod index;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use arena::BLOCK_ROWS;
pub(crate) use arena::VectorArena;
pub(crate) use column::MemColumn;
pub(crate) use index::{IndexFlavor, MemScalarIndex, index_keys};

use logpose_types::{
    RowId, SeqNo, UnitId,
    record::PrimaryKey,
    schema::{CollectionSchema, FieldId, FieldRef},
    value::Value,
};
use logpose_wal::codec::{F32Bytes, RowImage, ValueBytes, WirePk};
use std::{fmt, sync::Arc, time::Duration};

/// Bytes a memtable holds, as the flush trigger measures them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MemtableBytes {
    /// Key, vector, scalar, and dynamic bytes of every slot, dead slots included.
    pub(crate) payload: u64,
    /// 64 bytes per slot for persistent-structure overhead, plus 16 bytes per index entry.
    pub(crate) overhead: u64,
}

impl MemtableBytes {
    pub(crate) fn total(self) -> u64 {
        self.payload + self.overhead
    }
}

/// Engine-wide memtable limits. A collection's descriptor can lower the operation and byte
/// thresholds (`flush_threshold_ops`, `flush_threshold_bytes`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MemtableConfig {
    /// Flush once the active memtable holds this many bytes (payload plus overhead). Default
    /// 64 MiB.
    pub max_bytes: u64,
    /// Flush once the active memtable holds this many slots. Default 1,000,000.
    pub max_rows: u32,
    /// Flush once the active memtable's first slot is this old, which bounds replay time.
    /// Default 10 minutes.
    pub max_age: Duration,
    /// Share of the engine's `memory_limit` that every collection's memtables may hold
    /// together; past it the engine flushes the largest. Default 0.125.
    pub global_fraction: f64,
    /// Most memtables frozen and waiting for their flush at once. With this many frozen, an
    /// active memtable that reaches a trigger stalls writes until a flush commits. Default 2.
    pub max_frozen: usize,
    /// How long a write may wait through a write stall before it fails with
    /// `WriteStalled`. Default 30 seconds.
    pub write_stall_timeout: Duration,
    /// Flushes of a collection that may fail in a row before it is poisoned: it then refuses
    /// writes with `CollectionPoisoned` until the engine is reopened, instead of stalling them
    /// against a device that cannot take a flush. A flush that fails because the device is
    /// full or read-only, or on corrupt data, poisons at once. Default 5.
    pub max_flush_failures: u32,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_rows: 1_000_000,
            max_age: Duration::from_secs(10 * 60),
            global_fraction: 0.125,
            max_frozen: 2,
            write_stall_timeout: Duration::from_secs(30),
            max_flush_failures: 5,
        }
    }
}

/// Per-slot persistent-structure overhead charged to [`MemtableBytes::overhead`].
const SLOT_OVERHEAD: u64 = 64;
/// Per-posting-entry overhead charged to [`MemtableBytes::overhead`].
const INDEX_ENTRY_OVERHEAD: u64 = 16;

/// One memtable. Clone is O(number of fields).
#[derive(Clone)]
pub(crate) struct MemtableData {
    pub(crate) unit: UnitId,
    /// The latest schema applied. Readers use `Version::schema`, never this, to map names.
    pub(crate) schema: Arc<CollectionSchema>,
    /// The first sequence number that can be applied to this memtable.
    pub(crate) first_seq_no: SeqNo,
    /// The last sequence number applied (`first_seq_no - 1` while none was). Deletes and schema
    /// changes count: a memtable with operations but no slots still ends a checkpoint.
    pub(crate) last_seq_no: SeqNo,
    /// Engine-clock time the memtable was created, for the age trigger.
    pub(crate) created_at: Duration,
    slot_count: u32,
    pks: imbl::Vector<PrimaryKey>,
    seq_nos: imbl::Vector<SeqNo>,
    /// Key to latest slot in this memtable, live or dead. Ordered, so it also serves keyset
    /// scroll by key.
    pk_to_slot: imbl::OrdMap<PrimaryKey, RowId>,
    /// One arena per vector field of `schema`, sorted by field id.
    vectors: Vec<(FieldId, VectorArena)>,
    /// One column per scalar field of `schema`, sorted by field id, with the first slot it
    /// covers (slots below read null).
    columns: Vec<(FieldId, RowId, MemColumn)>,
    /// `$extra` per slot.
    dynamic: imbl::Vector<Option<Arc<[u8]>>>,
    /// Postings per indexed scalar field.
    indexes: Vec<(FieldId, MemScalarIndex)>,
    bytes: MemtableBytes,
}

impl MemtableData {
    /// An empty memtable for operations from `first_seq_no` on.
    pub(crate) fn new(
        unit: UnitId,
        schema: Arc<CollectionSchema>,
        first_seq_no: SeqNo,
        created_at: Duration,
    ) -> Self {
        let mut memtable = Self {
            unit,
            schema: Arc::clone(&schema),
            first_seq_no,
            last_seq_no: first_seq_no.saturating_sub(1),
            created_at,
            slot_count: 0,
            pks: imbl::Vector::new(),
            seq_nos: imbl::Vector::new(),
            pk_to_slot: imbl::OrdMap::new(),
            vectors: Vec::new(),
            columns: Vec::new(),
            dynamic: imbl::Vector::new(),
            indexes: Vec::new(),
            bytes: MemtableBytes::default(),
        };
        memtable.apply_schema(schema);
        memtable
    }

    /// Whether any operation was applied.
    pub(crate) fn has_ops(&self) -> bool {
        self.last_seq_no >= self.first_seq_no
    }

    /// Operations applied: one per sequence number.
    pub(crate) fn op_count(&self) -> u64 {
        if self.has_ops() {
            self.last_seq_no - self.first_seq_no + 1
        } else {
            0
        }
    }

    pub(crate) fn slot_count(&self) -> u32 {
        self.slot_count
    }

    pub(crate) fn bytes(&self) -> MemtableBytes {
        self.bytes
    }

    /// Record that the operation with sequence number `seq_no` was applied.
    pub(crate) fn note_op(&mut self, seq_no: SeqNo) {
        self.last_seq_no = self.last_seq_no.max(seq_no);
    }

    /// Follow a schema change in place: add a column (starting at the next slot) and empty
    /// indexes for every new scalar field, an arena for every new vector field, and drop the
    /// structures of every field `schema` no longer declares (nothing can read them: readers
    /// resolve names with a schema that does not declare them either, and published versions
    /// keep their own clones). A rename changes nothing.
    pub(crate) fn apply_schema(&mut self, schema: Arc<CollectionSchema>) {
        let declared = |id: FieldId| schema.field_by_id(id).is_some();
        self.vectors.retain(|(id, _)| declared(*id));
        self.columns.retain(|(id, _, _)| declared(*id));
        self.indexes.retain(|(id, _)| declared(*id));
        for field in schema.vectors() {
            if !self.vectors.iter().any(|(id, _)| *id == field.id) {
                let mut arena = VectorArena::new(field.dimensions);
                for _ in 0..self.slot_count {
                    arena.push(None);
                }
                self.vectors.push((field.id, arena));
            }
        }
        for field in schema.fields() {
            if self.columns.iter().any(|(id, _, _)| *id == field.id) {
                continue;
            }
            self.columns
                .push((field.id, self.slot_count, MemColumn::new(field.field_type)));
            for flavor in IndexFlavor::for_index(field.index) {
                self.indexes
                    .push((field.id, MemScalarIndex::new(flavor, self.slot_count)));
            }
        }
        self.vectors.sort_by_key(|(id, _)| *id);
        self.columns.sort_by_key(|(id, _, _)| *id);
        self.indexes
            .sort_by_key(|(id, index)| (*id, index.flavor()));
        self.schema = schema;
    }

    /// Append `image` as a new slot written at `seq_no` and return the slot. Values of fields
    /// the memtable's schema does not declare are skipped (replay of a batch written before a
    /// drop). Either the whole row is appended or, on error, nothing is.
    pub(crate) fn push(&mut self, seq_no: SeqNo, image: &RowImage) -> Result<RowId, String> {
        let slot = self.slot_count;
        let next = slot
            .checked_add(1)
            .filter(|next| *next < u32::MAX)
            .ok_or_else(|| "the memtable is full".to_owned())?;
        let pk = PrimaryKey::from(image.pk.clone());
        if pk.key_type() != self.schema.primary_key_type() {
            return Err(format!(
                "row key {pk} does not have the schema's key type {:?}",
                self.schema.primary_key_type()
            ));
        }
        // Decode and check everything before any structure changes.
        let mut vectors = Vec::with_capacity(image.vectors.len());
        for (field, bytes) in &image.vectors {
            if let Some(index) = self.vectors.iter().position(|(id, _)| id == field) {
                let arena = &self.vectors[index].1;
                if bytes.dimensions() != arena.dim() as usize {
                    return Err(format!(
                        "vector field {field} has {} dimensions; the schema declares {}",
                        bytes.dimensions(),
                        arena.dim()
                    ));
                }
                vectors.push((index, bytes.to_f32s()));
            }
        }
        let mut scalars = Vec::with_capacity(image.scalars.len());
        for (field, bytes) in &image.scalars {
            let Some(FieldRef::Scalar(declared)) = self.schema.field_by_id(*field) else {
                continue;
            };
            let value = bytes
                .decode(declared.field_type)
                .map_err(|error| format!("field {field} does not decode: {error}"))?;
            if !value.is_null() {
                scalars.push((*field, value, bytes.as_bytes().len()));
            }
        }
        vectors.sort_by_key(|(index, _)| *index);
        scalars.sort_by_key(|(field, _, _)| *field);
        if vectors.windows(2).any(|pair| pair[0].0 == pair[1].0)
            || scalars.windows(2).any(|pair| pair[0].0 == pair[1].0)
        {
            return Err("the row sets a field twice".to_owned());
        }
        let dynamic = image
            .dynamic
            .as_ref()
            .map(|bytes| Arc::<[u8]>::from(bytes.as_bytes()));

        let mut payload = match &pk {
            PrimaryKey::Int64(_) => 8,
            PrimaryKey::String(value) => value.len() as u64,
        };
        let mut index_entries = 0_u64;
        let mut values = scalars.into_iter().peekable();
        let mut vectors = vectors.into_iter().peekable();
        for (index, (_, arena)) in self.vectors.iter_mut().enumerate() {
            match vectors.next_if(|(position, _)| *position == index) {
                Some((_, vector)) => {
                    payload += vector.len() as u64 * 4;
                    arena.push(Some(&vector));
                }
                None => arena.push(None),
            }
        }
        for (field, _, column) in &mut self.columns {
            match values.next_if(|(id, _, _)| id == field) {
                Some((_, value, len)) => {
                    payload += len as u64;
                    for (_, index) in self.indexes.iter_mut().filter(|(id, _)| id == field) {
                        index_entries += index.insert(slot, Some(&value));
                    }
                    column.push(Some(value));
                }
                None => {
                    for (_, index) in self.indexes.iter_mut().filter(|(id, _)| id == field) {
                        index_entries += index.insert(slot, None);
                    }
                    column.push(None);
                }
            }
        }
        payload += dynamic.as_ref().map_or(0, |bytes| bytes.len() as u64);
        self.pks.push_back(pk.clone());
        self.seq_nos.push_back(seq_no);
        self.pk_to_slot.insert(pk, slot);
        self.dynamic.push_back(dynamic);
        self.slot_count = next;
        self.bytes.payload += payload;
        self.bytes.overhead += SLOT_OVERHEAD + INDEX_ENTRY_OVERHEAD * index_entries;
        self.note_op(seq_no);
        Ok(slot)
    }

    /// The slot holding `pk`'s latest row in this memtable, live or dead.
    pub(crate) fn find(&self, pk: &PrimaryKey) -> Option<RowId> {
        self.pk_to_slot.get(pk).copied()
    }

    pub(crate) fn pk(&self, slot: RowId) -> Option<&PrimaryKey> {
        self.pks.get(slot as usize)
    }

    pub(crate) fn seq_no(&self, slot: RowId) -> Option<SeqNo> {
        self.seq_nos.get(slot as usize).copied()
    }

    /// The vector of `field` at `slot`, `None` when it is null or the field has no arena.
    pub(crate) fn vector(&self, field: FieldId, slot: RowId) -> Option<&[f32]> {
        self.vectors
            .iter()
            .find(|(id, _)| *id == field)
            .and_then(|(_, arena)| arena.get(slot))
    }

    /// The value of scalar `field` at `slot`; `None` for null, a slot before the field existed,
    /// or a field the memtable has no column for.
    pub(crate) fn value(&self, field: FieldId, slot: RowId) -> Option<Value> {
        let (_, first, column) = self.columns.iter().find(|(id, _, _)| *id == field)?;
        column.get(slot.checked_sub(*first)?)
    }

    /// The postings of `field` of `flavor`, if the field has such an index.
    pub(crate) fn index(&self, field: FieldId, flavor: IndexFlavor) -> Option<&MemScalarIndex> {
        self.indexes
            .iter()
            .find(|(id, index)| *id == field && index.flavor() == flavor)
            .map(|(_, index)| index)
    }

    /// The row at `slot`, keyed by field id: every vector and scalar value the memtable holds
    /// for it and its `$extra`.
    pub(crate) fn row_image(&self, slot: RowId) -> Result<RowImage, String> {
        let pk = self
            .pk(slot)
            .ok_or_else(|| format!("slot {slot} is out of range"))?;
        let mut image = RowImage {
            pk: WirePk::from(pk.clone()),
            vectors: Vec::new(),
            scalars: Vec::new(),
            dynamic: None,
        };
        for (field, arena) in &self.vectors {
            if let Some(vector) = arena.get(slot) {
                image.vectors.push((*field, F32Bytes::from_f32s(vector)));
            }
        }
        for (field, first, column) in &self.columns {
            let Some(value) = slot.checked_sub(*first).and_then(|row| column.get(row)) else {
                continue;
            };
            let bytes = ValueBytes::encode(&value)
                .map_err(|error| format!("field {field} does not encode: {error}"))?;
            image.scalars.push((*field, bytes));
        }
        if let Some(Some(dynamic)) = self.dynamic.get(slot as usize) {
            image.dynamic = Some(ValueBytes::from_encoded(dynamic.to_vec()));
        }
        Ok(image)
    }

    /// Keys in ascending order with their latest slot.
    pub(crate) fn keys(&self) -> impl Iterator<Item = (&PrimaryKey, RowId)> + '_ {
        self.pk_to_slot.iter().map(|(pk, slot)| (pk, *slot))
    }

    /// Keys in `range`, ascending, with their latest slot.
    pub(crate) fn keys_range(
        &self,
        range: (std::ops::Bound<PrimaryKey>, std::ops::Bound<PrimaryKey>),
    ) -> impl Iterator<Item = (&PrimaryKey, RowId)> + '_ {
        self.pk_to_slot
            .range::<_, PrimaryKey>(range)
            .map(|(pk, slot)| (pk, *slot))
    }

    /// The `$extra` object stored at `slot` (before shadowing), or `None`.
    pub(crate) fn dynamic_object(
        &self,
        slot: RowId,
    ) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
        match self.dynamic.get(slot as usize) {
            Some(Some(bytes)) => crate::segment_v2::dynamic::decode_object(bytes)
                .map(Some)
                .map_err(|error| format!("slot {slot} has invalid dynamic bytes: {}", error.0)),
            _ => Ok(None),
        }
    }
}

impl fmt::Debug for MemtableData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemtableData")
            .field("unit", &self.unit)
            .field("first_seq_no", &self.first_seq_no)
            .field("last_seq_no", &self.last_seq_no)
            .field("slots", &self.slot_count)
            .field("bytes", &self.bytes)
            .finish()
    }
}
