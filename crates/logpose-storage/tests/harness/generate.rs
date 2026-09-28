//! Random actions, drawn from the state of a run: batches over a small key pool (so keys are
//! upserted, updated, and deleted again and again, across memtables and segments), filters and
//! schema changes over the fields the schema declares now, reads, token and scroll actions,
//! job phases, and faults.

use crate::{
    actions::{Action, Read, Runner},
    model::Model,
    session::{Backend, DIMS, Maintenance},
};
use logpose_query::FilterExpr;
use logpose_storage::{JobKind, SchemaChange};
use logpose_types::{
    record::{ClientOp, PartialUpdate, PrimaryKey, Record},
    schema::{FieldType, ScalarFieldSpec},
    value::Value,
};
use logpose_vfs::TearMode;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::{Value as Json, json};
use std::time::Duration;

/// Keys `k00` to `k15`.
pub const KEYS: u64 = 16;
/// `$extra` keys the generator writes; field names come from the same pool, so declaring or
/// retiring one shadows values already stored under it.
const DYNAMIC: [&str; 3] = ["x", "y", "z"];
/// Names for added and renamed fields.
const NAMES: [&str; 6] = ["x", "y", "z", "p", "q", "r"];
/// String values, chosen so that bytewise order and prefixes matter.
const WORDS: [&str; 5] = ["a", "ab", "b", "bc", "c"];

pub fn key(index: u64) -> PrimaryKey {
    PrimaryKey::from(format!("k{index:02}").as_str())
}

/// Draws actions for one run.
pub struct Generator {
    rng: StdRng,
}

/// What kind of action to draw, before its parameters.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Write,
    DeleteByFilter,
    UpdateByFilter,
    Alter,
    Flush,
    Compact,
    Begin(JobKind),
    Build(JobKind),
    Commit(JobKind),
    Abandon(JobKind),
    Step,
    Pin,
    Release,
    Advance,
    Read,
    ReadAt,
    ScrollStart,
    ScrollNext,
    Crash,
    FailSync,
    Reopen,
}

impl Generator {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed ^ 0x5eed),
        }
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.rng.random_range(0..bound.max(1))
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }

    /// The next action for `runner`'s current state.
    pub fn next(&mut self, runner: &Runner) -> Action {
        let kind = self.kind(runner);
        self.action(kind, runner)
    }

    fn kind(&mut self, runner: &Runner) -> Kind {
        let setup = runner.session.setup;
        let fault = setup.backend == Backend::Fault;
        let jobs = runner.open_jobs();
        let open = |kind: JobKind| jobs.iter().any(|(open, _)| *open == kind);
        let mut weights: Vec<(u64, Kind)> = Vec::new();
        if runner.poisoned {
            // Read-only until a reopen or a crash settles it.
            weights.extend([(30, Kind::Read), (10, Kind::ReadAt), (40, Kind::Reopen)]);
            if fault {
                weights.push((30, Kind::Crash));
            }
        } else {
            weights.extend([
                (300, Kind::Write),
                (15, Kind::DeleteByFilter),
                (15, Kind::UpdateByFilter),
                (15, Kind::Alter),
                (20, Kind::Pin),
                (10, Kind::Release),
                (15, Kind::Advance),
                (90, Kind::Read),
                (40, Kind::ReadAt),
                (20, Kind::ScrollStart),
                (40, Kind::ScrollNext),
                (15, Kind::Reopen),
            ]);
            if jobs.is_empty() {
                weights.extend([(30, Kind::Flush), (20, Kind::Compact)]);
            }
            let mut stepped = vec![JobKind::Compact];
            if setup.maintenance == Maintenance::Stepped {
                stepped.push(JobKind::Flush);
            }
            for kind in stepped {
                if open(kind) {
                    weights.extend([
                        (25, Kind::Build(kind)),
                        (35, Kind::Commit(kind)),
                        (4, Kind::Abandon(kind)),
                    ]);
                } else {
                    weights.push((35, Kind::Begin(kind)));
                }
            }
            if setup.maintenance == Maintenance::Paused {
                weights.push((70, Kind::Step));
            }
            if fault {
                weights.extend([(50, Kind::Crash), (15, Kind::FailSync)]);
            }
        }
        let total = weights.iter().map(|(weight, _)| weight).sum::<u64>();
        let mut draw = self.below(total);
        for (weight, kind) in weights {
            if draw < weight {
                return kind;
            }
            draw -= weight;
        }
        Kind::Read
    }

    fn action(&mut self, kind: Kind, runner: &Runner) -> Action {
        let model = &runner.model;
        match kind {
            Kind::Write => Action::Write(self.batch(model)),
            Kind::DeleteByFilter => match self.filter(model) {
                Some(filter) => Action::DeleteByFilter(filter),
                None => Action::Write(self.batch(model)),
            },
            Kind::UpdateByFilter => match self.filter(model) {
                Some(filter) => {
                    let mut patch = self.patch(model, &key(0));
                    patch.vectors.clear();
                    if patch.is_empty() {
                        patch.vectors.insert(model.vector_name(), self.vector());
                    }
                    Action::UpdateByFilter(filter, patch)
                }
                None => Action::Write(self.batch(model)),
            },
            Kind::Alter => Action::Alter(self.schema_change(model)),
            Kind::Flush => Action::Flush,
            Kind::Compact => Action::Compact,
            Kind::Begin(kind) => Action::BeginJob(kind),
            Kind::Build(kind) => Action::BuildJob(kind),
            Kind::Commit(kind) => Action::CommitJob(kind),
            Kind::Abandon(kind) => Action::AbandonJob(kind),
            Kind::Step => Action::StepScheduler,
            Kind::Pin => Action::Pin,
            Kind::Release => match self.token(runner) {
                Some(id) => Action::Release(id),
                None => Action::Pin,
            },
            Kind::Advance => {
                Action::AdvanceClock(Duration::from_secs(self.pick(&[1, 7, 13, 29, 45, 61])))
            }
            Kind::Read => Action::Read(self.read(model)),
            Kind::ReadAt => match self.token(runner) {
                Some(id) => {
                    let pinned = runner.token_model(id).unwrap_or(model);
                    Action::ReadAt(id, self.read(pinned))
                }
                None => Action::Pin,
            },
            Kind::ScrollStart => self.scroll_start(runner),
            Kind::ScrollNext => {
                let scrolls = runner.open_scrolls();
                if scrolls.is_empty() {
                    self.scroll_start(runner)
                } else {
                    Action::ScrollNext(self.pick(&scrolls))
                }
            }
            Kind::Crash => self.crash(runner),
            Kind::FailSync => {
                let (during, nth) = match self.below(4) {
                    0 | 1 => (Action::Write(self.batch(model)), self.below(2)),
                    2 => (Action::Alter(self.schema_change(model)), self.below(2)),
                    _ => (self.job_step(runner), self.below(10)),
                };
                Action::FailSync {
                    during: Box::new(during),
                    nth,
                    dir: self.chance(30),
                }
            }
            Kind::Reopen => Action::Reopen,
        }
    }

    fn scroll_start(&mut self, runner: &Runner) -> Action {
        let model = &runner.model;
        let by = if self.chance(40) {
            self.order_field(model)
        } else {
            None
        };
        let filter = if self.chance(40) {
            self.filter(model)
        } else {
            None
        };
        Action::ScrollStart {
            id: runner.next_id,
            by,
            filter,
            limit: 1 + self.below(4) as u32,
        }
    }

    /// A job phase to crash in or fail a sync in: the next phase of an open job, or a step of
    /// the scheduler, or a new flush.
    fn job_step(&mut self, runner: &Runner) -> Action {
        let jobs = runner.open_jobs();
        if let Some((kind, built)) = jobs.first().copied() {
            return if built || self.chance(40) {
                Action::CommitJob(kind)
            } else {
                Action::BuildJob(kind)
            };
        }
        if runner.session.setup.maintenance == Maintenance::Paused && self.chance(70) {
            return Action::StepScheduler;
        }
        if self.chance(50) {
            Action::Flush
        } else {
            Action::Compact
        }
    }

    fn crash(&mut self, runner: &Runner) -> Action {
        let model = &runner.model;
        let (during, window) = match self.below(10) {
            0..=2 => (Some(Action::Write(self.batch(model))), 4),
            3 => (self.filter(model).map(Action::DeleteByFilter), 4),
            4 => (Some(Action::Alter(self.schema_change(model))), 4),
            5..=7 => (Some(self.job_step(runner)), 70),
            _ => (None, 3),
        };
        let recovery = self
            .chance(30)
            .then(|| (self.below(30), self.pick(&TearMode::ALL)));
        Action::Crash {
            during: during.map(Box::new),
            after_ops: self.below(window),
            tear: self.pick(&TearMode::ALL),
            recovery,
        }
    }

    fn token(&mut self, runner: &Runner) -> Option<u64> {
        let tokens = runner.all_tokens();
        if tokens.is_empty() {
            None
        } else {
            let live = runner.live_tokens();
            // Mostly live tokens; sometimes a released, expired, or lost one.
            if !live.is_empty() && self.chance(85) {
                Some(self.pick(&live))
            } else {
                Some(self.pick(&tokens))
            }
        }
    }

    fn vector(&mut self) -> Vec<f32> {
        (0..DIMS)
            .map(|_| (self.below(7) as i64 - 3) as f32)
            .collect()
    }

    fn value(&mut self, field_type: FieldType) -> Value {
        match field_type {
            FieldType::String => Value::String(self.pick(&WORDS).to_owned()),
            _ => Value::Int64(self.below(8) as i64),
        }
    }

    fn record(&mut self, model: &Model, pk: &PrimaryKey) -> Record {
        let mut record = Record::new(pk.clone()).with_vector(model.vector_name(), self.vector());
        for (name, field_type) in model.scalar_fields() {
            if self.chance(70) {
                let value = self.value(field_type);
                record.fields.insert(name, value);
            }
        }
        for name in DYNAMIC {
            if !model.schema.shadows_dynamic_key(name) && self.chance(30) {
                record.extra.insert(name.to_owned(), json!(self.below(5)));
            }
        }
        if self.chance(2) {
            // A dynamic key the schema declares or retires: the write is refused.
            if let Some((name, _)) = model.scalar_fields().first() {
                record.extra.insert(name.clone(), json!(1));
            }
        }
        record
    }

    fn patch(&mut self, model: &Model, pk: &PrimaryKey) -> PartialUpdate {
        let mut patch = PartialUpdate::new(pk.clone());
        if self.chance(30) {
            patch.vectors.insert(model.vector_name(), self.vector());
        }
        for (name, field_type) in model.scalar_fields() {
            match self.below(100) {
                0..30 => {
                    let value = self.value(field_type);
                    patch.fields.insert(name, value);
                }
                30..40 => {
                    patch.fields.insert(name, Value::Null);
                }
                _ => {}
            }
        }
        for name in DYNAMIC {
            if model.schema.shadows_dynamic_key(name) {
                continue;
            }
            match self.below(100) {
                0..20 => {
                    patch.extra.insert(name.to_owned(), json!(self.below(5)));
                }
                20..30 => {
                    patch.extra.insert(name.to_owned(), Json::Null);
                }
                _ => {}
            }
        }
        if patch.is_empty() {
            patch.vectors.insert(model.vector_name(), self.vector());
        }
        patch
    }

    /// One to four operations on distinct keys.
    fn batch(&mut self, model: &Model) -> Vec<ClientOp> {
        let size = 1 + self.below(4) as usize;
        let live = model.rows.keys().cloned().collect::<Vec<_>>();
        let mut keys: Vec<PrimaryKey> = Vec::new();
        let mut ops = Vec::new();
        while ops.len() < size {
            let kind = self.below(10);
            let pk = if kind >= 5 && !live.is_empty() && self.chance(80) {
                live[self.below(live.len() as u64) as usize].clone()
            } else {
                key(self.below(KEYS))
            };
            if keys.contains(&pk) {
                if keys.len() as u64 >= KEYS {
                    break;
                }
                continue;
            }
            keys.push(pk.clone());
            ops.push(match kind {
                0..=4 => ClientOp::Upsert(self.record(model, &pk)),
                5..=7 => ClientOp::Update(self.patch(model, &pk)),
                _ => ClientOp::Delete(pk),
            });
        }
        ops
    }

    fn comparison(&mut self, model: &Model) -> Option<FilterExpr> {
        let fields = model.scalar_fields();
        if fields.is_empty() {
            return None;
        }
        let (name, field_type) = fields[self.below(fields.len() as u64) as usize].clone();
        let operator = self.pick(&["eq", "ne", "lt", "lte", "gt", "gte", "exists", "is_null"]);
        if matches!(operator, "exists" | "is_null") {
            return Some(if operator == "exists" {
                FilterExpr::exists(name)
            } else {
                FilterExpr::is_null(name)
            });
        }
        let value = match field_type {
            FieldType::String => Value::String(self.pick(&WORDS).to_owned()),
            _ => Value::Int64(self.below(9) as i64 - 1),
        };
        Some(match operator {
            "eq" => FilterExpr::eq(name, value),
            "ne" => FilterExpr::ne(name, value),
            "lt" => FilterExpr::lt(name, value),
            "lte" => FilterExpr::lte(name, value),
            "gt" => FilterExpr::gt(name, value),
            _ => FilterExpr::gte(name, value),
        })
    }

    pub fn filter(&mut self, model: &Model) -> Option<FilterExpr> {
        let first = self.comparison(model)?;
        Some(match self.below(6) {
            0 => FilterExpr::And(vec![first, self.comparison(model)?]),
            1 => FilterExpr::Or(vec![first, self.comparison(model)?]),
            2 => FilterExpr::Not(Box::new(first)),
            _ => first,
        })
    }

    fn order_field(&mut self, model: &Model) -> Option<(String, bool)> {
        let fields = model.scalar_fields();
        if fields.is_empty() {
            return None;
        }
        let (name, _) = fields[self.below(fields.len() as u64) as usize].clone();
        Some((name, self.chance(50)))
    }

    fn read(&mut self, model: &Model) -> Read {
        match self.below(10) {
            0 | 1 => {
                let mut keys = (0..1 + self.below(5))
                    .map(|_| key(self.below(KEYS + 2)))
                    .collect::<Vec<_>>();
                keys.dedup();
                Read::Get(keys)
            }
            2 | 3 => Read::Count(self.filter(model)),
            4 => Read::Scan {
                limit: 1 + self.below(6) as u32,
            },
            5 | 6 => match self.order_field(model) {
                Some((field, descending)) => Read::OrderBy {
                    field,
                    descending,
                    filter: if self.chance(30) {
                        self.filter(model)
                    } else {
                        None
                    },
                    limit: 1 + self.below(5) as u32,
                },
                None => Read::Count(None),
            },
            _ => Read::Search {
                query: self.vector(),
                k: 1 + self.below(6) as usize,
                filter: if self.chance(50) {
                    self.filter(model)
                } else {
                    None
                },
            },
        }
    }

    pub fn schema_change(&mut self, model: &Model) -> SchemaChange {
        let fields = model.scalar_fields();
        let free = NAMES
            .iter()
            .filter(|name| model.schema.field(name).is_none())
            .copied()
            .collect::<Vec<_>>();
        match self.below(3) {
            0 if !fields.is_empty() => {
                let (name, _) = fields[self.below(fields.len() as u64) as usize].clone();
                SchemaChange::DropField { name }
            }
            1 if !free.is_empty() => {
                let from = if self.chance(20) || fields.is_empty() {
                    model.vector_name()
                } else {
                    fields[self.below(fields.len() as u64) as usize].0.clone()
                };
                SchemaChange::RenameField {
                    from,
                    to: self.pick(&free).to_owned(),
                }
            }
            _ if !free.is_empty() => {
                let field_type = if self.chance(50) {
                    FieldType::Int64
                } else {
                    FieldType::String
                };
                SchemaChange::AddField(ScalarFieldSpec::new(self.pick(&free), field_type))
            }
            _ => SchemaChange::DropField {
                name: fields
                    .first()
                    .map_or_else(|| "n".to_owned(), |(name, _)| name.clone()),
            },
        }
    }
}
