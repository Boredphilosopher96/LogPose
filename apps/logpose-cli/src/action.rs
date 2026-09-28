use anyhow::{Context, bail};
use clap::ValueEnum;
use logpose_auth::DatabaseAccessPolicy;
use logpose_catalog::DatabaseDescriptor;
use logpose_query::{
    CountRecordsRequest, ExplainMode, FilterExpr, OrderBy, QueryRequest, ReadConsistency,
    ScrollRecordsRequest, SortDirection, VectorQuery,
};
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{
    CollectionRef, DEFAULT_DATABASE_NAME, DistanceMetric, Snapshot,
    record::{PrimaryKey, Record},
    schema::{CollectionSchema, CreateCollectionSpec, PrimaryKeyType, SchemaChange},
};
use serde_json::Value;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

pub const CLI_PUT_BATCH_BYTES: usize = 1024 * 1024;
pub const FILE_PICKER_LIMIT: usize = 4000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum MetricArg {
    Cosine,
    Dot,
    L2,
}

impl From<MetricArg> for DistanceMetric {
    fn from(value: MetricArg) -> Self {
        match value {
            MetricArg::Cosine => Self::Cosine,
            MetricArg::Dot => Self::Dot,
            MetricArg::L2 => Self::L2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ExplainArg {
    Plan,
    Profile,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryVector(pub Vec<f32>);

/// An equality shorthand, `FIELD=VALUE`: the filter `{"eq": {FIELD: VALUE}}`.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryFilter {
    pub field: String,
    pub value: Value,
}

/// Filter inputs shared by query, count, scroll, and delete, combined with AND: equality
/// shorthands (`--filter`), where clauses (`--where`), and a filter document (`--filter-json`).
/// Each is the natural JSON filter of the REST API, typed by the schema when the command runs.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FilterInput {
    pub filters: Vec<QueryFilter>,
    pub where_clauses: Vec<Value>,
    pub filter_json: Option<PathBuf>,
}

impl FilterInput {
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty() && self.where_clauses.is_empty() && self.filter_json.is_none()
    }

    /// The combined filter document: one node, or `{"and": [...]}` of every node.
    pub fn document(&self) -> anyhow::Result<Option<Value>> {
        let mut nodes = self
            .filters
            .iter()
            .map(|filter| serde_json::json!({ "eq": { filter.field.as_str(): filter.value } }))
            .collect::<Vec<_>>();
        nodes.extend(self.where_clauses.iter().cloned());
        if let Some(path) = &self.filter_json {
            let file = File::open(path)
                .with_context(|| format!("failed to open filter json '{}'", path.display()))?;
            let document = serde_json::from_reader::<_, Value>(file)
                .with_context(|| format!("failed to parse filter json '{}'", path.display()))?;
            nodes.push(document);
        }
        Ok(match nodes.len() {
            0 => None,
            1 => nodes.pop(),
            _ => Some(serde_json::json!({ "and": nodes })),
        })
    }

    /// The typed filter, checked against `schema`.
    pub fn resolve(&self, schema: &CollectionSchema) -> anyhow::Result<Option<FilterExpr>> {
        self.document()?
            .map(|document| {
                FilterExpr::from_json(schema, document, "filter")
                    .map_err(|error| anyhow::anyhow!("invalid filter: {error}"))
            })
            .transpose()
    }

    fn push_flags(&self, parts: &mut Vec<String>) {
        for filter in &self.filters {
            parts.push("--filter".to_owned());
            parts.push(shell_quote(&format_filter(filter)));
        }
        for clause in &self.where_clauses {
            parts.push("--where".to_owned());
            parts.push(shell_quote(&format_predicate(clause)));
        }
        if let Some(path) = &self.filter_json {
            parts.push("--filter-json".to_owned());
            parts.push(shell_quote(&path.to_string_lossy()));
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectionCreateAction {
    pub collection: CollectionRef,
    pub dimensions: usize,
    pub metric: DistanceMetric,
}

/// Create a collection from a typed schema document.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionSchemaCreateAction {
    pub collection: CollectionRef,
    pub schema: PathBuf,
}

/// Apply one online schema change.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionAlterAction {
    pub collection: CollectionRef,
    pub change: SchemaChange,
}

/// Point lookups by primary key.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordGetAction {
    pub collection: CollectionRef,
    pub keys: Vec<String>,
    pub output_fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectionStatsAction {
    pub collection: CollectionRef,
    pub snapshot_manifest_generation: Option<u64>,
    pub snapshot_visible_seq_no: Option<u64>,
    pub read_barrier_manifest_generation: Option<u64>,
    pub read_barrier_visible_seq_no: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatabasePolicySetAction {
    pub database_name: String,
    pub input: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatabasePutAction {
    pub database_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordPutAction {
    pub collection: CollectionRef,
    pub input: PathBuf,
}

/// Delete one record by key, or every record a filter matches.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordDeleteAction {
    pub collection: CollectionRef,
    pub id: Option<String>,
    pub filter: FilterInput,
}

/// Count the records matching a filter.
#[derive(Debug, Clone, PartialEq)]
pub struct CountAction {
    pub collection: CollectionRef,
    pub filter: FilterInput,
    pub snapshot_token: Option<String>,
    pub pin: bool,
}

/// One page of a scroll.
#[derive(Debug, Clone, PartialEq)]
pub struct ScrollAction {
    pub collection: CollectionRef,
    pub filter: FilterInput,
    pub order_by: Option<OrderBy>,
    pub page_size: Option<u32>,
    pub output_fields: Vec<String>,
    pub cursor: Option<String>,
    pub snapshot_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryAction {
    pub collection: CollectionRef,
    pub top_k: usize,
    pub vector: Option<QueryVector>,
    pub vector_field: Option<String>,
    pub filter: FilterInput,
    pub order_by: Option<OrderBy>,
    pub output_fields: Vec<String>,
    pub ef: Option<usize>,
    pub snapshot_token: Option<String>,
    pub pin: bool,
    pub explain: Option<ExplainArg>,
    pub snapshot_manifest_generation: Option<u64>,
    pub snapshot_visible_seq_no: Option<u64>,
    pub read_barrier_manifest_generation: Option<u64>,
    pub read_barrier_visible_seq_no: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Status,
    ConfigShow,
    DatabaseList,
    DatabaseShow {
        database_name: String,
    },
    DatabasePut(DatabasePutAction),
    DatabasePolicyShow {
        database_name: String,
    },
    DatabasePolicySet(DatabasePolicySetAction),
    DatabaseDrop {
        database_name: String,
    },
    CollectionCreate(CollectionCreateAction),
    CollectionCreateFromSchema(CollectionSchemaCreateAction),
    CollectionList {
        database_name: String,
    },
    CollectionShow(CollectionRef),
    CollectionAlter(CollectionAlterAction),
    CollectionDrop(CollectionRef),
    CollectionStats(CollectionStatsAction),
    CollectionPlacement(CollectionRef),
    CollectionFlush(CollectionRef),
    CollectionCompact(CollectionRef),
    RecordPut(RecordPutAction),
    RecordDelete(RecordDeleteAction),
    RecordGet(RecordGetAction),
    Count(CountAction),
    Scroll(ScrollAction),
    Query(QueryAction),
    Inspect {
        collection: CollectionRef,
        target: InspectTarget,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowKind {
    CollectionCreate,
    CollectionShow,
    CollectionStats,
    CollectionPlacement,
    CollectionFlush,
    CollectionCompact,
    RecordPut,
    RecordDelete,
    Query,
    InspectManifest,
    InspectWal,
    InspectMaintenance,
    InspectSegment,
    Status,
    ConfigShow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowDefinition {
    pub kind: WorkflowKind,
    pub group: &'static str,
    pub label: &'static str,
    pub detail: &'static str,
    pub aliases: &'static [&'static str],
}

#[derive(Clone)]
pub struct PickerChoice<T> {
    pub value: T,
    pub label: String,
    pub detail: String,
    pub search_text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PathChoice {
    pub path: PathBuf,
    pub display: String,
}

pub fn workflow_definitions() -> Vec<WorkflowDefinition> {
    vec![
        WorkflowDefinition {
            kind: WorkflowKind::Status,
            group: "Diagnostics",
            label: "status",
            detail: "Read runtime status, endpoints, and readiness.",
            aliases: &["health", "status"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::ConfigShow,
            group: "Diagnostics",
            label: "config show",
            detail: "Inspect the effective node configuration.",
            aliases: &["config", "show config"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::CollectionCreate,
            group: "Collections",
            label: "collection create",
            detail: "Create a collection with dimensions and metric.",
            aliases: &["create", "new collection"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::CollectionShow,
            group: "Collections",
            label: "collection show",
            detail: "Read collection metadata.",
            aliases: &["show", "describe"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::CollectionStats,
            group: "Collections",
            label: "collection stats",
            detail: "Show collection storage statistics.",
            aliases: &["stats", "metrics"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::CollectionPlacement,
            group: "Collections",
            label: "collection placement",
            detail: "Explain where a collection is placed.",
            aliases: &["placement", "routing"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::RecordPut,
            group: "Records",
            label: "record put",
            detail: "Write JSONL records into a collection.",
            aliases: &["put", "ingest", "upload"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::RecordDelete,
            group: "Records",
            label: "record delete",
            detail: "Delete one record from a collection.",
            aliases: &["delete", "remove"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::Query,
            group: "Query",
            label: "query",
            detail: "Run vector search with optional filters and predicates.",
            aliases: &["search", "find"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::InspectManifest,
            group: "Inspect",
            label: "inspect manifest",
            detail: "Inspect the active manifest.",
            aliases: &["manifest", "inspect"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::InspectWal,
            group: "Inspect",
            label: "inspect wal",
            detail: "Inspect WAL records above the checkpoint.",
            aliases: &["wal", "inspect"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::InspectMaintenance,
            group: "Inspect",
            label: "inspect maintenance",
            detail: "Inspect persisted maintenance state.",
            aliases: &["maintenance", "inspect"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::InspectSegment,
            group: "Inspect",
            label: "inspect segment",
            detail: "Inspect one immutable segment.",
            aliases: &["segment", "inspect"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::CollectionFlush,
            group: "Maintenance",
            label: "collection flush",
            detail: "Flush mutable data into an immutable segment.",
            aliases: &["flush"],
        },
        WorkflowDefinition {
            kind: WorkflowKind::CollectionCompact,
            group: "Maintenance",
            label: "collection compact",
            detail: "Compact immutable segments.",
            aliases: &["compact"],
        },
    ]
}

pub fn metric_choices() -> Vec<PickerChoice<MetricArg>> {
    vec![
        picker_choice(
            MetricArg::Dot,
            "dot",
            "Dot-product similarity.",
            &["dot-product", "similarity"],
        ),
        picker_choice(
            MetricArg::Cosine,
            "cosine",
            "Cosine similarity.",
            &["cosine similarity"],
        ),
        picker_choice(
            MetricArg::L2,
            "l2",
            "Euclidean distance.",
            &["euclidean", "distance"],
        ),
    ]
}

pub fn explain_choices() -> Vec<PickerChoice<Option<ExplainArg>>> {
    vec![
        picker_choice(
            None,
            "none",
            "Return only query results.",
            &["no diagnostics"],
        ),
        picker_choice(
            Some(ExplainArg::Plan),
            "plan",
            "Return the chosen plan and planner summary.",
            &["diagnostics", "plan"],
        ),
        picker_choice(
            Some(ExplainArg::Profile),
            "profile",
            "Return timings and planner counters.",
            &["diagnostics", "timings", "profile"],
        ),
    ]
}

pub fn workflow_choices() -> Vec<PickerChoice<WorkflowKind>> {
    workflow_definitions()
        .into_iter()
        .map(|definition| {
            picker_choice(
                definition.kind,
                definition.label,
                definition.detail,
                definition.aliases,
            )
        })
        .collect()
}

pub fn picker_choice<T: Clone>(
    value: T,
    label: &str,
    detail: &str,
    aliases: &[&str],
) -> PickerChoice<T> {
    let search_text = std::iter::once(label)
        .chain(std::iter::once(detail))
        .chain(aliases.iter().copied())
        .collect::<Vec<_>>()
        .join(" ");
    PickerChoice {
        value,
        label: label.to_owned(),
        detail: detail.to_owned(),
        search_text,
    }
}

pub fn rank_picker_choices<'a, T>(
    choices: &'a [PickerChoice<T>],
    query: &str,
    default_index: usize,
) -> Vec<&'a PickerChoice<T>> {
    let trimmed = query.trim();
    let mut ranked = choices
        .iter()
        .enumerate()
        .filter_map(|(index, choice)| {
            let score = if trimmed.is_empty() {
                10_000 - index as i64
            } else {
                fuzzy_score(&choice.search_text, trimmed)?
            };
            let default_bonus = if index == default_index { 250 } else { 0 };
            Some((score + default_bonus, index, choice))
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    ranked.into_iter().map(|(_, _, choice)| choice).collect()
}

pub fn rank_path_choices(paths: &[PathBuf], root: &Path, query: &str) -> Vec<PathChoice> {
    let trimmed = query.trim();
    let mut ranked = paths
        .iter()
        .filter_map(|path| {
            let display = relative_path(path, root);
            let search_text = format!(
                "{} {}",
                path.file_name()
                    .map(|name| name.to_string_lossy())
                    .unwrap_or_default(),
                display
            );
            let score = if trimmed.is_empty() {
                0
            } else {
                fuzzy_score(&search_text, trimmed)?
            };
            Some((
                score,
                PathChoice {
                    path: path.clone(),
                    display,
                },
            ))
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.display.cmp(&right.1.display))
    });
    ranked.into_iter().map(|(_, choice)| choice).collect()
}

pub fn fuzzy_score(candidate: &str, query: &str) -> Option<i64> {
    let candidate = candidate.to_ascii_lowercase();
    let query = query.to_ascii_lowercase();
    if query.is_empty() {
        return Some(0);
    }

    if let Some(position) = candidate.find(&query) {
        let prefix_bonus = if position == 0 { 400 } else { 0 };
        return Some(1_500 - position as i64 + prefix_bonus - candidate.len() as i64);
    }

    let mut score = 0i64;
    let mut last_index = None;
    let mut search_start = 0usize;
    for query_char in query.chars() {
        let haystack = &candidate[search_start..];
        let (relative_index, matched_char) = haystack
            .char_indices()
            .find(|(_, candidate_char)| *candidate_char == query_char)?;
        let absolute_index = search_start + relative_index;
        score += 20;
        if absolute_index == 0 {
            score += 30;
        }
        if let Some(previous) = last_index
            && absolute_index == previous + 1
        {
            score += 12;
        }
        last_index = Some(absolute_index);
        search_start = absolute_index + matched_char.len_utf8();
    }

    Some(score - candidate.len() as i64)
}

pub fn collect_picker_files(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| should_descend(entry.path()))
    {
        let entry = entry.context("failed to scan current directory for the file picker")?;
        if entry.file_type().is_file() {
            files.push(entry.path().to_path_buf());
            if files.len() >= FILE_PICKER_LIMIT {
                break;
            }
        }
    }
    files.sort();
    Ok(files)
}

pub fn should_descend(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return true;
    };
    !matches!(name, ".git" | ".worktrees" | "target" | "node_modules")
}

pub fn relative_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn snapshot_from_parts(
    manifest_generation: Option<u64>,
    visible_seq_no: Option<u64>,
    manifest_flag: &str,
    visible_seq_flag: &str,
) -> anyhow::Result<Option<Snapshot>> {
    match (manifest_generation, visible_seq_no) {
        (Some(manifest_generation), Some(visible_seq_no)) => Ok(Some(Snapshot {
            manifest_generation,
            visible_seq_no,
        })),
        (None, None) => Ok(None),
        _ => bail!("{manifest_flag} and {visible_seq_flag} must be provided together"),
    }
}

fn read_constraints_from_parts(
    snapshot_manifest_generation: Option<u64>,
    snapshot_visible_seq_no: Option<u64>,
    read_barrier_manifest_generation: Option<u64>,
    read_barrier_visible_seq_no: Option<u64>,
) -> anyhow::Result<(Option<Snapshot>, Option<Snapshot>)> {
    let snapshot = snapshot_from_parts(
        snapshot_manifest_generation,
        snapshot_visible_seq_no,
        "--snapshot-manifest-generation",
        "--snapshot-visible-seq-no",
    )?;
    let read_barrier = snapshot_from_parts(
        read_barrier_manifest_generation,
        read_barrier_visible_seq_no,
        "--read-barrier-manifest-generation",
        "--read-barrier-visible-seq-no",
    )?;
    if snapshot.is_some() && read_barrier.is_some() {
        bail!("--snapshot-* and --read-barrier-* cannot be provided together");
    }
    Ok((snapshot, read_barrier))
}

pub fn query_snapshot_from_action(action: &QueryAction) -> anyhow::Result<Option<Snapshot>> {
    let (snapshot, _) = read_constraints_from_parts(
        action.snapshot_manifest_generation,
        action.snapshot_visible_seq_no,
        action.read_barrier_manifest_generation,
        action.read_barrier_visible_seq_no,
    )?;
    Ok(snapshot)
}

pub fn query_read_barrier_from_action(action: &QueryAction) -> anyhow::Result<Option<Snapshot>> {
    let (_, read_barrier) = read_constraints_from_parts(
        action.snapshot_manifest_generation,
        action.snapshot_visible_seq_no,
        action.read_barrier_manifest_generation,
        action.read_barrier_visible_seq_no,
    )?;
    Ok(read_barrier)
}

pub fn stats_snapshot_from_action(
    action: &CollectionStatsAction,
) -> anyhow::Result<Option<Snapshot>> {
    let (snapshot, _) = read_constraints_from_parts(
        action.snapshot_manifest_generation,
        action.snapshot_visible_seq_no,
        action.read_barrier_manifest_generation,
        action.read_barrier_visible_seq_no,
    )?;
    Ok(snapshot)
}

pub fn stats_read_barrier_from_action(
    action: &CollectionStatsAction,
) -> anyhow::Result<Option<Snapshot>> {
    let (_, read_barrier) = read_constraints_from_parts(
        action.snapshot_manifest_generation,
        action.snapshot_visible_seq_no,
        action.read_barrier_manifest_generation,
        action.read_barrier_visible_seq_no,
    )?;
    Ok(read_barrier)
}

pub fn query_explain_mode_from_action(action: &QueryAction) -> ExplainMode {
    match action.explain {
        Some(ExplainArg::Plan) => ExplainMode::Plan,
        Some(ExplainArg::Profile) => ExplainMode::Profile,
        None => ExplainMode::None,
    }
}

/// The query `action` describes, its filter typed by `schema`.
pub fn query_request_from_action(
    action: &QueryAction,
    schema: &CollectionSchema,
) -> anyhow::Result<QueryRequest> {
    Ok(QueryRequest {
        vector: action.vector.as_ref().map(|vector| VectorQuery {
            field: action.vector_field.clone(),
            values: vector.0.clone(),
        }),
        filter: action.filter.resolve(schema)?,
        order_by: action.order_by.clone().into_iter().collect(),
        top_k: action.top_k,
        output_fields: action.output_fields.clone(),
        ef: action.ef,
        explain: query_explain_mode_from_action(action),
        read: ReadConsistency {
            snapshot: query_snapshot_from_action(action)?,
            read_barrier: query_read_barrier_from_action(action)?,
            snapshot_token: action.snapshot_token.clone(),
            pin: action.pin,
        },
    })
}

/// The count `action` describes, its filter typed by `schema`.
pub fn count_request_from_action(
    action: &CountAction,
    schema: &CollectionSchema,
) -> anyhow::Result<CountRecordsRequest> {
    Ok(CountRecordsRequest {
        filter: action.filter.resolve(schema)?,
        read: ReadConsistency {
            snapshot_token: action.snapshot_token.clone(),
            pin: action.pin,
            ..ReadConsistency::default()
        },
    })
}

/// The scroll page `action` describes, its filter typed by `schema`.
pub fn scroll_request_from_action(
    action: &ScrollAction,
    schema: &CollectionSchema,
) -> anyhow::Result<ScrollRecordsRequest> {
    Ok(ScrollRecordsRequest {
        filter: action.filter.resolve(schema)?,
        order_by: action.order_by.clone().into_iter().collect(),
        page_size: action.page_size,
        output_fields: action.output_fields.clone(),
        cursor: action.cursor.clone(),
        snapshot_token: action.snapshot_token.clone(),
    })
}

/// Parse `FIELD` or `FIELD:asc|desc`.
pub fn parse_order_by(value: &str) -> Result<OrderBy, String> {
    let (field, direction) = match value.rsplit_once(':') {
        Some((field, "asc")) => (field, SortDirection::Asc),
        Some((field, "desc")) => (field, SortDirection::Desc),
        Some((_, other)) => {
            return Err(format!(
                "unsupported order direction '{other}'; use asc or desc"
            ));
        }
        None => (value, SortDirection::Asc),
    };
    let field = field.trim();
    if field.is_empty() {
        return Err("order-by needs a field name".to_owned());
    }
    Ok(OrderBy {
        field: field.to_owned(),
        direction,
    })
}

/// One JSONL input line: its 1-based line number and the natural JSON document on it.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonlDocument {
    pub line: usize,
    pub document: Value,
}

/// Read a JSONL file of natural JSON documents, one record per line keyed by field name (such
/// as `{"id": "a", "vector": [0.1, 0.2], "color": "red"}`), into batches of at most
/// `max_batch_bytes` of input each.
pub fn read_jsonl_put_batches(
    path: &Path,
    max_batch_bytes: usize,
) -> anyhow::Result<Vec<Vec<JsonlDocument>>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open JSONL input '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut batches = Vec::new();
    let mut current_batch = Vec::new();
    let mut current_batch_bytes = 0usize;

    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "failed to read line {} from '{}'",
                index + 1,
                path.display()
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }

        let line_bytes = line.len() + 1;
        if line_bytes > max_batch_bytes {
            bail!(
                "JSONL record on line {} exceeds max_batch_bytes {} ({} bytes)",
                index + 1,
                max_batch_bytes,
                line_bytes
            );
        }
        if !current_batch.is_empty() && current_batch_bytes + line_bytes > max_batch_bytes {
            batches.push(current_batch);
            current_batch = Vec::new();
            current_batch_bytes = 0;
        }

        let document = serde_json::from_str::<Value>(&line)
            .with_context(|| format!("failed to parse JSONL record on line {}", index + 1))?;
        current_batch.push(JsonlDocument {
            line: index + 1,
            document,
        });
        current_batch_bytes += line_bytes;
    }

    if current_batch.is_empty() && batches.is_empty() {
        bail!(
            "JSONL input '{}' did not contain any records",
            path.display()
        );
    }

    if !current_batch.is_empty() {
        batches.push(current_batch);
    }

    Ok(batches)
}

pub fn read_database_policy_input(
    path: &Path,
    database_name: &str,
) -> anyhow::Result<DatabaseAccessPolicy> {
    let file = File::open(path)
        .with_context(|| format!("failed to open database policy input '{}'", path.display()))?;
    let policy = serde_json::from_reader::<_, DatabaseAccessPolicy>(file)
        .with_context(|| format!("failed to parse database policy JSON '{}'", path.display()))?;

    if policy.database_name != database_name {
        bail!(
            "database policy database_name '{}' does not match request database '{}'",
            policy.database_name,
            database_name
        );
    }

    policy
        .validate()
        .map_err(anyhow::Error::msg)
        .context("database policy input failed validation")?;

    Ok(policy)
}

pub fn parse_query_vector(value: &str) -> Result<QueryVector, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("vector must not be empty".to_owned());
    }

    trimmed
        .split(',')
        .map(|component| {
            component
                .trim()
                .parse::<f32>()
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(QueryVector)
}

pub fn parse_query_filter(value: &str) -> Result<QueryFilter, String> {
    let (field, raw_value) = value
        .split_once('=')
        .ok_or_else(|| "filters must use field=value syntax".to_owned())?;
    let field = field.trim();
    if field.is_empty() {
        return Err("filter field must not be empty".to_owned());
    }
    Ok(QueryFilter {
        field: field.to_owned(),
        value: parse_literal(raw_value.trim())?,
    })
}

pub fn parse_filter_list(value: &str) -> Result<Vec<QueryFilter>, String> {
    split_multi_value(value)
        .into_iter()
        .map(|item| parse_query_filter(&item))
        .collect()
}

/// Operators of a where clause.
const WHERE_OPERATORS: &str =
    "eq, ne, lt, lte, gt, gte, in, not_in, contains, contains_any, exists, is_null";

/// Parse `FIELD:OP[:VALUE]` into its natural JSON filter node.
pub fn parse_query_where(value: &str) -> Result<Value, String> {
    let mut parts = value.splitn(3, ':');
    let field = parts
        .next()
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .ok_or_else(|| "where clauses must use field:op:value syntax".to_owned())?;
    let operator = parts
        .next()
        .map(str::trim)
        .filter(|operator| !operator.is_empty())
        .ok_or_else(|| "where clauses must use field:op:value syntax".to_owned())?;
    let raw_value = parts.next().map(str::trim);
    match (operator, raw_value) {
        ("exists" | "is_null", None) => Ok(serde_json::json!({ operator: field })),
        ("exists" | "is_null", Some(_)) => Err(format!(
            "where operator '{operator}' does not accept a value"
        )),
        (
            "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "in" | "not_in" | "contains"
            | "contains_any",
            None,
        ) => Err(format!("where operator '{operator}' requires a value")),
        ("eq" | "ne" | "contains", Some(raw)) => {
            Ok(serde_json::json!({ operator: { field: parse_literal(raw)? } }))
        }
        ("lt" | "lte" | "gt" | "gte", Some(raw)) => Ok(serde_json::json!({
            "range": { field: { operator: parse_literal(raw)? } }
        })),
        ("in" | "not_in" | "contains_any", Some(raw)) => match parse_literal(raw)? {
            list @ Value::Array(_) => Ok(serde_json::json!({ operator: { field: list } })),
            _ => Err(format!(
                "where operator '{operator}' takes a JSON array, such as json:[1,2]"
            )),
        },
        _ => Err(format!(
            "unsupported where operator '{operator}'. Supported operators: {WHERE_OPERATORS}"
        )),
    }
}

pub fn parse_where_list(value: &str) -> Result<Vec<Value>, String> {
    split_multi_value(value)
        .into_iter()
        .map(|item| parse_query_where(&item))
        .collect()
}

fn split_multi_value(value: &str) -> Vec<String> {
    value
        .split(['\n', ';'])
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// A literal: `json:<JSON>` for any JSON value, anything else a string.
pub fn parse_literal(value: &str) -> Result<Value, String> {
    match value.strip_prefix("json:") {
        Some(raw_json) => serde_json::from_str::<Value>(raw_json)
            .map_err(|error| format!("invalid json filter value: {error}")),
        None => Ok(Value::String(value.to_owned())),
    }
}

/// Records typed by `schema` from JSONL documents; an invalid one is reported with its line.
pub fn records_from_documents(
    schema: &CollectionSchema,
    documents: Vec<JsonlDocument>,
) -> anyhow::Result<Vec<Record>> {
    documents
        .into_iter()
        .map(|JsonlDocument { line, document }| {
            Record::from_json(schema, document)
                .with_context(|| format!("JSONL record on line {line} does not fit the schema"))
        })
        .collect()
}

/// Read a typed collection schema: a JSON document in the shape of the REST create body. The
/// command line names the collection, so the document's `name` may be left out; when present it
/// must match.
pub fn read_collection_spec(path: &Path, name: &str) -> anyhow::Result<CreateCollectionSpec> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read schema file {}", path.display()))?;
    let mut document: Value = serde_json::from_str(&text)
        .with_context(|| format!("schema file {} is not valid JSON", path.display()))?;
    let Some(object) = document.as_object_mut() else {
        bail!("schema file {} must hold a JSON object", path.display());
    };
    match object.get("name").map(Value::as_str) {
        None => {
            object.insert("name".to_owned(), Value::from(name));
        }
        Some(Some(existing)) if existing == name => {}
        Some(_) => bail!(
            "schema file {} names a different collection than '{name}'",
            path.display()
        ),
    }
    serde_json::from_value(document).with_context(|| {
        format!(
            "schema file {} is not a valid collection schema",
            path.display()
        )
    })
}

/// Parse a `--change` argument: one schema change as JSON.
pub fn parse_schema_change(value: &str) -> Result<SchemaChange, String> {
    serde_json::from_str(value).map_err(|error| {
        format!(
            "expected one of {{\"add_field\":{{...}}}}, {{\"drop_field\":{{\"name\":...}}}}, or {{\"rename_field\":{{\"from\":...,\"to\":...}}}}: {error}"
        )
    })
}

/// A primary key from its command-line text, typed by the collection's key type.
pub fn primary_key_from_text(schema: &CollectionSchema, text: &str) -> anyhow::Result<PrimaryKey> {
    match schema.primary_key_type() {
        PrimaryKeyType::String => Ok(PrimaryKey::String(text.to_owned())),
        PrimaryKeyType::Int64 => text
            .trim()
            .parse::<i64>()
            .map(PrimaryKey::Int64)
            .with_context(|| {
                format!(
                    "primary key '{}' of type int64 must be an integer, got '{text}'",
                    schema.primary_key().name
                )
            }),
    }
}

pub fn format_command(action: &Action) -> String {
    let mut parts = vec!["logpose".to_owned()];
    match action {
        Action::Status => parts.push("status".to_owned()),
        Action::ConfigShow => {
            parts.push("config".to_owned());
            parts.push("show".to_owned());
        }
        Action::DatabaseList => {
            parts.push("database".to_owned());
            parts.push("list".to_owned());
        }
        Action::DatabaseShow { database_name } => {
            parts.push("database".to_owned());
            parts.push("show".to_owned());
            parts.push(shell_quote(database_name));
        }
        Action::DatabasePut(action) => {
            parts.push("database".to_owned());
            parts.push("put".to_owned());
            parts.push(shell_quote(&action.database_name));
        }
        Action::DatabasePolicyShow { database_name } => {
            parts.push("database".to_owned());
            parts.push("policy".to_owned());
            parts.push("show".to_owned());
            push_database_flag(&mut parts, database_name);
        }
        Action::DatabasePolicySet(action) => {
            parts.push("database".to_owned());
            parts.push("policy".to_owned());
            parts.push("set".to_owned());
            push_database_flag(&mut parts, &action.database_name);
            parts.push("--input".to_owned());
            parts.push(shell_quote(&action.input.to_string_lossy()));
        }
        Action::CollectionCreate(action) => {
            parts.push("collection".to_owned());
            parts.push("create".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            parts.push("--dimensions".to_owned());
            parts.push(action.dimensions.to_string());
            parts.push("--metric".to_owned());
            parts.push(metric_name(action.metric).to_owned());
        }
        Action::DatabaseDrop { database_name } => {
            parts.push("database".to_owned());
            parts.push("drop".to_owned());
            parts.push(shell_quote(database_name));
        }
        Action::CollectionCreateFromSchema(action) => {
            parts.push("collection".to_owned());
            parts.push("create".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            parts.push("--schema".to_owned());
            parts.push(shell_quote(&action.schema.to_string_lossy()));
        }
        Action::CollectionList { database_name } => {
            parts.push("collection".to_owned());
            parts.push("list".to_owned());
            push_database_flag(&mut parts, database_name);
        }
        Action::CollectionShow(collection) => {
            parts.push("collection".to_owned());
            parts.push("show".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_database_flag(&mut parts, &collection.database_name);
        }
        Action::CollectionAlter(action) => {
            parts.push("collection".to_owned());
            parts.push("alter".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            parts.push("--change".to_owned());
            parts.push(shell_quote(
                &serde_json::to_string(&action.change).unwrap_or_default(),
            ));
        }
        Action::CollectionDrop(collection) => {
            parts.push("collection".to_owned());
            parts.push("drop".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_database_flag(&mut parts, &collection.database_name);
        }
        Action::RecordGet(action) => {
            parts.push("record".to_owned());
            parts.push("get".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            parts.extend(action.keys.iter().map(|key| shell_quote(key)));
            for field in &action.output_fields {
                parts.push("--output-field".to_owned());
                parts.push(shell_quote(field));
            }
        }
        Action::CollectionStats(action) => {
            parts.push("collection".to_owned());
            parts.push("stats".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            if let Some(generation) = action.snapshot_manifest_generation {
                parts.push("--snapshot-manifest-generation".to_owned());
                parts.push(generation.to_string());
            }
            if let Some(seq_no) = action.snapshot_visible_seq_no {
                parts.push("--snapshot-visible-seq-no".to_owned());
                parts.push(seq_no.to_string());
            }
            if let Some(generation) = action.read_barrier_manifest_generation {
                parts.push("--read-barrier-manifest-generation".to_owned());
                parts.push(generation.to_string());
            }
            if let Some(seq_no) = action.read_barrier_visible_seq_no {
                parts.push("--read-barrier-visible-seq-no".to_owned());
                parts.push(seq_no.to_string());
            }
        }
        Action::CollectionPlacement(collection) => {
            parts.push("collection".to_owned());
            parts.push("placement".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_database_flag(&mut parts, &collection.database_name);
        }
        Action::CollectionFlush(collection) => {
            parts.push("collection".to_owned());
            parts.push("flush".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_database_flag(&mut parts, &collection.database_name);
        }
        Action::CollectionCompact(collection) => {
            parts.push("collection".to_owned());
            parts.push("compact".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_database_flag(&mut parts, &collection.database_name);
        }
        Action::RecordPut(action) => {
            parts.push("record".to_owned());
            parts.push("put".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            parts.push("--input".to_owned());
            parts.push(shell_quote(&action.input.to_string_lossy()));
        }
        Action::RecordDelete(action) => {
            parts.push("record".to_owned());
            parts.push("delete".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            if let Some(id) = &action.id {
                parts.push(shell_quote(id));
            }
            action.filter.push_flags(&mut parts);
        }
        Action::Count(action) => {
            parts.push("count".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            action.filter.push_flags(&mut parts);
            if let Some(token) = &action.snapshot_token {
                parts.push("--snapshot-token".to_owned());
                parts.push(shell_quote(token));
            }
            if action.pin {
                parts.push("--pin".to_owned());
            }
        }
        Action::Scroll(action) => {
            parts.push("scroll".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            action.filter.push_flags(&mut parts);
            push_order_by(&mut parts, action.order_by.as_ref());
            if let Some(page_size) = action.page_size {
                parts.push("--page-size".to_owned());
                parts.push(page_size.to_string());
            }
            for field in &action.output_fields {
                parts.push("--output-field".to_owned());
                parts.push(shell_quote(field));
            }
            if let Some(cursor) = &action.cursor {
                parts.push("--cursor".to_owned());
                parts.push(shell_quote(cursor));
            }
            if let Some(token) = &action.snapshot_token {
                parts.push("--snapshot-token".to_owned());
                parts.push(shell_quote(token));
            }
        }
        Action::Query(action) => {
            parts.push("query".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_database_flag(&mut parts, &action.collection.database_name);
            parts.push("--top-k".to_owned());
            parts.push(action.top_k.to_string());
            if let Some(vector) = &action.vector {
                parts.push("--vector".to_owned());
                parts.push(shell_quote(&format_vector(vector)));
            }
            if let Some(field) = &action.vector_field {
                parts.push("--vector-field".to_owned());
                parts.push(shell_quote(field));
            }
            action.filter.push_flags(&mut parts);
            push_order_by(&mut parts, action.order_by.as_ref());
            for field in &action.output_fields {
                parts.push("--output-field".to_owned());
                parts.push(shell_quote(field));
            }
            if let Some(ef) = action.ef {
                parts.push("--ef".to_owned());
                parts.push(ef.to_string());
            }
            if let Some(token) = &action.snapshot_token {
                parts.push("--snapshot-token".to_owned());
                parts.push(shell_quote(token));
            }
            if action.pin {
                parts.push("--pin".to_owned());
            }
            if let Some(explain) = action.explain {
                parts.push("--explain".to_owned());
                parts.push(match explain {
                    ExplainArg::Plan => "plan".to_owned(),
                    ExplainArg::Profile => "profile".to_owned(),
                });
            }
            if let Some(generation) = action.snapshot_manifest_generation {
                parts.push("--snapshot-manifest-generation".to_owned());
                parts.push(generation.to_string());
            }
            if let Some(seq_no) = action.snapshot_visible_seq_no {
                parts.push("--snapshot-visible-seq-no".to_owned());
                parts.push(seq_no.to_string());
            }
            if let Some(generation) = action.read_barrier_manifest_generation {
                parts.push("--read-barrier-manifest-generation".to_owned());
                parts.push(generation.to_string());
            }
            if let Some(seq_no) = action.read_barrier_visible_seq_no {
                parts.push("--read-barrier-visible-seq-no".to_owned());
                parts.push(seq_no.to_string());
            }
        }
        Action::Inspect { collection, target } => {
            parts.push("inspect".to_owned());
            match target {
                InspectTarget::Manifest => parts.push("manifest".to_owned()),
                InspectTarget::Wal => parts.push("wal".to_owned()),
                InspectTarget::Maintenance => parts.push("maintenance".to_owned()),
                InspectTarget::Segment(segment_id) => {
                    parts.push("segment".to_owned());
                    parts.push(shell_quote(&collection.collection_name));
                    parts.push(shell_quote(segment_id));
                    push_database_flag(&mut parts, &collection.database_name);
                    return parts.join(" ");
                }
            }
            parts.push(shell_quote(&collection.collection_name));
            push_database_flag(&mut parts, &collection.database_name);
        }
    }
    parts.join(" ")
}

pub fn database_descriptor(database_name: &str) -> DatabaseDescriptor {
    DatabaseDescriptor::new(database_name)
}

pub fn collection_lookup_name(database_name: &str, collection_name: &str) -> String {
    if database_name == DEFAULT_DATABASE_NAME {
        collection_name.to_owned()
    } else {
        format!("{database_name}/{collection_name}")
    }
}

pub fn split_collection_lookup_key(value: &str) -> (String, String) {
    let parts = value.split('/').collect::<Vec<_>>();
    if parts.len() == 2 && parts.iter().all(|part| !part.trim().is_empty()) {
        (parts[0].to_owned(), parts[1].to_owned())
    } else {
        (DEFAULT_DATABASE_NAME.to_owned(), value.to_owned())
    }
}

pub fn collection_ref_from_lookup_key(value: &str) -> CollectionRef {
    let (database_name, collection_name) = split_collection_lookup_key(value);
    CollectionRef::new(database_name, collection_name)
}

pub fn collection_ref_from_lookup_or_namespace(value: &str, database_name: &str) -> CollectionRef {
    let trimmed = value.trim();
    let parts = trimmed.split('/').collect::<Vec<_>>();
    if parts.len() == 2 && parts.iter().all(|part| !part.trim().is_empty()) {
        CollectionRef::new(parts[0], parts[1])
    } else {
        CollectionRef::new(database_name, trimmed)
    }
}

fn push_database_flag(parts: &mut Vec<String>, database_name: &str) {
    if database_name != DEFAULT_DATABASE_NAME {
        parts.push("--database".to_owned());
        parts.push(shell_quote(database_name));
    }
}

pub fn metric_name(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Dot => "dot",
        DistanceMetric::L2 => "l2",
    }
}

fn format_vector(vector: &QueryVector) -> String {
    vector
        .0
        .iter()
        .map(|component| format!("{component}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn format_filter(filter: &QueryFilter) -> String {
    format!("{}={}", filter.field, literal(&filter.value))
}

/// A where clause in its `FIELD:OP[:VALUE]` form, for the nodes [`parse_query_where`] builds;
/// any other node as compact JSON.
pub fn format_predicate(predicate: &Value) -> String {
    let single = |value: &Value| -> Option<(String, Value)> {
        let object = value.as_object()?;
        if object.len() != 1 {
            return None;
        }
        object
            .iter()
            .next()
            .map(|(key, value)| (key.clone(), value.clone()))
    };
    let formatted =
        single(predicate).and_then(|(operator, body)| match (operator.as_str(), body) {
            ("exists" | "is_null", Value::String(field)) => Some(format!("{field}:{operator}")),
            ("range", body) => {
                let (field, bounds) = single(&body)?;
                let (bound, value) = single(&bounds)?;
                Some(format!("{field}:{bound}:{}", literal(&value)))
            }
            (_, body) => {
                let (field, value) = single(&body)?;
                Some(format!("{field}:{operator}:{}", literal(&value)))
            }
        });
    formatted.unwrap_or_else(|| predicate.to_string())
}

fn literal(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => format!("json:{other}"),
    }
}

fn push_order_by(parts: &mut Vec<String>, order_by: Option<&OrderBy>) {
    if let Some(order) = order_by {
        parts.push("--order-by".to_owned());
        parts.push(shell_quote(&match order.direction {
            SortDirection::Asc => order.field.clone(),
            SortDirection::Desc => format!("{}:desc", order.field),
        }));
    }
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "-_./=:,".contains(character))
    {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', r#"'"'"'"#))
    }
}

impl CollectionCreateAction {
    /// A collection with string primary key `id`, one vector field `vector` of the requested
    /// dimensions and metric, and dynamic fields on.
    pub fn request(&self) -> CreateCollectionRequest {
        CreateCollectionRequest::in_database(
            self.collection.database_name.clone(),
            self.collection.collection_name.clone(),
            self.dimensions,
            self.metric,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_filter_values_are_preserved_as_strings() {
        let parsed = parse_query_filter("code=123").expect("filter should parse");

        assert_eq!(parsed.field, "code");
        assert_eq!(parsed.value, Value::String("123".to_owned()));
    }

    #[test]
    fn json_prefixed_filter_values_support_non_string_scalars() {
        let parsed = parse_query_filter("enabled=json:true").expect("filter should parse");

        assert_eq!(parsed.field, "enabled");
        assert_eq!(parsed.value, Value::Bool(true));
    }

    #[test]
    fn where_clauses_parse_to_natural_json_filters() {
        let cases = [
            (
                "score:gte:json:7",
                serde_json::json!({ "range": { "score": { "gte": 7 } } }),
            ),
            (
                "kind:eq:keep",
                serde_json::json!({ "eq": { "kind": "keep" } }),
            ),
            (
                "archived:is_null",
                serde_json::json!({ "is_null": "archived" }),
            ),
            (
                "tags:contains_any:json:[\"a\",\"b\"]",
                serde_json::json!({ "contains_any": { "tags": ["a", "b"] } }),
            ),
        ];
        for (clause, expected) in cases {
            let parsed = parse_query_where(clause).expect("where clause should parse");
            assert_eq!(parsed, expected, "{clause}");
            assert_eq!(format_predicate(&parsed), clause);
        }
        assert!(parse_query_where("kind:in:keep").is_err());
        assert!(parse_query_where("kind:like:keep").is_err());
        assert!(parse_query_where("kind:exists:keep").is_err());
    }

    #[test]
    fn filter_inputs_combine_with_and() {
        let input = FilterInput {
            filters: vec![parse_query_filter("kind=keep").expect("filter")],
            where_clauses: vec![parse_query_where("score:lt:json:3").expect("where")],
            filter_json: None,
        };
        assert_eq!(
            input.document().expect("document"),
            Some(serde_json::json!({ "and": [
                { "eq": { "kind": "keep" } },
                { "range": { "score": { "lt": 3 } } }
            ] }))
        );
        assert_eq!(FilterInput::default().document().expect("document"), None);
    }

    #[test]
    fn order_by_parses_a_field_and_an_optional_direction() {
        assert_eq!(
            parse_order_by("price:desc"),
            Ok(OrderBy {
                field: "price".to_owned(),
                direction: SortDirection::Desc,
            })
        );
        assert_eq!(
            parse_order_by("price"),
            Ok(OrderBy {
                field: "price".to_owned(),
                direction: SortDirection::Asc,
            })
        );
        assert!(parse_order_by("price:sideways").is_err());
    }

    #[test]
    fn read_jsonl_put_batches_splits_records_by_size_budget() {
        let path = std::env::temp_dir().join(format!(
            "logpose-cli-batch-test-{}.jsonl",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"{"id":"alpha","vector":[1.0],"label":"aaaaaaaaaaaaaaaaaaaaaaa"}
{"id":"beta","vector":[2.0],"label":"bbbbbbbbbbbbbbbbbbbbbbb"}"#,
        )
        .expect("jsonl should be written");

        let batches = read_jsonl_put_batches(&path, 70).expect("batches should parse");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 1);
        assert_eq!(batches[1].len(), 1);

        std::fs::remove_file(&path).expect("temp file should be removed");
    }

    #[test]
    fn read_jsonl_put_batches_rejects_oversized_first_record() {
        let path = std::env::temp_dir().join(format!(
            "logpose-cli-oversized-batch-test-{}.jsonl",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"{"id":"alpha","vector":[1.0],"label":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
        )
        .expect("jsonl should be written");

        let error = read_jsonl_put_batches(&path, 40).expect_err("oversized record should fail");

        assert!(error.to_string().contains("line 1"));
        assert!(error.to_string().contains("max_batch_bytes"));

        std::fs::remove_file(&path).expect("temp file should be removed");
    }

    #[test]
    fn fuzzy_score_prefers_exact_substring_matches() {
        let exact = fuzzy_score("record put", "record").expect("prefix match should work");
        let distant =
            fuzzy_score("write record batch", "record").expect("substring match should work");

        assert!(exact > distant);
    }

    #[test]
    fn rank_path_choices_prefers_filename_matches() {
        let root = Path::new("/tmp/logpose-tests");
        let ranked = rank_path_choices(
            &[
                root.join("records.jsonl"),
                root.join("nested/archive.jsonl"),
                root.join("notes.txt"),
            ],
            root,
            "records",
        );

        assert_eq!(ranked[0].display, "records.jsonl");
    }

    #[test]
    fn collection_ref_from_lookup_or_namespace_uses_fallback_for_bare_names() {
        let collection = collection_ref_from_lookup_or_namespace("documents", "analytics");

        assert_eq!(collection.database_name, "analytics");
        assert_eq!(collection.collection_name, "documents");
    }

    #[test]
    fn collection_lookup_name_uses_database_namespace_outside_the_default_database() {
        let lookup = collection_lookup_name("analytics", "documents");

        assert_eq!(lookup, "analytics/documents");
    }

    #[test]
    fn collection_ref_from_lookup_or_namespace_accepts_database_scoped_lookup_keys() {
        let collection = collection_ref_from_lookup_or_namespace("analytics/documents", "default");

        assert_eq!(collection.database_name, "analytics");
        assert_eq!(collection.collection_name, "documents");
    }

    #[test]
    fn format_command_emits_database_flags_for_non_default_collection_refs() {
        let command = format_command(&Action::RecordDelete(RecordDeleteAction {
            collection: CollectionRef::new("analytics", "documents"),
            id: Some("alpha".to_owned()),
            filter: FilterInput::default(),
        }));

        assert_eq!(
            command,
            "logpose record delete documents --database analytics alpha"
        );
        let by_filter = format_command(&Action::RecordDelete(RecordDeleteAction {
            collection: CollectionRef::new("analytics", "documents"),
            id: None,
            filter: FilterInput {
                filters: vec![parse_query_filter("kind=drop").expect("filter")],
                where_clauses: vec![parse_query_where("score:lt:json:3").expect("where")],
                filter_json: None,
            },
        }));
        assert_eq!(
            by_filter,
            "logpose record delete documents --database analytics --filter kind=drop --where score:lt:json:3"
        );
    }

    #[test]
    fn read_database_policy_input_rejects_database_mismatch() {
        let path = std::env::temp_dir().join(format!(
            "logpose-cli-policy-test-{}.json",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"{"database_name":"default","authentication_mode":"password","role_bindings":[]}"#,
        )
        .expect("policy json should be written");

        let error = read_database_policy_input(&path, "analytics")
            .expect_err("mismatched database should fail");

        assert!(error.to_string().contains("database_name"));

        std::fs::remove_file(&path).expect("temp file should be removed");
    }

    #[test]
    fn read_constraints_use_read_barrier_flag_names_in_pair_error() {
        let error = read_constraints_from_parts(None, None, Some(7), None)
            .expect_err("partial read barrier pair should fail");

        assert_eq!(
            error.to_string(),
            "--read-barrier-manifest-generation and --read-barrier-visible-seq-no must be provided together"
        );
    }
}
