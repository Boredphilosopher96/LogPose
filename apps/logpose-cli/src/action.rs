use anyhow::{Context, bail};
use clap::ValueEnum;
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator, QueryRequest,
    ScalarMetadataValue,
};
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{
    CollectionRef, DEFAULT_DATABASE_NAME, DEFAULT_TENANT_NAME, DeleteRecord, DistanceMetric,
    PutRecord, RecordId, Snapshot, WriteOperation,
};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fs::File,
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

#[derive(Clone, Debug, PartialEq)]
pub struct QueryFilter {
    pub field: String,
    pub value: ScalarMetadataValue,
}

#[derive(Debug, Deserialize)]
pub struct JsonlRecord {
    pub id: String,
    pub vector: Vec<f32>,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectionCreateAction {
    pub collection: CollectionRef,
    pub dimensions: usize,
    pub metric: DistanceMetric,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordPutAction {
    pub collection: CollectionRef,
    pub input: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordDeleteAction {
    pub collection: CollectionRef,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryAction {
    pub collection: CollectionRef,
    pub top_k: usize,
    pub vector: QueryVector,
    pub filters: Vec<QueryFilter>,
    pub where_clauses: Vec<Predicate>,
    pub predicate_json: Option<PathBuf>,
    pub explain: Option<ExplainArg>,
    pub snapshot_manifest_generation: Option<u64>,
    pub snapshot_visible_seq_no: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Status,
    ConfigShow,
    CollectionCreate(CollectionCreateAction),
    CollectionShow(CollectionRef),
    CollectionStats(CollectionRef),
    CollectionPlacement(CollectionRef),
    CollectionFlush(CollectionRef),
    CollectionCompact(CollectionRef),
    RecordPut(RecordPutAction),
    RecordDelete(RecordDeleteAction),
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

pub fn query_snapshot_from_action(action: &QueryAction) -> anyhow::Result<Option<Snapshot>> {
    match (
        action.snapshot_manifest_generation,
        action.snapshot_visible_seq_no,
    ) {
        (Some(manifest_generation), Some(visible_seq_no)) => Ok(Some(Snapshot {
            manifest_generation,
            visible_seq_no,
        })),
        (None, None) => Ok(None),
        _ => bail!(
            "--snapshot-manifest-generation and --snapshot-visible-seq-no must be provided together"
        ),
    }
}

pub fn query_predicate_from_action(action: &QueryAction) -> anyhow::Result<Option<Predicate>> {
    let mut predicates = action.where_clauses.clone();
    if let Some(path) = &action.predicate_json {
        let file = File::open(path)
            .with_context(|| format!("failed to open predicate json '{}'", path.display()))?;
        let predicate = serde_json::from_reader::<_, Predicate>(file)
            .with_context(|| format!("failed to parse predicate json '{}'", path.display()))?;
        predicates.push(predicate);
    }

    Ok(match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(Predicate::And {
            children: predicates,
        }),
    })
}

pub fn query_explain_mode_from_action(action: &QueryAction) -> ExplainMode {
    match action.explain {
        Some(ExplainArg::Plan) => ExplainMode::Plan,
        Some(ExplainArg::Profile) => ExplainMode::Profile,
        None => ExplainMode::None,
    }
}

pub fn query_request_from_action(action: &QueryAction) -> anyhow::Result<QueryRequest> {
    let snapshot = query_snapshot_from_action(action)?;
    let predicate = query_predicate_from_action(action)?;
    let explain = query_explain_mode_from_action(action);
    let filters = action
        .filters
        .iter()
        .cloned()
        .map(|filter| MetadataFilter {
            field: filter.field,
            value: filter.value,
        })
        .collect();
    Ok(QueryRequest {
        collection_name: action.collection.collection_name.clone(),
        vector: action.vector.0.clone(),
        top_k: action.top_k,
        snapshot,
        filters,
        predicate,
        explain,
    })
}

pub fn read_jsonl_put_batches(
    path: &Path,
    max_batch_bytes: usize,
) -> anyhow::Result<Vec<Vec<WriteOperation>>> {
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

        let record = serde_json::from_str::<JsonlRecord>(&line)
            .with_context(|| format!("failed to parse JSONL record on line {}", index + 1))?;
        current_batch.push(WriteOperation::Put(PutRecord {
            id: RecordId::new(record.id),
            vector: record.vector,
            metadata: record.metadata,
        }));
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

    let scalar = parse_scalar_metadata_value(raw_value.trim())?;
    Ok(QueryFilter {
        field: field.to_owned(),
        value: scalar,
    })
}

pub fn parse_filter_list(value: &str) -> Result<Vec<QueryFilter>, String> {
    split_multi_value(value)
        .into_iter()
        .map(|item| parse_query_filter(&item))
        .collect()
}

pub fn parse_query_where(value: &str) -> Result<Predicate, String> {
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

    let operator = parse_predicate_operator(operator)?;
    let value = match operator {
        PredicateOperator::Exists | PredicateOperator::IsNull => {
            if raw_value.is_some() {
                return Err(format!(
                    "where operator '{}' does not accept a value",
                    operator_name(operator)
                ));
            }
            None
        }
        _ => {
            let raw_value = raw_value.ok_or_else(|| {
                format!(
                    "where operator '{}' requires a value",
                    operator_name(operator)
                )
            })?;
            Some(parse_scalar_metadata_value(raw_value)?)
        }
    };

    Ok(Predicate::Comparison(PredicateComparison {
        field: field.to_owned(),
        operator,
        value,
    }))
}

pub fn parse_where_list(value: &str) -> Result<Vec<Predicate>, String> {
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

pub fn parse_predicate_operator(value: &str) -> Result<PredicateOperator, String> {
    match value {
        "eq" => Ok(PredicateOperator::Eq),
        "ne" => Ok(PredicateOperator::Ne),
        "lt" => Ok(PredicateOperator::Lt),
        "lte" => Ok(PredicateOperator::Lte),
        "gt" => Ok(PredicateOperator::Gt),
        "gte" => Ok(PredicateOperator::Gte),
        "exists" => Ok(PredicateOperator::Exists),
        "is_null" => Ok(PredicateOperator::IsNull),
        _ => Err(format!(
            "unsupported where operator '{value}'. Supported operators: eq, ne, lt, lte, gt, gte, exists, is_null"
        )),
    }
}

pub fn operator_name(operator: PredicateOperator) -> &'static str {
    match operator {
        PredicateOperator::Eq => "eq",
        PredicateOperator::Ne => "ne",
        PredicateOperator::Lt => "lt",
        PredicateOperator::Lte => "lte",
        PredicateOperator::Gt => "gt",
        PredicateOperator::Gte => "gte",
        PredicateOperator::Exists => "exists",
        PredicateOperator::IsNull => "is_null",
    }
}

pub fn parse_scalar_metadata_value(value: &str) -> Result<ScalarMetadataValue, String> {
    let json_value = if let Some(raw_json) = value.strip_prefix("json:") {
        serde_json::from_str::<Value>(raw_json)
            .map_err(|error| format!("invalid json filter value: {error}"))?
    } else {
        Value::String(value.to_owned())
    };
    scalar_metadata_value_from_json(&json_value)
        .ok_or_else(|| "query filters must contain only scalar JSON values".to_owned())
}

fn scalar_metadata_value_from_json(value: &Value) -> Option<ScalarMetadataValue> {
    match value {
        Value::String(value) => Some(ScalarMetadataValue::String(value.clone())),
        Value::Number(value) => Some(ScalarMetadataValue::Number(value.clone())),
        Value::Bool(value) => Some(ScalarMetadataValue::Bool(*value)),
        Value::Null => Some(ScalarMetadataValue::Null),
        Value::Array(_) | Value::Object(_) => None,
    }
}

pub fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

pub fn format_command(action: &Action) -> String {
    let mut parts = vec!["logpose".to_owned()];
    match action {
        Action::Status => parts.push("status".to_owned()),
        Action::ConfigShow => {
            parts.push("config".to_owned());
            parts.push("show".to_owned());
        }
        Action::CollectionCreate(action) => {
            parts.push("collection".to_owned());
            parts.push("create".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &action.collection.tenant_name,
                &action.collection.database_name,
            );
            parts.push("--dimensions".to_owned());
            parts.push(action.dimensions.to_string());
            parts.push("--metric".to_owned());
            parts.push(metric_name(action.metric).to_owned());
        }
        Action::CollectionShow(collection) => {
            parts.push("collection".to_owned());
            parts.push("show".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &collection.tenant_name,
                &collection.database_name,
            );
        }
        Action::CollectionStats(collection) => {
            parts.push("collection".to_owned());
            parts.push("stats".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &collection.tenant_name,
                &collection.database_name,
            );
        }
        Action::CollectionPlacement(collection) => {
            parts.push("collection".to_owned());
            parts.push("placement".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &collection.tenant_name,
                &collection.database_name,
            );
        }
        Action::CollectionFlush(collection) => {
            parts.push("collection".to_owned());
            parts.push("flush".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &collection.tenant_name,
                &collection.database_name,
            );
        }
        Action::CollectionCompact(collection) => {
            parts.push("collection".to_owned());
            parts.push("compact".to_owned());
            parts.push(shell_quote(&collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &collection.tenant_name,
                &collection.database_name,
            );
        }
        Action::RecordPut(action) => {
            parts.push("record".to_owned());
            parts.push("put".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &action.collection.tenant_name,
                &action.collection.database_name,
            );
            parts.push("--input".to_owned());
            parts.push(shell_quote(&action.input.to_string_lossy()));
        }
        Action::RecordDelete(action) => {
            parts.push("record".to_owned());
            parts.push("delete".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &action.collection.tenant_name,
                &action.collection.database_name,
            );
            parts.push(shell_quote(&action.id));
        }
        Action::Query(action) => {
            parts.push("query".to_owned());
            parts.push(shell_quote(&action.collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &action.collection.tenant_name,
                &action.collection.database_name,
            );
            parts.push("--top-k".to_owned());
            parts.push(action.top_k.to_string());
            parts.push("--vector".to_owned());
            parts.push(shell_quote(&format_vector(&action.vector)));
            for filter in &action.filters {
                parts.push("--filter".to_owned());
                parts.push(shell_quote(&format_filter(filter)));
            }
            for predicate in &action.where_clauses {
                parts.push("--where".to_owned());
                parts.push(shell_quote(&format_predicate(predicate)));
            }
            if let Some(path) = &action.predicate_json {
                parts.push("--predicate-json".to_owned());
                parts.push(shell_quote(&path.to_string_lossy()));
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
                    push_namespace_flags(
                        &mut parts,
                        &collection.tenant_name,
                        &collection.database_name,
                    );
                    return parts.join(" ");
                }
            }
            parts.push(shell_quote(&collection.collection_name));
            push_namespace_flags(
                &mut parts,
                &collection.tenant_name,
                &collection.database_name,
            );
        }
    }
    parts.join(" ")
}

pub fn collection_lookup_name(
    tenant_name: &str,
    database_name: &str,
    collection_name: &str,
) -> String {
    if tenant_name == DEFAULT_TENANT_NAME && database_name == DEFAULT_DATABASE_NAME {
        collection_name.to_owned()
    } else {
        format!("{tenant_name}/{database_name}/{collection_name}")
    }
}

pub fn split_collection_lookup_key(value: &str) -> (String, String, String) {
    let collection = CollectionRef::from_lookup_key(value);
    (
        collection.tenant_name,
        collection.database_name,
        collection.collection_name,
    )
}

pub fn collection_ref_from_lookup_key(value: &str) -> CollectionRef {
    CollectionRef::from_lookup_key(value)
}

pub fn collection_ref_from_lookup_or_namespace(
    value: &str,
    tenant_name: &str,
    database_name: &str,
) -> CollectionRef {
    CollectionRef::from_lookup_key_or(value.trim(), tenant_name, database_name)
}

fn push_namespace_flags(parts: &mut Vec<String>, tenant_name: &str, database_name: &str) {
    if tenant_name != DEFAULT_TENANT_NAME {
        parts.push("--tenant".to_owned());
        parts.push(shell_quote(tenant_name));
    }
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
    format!(
        "{}={}",
        filter.field,
        scalar_value_to_cli_literal(&filter.value)
    )
}

pub fn format_predicate(predicate: &Predicate) -> String {
    match predicate {
        Predicate::Comparison(PredicateComparison {
            field,
            operator,
            value,
        }) => match value {
            Some(value) => format!(
                "{field}:{}:{}",
                operator_name(*operator),
                scalar_value_to_cli_literal(value)
            ),
            None => format!("{field}:{}", operator_name(*operator)),
        },
        _ => "predicate.json".to_owned(),
    }
}

fn scalar_value_to_cli_literal(value: &ScalarMetadataValue) -> String {
    match value {
        ScalarMetadataValue::String(value) => value.clone(),
        ScalarMetadataValue::Number(value) => format!("json:{value}"),
        ScalarMetadataValue::Bool(value) => format!("json:{value}"),
        ScalarMetadataValue::Null => "json:null".to_owned(),
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
    pub fn request(&self) -> CreateCollectionRequest {
        CreateCollectionRequest {
            tenant_name: self.collection.tenant_name.clone(),
            database_name: self.collection.database_name.clone(),
            name: self.collection.collection_name.clone(),
            dimensions: self.dimensions,
            metric: self.metric,
        }
    }
}

impl RecordDeleteAction {
    pub fn operation(&self) -> WriteOperation {
        WriteOperation::Delete(DeleteRecord {
            id: RecordId::new(self.id.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_filter_values_are_preserved_as_strings() {
        let parsed = parse_query_filter("code=123").expect("filter should parse");

        assert_eq!(parsed.field, "code");
        assert_eq!(parsed.value, ScalarMetadataValue::String("123".to_owned()));
    }

    #[test]
    fn json_prefixed_filter_values_support_non_string_scalars() {
        let parsed = parse_query_filter("enabled=json:true").expect("filter should parse");

        assert_eq!(parsed.field, "enabled");
        assert_eq!(parsed.value, ScalarMetadataValue::Bool(true));
    }

    #[test]
    fn where_clauses_parse_scalar_comparisons() {
        let parsed = parse_query_where("score:gte:json:7").expect("where clause should parse");

        assert_eq!(
            parsed,
            Predicate::Comparison(PredicateComparison {
                field: "score".to_owned(),
                operator: PredicateOperator::Gte,
                value: Some(ScalarMetadataValue::Number(7.into())),
            })
        );
    }

    #[test]
    fn where_clauses_parse_unary_operators_without_values() {
        let parsed = parse_query_where("archived:is_null").expect("where clause should parse");

        assert_eq!(
            parsed,
            Predicate::Comparison(PredicateComparison {
                field: "archived".to_owned(),
                operator: PredicateOperator::IsNull,
                value: None,
            })
        );
    }

    #[test]
    fn read_jsonl_put_batches_splits_records_by_size_budget() {
        let path = std::env::temp_dir().join(format!(
            "logpose-cli-batch-test-{}.jsonl",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"{"id":"alpha","vector":[1.0],"metadata":{"label":"aaaaaaaaaa"}}
{"id":"beta","vector":[2.0],"metadata":{"label":"bbbbbbbbbb"}}"#,
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
            r#"{"id":"alpha","vector":[1.0],"metadata":{"label":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}"#,
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
        let collection = collection_ref_from_lookup_or_namespace("documents", "acme", "analytics");

        assert_eq!(collection.tenant_name, "acme");
        assert_eq!(collection.database_name, "analytics");
        assert_eq!(collection.collection_name, "documents");
    }

    #[test]
    fn format_command_emits_namespace_flags_for_non_default_collection_refs() {
        let command = format_command(&Action::RecordDelete(RecordDeleteAction {
            collection: CollectionRef::new("acme", "analytics", "documents"),
            id: "alpha".to_owned(),
        }));

        assert_eq!(
            command,
            "logpose record delete documents --tenant acme --database analytics alpha"
        );
    }
}
