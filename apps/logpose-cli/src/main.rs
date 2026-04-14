//! LogPose operator CLI.

#[cfg(test)]
use logpose_api_grpc as _;
#[cfg(test)]
use logpose_api_rest as _;
#[cfg(test)]
use logpose_core as _;

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use logpose_client::LogPoseClient;
use logpose_query::{
    ExplainMode, MetadataFilter, Predicate, PredicateComparison, PredicateOperator, QueryRequest,
    ScalarMetadataValue,
};
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, Snapshot, WriteOperation};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
#[command(
    name = "logpose",
    version,
    about = "Operate LogPose clusters with fast diagnostics, administration, and data workflows."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Cluster and node administration commands.
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
    /// Health, topology, and runtime diagnostics.
    Diagnostics {
        #[command(subcommand)]
        command: DiagnosticsCommand,
    },
    /// Data movement and lifecycle operations.
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    /// Show the effective node configuration.
    ShowConfig,
}

#[derive(Debug, Subcommand)]
enum DiagnosticsCommand {
    /// Print runtime status, endpoints, and control-plane diagnostics.
    Status,
    /// Explain where a collection is placed.
    Placement(CollectionArgs),
}

#[derive(Debug, Subcommand)]
enum DataCommand {
    /// Create a new collection.
    CreateCollection(CreateCollectionArgs),
    /// Fetch collection metadata.
    GetCollection(CollectionArgs),
    /// Ingest JSONL records into a collection.
    Put(PutArgs),
    /// Tombstone a record id in a collection.
    Delete(DeleteArgs),
    /// Flush the mutable delta into an immutable segment.
    Flush(CollectionArgs),
    /// Compact immutable segments into a replacement segment.
    Compact(CollectionArgs),
    /// Query a collection with planner-controlled exact, ANN, or hybrid vector search.
    Query(QueryArgs),
    /// Show collection-level storage statistics.
    Stats(CollectionArgs),
    /// Inspect manifest, WAL, or a single segment.
    Inspect(InspectArgs),
}

#[derive(Debug, Args)]
struct CreateCollectionArgs {
    #[arg(long)]
    name: String,
    #[arg(long)]
    dimensions: usize,
    #[arg(long, value_parser = parse_distance_metric)]
    metric: DistanceMetric,
}

#[derive(Debug, Args)]
struct PutArgs {
    #[arg(long)]
    collection: String,
    #[arg(long)]
    input: PathBuf,
}

#[derive(Debug, Args)]
struct DeleteArgs {
    #[arg(long)]
    collection: String,
    #[arg(long)]
    id: String,
}

#[derive(Debug, Args)]
struct CollectionArgs {
    #[arg(long)]
    collection: String,
}

#[derive(Debug, Args)]
struct QueryArgs {
    #[arg(long)]
    collection: String,
    #[arg(long)]
    top_k: usize,
    #[arg(long, value_parser = parse_query_vector)]
    vector: QueryVector,
    #[arg(long = "filter", value_parser = parse_query_filter)]
    filters: Vec<QueryFilter>,
    #[arg(long = "where", value_parser = parse_query_where)]
    where_clauses: Vec<Predicate>,
    #[arg(long)]
    predicate_json: Option<PathBuf>,
    #[arg(long, conflicts_with = "profile")]
    explain: bool,
    #[arg(long, conflicts_with = "explain")]
    profile: bool,
    #[arg(long)]
    snapshot_manifest_generation: Option<u64>,
    #[arg(long)]
    snapshot_visible_seq_no: Option<u64>,
}

#[derive(Clone, Debug)]
struct QueryVector(Vec<f32>);

#[derive(Clone, Debug)]
struct QueryFilter {
    field: String,
    value: ScalarMetadataValue,
}

#[derive(Debug, Args)]
struct InspectArgs {
    #[arg(long)]
    collection: String,
    #[arg(long, conflicts_with_all = ["wal", "segment", "maintenance"])]
    manifest: bool,
    #[arg(long, conflicts_with_all = ["manifest", "segment", "maintenance"])]
    wal: bool,
    #[arg(long, conflicts_with_all = ["manifest", "wal", "maintenance"])]
    segment: Option<String>,
    #[arg(long, conflicts_with_all = ["manifest", "wal", "segment"])]
    maintenance: bool,
}

#[derive(Debug, Deserialize)]
struct JsonlRecord {
    id: String,
    vector: Vec<f32>,
    #[serde(default = "empty_object")]
    metadata: Value,
}

const CLI_PUT_BATCH_BYTES: usize = 1024 * 1024;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = logpose_config::LogPoseConfig::load().context("failed to load configuration")?;
    logpose_telemetry::init(&config.log_filter);

    match cli.command {
        Commands::Admin {
            command: AdminCommand::ShowConfig,
        } => {
            print_json(&config)?;
        }
        Commands::Diagnostics {
            command: DiagnosticsCommand::Status,
        } => {
            let client = connect_client(&config).await?;
            let status = client
                .runtime_status()
                .await
                .context("failed to fetch runtime status")?;
            print_json(&status)?;
        }
        Commands::Diagnostics {
            command: DiagnosticsCommand::Placement(args),
        } => {
            let client = connect_client(&config).await?;
            let placement = client
                .collection_placement(&args.collection)
                .await
                .context("failed to fetch collection placement")?;
            print_json(&placement)?;
        }
        Commands::Data {
            command: DataCommand::CreateCollection(args),
        } => {
            let client = connect_client(&config).await?;
            let descriptor = client
                .create_collection(CreateCollectionRequest {
                    name: args.name,
                    dimensions: args.dimensions,
                    metric: args.metric,
                })
                .await
                .context("failed to create collection")?;
            print_json(&descriptor)?;
        }
        Commands::Data {
            command: DataCommand::GetCollection(args),
        } => {
            let client = connect_client(&config).await?;
            let descriptor = client
                .get_collection(&args.collection)
                .await
                .context("failed to fetch collection")?;
            print_json(&descriptor)?;
        }
        Commands::Data {
            command: DataCommand::Put(args),
        } => {
            let batches = read_jsonl_put_batches(&args.input, CLI_PUT_BATCH_BYTES)?;
            let client = connect_client(&config).await?;
            let mut last_seq_no = 0;
            let mut applied_ops = 0;
            for operations in batches {
                let ack = client
                    .write(&args.collection, operations)
                    .await
                    .context("failed to write records")?;
                last_seq_no = ack.last_seq_no;
                applied_ops += ack.applied_ops;
            }
            let ack = logpose_types::CommitAck {
                last_seq_no,
                applied_ops,
            };
            print_json(&ack)?;
        }
        Commands::Data {
            command: DataCommand::Delete(args),
        } => {
            let client = connect_client(&config).await?;
            let ack = client
                .write(
                    &args.collection,
                    vec![WriteOperation::Delete(DeleteRecord {
                        id: RecordId::new(args.id),
                    })],
                )
                .await
                .context("failed to delete record")?;
            print_json(&ack)?;
        }
        Commands::Data {
            command: DataCommand::Flush(args),
        } => {
            let client = connect_client(&config).await?;
            let snapshot = client
                .flush(&args.collection)
                .await
                .context("failed to flush collection")?;
            print_json(&snapshot)?;
        }
        Commands::Data {
            command: DataCommand::Compact(args),
        } => {
            let client = connect_client(&config).await?;
            let snapshot = client
                .compact(&args.collection)
                .await
                .context("failed to compact collection")?;
            print_json(&snapshot)?;
        }
        Commands::Data {
            command: DataCommand::Query(args),
        } => {
            let snapshot = query_snapshot_from_args(&args)?;
            let predicate = query_predicate_from_args(&args)?;
            let explain = query_explain_mode_from_args(&args);
            let filters = args
                .filters
                .into_iter()
                .map(|filter| MetadataFilter {
                    field: filter.field,
                    value: filter.value,
                })
                .collect();
            let client = connect_client(&config).await?;
            let response = client
                .query(QueryRequest {
                    collection_name: args.collection,
                    vector: args.vector.0,
                    top_k: args.top_k,
                    snapshot,
                    filters,
                    predicate,
                    explain,
                })
                .await
                .context("failed to query collection")?;
            print_json(&response)?;
        }
        Commands::Data {
            command: DataCommand::Stats(args),
        } => {
            let client = connect_client(&config).await?;
            let stats = client
                .stats(&args.collection)
                .await
                .context("failed to read collection stats")?;
            print_json(&stats)?;
        }
        Commands::Data {
            command: DataCommand::Inspect(args),
        } => {
            let client = connect_client(&config).await?;
            let target = inspect_target_from_args(&args);
            let report = client
                .inspect(&args.collection, target)
                .await
                .context("failed to inspect collection")?;
            print_json(&report)?;
        }
    }

    Ok(())
}

async fn connect_client(config: &logpose_config::LogPoseConfig) -> anyhow::Result<LogPoseClient> {
    let endpoint = grpc_dial_endpoint(config);
    LogPoseClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("failed to connect to {endpoint}"))
}

#[cfg(test)]
fn rest_endpoint(config: &logpose_config::LogPoseConfig) -> String {
    endpoint_url(&config.rest_host, config.rest_port)
}

#[cfg(test)]
fn rest_dial_endpoint(config: &logpose_config::LogPoseConfig) -> String {
    dial_endpoint_url(&config.rest_host, config.rest_port)
}

#[cfg(test)]
fn grpc_endpoint(config: &logpose_config::LogPoseConfig) -> String {
    endpoint_url(&config.grpc_host, config.grpc_port)
}

fn grpc_dial_endpoint(config: &logpose_config::LogPoseConfig) -> String {
    dial_endpoint_url(&config.grpc_host, config.grpc_port)
}

fn endpoint_url(host: &str, port: u16) -> String {
    let authority = match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]"),
        Ok(std::net::IpAddr::V4(_)) | Err(_) => host.to_owned(),
    };

    format!("http://{authority}:{port}")
}

fn dial_endpoint_url(host: &str, port: u16) -> String {
    let dial_host = match host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "::1",
        _ => host,
    };
    endpoint_url(dial_host, port)
}

fn inspect_target_from_args(args: &InspectArgs) -> InspectTarget {
    if args.wal {
        InspectTarget::Wal
    } else if args.maintenance {
        InspectTarget::Maintenance
    } else if let Some(segment_id) = &args.segment {
        InspectTarget::Segment(segment_id.clone())
    } else {
        InspectTarget::Manifest
    }
}

fn query_snapshot_from_args(args: &QueryArgs) -> anyhow::Result<Option<Snapshot>> {
    match (
        args.snapshot_manifest_generation,
        args.snapshot_visible_seq_no,
    ) {
        (Some(manifest_generation), Some(visible_seq_no)) => Ok(Some(Snapshot {
            manifest_generation,
            visible_seq_no,
        })),
        (None, None) => Ok(None),
        _ => bail!(
            "snapshot_manifest_generation and snapshot_visible_seq_no must be provided together"
        ),
    }
}

fn query_predicate_from_args(args: &QueryArgs) -> anyhow::Result<Option<Predicate>> {
    let mut predicates = args.where_clauses.clone();
    if let Some(path) = &args.predicate_json {
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

fn query_explain_mode_from_args(args: &QueryArgs) -> ExplainMode {
    if args.profile {
        ExplainMode::Profile
    } else if args.explain {
        ExplainMode::Plan
    } else {
        ExplainMode::None
    }
}

fn parse_distance_metric(value: &str) -> Result<DistanceMetric, String> {
    value
        .parse::<DistanceMetric>()
        .map_err(|error| error.to_string())
}

fn parse_query_vector(value: &str) -> Result<QueryVector, String> {
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

fn parse_query_filter(value: &str) -> Result<QueryFilter, String> {
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

fn parse_query_where(value: &str) -> Result<Predicate, String> {
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

fn parse_predicate_operator(value: &str) -> Result<PredicateOperator, String> {
    match value {
        "eq" => Ok(PredicateOperator::Eq),
        "ne" => Ok(PredicateOperator::Ne),
        "lt" => Ok(PredicateOperator::Lt),
        "lte" => Ok(PredicateOperator::Lte),
        "gt" => Ok(PredicateOperator::Gt),
        "gte" => Ok(PredicateOperator::Gte),
        "exists" => Ok(PredicateOperator::Exists),
        "is_null" => Ok(PredicateOperator::IsNull),
        _ => Err(format!("unsupported where operator '{value}'")),
    }
}

fn operator_name(operator: PredicateOperator) -> &'static str {
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

fn parse_scalar_metadata_value(value: &str) -> Result<ScalarMetadataValue, String> {
    let json_value = if let Some(raw_json) = value.strip_prefix("json:") {
        serde_json::from_str::<Value>(raw_json)
            .map_err(|error| format!("invalid json: filter value: {error}"))?
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

fn read_jsonl_put_batches(
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

fn print_json<T>(value: &T) -> anyhow::Result<()>
where
    T: serde::Serialize,
{
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("failed to serialize JSON output")?
    );
    Ok(())
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_helpers_bracket_ipv6_hosts() {
        let config = logpose_config::LogPoseConfig {
            rest_host: "::1".to_owned(),
            rest_port: 18080,
            grpc_host: "::1".to_owned(),
            grpc_port: 15051,
            ..logpose_config::LogPoseConfig::default()
        };

        assert_eq!(rest_endpoint(&config), "http://[::1]:18080");
        assert_eq!(grpc_endpoint(&config), "http://[::1]:15051");
    }

    #[test]
    fn dial_endpoint_helpers_rewrite_wildcard_bind_addresses() {
        let config = logpose_config::LogPoseConfig {
            rest_host: "0.0.0.0".to_owned(),
            rest_port: 18080,
            grpc_host: "::".to_owned(),
            grpc_port: 15051,
            ..logpose_config::LogPoseConfig::default()
        };

        assert_eq!(rest_dial_endpoint(&config), "http://127.0.0.1:18080");
        assert_eq!(grpc_dial_endpoint(&config), "http://[::1]:15051");
    }

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
}
