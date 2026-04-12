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
use logpose_query::{MetadataFilter, QueryRequest, ScalarMetadataValue};
use logpose_storage::{CreateCollectionRequest, InspectTarget};
use logpose_types::{
    DeleteRecord, DistanceMetric, NodeMetadata, PutRecord, RecordId, Snapshot, WriteOperation,
};
use serde::{Deserialize, Serialize};
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
    /// Print service endpoints and bootstrap metadata.
    Status,
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
    /// Query a collection with an exact vector search.
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
    #[arg(long, conflicts_with_all = ["wal", "segment"])]
    manifest: bool,
    #[arg(long, conflicts_with_all = ["manifest", "segment"])]
    wal: bool,
    #[arg(long, conflicts_with_all = ["manifest", "wal"])]
    segment: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonlRecord {
    id: String,
    vector: Vec<f32>,
    #[serde(default = "empty_object")]
    metadata: Value,
}

#[derive(Debug, Serialize)]
struct DiagnosticsStatus {
    #[serde(flatten)]
    metadata: NodeMetadata,
    rest_endpoint: String,
    grpc_endpoint: String,
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
            let metadata = client
                .metadata()
                .await
                .context("failed to fetch server metadata")?;
            print_json(&DiagnosticsStatus {
                metadata,
                rest_endpoint: rest_endpoint(&config),
                grpc_endpoint: grpc_endpoint(&config),
            })?;
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
    LogPoseClient::connect(grpc_endpoint(config))
        .await
        .with_context(|| format!("failed to connect to {}", grpc_endpoint(config)))
}

fn rest_endpoint(config: &logpose_config::LogPoseConfig) -> String {
    endpoint_url(&config.rest_host, config.rest_port)
}

fn grpc_endpoint(config: &logpose_config::LogPoseConfig) -> String {
    endpoint_url(&config.grpc_host, config.grpc_port)
}

fn endpoint_url(host: &str, port: u16) -> String {
    let authority = match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]"),
        Ok(std::net::IpAddr::V4(_)) | Err(_) => host.to_owned(),
    };

    format!("http://{authority}:{port}")
}

fn inspect_target_from_args(args: &InspectArgs) -> InspectTarget {
    if args.wal {
        InspectTarget::Wal
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
}
