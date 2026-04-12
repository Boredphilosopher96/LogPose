//! LogPose operator CLI.

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use logpose_query::{QueryRequest, query_exact};
use logpose_storage::{CreateCollectionRequest, InspectTarget, LocalStorageEngine, StorageEngine};
use logpose_types::{DeleteRecord, DistanceMetric, PutRecord, RecordId, WriteOperation};
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
    /// Print service endpoints and bootstrap metadata.
    Status,
}

#[derive(Debug, Subcommand)]
enum DataCommand {
    /// Create a new local collection.
    CreateCollection(CreateCollectionArgs),
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
}

#[derive(Clone, Debug)]
struct QueryVector(Vec<f32>);

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = logpose_config::LogPoseConfig::load().context("failed to load configuration")?;
    logpose_telemetry::init(&config.log_filter);
    let engine = LocalStorageEngine::new(&config.storage_root);

    match cli.command {
        Commands::Admin {
            command: AdminCommand::ShowConfig,
        } => {
            println!("{config:#?}");
        }
        Commands::Diagnostics {
            command: DiagnosticsCommand::Status,
        } => {
            println!(
                "LogPose node '{}' listening on REST {}:{} and gRPC {}:{}",
                config.node_name,
                config.rest_host,
                config.rest_port,
                config.grpc_host,
                config.grpc_port
            );
        }
        Commands::Data {
            command: DataCommand::CreateCollection(args),
        } => {
            let descriptor = engine
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
            command: DataCommand::Put(args),
        } => {
            let operations = read_jsonl_puts(&args.input)?;
            let ack = engine
                .write(&args.collection, operations)
                .await
                .context("failed to write records")?;
            print_json(&ack)?;
        }
        Commands::Data {
            command: DataCommand::Delete(args),
        } => {
            let ack = engine
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
            let snapshot = engine
                .flush(&args.collection)
                .await
                .context("failed to flush collection")?;
            print_json(&snapshot)?;
        }
        Commands::Data {
            command: DataCommand::Compact(args),
        } => {
            let snapshot = engine
                .compact(&args.collection)
                .await
                .context("failed to compact collection")?;
            print_json(&snapshot)?;
        }
        Commands::Data {
            command: DataCommand::Query(args),
        } => {
            let response = query_exact(
                &engine,
                QueryRequest {
                    collection_name: args.collection,
                    vector: args.vector.0,
                    top_k: args.top_k,
                    snapshot: None,
                },
            )
            .await
            .context("failed to query collection")?;
            print_json(&response)?;
        }
        Commands::Data {
            command: DataCommand::Stats(args),
        } => {
            let stats = engine
                .stats(&args.collection)
                .await
                .context("failed to read collection stats")?;
            print_json(&stats)?;
        }
        Commands::Data {
            command: DataCommand::Inspect(args),
        } => {
            let target = inspect_target_from_args(&args);
            let report = engine
                .inspect(&args.collection, target)
                .await
                .context("failed to inspect collection")?;
            print_json(&report)?;
        }
    }

    Ok(())
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

fn read_jsonl_puts(path: &Path) -> anyhow::Result<Vec<WriteOperation>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open JSONL input '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut operations = Vec::new();

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
        let record = serde_json::from_str::<JsonlRecord>(&line)
            .with_context(|| format!("failed to parse JSONL record on line {}", index + 1))?;
        operations.push(WriteOperation::Put(PutRecord {
            id: RecordId::new(record.id),
            vector: record.vector,
            metadata: record.metadata,
        }));
    }

    if operations.is_empty() {
        bail!(
            "JSONL input '{}' did not contain any records",
            path.display()
        );
    }

    Ok(operations)
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
