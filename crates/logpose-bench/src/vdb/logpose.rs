//! The LogPose driver: loads a prepared dataset into a running server over the
//! `logpose.v2` gRPC API and runs the VectorDBBench-style cases against it.
//!
//! Every search goes over the network through the generated gRPC client, so the
//! numbers include serialization and transport, as they do for the Milvus
//! driver. Inserts use one `BulkUpsertRecords` stream; each concurrent search
//! client opens its own connection.

use super::{
    files::{CaseSpec, Prepared},
    report::{CaseResult, ConcurrencyResult, LoadReport, SweepPoint},
};
use crate::metrics::{LatencySummary, recall_at_k};
use anyhow::{Context, Result, anyhow, bail, ensure};
use logpose_api_grpc::proto::{
    self, BulkUpsertRecordsRequest, CompactCollectionRequest, CreateCollectionRequest,
    DropCollectionRequest, FieldRange, Filter, FlushCollectionRequest, GetCollectionRequest,
    GetCollectionStatsRequest, GetDatabaseRequest, GetMetadataRequest, PrimaryKey, PrimaryKeySpec,
    PutDatabaseRequest, QueryCollectionRequest, Record, ScalarFieldSpec, Value, Vector,
    VectorFieldSpec, VectorQuery, log_pose_service_client::LogPoseServiceClient,
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::{Code, transport::Channel};

/// Name of the vector field.
const VECTOR_FIELD: &str = "embedding";
/// Name of the primary key field.
const PK_FIELD: &str = "id";
/// How long to wait for background maintenance to settle after a load.
const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(1_800);
/// Largest reply the driver accepts.
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Where and how to run.
#[derive(Clone, Debug)]
pub struct DriverConfig {
    /// gRPC endpoint, such as `http://127.0.0.1:50051`.
    pub endpoint: String,
    /// Database to create the collection in.
    pub database: String,
    /// Collection name; an existing collection of this name is dropped.
    pub collection: String,
    /// Rows per insert request.
    pub batch_size: usize,
    /// `ef` values to sweep, ascending.
    pub ef_sweep: Vec<u32>,
    /// Recall the sweep looks for.
    pub target_recall: f64,
    /// Client counts for the throughput runs.
    pub concurrency: Vec<usize>,
    /// Length of each throughput run.
    pub duration: Duration,
    /// Untimed queries before each case.
    pub warmup: usize,
    /// Skip the load and query the existing collection.
    pub skip_load: bool,
}

/// A connected client.
#[derive(Clone)]
struct Client {
    inner: LogPoseServiceClient<Channel>,
    database: String,
    collection: String,
}

impl Client {
    async fn connect(config: &DriverConfig) -> Result<Self> {
        let channel = tonic::transport::Endpoint::new(config.endpoint.clone())?
            .tcp_nodelay(true)
            .connect()
            .await
            .with_context(|| format!("connecting to {}", config.endpoint))?;
        Ok(Self {
            inner: LogPoseServiceClient::new(channel)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
            database: config.database.clone(),
            collection: config.collection.clone(),
        })
    }

    async fn search(
        &mut self,
        vector: &[f32],
        filter: Option<&Filter>,
        k: usize,
        ef: u32,
        explain: proto::ExplainMode,
    ) -> Result<proto::QueryCollectionReply> {
        let reply = self
            .inner
            .query_collection(QueryCollectionRequest {
                database_name: self.database.clone(),
                collection_name: self.collection.clone(),
                vector: Some(VectorQuery {
                    field: VECTOR_FIELD.to_owned(),
                    values: vector.to_vec(),
                }),
                filter: filter.cloned(),
                top_k: u64::try_from(k)?,
                output_fields: vec![PK_FIELD.to_owned()],
                ef,
                explain: explain as i32,
                ..QueryCollectionRequest::default()
            })
            .await
            .map_err(|status| anyhow!("query failed: {status}"))?;
        Ok(reply.into_inner())
    }

    async fn search_ids(
        &mut self,
        vector: &[f32],
        filter: Option<&Filter>,
        k: usize,
        ef: u32,
    ) -> Result<Vec<u64>> {
        let reply = self
            .search(vector, filter, k, ef, proto::ExplainMode::None)
            .await?;
        hit_ids(&reply)
    }

    fn collection_request<T>(&self, build: impl FnOnce(String, String) -> T) -> T {
        build(self.database.clone(), self.collection.clone())
    }
}

fn hit_ids(reply: &proto::QueryCollectionReply) -> Result<Vec<u64>> {
    reply
        .hits
        .iter()
        .map(|hit| {
            match hit
                .record
                .as_ref()
                .and_then(|record| record.pk.as_ref())
                .and_then(|pk| pk.kind.as_ref())
            {
                Some(proto::primary_key::Kind::Int64Value(id)) => {
                    u64::try_from(*id).context("negative primary key")
                }
                other => bail!("hit without an int64 primary key: {other:?}"),
            }
        })
        .collect()
}

/// The filter of a case, if it has one.
#[must_use]
pub fn case_filter(scalar_field: &str, case: &CaseSpec) -> Option<Filter> {
    case.filter_lt.map(|limit| Filter {
        node: Some(proto::filter::Node::Range(FieldRange {
            field: scalar_field.to_owned(),
            lt: Some(Value {
                kind: Some(proto::value::Kind::Int64Value(limit)),
            }),
            ..FieldRange::default()
        })),
    })
}

/// Server version string from `GetMetadata`.
pub async fn server_version(config: &DriverConfig) -> Result<String> {
    let mut client = Client::connect(config).await?;
    let metadata = client
        .inner
        .get_metadata(GetMetadataRequest {})
        .await?
        .into_inner();
    Ok(format!(
        "{} {} ({}, {})",
        metadata.product, metadata.version, metadata.git_sha, metadata.profile
    ))
}

fn log(message: impl AsRef<str>) {
    eprintln!("[logpose-bench vdb] {}", message.as_ref());
}

/// Create the collection and insert every row, then flush, compact, and wait
/// for background maintenance, so every row is in one indexed segment.
pub async fn load(config: &DriverConfig, prepared: Arc<Prepared>) -> Result<LoadReport> {
    ensure!(config.batch_size > 0, "batch size must be positive");
    let mut client = Client::connect(config).await?;
    reset_collection(&mut client, &prepared).await?;

    let n = prepared.dataset.len();
    let batch_size = config.batch_size;
    log(format!("inserting {n} rows in batches of {batch_size}"));
    let first = Arc::clone(&prepared);
    let (database, collection) = (config.database.clone(), config.collection.clone());
    let batches = (0..n).step_by(batch_size).map(move |start| {
        let end = (start + batch_size).min(n);
        BulkUpsertRecordsRequest {
            database_name: database.clone(),
            collection_name: collection.clone(),
            records: (start..end).map(|row| record(&first, row)).collect(),
        }
    });
    let insert_started = Instant::now();
    let reply = client
        .inner
        .bulk_upsert_records(tokio_stream::iter(batches))
        .await
        .map_err(|status| anyhow!("bulk upsert failed: {status}"))?
        .into_inner();
    let insert_seconds = insert_started.elapsed().as_secs_f64();
    ensure!(
        reply.applied_ops == n as u64,
        "server applied {} of {n} rows",
        reply.applied_ops
    );

    log("flushing, compacting, and waiting for maintenance");
    let optimize_started = Instant::now();
    client
        .inner
        .flush_collection(client.collection_request(|database_name, collection_name| {
            FlushCollectionRequest {
                database_name,
                collection_name,
            }
        }))
        .await
        .map_err(|status| anyhow!("flush failed: {status}"))?;
    wait_for_maintenance(&mut client).await?;
    client
        .inner
        .compact_collection(client.collection_request(|database_name, collection_name| {
            CompactCollectionRequest {
                database_name,
                collection_name,
            }
        }))
        .await
        .map_err(|status| anyhow!("compact failed: {status}"))?;
    wait_for_maintenance(&mut client).await?;
    let optimize_seconds = optimize_started.elapsed().as_secs_f64();
    log(format!(
        "loaded in {insert_seconds:.1} s, optimized in {optimize_seconds:.1} s"
    ));
    Ok(LoadReport {
        rows: n,
        batch_size,
        insert_seconds,
        optimize_seconds,
        total_seconds: insert_seconds + optimize_seconds,
        insert_rows_per_sec: n as f64 / insert_seconds.max(f64::MIN_POSITIVE),
    })
}

fn record(prepared: &Prepared, row: usize) -> Record {
    let mut vectors = HashMap::with_capacity(1);
    vectors.insert(
        VECTOR_FIELD.to_owned(),
        Vector {
            values: prepared.dataset.row(row).to_vec(),
        },
    );
    let mut fields = HashMap::with_capacity(1);
    fields.insert(
        prepared.manifest.scalar_field.clone(),
        Value {
            kind: Some(proto::value::Kind::Int64Value(prepared.ranks[row])),
        },
    );
    Record {
        pk: Some(PrimaryKey {
            kind: Some(proto::primary_key::Kind::Int64Value(row as i64)),
        }),
        vectors,
        fields,
        extra: None,
    }
}

async fn reset_collection(client: &mut Client, prepared: &Prepared) -> Result<()> {
    let database = client.database.clone();
    match client
        .inner
        .get_database(GetDatabaseRequest {
            database_name: database.clone(),
        })
        .await
    {
        Ok(_) => {}
        Err(status) if status.code() == Code::NotFound => {
            client
                .inner
                .put_database(PutDatabaseRequest {
                    database_name: database.clone(),
                })
                .await
                .map_err(|status| anyhow!("creating database {database}: {status}"))?;
        }
        Err(status) => bail!("reading database {database}: {status}"),
    }
    let exists = client
        .inner
        .get_collection(client.collection_request(|database_name, collection_name| {
            GetCollectionRequest {
                database_name,
                collection_name,
            }
        }))
        .await;
    match exists {
        Ok(_) => {
            log(format!(
                "dropping existing collection {}",
                client.collection
            ));
            client
                .inner
                .drop_collection(client.collection_request(|database_name, collection_name| {
                    DropCollectionRequest {
                        database_name,
                        collection_name,
                    }
                }))
                .await
                .map_err(|status| anyhow!("dropping collection: {status}"))?;
        }
        Err(status) if status.code() == Code::NotFound => {}
        Err(status) => bail!("reading collection: {status}"),
    }
    let metric = match prepared.manifest.metric {
        crate::dataset::Metric::Cosine => proto::DistanceMetric::Cosine,
        crate::dataset::Metric::Dot => proto::DistanceMetric::Dot,
        crate::dataset::Metric::L2 => proto::DistanceMetric::L2,
    };
    client
        .inner
        .create_collection(CreateCollectionRequest {
            database_name: client.database.clone(),
            collection_name: client.collection.clone(),
            primary_key: Some(PrimaryKeySpec {
                name: PK_FIELD.to_owned(),
                r#type: proto::PrimaryKeyType::Int64 as i32,
            }),
            vectors: vec![VectorFieldSpec {
                name: VECTOR_FIELD.to_owned(),
                dimensions: u32::try_from(prepared.manifest.dims)?,
                metric: metric as i32,
            }],
            fields: vec![ScalarFieldSpec {
                name: prepared.manifest.scalar_field.clone(),
                r#type: proto::FieldType::Int64 as i32,
                index: proto::FieldIndex::Auto as i32,
                nullable: Some(false),
            }],
            dynamic_fields: Some(false),
        })
        .await
        .map_err(|status| anyhow!("creating collection: {status}"))?;
    Ok(())
}

async fn stats(client: &mut Client) -> Result<proto::CollectionStatsReply> {
    Ok(client
        .inner
        .get_collection_stats(client.collection_request(|database_name, collection_name| {
            GetCollectionStatsRequest {
                database_name,
                collection_name,
                ..GetCollectionStatsRequest::default()
            }
        }))
        .await
        .map_err(|status| anyhow!("reading stats: {status}"))?
        .into_inner())
}

/// Wait until no maintenance job is queued or running, twice in a row with no
/// run completing in between (a job can enqueue a follow-up after it clears).
async fn wait_for_maintenance(client: &mut Client) -> Result<()> {
    let started = Instant::now();
    let mut idle_after_runs = None;
    loop {
        let reply = stats(client).await?;
        let maintenance = reply.maintenance.unwrap_or_default();
        if let Some(error) = maintenance.last_error {
            bail!("maintenance {} failed: {}", error.job, error.message);
        }
        if maintenance.pending.is_empty() && maintenance.in_progress.is_none() {
            if idle_after_runs == Some(maintenance.completed_runs) {
                return Ok(());
            }
            idle_after_runs = Some(maintenance.completed_runs);
        } else {
            idle_after_runs = None;
        }
        ensure!(
            started.elapsed() < MAINTENANCE_TIMEOUT,
            "maintenance did not settle within {MAINTENANCE_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Collection statistics for the report.
pub async fn server_stats(config: &DriverConfig) -> Result<serde_json::Value> {
    let mut client = Client::connect(config).await?;
    let reply = stats(&mut client).await?;
    let units = reply
        .query_units
        .iter()
        .map(|unit| {
            json!({
                "tier": unit.tier,
                "index_kind": unit.index_kind,
                "put_count": unit.put_count,
                "approx_bytes": unit.approx_bytes,
                "component_bytes": unit.component_bytes,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "segment_count": reply.segment_count,
        "live_record_count": reply.live_record_count,
        "mutable_op_count": reply.mutable_op_count,
        "manifest_generation": reply.manifest_generation,
        "query_units": units,
    }))
}

/// Run every case: an ef sweep, then throughput runs at the chosen ef.
pub async fn run_cases(config: &DriverConfig, prepared: Arc<Prepared>) -> Result<Vec<CaseResult>> {
    let mut results = Vec::with_capacity(prepared.manifest.cases.len());
    for (index, case) in prepared.manifest.cases.iter().enumerate() {
        let result = run_case(config, &prepared, index).await?;
        let chosen = result
            .concurrency
            .iter()
            .map(|run| format!("{} clients {:.1} qps", run.clients, run.qps))
            .collect::<Vec<_>>()
            .join(", ");
        log(format!(
            "{}: ef {} recall {:.4}; {chosen}",
            case.name, result.chosen_ef, result.chosen_recall
        ));
        results.push(result);
    }
    Ok(results)
}

async fn run_case(
    config: &DriverConfig,
    prepared: &Arc<Prepared>,
    index: usize,
) -> Result<CaseResult> {
    let case = &prepared.manifest.cases[index];
    let truth = &prepared.truths[index];
    let filter = case_filter(&prepared.manifest.scalar_field, case);
    let k = prepared.manifest.k;
    let dataset = &prepared.dataset;
    let queries = dataset.query_count();
    ensure!(!config.ef_sweep.is_empty(), "the ef sweep is empty");
    let mut client = Client::connect(config).await?;

    for warm in 0..config.warmup.min(queries) {
        client
            .search_ids(dataset.query(warm), filter.as_ref(), k, config.ef_sweep[0])
            .await?;
    }

    let mut sweep = Vec::with_capacity(config.ef_sweep.len());
    for ef in &config.ef_sweep {
        let mut latencies = Vec::with_capacity(queries);
        let mut recalls = Vec::with_capacity(queries);
        let started = Instant::now();
        for (query, expected) in truth.iter().enumerate() {
            let search_started = Instant::now();
            let ids = client
                .search_ids(dataset.query(query), filter.as_ref(), k, *ef)
                .await
                .with_context(|| format!("case {} ef {ef} query {query}", case.name))?;
            latencies.push(search_started.elapsed());
            recalls.push(recall_at_k(expected, &ids, k));
        }
        let wall = started.elapsed();
        let point = SweepPoint {
            ef: *ef,
            recall: mean(&recalls),
            min_recall: recalls.iter().copied().fold(1.0, f64::min),
            queries,
            qps: queries as f64 / wall.as_secs_f64().max(f64::MIN_POSITIVE),
            latency: LatencySummary::from_durations(&latencies),
        };
        log(format!(
            "{} ef {ef}: recall {:.4}, {:.1} qps, p99 {:.2} ms",
            case.name, point.recall, point.qps, point.latency.p99_ms
        ));
        let met = point.recall >= config.target_recall;
        sweep.push(point);
        if met {
            break;
        }
    }
    let (chosen_ef, chosen_recall, met_target) = choose_ef(&sweep, config.target_recall)?;

    let explained = client
        .search(
            dataset.query(0),
            filter.as_ref(),
            k,
            chosen_ef,
            proto::ExplainMode::Plan,
        )
        .await?;
    let plan = explained.diagnostics.map(|diagnostics| {
        json!({
            "chosen_plan": proto::QueryPlanKind::try_from(diagnostics.chosen_plan)
                .map(|kind| kind.as_str_name().to_owned())
                .unwrap_or_else(|_| diagnostics.chosen_plan.to_string()),
            "planner_reason": diagnostics.planner_reason,
            "estimated_selectivity": diagnostics.estimated_selectivity,
            "units_scanned": diagnostics.units_scanned,
            "fallback_reason": diagnostics.fallback_reason,
        })
    });

    let mut concurrency = Vec::with_capacity(config.concurrency.len());
    for clients in &config.concurrency {
        concurrency.push(
            throughput(
                config,
                Arc::clone(prepared),
                index,
                *clients,
                chosen_ef,
                filter.clone(),
            )
            .await?,
        );
    }

    Ok(CaseResult {
        name: case.name.clone(),
        selectivity: case.selectivity,
        filter: case
            .filter_lt
            .map(|limit| format!("{} < {limit}", prepared.manifest.scalar_field)),
        matching_rows: case.matching_rows,
        sweep,
        chosen_ef,
        chosen_recall,
        met_target,
        plan,
        concurrency,
    })
}

/// The smallest swept ef that meets `target`, or the ef with the best recall.
pub fn choose_ef(sweep: &[SweepPoint], target: f64) -> Result<(u32, f64, bool)> {
    if let Some(point) = sweep.iter().find(|point| point.recall >= target) {
        return Ok((point.ef, point.recall, true));
    }
    sweep
        .iter()
        .max_by(|left, right| left.recall.total_cmp(&right.recall))
        .map(|point| (point.ef, point.recall, false))
        .ok_or_else(|| anyhow!("the ef sweep is empty"))
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len().max(1) as f64
}

/// Run `clients` concurrent clients for the configured duration. Client `c`
/// sends queries `c, c + clients, ...`, wrapping around the query set.
async fn throughput(
    config: &DriverConfig,
    prepared: Arc<Prepared>,
    index: usize,
    clients: usize,
    ef: u32,
    filter: Option<Filter>,
) -> Result<ConcurrencyResult> {
    ensure!(clients > 0, "client count must be positive");
    let mut connected = Vec::with_capacity(clients);
    for _ in 0..clients {
        connected.push(Client::connect(config).await?);
    }
    let k = prepared.manifest.k;
    let duration = config.duration;
    let started = Instant::now();
    let deadline = started + duration;
    let tasks = connected
        .into_iter()
        .enumerate()
        .map(|(worker, mut client)| {
            let prepared = Arc::clone(&prepared);
            let filter = filter.clone();
            tokio::spawn(async move {
                let queries = prepared.dataset.query_count();
                let truth = &prepared.truths[index];
                let mut latencies = Vec::new();
                let mut recall_sum = 0.0;
                let mut query = worker % queries;
                while Instant::now() < deadline {
                    let search_started = Instant::now();
                    let ids = client
                        .search_ids(prepared.dataset.query(query), filter.as_ref(), k, ef)
                        .await?;
                    latencies.push(search_started.elapsed());
                    recall_sum += recall_at_k(&truth[query], &ids, k);
                    query = (query + clients) % queries;
                }
                Ok::<_, anyhow::Error>((latencies, recall_sum))
            })
        })
        .collect::<Vec<_>>();
    let mut latencies = Vec::new();
    let mut recall_sum = 0.0;
    for task in tasks {
        let (worker_latencies, worker_recall) = task.await.context("search task failed")??;
        latencies.extend(worker_latencies);
        recall_sum += worker_recall;
    }
    let wall = started.elapsed().as_secs_f64();
    let queries = latencies.len();
    Ok(ConcurrencyResult {
        clients,
        duration_seconds: wall,
        queries,
        qps: queries as f64 / wall.max(f64::MIN_POSITIVE),
        latency: LatencySummary::from_durations(&latencies),
        recall: recall_sum / queries.max(1) as f64,
    })
}
