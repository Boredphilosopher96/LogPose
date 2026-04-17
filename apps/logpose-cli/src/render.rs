use crate::action::{Action, format_command, metric_name};
use crate::cli::OutputMode;
use anyhow::Context;
use logpose_catalog::CollectionDescriptor;
use logpose_config::LogPoseConfig;
use logpose_query::QueryResponse;
use logpose_storage::InspectReport;
use logpose_types::{CollectionPlacement, CollectionStats, CommitAck, NodeRuntimeStatus, Snapshot};
use serde::Serialize;

pub enum ActionOutput {
    Status(NodeRuntimeStatus),
    Config(LogPoseConfig),
    CollectionCreated(CollectionDescriptor),
    CollectionShown(CollectionDescriptor),
    CollectionStats(CollectionStats),
    CollectionPlacement(CollectionPlacement),
    CollectionFlushed(Snapshot),
    CollectionCompacted(Snapshot),
    RecordsWritten(CommitAck),
    RecordDeleted(CommitAck),
    Query(QueryResponse),
    Inspect(InspectReport),
}

impl ActionOutput {
    pub fn render_direct(&self, mode: OutputMode) -> anyhow::Result<()> {
        match mode {
            OutputMode::Human => {
                println!("{}", self.human_text()?);
                Ok(())
            }
            OutputMode::Json => self.write_json(),
        }
    }

    pub fn human_text(&self) -> anyhow::Result<String> {
        Ok(match self {
            ActionOutput::Status(status) => render_status(status),
            ActionOutput::Config(config) => render_config(config),
            ActionOutput::CollectionCreated(descriptor) => format!(
                "Collection created\nName: {}\nDimensions: {}\nMetric: {}",
                descriptor.name,
                descriptor.dimensions,
                metric_name(descriptor.metric)
            ),
            ActionOutput::CollectionShown(descriptor) => format!(
                "Collection\nName: {}\nDimensions: {}\nMetric: {}",
                descriptor.name,
                descriptor.dimensions,
                metric_name(descriptor.metric)
            ),
            ActionOutput::CollectionStats(stats) => render_stats(stats),
            ActionOutput::CollectionPlacement(placement) => render_placement(placement),
            ActionOutput::CollectionFlushed(snapshot) => format!(
                "Collection flushed\nManifest generation: {}\nVisible sequence number: {}",
                snapshot.manifest_generation, snapshot.visible_seq_no
            ),
            ActionOutput::CollectionCompacted(snapshot) => format!(
                "Collection compacted\nManifest generation: {}\nVisible sequence number: {}",
                snapshot.manifest_generation, snapshot.visible_seq_no
            ),
            ActionOutput::RecordsWritten(ack) => format!(
                "Write completed\nApplied operations: {}\nLast sequence number: {}",
                ack.applied_ops, ack.last_seq_no
            ),
            ActionOutput::RecordDeleted(ack) => format!(
                "Delete completed\nApplied operations: {}\nLast sequence number: {}",
                ack.applied_ops, ack.last_seq_no
            ),
            ActionOutput::Query(response) => render_query(response)?,
            ActionOutput::Inspect(report) => format!(
                "Inspection: {}\n{}",
                report.target,
                serde_json::to_string_pretty(&report.payload)
                    .context("failed to serialize inspect payload")?
            ),
        })
    }

    pub fn json_text(&self) -> anyhow::Result<String> {
        match self {
            ActionOutput::Status(status) => pretty_json(status),
            ActionOutput::Config(config) => pretty_json(config),
            ActionOutput::CollectionCreated(descriptor)
            | ActionOutput::CollectionShown(descriptor) => pretty_json(descriptor),
            ActionOutput::CollectionStats(stats) => pretty_json(stats),
            ActionOutput::CollectionPlacement(placement) => pretty_json(placement),
            ActionOutput::CollectionFlushed(snapshot)
            | ActionOutput::CollectionCompacted(snapshot) => pretty_json(snapshot),
            ActionOutput::RecordsWritten(ack) | ActionOutput::RecordDeleted(ack) => {
                pretty_json(ack)
            }
            ActionOutput::Query(response) => pretty_json(response),
            ActionOutput::Inspect(report) => pretty_json(report),
        }
    }

    pub fn write_json(&self) -> anyhow::Result<()> {
        println!("{}", self.json_text()?);
        Ok(())
    }

    pub fn title(&self) -> &'static str {
        match self {
            ActionOutput::Status(_) => "Runtime Status",
            ActionOutput::Config(_) => "Configuration",
            ActionOutput::CollectionCreated(_) => "Collection Created",
            ActionOutput::CollectionShown(_) => "Collection",
            ActionOutput::CollectionStats(_) => "Collection Statistics",
            ActionOutput::CollectionPlacement(_) => "Collection Placement",
            ActionOutput::CollectionFlushed(_) => "Collection Flushed",
            ActionOutput::CollectionCompacted(_) => "Collection Compacted",
            ActionOutput::RecordsWritten(_) => "Write Completed",
            ActionOutput::RecordDeleted(_) => "Delete Completed",
            ActionOutput::Query(_) => "Query Results",
            ActionOutput::Inspect(_) => "Inspection",
        }
    }
}

pub fn command_preview(action: &Action) -> String {
    format_command(action)
}

fn render_status(status: &NodeRuntimeStatus) -> String {
    let mut lines = vec![
        "Runtime Status".to_owned(),
        format!("Node: {}", status.metadata.node_name),
        format!("Role: {}", status.role.as_str()),
        format!("REST: {}", status.rest_endpoint),
        format!("gRPC: {}", status.grpc_endpoint),
        format!("Storage: {}", status.storage_engine),
        format!(
            "Control Plane Ready: {}",
            yes_no(status.control_plane_ready)
        ),
        format!("Data Plane Ready: {}", yes_no(status.data_plane_ready)),
        format!("Collections: {}", status.collection_count),
        format!(
            "Maintenance Pending: {}",
            status.maintenance.pending_operations
        ),
    ];

    if !status.collections.is_empty() {
        lines.push("Placements:".to_owned());
        for placement in status.collections.iter().take(5) {
            lines.push(format!(
                "  - {} -> {} ({})",
                placement.collection_name, placement.assigned_node, placement.route_kind
            ));
        }
    }

    lines.join("\n")
}

fn render_config(config: &LogPoseConfig) -> String {
    format!(
        "Configuration\nNode name: {}\nRole: {}\nREST: {}\ngRPC: {}\nStorage root: {}\nLog filter: {}",
        config.node_name,
        config.node_role.as_str(),
        format_host_port(&config.rest_host, config.rest_port),
        format_host_port(&config.grpc_host, config.grpc_port),
        config.storage_root.display(),
        config.log_filter
    )
}

fn format_host_port(host: &str, port: u16) -> String {
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]:{port}"),
        Ok(std::net::IpAddr::V4(_)) | Err(_) => format!("{host}:{port}"),
    }
}

fn render_stats(stats: &CollectionStats) -> String {
    format!(
        "Collection Statistics\nCollection: {}\nManifest generation: {}\nVisible sequence number: {}\nLive records: {}\nDeleted records: {}\nMutable operations: {}\nSegments: {}\nPending maintenance: {}",
        stats.collection_name,
        stats.manifest_generation,
        stats.visible_seq_no,
        stats.live_record_count,
        stats.deleted_record_count,
        stats.mutable_op_count,
        stats.segment_count,
        stats.maintenance.pending.len()
    )
}

fn render_placement(placement: &CollectionPlacement) -> String {
    format!(
        "Collection Placement\nCollection: {}\nAssigned node: {}\nAssigned role: {}\nRoute kind: {}\nReason: {}",
        placement.collection_name,
        placement.assigned_node,
        placement.assigned_role.as_str(),
        placement.route_kind,
        placement.route_reason
    )
}

fn render_query(response: &QueryResponse) -> anyhow::Result<String> {
    let mut lines = vec![
        "Query Results".to_owned(),
        format!("Metric: {}", metric_name(response.metric)),
        format!("Returned: {}/{}", response.returned, response.top_k),
        format!(
            "Snapshot: generation {}, visible seq {}",
            response.snapshot.manifest_generation, response.snapshot.visible_seq_no
        ),
    ];
    if response.matches.is_empty() {
        lines.push("Matches: none".to_owned());
    } else {
        lines.push("Matches:".to_owned());
        for (index, item) in response.matches.iter().enumerate() {
            lines.push(format!(
                "  {}. {}  value={}  metadata={}",
                index + 1,
                item.id,
                item.value,
                compact_json(&item.metadata)?
            ));
        }
    }
    if let Some(diagnostics) = &response.diagnostics {
        lines.push("Diagnostics:".to_owned());
        lines.push(format!("  chosen plan: {:?}", diagnostics.chosen_plan));
        lines.push(format!("  units scanned: {}", diagnostics.units_scanned));
        lines.push(format!(
            "  candidates reranked: {}",
            diagnostics.candidates_reranked
        ));
    }
    Ok(lines.join("\n"))
}

fn compact_json<T: Serialize>(value: &T) -> anyhow::Result<String> {
    serde_json::to_string(value).context("failed to serialize compact json")
}

fn pretty_json<T: Serialize>(value: &T) -> anyhow::Result<String> {
    serde_json::to_string_pretty(value).context("failed to serialize json output")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
