use crate::{
    action::{Action, CLI_PUT_BATCH_BYTES, query_request_from_action, read_jsonl_put_batches},
    feedback::{ProgressHandle, Reporter},
    render::ActionOutput,
};
use anyhow::Context;
use logpose_client::LogPoseClient;
use logpose_config::LogPoseConfig;
use logpose_types::{DeleteRecord, RecordId, WriteOperation};

pub async fn execute_action<R: Reporter>(
    config: &LogPoseConfig,
    action: &Action,
    reporter: &R,
) -> anyhow::Result<ActionOutput> {
    match action {
        Action::Status => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching runtime status...");
            let client = connect_client(config).await?;
            let status = client
                .runtime_status()
                .await
                .context("failed to fetch runtime status")?;
            progress.finish_success("Runtime status ready");
            Ok(ActionOutput::Status(status))
        }
        Action::ConfigShow => {
            reporter.emit(crate::feedback::ProgressEvent::Info(
                "Configuration ready".to_owned(),
            ));
            Ok(ActionOutput::Config(config.clone()))
        }
        Action::CollectionCreate(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Creating collection...");
            let client = connect_client(config).await?;
            let descriptor = client
                .create_collection(action.request())
                .await
                .context("failed to create collection")?;
            progress.finish_success("Collection created");
            Ok(ActionOutput::CollectionCreated(descriptor))
        }
        Action::CollectionShow(collection) => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Fetching collection metadata...");
            let client = connect_client(config).await?;
            let descriptor = client
                .get_collection(collection)
                .await
                .context("failed to fetch collection")?;
            progress.finish_success("Collection metadata ready");
            Ok(ActionOutput::CollectionShown(descriptor))
        }
        Action::CollectionStats(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Fetching collection stats...");
            let client = connect_client(config).await?;
            let stats = client
                .stats(collection)
                .await
                .context("failed to read collection stats")?;
            progress.finish_success("Collection stats ready");
            Ok(ActionOutput::CollectionStats(stats))
        }
        Action::CollectionPlacement(collection) => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Fetching collection placement...");
            let client = connect_client(config).await?;
            let placement = client
                .collection_placement(collection)
                .await
                .context("failed to fetch collection placement")?;
            progress.finish_success("Collection placement ready");
            Ok(ActionOutput::CollectionPlacement(placement))
        }
        Action::CollectionFlush(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Flushing collection...");
            let client = connect_client(config).await?;
            let snapshot = client
                .flush(collection)
                .await
                .context("failed to flush collection")?;
            progress.finish_success("Flush completed");
            Ok(ActionOutput::CollectionFlushed(snapshot))
        }
        Action::CollectionCompact(collection) => {
            let progress = ProgressHandle::start(reporter.clone(), "Compacting collection...");
            let client = connect_client(config).await?;
            let snapshot = client
                .compact(collection)
                .await
                .context("failed to compact collection")?;
            progress.finish_success("Compaction completed");
            Ok(ActionOutput::CollectionCompacted(snapshot))
        }
        Action::RecordPut(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Reading JSONL input...");
            let batches = read_jsonl_put_batches(&action.input, CLI_PUT_BATCH_BYTES)?;
            progress.set_message("Writing records...");
            let client = connect_client(config).await?;
            let mut last_seq_no = 0;
            let mut applied_ops = 0;
            for operations in batches {
                let ack = client
                    .write(&action.collection, operations)
                    .await
                    .context("failed to write records")?;
                last_seq_no = ack.last_seq_no;
                applied_ops += ack.applied_ops;
            }
            progress.finish_success("Write completed");
            Ok(ActionOutput::RecordsWritten(logpose_types::CommitAck {
                last_seq_no,
                applied_ops,
            }))
        }
        Action::RecordDelete(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Deleting record...");
            let client = connect_client(config).await?;
            let ack = client
                .write(
                    &action.collection,
                    vec![WriteOperation::Delete(DeleteRecord {
                        id: RecordId::new(action.id.clone()),
                    })],
                )
                .await
                .context("failed to delete record")?;
            progress.finish_success("Delete completed");
            Ok(ActionOutput::RecordDeleted(ack))
        }
        Action::Query(action) => {
            let progress = ProgressHandle::start(reporter.clone(), "Running query...");
            let request = query_request_from_action(action)?;
            let client = connect_client(config).await?;
            let response = client
                .query(request)
                .await
                .context("failed to query collection")?;
            progress.finish_success("Query completed");
            Ok(ActionOutput::Query(response))
        }
        Action::Inspect { collection, target } => {
            let progress =
                ProgressHandle::start(reporter.clone(), "Inspecting collection storage...");
            let client = connect_client(config).await?;
            let report = client
                .inspect(collection, target.clone())
                .await
                .context("failed to inspect collection")?;
            progress.finish_success("Inspection ready");
            Ok(ActionOutput::Inspect(report))
        }
    }
}

pub async fn connect_client(config: &LogPoseConfig) -> anyhow::Result<LogPoseClient> {
    let endpoint = grpc_dial_endpoint(config);
    LogPoseClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("failed to connect to {endpoint}"))
}

#[cfg(test)]
pub fn rest_endpoint(config: &LogPoseConfig) -> String {
    endpoint_url(&config.rest_host, config.rest_port)
}

#[cfg(test)]
pub fn rest_dial_endpoint(config: &LogPoseConfig) -> String {
    dial_endpoint_url(&config.rest_host, config.rest_port)
}

#[cfg(test)]
pub fn grpc_endpoint(config: &LogPoseConfig) -> String {
    endpoint_url(&config.grpc_host, config.grpc_port)
}

pub fn grpc_dial_endpoint(config: &LogPoseConfig) -> String {
    dial_endpoint_url(&config.grpc_host, config.grpc_port)
}

pub fn endpoint_url(host: &str, port: u16) -> String {
    let authority = match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(_)) => format!("[{host}]"),
        Ok(std::net::IpAddr::V4(_)) | Err(_) => host.to_owned(),
    };

    format!("http://{authority}:{port}")
}

pub fn dial_endpoint_url(host: &str, port: u16) -> String {
    let dial_host = match host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "::1",
        _ => host,
    };
    endpoint_url(dial_host, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_helpers_bracket_ipv6_hosts() {
        let config = LogPoseConfig {
            rest_host: "::1".to_owned(),
            rest_port: 18080,
            grpc_host: "::1".to_owned(),
            grpc_port: 15051,
            ..LogPoseConfig::default()
        };

        assert_eq!(rest_endpoint(&config), "http://[::1]:18080");
        assert_eq!(grpc_endpoint(&config), "http://[::1]:15051");
    }

    #[test]
    fn dial_endpoint_helpers_rewrite_wildcard_bind_addresses() {
        let config = LogPoseConfig {
            rest_host: "0.0.0.0".to_owned(),
            rest_port: 18080,
            grpc_host: "::".to_owned(),
            grpc_port: 15051,
            ..LogPoseConfig::default()
        };

        assert_eq!(rest_dial_endpoint(&config), "http://127.0.0.1:18080");
        assert_eq!(grpc_dial_endpoint(&config), "http://[::1]:15051");
    }
}
