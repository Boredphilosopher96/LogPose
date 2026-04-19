//! Operator-facing etcd coordination admin helper for local chaos workflows.

use anyhow::{Context, Error, Result, bail};
use async_trait as _;
use clap::{Parser, Subcommand, error::ErrorKind};
use etcd_client::{Client, ConnectOptions, DeleteOptions};
use logpose_auth as _;
use logpose_catalog as _;
use logpose_storage as _;
use logpose_storage_etcd::{EtcdCoordinationClient, PromotionResult};
use logpose_types::{CollectionRef, EtcdMetadataConfig};
use serde::Serialize;
use serde_json::{Value, json};
use std::{process::ExitCode, time::Duration};

#[derive(Debug, Parser)]
#[command(
    name = "etcd-coordination-admin",
    about = "JSON-based coordination helper for local LogPose etcd chaos workflows"
)]
struct Cli {
    #[arg(
        long,
        env = "LOGPOSE_ETCD_ENDPOINTS",
        value_delimiter = ',',
        default_value = "http://127.0.0.1:2379"
    )]
    endpoints: Vec<String>,
    #[arg(long, env = "LOGPOSE_ETCD_CLUSTER", default_value = "chaos-local")]
    cluster_name: String,
    #[arg(
        long,
        env = "LOGPOSE_ETCD_KEY_PREFIX",
        default_value = "/logpose/metadata"
    )]
    key_prefix: String,
    #[arg(long, env = "LOGPOSE_ETCD_TIMEOUT_MS", default_value_t = 1_500)]
    timeout_ms: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    WipeCluster {
        #[arg(long)]
        yes: bool,
    },
    ListMembership,
    ShowLeader,
    ShowShardOwner {
        collection: String,
        #[arg(long, default_value = "0")]
        shard_id: String,
    },
    PromoteShardOwner {
        collection: String,
        new_owner_node_id: String,
        #[arg(long, default_value = "0")]
        shard_id: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            return match error.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                    print!("{error}");
                    ExitCode::SUCCESS
                }
                _ => {
                    print_json_to_stderr(&error_json(None, &Error::msg(error.to_string())));
                    ExitCode::from(2)
                }
            };
        }
    };
    let command_name = cli.command.name();
    match run(cli).await {
        Ok(output) => {
            print_json_to_stdout(&output);
            ExitCode::SUCCESS
        }
        Err(error) => {
            print_json_to_stderr(&error_json(Some(command_name), &error));
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<Value> {
    let config = config_from_cli(&cli)?;
    let command_name = cli.command.name();
    let cluster = cluster_json(&config);
    let result = match cli.command {
        Command::WipeCluster { yes } => wipe_cluster(&config, yes).await?,
        Command::ListMembership => {
            let client = EtcdCoordinationClient::new(config)?;
            json!({
                "members": client.list_membership().await?,
            })
        }
        Command::ShowLeader => {
            let client = EtcdCoordinationClient::new(config)?;
            json!({
                "leader": client.current_leader().await?,
            })
        }
        Command::ShowShardOwner {
            collection,
            shard_id,
        } => {
            let client = EtcdCoordinationClient::new(config)?;
            let collection = parse_collection_ref(&collection)?;
            json!({
                "collection": collection.lookup_name(),
                "shard_id": shard_id,
                "owner": client.shard_owner(&collection, &shard_id).await?,
            })
        }
        Command::PromoteShardOwner {
            collection,
            new_owner_node_id,
            shard_id,
        } => {
            let client = EtcdCoordinationClient::new(config)?;
            let collection = parse_collection_ref(&collection)?;
            let new_owner_node_id = parse_node_id(&new_owner_node_id)?;
            let current = client
                .shard_owner(&collection, &shard_id)
                .await?
                .with_context(|| {
                    format!(
                        "collection '{}' shard '{}' has no current owner record",
                        collection.lookup_name(),
                        shard_id
                    )
                })?;
            let result = client
                .promote_shard_owner(&current, &new_owner_node_id)
                .await
                .with_context(|| {
                    format!(
                        "failed to promote '{}' shard '{}' to '{}'",
                        collection.lookup_name(),
                        shard_id,
                        new_owner_node_id
                    )
                })?;
            let (promotion, owner) = match result {
                PromotionResult::Applied(owner) => (promotion_result_json("applied"), Some(owner)),
                PromotionResult::Conflict => (
                    promotion_result_json("conflict"),
                    client.shard_owner(&collection, &shard_id).await?,
                ),
            };
            json!({
                "collection": collection.lookup_name(),
                "shard_id": shard_id,
                "requested_owner_node_id": new_owner_node_id,
                "previous_owner": current,
                "promotion": promotion,
                "owner": owner,
            })
        }
    };
    Ok(json!({
        "ok": true,
        "command": command_name,
        "cluster": cluster,
        "result": result,
    }))
}

fn config_from_cli(cli: &Cli) -> Result<EtcdMetadataConfig> {
    let config = EtcdMetadataConfig {
        endpoints: cli
            .endpoints
            .iter()
            .map(|endpoint| endpoint.trim())
            .filter(|endpoint| !endpoint.is_empty())
            .map(str::to_owned)
            .collect(),
        key_prefix: cli.key_prefix.trim_end_matches('/').to_owned(),
        timeout_ms: cli.timeout_ms,
        cluster_name: cli.cluster_name.clone(),
        ..EtcdMetadataConfig::default()
    };
    config.validate()?;
    Ok(config)
}

fn cluster_prefix(config: &EtcdMetadataConfig) -> String {
    format!("{}/clusters/{}", config.key_prefix, config.cluster_name)
}

fn parse_collection_ref(value: &str) -> Result<CollectionRef> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("collection must not be blank");
    }
    match trimmed.split('/').collect::<Vec<_>>().as_slice() {
        [collection_name] if !collection_name.is_empty() => {
            Ok(CollectionRef::new_default(*collection_name))
        }
        [database_name, collection_name]
            if !database_name.is_empty() && !collection_name.is_empty() =>
        {
            Ok(CollectionRef::new(*database_name, *collection_name))
        }
        _ => bail!("collection must be '<collection>' or '<database>/<collection>'"),
    }
}

fn parse_node_id(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("new owner node id must not be blank");
    }
    Ok(trimmed.to_owned())
}

async fn wipe_cluster(config: &EtcdMetadataConfig, yes: bool) -> Result<Value> {
    if !yes {
        bail!("refusing to delete cluster metadata without --yes");
    }
    let mut client = raw_client(config).await?;
    let cluster_prefix = cluster_prefix(config);
    let deleted = client
        .delete(
            format!("{cluster_prefix}/"),
            Some(DeleteOptions::new().with_prefix()),
        )
        .await
        .context("failed to delete cluster prefix")?;
    Ok(json!({
        "cluster_prefix": cluster_prefix,
        "deleted_keys": deleted.deleted(),
    }))
}

async fn raw_client(config: &EtcdMetadataConfig) -> Result<Client> {
    let options = ConnectOptions::default()
        .with_keep_alive(Duration::from_secs(5), Duration::from_secs(2))
        .with_timeout(Duration::from_millis(config.timeout_ms));
    Client::connect(config.endpoints.clone(), Some(options))
        .await
        .context("failed to connect raw etcd client")
}

fn promotion_result_json(status: &str) -> Value {
    json!({
        "status": status,
    })
}

fn cluster_json(config: &EtcdMetadataConfig) -> Value {
    json!({
        "endpoints": &config.endpoints,
        "cluster_name": &config.cluster_name,
        "key_prefix": &config.key_prefix,
        "cluster_prefix": cluster_prefix(config),
    })
}

fn error_json(command: Option<&str>, error: &Error) -> Value {
    let causes = error
        .chain()
        .skip(1)
        .map(|cause| Value::String(cause.to_string()))
        .collect::<Vec<_>>();
    let mut value = json!({
        "ok": false,
        "error": {
            "message": error.to_string(),
        },
    });
    if let Some(command) = command {
        value["command"] = Value::String(command.to_owned());
    }
    if !causes.is_empty() {
        value["error"]["causes"] = Value::Array(causes);
    }
    value
}

fn print_json_to_stdout(value: &impl Serialize) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("json serialization should succeed")
    );
}

fn print_json_to_stderr(value: &impl Serialize) {
    eprintln!(
        "{}",
        serde_json::to_string_pretty(value).expect("json serialization should succeed")
    );
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Self::WipeCluster { .. } => "wipe-cluster",
            Self::ListMembership => "list-membership",
            Self::ShowLeader => "show-leader",
            Self::ShowShardOwner { .. } => "show-shard-owner",
            Self::PromoteShardOwner { .. } => "promote-shard-owner",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn cli_parses_promote_shard_owner_command() {
        let cli = Cli::try_parse_from([
            "etcd-coordination-admin",
            "--endpoints",
            "http://127.0.0.1:2379,http://127.0.0.1:22379",
            "--cluster-name",
            "chaos-dev",
            "promote-shard-owner",
            "analytics/documents",
            "node-b",
            "--shard-id",
            "7",
        ])
        .expect("cli should parse");

        assert_eq!(
            cli.endpoints,
            vec![
                "http://127.0.0.1:2379".to_owned(),
                "http://127.0.0.1:22379".to_owned()
            ]
        );
        assert_eq!(cli.cluster_name, "chaos-dev");
        match cli.command {
            Command::PromoteShardOwner {
                collection,
                new_owner_node_id,
                shard_id,
            } => {
                assert_eq!(collection, "analytics/documents");
                assert_eq!(new_owner_node_id, "node-b");
                assert_eq!(shard_id, "7");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn cluster_prefix_trims_trailing_slash_from_key_prefix() {
        let config = EtcdMetadataConfig {
            endpoints: vec!["http://127.0.0.1:2379".to_owned()],
            key_prefix: "/logpose".to_owned(),
            cluster_name: "chaos-local".to_owned(),
            ..EtcdMetadataConfig::default()
        };

        assert_eq!(
            cluster_prefix(&config),
            "/logpose/clusters/chaos-local".to_owned()
        );
    }

    #[test]
    fn parse_collection_ref_supports_default_and_explicit_namespaces() {
        assert_eq!(
            parse_collection_ref("documents").expect("default namespace should parse"),
            CollectionRef::new_default("documents")
        );
        assert_eq!(
            parse_collection_ref("analytics/documents").expect("explicit namespace should parse"),
            CollectionRef::new("analytics", "documents")
        );
    }

    #[test]
    fn parse_collection_ref_rejects_malformed_values() {
        for value in ["", "analytics/", "/documents", "a/b/c"] {
            assert!(
                parse_collection_ref(value).is_err(),
                "expected '{value}' to be rejected"
            );
        }
    }

    #[test]
    fn config_from_cli_normalizes_key_prefix() {
        let cli = Cli::try_parse_from([
            "etcd-coordination-admin",
            "--key-prefix",
            "/logpose/metadata/",
            "list-membership",
        ])
        .expect("cli should parse");

        let config = config_from_cli(&cli).expect("config should validate");
        assert_eq!(config.key_prefix, "/logpose/metadata");
    }

    #[test]
    fn parse_node_id_rejects_blank_values() {
        let error = parse_node_id("   ").expect_err("blank node ids should be rejected");
        assert!(
            error.to_string().contains("must not be blank"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn promotion_result_json_reports_conflicts() {
        let json = promotion_result_json("conflict");
        assert_eq!(json["status"], "conflict");
    }
}
