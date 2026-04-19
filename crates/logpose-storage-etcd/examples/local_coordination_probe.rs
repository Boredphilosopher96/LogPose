//! Manual coordination smoke probe for a local etcd-backed LogPose cluster.

use anyhow as _;
use async_trait as _;
use clap as _;
use etcd_client::Client;
use logpose_auth as _;
use logpose_catalog::CollectionDescriptor;
use logpose_storage as _;
use logpose_storage_etcd::{EtcdCoordinationClient, PromotionResult, ShardOwnership};
use logpose_types::{CollectionAssignment, CollectionRef, DistanceMetric, EtcdMetadataConfig};
use serde as _;
use std::path::Path;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoints = std::env::var("LOGPOSE_ETCD_ENDPOINTS")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())
        .split(',')
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let cluster_name =
        std::env::var("LOGPOSE_ETCD_CLUSTER").unwrap_or_else(|_| "chaos-local".to_owned());
    let key_prefix =
        std::env::var("LOGPOSE_ETCD_KEY_PREFIX").unwrap_or_else(|_| "/logpose/metadata".to_owned());
    let config = EtcdMetadataConfig {
        endpoints: endpoints.clone(),
        key_prefix: key_prefix.trim_end_matches('/').to_owned(),
        cluster_name: cluster_name.clone(),
        ..EtcdMetadataConfig::default()
    };
    let client = EtcdCoordinationClient::new(config.clone())?;
    let mut raw = Client::connect(endpoints, None).await?;
    let cluster_prefix = format!("{}/clusters/{}", config.key_prefix, config.cluster_name);

    raw.delete(
        format!("{cluster_prefix}/"),
        Some(etcd_client::DeleteOptions::new().with_prefix()),
    )
    .await?;

    let node_a = client
        .register_membership("node-a", logpose_types::NodeRole::Combined)
        .await?;
    let node_b = client
        .register_membership("node-b", logpose_types::NodeRole::Data)
        .await?;
    let node_c = client
        .register_membership("node-c", logpose_types::NodeRole::Data)
        .await?;
    client.keep_alive(node_a.lease_id).await?;
    client.keep_alive(node_b.lease_id).await?;
    client.keep_alive(node_c.lease_id).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let members = client.list_membership().await?;
    println!("members: {members:#?}");

    let leader = client
        .try_acquire_leadership("node-a")
        .await?
        .expect("node-a should acquire leadership");
    let second_leader = client.try_acquire_leadership("node-b").await?;
    println!("leader: {leader:#?}");
    println!("node-b leadership attempt: {second_leader:#?}");

    let collection = CollectionRef::new_default("chaos");
    let descriptor = CollectionDescriptor::new("chaos", 2, DistanceMetric::Dot, Path::new("/tmp"))
        .without_root_path();
    let assignment = CollectionAssignment {
        assigned_node: "node-a".to_owned(),
        assigned_role: logpose_types::NodeRole::Combined,
    };
    let assignment_key = format!(
        "{cluster_prefix}/collections/{}/assignment",
        collection.lookup_name()
    );
    let descriptor_key = format!(
        "{cluster_prefix}/collections/{}/descriptor",
        collection.lookup_name()
    );
    let shard_key = format!(
        "{cluster_prefix}/collections/{}/shards/0/owner",
        collection.lookup_name()
    );
    raw.put(assignment_key, serde_json::to_string(&assignment)?, None)
        .await?;
    raw.put(
        descriptor_key,
        serde_json::json!({
            "descriptor": descriptor,
            "ready": true,
        })
        .to_string(),
        None,
    )
    .await?;
    let seed = ShardOwnership {
        collection: collection.clone(),
        shard_id: "0".to_owned(),
        owner_node_id: "node-a".to_owned(),
        epoch: 1,
        mod_revision: 0,
    };
    raw.put(shard_key, serde_json::to_string(&seed)?, None)
        .await?;

    let current = client
        .shard_owner(&collection, "0")
        .await?
        .expect("seeded shard owner should exist");
    let contender_a = client.clone();
    let contender_b = client.clone();
    let current_for_a = current.clone();
    let current_for_b = current.clone();

    let (result_a, result_b) = tokio::join!(
        async move {
            contender_a
                .promote_shard_owner(&current_for_a, "node-b")
                .await
        },
        async move {
            contender_b
                .promote_shard_owner(&current_for_b, "node-c")
                .await
        }
    );

    println!("promotion race: {result_a:#?} / {result_b:#?}");

    let final_owner = client
        .shard_owner(&collection, "0")
        .await?
        .expect("final shard owner should exist");
    println!("final owner: {final_owner:#?}");

    match (result_a?, result_b?) {
        (PromotionResult::Applied(_), PromotionResult::Conflict)
        | (PromotionResult::Conflict, PromotionResult::Applied(_)) => {}
        other => {
            return Err(format!("unexpected promotion outcome: {other:#?}").into());
        }
    }

    assert_eq!(final_owner.epoch, 2);
    Ok(())
}
