//! Creates and drops that complete after their caller stopped waiting, and the reconciler
//! converging creates that stopped between their steps.
//!
//! These tests need a live etcd: they run only when `LOGPOSE_TEST_ETCD_ENDPOINTS` is set, for
//! example to `http://127.0.0.1:2379`, and otherwise pass without running (the skip message
//! shows with `--nocapture`). When the variable is set, an unreachable etcd is a failure, and
//! under CI a missing variable is too.

use super::*;
use etcd_client::Client;
use logpose_storage::EngineConfig;
use logpose_types::{DistanceMetric, NodeRole};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, sleep};

/// Environment variable that enables the etcd tests.
const ETCD_ENDPOINTS_ENV: &str = "LOGPOSE_TEST_ETCD_ENDPOINTS";
/// The node the tests' catalog runs on, which leads.
const NODE: &str = "node-a";

/// A catalog over a fresh engine and a fresh etcd key prefix, the membership of `NODE`, and
/// its leadership.
struct Fixture {
    catalog: EtcdCollectionCatalog,
    coordination: EtcdCoordinationClient,
    fence: LeadershipFence,
    member: MembershipFence,
    endpoints: Vec<String>,
    key_prefix: String,
    /// Last, so the engine inside `catalog` closes before its root is removed.
    _root: tempfile::TempDir,
}

impl Fixture {
    /// The fixture, or `None` to skip the test.
    async fn new(label: &str) -> Option<Self> {
        let endpoints = etcd_endpoints_or_skip(label).await?;
        let key_prefix = format!(
            "/logpose/tests/reconcile-{label}/{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time should be monotonic")
                .as_nanos()
        );
        let config = EtcdMetadataConfig {
            endpoints: endpoints.clone(),
            key_prefix: key_prefix.clone(),
            timeout_ms: 1_500,
            membership_ttl_secs: 15,
            leadership_ttl_secs: 60,
            cluster_name: "reconcile".to_owned(),
        };
        let root = tempfile::Builder::new()
            .prefix(&format!("logpose-storage-etcd-{label}-"))
            .tempdir()
            .expect("temp dir should be created");
        let engine =
            Engine::open_local(root.path(), EngineConfig::default()).expect("engine should open");
        let catalog =
            EtcdCollectionCatalog::new(engine, config.clone()).expect("catalog should build");
        let coordination =
            EtcdCoordinationClient::new(config).expect("coordination client should build");
        // Reconciliation is fenced by the node's membership lease, and creates by the leader's:
        // register the node and take the leadership of this fresh prefix before anything else.
        let membership = coordination
            .register_membership(NODE, NodeRole::Combined)
            .await
            .expect("etcd should register the node");
        let lease = coordination
            .try_acquire_leadership(NODE)
            .await
            .expect("etcd should grant the leadership")
            .expect("a fresh prefix has no leader");
        Some(Self {
            catalog,
            coordination,
            fence: LeadershipFence {
                node_id: NODE.to_owned(),
                lease_id: lease.lease_id,
            },
            member: MembershipFence {
                node_id: NODE.to_owned(),
                lease_id: membership.lease_id,
            },
            endpoints,
            key_prefix,
            _root: root,
        })
    }

    /// Create `name` in the default database, placed on `NODE`, as a caller would.
    fn create(&self, name: &str) -> impl Future<Output = Result<CollectionDescriptor>> + use<> {
        let catalog = self.catalog.clone();
        let fence = self.fence.clone();
        let request = CreateCollectionRequest::new(name, 2, DistanceMetric::Dot);
        async move {
            catalog
                .create_collection(request, placement(NODE), fence)
                .await
        }
    }

    /// Wait until no create or drop runs on this node.
    async fn settle(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.catalog.operations_in_flight() > 0 {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for creates and drops to finish"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Whether etcd holds the pending metadata of `default/name`.
    async fn pending(&self, name: &str) -> bool {
        matches!(
            self.catalog.describe(name).await,
            Err(LogPoseError::ReconciliationRequired { .. })
        )
    }

    fn local(&self, name: &str) -> Result<Arc<logpose_storage::CollectionHandle>> {
        self.catalog
            .engine
            .collection(&CollectionRef::new_default(name))
    }

    /// Give up the leadership and the membership and remove the test's keys.
    async fn finish(self) {
        let _ = self.coordination.revoke_lease(self.fence.lease_id).await;
        let _ = self.coordination.revoke_lease(self.member.lease_id).await;
        if let Ok(mut client) = Client::connect(self.endpoints.clone(), None).await {
            let _ = client
                .delete(
                    self.key_prefix.clone(),
                    Some(DeleteOptions::new().with_prefix()),
                )
                .await;
        }
    }
}

fn placement(node: &str) -> CollectionAssignment {
    CollectionAssignment {
        assigned_node: node.to_owned(),
        assigned_role: NodeRole::Combined,
    }
}

/// A create whose caller stops waiting after either step, pending metadata written or local
/// collection created, still completes: the metadata ends ready over the local collection.
#[tokio::test]
async fn a_create_completes_after_its_caller_stops_waiting() {
    let Some(fixture) = Fixture::new("create-caller-gone").await else {
        return;
    };
    for (step, name) in [
        (Step::MetadataWritten, "first"),
        (Step::LocalChanged, "second"),
    ] {
        let (reached, resume) = fixture.catalog.interrupt_at(step);
        let caller = tokio::spawn(fixture.create(name));
        reached.await.expect("the create reaches the step");
        // The caller goes away: its future is dropped.
        caller.abort();
        assert!(
            caller.await.is_err_and(|error| error.is_cancelled()),
            "{step:?}: the caller was cancelled"
        );
        resume.send(true).expect("the create waits to resume");
        fixture.settle().await;

        let described = fixture
            .catalog
            .describe(name)
            .await
            .expect("the create completed");
        let local = fixture.local(name).expect("the local collection exists");
        assert_eq!(local.meta().id, described.collection_id, "{step:?}");
    }
    fixture.finish().await;
}

/// A drop whose caller stops waiting after the local drop still removes the metadata.
#[tokio::test]
async fn a_drop_completes_after_its_caller_stops_waiting() {
    let Some(fixture) = Fixture::new("drop-caller-gone").await else {
        return;
    };
    fixture.create("documents").await.expect("create");
    let (reached, resume) = fixture.catalog.interrupt_at(Step::LocalChanged);
    let caller = tokio::spawn({
        let catalog = fixture.catalog.clone();
        let fence = fixture.fence.clone();
        async move { catalog.drop_collection("documents", fence).await }
    });
    reached.await.expect("the drop reaches the step");
    caller.abort();
    assert!(caller.await.is_err_and(|error| error.is_cancelled()));
    resume.send(true).expect("the drop waits to resume");
    fixture.settle().await;

    assert!(matches!(
        fixture.catalog.describe("documents").await,
        Err(LogPoseError::NotFound { .. })
    ));
    assert!(fixture.local("documents").is_err());
    assert!(
        fixture
            .catalog
            .etcd
            .list_collection_metadata()
            .await
            .expect("metadata lists")
            .is_empty(),
        "no metadata key is left"
    );
    fixture.finish().await;
}

/// A create that stopped after writing its pending metadata, before its local collection (its
/// process died), is rolled back by the leader: the metadata is removed and the name can be
/// created again. A pass under a fence that is not the node's membership changes nothing.
#[tokio::test]
async fn the_reconciler_rolls_back_a_create_stopped_before_its_local_collection() {
    let Some(fixture) = Fixture::new("roll-back").await else {
        return;
    };
    let (reached, resume) = fixture.catalog.interrupt_at(Step::MetadataWritten);
    let caller = tokio::spawn(fixture.create("documents"));
    reached.await.expect("the create reaches the step");
    drop(resume);
    caller
        .await
        .expect("the caller runs")
        .expect_err("the create stopped");
    fixture.settle().await;
    assert!(fixture.pending("documents").await);
    assert!(fixture.local("documents").is_err());

    let stale = MembershipFence {
        node_id: NODE.to_owned(),
        lease_id: fixture.member.lease_id + 1,
    };
    let report = fixture
        .catalog
        .reconcile_pending(&stale)
        .await
        .expect("the pass runs");
    assert_eq!(report.failed.len(), 1, "{report:?}");
    assert!(fixture.pending("documents").await, "the fence held");

    let report = fixture
        .catalog
        .reconcile_pending(&fixture.member)
        .await
        .expect("the pass runs");
    assert_eq!(
        report,
        ReconcileReport {
            rolled_back: vec!["default/documents".to_owned()],
            ..ReconcileReport::default()
        }
    );
    assert!(matches!(
        fixture.catalog.describe("documents").await,
        Err(LogPoseError::NotFound { .. })
    ));
    fixture
        .create("documents")
        .await
        .expect("the name can be created again");
    fixture.finish().await;
}

/// A create that stopped after its local collection, before marking the metadata ready, is
/// rolled forward by the leader: the collection is described with its local id.
#[tokio::test]
async fn the_reconciler_rolls_forward_a_create_stopped_after_its_local_collection() {
    let Some(fixture) = Fixture::new("roll-forward").await else {
        return;
    };
    let (reached, resume) = fixture.catalog.interrupt_at(Step::LocalChanged);
    let caller = tokio::spawn(fixture.create("documents"));
    reached.await.expect("the create reaches the step");
    drop(resume);
    caller
        .await
        .expect("the caller runs")
        .expect_err("the create stopped");
    fixture.settle().await;
    assert!(fixture.pending("documents").await);
    let local = fixture
        .local("documents")
        .expect("the local collection exists");

    let report = fixture
        .catalog
        .reconcile_pending(&fixture.member)
        .await
        .expect("the pass runs");
    assert_eq!(
        report,
        ReconcileReport {
            rolled_forward: vec!["default/documents".to_owned()],
            ..ReconcileReport::default()
        }
    );
    let described = fixture
        .catalog
        .describe("documents")
        .await
        .expect("the collection is ready");
    assert_eq!(described.collection_id, local.meta().id);
    // A second pass finds nothing to do.
    assert_eq!(
        fixture
            .catalog
            .reconcile_pending(&fixture.member)
            .await
            .expect("the pass runs"),
        ReconcileReport::default()
    );
    fixture.finish().await;
}

/// A create whose node loses the leadership before its last step cannot mark its metadata
/// ready: the step is fenced by the leader's lease. The node rolls it forward while another
/// node leads, since only it sees that the local collection exists.
#[tokio::test]
async fn the_reconciler_rolls_forward_a_create_whose_node_lost_the_leadership() {
    let Some(fixture) = Fixture::new("lost-leadership").await else {
        return;
    };
    let (reached, resume) = fixture.catalog.interrupt_at(Step::LocalChanged);
    let caller = tokio::spawn(fixture.create("documents"));
    reached.await.expect("the create reaches the step");
    fixture
        .coordination
        .revoke_lease(fixture.fence.lease_id)
        .await
        .expect("the leadership is given up");
    let other_leader = fixture
        .coordination
        .try_acquire_leadership("node-b")
        .await
        .expect("etcd should grant the leadership")
        .expect("the leadership is vacant");
    resume.send(true).expect("the create waits to resume");
    let error = caller
        .await
        .expect("the caller runs")
        .expect_err("the create lost its fence");
    assert!(
        matches!(error, LogPoseError::ReconciliationRequired { .. }),
        "{error:?}"
    );
    fixture.settle().await;
    assert!(fixture.pending("documents").await);
    let local = fixture
        .local("documents")
        .expect("the local collection exists");

    let report = fixture
        .catalog
        .reconcile_pending(&fixture.member)
        .await
        .expect("the pass runs");
    assert_eq!(
        report,
        ReconcileReport {
            rolled_forward: vec!["default/documents".to_owned()],
            ..ReconcileReport::default()
        }
    );
    let described = fixture
        .catalog
        .describe("documents")
        .await
        .expect("the collection is ready");
    assert_eq!(described.collection_id, local.meta().id);
    let _ = fixture
        .coordination
        .revoke_lease(other_leader.lease_id)
        .await;
    fixture.finish().await;
}

/// The reconciler leaves alone the pending metadata of a create still in flight on this node,
/// which then completes, and of a collection placed on another node, whose local state this
/// node cannot see.
#[tokio::test]
async fn the_reconciler_leaves_creates_in_flight_and_placed_elsewhere() {
    let Some(fixture) = Fixture::new("skip").await else {
        return;
    };
    let descriptor = fixture
        .catalog
        .engine
        .plan_collection_descriptor(&CreateCollectionRequest::new(
            "elsewhere",
            2,
            DistanceMetric::Dot,
        ))
        .expect("descriptor should plan");
    fixture
        .catalog
        .etcd
        .put_collection_metadata_if_absent(
            "default/elsewhere",
            &descriptor,
            &placement("node-b"),
            &fixture.fence,
        )
        .await
        .expect("pending metadata placed on another node");
    let (reached, resume) = fixture.catalog.interrupt_at(Step::MetadataWritten);
    let caller = tokio::spawn(fixture.create("busy"));
    reached.await.expect("the create reaches the step");

    let report = fixture
        .catalog
        .reconcile_pending(&fixture.member)
        .await
        .expect("the pass runs");
    assert_eq!(
        report,
        ReconcileReport {
            skipped: vec!["default/busy".to_owned(), "default/elsewhere".to_owned()],
            ..ReconcileReport::default()
        }
    );

    resume.send(true).expect("the create waits to resume");
    caller
        .await
        .expect("the caller runs")
        .expect("the create completes");
    assert!(!fixture.pending("busy").await);
    assert!(fixture.pending("elsewhere").await);
    fixture.finish().await;
}

/// Returns the etcd endpoints for a test, or `None` to skip it.
///
/// The etcd tests run only when `LOGPOSE_TEST_ETCD_ENDPOINTS` names one or more
/// comma-separated endpoints. When it does, an unreachable etcd fails the test instead of
/// skipping it. Under CI (`CI` is set) a missing variable also fails, so CI cannot silently
/// lose this coverage.
async fn etcd_endpoints_or_skip(test_name: &str) -> Option<Vec<String>> {
    let endpoints = std::env::var(ETCD_ENDPOINTS_ENV)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|endpoint| !endpoint.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if endpoints.is_empty() {
        assert!(
            std::env::var_os("CI").is_none(),
            "{test_name}: CI is set but {ETCD_ENDPOINTS_ENV} is not; set it so the etcd tests run in CI"
        );
        eprintln!(
            "skipping {test_name}: set {ETCD_ENDPOINTS_ENV} (for example http://127.0.0.1:2379) to run the etcd tests"
        );
        return None;
    }
    let probe = async {
        let mut client = Client::connect(endpoints.clone(), None).await?;
        client.status().await.map(|_| ())
    };
    let reachable = tokio::time::timeout(Duration::from_secs(5), probe).await;
    assert!(
        matches!(reachable, Ok(Ok(()))),
        "{ETCD_ENDPOINTS_ENV} is set to {endpoints:?} but etcd is unreachable: {reachable:?}; start etcd or unset {ETCD_ENDPOINTS_ENV} to skip the etcd tests"
    );
    Some(endpoints)
}
