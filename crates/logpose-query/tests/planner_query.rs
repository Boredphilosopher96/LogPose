//! Planner-focused exact query tests.

use async_trait::async_trait;
use logpose_catalog::CollectionDescriptor;
use logpose_query::{
    ExplainMode, Predicate, PredicateComparison, PredicateOperator, QueryPlanKind, QueryRequest,
    query_exact,
};
use logpose_storage::{CreateCollectionRequest, InspectReport, InspectTarget, StorageEngine};
use logpose_types::{
    CollectionStats, CommitAck, DistanceMetric, MaintenanceStatus, QueryUnitStats, RecordId,
    ScalarFieldStats, ScalarMetadataValue, Snapshot, VisibleRecord, WriteOperation,
};
use serde as _;
use serde_json::json;
use std::{collections::BTreeMap, path::Path};
use thiserror as _;

#[tokio::test]
async fn planner_prunes_units_and_reports_tiny_population_fallback() {
    let storage = PlannerStorage::selective();

    let response = query_exact(
        &storage,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("query should succeed");

    assert_eq!(
        response
            .matches
            .iter()
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "gamma"]
    );

    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(
        diagnostics.chosen_plan,
        QueryPlanKind::TinyPopulationExactFallback
    );
    assert_eq!(diagnostics.units_considered, 3);
    assert_eq!(diagnostics.units_pruned, 1);
    assert_eq!(diagnostics.units_scanned, 2);
    assert!(diagnostics.estimated_selectivity < 1.0);
}

#[tokio::test]
async fn planner_uses_vector_first_for_broad_predicates_and_profiles_stages() {
    let storage = PlannerStorage::broad();

    let response = query_exact(
        &storage,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 2,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "score".to_owned(),
                operator: PredicateOperator::Gte,
                value: Some(ScalarMetadataValue::Number(1.into())),
            })),
            explain: ExplainMode::Profile,
        },
    )
    .await
    .expect("query should succeed");

    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.chosen_plan, QueryPlanKind::VectorFirstExact);
    assert!(diagnostics.stage_timings.is_some());
    assert_eq!(diagnostics.rerank_count, 0);
}

#[tokio::test]
async fn planner_keeps_delete_bearing_units_visible_during_predicate_pruning() {
    let storage = DeleteAwarePlannerStorage::new();

    let response = query_exact(
        &storage,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 3,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("query should succeed");

    assert!(
        response.matches.is_empty(),
        "tombstone should hide stale keep record"
    );
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.units_considered, 2);
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 2);
}

#[tokio::test]
async fn planner_keeps_newer_non_matching_versions_visible_during_predicate_pruning() {
    let storage = ShadowingPlannerStorage::new();

    let response = query_exact(
        &storage,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 3,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("query should succeed");

    assert!(
        response.matches.is_empty(),
        "newer non-matching version should keep older keep version hidden"
    );
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 2);
}

#[tokio::test]
async fn planner_selectivity_ignores_empty_mutable_units() {
    let storage = PlannerStorage::new(
        Vec::new(),
        BTreeMap::from([(
            "segment-keep".to_owned(),
            vec![visible_record(
                "alpha",
                vec![1.0, 0.0],
                json!({"kind":"keep"}),
            )],
        )]),
    );

    let response = query_exact(
        &storage,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Eq,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("query should succeed");

    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert!((diagnostics.estimated_selectivity - 1.0).abs() < f32::EPSILON);
}

#[tokio::test]
async fn planner_keeps_units_for_exists_predicates_on_non_scalar_fields() {
    let storage = PlannerStorage::new(
        Vec::new(),
        BTreeMap::from([(
            "segment-details".to_owned(),
            vec![visible_record(
                "alpha",
                vec![1.0, 0.0],
                json!({"details":{"kind":"keep"}}),
            )],
        )]),
    );

    let response = query_exact(
        &storage,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "details".to_owned(),
                operator: PredicateOperator::Exists,
                value: None,
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("query should succeed");

    assert_eq!(response.matches.len(), 1);
    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(diagnostics.units_pruned, 0);
    assert_eq!(diagnostics.units_scanned, 1);
}

#[tokio::test]
async fn planner_treats_sparse_ne_predicates_as_selective() {
    let storage = PlannerStorage::new(
        Vec::new(),
        BTreeMap::from([
            (
                "segment-a-old-sparse".to_owned(),
                vec![
                    visible_record("alpha", vec![0.5, 0.0], json!({"kind":"keep"})),
                    visible_record("beta", vec![0.25, 0.0], json!({"details":{"kind":"keep"}})),
                ],
            ),
            (
                "segment-b-new-drop".to_owned(),
                vec![visible_record(
                    "gamma",
                    vec![1.0, 0.0],
                    json!({"kind":"drop"}),
                )],
            ),
        ]),
    );

    let response = query_exact(
        &storage,
        QueryRequest {
            collection_name: "documents".to_owned(),
            vector: vec![1.0, 0.0],
            top_k: 1,
            snapshot: None,
            filters: Vec::new(),
            predicate: Some(Predicate::Comparison(PredicateComparison {
                field: "kind".to_owned(),
                operator: PredicateOperator::Ne,
                value: Some(ScalarMetadataValue::String("keep".to_owned())),
            })),
            explain: ExplainMode::Plan,
        },
    )
    .await
    .expect("query should succeed");

    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].id.as_str(), "gamma");

    let diagnostics = response.diagnostics.expect("diagnostics should be present");
    assert_eq!(
        diagnostics.chosen_plan,
        QueryPlanKind::TinyPopulationExactFallback
    );
    assert_eq!(diagnostics.units_pruned, 1);
    assert_eq!(diagnostics.units_scanned, 1);
    assert!((diagnostics.estimated_selectivity - (1.0 / 3.0)).abs() < 1e-6);
}

#[derive(Clone)]
struct PlannerStorage {
    descriptor: CollectionDescriptor,
    snapshot: Snapshot,
    stats: CollectionStats,
    mutable_records: Vec<VisibleRecord>,
    immutable_records: BTreeMap<String, Vec<VisibleRecord>>,
}

#[derive(Clone)]
struct DeleteAwarePlannerStorage {
    descriptor: CollectionDescriptor,
    snapshot: Snapshot,
    stats: CollectionStats,
    immutable_records: BTreeMap<String, Vec<VisibleRecord>>,
}

#[derive(Clone)]
struct ShadowingPlannerStorage {
    descriptor: CollectionDescriptor,
    snapshot: Snapshot,
    stats: CollectionStats,
    immutable_records: BTreeMap<String, Vec<VisibleRecord>>,
}

impl PlannerStorage {
    fn selective() -> Self {
        Self::new(
            vec![visible_record(
                "alpha",
                vec![2.0, 0.0],
                json!({"kind":"keep","score": 2}),
            )],
            BTreeMap::from([
                (
                    "segment-keep".to_owned(),
                    vec![visible_record(
                        "gamma",
                        vec![1.5, 0.0],
                        json!({"kind":"keep","score": 1}),
                    )],
                ),
                (
                    "segment-drop".to_owned(),
                    vec![visible_record(
                        "beta",
                        vec![3.0, 0.0],
                        json!({"kind":"drop","score": 3}),
                    )],
                ),
            ]),
        )
    }

    fn broad() -> Self {
        Self::new(
            vec![visible_record(
                "alpha",
                vec![2.0, 0.0],
                json!({"kind":"keep","score": 2}),
            )],
            BTreeMap::from([(
                "segment-wide".to_owned(),
                vec![
                    visible_record("beta", vec![3.0, 0.0], json!({"kind":"keep","score": 3})),
                    visible_record("gamma", vec![1.0, 0.0], json!({"kind":"drop","score": 1})),
                ],
            )]),
        )
    }

    fn new(
        mutable_records: Vec<VisibleRecord>,
        immutable_records: BTreeMap<String, Vec<VisibleRecord>>,
    ) -> Self {
        let descriptor = CollectionDescriptor::new(
            "documents",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/planner-storage"),
        );
        let snapshot = Snapshot {
            manifest_generation: 1,
            visible_seq_no: 3,
        };
        let mut query_units = vec![QueryUnitStats {
            unit_id: "mutable-delta".to_owned(),
            tier: "mutable".to_owned(),
            index_kind: "raw".to_owned(),
            index_file_name: String::new(),
            min_seq_no: 1,
            max_seq_no: 1,
            put_count: mutable_records.len(),
            delete_count: 0,
            approx_bytes: 128,
            scalar_fields: scalar_summary(&mutable_records),
        }];
        for (index, (unit_id, records)) in immutable_records.iter().enumerate() {
            query_units.push(QueryUnitStats {
                unit_id: unit_id.clone(),
                tier: "immutable".to_owned(),
                index_kind: "flat".to_owned(),
                index_file_name: format!("{unit_id}.flat.json"),
                min_seq_no: 1,
                max_seq_no: (index + 1) as u64,
                put_count: records.len(),
                delete_count: 0,
                approx_bytes: 256,
                scalar_fields: scalar_summary(records),
            });
        }
        let stats = CollectionStats {
            collection_id: descriptor.collection_id.clone(),
            collection_name: descriptor.name.clone(),
            manifest_generation: snapshot.manifest_generation,
            visible_seq_no: snapshot.visible_seq_no,
            mutable_op_count: mutable_records.len(),
            segment_count: immutable_records.len(),
            live_record_count: mutable_records.len()
                + immutable_records.values().map(Vec::len).sum::<usize>(),
            deleted_record_count: 0,
            maintenance: MaintenanceStatus::default(),
            query_units,
        };

        Self {
            descriptor,
            snapshot,
            stats,
            mutable_records,
            immutable_records,
        }
    }
}

impl DeleteAwarePlannerStorage {
    fn new() -> Self {
        let descriptor = CollectionDescriptor::new(
            "documents",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/delete-aware-planner-storage"),
        );
        let snapshot = Snapshot {
            manifest_generation: 1,
            visible_seq_no: 2,
        };
        let immutable_records = BTreeMap::from([(
            "segment-keep".to_owned(),
            vec![visible_record(
                "stale",
                vec![1.0, 0.0],
                json!({"kind":"keep"}),
            )],
        )]);
        let stats = CollectionStats {
            collection_id: descriptor.collection_id.clone(),
            collection_name: descriptor.name.clone(),
            manifest_generation: snapshot.manifest_generation,
            visible_seq_no: snapshot.visible_seq_no,
            mutable_op_count: 1,
            segment_count: 1,
            live_record_count: 0,
            deleted_record_count: 1,
            maintenance: MaintenanceStatus::default(),
            query_units: vec![
                QueryUnitStats {
                    unit_id: "mutable-delta".to_owned(),
                    tier: "mutable".to_owned(),
                    index_kind: "raw".to_owned(),
                    index_file_name: String::new(),
                    min_seq_no: 2,
                    max_seq_no: 2,
                    put_count: 0,
                    delete_count: 1,
                    approx_bytes: 64,
                    scalar_fields: BTreeMap::new(),
                },
                QueryUnitStats {
                    unit_id: "segment-keep".to_owned(),
                    tier: "immutable".to_owned(),
                    index_kind: "flat".to_owned(),
                    index_file_name: "segment-keep.flat.json".to_owned(),
                    min_seq_no: 1,
                    max_seq_no: 1,
                    put_count: 1,
                    delete_count: 0,
                    approx_bytes: 128,
                    scalar_fields: scalar_summary(immutable_records["segment-keep"].as_slice()),
                },
            ],
        };

        Self {
            descriptor,
            snapshot,
            stats,
            immutable_records,
        }
    }
}

impl ShadowingPlannerStorage {
    fn new() -> Self {
        let descriptor = CollectionDescriptor::new(
            "documents",
            2,
            DistanceMetric::Dot,
            Path::new("/tmp/shadowing-planner-storage"),
        );
        let snapshot = Snapshot {
            manifest_generation: 1,
            visible_seq_no: 2,
        };
        let immutable_records = BTreeMap::from([
            (
                "segment-old-keep".to_owned(),
                vec![visible_record(
                    "shadowed",
                    vec![1.0, 0.0],
                    json!({"kind":"keep"}),
                )],
            ),
            (
                "segment-new-drop".to_owned(),
                vec![visible_record(
                    "shadowed",
                    vec![2.0, 0.0],
                    json!({"kind":"drop"}),
                )],
            ),
        ]);
        let stats = CollectionStats {
            collection_id: descriptor.collection_id.clone(),
            collection_name: descriptor.name.clone(),
            manifest_generation: snapshot.manifest_generation,
            visible_seq_no: snapshot.visible_seq_no,
            mutable_op_count: 0,
            segment_count: 2,
            live_record_count: 0,
            deleted_record_count: 0,
            maintenance: MaintenanceStatus::default(),
            query_units: vec![
                QueryUnitStats {
                    unit_id: "mutable-delta".to_owned(),
                    tier: "mutable".to_owned(),
                    index_kind: "raw".to_owned(),
                    index_file_name: String::new(),
                    min_seq_no: 0,
                    max_seq_no: 0,
                    put_count: 0,
                    delete_count: 0,
                    approx_bytes: 0,
                    scalar_fields: BTreeMap::new(),
                },
                QueryUnitStats {
                    unit_id: "segment-old-keep".to_owned(),
                    tier: "immutable".to_owned(),
                    index_kind: "flat".to_owned(),
                    index_file_name: "segment-old-keep.flat.json".to_owned(),
                    min_seq_no: 1,
                    max_seq_no: 1,
                    put_count: 1,
                    delete_count: 0,
                    approx_bytes: 128,
                    scalar_fields: scalar_summary(immutable_records["segment-old-keep"].as_slice()),
                },
                QueryUnitStats {
                    unit_id: "segment-new-drop".to_owned(),
                    tier: "immutable".to_owned(),
                    index_kind: "flat".to_owned(),
                    index_file_name: "segment-new-drop.flat.json".to_owned(),
                    min_seq_no: 2,
                    max_seq_no: 2,
                    put_count: 1,
                    delete_count: 0,
                    approx_bytes: 128,
                    scalar_fields: scalar_summary(immutable_records["segment-new-drop"].as_slice()),
                },
            ],
        };

        Self {
            descriptor,
            snapshot,
            stats,
            immutable_records,
        }
    }
}

#[async_trait]
impl StorageEngine for PlannerStorage {
    async fn engine_name(&self) -> &'static str {
        "planner-test"
    }

    async fn create_collection(
        &self,
        _request: CreateCollectionRequest,
    ) -> logpose_types::Result<CollectionDescriptor> {
        Err(logpose_types::LogPoseError::Message(
            "create_collection is not used by planner tests".to_owned(),
        ))
    }

    async fn open_collection(&self, _name: &str) -> logpose_types::Result<CollectionDescriptor> {
        Ok(self.descriptor.clone())
    }

    async fn write(
        &self,
        _collection_name: &str,
        _operations: Vec<WriteOperation>,
    ) -> logpose_types::Result<CommitAck> {
        Err(logpose_types::LogPoseError::Message(
            "write is not used by planner tests".to_owned(),
        ))
    }

    async fn snapshot(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Ok(self.snapshot.clone())
    }

    async fn scan_exact(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
    ) -> logpose_types::Result<Vec<VisibleRecord>> {
        let mut records = self.mutable_records.clone();
        for unit_records in self.immutable_records.values() {
            records.extend(unit_records.clone());
        }
        Ok(records)
    }

    async fn scan_exact_selected(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> logpose_types::Result<Vec<VisibleRecord>> {
        let mut records = Vec::new();
        if include_mutable {
            records.extend(self.mutable_records.clone());
        }
        for unit_id in immutable_unit_ids {
            records.extend(
                self.immutable_records
                    .get(&unit_id)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        Ok(records)
    }

    async fn flush(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Err(logpose_types::LogPoseError::Message(
            "flush is not used by planner tests".to_owned(),
        ))
    }

    async fn compact(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Err(logpose_types::LogPoseError::Message(
            "compact is not used by planner tests".to_owned(),
        ))
    }

    async fn stats(&self, _collection_name: &str) -> logpose_types::Result<CollectionStats> {
        Ok(self.stats.clone())
    }

    async fn stats_snapshot(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
    ) -> logpose_types::Result<CollectionStats> {
        Ok(self.stats.clone())
    }

    async fn inspect(
        &self,
        _collection_name: &str,
        _target: InspectTarget,
    ) -> logpose_types::Result<InspectReport> {
        Err(logpose_types::LogPoseError::Message(
            "inspect is not used by planner tests".to_owned(),
        ))
    }
}

#[async_trait]
impl StorageEngine for DeleteAwarePlannerStorage {
    async fn engine_name(&self) -> &'static str {
        "delete-aware-planner-test"
    }

    async fn create_collection(
        &self,
        _request: CreateCollectionRequest,
    ) -> logpose_types::Result<CollectionDescriptor> {
        Err(logpose_types::LogPoseError::Message(
            "create_collection is not used by planner tests".to_owned(),
        ))
    }

    async fn open_collection(&self, _name: &str) -> logpose_types::Result<CollectionDescriptor> {
        Ok(self.descriptor.clone())
    }

    async fn write(
        &self,
        _collection_name: &str,
        _operations: Vec<WriteOperation>,
    ) -> logpose_types::Result<CommitAck> {
        Err(logpose_types::LogPoseError::Message(
            "write is not used by planner tests".to_owned(),
        ))
    }

    async fn snapshot(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Ok(self.snapshot.clone())
    }

    async fn scan_exact(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
    ) -> logpose_types::Result<Vec<VisibleRecord>> {
        Ok(Vec::new())
    }

    async fn scan_exact_selected(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> logpose_types::Result<Vec<VisibleRecord>> {
        if include_mutable {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for unit_id in immutable_unit_ids {
            records.extend(
                self.immutable_records
                    .get(&unit_id)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        Ok(records)
    }

    async fn flush(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Err(logpose_types::LogPoseError::Message(
            "flush is not used by planner tests".to_owned(),
        ))
    }

    async fn compact(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Err(logpose_types::LogPoseError::Message(
            "compact is not used by planner tests".to_owned(),
        ))
    }

    async fn stats(&self, _collection_name: &str) -> logpose_types::Result<CollectionStats> {
        Ok(self.stats.clone())
    }

    async fn stats_snapshot(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
    ) -> logpose_types::Result<CollectionStats> {
        Ok(self.stats.clone())
    }

    async fn inspect(
        &self,
        _collection_name: &str,
        _target: InspectTarget,
    ) -> logpose_types::Result<InspectReport> {
        Err(logpose_types::LogPoseError::Message(
            "inspect is not used by planner tests".to_owned(),
        ))
    }
}

#[async_trait]
impl StorageEngine for ShadowingPlannerStorage {
    async fn engine_name(&self) -> &'static str {
        "shadowing-planner-test"
    }

    async fn create_collection(
        &self,
        _request: CreateCollectionRequest,
    ) -> logpose_types::Result<CollectionDescriptor> {
        Err(logpose_types::LogPoseError::Message(
            "create_collection is not used by planner tests".to_owned(),
        ))
    }

    async fn open_collection(&self, _name: &str) -> logpose_types::Result<CollectionDescriptor> {
        Ok(self.descriptor.clone())
    }

    async fn write(
        &self,
        _collection_name: &str,
        _operations: Vec<WriteOperation>,
    ) -> logpose_types::Result<CommitAck> {
        Err(logpose_types::LogPoseError::Message(
            "write is not used by planner tests".to_owned(),
        ))
    }

    async fn snapshot(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Ok(self.snapshot.clone())
    }

    async fn scan_exact(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
    ) -> logpose_types::Result<Vec<VisibleRecord>> {
        Ok(Vec::new())
    }

    async fn scan_exact_selected(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
        include_mutable: bool,
        immutable_unit_ids: Vec<String>,
    ) -> logpose_types::Result<Vec<VisibleRecord>> {
        if include_mutable {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for unit_id in immutable_unit_ids {
            records.extend(
                self.immutable_records
                    .get(&unit_id)
                    .cloned()
                    .unwrap_or_default(),
            );
        }

        let mut visible = Vec::new();
        let mut seen_ids = std::collections::BTreeSet::new();
        for record in records {
            if seen_ids.insert(record.id.clone()) {
                visible.push(record);
            }
        }

        Ok(visible)
    }

    async fn flush(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Err(logpose_types::LogPoseError::Message(
            "flush is not used by planner tests".to_owned(),
        ))
    }

    async fn compact(&self, _collection_name: &str) -> logpose_types::Result<Snapshot> {
        Err(logpose_types::LogPoseError::Message(
            "compact is not used by planner tests".to_owned(),
        ))
    }

    async fn stats(&self, _collection_name: &str) -> logpose_types::Result<CollectionStats> {
        Ok(self.stats.clone())
    }

    async fn stats_snapshot(
        &self,
        _collection_name: &str,
        _snapshot: Option<Snapshot>,
    ) -> logpose_types::Result<CollectionStats> {
        Ok(self.stats.clone())
    }

    async fn inspect(
        &self,
        _collection_name: &str,
        _target: InspectTarget,
    ) -> logpose_types::Result<InspectReport> {
        Err(logpose_types::LogPoseError::Message(
            "inspect is not used by planner tests".to_owned(),
        ))
    }
}

fn visible_record(id: &str, vector: Vec<f32>, metadata: serde_json::Value) -> VisibleRecord {
    VisibleRecord {
        id: RecordId::new(id),
        vector,
        metadata,
        seq_no: 1,
    }
}

fn scalar_summary(records: &[VisibleRecord]) -> BTreeMap<String, ScalarFieldStats> {
    let mut summary = BTreeMap::new();
    for record in records {
        let serde_json::Value::Object(fields) = &record.metadata else {
            continue;
        };
        for (field, value) in fields {
            let stats = summary
                .entry(field.clone())
                .or_insert_with(|| ScalarFieldStats {
                    present_count: 0,
                    null_count: 0,
                    distinct_count: 0,
                    min: None,
                    max: None,
                    value_counts: BTreeMap::new(),
                });
            stats.present_count += 1;
            let Some(scalar) = ScalarMetadataValue::from_json(value) else {
                continue;
            };
            if scalar == ScalarMetadataValue::Null {
                stats.null_count += 1;
            }
            let next = stats.value_counts.entry(scalar.summary_key()).or_insert(0);
            *next += 1;
            stats.distinct_count = stats.value_counts.len();
            if scalar != ScalarMetadataValue::Null {
                if stats.min.is_none() {
                    stats.min = Some(scalar.clone());
                }
                stats.max = Some(scalar);
            }
        }
    }
    summary
}
