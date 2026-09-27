//! Helpers shared by the crate's unit tests.

use logpose_types::{PutRecord, RecordId, VisibleRecord, WriteOperation};
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn put(id: &str, vector: Vec<f32>) -> WriteOperation {
    WriteOperation::Put(PutRecord {
        id: RecordId::new(id),
        vector,
        metadata: json!({"key": id}),
    })
}

pub(crate) fn visible_ids(records: &[VisibleRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| record.id.as_str().to_owned())
        .collect()
}

pub(crate) fn unique_temp_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("logpose-{prefix}-{suffix}"));
    fs::create_dir_all(&dir).expect("temp dir should be created");
    dir
}
