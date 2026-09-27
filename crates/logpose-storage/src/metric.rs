//! Distance scoring used by the storage-side exact and ANN candidate paths.

use logpose_types::{DistanceMetric, LogPoseError, Result};
use std::cmp::Ordering;

pub(crate) fn storage_metric_value(
    metric: DistanceMetric,
    query: &[f32],
    candidate: &[f32],
) -> Result<f32> {
    if query.len() != candidate.len() {
        return Err(LogPoseError::DimensionMismatch {
            field: "vector".to_owned(),
            record_id: None,
            expected: query.len(),
            actual: candidate.len(),
        });
    }

    Ok(match metric {
        DistanceMetric::Dot => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| lhs * rhs)
            .sum(),
        DistanceMetric::Cosine => {
            let dot: f32 = query
                .iter()
                .zip(candidate)
                .map(|(lhs, rhs)| lhs * rhs)
                .sum();
            let query_norm = query.iter().map(|value| value * value).sum::<f32>().sqrt();
            let candidate_norm = candidate
                .iter()
                .map(|value| value * value)
                .sum::<f32>()
                .sqrt();
            if query_norm == 0.0 || candidate_norm == 0.0 {
                0.0
            } else {
                dot / (query_norm * candidate_norm)
            }
        }
        DistanceMetric::L2 => query
            .iter()
            .zip(candidate)
            .map(|(lhs, rhs)| {
                let delta = lhs - rhs;
                delta * delta
            })
            .sum::<f32>()
            .sqrt(),
    })
}

pub(crate) fn storage_metric_compare(metric: DistanceMetric, left: f32, right: f32) -> Ordering {
    match metric {
        DistanceMetric::Dot | DistanceMetric::Cosine => {
            left.partial_cmp(&right).unwrap_or(Ordering::Equal)
        }
        DistanceMetric::L2 => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
    }
}
