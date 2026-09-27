//! Brute-force ground truth, computed in the harness and independent of any engine.

use crate::dataset::{Dataset, Metric};
use std::{cmp::Ordering, collections::BinaryHeap, thread};

/// Closeness score where larger is always closer, for every metric.
///
/// L2 uses negative squared distance, which ranks identically to distance.
#[must_use]
pub fn closeness(metric: Metric, left: &[f32], right: &[f32]) -> f32 {
    match metric {
        Metric::L2 => -left
            .iter()
            .zip(right)
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f32>(),
        Metric::Dot => left.iter().zip(right).map(|(a, b)| a * b).sum(),
        Metric::Cosine => {
            let dot = left.iter().zip(right).map(|(a, b)| a * b).sum::<f32>();
            let left_norm = left.iter().map(|a| a * a).sum::<f32>().sqrt();
            let right_norm = right.iter().map(|b| b * b).sum::<f32>().sqrt();
            if left_norm == 0.0 || right_norm == 0.0 {
                0.0
            } else {
                dot / (left_norm * right_norm)
            }
        }
    }
}

/// A scored row. Ordering puts better rows first: higher score, then lower id.
#[derive(Clone, Copy, Debug)]
struct Scored {
    score: f32,
    id: u64,
}

impl Scored {
    fn better_first(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialEq for Scored {
    fn eq(&self, other: &Self) -> bool {
        self.better_first(other) == Ordering::Equal
    }
}

impl Eq for Scored {}

impl PartialOrd for Scored {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scored {
    /// Max-heap order is "worst on top", so the heap evicts the worst kept row.
    fn cmp(&self, other: &Self) -> Ordering {
        self.better_first(other)
    }
}

/// Exact top-`k` row ids for one query over rows admitted by `admit`, best first.
///
/// Ties break toward the lower row id.
pub fn exact_top_k<F>(dataset: &Dataset, query: &[f32], k: usize, admit: F) -> Vec<u64>
where
    F: Fn(usize) -> bool,
{
    if k == 0 {
        return Vec::new();
    }
    let mut heap = BinaryHeap::with_capacity(k + 1);
    for row in 0..dataset.len() {
        if !admit(row) {
            continue;
        }
        let candidate = Scored {
            score: closeness(dataset.metric, query, dataset.row(row)),
            id: row as u64,
        };
        if heap.len() < k {
            heap.push(candidate);
        } else if heap
            .peek()
            .is_some_and(|worst| candidate.better_first(worst) == Ordering::Less)
        {
            heap.pop();
            heap.push(candidate);
        }
    }
    let mut kept = heap.into_vec();
    kept.sort_by(Scored::better_first);
    kept.into_iter().map(|scored| scored.id).collect()
}

/// Exact top-`k` for every query, parallelized across available CPUs.
pub fn ground_truth<F>(dataset: &Dataset, k: usize, admit: F) -> Vec<Vec<u64>>
where
    F: Fn(usize) -> bool + Sync,
{
    let queries = dataset.query_count();
    let workers = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(1, queries.max(1));
    let mut results = vec![Vec::new(); queries];
    let chunk = queries.div_ceil(workers).max(1);
    thread::scope(|scope| {
        for (chunk_index, slots) in results.chunks_mut(chunk).enumerate() {
            let admit = &admit;
            scope.spawn(move || {
                for (offset, slot) in slots.iter_mut().enumerate() {
                    let query = chunk_index * chunk + offset;
                    *slot = exact_top_k(dataset, dataset.query(query), k, admit);
                }
            });
        }
    });
    results
}

#[cfg(test)]
mod tests {
    use super::{closeness, exact_top_k, ground_truth};
    use crate::dataset::{Dataset, DatasetSource, Metric, SyntheticSpec};

    fn tiny(metric: Metric) -> Dataset {
        Dataset {
            dims: 2,
            metric,
            base: vec![0.0, 0.0, 1.0, 0.0, 3.0, 0.0, 0.0, 2.0, 10.0, 10.0],
            queries: vec![1.0, 0.0, 0.0, 1.0],
            source: DatasetSource::Synthetic(SyntheticSpec {
                n: 5,
                queries: 2,
                dims: 2,
                clusters: 1,
                query_cluster_fraction: 1.0,
                spread: 0.0,
                seed: 0,
            }),
            reference_ground_truth: None,
        }
    }

    #[test]
    fn l2_oracle_returns_nearest_rows_in_order() {
        let dataset = tiny(Metric::L2);
        assert_eq!(
            exact_top_k(&dataset, dataset.query(0), 3, |_| true),
            vec![1, 0, 2]
        );
        // Rows 0 and 3 tie; the lower id wins.
        assert_eq!(
            exact_top_k(&dataset, dataset.query(1), 2, |_| true),
            vec![0, 3]
        );
    }

    #[test]
    fn dot_and_cosine_oracles_prefer_larger_similarity() {
        let dot = tiny(Metric::Dot);
        assert_eq!(exact_top_k(&dot, dot.query(0), 2, |_| true), vec![4, 2]);
        let cosine = tiny(Metric::Cosine);
        // Rows 1 and 2 both have cosine 1.0; ties break toward the lower id.
        assert_eq!(
            exact_top_k(&cosine, cosine.query(0), 2, |_| true),
            vec![1, 2]
        );
        assert!((closeness(Metric::Cosine, &[1.0, 0.0], &[0.0, 0.0])).abs() < f32::EPSILON);
    }

    #[test]
    fn oracle_applies_filter_and_handles_short_results() {
        let dataset = tiny(Metric::L2);
        let admitted = exact_top_k(&dataset, dataset.query(0), 10, |row| row % 2 == 0);
        assert_eq!(admitted, vec![0, 2, 4]);
        assert!(exact_top_k(&dataset, dataset.query(0), 0, |_| true).is_empty());
    }

    #[test]
    fn parallel_ground_truth_matches_serial_oracle() {
        let dataset = tiny(Metric::L2);
        let truth = ground_truth(&dataset, 2, |row| row != 1);
        assert_eq!(truth.len(), 2);
        for (query, ids) in truth.iter().enumerate() {
            assert_eq!(
                ids,
                &exact_top_k(&dataset, dataset.query(query), 2, |row| row != 1)
            );
        }
    }
}
