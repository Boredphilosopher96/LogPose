//! Scalar attributes and filters with controlled selectivity.
//!
//! The harness keeps one rank column per filter mode. Each column is a
//! permutation of `0..n`, and a filter at selectivity `s` matches the rows whose
//! rank is below `ceil(s * n)`, so the matching count is exact. Which rows get
//! the low ranks decides the mode:
//!
//! - uncorrelated: a seeded random permutation
//! - anti-correlated: rows ranked by distance to the queries, farthest first, so
//!   selective filters keep only rows far from every query (the hard case for
//!   filtered ANN)
//!
//! How that membership reaches the engine is the [`FilterStyle`]: an equality
//! flag per filter (the common "tenant = x" shape, and one the current planner
//! estimates exactly), or a range over the rank column.

use crate::{dataset::Dataset, oracle::closeness, rng::SplitMix64};
use clap::ValueEnum;
use logpose_query::FilterExpr;
use serde::Serialize;

const STREAM_UNCORRELATED: u64 = 101;

/// How matching rows relate to the query vectors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum FilterMode {
    /// Matching rows are a uniform random subset.
    Uncorrelated,
    /// Matching rows are the rows farthest from the queries.
    AntiCorrelated,
}

impl FilterMode {
    /// Short label used in case and field names.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Uncorrelated => "uncorr",
            Self::AntiCorrelated => "anti",
        }
    }
}

/// How a filter is expressed to the engine.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum FilterStyle {
    /// One 0/1 field per filter, queried with `field == 1`.
    #[default]
    Equality,
    /// One rank field per mode, queried with `rank < threshold`.
    Range,
}

/// Per-row filter attributes for every mode.
#[derive(Clone, Debug)]
pub struct Attributes {
    /// Uncorrelated attribute per row.
    pub uncorrelated: Vec<u32>,
    /// Anti-correlated attribute per row.
    pub anti_correlated: Vec<u32>,
}

impl Attributes {
    /// Build both attribute columns for a dataset.
    #[must_use]
    pub fn build(dataset: &Dataset, seed: u64) -> Self {
        let n = dataset.len();
        let mut uncorrelated = (0..n as u32).collect::<Vec<_>>();
        SplitMix64::stream(seed, STREAM_UNCORRELATED).shuffle(&mut uncorrelated);
        Self {
            uncorrelated,
            anti_correlated: anti_correlated_ranks(dataset),
        }
    }

    /// Attribute column for a mode.
    #[must_use]
    pub fn column(&self, mode: FilterMode) -> &[u32] {
        match mode {
            FilterMode::Uncorrelated => &self.uncorrelated,
            FilterMode::AntiCorrelated => &self.anti_correlated,
        }
    }
}

/// Rank rows by their best closeness to any query, farthest first.
///
/// Every query counts, so a selective anti-correlated filter keeps only rows
/// that are far from all of them. Ties break toward the lower row id so the
/// ranking is deterministic.
#[must_use]
pub fn anti_correlated_ranks(dataset: &Dataset) -> Vec<u32> {
    let mut nearness = (0..dataset.len())
        .map(|row| {
            let best = (0..dataset.query_count())
                .map(|query| closeness(dataset.metric, dataset.query(query), dataset.row(row)))
                .fold(f32::NEG_INFINITY, f32::max);
            (best, row)
        })
        .collect::<Vec<_>>();
    nearness.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    let mut ranks = vec![0_u32; dataset.len()];
    for (rank, (_, row)) in nearness.into_iter().enumerate() {
        ranks[row] = rank as u32;
    }
    ranks
}

/// A filter that matches a target fraction of rows.
#[derive(Clone, Debug, Serialize)]
pub struct FilterSpec {
    /// Attribute mode.
    pub mode: FilterMode,
    /// How the predicate is expressed.
    pub style: FilterStyle,
    /// Requested fraction of matching rows.
    pub target_selectivity: f64,
    /// Rows match when their rank is strictly below this value.
    pub threshold: u32,
    /// Metadata field the predicate reads.
    pub field: String,
}

impl FilterSpec {
    /// Build a filter matching `ceil(selectivity * n)` rows, at least one.
    #[must_use]
    pub fn new(mode: FilterMode, style: FilterStyle, target_selectivity: f64, n: usize) -> Self {
        let threshold = ((target_selectivity * n as f64).ceil() as usize).clamp(1, n.max(1));
        let field = match style {
            FilterStyle::Equality => format!(
                "{}_{}pct",
                mode.label(),
                format_percent(target_selectivity).replace('.', "_")
            ),
            FilterStyle::Range => format!("{}_rank", mode.label()),
        };
        Self {
            mode,
            style,
            target_selectivity,
            threshold: threshold as u32,
            field,
        }
    }

    /// Whether a row with this rank matches.
    #[must_use]
    pub fn matches(&self, rank: u32) -> bool {
        rank < self.threshold
    }

    /// The value stored in [`Self::field`] for a row with this rank.
    #[must_use]
    pub fn stored_value(&self, rank: u32) -> i64 {
        match self.style {
            FilterStyle::Equality => i64::from(self.matches(rank)),
            FilterStyle::Range => i64::from(rank),
        }
    }

    /// The filter in the query crate's predicate AST.
    #[must_use]
    pub fn predicate(&self) -> FilterExpr {
        match self.style {
            FilterStyle::Equality => FilterExpr::eq(self.field.clone(), 1_i64),
            FilterStyle::Range => FilterExpr::lt(self.field.clone(), i64::from(self.threshold)),
        }
    }
}

/// Format a fraction as a percentage without trailing zeros, e.g. `0.001` as `0.1`.
#[must_use]
pub fn format_percent(fraction: f64) -> String {
    let text = format!("{:.4}", fraction * 100.0);
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}

#[cfg(test)]
mod tests {
    use super::{Attributes, FilterMode, FilterSpec, FilterStyle, format_percent};
    use crate::{
        dataset::{Metric, SyntheticSpec, generate_synthetic},
        oracle::closeness,
    };

    fn dataset() -> anyhow::Result<crate::dataset::Dataset> {
        generate_synthetic(
            &SyntheticSpec {
                n: 2_000,
                queries: 16,
                dims: 8,
                clusters: 8,
                query_cluster_fraction: 0.25,
                spread: 0.3,
                seed: 5,
            },
            Metric::L2,
        )
    }

    #[test]
    fn filters_hit_requested_selectivity() -> anyhow::Result<()> {
        let dataset = dataset()?;
        let attributes = Attributes::build(&dataset, 5);
        for mode in [FilterMode::Uncorrelated, FilterMode::AntiCorrelated] {
            for selectivity in [0.001, 0.01, 0.1, 0.5, 0.99] {
                let filter =
                    FilterSpec::new(mode, FilterStyle::Equality, selectivity, dataset.len());
                let matched = attributes
                    .column(mode)
                    .iter()
                    .filter(|value| filter.matches(**value))
                    .count();
                let expected = (selectivity * dataset.len() as f64).ceil() as usize;
                assert_eq!(matched, expected, "{mode:?} at {selectivity}");
            }
        }
        Ok(())
    }

    #[test]
    fn tiny_selectivity_still_matches_one_row() {
        let filter = FilterSpec::new(FilterMode::Uncorrelated, FilterStyle::Range, 0.001, 10);
        assert_eq!(filter.threshold, 1);
    }

    #[test]
    fn styles_name_fields_and_encode_membership() {
        assert_eq!(format_percent(0.001), "0.1");
        assert_eq!(format_percent(0.5), "50");
        let equality = FilterSpec::new(
            FilterMode::AntiCorrelated,
            FilterStyle::Equality,
            0.001,
            5_000,
        );
        assert_eq!(equality.field, "anti_0_1pct");
        assert_eq!(equality.threshold, 5);
        assert_eq!(equality.stored_value(4), 1);
        assert_eq!(equality.stored_value(5), 0);
        let range = FilterSpec::new(FilterMode::Uncorrelated, FilterStyle::Range, 0.5, 10);
        assert_eq!(range.field, "uncorr_rank");
        assert_eq!(range.stored_value(7), 7);
        assert!(range.matches(4) && !range.matches(5));
    }

    #[test]
    fn anti_correlated_rows_are_farther_from_queries() -> anyhow::Result<()> {
        let dataset = dataset()?;
        let attributes = Attributes::build(&dataset, 5);
        let filter = FilterSpec::new(
            FilterMode::AntiCorrelated,
            FilterStyle::Equality,
            0.1,
            dataset.len(),
        );
        let uncorrelated = FilterSpec::new(
            FilterMode::Uncorrelated,
            FilterStyle::Equality,
            0.1,
            dataset.len(),
        );
        let mean_best = |column: &[u32], spec: &FilterSpec| {
            let rows = (0..dataset.len())
                .filter(|row| spec.matches(column[*row]))
                .collect::<Vec<_>>();
            rows.iter()
                .map(|row| {
                    (0..dataset.query_count())
                        .map(|query| {
                            closeness(dataset.metric, dataset.query(query), dataset.row(*row))
                        })
                        .fold(f32::NEG_INFINITY, f32::max)
                })
                .sum::<f32>()
                / rows.len() as f32
        };
        let anti = mean_best(&attributes.anti_correlated, &filter);
        let random = mean_best(&attributes.uncorrelated, &uncorrelated);
        assert!(
            anti < random,
            "anti {anti} should be farther than random {random}"
        );
        Ok(())
    }

    #[test]
    fn anti_correlated_filter_excludes_rows_near_any_query() -> anyhow::Result<()> {
        // More queries than the old 64-anchor cap, so every query must count.
        let dataset = generate_synthetic(
            &SyntheticSpec {
                n: 1_000,
                queries: 100,
                dims: 8,
                clusters: 8,
                query_cluster_fraction: 1.0,
                spread: 0.3,
                seed: 9,
            },
            Metric::L2,
        )?;
        let attributes = Attributes::build(&dataset, 9);
        let filter = FilterSpec::new(
            FilterMode::AntiCorrelated,
            FilterStyle::Equality,
            0.1,
            dataset.len(),
        );
        let best = |row: usize| {
            (0..dataset.query_count())
                .map(|query| closeness(dataset.metric, dataset.query(query), dataset.row(row)))
                .fold(f32::NEG_INFINITY, f32::max)
        };
        let (matched, rest): (Vec<_>, Vec<_>) =
            (0..dataset.len()).partition(|row| filter.matches(attributes.anti_correlated[*row]));
        let nearest_matched = matched
            .iter()
            .map(|row| best(*row))
            .fold(f32::NEG_INFINITY, f32::max);
        let farthest_rest = rest
            .iter()
            .map(|row| best(*row))
            .fold(f32::INFINITY, f32::min);
        assert!(
            nearest_matched <= farthest_rest,
            "a matching row ({nearest_matched}) is closer to some query than a non-matching row ({farthest_rest})"
        );
        Ok(())
    }

    #[test]
    fn attributes_are_deterministic() -> anyhow::Result<()> {
        let dataset = dataset()?;
        let first = Attributes::build(&dataset, 5);
        let second = Attributes::build(&dataset, 5);
        assert_eq!(first.uncorrelated, second.uncorrelated);
        assert_eq!(first.anti_correlated, second.anti_correlated);
        Ok(())
    }
}
