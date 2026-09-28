//! `EXPLAIN`: a query's plan as an operator tree, each operator with its estimated and actual
//! work and, for the per-segment strategy, the reason it was chosen.
//!
//! A vector search is
//!
//! ```text
//! Project                         read the final rows
//! └─ Merge                        global heap merge of the units' top k
//!    ├─ Rerank                    exact f32 scores of a unit's candidates, its top k
//!    │  └─ TopK                   the unit's best `k * rerank_factor` candidates
//!    │     └─ GraphScan | ExactScan
//!    │        └─ MaskDeletes      B := B AND NOT deleted
//!    │           └─ BitmapProbe   B := rows matching the filter (exact cardinality)
//!    │              └─ SegmentSource
//!    └─ ...                       one branch per unit; memtables scan exactly, no rerank
//! ```
//!
//! and a query without a vector is `Project` over `Merge` over one `OrderedScan` per unit.
//! Estimates come from the [cost model](crate::cost); actual counts from execution. Times
//! (`micros`) are the model's price in estimates and are measured only in profile mode.

use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

/// An operator of a plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    /// One unit (segment or memtable) and its residency.
    SegmentSource,
    /// The filter compiled to the unit's bitmap of matching rows, through scalar indexes,
    /// columns, or `$extra` blocks.
    BitmapProbe,
    /// Deleted rows removed from the bitmap.
    MaskDeletes,
    /// Every allowed row scored exactly (SQ8 codes or f32).
    ExactScan,
    /// A graph walk over the allowed rows (admit-only or ACORN-1 style).
    GraphScan,
    /// The best rows of its input by distance.
    TopK,
    /// Exact f32 scores of approximate candidates, keeping the best `k`.
    Rerank,
    /// The global heap merge of the units' results.
    Merge,
    /// Rows read in a field's or the key's order (a query without a vector).
    OrderedScan,
    /// The final rows read and projected.
    Project,
}

impl Operator {
    /// The name `EXPLAIN` prints.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::SegmentSource => "SegmentSource",
            Self::BitmapProbe => "BitmapProbe",
            Self::MaskDeletes => "MaskDeletes",
            Self::ExactScan => "ExactScan",
            Self::GraphScan => "GraphScan",
            Self::TopK => "TopK",
            Self::Rerank => "Rerank",
            Self::Merge => "Merge",
            Self::OrderedScan => "OrderedScan",
            Self::Project => "Project",
        }
    }
}

/// Work of one operator: estimated by the planner or counted by execution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct OperatorStats {
    /// Rows the operator produces.
    pub rows: u64,
    /// Distance computations (SQ8 or f32).
    pub distances: u64,
    /// Graph adjacency lists read.
    pub hops: u64,
    /// Bytes read from resident sections.
    pub resident_bytes: u64,
    /// Bytes read from disk through the buffer cache.
    pub cold_bytes: u64,
    /// Estimated: the cost model's price. Actual: measured time (profile mode only; 0
    /// otherwise).
    pub micros: f64,
}

impl OperatorStats {
    /// Stats with only a row count.
    #[must_use]
    pub fn rows(rows: u64) -> Self {
        Self {
            rows,
            ..Self::default()
        }
    }

    fn render(&self, timings: bool) -> String {
        let mut parts = vec![format!("rows={}", self.rows)];
        if self.distances > 0 {
            parts.push(format!("dist={}", self.distances));
        }
        if self.hops > 0 {
            parts.push(format!("hops={}", self.hops));
        }
        if self.resident_bytes > 0 {
            parts.push(format!("resident={}", bytes(self.resident_bytes)));
        }
        if self.cold_bytes > 0 {
            parts.push(format!("cold={}", bytes(self.cold_bytes)));
        }
        if timings && self.micros > 0.0 {
            parts.push(format!("{:.0}us", self.micros));
        }
        parts.join(" ")
    }
}

fn bytes(value: u64) -> String {
    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    if value >= MIB {
        format!("{:.1}MiB", value as f64 / MIB as f64)
    } else if value >= KIB {
        format!("{:.1}KiB", value as f64 / KIB as f64)
    } else {
        format!("{value}B")
    }
}

/// One operator of a plan and its inputs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanNode {
    /// The operator.
    pub operator: Operator,
    /// Its parameters, such as `unit=0000002a` or `acorn ef=64->128`.
    pub detail: String,
    /// Why the planner chose this operator, for per-unit strategies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The planner's estimate.
    pub estimated: OperatorStats,
    /// What execution did.
    pub actual: OperatorStats,
    /// Its inputs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    /// A node without inputs.
    #[must_use]
    pub fn new(operator: Operator, detail: impl Into<String>) -> Self {
        Self {
            operator,
            detail: detail.into(),
            reason: None,
            estimated: OperatorStats::default(),
            actual: OperatorStats::default(),
            children: Vec::new(),
        }
    }

    /// This node with `reason`.
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// This node with estimated and actual stats.
    #[must_use]
    pub fn with_stats(mut self, estimated: OperatorStats, actual: OperatorStats) -> Self {
        self.estimated = estimated;
        self.actual = actual;
        self
    }

    /// This node over `child`.
    #[must_use]
    pub fn over(mut self, child: PlanNode) -> Self {
        self.children.push(child);
        self
    }

    /// Every node of the tree, depth first, this one first.
    #[must_use]
    pub fn walk(&self) -> Vec<&PlanNode> {
        let mut out = vec![self];
        for child in &self.children {
            out.extend(child.walk());
        }
        out
    }

    /// The tree as indented text, one operator per line with its estimated and actual stats
    /// and, below a strategy, its reason. `timings` includes the micros (estimated prices and
    /// measured times); without it the text depends only on the plan and the data, so it is
    /// stable across runs.
    #[must_use]
    pub fn render(&self, timings: bool) -> String {
        let mut out = String::new();
        self.render_into(&mut out, "", "", timings);
        out
    }

    fn render_into(&self, out: &mut String, first: &str, rest: &str, timings: bool) {
        let detail = if self.detail.is_empty() {
            String::new()
        } else {
            format!(" {}", self.detail)
        };
        let _ = writeln!(
            out,
            "{first}{}{detail} (est {}) (actual {})",
            self.operator.name(),
            self.estimated.render(timings),
            self.actual.render(timings)
        );
        let child_rest = if self.children.is_empty() {
            "   "
        } else {
            "│  "
        };
        if let Some(reason) = &self.reason {
            let _ = writeln!(out, "{rest}{child_rest}reason: {reason}");
        }
        let count = self.children.len();
        for (index, child) in self.children.iter().enumerate() {
            let last = index + 1 == count;
            let (branch, indent) = if last {
                ("└─ ", "   ")
            } else {
                ("├─ ", "│  ")
            };
            child.render_into(
                out,
                &format!("{rest}{branch}"),
                &format!("{rest}{indent}"),
                timings,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_an_indented_tree_with_reasons() {
        let scan = PlanNode::new(Operator::GraphScan, "unit=00000001 admit ef=64")
            .with_reason("cheapest")
            .with_stats(
                OperatorStats {
                    rows: 40,
                    distances: 1_800,
                    hops: 110,
                    resident_bytes: 300_000,
                    micros: 250.0,
                    ..OperatorStats::default()
                },
                OperatorStats {
                    rows: 40,
                    distances: 1_750,
                    hops: 101,
                    micros: 240.0,
                    ..OperatorStats::default()
                },
            )
            .over(PlanNode::new(Operator::SegmentSource, "unit=00000001"));
        let plan = PlanNode::new(Operator::Project, "k=10").over(
            PlanNode::new(Operator::Merge, "units=2")
                .over(scan)
                .over(PlanNode::new(
                    Operator::ExactScan,
                    "unit=00000002 memtable f32",
                )),
        );
        assert_eq!(
            plan.render(false),
            "Project k=10 (est rows=0) (actual rows=0)\n\
             └─ Merge units=2 (est rows=0) (actual rows=0)\n\
             \x20  ├─ GraphScan unit=00000001 admit ef=64 (est rows=40 dist=1800 hops=110 \
             resident=293.0KiB) (actual rows=40 dist=1750 hops=101)\n\
             \x20  │  │  reason: cheapest\n\
             \x20  │  └─ SegmentSource unit=00000001 (est rows=0) (actual rows=0)\n\
             \x20  └─ ExactScan unit=00000002 memtable f32 (est rows=0) (actual rows=0)\n"
        );
        assert!(
            plan.render(true)
                .contains("dist=1800 hops=110 resident=293.0KiB 250us")
        );
        assert_eq!(plan.walk().len(), 5);
    }
}
