//! Policy unit tests: which segments the size-tiered policy picks, how it sizes jobs to the
//! maintenance-memory pool, and the write amplification it leads to.

use super::*;

/// A segment of `rows` rows, `deleted` of them deleted, stored at 100 bytes a row.
fn segment(unit: u32, rows: u32, deleted: u64) -> Candidate {
    Candidate {
        unit: UnitId(unit),
        rows,
        deleted,
        file_len: u64::from(rows) * 100,
    }
}

fn config() -> CompactionConfig {
    CompactionConfig {
        base_rows: 100,
        tier_ratio: 4,
        min_merge: 4,
        max_merge: 10,
        max_output_rows: 1_000_000,
        max_output_bytes: u64::MAX,
        deleted_ratio: 0.2,
        max_jobs_per_collection: 2,
    }
}

/// No vector fields, so a row's build bytes are its stored bytes.
fn policy(config: CompactionConfig, pool_bytes: u64) -> Policy {
    Policy::new(config, config.min_merge, pool_bytes, RowShape::default())
}

fn units(plan: &CompactionPlan) -> Vec<u32> {
    plan.inputs.iter().map(|unit| unit.0).collect()
}

#[test]
fn tiers_are_bounded_by_the_base_and_grow_by_the_ratio() {
    let policy = policy(config(), u64::MAX);
    assert_eq!(policy.tier(0), 0);
    assert_eq!(policy.tier(99), 0);
    assert_eq!(policy.tier(100), 1);
    assert_eq!(policy.tier(399), 1);
    assert_eq!(policy.tier(400), 2);
    assert_eq!(tier_with(u64::MAX, 1, 2), 64);
}

#[test]
fn fewer_than_min_merge_segments_of_a_tier_start_no_job() {
    let policy = policy(config(), u64::MAX);
    let segments = [segment(1, 10, 0), segment(2, 10, 0), segment(3, 10, 0)];
    assert!(policy.plan(&segments, &BTreeSet::new(), 2).is_empty());
}

#[test]
fn a_full_tier_is_merged_in_ascending_unit_order() {
    let policy = policy(config(), u64::MAX);
    // Tier 0: 1, 3, 4, 6; tier 1: 2, 5.
    let segments = [
        segment(1, 10, 0),
        segment(2, 150, 0),
        segment(3, 20, 0),
        segment(4, 30, 0),
        segment(5, 200, 0),
        segment(6, 40, 0),
    ];
    let plans = policy.plan(&segments, &BTreeSet::new(), 2);
    assert_eq!(plans.len(), 1);
    assert_eq!(units(&plans[0]), [1, 3, 4, 6]);
    assert_eq!(plans[0].reason, PlanReason::Tiered { tier: 0 });
    // 100 live rows at 100 bytes, plus the largest input (40 rows) read whole.
    assert_eq!(plans[0].build_bytes, 100 * 100 + 40 * 100);
}

#[test]
fn tiers_use_live_rows() {
    let policy = policy(config(), u64::MAX);
    // Unit 1 has 150 rows but only 90 live (its deleted fraction is 0.4, which also makes it a
    // deletion-driven candidate). The others are tier 0 by row count.
    let segments = [
        segment(1, 150, 60),
        segment(2, 10, 0),
        segment(3, 10, 0),
        segment(4, 10, 0),
    ];
    let plans = policy.plan(&segments, &BTreeSet::new(), 2);
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].reason, PlanReason::Deletions);
    assert_eq!(
        units(&plans[0]),
        [1, 2, 3, 4],
        "all four are tier 0 by live rows"
    );
}

#[test]
fn a_segment_past_the_deleted_ratio_is_rewritten_with_the_smallest_lower_tier_segments() {
    let policy = policy(config(), u64::MAX);
    let segments = [
        segment(1, 1000, 100), // tier 2 by 900 live rows, 10 % deleted
        segment(2, 1000, 400), // tier 2 by 600 live rows, 40 % deleted: the heavy one
        segment(3, 50, 0),     // tier 0
        segment(4, 20, 0),     // tier 0
        segment(5, 5000, 0),   // tier 3, never added to a tier 2 job
        segment(6, 30, 0),     // tier 0
        segment(7, 60, 0),     // tier 0
        segment(8, 200, 100),  // tier 1, 50 % deleted but fewer rows deleted than unit 2
    ];
    let plans = policy.plan(&segments, &BTreeSet::new(), 1);
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].reason, PlanReason::Deletions);
    // Unit 2 plus the three smallest segments in tier 2 or below: 4 (20), 6 (30), 3 (50).
    assert_eq!(units(&plans[0]), [2, 3, 4, 6]);

    let plans = policy.plan(&segments, &BTreeSet::new(), 2);
    assert_eq!(plans.len(), 2, "a second deletion-driven job takes unit 8");
    assert_eq!(plans[1].reason, PlanReason::Deletions);
    assert!(units(&plans[1]).contains(&8));
}

#[test]
fn a_tier_0_segment_is_never_rewritten_for_its_deletions_alone() {
    let policy = policy(config(), u64::MAX);
    // 90 of 99 rows deleted, but below base_rows: the tiers merge it with its peers instead.
    let segments = [segment(1, 99, 90), segment(2, 10, 0)];
    assert!(policy.plan(&segments, &BTreeSet::new(), 2).is_empty());
    let segments = [segment(1, 100, 90), segment(2, 10, 0)];
    let plans = policy.plan(&segments, &BTreeSet::new(), 2);
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].reason, PlanReason::Deletions);
    assert_eq!(units(&plans[0]), [1, 2]);
}

#[test]
fn reserved_segments_are_never_planned_twice_and_jobs_stop_at_the_slot_limit() {
    let policy = policy(config(), u64::MAX);
    let segments = (1..=12)
        .map(|unit| segment(unit, 10, 0))
        .collect::<Vec<_>>();
    let reserved = [UnitId(1), UnitId(2)].into_iter().collect::<BTreeSet<_>>();
    let plans = policy.plan(&segments, &reserved, 2);
    assert_eq!(
        plans.len(),
        1,
        "ten free segments make one job of max_merge"
    );
    assert_eq!(units(&plans[0]), (3..=12).collect::<Vec<_>>());
    let taken = plans[0].inputs.iter().copied().collect::<BTreeSet<_>>();
    assert!(taken.is_disjoint(&reserved));

    let small = CompactionConfig {
        max_merge: 4,
        ..config()
    };
    let plans =
        Policy::new(small, 4, u64::MAX, RowShape::default()).plan(&segments, &BTreeSet::new(), 2);
    assert_eq!(plans.len(), 2, "capped by the free slots");
    assert_eq!(units(&plans[0]), [1, 2, 3, 4]);
    assert_eq!(units(&plans[1]), [5, 6, 7, 8]);
}

#[test]
fn a_job_is_capped_at_half_the_memory_pool() {
    // Each segment holds 10 rows * 100 bytes, and the input being read adds 1,000: half the
    // pool (3,500) fits two of them (3,000), not three (4,000).
    let policy = Policy::new(config(), 2, 7_000, RowShape::default());
    let segments = (1..=5).map(|unit| segment(unit, 10, 0)).collect::<Vec<_>>();
    let plans = policy.plan(&segments, &BTreeSet::new(), 2);
    assert_eq!(plans.len(), 2);
    for plan in &plans {
        assert_eq!(plan.inputs.len(), 2, "{plan:?}");
        assert!(plan.build_bytes <= 3_500);
    }
}

#[test]
fn build_bytes_count_stored_rows_and_the_index_builds() {
    let shape = RowShape {
        vector_fields: 2,
        vector_dims: 96,
        indexed_scalar_fields: 3,
    };
    let inputs = [segment(1, 10, 5), segment(2, 20, 0)];
    // Per row: an f32 copy and an SQ8 code of every dimension (5 * 96), the graph build and its
    // encoding per vector field (2 * (3 * 141 + 64)), and the scalar indexes (3 * 32).
    let index = 5 * 96 + 2 * (3 * 141 + 64) + 3 * 32;
    assert_eq!(shape.index_bytes_per_row(), index);
    // 5 + 20 live rows at (100 stored + index) bytes each, plus the larger input's 2,000-byte
    // file.
    assert_eq!(build_bytes(&inputs, shape), 25 * (100 + index) + 2_000);
    // A flush holds the builder's copy of the memtable's payload and the same index build.
    assert_eq!(flush_build_bytes(25, 7_000, shape), 7_000 + 25 * index);
}

/// The build reads each input whole, deleted rows included, so a rewrite of a mostly deleted
/// segment is charged its whole file, not just its few live rows.
#[test]
fn build_bytes_charge_the_largest_input_whole_deleted_rows_included() {
    let shape = RowShape::default();
    // 10 of 1,000 rows live: 1,000 bytes of output, but the build holds the 100,000-byte file.
    let mostly_deleted = [segment(1, 1_000, 990)];
    assert_eq!(build_bytes(&mostly_deleted, shape), 10 * 100 + 100_000);
    // Inputs are read one at a time: only the largest is charged whole.
    let inputs = [segment(1, 10, 0), segment(2, 30, 0), segment(3, 20, 0)];
    assert_eq!(build_bytes(&inputs, shape), 60 * 100 + 30 * 100);
}

/// A segment built to half the pool (the largest a background job makes) that passes the
/// deleted ratio is still rewritten: a deletion-driven rewrite of one segment alone may take
/// the whole pool, since reading it whole beside its live rows needs more than half.
#[test]
fn a_segment_at_the_half_pool_cap_is_still_rewritten_for_its_deletions() {
    // 400 rows at 100 bytes: a 40,000-byte file. With 30 % deleted, the rewrite holds 28,000
    // bytes of output plus the file, 68,000 bytes: more than half of a 100,000-byte pool.
    let roomy = policy(config(), 100_000);
    let heavy = segment(1, 400, 120);
    let plans = roomy.plan(&[heavy, segment(2, 10, 0)], &BTreeSet::new(), 2);
    assert_eq!(plans.len(), 1, "{plans:?}");
    assert_eq!(plans[0].reason, PlanReason::Deletions);
    assert_eq!(
        units(&plans[0]),
        [1],
        "nothing joins a job past half the pool"
    );
    assert_eq!(plans[0].build_bytes, 68_000);

    // A segment that needs more than the whole pool is never planned.
    let tight = policy(config(), 60_000);
    assert!(tight.plan(&[heavy], &BTreeSet::new(), 2).is_empty());
}

#[test]
fn outputs_stay_within_max_output_rows_and_vector_bytes() {
    let rows = CompactionConfig {
        max_output_rows: 25,
        ..config()
    };
    let segments = (1..=6).map(|unit| segment(unit, 10, 0)).collect::<Vec<_>>();
    let plans = policy(rows, u64::MAX).plan(&segments, &BTreeSet::new(), 1);
    assert_eq!(units(&plans[0]), [1, 2]);

    let bytes = CompactionConfig {
        max_output_bytes: 30 * 4 * 4,
        ..config()
    };
    let shape = RowShape {
        vector_fields: 1,
        vector_dims: 4,
        indexed_scalar_fields: 0,
    };
    let plans = Policy::new(bytes, 4, u64::MAX, shape).plan(&segments, &BTreeSet::new(), 1);
    assert_eq!(units(&plans[0]), [1, 2, 3], "30 rows of 16 vector bytes");
}

#[test]
fn a_top_tier_segment_at_the_cap_is_never_rewritten() {
    let capped = CompactionConfig {
        max_output_rows: 1000,
        ..config()
    };
    let segments = (1..=6)
        .map(|unit| segment(unit, 900, 0))
        .collect::<Vec<_>>();
    assert!(
        policy(capped, u64::MAX)
            .plan(&segments, &BTreeSet::new(), 2)
            .is_empty()
    );
}

#[test]
fn background_compaction_is_off_at_the_maximum_threshold_but_explicit_still_plans() {
    let policy = Policy::new(config(), usize::MAX, u64::MAX, RowShape::default());
    let segments = (1..=8).map(|unit| segment(unit, 10, 9)).collect::<Vec<_>>();
    assert!(policy.plan(&segments, &BTreeSet::new(), 2).is_empty());
    let plan = policy
        .plan_explicit(&segments, &BTreeSet::new())
        .expect("explicit plan");
    assert_eq!(plan.inputs.len(), 8);
    assert_eq!(plan.reason, PlanReason::Explicit);
}

#[test]
fn an_explicit_compaction_takes_what_fits_the_whole_pool_or_is_left_for_the_scheduler_to_decline() {
    let segments = (1..=5).map(|unit| segment(unit, 10, 0)).collect::<Vec<_>>();
    // Two segments need 3,000 bytes, three 4,000, four 5,000.
    let fits_three = policy(config(), 4_000);
    let plan = fits_three
        .plan_explicit(&segments, &BTreeSet::new())
        .expect("plan");
    assert_eq!(units(&plan), [1, 2, 3]);

    let too_small = policy(config(), 1_500);
    let plan = too_small
        .plan_explicit(&segments, &BTreeSet::new())
        .expect("the first two are always planned");
    assert_eq!(units(&plan), [1, 2]);
    assert!(plan.build_bytes > too_small.pool_bytes);

    assert!(
        fits_three
            .plan_explicit(&segments[..1], &BTreeSet::new())
            .is_none(),
        "one segment is nothing to compact"
    );
}

/// Simulate `flushes` flushes of `rows` rows each, running the policy after every one and
/// committing every job it plans at once. Returns the rows compactions rewrote and the segments
/// left.
fn simulate(policy: &Policy, flushes: u32, rows: u32) -> (u64, Vec<Candidate>) {
    let mut segments = Vec::new();
    let mut next_unit = 0;
    let mut rewritten = 0;
    for _ in 0..flushes {
        next_unit += 1;
        segments.push(segment(next_unit, rows, 0));
        loop {
            let plans = policy.plan(&segments, &BTreeSet::new(), 2);
            if plans.is_empty() {
                break;
            }
            for plan in plans {
                let live = segments
                    .iter()
                    .filter(|segment| plan.inputs.contains(&segment.unit))
                    .map(Candidate::live)
                    .sum::<u64>();
                segments.retain(|segment| !plan.inputs.contains(&segment.unit));
                next_unit += 1;
                segments.push(segment(next_unit, u32::try_from(live).expect("fits"), 0));
                rewritten += live;
            }
            segments.sort_by_key(|segment| segment.unit);
        }
    }
    (rewritten, segments)
}

#[test]
fn write_amplification_is_one_rewrite_per_tier_climbed() {
    // Flushes of 25 rows into a policy whose tiers are 100, 400, 1600, 6400 rows and whose
    // jobs merge exactly four segments: every row climbs one tier per rewrite.
    let exact = CompactionConfig {
        max_merge: 4,
        ..config()
    };
    let exact_policy = policy(exact, u64::MAX);
    let flushes = 1024;
    let (rewritten, segments) = simulate(&exact_policy, flushes, 25);
    let ingested = u64::from(flushes) * 25;
    // 25 -> 100 -> 400 -> 1600 -> 6400 -> 25600: five rewrites per row, log4(25600 / 25).
    assert_eq!(rewritten, 5 * ingested);
    assert_eq!(segments.len(), 1);

    // With the default merge width, the rewrites stay within one per tier the data spans.
    let tiered = policy(config(), u64::MAX);
    for flushes in [7, 64, 333, 1000] {
        let (rewritten, segments) = simulate(&tiered, flushes, 25);
        let ingested = u64::from(flushes) * 25;
        let tiers = u64::from(tiered.tier(ingested)) + 1;
        assert!(
            rewritten <= tiers * ingested,
            "{flushes} flushes: {rewritten} rows rewritten for {ingested} ingested over \
             {tiers} tiers"
        );
        // Fewer than min_merge segments are left per tier.
        let mut per_tier = BTreeMap::<u8, usize>::new();
        for segment in &segments {
            *per_tier.entry(tiered.tier(segment.live())).or_default() += 1;
        }
        assert!(
            per_tier.values().all(|count| *count < 4),
            "{flushes} flushes: {per_tier:?}"
        );
    }
}
