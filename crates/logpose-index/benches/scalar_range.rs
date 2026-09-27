#![allow(missing_docs)]

use crc32fast as _;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use logpose_index::scalar::{
    Direction, InvertedIndex, KeyKind, MutableSortedIndex, RoaringBitmap, ScalarIndex,
    ScalarIndexBuilder, ScalarKey, SortedIndex,
};
use logpose_types as _;
use pulp as _;
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use std::{ops::Bound, time::Duration};
use thiserror as _;

const ROWS: u32 = 1_000_000;
const SELECTIVITIES: [f64; 5] = [0.001, 0.01, 0.1, 0.5, 0.9];

/// Deterministic SplitMix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// One million single-valued rows with keys uniform in `0..domain`.
fn build(domain: u64) -> (InvertedIndex, SortedIndex) {
    let mut rng = Rng(42);
    let mut builder = ScalarIndexBuilder::new(KeyKind::Int);
    for row in 0..ROWS {
        builder
            .insert(row, ScalarKey::Int((rng.next() % domain) as i64))
            .expect("int key");
    }
    builder.build().expect("builds")
}

fn bounds(domain: u64, selectivity: f64) -> (ScalarKey, ScalarKey) {
    let width = ((domain as f64) * selectivity) as i64;
    let start = (domain as i64 - width) / 3;
    (ScalarKey::Int(start), ScalarKey::Int(start + width))
}

fn range_benchmarks(criterion: &mut Criterion) {
    let domain = u64::from(ROWS);
    let (inverted, sorted) = build(domain);
    let (_, low_cardinality) = build(1_000);
    let low_cardinality_inverted = InvertedIndex::from_sorted(&low_cardinality);
    let mut mutable = MutableSortedIndex::new(KeyKind::Int);
    let mut rng = Rng(42);
    for row in 0..ROWS {
        mutable
            .insert(row, ScalarKey::Int((rng.next() % domain) as i64))
            .expect("int key");
    }

    let mut group = criterion.benchmark_group("scalar_range_1m");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));

    for selectivity in SELECTIVITIES {
        let label = format!("{:.1}%", selectivity * 100.0);
        let (low, high) = bounds(domain, selectivity);
        let range = (Bound::Included(&low), Bound::Excluded(&high));
        group.bench_with_input(BenchmarkId::new("sorted_range", &label), &range, |b, r| {
            b.iter(|| black_box(sorted.range(r.0, r.1)));
        });
        group.bench_with_input(
            BenchmarkId::new("inverted_range", &label),
            &range,
            |b, r| {
                b.iter(|| black_box(inverted.range(r.0, r.1)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("mutable_sorted_range", &label),
            &range,
            |b, r| {
                b.iter(|| black_box(mutable.range(r.0, r.1)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("sorted_count_range", &label),
            &range,
            |b, r| {
                b.iter(|| black_box(sorted.count_range(r.0, r.1)));
            },
        );

        let (low, high) = bounds(1_000, selectivity);
        let range = (Bound::Included(&low), Bound::Excluded(&high));
        group.bench_with_input(
            BenchmarkId::new("low_card_sorted_range", &label),
            &range,
            |b, r| b.iter(|| black_box(low_cardinality.range(r.0, r.1))),
        );
        group.bench_with_input(
            BenchmarkId::new("low_card_inverted_range", &label),
            &range,
            |b, r| b.iter(|| black_box(low_cardinality_inverted.range(r.0, r.1))),
        );
    }

    // ORDER BY key LIMIT 10 under a 10 percent allow bitmap.
    let allow: RoaringBitmap = (0..ROWS).filter(|row| row % 10 == 3).collect();
    group.bench_function("sorted_top10_allow_10%", |b| {
        b.iter(|| {
            black_box(
                sorted
                    .iter_ordered(Direction::Descending, Some(&allow))
                    .take(10)
                    .count(),
            )
        });
    });
    group.bench_function("sorted_equals", |b| {
        let key = ScalarKey::Int(123_456);
        b.iter(|| black_box(sorted.equals(&key)));
    });
    group.bench_function("inverted_equals", |b| {
        let key = ScalarKey::Int(123_456);
        b.iter(|| black_box(inverted.equals(&key)));
    });
    group.finish();

    let mut group = criterion.benchmark_group("scalar_build_1m");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("builder_sorted_and_inverted", |b| {
        b.iter(|| black_box(build(domain)));
    });
    group.bench_function("freeze_mutable_sorted", |b| {
        b.iter(|| black_box(mutable.freeze(Some).expect("identity remap")));
    });
    let bytes = sorted.to_bytes().expect("encodes");
    group.bench_function("sorted_decode", |b| {
        b.iter(|| black_box(SortedIndex::from_bytes(&bytes).expect("decodes")));
    });
    group.finish();
}

criterion_group!(benches, range_benchmarks);
criterion_main!(benches);
