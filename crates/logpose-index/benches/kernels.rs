#![allow(missing_docs)]

use crc32fast as _;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use logpose_index::{
    kernels::{self, scalar},
    sq8::{Sq8Metric, Sq8Params},
};
use logpose_types as _;
use pulp as _;
use rayon as _;
use roaring as _;
use serde as _;
use serde_json as _;
use std::time::Duration;
use thiserror as _;

const DIMS: [usize; 3] = [128, 768, 1536];
/// Rows per batched call: large enough to amortize dispatch, small enough to
/// stay in L2 at 1536 dims for f32.
const BATCH_ROWS: usize = 128;

/// SplitMix64-driven uniform values in `[-1, 1)`.
fn vector(seed: u64, len: usize) -> Vec<f32> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^= z >> 31;
            (z >> 40) as f32 / (1_u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn pair_kernels(criterion: &mut Criterion) {
    eprintln!("kernels dispatch to {}", kernels::simd_level());
    for (name, simd, reference) in [
        (
            "dot",
            kernels::dot as fn(&[f32], &[f32]) -> f32,
            scalar::dot as fn(&[f32], &[f32]) -> f32,
        ),
        ("l2_squared", kernels::l2_squared, scalar::l2_squared),
    ] {
        let mut group = criterion.benchmark_group(name);
        for dims in DIMS {
            let a = vector(1, dims);
            let b = vector(2, dims);
            group.throughput(Throughput::Elements(dims as u64));
            group.bench_with_input(BenchmarkId::new("scalar", dims), &dims, |bench, _| {
                bench.iter(|| reference(black_box(&a), black_box(&b)));
            });
            group.bench_with_input(BenchmarkId::new("simd", dims), &dims, |bench, _| {
                bench.iter(|| simd(black_box(&a), black_box(&b)));
            });
        }
        group.finish();
    }
}

fn batched_kernels(criterion: &mut Criterion) {
    type Many = fn(&[f32], &[f32], usize, &mut [f32]);
    for (name, simd, reference) in [
        (
            "dot_many",
            kernels::dot_many as Many,
            scalar::dot_many as Many,
        ),
        (
            "l2_squared_many",
            kernels::l2_squared_many,
            scalar::l2_squared_many,
        ),
    ] {
        let mut group = criterion.benchmark_group(name);
        for dims in DIMS {
            let query = vector(3, dims);
            let matrix = vector(4, dims * BATCH_ROWS);
            let mut out = vec![0.0; BATCH_ROWS];
            group.throughput(Throughput::Elements((dims * BATCH_ROWS) as u64));
            group.bench_with_input(BenchmarkId::new("scalar", dims), &dims, |bench, _| {
                bench.iter(|| reference(black_box(&query), black_box(&matrix), dims, &mut out));
            });
            group.bench_with_input(BenchmarkId::new("simd", dims), &dims, |bench, _| {
                bench.iter(|| simd(black_box(&query), black_box(&matrix), dims, &mut out));
            });
        }
        group.finish();
    }
}

fn sq8_kernels(criterion: &mut Criterion) {
    for (name, metric) in [
        ("sq8_dot", Sq8Metric::Dot),
        ("sq8_l2_squared", Sq8Metric::L2Squared),
    ] {
        let mut group = criterion.benchmark_group(name);
        for dims in DIMS {
            let rows = vector(5, dims * BATCH_ROWS);
            let Ok(params) = Sq8Params::train(&rows, dims) else {
                continue;
            };
            let mut codes = vec![0_u8; dims * BATCH_ROWS];
            for (row, code) in rows.chunks_exact(dims).zip(codes.chunks_exact_mut(dims)) {
                if params.encode_into(row, code).is_err() {
                    return;
                }
            }
            let Ok(query) = params.query(metric, &vector(6, dims)) else {
                continue;
            };
            let code = &codes[..dims];
            let mut out = vec![0.0; BATCH_ROWS];

            group.throughput(Throughput::Elements(dims as u64));
            group.bench_with_input(BenchmarkId::new("single", dims), &dims, |bench, _| {
                bench.iter(|| query.estimate(black_box(code)));
            });
            group.throughput(Throughput::Elements((dims * BATCH_ROWS) as u64));
            group.bench_with_input(BenchmarkId::new("many", dims), &dims, |bench, _| {
                bench.iter(|| query.estimate_many(black_box(&codes), &mut out));
            });
        }
        group.finish();
    }
}

fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50)
}

criterion_group! {
    name = benches;
    config = config();
    targets = pair_kernels, batched_kernels, sq8_kernels
}
criterion_main!(benches);
