//! Property-style tests against a naive scan oracle, plus codec and error
//! cases.

use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Bound, RangeBounds},
};

/// Deterministic SplitMix64, so failures reproduce from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }

    fn percent(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for index in (1..items.len()).rev() {
            let other = self.below(index as u64 + 1) as usize;
            items.swap(index, other);
        }
    }
}

const KINDS: [KeyKind; 4] = [KeyKind::Bool, KeyKind::Int, KeyKind::Float, KeyKind::Str];

fn float(value: f64) -> ScalarKey {
    ScalarKey::float(value).expect("test floats are not NaN")
}

fn random_key(rng: &mut Rng, kind: KeyKind) -> ScalarKey {
    match kind {
        KeyKind::Bool => ScalarKey::Bool(rng.percent(50)),
        KeyKind::Int => match rng.below(20) {
            0 => ScalarKey::Int(i64::MIN),
            1 => ScalarKey::Int(i64::MAX),
            _ => ScalarKey::Int(rng.below(40) as i64 - 20),
        },
        KeyKind::Float => {
            const SPECIAL: [f64; 7] = [
                f64::NEG_INFINITY,
                -0.0,
                0.0,
                1e300,
                -1e-300,
                f64::INFINITY,
                2.5,
            ];
            if rng.percent(25) {
                float(SPECIAL[rng.below(SPECIAL.len() as u64) as usize])
            } else {
                float(rng.below(40) as f64 / 4.0 - 5.0)
            }
        }
        KeyKind::Str => {
            let len = rng.below(4) as usize;
            let text: String = (0..len)
                .map(|_| ['a', 'b', 'c', 'é'][rng.below(4) as usize])
                .collect();
            ScalarKey::string(text)
        }
    }
}

/// A query key: usually of the index's kind, sometimes of another.
fn query_key(rng: &mut Rng, kind: KeyKind) -> ScalarKey {
    if rng.percent(8) {
        let other = KINDS[rng.below(4) as usize];
        random_key(rng, other)
    } else {
        random_key(rng, kind)
    }
}

fn random_bound(rng: &mut Rng, kind: KeyKind) -> Bound<ScalarKey> {
    match rng.below(5) {
        0 => Bound::Unbounded,
        1 | 2 => Bound::Included(query_key(rng, kind)),
        _ => Bound::Excluded(query_key(rng, kind)),
    }
}

fn random_prefix(rng: &mut Rng) -> String {
    let len = rng.below(3) as usize;
    (0..len)
        .map(|_| ['a', 'b', 'c', 'é'][rng.below(4) as usize])
        .collect()
}

/// Per-row truth: null, or a set of values (possibly empty).
#[derive(Clone, Debug)]
enum Cell {
    Null,
    Values(BTreeSet<ScalarKey>),
}

#[derive(Clone, Debug)]
struct Oracle {
    kind: KeyKind,
    rows: BTreeMap<u32, Cell>,
}

impl Oracle {
    fn random(rng: &mut Rng, kind: KeyKind, row_count: u32, arrays: bool) -> Self {
        let mut rows = BTreeMap::new();
        for row in 0..row_count {
            if rng.percent(5) {
                continue;
            }
            if rng.percent(15) {
                rows.insert(row, Cell::Null);
                continue;
            }
            let count = if arrays { rng.below(5) } else { 1 };
            let values = (0..count).map(|_| random_key(rng, kind)).collect();
            rows.insert(row, Cell::Values(values));
        }
        Self { kind, rows }
    }

    fn entries(&self) -> Vec<(ScalarKey, u32)> {
        let mut entries: Vec<_> = self
            .rows
            .iter()
            .flat_map(|(&row, cell)| match cell {
                Cell::Null => Vec::new(),
                Cell::Values(values) => values.iter().map(|key| (key.clone(), row)).collect(),
            })
            .collect();
        entries.sort();
        entries
    }

    fn select(&self, predicate: impl Fn(&BTreeSet<ScalarKey>) -> bool) -> RoaringBitmap {
        self.rows
            .iter()
            .filter(|(_, cell)| matches!(cell, Cell::Values(values) if predicate(values)))
            .map(|(&row, _)| row)
            .collect()
    }

    fn nulls(&self) -> RoaringBitmap {
        self.rows
            .iter()
            .filter(|(_, cell)| matches!(cell, Cell::Null))
            .map(|(&row, _)| row)
            .collect()
    }

    fn exists(&self) -> RoaringBitmap {
        self.select(|values| !values.is_empty())
    }

    fn bounds_ok(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> bool {
        [lower, upper].iter().all(|bound| match bound {
            Bound::Included(key) | Bound::Excluded(key) => key.kind() == self.kind,
            Bound::Unbounded => true,
        })
    }

    fn range(&self, lower: Bound<&ScalarKey>, upper: Bound<&ScalarKey>) -> RoaringBitmap {
        if !self.bounds_ok(lower, upper) {
            return RoaringBitmap::new();
        }
        self.select(|values| values.iter().any(|key| (lower, upper).contains(key)))
    }

    fn range_entries(
        &self,
        lower: Bound<&ScalarKey>,
        upper: Bound<&ScalarKey>,
    ) -> Vec<(ScalarKey, u32)> {
        if !self.bounds_ok(lower, upper) {
            return Vec::new();
        }
        self.entries()
            .into_iter()
            .filter(|(key, _)| (lower, upper).contains(key))
            .collect()
    }

    fn stats(&self) -> ScalarIndexStats {
        let entries = self.entries();
        let distinct: BTreeSet<_> = entries.iter().map(|(key, _)| key.clone()).collect();
        ScalarIndexStats {
            kind: self.kind,
            entries: entries.len() as u64,
            distinct_keys: distinct.len() as u64,
            rows_with_values: self.exists().len(),
            null_rows: self.nulls().len(),
        }
    }

    fn remapped(&self, remap: &[Option<u32>]) -> Self {
        let rows = self
            .rows
            .iter()
            .filter_map(|(&row, cell)| {
                remap
                    .get(row as usize)
                    .copied()
                    .flatten()
                    .map(|target| (target, cell.clone()))
            })
            .collect();
        Self {
            kind: self.kind,
            rows,
        }
    }

    fn builder(&self) -> ScalarIndexBuilder {
        let mut builder = ScalarIndexBuilder::new(self.kind);
        for (&row, cell) in &self.rows {
            match cell {
                Cell::Null => builder.insert_null(row),
                Cell::Values(values) => {
                    for key in values {
                        builder.insert(row, key.clone()).expect("kinds match");
                    }
                }
            }
        }
        builder
    }
}

fn random_keys(rng: &mut Rng, kind: KeyKind) -> Vec<ScalarKey> {
    (0..rng.below(4)).map(|_| query_key(rng, kind)).collect()
}

fn check_queries(index: &dyn ScalarIndex, oracle: &Oracle, rng: &mut Rng, rounds: usize) {
    assert_eq!(index.kind(), oracle.kind);
    assert_eq!(index.is_null(), &oracle.nulls());
    assert_eq!(index.exists(), &oracle.exists());
    assert_eq!(index.stats(), oracle.stats());
    for _ in 0..rounds {
        let key = query_key(rng, oracle.kind);
        let expected = oracle.select(|values| values.contains(&key));
        assert_eq!(index.equals(&key), expected, "equals {key:?}");
        assert_eq!(index.contains(&key), expected, "contains {key:?}");
        assert_eq!(
            index.cardinality(&key),
            expected.len(),
            "cardinality {key:?}"
        );

        let keys = random_keys(rng, oracle.kind);
        let any = oracle.select(|values| keys.iter().any(|key| values.contains(key)));
        assert_eq!(index.in_set(&keys), any, "in {keys:?}");
        assert_eq!(index.contains_any(&keys), any, "contains_any {keys:?}");
        assert_eq!(
            index.not_in_set(&keys),
            oracle.select(|values| !values.is_empty() && !keys.iter().any(|k| values.contains(k))),
            "not in {keys:?}"
        );
        assert_eq!(
            index.contains_all(&keys),
            oracle.select(|values| !values.is_empty() && keys.iter().all(|k| values.contains(k))),
            "contains_all {keys:?}"
        );

        let lower = random_bound(rng, oracle.kind);
        let upper = random_bound(rng, oracle.kind);
        let (lower, upper) = (lower.as_ref(), upper.as_ref());
        assert_eq!(
            index.range(lower, upper),
            oracle.range(lower, upper),
            "range {lower:?}..{upper:?}"
        );
        assert_eq!(
            index.count_range(lower, upper),
            oracle.range_entries(lower, upper).len() as u64,
            "count_range {lower:?}..{upper:?}"
        );

        let prefix = random_prefix(rng);
        let expected = if oracle.kind == KeyKind::Str {
            oracle.select(|values| {
                values
                    .iter()
                    .any(|key| matches!(key, ScalarKey::Str(text) if text.starts_with(&prefix)))
            })
        } else {
            RoaringBitmap::new()
        };
        assert_eq!(index.prefix(&prefix), expected, "prefix {prefix:?}");
    }
}

fn check_ordered(index: &dyn OrderedScalarIndex, oracle: &Oracle, rng: &mut Rng, rounds: usize) {
    let entries = oracle.entries();
    assert_eq!(
        index.min_key().map(|key| key.to_key()),
        entries.first().map(|(key, _)| key.clone())
    );
    assert_eq!(
        index.max_key().map(|key| key.to_key()),
        entries.last().map(|(key, _)| key.clone())
    );
    let all_rows: Vec<u32> = oracle.rows.keys().copied().collect();
    for _ in 0..rounds {
        let (lower, upper) = if rng.percent(30) {
            (Bound::Unbounded, Bound::Unbounded)
        } else {
            (
                random_bound(rng, oracle.kind),
                random_bound(rng, oracle.kind),
            )
        };
        let (lower, upper) = (lower.as_ref(), upper.as_ref());
        let direction = if rng.percent(50) {
            Direction::Ascending
        } else {
            Direction::Descending
        };
        let allow: Option<RoaringBitmap> = rng.percent(50).then(|| {
            all_rows
                .iter()
                .copied()
                .filter(|_| rng.percent(40))
                .collect()
        });
        let limit = if rng.percent(20) {
            usize::MAX
        } else {
            rng.below(12) as usize
        };

        let mut expected: Vec<(ScalarKey, u32)> = oracle
            .range_entries(lower, upper)
            .into_iter()
            .filter(|(_, row)| allow.as_ref().is_none_or(|allow| allow.contains(*row)))
            .collect();
        if direction == Direction::Descending {
            expected.reverse();
        }
        expected.truncate(limit);

        let actual: Vec<(ScalarKey, u32)> = index
            .scan_range(lower, upper, direction, allow.as_ref())
            .take(limit)
            .map(|(key, row)| (key.to_key(), row))
            .collect();
        assert_eq!(actual, expected, "scan {lower:?}..{upper:?} {direction:?}");
    }
}

/// Every dataset shape the tests sweep: each kind, single-valued and arrays,
/// several sizes including empty.
fn datasets() -> impl Iterator<Item = (u64, Oracle)> {
    (0..48u64).map(|seed| {
        let mut rng = Rng(seed);
        let kind = KINDS[(seed % 4) as usize];
        let arrays = (seed / 4) % 2 == 1;
        let row_count = [0, 1, 7, 60, 400, 1500][(seed / 8) as usize % 6];
        (seed, Oracle::random(&mut rng, kind, row_count, arrays))
    })
}

#[test]
fn immutable_indexes_match_scan_oracle() {
    for (seed, oracle) in datasets() {
        let mut rng = Rng(seed ^ 0xABCD);
        let (inverted, sorted) = oracle.builder().build().expect("builds");
        check_queries(&inverted, &oracle, &mut rng, 60);
        check_queries(&sorted, &oracle, &mut rng, 60);
        check_ordered(&sorted, &oracle, &mut rng, 60);
        assert_eq!(sorted.is_single_valued(), {
            let stats = oracle.stats();
            stats.entries == stats.rows_with_values
        });

        let inverted_bytes = inverted.to_bytes().expect("encodes");
        let sorted_bytes = sorted.to_bytes().expect("encodes");
        let inverted_loaded = InvertedIndex::from_bytes(&inverted_bytes).expect("decodes");
        let sorted_loaded = SortedIndex::from_bytes(&sorted_bytes).expect("decodes");
        assert_eq!(inverted_loaded, inverted);
        assert_eq!(sorted_loaded, sorted);
        check_queries(&inverted_loaded, &oracle, &mut rng, 10);
        check_ordered(&sorted_loaded, &oracle, &mut rng, 10);
    }
}

#[test]
fn from_entries_matches_builder() {
    for (_, oracle) in datasets().filter(|(_, oracle)| oracle.nulls().is_empty()) {
        let pairs = oracle.entries().into_iter().map(|(key, row)| (row, key));
        let sorted = SortedIndex::from_entries(oracle.kind, pairs.clone()).expect("builds");
        let inverted = InvertedIndex::from_entries(oracle.kind, pairs).expect("builds");
        assert_eq!(inverted, InvertedIndex::from_sorted(&sorted));
    }
}

/// Load `oracle` into both memtable indexes in random order, then apply
/// random removes and re-inserts, keeping `oracle` in step.
fn random_mutable(
    rng: &mut Rng,
    oracle: &mut Oracle,
) -> (MutableInvertedIndex, MutableSortedIndex) {
    let mut inverted = MutableInvertedIndex::new(oracle.kind);
    let mut sorted = MutableSortedIndex::new(oracle.kind);
    let mut ops: Vec<(u32, Option<ScalarKey>)> = oracle
        .rows
        .iter()
        .flat_map(|(&row, cell)| match cell {
            Cell::Null => vec![(row, None)],
            Cell::Values(values) => values.iter().map(|key| (row, Some(key.clone()))).collect(),
        })
        .collect();
    rng.shuffle(&mut ops);
    for (row, key) in ops {
        match key {
            None => {
                assert!(inverted.insert_null(row).expect("no conflict"));
                assert!(sorted.insert_null(row).expect("no conflict"));
            }
            Some(key) => {
                assert!(inverted.insert(row, key.clone()).expect("no conflict"));
                assert!(sorted.insert(row, key).expect("no conflict"));
            }
        }
    }

    let rows: Vec<u32> = oracle.rows.keys().copied().collect();
    for _ in 0..rows.len() / 2 {
        let row = rows[rng.below(rows.len() as u64) as usize];
        let cell = oracle.rows.get_mut(&row).expect("row exists");
        match cell {
            Cell::Null => {
                assert!(inverted.remove_null(row));
                assert!(sorted.remove_null(row));
                *cell = Cell::Values(BTreeSet::new());
            }
            Cell::Values(values) => {
                let key = random_key(rng, oracle.kind);
                if rng.percent(50) {
                    let existed = values.remove(&key);
                    assert_eq!(inverted.remove(row, &key), existed);
                    assert_eq!(sorted.remove(row, &key), existed);
                } else {
                    let added = values.insert(key.clone());
                    assert_eq!(inverted.insert(row, key.clone()).expect("insert"), added);
                    assert_eq!(sorted.insert(row, key).expect("insert"), added);
                }
            }
        }
    }
    (inverted, sorted)
}

#[test]
fn mutable_indexes_match_scan_oracle_under_inserts_and_removes() {
    for (seed, mut oracle) in datasets() {
        let mut rng = Rng(seed ^ 0x1234);
        let (inverted, sorted) = random_mutable(&mut rng, &mut oracle);
        assert_eq!(inverted.len(), oracle.stats().entries);
        check_queries(&inverted, &oracle, &mut rng, 60);
        check_queries(&sorted, &oracle, &mut rng, 60);
        check_ordered(&sorted, &oracle, &mut rng, 60);
    }
}

#[test]
fn freeze_with_remapping_matches_remapped_oracle() {
    for (seed, mut oracle) in datasets() {
        let mut rng = Rng(seed ^ 0x5678);
        let (inverted, sorted) = random_mutable(&mut rng, &mut oracle);
        let slots = oracle.rows.keys().next_back().map_or(0, |&last| last + 1);
        // Sparse, shuffled targets; about a fifth of the slots are dropped.
        let mut targets: Vec<u32> = (0..slots).map(|slot| slot * 3 + 1).collect();
        rng.shuffle(&mut targets);
        let remap: Vec<Option<u32>> = targets
            .into_iter()
            .map(|target| (!rng.percent(20)).then_some(target))
            .collect();
        let lookup = |slot: u32| remap.get(slot as usize).copied().flatten();

        let frozen_inverted = inverted.freeze(lookup).expect("injective");
        let frozen_sorted = sorted.freeze(lookup).expect("injective");
        let expected = oracle.remapped(&remap);
        check_queries(&frozen_inverted, &expected, &mut rng, 40);
        check_queries(&frozen_sorted, &expected, &mut rng, 40);
        check_ordered(&frozen_sorted, &expected, &mut rng, 40);

        let (built_inverted, built_sorted) = expected.builder().build().expect("builds");
        assert_eq!(frozen_sorted, built_sorted);
        assert_eq!(frozen_inverted, built_inverted);
    }
}

#[test]
fn freeze_rejects_non_injective_remap() {
    let mut index = MutableSortedIndex::new(KeyKind::Int);
    index.insert(0, ScalarKey::Int(5)).expect("insert");
    index.insert_null(1).expect("insert");
    let collapse = |_slot: u32| Some(9);
    assert!(matches!(
        index.freeze(collapse),
        Err(ScalarError::NonInjectiveRemap { row: 9 })
    ));
    let mut inverted = MutableInvertedIndex::new(KeyKind::Int);
    inverted.insert(0, ScalarKey::Int(5)).expect("insert");
    inverted.insert(1, ScalarKey::Int(6)).expect("insert");
    assert!(matches!(
        inverted.freeze(collapse),
        Err(ScalarError::NonInjectiveRemap { row: 9 })
    ));
}

#[test]
fn nan_keys_are_rejected() {
    assert!(matches!(
        ScalarKey::float(f64::NAN),
        Err(ScalarError::NanKey)
    ));
    assert!(matches!(
        ScalarKey::try_from(-f64::NAN),
        Err(ScalarError::NanKey)
    ));
    assert!(matches!(F64Key::new(f64::NAN), Err(ScalarError::NanKey)));

    // Negative zero is normalized, so it finds rows written as positive zero.
    let index = SortedIndex::from_entries(KeyKind::Float, [(3, float(0.0)), (4, float(-0.0))])
        .expect("builds");
    assert_eq!(index.distinct_keys(), 1);
    assert_eq!(index.equals(&float(-0.0)), RoaringBitmap::from_iter([3, 4]));
    assert_eq!(
        index.range(Bound::Included(&float(f64::NEG_INFINITY)), Bound::Unbounded),
        RoaringBitmap::from_iter([3, 4])
    );
}

#[test]
fn timestamps_are_int_micros() {
    let key = ScalarKey::timestamp_micros(1_700_000_000_000_000);
    assert_eq!(key.kind(), KeyKind::Int);
    let index = InvertedIndex::from_entries(KeyKind::Int, [(0, key.clone())]).expect("builds");
    assert_eq!(index.equals(&key), RoaringBitmap::from_iter([0]));
}

#[test]
fn rejects_kind_mismatch_and_null_conflicts() {
    let mut builder = ScalarIndexBuilder::new(KeyKind::Int);
    assert!(matches!(
        builder.insert(0, ScalarKey::from("x")),
        Err(ScalarError::KindMismatch {
            expected: KeyKind::Int,
            found: KeyKind::Str
        })
    ));
    builder.insert(1, ScalarKey::Int(1)).expect("insert");
    builder.insert_null(1);
    assert!(matches!(
        builder.build_sorted(),
        Err(ScalarError::NullConflict { row: 1 })
    ));

    let mut index = MutableInvertedIndex::new(KeyKind::Bool);
    assert!(matches!(
        index.insert(0, ScalarKey::Int(1)),
        Err(ScalarError::KindMismatch { .. })
    ));
    index.insert(0, ScalarKey::Bool(true)).expect("insert");
    assert!(matches!(
        index.insert_null(0),
        Err(ScalarError::NullConflict { row: 0 })
    ));
    index.insert_null(1).expect("insert");
    assert!(matches!(
        index.insert(1, ScalarKey::Bool(false)),
        Err(ScalarError::NullConflict { row: 1 })
    ));
}

#[test]
fn empty_indexes_answer_every_query() {
    let mut rng = Rng(7);
    for kind in KINDS {
        let oracle = Oracle {
            kind,
            rows: BTreeMap::new(),
        };
        let sorted = SortedIndex::empty(kind);
        let inverted = InvertedIndex::empty(kind);
        assert_eq!(
            sorted,
            ScalarIndexBuilder::new(kind)
                .build_sorted()
                .expect("builds")
        );
        assert_eq!(
            inverted,
            ScalarIndexBuilder::new(kind)
                .build_inverted()
                .expect("builds")
        );
        let mutable = MutableSortedIndex::new(kind);
        check_queries(&sorted, &oracle, &mut rng, 20);
        check_queries(&inverted, &oracle, &mut rng, 20);
        check_queries(&mutable, &oracle, &mut rng, 20);
        check_ordered(&sorted, &oracle, &mut rng, 20);
        check_ordered(&mutable, &oracle, &mut rng, 20);
        assert!(sorted.equi_depth_histogram(8).is_empty());
        assert_eq!(
            SortedIndex::from_bytes(&sorted.to_bytes().expect("encodes")).expect("decodes"),
            sorted
        );
        assert_eq!(
            InvertedIndex::from_bytes(&inverted.to_bytes().expect("encodes")).expect("decodes"),
            inverted
        );
        assert_eq!(mutable.freeze(Some).expect("freezes"), sorted);
    }
}

#[test]
fn histogram_buckets_partition_entries() {
    for (_, oracle) in datasets() {
        let sorted = oracle.builder().build_sorted().expect("builds");
        for buckets in [0, 1, 2, 5, 16, 1000] {
            let histogram = sorted.equi_depth_histogram(buckets);
            assert!(histogram.len() <= buckets);
            assert_eq!(histogram.is_empty(), buckets == 0 || sorted.is_empty());
            assert_eq!(
                histogram.iter().map(|bucket| bucket.entries).sum::<u64>(),
                if buckets == 0 { 0 } else { sorted.len() as u64 }
            );
            if buckets > 0 {
                assert_eq!(
                    histogram
                        .iter()
                        .map(|bucket| bucket.distinct_keys)
                        .sum::<u64>(),
                    sorted.distinct_keys() as u64
                );
            }
            for bucket in &histogram {
                assert!(bucket.lower <= bucket.upper);
                assert_eq!(
                    sorted.count_range(
                        Bound::Included(&bucket.lower),
                        Bound::Included(&bucket.upper)
                    ),
                    bucket.entries
                );
            }
            for pair in histogram.windows(2) {
                assert!(pair[0].upper < pair[1].lower);
            }
        }
    }

    // Uniform keys split into exactly equal buckets.
    let uniform = SortedIndex::from_entries(
        KeyKind::Int,
        (0..1000u32).map(|row| (row, ScalarKey::Int(i64::from(row)))),
    )
    .expect("builds");
    let histogram = uniform.equi_depth_histogram(10);
    assert_eq!(histogram.len(), 10);
    assert!(histogram.iter().all(|bucket| bucket.entries == 100));
    assert_eq!(histogram[3].lower, ScalarKey::Int(300));
    assert_eq!(histogram[3].upper, ScalarKey::Int(399));

    // A heavy key stays in one bucket.
    let skewed = SortedIndex::from_entries(
        KeyKind::Int,
        (0..100u32).map(|row| {
            (
                row,
                ScalarKey::Int(if row < 90 { 7 } else { i64::from(row) }),
            )
        }),
    )
    .expect("builds");
    let histogram = skewed.equi_depth_histogram(4);
    assert_eq!(histogram[0].entries, 90);
    assert_eq!(histogram[0].distinct_keys, 1);
}

fn sample_bytes() -> (Vec<u8>, Vec<u8>) {
    let mut builder = ScalarIndexBuilder::new(KeyKind::Str);
    for row in 0..40u32 {
        if row % 7 == 0 {
            builder.insert_null(row);
            continue;
        }
        let key = ["apple", "apricot", "banana", "cherry", ""][row as usize % 5];
        builder.insert(row, ScalarKey::from(key)).expect("insert");
    }
    let (inverted, sorted) = builder.build().expect("builds");
    (
        inverted.to_bytes().expect("encodes"),
        sorted.to_bytes().expect("encodes"),
    )
}

/// Recompute the trailer so structural validation, not the checksum, is
/// what rejects a crafted input.
fn reseal(bytes: &mut [u8]) {
    let split = bytes.len() - 4;
    let crc = crc32fast::hash(&bytes[..split]);
    bytes[split..].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn detects_every_single_byte_corruption_and_truncation() {
    let (inverted, sorted) = sample_bytes();
    for position in 0..inverted.len() {
        let mut corrupt = inverted.clone();
        corrupt[position] ^= 0x5A;
        assert!(
            InvertedIndex::from_bytes(&corrupt).is_err(),
            "byte {position}"
        );
    }
    for position in 0..sorted.len() {
        let mut corrupt = sorted.clone();
        corrupt[position] ^= 0x5A;
        assert!(
            SortedIndex::from_bytes(&corrupt).is_err(),
            "byte {position}"
        );
    }
    for len in 0..inverted.len() {
        assert!(InvertedIndex::from_bytes(&inverted[..len]).is_err());
    }
    for len in 0..sorted.len() {
        assert!(SortedIndex::from_bytes(&sorted[..len]).is_err());
    }
    let mut extended = sorted.clone();
    extended.push(0);
    assert!(SortedIndex::from_bytes(&extended).is_err());
}

#[test]
fn rejects_wrong_type_version_and_invalid_structure() {
    let (inverted, sorted) = sample_bytes();
    assert!(matches!(
        SortedIndex::from_bytes(&inverted),
        Err(ScalarError::Corrupt("wrong index type"))
    ));
    assert!(matches!(
        InvertedIndex::from_bytes(&sorted),
        Err(ScalarError::Corrupt("wrong index type"))
    ));
    assert!(matches!(
        SortedIndex::from_bytes(b"nope"),
        Err(ScalarError::BadMagic | ScalarError::Corrupt(_))
    ));

    let mut future = sorted.clone();
    future[4] = 2;
    reseal(&mut future);
    assert!(matches!(
        SortedIndex::from_bytes(&future),
        Err(ScalarError::UnsupportedVersion(2))
    ));

    let mut unordered = SortedIndex::from_entries(
        KeyKind::Int,
        [(0, ScalarKey::Int(1)), (1, ScalarKey::Int(2))],
    )
    .expect("builds")
    .to_bytes()
    .expect("encodes");
    // Body: nulls (4-byte length + roaring), then the key count and keys.
    let keys_at = 16 + 4 + RoaringBitmap::new().serialized_size() + 8;
    unordered[keys_at..keys_at + 8].copy_from_slice(&3i64.to_le_bytes());
    reseal(&mut unordered);
    assert!(matches!(
        SortedIndex::from_bytes(&unordered),
        Err(ScalarError::Corrupt("keys are not strictly ascending"))
    ));

    let mut nan = SortedIndex::from_entries(KeyKind::Float, [(0, float(1.0))])
        .expect("builds")
        .to_bytes()
        .expect("encodes");
    nan[keys_at..keys_at + 8].copy_from_slice(&f64::NAN.to_bits().to_le_bytes());
    reseal(&mut nan);
    assert!(matches!(
        SortedIndex::from_bytes(&nan),
        Err(ScalarError::Corrupt("NaN float key"))
    ));
}

#[test]
fn resealed_random_corruption_never_panics() {
    let (inverted, sorted) = sample_bytes();
    let mut rng = Rng(99);
    for _ in 0..3000 {
        let mut inverted_bad = inverted.clone();
        let mut sorted_bad = sorted.clone();
        for bytes in [&mut inverted_bad, &mut sorted_bad] {
            for _ in 0..=rng.below(3) {
                let position = 16 + rng.below(bytes.len() as u64 - 20) as usize;
                bytes[position] = rng.next() as u8;
            }
            reseal(bytes);
        }
        if let Ok(index) = InvertedIndex::from_bytes(&inverted_bad) {
            let _ = index.range(Bound::Unbounded, Bound::Unbounded);
            let _ = index.prefix("ap");
        }
        if let Ok(index) = SortedIndex::from_bytes(&sorted_bad) {
            let _ = index.range(Bound::Unbounded, Bound::Unbounded);
            let _ = index.iter_ordered(Direction::Descending, None).count();
            let _ = index.equi_depth_histogram(3);
        }
    }
}
