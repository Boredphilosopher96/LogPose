//! The writer-private primary-key index: `pk -> RowAddr` of the key's live row, with forwarding
//! tables for units that a flush or compaction retired and an incremental, FIFO rewrite of the
//! entries that still point into them.
//!
//! Only the writer needs the map (to find the row an upsert, update, or delete must mark), and
//! it is the only mutator, so the map is a flat hash table that is never published: readers
//! resolve keys through each unit's own key structures. It is rebuilt from the segments' key
//! and row-meta sections minus their deletion vectors at every open, then WAL replay updates it
//! through `apply`.
//!
//! A flush of memtable `m` into segment `s`, or a compaction of inputs `I` into `o`, installs a
//! forwarding table `unit -> (target, old row -> new row)`, so a lookup that lands on a retired
//! unit follows it to the row's new address. A rewrite task then walks the new unit's rows in
//! slices between groups and repoints every entry that still equals the row's old address;
//! entries that moved on (the key was upserted or deleted since) are left alone. Tasks run
//! strictly in FIFO order, which is what makes chains (`m -> s -> o`) safe: when a task finishes
//! and drops its forwarding entries, no raw entry points into those units any more.

use crate::memtable::MemtableData;
use logpose_types::{RowAddr, RowId, UnitId, record::PrimaryKey};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

/// Rows the writer rewrites per slice.
pub(crate) const PK_REWRITE_SLICE: u32 = 65_536;

/// Where the rows of a retired unit went.
#[derive(Clone, Debug)]
pub(crate) struct Forwarding {
    pub(crate) target: UnitId,
    /// Old row to new row, or `u32::MAX` when the row was not copied (it was already deleted
    /// in the job's snapshot).
    pub(crate) map: Arc<[u32]>,
}

/// The rows of a job's output unit, in row order, with where each came from.
#[derive(Clone, Debug)]
pub(crate) enum RewriteRows {
    /// A flush: output row `o` is slot `row_to_slot[o]` of `memtable`.
    Flush {
        memtable: Arc<MemtableData>,
        row_to_slot: Arc<[u32]>,
    },
    /// A compaction: output row `o` holds `pks[o]` and was copied from `sources[o]`.
    Compaction {
        pks: Arc<[PrimaryKey]>,
        sources: Arc<[RowAddr]>,
    },
}

impl RewriteRows {
    fn len(&self) -> u32 {
        let len = match self {
            Self::Flush { row_to_slot, .. } => row_to_slot.len(),
            Self::Compaction { sources, .. } => sources.len(),
        };
        u32::try_from(len).unwrap_or(u32::MAX)
    }

    fn row(&self, row: u32) -> Option<(&PrimaryKey, RowAddr)> {
        match self {
            Self::Flush {
                memtable,
                row_to_slot,
            } => {
                let slot = *row_to_slot.get(row as usize)?;
                Some((
                    memtable.pk(slot)?,
                    RowAddr {
                        unit: memtable.unit,
                        row: slot,
                    },
                ))
            }
            Self::Compaction { pks, sources } => {
                Some((pks.get(row as usize)?, *sources.get(row as usize)?))
            }
        }
    }
}

struct RewriteTask {
    target: UnitId,
    rows: RewriteRows,
    next_row: u32,
    /// Units whose forwarding entries the task retires when it finishes.
    retires: Vec<UnitId>,
}

/// A lookup reached `u32::MAX` in a forwarding table: an index entry pointed at a row that was
/// already deleted when the job that retired its unit started, which I5 forbids.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ForwardingViolation {
    pub(crate) addr: RowAddr,
}

/// The writer-private primary-key index.
#[derive(Default)]
pub(crate) struct PkIndex {
    map: HashMap<PrimaryKey, RowAddr>,
    forwards: HashMap<UnitId, Forwarding>,
    rewrites: VecDeque<RewriteTask>,
    /// Undo log of the group being prepared: each changed key with its previous entry.
    journal: Option<Vec<(PrimaryKey, Option<RowAddr>)>>,
    /// Heap bytes of string keys in `map`.
    key_bytes: u64,
    /// Lookups that hit a forwarding violation and were treated as absent.
    pub(crate) violations: u64,
}

impl PkIndex {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            ..Self::default()
        }
    }

    /// Number of keys with a live row.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Approximate memory of the index: the table's slots and the string keys' heap bytes.
    pub(crate) fn approximate_bytes(&self) -> u64 {
        let slot = std::mem::size_of::<(PrimaryKey, RowAddr)>() as u64 + 1;
        self.map.capacity() as u64 * slot + self.key_bytes
    }

    /// The raw entry for `pk`, without following forwarding.
    pub(crate) fn raw(&self, pk: &PrimaryKey) -> Option<RowAddr> {
        self.map.get(pk).copied()
    }

    /// The address of `pk`'s live row in the current units, following forwarding tables.
    pub(crate) fn resolve(&self, pk: &PrimaryKey) -> Result<Option<RowAddr>, ForwardingViolation> {
        let Some(mut addr) = self.raw(pk) else {
            return Ok(None);
        };
        while let Some(forwarding) = self.forwards.get(&addr.unit) {
            match forwarding.map.get(addr.row as usize).copied() {
                Some(row) if row != u32::MAX => {
                    addr = RowAddr {
                        unit: forwarding.target,
                        row,
                    };
                }
                _ => return Err(ForwardingViolation { addr }),
            }
        }
        Ok(Some(addr))
    }

    /// Point `pk` at `addr`.
    pub(crate) fn insert(&mut self, pk: PrimaryKey, addr: RowAddr) {
        let key_len = key_heap_bytes(&pk);
        let previous = match &mut self.journal {
            Some(journal) => {
                let previous = self.map.insert(pk.clone(), addr);
                journal.push((pk, previous));
                previous
            }
            None => self.map.insert(pk, addr),
        };
        if previous.is_none() {
            self.key_bytes += key_len;
        }
    }

    /// Forget `pk`.
    pub(crate) fn remove(&mut self, pk: &PrimaryKey) {
        if let Some(previous) = self.map.remove(pk) {
            self.key_bytes = self.key_bytes.saturating_sub(key_heap_bytes(pk));
            if let Some(journal) = &mut self.journal {
                journal.push((pk.clone(), Some(previous)));
            }
        }
    }

    /// Start recording an undo log for a group being prepared. Any earlier log is discarded.
    pub(crate) fn begin_group(&mut self) {
        self.journal = Some(Vec::new());
    }

    /// The group was handed to the WAL (or the state is abandoned): drop its undo log.
    pub(crate) fn end_group(&mut self) {
        self.journal = None;
    }

    /// Undo every change since [`begin_group`](Self::begin_group), newest first.
    pub(crate) fn rollback_group(&mut self) {
        let Some(journal) = self.journal.take() else {
            return;
        };
        for (pk, previous) in journal.into_iter().rev() {
            let key_len = key_heap_bytes(&pk);
            let replaced = match previous {
                Some(addr) => self.map.insert(pk, addr),
                None => self.map.remove(&pk),
            };
            match (previous.is_some(), replaced.is_some()) {
                (true, false) => self.key_bytes += key_len,
                (false, true) => self.key_bytes = self.key_bytes.saturating_sub(key_len),
                _ => {}
            }
        }
    }

    /// Install forwarding for the units a job retired, and queue the rewrite of the entries
    /// that point into them.
    pub(crate) fn retire(
        &mut self,
        forwards: Vec<(UnitId, Forwarding)>,
        target: UnitId,
        rows: RewriteRows,
    ) {
        let retires = forwards.iter().map(|(unit, _)| *unit).collect::<Vec<_>>();
        self.forwards.extend(forwards);
        self.rewrites.push_back(RewriteTask {
            target,
            rows,
            next_row: 0,
            retires,
        });
    }

    /// Whether a rewrite is still pending.
    pub(crate) fn rewriting(&self) -> bool {
        !self.rewrites.is_empty()
    }

    /// Rewrite up to `budget` rows of the front task (and of later tasks, once it finishes),
    /// strictly in FIFO order. Never runs while a group's undo log is open: a rollback must not
    /// resurrect an entry into a unit whose forwarding a finished task dropped. Returns the rows
    /// processed.
    pub(crate) fn rewrite_slice(&mut self, budget: u32) -> u32 {
        if self.journal.is_some() {
            return 0;
        }
        let mut done = 0;
        while done < budget {
            let Some(task) = self.rewrites.front_mut() else {
                break;
            };
            let len = task.rows.len();
            while task.next_row < len && done < budget {
                let row = task.next_row;
                if let Some((pk, source)) = task.rows.row(row)
                    && let Some(entry) = self.map.get_mut(pk)
                    && *entry == source
                {
                    *entry = RowAddr {
                        unit: task.target,
                        row,
                    };
                }
                task.next_row += 1;
                done += 1;
            }
            if task.next_row >= len {
                if let Some(task) = self.rewrites.pop_front() {
                    for unit in task.retires {
                        self.forwards.remove(&unit);
                    }
                }
            } else {
                break;
            }
        }
        done
    }

    /// Forwarding tables still installed.
    #[cfg(test)]
    pub(crate) fn forwarding_units(&self) -> Vec<UnitId> {
        let mut units = self.forwards.keys().copied().collect::<Vec<_>>();
        units.sort();
        units
    }
}

fn key_heap_bytes(pk: &PrimaryKey) -> u64 {
    match pk {
        PrimaryKey::Int64(_) => 0,
        PrimaryKey::String(value) => value.capacity() as u64,
    }
}

/// The dense old-to-new row map of a job over `rows` old rows, where `kept(row)` says whether
/// the row was copied.
pub(crate) fn row_map(rows: u32, mut kept: impl FnMut(RowId) -> bool) -> (Vec<u32>, u32) {
    let mut map = Vec::with_capacity(rows as usize);
    let mut next = 0;
    for row in 0..rows {
        if kept(row) {
            map.push(next);
            next += 1;
        } else {
            map.push(u32::MAX);
        }
    }
    (map, next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(unit: u32, row: u32) -> RowAddr {
        RowAddr {
            unit: UnitId(unit),
            row,
        }
    }

    fn key(index: i64) -> PrimaryKey {
        PrimaryKey::Int64(index)
    }

    #[test]
    fn a_rolled_back_group_restores_every_entry() {
        let mut index = PkIndex::default();
        index.insert(key(1), addr(1, 0));
        index.insert(key(2), addr(1, 1));
        let bytes = index.approximate_bytes();
        index.begin_group();
        index.insert(key(1), addr(2, 0));
        index.remove(&key(2));
        index.insert(key(3), addr(2, 1));
        index.insert(key(3), addr(2, 2));
        index.rollback_group();
        assert_eq!(index.raw(&key(1)), Some(addr(1, 0)));
        assert_eq!(index.raw(&key(2)), Some(addr(1, 1)));
        assert_eq!(index.raw(&key(3)), None);
        assert_eq!(index.len(), 2);
        assert_eq!(index.approximate_bytes(), bytes);
    }

    #[test]
    fn lookups_follow_forwarding_chains_until_fifo_rewrites_retire_them() {
        let mut index = PkIndex::default();
        // Unit 1 rows 0..4; row 1 was deleted before the jobs.
        for row in [0, 2, 3] {
            index.insert(key(i64::from(row)), addr(1, row));
        }
        // Job A: unit 1 -> unit 5, dropping row 1.
        let (map_a, rows_a) = row_map(4, |row| row != 1);
        assert_eq!(rows_a, 3);
        index.retire(
            vec![(
                UnitId(1),
                Forwarding {
                    target: UnitId(5),
                    map: Arc::from(map_a),
                },
            )],
            UnitId(5),
            RewriteRows::Compaction {
                pks: Arc::from(vec![key(0), key(2), key(3)]),
                sources: Arc::from(vec![addr(1, 0), addr(1, 2), addr(1, 3)]),
            },
        );
        // Key 3 is upserted into a memtable before any rewrite ran.
        index.insert(key(3), addr(9, 0));
        // Job B compacts unit 5 into unit 7 (reversing its rows).
        index.retire(
            vec![(
                UnitId(5),
                Forwarding {
                    target: UnitId(7),
                    map: Arc::from(vec![2, 1, 0]),
                },
            )],
            UnitId(7),
            RewriteRows::Compaction {
                pks: Arc::from(vec![key(3), key(2), key(0)]),
                sources: Arc::from(vec![addr(5, 2), addr(5, 1), addr(5, 0)]),
            },
        );
        assert_eq!(index.resolve(&key(0)), Ok(Some(addr(7, 2))));
        assert_eq!(index.resolve(&key(2)), Ok(Some(addr(7, 1))));
        assert_eq!(index.resolve(&key(3)), Ok(Some(addr(9, 0))));

        // A slice of two rows does not finish task A: nothing is retired yet.
        assert_eq!(index.rewrite_slice(2), 2);
        assert_eq!(index.forwarding_units(), vec![UnitId(1), UnitId(5)]);
        assert_eq!(index.raw(&key(0)), Some(addr(5, 0)));
        assert_eq!(index.rewrite_slice(100), 4);
        assert!(!index.rewriting());
        assert!(index.forwarding_units().is_empty());
        assert_eq!(index.raw(&key(0)), Some(addr(7, 2)));
        assert_eq!(index.raw(&key(2)), Some(addr(7, 1)));
        assert_eq!(
            index.raw(&key(3)),
            Some(addr(9, 0)),
            "a key that moved on is never clobbered"
        );
    }

    #[test]
    fn a_forward_to_a_dropped_row_is_a_violation() {
        let mut index = PkIndex::default();
        index.insert(key(1), addr(1, 1));
        let (map, _) = row_map(2, |row| row == 0);
        index.retire(
            vec![(
                UnitId(1),
                Forwarding {
                    target: UnitId(2),
                    map: Arc::from(map),
                },
            )],
            UnitId(2),
            RewriteRows::Compaction {
                pks: Arc::from(Vec::new()),
                sources: Arc::from(Vec::new()),
            },
        );
        assert_eq!(
            index.resolve(&key(1)),
            Err(ForwardingViolation { addr: addr(1, 1) })
        );
    }

    #[test]
    fn rewrites_wait_while_a_group_is_open() {
        let mut index = PkIndex::default();
        index.insert(key(1), addr(1, 0));
        index.retire(
            vec![(
                UnitId(1),
                Forwarding {
                    target: UnitId(2),
                    map: Arc::from(vec![0]),
                },
            )],
            UnitId(2),
            RewriteRows::Compaction {
                pks: Arc::from(vec![key(1)]),
                sources: Arc::from(vec![addr(1, 0)]),
            },
        );
        index.begin_group();
        assert_eq!(index.rewrite_slice(10), 0);
        index.end_group();
        assert_eq!(index.rewrite_slice(10), 1);
        assert_eq!(index.raw(&key(1)), Some(addr(2, 0)));
    }
}
