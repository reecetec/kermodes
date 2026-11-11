//! Derived-table operators. Each turns its input's delta for a cycle into its own delta. A
//! static table is the same code fed every row as one addition.

use std::collections::{BTreeSet, HashSet};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, UInt32Array};
use arrow::compute::{filter_record_batch, take_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::Rows as EncodedRows;

use crate::Expression;
use crate::agg::AggBy;
use crate::groups::{KeyKind, LocalGroups, Membership};
use crate::parallel::{PIECE_ROWS, row_pieces, run, threads_for};
use crate::session::Reader;
use crate::storage::{Delta, End, Modified, RowKey, Rows, Storage, nearest};

pub(crate) enum Operator {
    Where(Where),
    Update(Update),
    LastBy(Box<LastBy>),
    AggBy(AggBy),
    Edge(Edge),
}

impl Operator {
    pub fn process(
        &mut self,
        input: Reader<'_>,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        match self {
            Operator::Where(op) => op.process(delta, own),
            Operator::Update(op) => op.process(delta, own),
            Operator::LastBy(op) => op.process(input, delta, own),
            Operator::AggBy(op) => op.process(input, delta, own),
            Operator::Edge(op) => op.process(input, delta, own),
        }
    }
}

pub(crate) struct Where {
    predicate: Expression,
    /// Over an append-only input the output is append-only too and numbers its own rows;
    /// otherwise it keeps the input's keys so modifies and removes can be followed.
    rekey: bool,
}

impl Where {
    pub fn new(predicate: Expression, input_add_only: bool) -> Self {
        Self {
            predicate,
            rekey: input_add_only,
        }
    }

    fn process(&self, delta: &Delta, own: &Storage) -> Result<Delta, ArrowError> {
        let mut out = Delta::default();
        let added = row_pieces(&delta.added, PIECE_ROWS);
        let threads = threads_for(added.iter().map(|(keys, _)| keys.len()).sum());
        if self.rekey {
            let filtered = run(threads, added, |(), (_, batch)| {
                filter_record_batch(&batch, &self.mask(&batch)?)
            });
            let mut next = own.next_key();
            for batch in filtered {
                let batch = batch?;
                let count = batch.num_rows() as RowKey;
                if count > 0 {
                    out.added.push(Rows {
                        keys: (next..next + count).collect(),
                        batch,
                    });
                    next += count;
                }
            }
            return Ok(out);
        }
        for rows in &delta.removed {
            let keys = rows
                .keys
                .iter()
                .copied()
                .filter(|k| own.contains(*k))
                .collect();
            push_removed(&mut out, own, keys)?;
        }
        for rows in run(threads, added, |(), (keys, batch)| {
            let mask = self.mask(&batch)?;
            let picked: Vec<usize> = (0..mask.len()).filter(|i| mask.value(*i)).collect();
            Ok::<_, ArrowError>(Rows {
                keys: pick_keys(keys, &picked),
                batch: take_rows(&batch, &picked)?,
            })
        }) {
            out.added.push(rows?);
        }
        for modified in &delta.modified {
            let mask = self.mask(&modified.current)?;
            let (mut stay, mut enter, mut leave) = (Vec::new(), Vec::new(), Vec::new());
            for (i, key) in modified.keys.iter().enumerate() {
                match (mask.value(i), own.contains(*key)) {
                    (true, true) => stay.push(i),
                    (true, false) => enter.push(i),
                    (false, true) => leave.push(*key),
                    (false, false) => {}
                }
            }
            if !stay.is_empty() {
                let keys = pick_keys(&modified.keys, &stay);
                out.modified.push(Modified {
                    previous: own.gather(&keys)?,
                    current: take_rows(&modified.current, &stay)?,
                    keys,
                });
            }
            push_added(&mut out, &modified.keys, &modified.current, &enter)?;
            push_removed(&mut out, own, leave)?;
        }
        Ok(out)
    }

    /// The predicate with NULL read as false.
    fn mask(&self, batch: &RecordBatch) -> Result<BooleanArray, ArrowError> {
        let value = self.predicate.evaluate(batch)?;
        let mask = value.as_boolean();
        if mask.null_count() == 0 {
            return Ok(mask.clone());
        }
        Ok(mask.iter().map(|v| Some(v == Some(true))).collect())
    }
}

pub(crate) struct Assignment {
    /// The schema the expression is bound to: the input's plus earlier assignments.
    pub input: SchemaRef,
    pub expression: Expression,
    /// The column it writes: an existing index to replace, or the next index to append.
    pub position: usize,
}

pub(crate) struct Update {
    pub assignments: Vec<Assignment>,
    pub output: SchemaRef,
}

impl Update {
    fn process(&self, delta: &Delta, own: &Storage) -> Result<Delta, ArrowError> {
        let mut out = Delta::default();
        for rows in &delta.removed {
            push_removed(&mut out, own, rows.keys.clone())?;
        }
        for modified in &delta.modified {
            out.modified.push(Modified {
                keys: modified.keys.clone(),
                previous: own.gather(&modified.keys)?,
                current: self.compute(&modified.current)?,
            });
        }
        let added = row_pieces(&delta.added, PIECE_ROWS);
        let threads = threads_for(added.iter().map(|(keys, _)| keys.len()).sum());
        for rows in run(threads, added, |(), (keys, batch)| {
            Ok::<_, ArrowError>(Rows {
                keys: keys.to_vec(),
                batch: self.compute(&batch)?,
            })
        }) {
            out.added.push(rows?);
        }
        Ok(out)
    }

    fn compute(&self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for assignment in &self.assignments {
            let input = RecordBatch::try_new_with_options(
                assignment.input.clone(),
                columns.clone(),
                &rows,
            )?;
            let value = assignment.expression.evaluate(&input)?;
            if assignment.position == columns.len() {
                columns.push(value);
            } else {
                columns[assignment.position] = value;
            }
        }
        RecordBatch::try_new_with_options(self.output.clone(), columns, &rows)
    }
}

/// The last row of each group, by input key (arrival order).
pub(crate) struct LastBy {
    groups: Membership,
    /// Reads the group key from input batches, for grouping added rows piece by piece.
    keys: KeyKind,
    /// Whether groups keep every row key (the input can lose rows), not just the newest.
    keep_all: bool,
    /// Per group id: the input key whose values the output row currently shows.
    published: Vec<Option<RowKey>>,
}

/// One piece of added rows grouped on its own, kept per local group only (per-row group ids are
/// dropped once used): its hash, a row holding its key, its newest row key, and every row key
/// when groups keep them all.
struct LocalLast {
    batch: RecordBatch,
    encoded: Option<EncodedRows>,
    hash: Vec<u64>,
    first_row: Vec<u32>,
    newest: Vec<RowKey>,
    all: Option<Vec<Vec<RowKey>>>,
}

impl LastBy {
    pub fn new(groups: Membership, keys: KeyKind, keep_all: bool) -> Self {
        Self {
            groups,
            keys,
            keep_all,
            published: Vec::new(),
        }
    }

    fn sync(&mut self) {
        self.published.resize(self.groups.capacity(), None);
        for id in self.groups.take_created() {
            self.published[id as usize] = None;
        }
    }

    fn assign(&mut self, batch: &RecordBatch) -> Result<Vec<u32>, ArrowError> {
        let ids = self.groups.assign(batch, None)?;
        self.sync();
        Ok(ids)
    }

    /// Groups added rows in two phases: pieces group their rows in parallel, each keeping its
    /// groups' newest row keys; then the membership takes one entry per local group instead of
    /// one per row.
    fn add(&mut self, added: &[Rows]) -> Result<(), ArrowError> {
        let pieces = row_pieces(added, PIECE_ROWS);
        let threads = threads_for(pieces.iter().map(|(keys, _)| keys.len()).sum());
        let (kind, keep_all) = (&self.keys, self.keep_all);
        let locals = run(threads, pieces, |table, (keys, batch)| {
            let encoded = kind.encode(&batch)?;
            let view = kind.keys(&batch, encoded.as_ref());
            let groups = LocalGroups::build(&view, batch.num_rows(), table);
            let mut newest = vec![0; groups.len()];
            let mut all = keep_all.then(|| vec![Vec::new(); groups.len()]);
            for (id, key) in groups.ids.iter().zip(keys) {
                let group = *id as usize;
                newest[group] = newest[group].max(*key);
                if let Some(all) = &mut all {
                    all[group].push(*key);
                }
            }
            Ok::<_, ArrowError>(LocalLast {
                batch,
                encoded,
                hash: groups.hash,
                first_row: groups.first_row,
                newest,
                all,
            })
        });
        for local in locals {
            let local = local?;
            let view = self.keys.keys(&local.batch, local.encoded.as_ref());
            for group in 0..local.hash.len() {
                let row = local.first_row[group] as usize;
                let id = self.groups.group(
                    local.hash[group],
                    |key| view.matches(row, key),
                    || view.owned(row),
                );
                let members = &mut self.groups.members[id as usize];
                match &local.all {
                    Some(all) => all[group].iter().for_each(|key| members.insert(*key)),
                    None => members.insert(local.newest[group]),
                }
            }
        }
        self.sync();
        Ok(())
    }

    fn process(
        &mut self,
        input: Reader<'_>,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        let mut modified_rows = HashSet::new();
        for rows in &delta.removed {
            let ids = self.assign(&rows.batch)?;
            for (id, key) in ids.iter().zip(&rows.keys) {
                self.groups.members[*id as usize].remove(*key)?;
            }
        }
        for modified in &delta.modified {
            let before = self.assign(&modified.previous)?;
            let after = self.assign(&modified.current)?;
            for ((old, new), key) in before.iter().zip(&after).zip(&modified.keys) {
                if old != new {
                    self.groups.members[*old as usize].remove(*key)?;
                    self.groups.members[*new as usize].insert(*key);
                }
                modified_rows.insert(*key);
            }
        }
        self.add(&delta.added)?;

        let (mut added, mut changed, mut removed) = (Vec::new(), Vec::new(), Vec::new());
        for id in self.groups.take_touched() {
            let group = id as usize;
            let slot = self.groups.slot[group];
            let last = self.groups.members[group].last();
            match (self.published[group], last) {
                (None, Some(now)) => added.push((slot, now)),
                (Some(_), None) => removed.push(slot),
                (Some(was), Some(now)) if was != now || modified_rows.contains(&now) => {
                    changed.push((slot, now));
                }
                _ => {}
            }
            self.published[group] = last;
            if last.is_none() {
                self.groups.release(id);
            }
        }

        let mut out = Delta::default();
        let (slots, keys): (Vec<_>, Vec<_>) = added.into_iter().unzip();
        out.added.push(Rows {
            keys: slots,
            batch: input.gather(&keys)?,
        });
        let (slots, keys): (Vec<_>, Vec<_>) = changed.into_iter().unzip();
        out.modified.push(Modified {
            previous: own.gather(&slots)?,
            current: input.gather(&keys)?,
            keys: slots,
        });
        push_removed(&mut out, own, removed)?;
        Ok(out)
    }
}

/// The `n` rows nearest one end of the input's key order: `head` or `tail`.
pub(crate) struct Edge {
    end: End,
    n: usize,
    /// The output's keys: the input's `n` keys nearest `end`, or all of them if fewer. Every
    /// other input key lies beyond the kept key farthest from `end`.
    kept: BTreeSet<RowKey>,
}

impl Edge {
    pub fn new(end: End, n: usize) -> Self {
        Self {
            end,
            n,
            kept: BTreeSet::new(),
        }
    }

    /// The kept key farthest from `end`: the boundary.
    fn boundary(&self) -> Option<RowKey> {
        match self.end {
            End::First => self.kept.last().copied(),
            End::Last => self.kept.first().copied(),
        }
    }

    fn process(
        &mut self,
        input: Reader<'_>,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        let (mut leaving, mut entering, mut changed) = (Vec::new(), HashSet::new(), HashSet::new());
        for key in delta.removed.iter().flat_map(|r| &r.keys) {
            if self.kept.remove(key) {
                leaving.push(*key);
            }
        }
        for key in delta.modified.iter().flat_map(|m| &m.keys) {
            if self.kept.contains(key) {
                changed.insert(*key);
            }
        }
        // Short of `n`: take the input's next keys beyond the boundary. The input already holds
        // this cycle's additions, so from an empty set this is the answer.
        let was_empty = self.kept.is_empty();
        if self.kept.len() < self.n && input.len() > self.kept.len() {
            let wanted = self.n - self.kept.len();
            for key in input.edge_keys(self.end, wanted, self.boundary())? {
                self.kept.insert(key);
                entering.insert(key);
            }
        }
        // Additions on the near side of the boundary displace kept keys; only the `n` nearest
        // `end` can.
        if !was_empty {
            let mut added: Vec<RowKey> = delta
                .added
                .iter()
                .flat_map(|r| r.keys.iter().copied())
                .collect();
            nearest(&mut added, self.end, self.n);
            for key in added {
                if self.kept.contains(&key) {
                    continue;
                }
                if self.kept.len() < self.n {
                    self.kept.insert(key);
                    entering.insert(key);
                    continue;
                }
                let worst = self.boundary().expect("n > 0 keys are kept");
                let nearer = match self.end {
                    End::First => key < worst,
                    End::Last => key > worst,
                };
                if nearer {
                    self.kept.remove(&worst);
                    self.kept.insert(key);
                    entering.insert(key);
                    if !entering.remove(&worst) {
                        changed.remove(&worst);
                        leaving.push(worst);
                    }
                }
            }
        }

        let mut out = Delta::default();
        let mut entering: Vec<RowKey> = entering.into_iter().collect();
        entering.sort_unstable();
        out.added.push(Rows {
            batch: input.gather(&entering)?,
            keys: entering,
        });
        let mut changed: Vec<RowKey> = changed.into_iter().collect();
        changed.sort_unstable();
        out.modified.push(Modified {
            previous: own.gather(&changed)?,
            current: input.gather(&changed)?,
            keys: changed,
        });
        push_removed(&mut out, own, leaving)?;
        Ok(out)
    }
}

/// One input column's values at `keys`, NULL where there is no key.
pub(crate) fn pick_column(
    input: Reader<'_>,
    column: usize,
    keys: &[Option<RowKey>],
) -> Result<ArrayRef, ArrowError> {
    Ok(input.columns_at(&[column], keys)?.remove(0))
}

pub(crate) fn take_rows(batch: &RecordBatch, rows: &[usize]) -> Result<RecordBatch, ArrowError> {
    if rows.len() == batch.num_rows() && rows.iter().enumerate().all(|(i, r)| i == *r) {
        return Ok(batch.clone());
    }
    let indices = UInt32Array::from_iter_values(rows.iter().map(|r| *r as u32));
    take_record_batch(batch, &indices)
}

pub(crate) fn pick_keys(keys: &[RowKey], rows: &[usize]) -> Vec<RowKey> {
    rows.iter().map(|r| keys[*r]).collect()
}

fn push_added(
    out: &mut Delta,
    keys: &[RowKey],
    batch: &RecordBatch,
    rows: &[usize],
) -> Result<(), ArrowError> {
    if !rows.is_empty() {
        out.added.push(Rows {
            keys: pick_keys(keys, rows),
            batch: take_rows(batch, rows)?,
        });
    }
    Ok(())
}

pub(crate) fn push_removed(
    out: &mut Delta,
    own: &Storage,
    keys: Vec<RowKey>,
) -> Result<(), ArrowError> {
    if !keys.is_empty() {
        out.removed.push(Rows {
            batch: own.gather(&keys)?,
            keys,
        });
    }
    Ok(())
}
