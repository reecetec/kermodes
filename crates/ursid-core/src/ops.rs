//! Derived-table operators. Each turns its input's delta for a cycle into its own delta. A
//! static table is the same code fed every row as one addition.

use std::collections::HashSet;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, UInt32Array};
use arrow::compute::{filter_record_batch, take_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::Expression;
use crate::agg::AggBy;
use crate::groups::Membership;
use crate::storage::{Delta, Modified, RowKey, Rows, Storage};

pub(crate) enum Operator {
    Where(Where),
    Update(Update),
    LastBy(LastBy),
    AggBy(AggBy),
}

impl Operator {
    pub fn process(
        &mut self,
        input: &Storage,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        match self {
            Operator::Where(op) => op.process(delta, own),
            Operator::Update(op) => op.process(delta, own),
            Operator::LastBy(op) => op.process(input, delta, own),
            Operator::AggBy(op) => op.process(input, delta, own),
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
        if self.rekey {
            let mut next = own.next_key();
            for rows in &delta.added {
                let batch = filter_record_batch(&rows.batch, &self.mask(&rows.batch)?)?;
                let count = batch.num_rows() as RowKey;
                out.added.push(Rows {
                    keys: (next..next + count).collect(),
                    batch,
                });
                next += count;
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
        for rows in &delta.added {
            let mask = self.mask(&rows.batch)?;
            let picked: Vec<usize> = (0..mask.len()).filter(|i| mask.value(*i)).collect();
            push_added(&mut out, &rows.keys, &rows.batch, &picked)?;
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
        for rows in &delta.added {
            out.added.push(Rows {
                keys: rows.keys.clone(),
                batch: self.compute(&rows.batch)?,
            });
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
    /// Per group id: the input key whose values the output row currently shows.
    published: Vec<Option<RowKey>>,
}

impl LastBy {
    pub fn new(groups: Membership) -> Self {
        Self {
            groups,
            published: Vec::new(),
        }
    }

    fn assign(&mut self, batch: &RecordBatch) -> Result<Vec<u32>, ArrowError> {
        let ids = self.groups.assign(batch, None)?;
        self.published.resize(self.groups.capacity(), None);
        for id in self.groups.take_created() {
            self.published[id as usize] = None;
        }
        Ok(ids)
    }

    fn process(
        &mut self,
        input: &Storage,
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
        for rows in &delta.added {
            let ids = self.assign(&rows.batch)?;
            for (id, key) in ids.iter().zip(&rows.keys) {
                self.groups.members[*id as usize].insert(*key);
            }
        }

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

/// One input column's values at `keys`, NULL where there is no key.
pub(crate) fn pick_column(
    input: &Storage,
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
