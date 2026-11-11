//! Joins: every left row, with columns from its matching right row.
//!
//! Output rows keep the left table's keys. Key values from both sides share one grouping table,
//! so a change on the right finds the left rows it affects through the group id. Keys match as in
//! `agg_by`: by value, NULL matching NULL. A left row without a match gets NULLs.
//!
//! A cycle applies the right side's changes first, then collects the left rows those changes
//! re-match, then applies the left side's changes. Left rows added this cycle are looked up
//! against the new right rows directly, so the only rows the re-match must skip are those the
//! left side removed or modified this cycle. Left rows are kept per group only when the right
//! side can change: against a static right side, nothing ever re-matches.

use std::collections::{BTreeSet, HashSet};
use std::ops::Bound;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::groups::{KeyKind, Membership};
use crate::ops::push_removed;
use crate::storage::{Delta, Modified, RowKey, Rows, Storage};

/// What a join reads and writes.
pub(crate) struct JoinShape {
    /// The `on` columns, within each side's schema.
    pub left_keys: KeyKind,
    pub right_keys: KeyKind,
    /// Right columns added after the left columns, in output order.
    pub right_columns: Vec<usize>,
    pub output: SchemaRef,
}

impl JoinShape {
    /// Output rows: `left`'s rows followed by the right columns of `matches`.
    fn rows(
        &self,
        left: &RecordBatch,
        right: &Storage,
        matches: &[Option<RowKey>],
    ) -> Result<RecordBatch, ArrowError> {
        let mut columns = left.columns().to_vec();
        columns.extend(right.columns_at(&self.right_columns, matches)?);
        let options = RecordBatchOptions::new().with_row_count(Some(left.num_rows()));
        RecordBatch::try_new_with_options(self.output.clone(), columns, &options)
    }
}

/// How a join keeps each group's left rows.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tracking {
    /// The right side never changes: no left row is ever re-matched.
    None,
    /// The left side only gains rows: each group's rows are appended in key order.
    Appended,
    /// The left side changes: each group's rows are a sorted set.
    Changing,
}

/// The group of each row of `batch`, creating groups as needed.
fn group_ids(
    kind: &KeyKind,
    groups: &mut Membership,
    batch: &RecordBatch,
) -> Result<Vec<u32>, ArrowError> {
    let encoded = kind.encode(batch)?;
    let keys = kind.keys(batch, encoded.as_ref());
    Ok((0..batch.num_rows())
        .map(|row| groups.group(keys.hash(row), |k| keys.matches(row, k), || keys.owned(row)))
        .collect())
}

/// Each row's value in an integer, date, or timestamp column read through `kind`.
fn times(kind: &KeyKind, batch: &RecordBatch) -> Vec<Option<i64>> {
    let keys = kind.keys(batch, None);
    (0..batch.num_rows()).map(|row| keys.int(row)).collect()
}

/// Grows one piece of per-group state to `len` groups and clears the groups in `created` (new or
/// reused since the last call to `Membership::take_created`).
fn sync<T: Default>(len: usize, created: &[u32], state: &mut Vec<T>) {
    state.resize_with(len, T::default);
    for id in created {
        state[*id as usize] = T::default();
    }
}

/// Rows per piece when a join works through a large batch: per-row scratch (group ids, times,
/// matches, gather indices) stays bounded instead of growing with the batch.
const PIECE_ROWS: usize = 1 << 16;

/// `rows` in pieces of at most `PIECE_ROWS`: each piece's keys and rows.
fn pieces(rows: &Rows) -> impl Iterator<Item = (&[RowKey], RecordBatch)> {
    (0..rows.keys.len()).step_by(PIECE_ROWS).map(move |start| {
        let len = PIECE_ROWS.min(rows.keys.len() - start);
        (&rows.keys[start..start + len], rows.batch.slice(start, len))
    })
}

/// How many leading `rows` have a time not after `time`, by exponential then binary search:
/// cheap when the answer is near the start.
fn gallop(rows: &[(i64, RowKey)], time: i64) -> usize {
    let mut bound = 1;
    while bound <= rows.len() && rows[bound - 1].0 <= time {
        bound *= 2;
    }
    let start = bound / 2;
    start + rows[start..bound.min(rows.len())].partition_point(|row| row.0 <= time)
}

/// The left rows a cycle removed or modified: re-matches skip them, since they are emitted (or
/// gone) already.
fn changed_left(delta: &Delta) -> HashSet<RowKey> {
    delta
        .removed
        .iter()
        .flat_map(|r| &r.keys)
        .chain(delta.modified.iter().flat_map(|m| &m.keys))
        .copied()
        .collect()
}

/// Appends re-matched left rows as modified rows, with current left values and new matches.
fn push_rematched(
    out: &mut Delta,
    shape: &JoinShape,
    left: &Storage,
    right: &Storage,
    own: &Storage,
    rematched: Vec<(RowKey, Option<RowKey>)>,
) -> Result<(), ArrowError> {
    if rematched.is_empty() {
        return Ok(());
    }
    let (keys, matches): (Vec<_>, Vec<_>) = rematched.into_iter().unzip();
    out.modified.push(Modified {
        previous: own.gather(&keys)?,
        current: shape.rows(&left.gather(&keys)?, right, &matches)?,
        keys,
    });
    Ok(())
}

pub(crate) enum Join {
    Natural(NaturalJoin),
    AsOf(AsOfJoin),
}

impl Join {
    pub fn process(
        &mut self,
        left: &Storage,
        left_delta: &Delta,
        right: &Storage,
        right_delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        match self {
            Join::Natural(join) => join.process(left, left_delta, right, right_delta, own),
            Join::AsOf(join) => join.process(left, left_delta, right, right_delta, own),
        }
    }
}

/// Each left row with the right row of the same key; a key with more than one right row fails
/// the table.
pub(crate) struct NaturalJoin {
    shape: JoinShape,
    tracking: Tracking,
    /// Key values seen on either side.
    groups: Membership,
    /// Per group: the right rows with the key.
    right_rows: Vec<BTreeSet<RowKey>>,
    /// Per group: its left rows, in the form `tracking` says (the other stays empty).
    appended: Vec<Vec<RowKey>>,
    changing: Vec<BTreeSet<RowKey>>,
}

impl NaturalJoin {
    pub fn new(shape: JoinShape, tracking: Tracking, groups: Membership) -> Self {
        Self {
            shape,
            tracking,
            groups,
            right_rows: Vec::new(),
            appended: Vec::new(),
            changing: Vec::new(),
        }
    }

    fn ids(&mut self, left: bool, batch: &RecordBatch) -> Result<Vec<u32>, ArrowError> {
        let kind = if left {
            &self.shape.left_keys
        } else {
            &self.shape.right_keys
        };
        let ids = group_ids(kind, &mut self.groups, batch)?;
        let (len, created) = (self.groups.capacity(), self.groups.take_created());
        sync(len, &created, &mut self.right_rows);
        sync(len, &created, &mut self.appended);
        sync(len, &created, &mut self.changing);
        Ok(ids)
    }

    fn matched(&self, group: u32) -> Option<RowKey> {
        self.right_rows[group as usize].first().copied()
    }

    fn insert_left(&mut self, group: u32, key: RowKey) {
        match self.tracking {
            Tracking::None => {}
            Tracking::Appended => self.appended[group as usize].push(key),
            Tracking::Changing => {
                self.changing[group as usize].insert(key);
            }
        }
    }

    fn remove_left(&mut self, group: u32, key: RowKey) {
        if self.tracking == Tracking::Changing {
            self.changing[group as usize].remove(&key);
        }
    }

    fn process(
        &mut self,
        left: &Storage,
        left_delta: &Delta,
        right: &Storage,
        right_delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        let mut changed = Vec::new();
        for rows in &right_delta.removed {
            for (id, key) in self.ids(false, &rows.batch)?.into_iter().zip(&rows.keys) {
                self.right_rows[id as usize].remove(key);
                changed.push(id);
            }
        }
        for modified in &right_delta.modified {
            let before = self.ids(false, &modified.previous)?;
            let after = self.ids(false, &modified.current)?;
            for ((old, new), key) in before.into_iter().zip(after).zip(&modified.keys) {
                self.right_rows[old as usize].remove(key);
                self.right_rows[new as usize].insert(*key);
                changed.extend([old, new]);
            }
        }
        for (keys, batch) in right_delta.added.iter().flat_map(pieces) {
            for (id, key) in self.ids(false, &batch)?.into_iter().zip(keys) {
                self.right_rows[id as usize].insert(*key);
                changed.push(id);
            }
        }
        changed.sort_unstable();
        changed.dedup();
        if let Some(&id) = changed
            .iter()
            .find(|id| self.right_rows[**id as usize].len() > 1)
        {
            return Err(ArrowError::ComputeError(format!(
                "natural_join: the right table has {} rows for one key; it needs at most one",
                self.right_rows[id as usize].len()
            )));
        }

        // Left rows joined before this cycle whose key's right row changed.
        let mut rematched = Vec::new();
        if self.tracking != Tracking::None && !changed.is_empty() {
            let skip = changed_left(left_delta);
            for id in changed {
                let matched = self.matched(id);
                let group = id as usize;
                let keys: Box<dyn Iterator<Item = &RowKey>> = match self.tracking {
                    Tracking::Appended => Box::new(self.appended[group].iter()),
                    _ => Box::new(self.changing[group].iter()),
                };
                rematched.extend(
                    keys.filter(|key| !skip.contains(key))
                        .map(|key| (*key, matched)),
                );
            }
        }

        let mut out = Delta::default();
        for rows in &left_delta.removed {
            for (id, key) in self.ids(true, &rows.batch)?.into_iter().zip(&rows.keys) {
                self.remove_left(id, *key);
            }
            push_removed(&mut out, own, rows.keys.clone())?;
        }
        for modified in &left_delta.modified {
            let before = self.ids(true, &modified.previous)?;
            let after = self.ids(true, &modified.current)?;
            for ((old, new), key) in before.into_iter().zip(&after).zip(&modified.keys) {
                self.remove_left(old, *key);
                self.insert_left(*new, *key);
            }
            let matches: Vec<_> = after.iter().map(|id| self.matched(*id)).collect();
            out.modified.push(Modified {
                previous: own.gather(&modified.keys)?,
                current: self.shape.rows(&modified.current, right, &matches)?,
                keys: modified.keys.clone(),
            });
        }
        for (keys, batch) in left_delta.added.iter().flat_map(pieces) {
            let ids = self.ids(true, &batch)?;
            for (id, key) in ids.iter().zip(keys) {
                self.insert_left(*id, *key);
            }
            let matches: Vec<_> = ids.iter().map(|id| self.matched(*id)).collect();
            out.added.push(Rows {
                keys: keys.to_vec(),
                batch: self.shape.rows(&batch, right, &matches)?,
            });
        }
        push_rematched(&mut out, &self.shape, left, right, own, rematched)?;

        for id in self.groups.take_touched() {
            let group = id as usize;
            if self.right_rows[group].is_empty()
                && self.appended[group].is_empty()
                && self.changing[group].is_empty()
            {
                self.groups.release(id);
            }
        }
        Ok(out)
    }
}

/// Each left row with the right row of the same key at the greatest time not after the left
/// row's; among equal times, the latest right row. The right table only gains rows, so a right
/// row added at time `t` becomes the match of exactly the left rows timed from `t` up to the
/// group's next right time.
pub(crate) struct AsOfJoin {
    shape: JoinShape,
    /// Whether left rows are kept per group: only if the right side ticks.
    tracking: bool,
    /// The time column of each side, read as integers.
    left_at: KeyKind,
    right_at: KeyKind,
    groups: Membership,
    /// Per group: right rows by `(time, row key)`, sorted; rows with a NULL time never match.
    /// Rows mostly arrive in time order, so a sorted vector usually just grows at the end.
    right_rows: Vec<Vec<(i64, RowKey)>>,
    /// Per group: left rows with a time, by `(time, row key)`, when tracked.
    left_rows: Vec<BTreeSet<(i64, RowKey)>>,
}

impl AsOfJoin {
    pub fn new(
        shape: JoinShape,
        tracking: bool,
        left_at: KeyKind,
        right_at: KeyKind,
        groups: Membership,
    ) -> Self {
        Self {
            shape,
            tracking,
            left_at,
            right_at,
            groups,
            right_rows: Vec::new(),
            left_rows: Vec::new(),
        }
    }

    /// Each row's group and time.
    fn locate(
        &mut self,
        left: bool,
        batch: &RecordBatch,
    ) -> Result<Vec<(u32, Option<i64>)>, ArrowError> {
        let (kind, at) = if left {
            (&self.shape.left_keys, &self.left_at)
        } else {
            (&self.shape.right_keys, &self.right_at)
        };
        let ids = group_ids(kind, &mut self.groups, batch)?;
        let times = times(at, batch);
        let (len, created) = (self.groups.capacity(), self.groups.take_created());
        sync(len, &created, &mut self.right_rows);
        sync(len, &created, &mut self.left_rows);
        Ok(ids.into_iter().zip(times).collect())
    }

    fn matched(&self, group: u32, time: Option<i64>) -> Option<RowKey> {
        let (rows, time) = (&self.right_rows[group as usize], time?);
        let after = rows.partition_point(|row| row.0 <= time);
        after.checked_sub(1).map(|at| rows[at].1)
    }

    /// `matched`, resuming from the group's previous lookup in `cursors` when times move
    /// forward: rows usually arrive in time order, so a lookup is a short gallop rather than a
    /// binary search over the group. Valid while the right rows do not change.
    fn matched_from(
        &self,
        cursors: &mut Vec<Option<(i64, usize)>>,
        group: u32,
        time: Option<i64>,
    ) -> Option<RowKey> {
        let (rows, time, at) = (&self.right_rows[group as usize], time?, group as usize);
        if at >= cursors.len() {
            cursors.resize(at + 1, None);
        }
        let after = match cursors[at] {
            Some((last, from)) if last <= time => from + gallop(&rows[from..], time),
            _ => rows.partition_point(|row| row.0 <= time),
        };
        cursors[at] = Some((time, after));
        after.checked_sub(1).map(|at| rows[at].1)
    }

    fn process(
        &mut self,
        left: &Storage,
        left_delta: &Delta,
        right: &Storage,
        right_delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        if right_delta.removed.iter().any(|r| !r.keys.is_empty())
            || right_delta.modified.iter().any(|m| !m.keys.is_empty())
        {
            return Err(ArrowError::ComputeError(
                "as_of_join: the right table removed or modified rows; it must only gain rows"
                    .into(),
            ));
        }
        let mut points = Vec::new();
        for (keys, batch) in right_delta.added.iter().flat_map(pieces) {
            for ((id, time), key) in self.locate(false, &batch)?.into_iter().zip(keys) {
                if let Some(time) = time {
                    let rows = &mut self.right_rows[id as usize];
                    let row = (time, *key);
                    if rows.last().is_none_or(|last| *last <= row) {
                        rows.push(row);
                    } else {
                        rows.insert(rows.partition_point(|other| *other < row), row);
                    }
                    points.push((id, time));
                }
            }
        }

        // Left rows joined before this cycle that a new right row now matches: from its time up
        // to the group's next right time.
        let mut rematched = Vec::new();
        if self.tracking && !points.is_empty() {
            let skip = changed_left(left_delta);
            points.sort_unstable();
            points.dedup();
            for (id, time) in points {
                let group = id as usize;
                let matched = self.matched(id, Some(time));
                let rows = &self.right_rows[group];
                let next = rows
                    .get(rows.partition_point(|row| row.0 <= time))
                    .map(|row| row.0);
                let end = next.map_or(Bound::Unbounded, |next| Bound::Excluded((next, 0)));
                rematched.extend(
                    self.left_rows[group]
                        .range((Bound::Included((time, 0)), end))
                        .filter(|(_, key)| !skip.contains(key))
                        .map(|(_, key)| (*key, matched)),
                );
            }
        }

        let mut out = Delta::default();
        for rows in &left_delta.removed {
            let located = self.locate(true, &rows.batch)?;
            if self.tracking {
                for ((id, time), key) in located.into_iter().zip(&rows.keys) {
                    if let Some(time) = time {
                        self.left_rows[id as usize].remove(&(time, *key));
                    }
                }
            }
            push_removed(&mut out, own, rows.keys.clone())?;
        }
        for modified in &left_delta.modified {
            let before = self.locate(true, &modified.previous)?;
            let after = self.locate(true, &modified.current)?;
            if self.tracking {
                for (((old, was), (new, now)), key) in
                    before.into_iter().zip(&after).zip(&modified.keys)
                {
                    if let Some(was) = was {
                        self.left_rows[old as usize].remove(&(was, *key));
                    }
                    if let Some(now) = now {
                        self.left_rows[*new as usize].insert((*now, *key));
                    }
                }
            }
            let matches: Vec<_> = after.iter().map(|(id, t)| self.matched(*id, *t)).collect();
            out.modified.push(Modified {
                previous: own.gather(&modified.keys)?,
                current: self.shape.rows(&modified.current, right, &matches)?,
                keys: modified.keys.clone(),
            });
        }
        let mut cursors = Vec::new();
        for (keys, batch) in left_delta.added.iter().flat_map(pieces) {
            let located = self.locate(true, &batch)?;
            if self.tracking {
                for ((id, time), key) in located.iter().zip(keys) {
                    if let Some(time) = time {
                        self.left_rows[*id as usize].insert((*time, *key));
                    }
                }
            }
            let matches: Vec<_> = located
                .iter()
                .map(|(id, t)| self.matched_from(&mut cursors, *id, *t))
                .collect();
            out.added.push(Rows {
                keys: keys.to_vec(),
                batch: self.shape.rows(&batch, right, &matches)?,
            });
        }
        push_rematched(&mut out, &self.shape, left, right, own, rematched)?;

        for id in self.groups.take_touched() {
            let group = id as usize;
            if self.left_rows[group].is_empty() && self.right_rows[group].is_empty() {
                self.groups.release(id);
            }
        }
        Ok(out)
    }
}
