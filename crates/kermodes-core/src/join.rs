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
//!
//! When the right side can change, the join stores no rows: a row is read as its left row plus
//! the right row the join's state matches it with (see `session::Reader`). Previous values are
//! therefore never read back from the output. They are built from the cycle's deltas: the left
//! row's previous values, and the right columns of its previous match as they were before the
//! cycle.

use std::collections::BTreeSet;
use std::ops::Bound;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::groups::{KeyKind, Membership};
use crate::parallel::{PIECE_ROWS, row_pieces, run, threads_for};
use crate::session::Reader;
use crate::storage::{Delta, Modified, RowKey, Rows};

type Map<K, V> = hashbrown::HashMap<K, V, RandomState>;
type Set<K> = hashbrown::HashSet<K, RandomState>;

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
        right: Reader<'_>,
        matches: &[Option<RowKey>],
    ) -> Result<RecordBatch, ArrowError> {
        self.output(left, right.columns_at(&self.right_columns, matches)?)
    }

    /// Output rows as they were before the cycle: `left` holds the left rows' previous values
    /// and `matches` their previous matches.
    fn previous(
        &self,
        left: &RecordBatch,
        right: Reader<'_>,
        before: &PreviousRight<'_>,
        matches: &[Option<RowKey>],
    ) -> Result<RecordBatch, ArrowError> {
        self.output(left, before.columns(right, &self.right_columns, matches)?)
    }

    fn output(&self, left: &RecordBatch, right: Vec<ArrayRef>) -> Result<RecordBatch, ArrowError> {
        let mut columns = left.columns().to_vec();
        columns.extend(right);
        let options = RecordBatchOptions::new().with_row_count(Some(left.num_rows()));
        RecordBatch::try_new_with_options(self.output.clone(), columns, &options)
    }
}

/// Right rows as they were before the cycle: the rows it removed or modified are read from the
/// right side's delta, the rest from its current rows.
struct PreviousRight<'a> {
    batches: Vec<&'a RecordBatch>,
    /// Per changed right row: its batch in `batches` and row within it.
    at: Map<RowKey, (usize, usize)>,
}

impl<'a> PreviousRight<'a> {
    fn new(delta: &'a Delta) -> Self {
        let changed = delta
            .removed
            .iter()
            .map(|r| (&r.keys, &r.batch))
            .chain(delta.modified.iter().map(|m| (&m.keys, &m.previous)));
        let (mut batches, mut at) = (Vec::new(), Map::default());
        for (keys, batch) in changed.filter(|(keys, _)| !keys.is_empty()) {
            for (row, key) in keys.iter().enumerate() {
                at.insert(*key, (batches.len(), row));
            }
            batches.push(batch);
        }
        Self { batches, at }
    }

    /// As `Reader::columns_at`, with each key's values from before the cycle.
    fn columns(
        &self,
        right: Reader<'_>,
        columns: &[usize],
        keys: &[Option<RowKey>],
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        if self.at.is_empty() {
            return right.columns_at(columns, keys);
        }
        // Unchanged rows come from current values (array 0), changed ones from their delta batch.
        let unchanged: Vec<_> = keys
            .iter()
            .map(|key| key.filter(|key| !self.at.contains_key(key)))
            .collect();
        let indices: Vec<(usize, usize)> = keys
            .iter()
            .enumerate()
            .map(|(i, key)| match key.and_then(|key| self.at.get(&key)) {
                Some((batch, row)) => (batch + 1, *row),
                None => (0, i),
            })
            .collect();
        let current = right.columns_at(columns, &unchanged)?;
        columns
            .iter()
            .zip(current)
            .map(|(column, current)| {
                let mut arrays: Vec<&dyn Array> = vec![current.as_ref()];
                arrays.extend(self.batches.iter().map(|b| b.column(*column).as_ref()));
                interleave(&arrays, &indices)
            })
            .collect()
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

/// The existing group of each row of `batch`, if any. Creates nothing, so pieces can look up in
/// parallel.
fn find_ids(
    kind: &KeyKind,
    groups: &Membership,
    batch: &RecordBatch,
) -> Result<Vec<Option<u32>>, ArrowError> {
    let encoded = kind.encode(batch)?;
    let keys = kind.keys(batch, encoded.as_ref());
    Ok((0..batch.num_rows())
        .map(|row| groups.find(keys.hash(row), |k| keys.matches(row, k)))
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

/// The latest of a group's `rows` (sorted by time) not after `time`. `cursor` holds the group's
/// previous lookup in this cycle: when times move forward, the search gallops on from it, so
/// time-ordered rows cost a short scan rather than a binary search over the group.
fn match_from(
    rows: &[(i64, RowKey)],
    time: i64,
    cursor: &mut Option<(i64, usize)>,
) -> Option<RowKey> {
    let after = match *cursor {
        Some((last, from)) if last <= time => from + gallop(&rows[from..], time),
        _ => rows.partition_point(|row| row.0 <= time),
    };
    *cursor = Some((time, after));
    after.checked_sub(1).map(|at| rows[at].1)
}

/// The cursor for `group` in a cycle's per-group cursors, growing them as groups appear.
fn cursor(cursors: &mut Vec<Option<(i64, usize)>>, group: u32) -> &mut Option<(i64, usize)> {
    let at = group as usize;
    if at >= cursors.len() {
        cursors.resize(at + 1, None);
    }
    &mut cursors[at]
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
fn changed_left(delta: &Delta) -> Set<RowKey> {
    delta
        .removed
        .iter()
        .flat_map(|r| &r.keys)
        .chain(delta.modified.iter().flat_map(|m| &m.keys))
        .copied()
        .collect()
}

/// Appends re-matched left rows (with their previous and new matches) as modified rows. Their
/// left values did not change this cycle.
fn push_rematched(
    out: &mut Delta,
    shape: &JoinShape,
    (left, right): (Reader<'_>, Reader<'_>),
    before: &PreviousRight<'_>,
    rematched: Vec<(RowKey, Option<RowKey>, Option<RowKey>)>,
) -> Result<(), ArrowError> {
    if rematched.is_empty() {
        return Ok(());
    }
    let (mut keys, mut was, mut now) = (Vec::new(), Vec::new(), Vec::new());
    for (key, previous, current) in rematched {
        keys.push(key);
        was.push(previous);
        now.push(current);
    }
    let rows = left.gather(&keys)?;
    out.modified.push(Modified {
        previous: shape.previous(&rows, right, before, &was)?,
        current: shape.rows(&rows, right, &now)?,
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
        left: Reader<'_>,
        left_delta: &Delta,
        right: Reader<'_>,
        right_delta: &Delta,
    ) -> Result<Delta, ArrowError> {
        match self {
            Join::Natural(join) => join.process(left, left_delta, right, right_delta),
            Join::AsOf(join) => join.process(left, left_delta, right, right_delta),
        }
    }

    /// Whether the output computes its rows on read rather than storing them: when the right
    /// side can change, stored rows would copy every left column for each row, while the join
    /// already keeps what a read needs (its groups and their right rows).
    pub fn computes_rows(&self) -> bool {
        match self {
            Join::Natural(join) => join.tracking != Tracking::None,
            Join::AsOf(join) => join.tracking,
        }
    }

    /// The current output rows for current `left` rows.
    pub fn rows(&self, left: &RecordBatch, right: Reader<'_>) -> Result<RecordBatch, ArrowError> {
        match self {
            Join::Natural(join) => {
                let matches: Vec<_> = find_ids(&join.shape.left_keys, &join.groups, left)?
                    .into_iter()
                    .map(|id| id.and_then(|id| join.matched(id)))
                    .collect();
                join.shape.rows(left, right, &matches)
            }
            Join::AsOf(join) => {
                let matches: Vec<_> = find_ids(&join.shape.left_keys, &join.groups, left)?
                    .into_iter()
                    .zip(times(&join.left_at, left))
                    .map(|(id, time)| id.and_then(|id| join.matched(id, time)))
                    .collect();
                join.shape.rows(left, right, &matches)
            }
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

    /// A group's match before this cycle: as recorded in `was` before the cycle changed the
    /// group's right rows, or its current match if it did not.
    fn previous_match(&self, was: &Map<u32, Option<RowKey>>, group: u32) -> Option<RowKey> {
        was.get(&group)
            .copied()
            .unwrap_or_else(|| self.matched(group))
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
        left: Reader<'_>,
        left_delta: &Delta,
        right: Reader<'_>,
        right_delta: &Delta,
    ) -> Result<Delta, ArrowError> {
        // Each group the right side changes, with its match before its first change.
        let mut was = Map::default();
        for rows in &right_delta.removed {
            for (id, key) in self.ids(false, &rows.batch)?.into_iter().zip(&rows.keys) {
                was.entry(id).or_insert_with(|| self.matched(id));
                self.right_rows[id as usize].remove(key);
            }
        }
        for modified in &right_delta.modified {
            let before = self.ids(false, &modified.previous)?;
            let after = self.ids(false, &modified.current)?;
            for ((old, new), key) in before.into_iter().zip(after).zip(&modified.keys) {
                for id in [old, new] {
                    was.entry(id).or_insert_with(|| self.matched(id));
                }
                self.right_rows[old as usize].remove(key);
                self.right_rows[new as usize].insert(*key);
            }
        }
        for (keys, batch) in row_pieces(&right_delta.added, PIECE_ROWS) {
            for (id, key) in self.ids(false, &batch)?.into_iter().zip(keys) {
                was.entry(id).or_insert_with(|| self.matched(id));
                self.right_rows[id as usize].insert(*key);
            }
        }
        let mut changed: Vec<u32> = was.keys().copied().collect();
        changed.sort_unstable();
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
                let (previous, matched) = (was[&id], self.matched(id));
                let group = id as usize;
                let keys: Box<dyn Iterator<Item = &RowKey>> = match self.tracking {
                    Tracking::Appended => Box::new(self.appended[group].iter()),
                    _ => Box::new(self.changing[group].iter()),
                };
                rematched.extend(
                    keys.filter(|key| !skip.contains(*key))
                        .map(|key| (*key, previous, matched)),
                );
            }
        }

        let before = PreviousRight::new(right_delta);
        let mut out = Delta::default();
        for rows in &left_delta.removed {
            let ids = self.ids(true, &rows.batch)?;
            for (id, key) in ids.iter().zip(&rows.keys) {
                self.remove_left(*id, *key);
            }
            let previous: Vec<_> = ids
                .iter()
                .map(|id| self.previous_match(&was, *id))
                .collect();
            out.removed.push(Rows {
                keys: rows.keys.clone(),
                batch: self
                    .shape
                    .previous(&rows.batch, right, &before, &previous)?,
            });
        }
        for modified in &left_delta.modified {
            let old_ids = self.ids(true, &modified.previous)?;
            let new_ids = self.ids(true, &modified.current)?;
            for ((old, new), key) in old_ids.iter().zip(&new_ids).zip(&modified.keys) {
                self.remove_left(*old, *key);
                self.insert_left(*new, *key);
            }
            let previous: Vec<_> = old_ids
                .iter()
                .map(|id| self.previous_match(&was, *id))
                .collect();
            let matches: Vec<_> = new_ids.iter().map(|id| self.matched(*id)).collect();
            out.modified.push(Modified {
                previous: self
                    .shape
                    .previous(&modified.previous, right, &before, &previous)?,
                current: self.shape.rows(&modified.current, right, &matches)?,
                keys: modified.keys.clone(),
            });
        }
        let added = row_pieces(&left_delta.added, PIECE_ROWS);
        if self.tracking == Tracking::None {
            // Nothing is recorded per group, so pieces look up in parallel and create no groups.
            let threads = threads_for(added.iter().map(|(keys, _)| keys.len()).sum());
            let (shape, groups, right_rows) = (&self.shape, &self.groups, &self.right_rows);
            for rows in run(threads, added, |(), (keys, batch)| {
                let matches: Vec<_> = find_ids(&shape.left_keys, groups, &batch)?
                    .into_iter()
                    .map(|id| id.and_then(|id| right_rows[id as usize].first().copied()))
                    .collect();
                Ok::<_, ArrowError>(Rows {
                    keys: keys.to_vec(),
                    batch: shape.rows(&batch, right, &matches)?,
                })
            }) {
                out.added.push(rows?);
            }
        } else {
            for (keys, batch) in added {
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
        }
        push_rematched(&mut out, &self.shape, (left, right), &before, rematched)?;

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

    /// A row's match before this cycle: as `matched`, among the right rows that existed then.
    /// Right rows are never removed or modified, so only those `added` this cycle are skipped.
    fn previous_matched(
        &self,
        added: &Set<RowKey>,
        group: u32,
        time: Option<i64>,
    ) -> Option<RowKey> {
        let (rows, time) = (&self.right_rows[group as usize], time?);
        let after = rows.partition_point(|row| row.0 <= time);
        rows[..after]
            .iter()
            .rev()
            .find(|(_, key)| !added.contains(key))
            .map(|(_, key)| *key)
    }

    fn process(
        &mut self,
        left: Reader<'_>,
        left_delta: &Delta,
        right: Reader<'_>,
        right_delta: &Delta,
    ) -> Result<Delta, ArrowError> {
        if right_delta.removed.iter().any(|r| !r.keys.is_empty())
            || right_delta.modified.iter().any(|m| !m.keys.is_empty())
        {
            return Err(ArrowError::ComputeError(
                "as_of_join: the right table removed or modified rows; it must only gain rows"
                    .into(),
            ));
        }
        // Right rows new this cycle, which previous matches skip. Only a ticking right side needs
        // them: a static one gains rows only as the join is built, when the left side only adds.
        let (mut points, mut new_right) = (Vec::new(), Set::default());
        for (keys, batch) in row_pieces(&right_delta.added, PIECE_ROWS) {
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
                    if self.tracking {
                        new_right.insert(*key);
                    }
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
                        .map(|(at, key)| {
                            (
                                *key,
                                self.previous_matched(&new_right, id, Some(*at)),
                                matched,
                            )
                        }),
                );
            }
        }

        // Right values never change, so previous right columns are current ones.
        let before = PreviousRight::new(right_delta);
        let mut out = Delta::default();
        for rows in &left_delta.removed {
            let located = self.locate(true, &rows.batch)?;
            if self.tracking {
                for ((id, time), key) in located.iter().zip(&rows.keys) {
                    if let Some(time) = time {
                        self.left_rows[*id as usize].remove(&(*time, *key));
                    }
                }
            }
            let previous: Vec<_> = located
                .iter()
                .map(|(id, time)| self.previous_matched(&new_right, *id, *time))
                .collect();
            out.removed.push(Rows {
                keys: rows.keys.clone(),
                batch: self
                    .shape
                    .previous(&rows.batch, right, &before, &previous)?,
            });
        }
        for modified in &left_delta.modified {
            let before_rows = self.locate(true, &modified.previous)?;
            let after = self.locate(true, &modified.current)?;
            if self.tracking {
                for (((old, was), (new, now)), key) in
                    before_rows.iter().zip(&after).zip(&modified.keys)
                {
                    if let Some(was) = was {
                        self.left_rows[*old as usize].remove(&(*was, *key));
                    }
                    if let Some(now) = now {
                        self.left_rows[*new as usize].insert((*now, *key));
                    }
                }
            }
            let previous: Vec<_> = before_rows
                .iter()
                .map(|(id, time)| self.previous_matched(&new_right, *id, *time))
                .collect();
            let matches: Vec<_> = after.iter().map(|(id, t)| self.matched(*id, *t)).collect();
            out.modified.push(Modified {
                previous: self
                    .shape
                    .previous(&modified.previous, right, &before, &previous)?,
                current: self.shape.rows(&modified.current, right, &matches)?,
                keys: modified.keys.clone(),
            });
        }
        let added = row_pieces(&left_delta.added, PIECE_ROWS);
        if !self.tracking {
            // Nothing is recorded per group, so pieces look up in parallel and create no groups;
            // each thread keeps its own cursors.
            let threads = threads_for(added.iter().map(|(keys, _)| keys.len()).sum());
            let (shape, left_at) = (&self.shape, &self.left_at);
            let (groups, right_rows) = (&self.groups, &self.right_rows);
            for rows in run(threads, added, |cursors: &mut Vec<_>, (keys, batch)| {
                let ids = find_ids(&shape.left_keys, groups, &batch)?;
                let matches: Vec<_> = ids
                    .into_iter()
                    .zip(times(left_at, &batch))
                    .map(|located| match located {
                        (Some(id), Some(time)) => {
                            match_from(&right_rows[id as usize], time, cursor(cursors, id))
                        }
                        _ => None,
                    })
                    .collect();
                Ok::<_, ArrowError>(Rows {
                    keys: keys.to_vec(),
                    batch: shape.rows(&batch, right, &matches)?,
                })
            }) {
                out.added.push(rows?);
            }
        } else {
            let mut cursors = Vec::new();
            for (keys, batch) in added {
                let located = self.locate(true, &batch)?;
                for ((id, time), key) in located.iter().zip(keys) {
                    if let Some(time) = time {
                        self.left_rows[*id as usize].insert((*time, *key));
                    }
                }
                let matches: Vec<_> = located
                    .iter()
                    .map(|(id, time)| {
                        let rows = &self.right_rows[*id as usize];
                        time.and_then(|time| match_from(rows, time, cursor(&mut cursors, *id)))
                    })
                    .collect();
                out.added.push(Rows {
                    keys: keys.to_vec(),
                    batch: self.shape.rows(&batch, right, &matches)?,
                });
            }
        }
        push_rematched(&mut out, &self.shape, (left, right), &before, rematched)?;

        for id in self.groups.take_touched() {
            let group = id as usize;
            if self.left_rows[group].is_empty() && self.right_rows[group].is_empty() {
                self.groups.release(id);
            }
        }
        Ok(out)
    }
}
