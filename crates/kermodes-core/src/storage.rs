//! Table storage and the per-cycle changes that flow between tables.
//!
//! Append-only tables keep the batches they were given as immutable chunks, keyed by position.
//! Every other table gives each live row a slot in mutable columns (`slots`): a new version
//! overwrites the row's slot and a removed row's slot is reused, so memory follows the live rows.
//! Deltas carry previous values as their own copies, taken before a cycle is applied.

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, new_empty_array, new_null_array};
use arrow::compute::{concat_batches, interleave, interleave_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use hashbrown::HashMap;

use crate::slots::Slots;

/// A row's identity within one table: no two live rows share a key, and a cycle never removes
/// and adds the same key (that is a modify). A key removed may come back in a later cycle: a
/// slice's keys are its positions.
pub(crate) type RowKey = u64;

/// Appended batches under `SMALL_ROWS` are combined into chunks of at least `CHUNK_ROWS`. Arrow
/// costs about 1 KB per batch: a table fed tiny batches (an ingester flushing every 100 ms gives
/// ~10 rows each) would otherwise hold about 3x its data, and every lookup would walk hundreds of
/// thousands of chunks. From 4,096 rows that cost is under 1%, and a copy would only raise the
/// peak, so larger batches are kept as given. Each row is copied at most once.
const SMALL_ROWS: usize = 4_096;
const CHUNK_ROWS: usize = 1 << 16;

enum Index {
    /// Keys `0..len`, in chunk order: append-only tables.
    Dense {
        chunks: Vec<RecordBatch>,
        starts: Vec<RowKey>,
        len: RowKey,
        /// The chunks from here on are small batches waiting to be combined.
        small_from: usize,
    },
    /// Any keys, each in a slot. A hash map from key to slot: lookups are the hot path; key order
    /// is needed only for snapshots, which sort.
    Sparse {
        slot_of: HashMap<RowKey, u32, RandomState>,
        columns: Slots,
        free: Vec<u32>,
    },
}

pub(crate) struct Storage {
    schema: SchemaRef,
    index: Index,
}

/// Rows with their keys, in the same order.
#[derive(Clone)]
pub(crate) struct Rows {
    pub keys: Vec<RowKey>,
    pub batch: RecordBatch,
}

/// Modified rows: same keys, values before and after the cycle.
#[derive(Clone)]
pub(crate) struct Modified {
    pub keys: Vec<RowKey>,
    pub previous: RecordBatch,
    pub current: RecordBatch,
}

/// What changed in a table during one cycle. `added`, `removed`, and `modified` keys are
/// disjoint; removed rows carry their last values.
#[derive(Clone, Default)]
pub(crate) struct Delta {
    pub added: Vec<Rows>,
    pub removed: Vec<Rows>,
    pub modified: Vec<Modified>,
}

impl Delta {
    pub fn is_empty(&self) -> bool {
        self.added.iter().all(|r| r.keys.is_empty())
            && self.removed.iter().all(|r| r.keys.is_empty())
            && self.modified.iter().all(|m| m.keys.is_empty())
    }
}

fn no_row(key: RowKey) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("no row {key}"))
}

/// Where a dense table keeps `key`: its chunk and row within it.
fn dense_location(
    starts: &[RowKey],
    len: RowKey,
    key: RowKey,
) -> Result<(usize, usize), ArrowError> {
    if key >= len {
        return Err(no_row(key));
    }
    let chunk = starts.partition_point(|start| *start <= key) - 1;
    Ok((chunk, (key - starts[chunk]) as usize))
}

/// The rows `indices` picks from `batches` (`(batch, row)` each), in that order, as
/// `interleave_record_batch`; a table without columns keeps its row count (`count(*)` reads no
/// column, and an Arrow batch without columns needs its count said).
pub(crate) fn interleave_rows(
    batches: &[&RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, ArrowError> {
    match batches.first() {
        Some(first) if first.num_columns() == 0 => {
            let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));
            RecordBatch::try_new_with_options(first.schema(), Vec::new(), &options)
        }
        _ => interleave_record_batch(batches, indices),
    }
}

/// Combines the dense chunks from `from` on into one.
fn combine(
    chunks: &mut Vec<RecordBatch>,
    starts: &mut Vec<RowKey>,
    from: usize,
) -> Result<(), ArrowError> {
    if chunks.len() - from < 2 {
        return Ok(());
    }
    let combined = concat_batches(&chunks[from].schema(), &chunks[from..])?;
    chunks.truncate(from);
    chunks.push(combined);
    starts.truncate(from + 1);
    Ok(())
}

impl Storage {
    /// For tables that only ever append, keyed by position.
    pub fn dense(schema: SchemaRef) -> Self {
        Self {
            schema,
            index: Index::Dense {
                chunks: Vec::new(),
                starts: Vec::new(),
                len: 0,
                small_from: 0,
            },
        }
    }

    pub fn sparse(schema: SchemaRef) -> Self {
        Self {
            index: Index::Sparse {
                slot_of: HashMap::with_hasher(RandomState::new()),
                columns: Slots::new(&schema),
                free: Vec::new(),
            },
            schema,
        }
    }

    pub fn len(&self) -> usize {
        match &self.index {
            Index::Dense { len, .. } => *len as usize,
            Index::Sparse { slot_of, .. } => slot_of.len(),
        }
    }

    /// The key the next appended row gets in a dense table.
    pub fn next_key(&self) -> RowKey {
        self.len() as RowKey
    }

    pub fn contains(&self, key: RowKey) -> bool {
        match &self.index {
            Index::Dense { len, .. } => key < *len,
            Index::Sparse { slot_of, .. } => slot_of.contains_key(&key),
        }
    }

    /// Current values of `keys`, in that order.
    pub fn gather(&self, keys: &[RowKey]) -> Result<RecordBatch, ArrowError> {
        if keys.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        match &self.index {
            Index::Dense {
                chunks,
                starts,
                len,
                ..
            } => {
                let locations = keys
                    .iter()
                    .map(|&key| dense_location(starts, *len, key))
                    .collect::<Result<Vec<_>, _>>()?;
                let (touched, indices) = touched(&locations);
                let chunks: Vec<&RecordBatch> = touched.iter().map(|at| &chunks[*at]).collect();
                interleave_rows(&chunks, &indices)
            }
            Index::Sparse {
                slot_of, columns, ..
            } => {
                let slots = keys
                    .iter()
                    .map(|key| slot_of.get(key).copied().ok_or_else(|| no_row(*key)))
                    .collect::<Result<Vec<_>, _>>()?;
                let all: Vec<usize> = (0..columns.width()).collect();
                let options = RecordBatchOptions::new().with_row_count(Some(slots.len()));
                RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    columns.read(&all, &slots)?,
                    &options,
                )
            }
        }
    }

    /// The current values of `keys` in each of `columns`, one array per column, NULL where a key
    /// is `None`. Each key is located once, and only the requested columns are built.
    pub fn columns_at(
        &self,
        columns: &[usize],
        keys: &[Option<RowKey>],
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        match &self.index {
            Index::Dense {
                chunks,
                starts,
                len,
                ..
            } => {
                // Nothing to interleave from, and nothing to give
                if keys.is_empty() {
                    return Ok(columns
                        .iter()
                        .map(|c| new_empty_array(self.schema.field(*c).data_type()))
                        .collect());
                }
                // Missing keys point at a one-row NULL array placed after the chunks.
                let locations = keys
                    .iter()
                    .map(|key| match key {
                        Some(key) => dense_location(starts, *len, *key),
                        None => Ok((chunks.len(), 0)),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let (touched, indices) = touched(&locations);
                columns
                    .iter()
                    .map(|column| {
                        let null = new_null_array(self.schema.field(*column).data_type(), 1);
                        let arrays: Vec<&dyn Array> = touched
                            .iter()
                            .map(|at| match chunks.get(*at) {
                                Some(chunk) => chunk.column(*column).as_ref(),
                                None => null.as_ref(),
                            })
                            .collect();
                        interleave(&arrays, &indices)
                    })
                    .collect()
            }
            Index::Sparse {
                slot_of,
                columns: values,
                ..
            } => {
                let slots = keys
                    .iter()
                    .map(|key| match key {
                        Some(key) => slot_of
                            .get(key)
                            .copied()
                            .map(Some)
                            .ok_or_else(|| no_row(*key)),
                        None => Ok(None),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                values.read(columns, &slots)
            }
        }
    }

    /// Live keys in order.
    pub fn keys(&self) -> Vec<RowKey> {
        match &self.index {
            Index::Dense { len, .. } => (0..*len).collect(),
            Index::Sparse { slot_of, .. } => {
                let mut keys: Vec<RowKey> = slot_of.keys().copied().collect();
                keys.sort_unstable();
                keys
            }
        }
    }

    /// The `n` keys nearest `end` among those beyond `bound` (greater than it for `End::First`,
    /// less for `End::Last`; every key without one), in key order. A sparse table selects them
    /// in linear time rather than sorting all of its keys.
    pub fn edge_keys(&self, end: End, n: usize, bound: Option<RowKey>) -> Vec<RowKey> {
        match &self.index {
            Index::Dense { len, .. } => {
                let (len, n) = (*len, n as RowKey);
                let range = match end {
                    End::First => {
                        let start = bound.map_or(0, |b| b.saturating_add(1)).min(len);
                        start..start.saturating_add(n).min(len)
                    }
                    End::Last => {
                        let stop = bound.map_or(len, |b| b.min(len));
                        stop.saturating_sub(n)..stop
                    }
                };
                range.collect()
            }
            Index::Sparse { slot_of, .. } => {
                let mut keys: Vec<RowKey> = slot_of
                    .keys()
                    .copied()
                    .filter(|key| match (end, bound) {
                        (_, None) => true,
                        (End::First, Some(bound)) => *key > bound,
                        (End::Last, Some(bound)) => *key < bound,
                    })
                    .collect();
                nearest(&mut keys, end, n);
                keys.sort_unstable();
                keys
            }
        }
    }

    /// The keys at positions `start..start + count` in key order (fewer past the end). A sparse
    /// table selects them in linear time and sorts only the window.
    pub fn keys_at(&self, start: usize, count: usize) -> Vec<RowKey> {
        match &self.index {
            Index::Dense { len, .. } => {
                let end = start.saturating_add(count) as RowKey;
                (start as RowKey).min(*len)..end.min(*len)
            }
            .collect(),
            Index::Sparse { slot_of, .. } => {
                let end = start.saturating_add(count).min(slot_of.len());
                if start >= end {
                    return Vec::new();
                }
                let mut keys: Vec<RowKey> = slot_of.keys().copied().collect();
                if end < keys.len() {
                    keys.select_nth_unstable(end);
                    keys.truncate(end);
                }
                if start > 0 {
                    keys.select_nth_unstable(start);
                    keys.drain(..start);
                }
                keys.sort_unstable();
                keys
            }
        }
    }

    /// Current rows in key order.
    pub fn snapshot(&self) -> Result<Vec<RecordBatch>, ArrowError> {
        match &self.index {
            Index::Dense { chunks, .. } => Ok(chunks.clone()),
            Index::Sparse { slot_of, .. } if slot_of.is_empty() => Ok(Vec::new()),
            Index::Sparse { .. } => Ok(vec![self.gather(&self.keys())?]),
        }
    }

    /// Every current row as an addition: how a newly built table catches up with its input.
    pub fn as_added(&self) -> Result<Delta, ArrowError> {
        let added = match &self.index {
            Index::Dense { chunks, starts, .. } => chunks
                .iter()
                .zip(starts)
                .map(|(batch, start)| Rows {
                    keys: (*start..*start + batch.num_rows() as RowKey).collect(),
                    batch: batch.clone(),
                })
                .collect(),
            Index::Sparse { slot_of, .. } if slot_of.is_empty() => Vec::new(),
            Index::Sparse { .. } => {
                let keys = self.keys();
                let batch = self.gather(&keys)?;
                vec![Rows { keys, batch }]
            }
        };
        Ok(Delta {
            added,
            ..Delta::default()
        })
    }

    /// Applies a cycle's changes and returns them as stored. An append-only table hands its
    /// added rows back as slices of its chunks: the batches it was given are freed as they are
    /// combined, rather than kept alive by the delta beside their copies.
    pub fn apply(&mut self, delta: Delta) -> Result<Delta, ArrowError> {
        match &mut self.index {
            Index::Dense {
                chunks,
                starts,
                len,
                small_from,
            } => {
                if delta.removed.iter().any(|r| !r.keys.is_empty())
                    || delta.modified.iter().any(|m| !m.keys.is_empty())
                {
                    return Err(ArrowError::InvalidArgumentError(
                        "an append-only table received removes or modifies".into(),
                    ));
                }
                let first = *len;
                for rows in delta.added.into_iter().filter(|r| !r.keys.is_empty()) {
                    if rows.keys.first() != Some(len) {
                        return Err(ArrowError::InvalidArgumentError(
                            "an append-only table received out-of-order keys".into(),
                        ));
                    }
                    let count = rows.keys.len();
                    let small = count < SMALL_ROWS;
                    if !small {
                        // A run left short would otherwise stay small between large chunks.
                        combine(chunks, starts, *small_from)?;
                    }
                    starts.push(*len);
                    *len += count as RowKey;
                    chunks.push(rows.batch);
                    if !small {
                        *small_from = chunks.len();
                    } else if (*len - starts[*small_from]) as usize >= CHUNK_ROWS {
                        combine(chunks, starts, *small_from)?;
                        *small_from = chunks.len();
                    }
                }
                Ok(Delta {
                    added: stored_rows(chunks, starts, first),
                    ..Delta::default()
                })
            }
            Index::Sparse {
                slot_of,
                columns,
                free,
            } => {
                for key in delta.removed.iter().flat_map(|r| &r.keys) {
                    let slot = slot_of.remove(key).ok_or_else(|| no_row(*key))?;
                    columns.clear(slot);
                    free.push(slot);
                }
                for modified in &delta.modified {
                    let slots = modified
                        .keys
                        .iter()
                        .map(|key| slot_of.get(key).copied().ok_or_else(|| no_row(*key)))
                        .collect::<Result<Vec<_>, _>>()?;
                    columns.write(&modified.current, &slots);
                }
                for rows in &delta.added {
                    let mut slots = Vec::with_capacity(rows.keys.len());
                    let mut len = columns.len();
                    for key in &rows.keys {
                        let slot = match free.pop() {
                            Some(slot) => slot,
                            None => {
                                len += 1;
                                u32::try_from(len - 1).map_err(|_| {
                                    ArrowError::InvalidArgumentError(
                                        "over 2^32 rows in one table".into(),
                                    )
                                })?
                            }
                        };
                        if slot_of.insert(*key, slot).is_some() {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "row {key} added twice"
                            )));
                        }
                        slots.push(slot);
                    }
                    if len > columns.len() {
                        columns.resize(len);
                    }
                    columns.write(&rows.batch, &slots);
                }
                columns.compact();
                Ok(delta)
            }
        }
    }
}

/// A dense table's rows from key `from` on, as slices of its chunks.
fn stored_rows(chunks: &[RecordBatch], starts: &[RowKey], from: RowKey) -> Vec<Rows> {
    let first = starts
        .partition_point(|start| *start <= from)
        .saturating_sub(1);
    chunks
        .iter()
        .zip(starts)
        .skip(first)
        .filter_map(|(chunk, start)| {
            let skip = from.saturating_sub(*start);
            let count = chunk.num_rows() as RowKey - skip.min(chunk.num_rows() as RowKey);
            (count > 0).then(|| Rows {
                keys: (*start + skip..*start + skip + count).collect(),
                batch: chunk.slice(skip as usize, count as usize),
            })
        })
        .collect()
}

/// Either end of a table's key order.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum End {
    First,
    Last,
}

/// Keeps only the `n` of `keys` nearest `end`, in no particular order, in linear time.
pub(crate) fn nearest(keys: &mut Vec<RowKey>, end: End, n: usize) {
    if n == 0 {
        keys.clear();
    } else if keys.len() > n {
        match end {
            End::First => {
                keys.select_nth_unstable(n);
                keys.truncate(n);
            }
            End::Last => {
                let cut = keys.len() - n;
                keys.select_nth_unstable(cut);
                keys.drain(..cut);
            }
        }
    }
}

/// The chunks `locations` (chunk, row) touch, in order, and the locations renumbered into them.
/// Interleaving costs what it is given: given every chunk, a few rows read from a table appended
/// to in small batches all day (hundreds of thousands of chunks) cost the whole day's chunk list.
fn touched(locations: &[(usize, usize)]) -> (Vec<usize>, Vec<(usize, usize)>) {
    let mut touched: Vec<usize> = locations.iter().map(|(chunk, _)| *chunk).collect();
    touched.sort_unstable();
    touched.dedup();
    let renumbered = locations
        .iter()
        .map(|(chunk, row)| (touched.partition_point(|at| at < chunk), *row))
        .collect();
    (touched, renumbered)
}
