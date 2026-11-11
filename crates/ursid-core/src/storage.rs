//! Table storage and the per-cycle changes that flow between tables.
//!
//! Append-only tables keep the batches they were given as immutable chunks, keyed by position.
//! Every other table gives each live row a slot in mutable columns (`slots`): a new version
//! overwrites the row's slot and a removed row's slot is reused, so memory follows the live rows.
//! Deltas carry previous values as their own copies, taken before a cycle is applied.

use ahash::RandomState;
use arrow::compute::interleave_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use hashbrown::HashMap;

use crate::slots::Slots;

/// A row's identity within one table. Keys are never reused.
pub(crate) type RowKey = u64;

enum Index {
    /// Keys `0..len`, in chunk order: append-only tables.
    Dense {
        chunks: Vec<RecordBatch>,
        starts: Vec<RowKey>,
        len: RowKey,
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
pub(crate) struct Rows {
    pub keys: Vec<RowKey>,
    pub batch: RecordBatch,
}

/// Modified rows: same keys, values before and after the cycle.
pub(crate) struct Modified {
    pub keys: Vec<RowKey>,
    pub previous: RecordBatch,
    pub current: RecordBatch,
}

/// What changed in a table during one cycle. `added`, `removed`, and `modified` keys are
/// disjoint; removed rows carry their last values.
#[derive(Default)]
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

impl Storage {
    /// For tables that only ever append, keyed by position.
    pub fn dense(schema: SchemaRef) -> Self {
        Self {
            schema,
            index: Index::Dense {
                chunks: Vec::new(),
                starts: Vec::new(),
                len: 0,
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
            } => {
                let indices = keys
                    .iter()
                    .map(|&key| {
                        if key >= *len {
                            return Err(no_row(key));
                        }
                        let chunk = starts.partition_point(|start| *start <= key) - 1;
                        Ok((chunk, (key - starts[chunk]) as usize))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let chunks: Vec<&RecordBatch> = chunks.iter().collect();
                interleave_record_batch(&chunks, &indices)
            }
            Index::Sparse {
                slot_of, columns, ..
            } => {
                let slots = keys
                    .iter()
                    .map(|key| slot_of.get(key).copied().ok_or_else(|| no_row(*key)))
                    .collect::<Result<Vec<_>, _>>()?;
                let options = RecordBatchOptions::new().with_row_count(Some(slots.len()));
                RecordBatch::try_new_with_options(
                    self.schema.clone(),
                    columns.read(&slots)?,
                    &options,
                )
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

    /// The first `n` rows in key order. A sparse table selects its `n` smallest keys in linear
    /// time rather than sorting all of them.
    pub fn head(&self, n: usize) -> Result<RecordBatch, ArrowError> {
        let keys = match &self.index {
            Index::Dense { len, .. } => (0..(n as RowKey).min(*len)).collect(),
            Index::Sparse { slot_of, .. } => {
                let mut keys: Vec<RowKey> = slot_of.keys().copied().collect();
                if keys.len() > n {
                    keys.select_nth_unstable(n);
                    keys.truncate(n);
                }
                keys.sort_unstable();
                keys
            }
        };
        self.gather(&keys)
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

    pub fn apply(&mut self, delta: &Delta) -> Result<(), ArrowError> {
        match &mut self.index {
            Index::Dense {
                chunks,
                starts,
                len,
            } => {
                if delta.removed.iter().any(|r| !r.keys.is_empty())
                    || delta.modified.iter().any(|m| !m.keys.is_empty())
                {
                    return Err(ArrowError::InvalidArgumentError(
                        "an append-only table received removes or modifies".into(),
                    ));
                }
                for rows in delta.added.iter().filter(|r| !r.keys.is_empty()) {
                    if rows.keys.first() != Some(len) {
                        return Err(ArrowError::InvalidArgumentError(
                            "an append-only table received out-of-order keys".into(),
                        ));
                    }
                    starts.push(*len);
                    *len += rows.keys.len() as RowKey;
                    chunks.push(rows.batch.clone());
                }
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
            }
        }
        Ok(())
    }
}
