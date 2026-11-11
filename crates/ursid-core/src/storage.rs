//! Table storage and the per-cycle changes that flow between tables.
//!
//! Row versions are appended to immutable chunks; an index maps each live row key to the chunk
//! and row holding its current version. A modified row gets a new version and its key is
//! re-pointed; the old version stays readable, so a cycle's previous values cost nothing extra.

use std::collections::BTreeMap;

use arrow::compute::interleave_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

/// A row's identity within one table. Keys are never reused.
pub(crate) type RowKey = u64;

#[derive(Debug, Clone, Copy)]
struct Location {
    chunk: usize,
    row: usize,
}

enum Index {
    /// Keys `0..len`, in chunk order: append-only tables.
    Dense {
        starts: Vec<RowKey>,
        len: RowKey,
    },
    Sparse(BTreeMap<RowKey, Location>),
}

pub(crate) struct Storage {
    schema: SchemaRef,
    chunks: Vec<RecordBatch>,
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

impl Storage {
    /// For tables that only ever append, keyed by position.
    pub fn dense(schema: SchemaRef) -> Self {
        Self {
            schema,
            chunks: Vec::new(),
            index: Index::Dense {
                starts: Vec::new(),
                len: 0,
            },
        }
    }

    pub fn sparse(schema: SchemaRef) -> Self {
        Self {
            schema,
            chunks: Vec::new(),
            index: Index::Sparse(BTreeMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        match &self.index {
            Index::Dense { len, .. } => *len as usize,
            Index::Sparse(map) => map.len(),
        }
    }

    /// The key the next appended row gets in a dense table.
    pub fn next_key(&self) -> RowKey {
        self.len() as RowKey
    }

    pub fn contains(&self, key: RowKey) -> bool {
        self.location(key).is_some()
    }

    fn location(&self, key: RowKey) -> Option<Location> {
        match &self.index {
            Index::Dense { starts, len } => (key < *len).then(|| {
                let chunk = starts.partition_point(|start| *start <= key) - 1;
                Location {
                    chunk,
                    row: (key - starts[chunk]) as usize,
                }
            }),
            Index::Sparse(map) => map.get(&key).copied(),
        }
    }

    /// Current values of `keys`, in that order.
    pub fn gather(&self, keys: &[RowKey]) -> Result<RecordBatch, ArrowError> {
        if keys.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let indices = keys
            .iter()
            .map(|key| {
                self.location(*key)
                    .map(|l| (l.chunk, l.row))
                    .ok_or_else(|| ArrowError::InvalidArgumentError(format!("no row {key}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let chunks: Vec<&RecordBatch> = self.chunks.iter().collect();
        interleave_record_batch(&chunks, &indices)
    }

    /// Live keys in order.
    pub fn keys(&self) -> Vec<RowKey> {
        match &self.index {
            Index::Dense { len, .. } => (0..*len).collect(),
            Index::Sparse(map) => map.keys().copied().collect(),
        }
    }

    /// Current rows in key order.
    pub fn snapshot(&self) -> Result<Vec<RecordBatch>, ArrowError> {
        match &self.index {
            Index::Dense { .. } => Ok(self.chunks.clone()),
            Index::Sparse(map) if map.is_empty() => Ok(Vec::new()),
            Index::Sparse(_) => Ok(vec![self.gather(&self.keys())?]),
        }
    }

    /// Every current row as an addition: how a newly built table catches up with its input.
    pub fn as_added(&self) -> Result<Delta, ArrowError> {
        let added = match &self.index {
            Index::Dense { starts, .. } => self
                .chunks
                .iter()
                .zip(starts)
                .map(|(batch, start)| Rows {
                    keys: (*start..*start + batch.num_rows() as RowKey).collect(),
                    batch: batch.clone(),
                })
                .collect(),
            Index::Sparse(map) if map.is_empty() => Vec::new(),
            Index::Sparse(_) => {
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
            Index::Dense { starts, len } => {
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
                    self.chunks.push(rows.batch.clone());
                }
            }
            Index::Sparse(map) => {
                for rows in &delta.removed {
                    for key in &rows.keys {
                        map.remove(key);
                    }
                }
                let versions = delta
                    .modified
                    .iter()
                    .map(|m| (&m.keys, &m.current))
                    .chain(delta.added.iter().map(|r| (&r.keys, &r.batch)));
                for (keys, batch) in versions {
                    if keys.is_empty() {
                        continue;
                    }
                    let chunk = self.chunks.len();
                    self.chunks.push(batch.clone());
                    for (row, key) in keys.iter().enumerate() {
                        map.insert(*key, Location { chunk, row });
                    }
                }
            }
        }
        Ok(())
    }
}
