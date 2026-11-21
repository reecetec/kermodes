//! A keyed table's state (architecture.md §7.3): every key's latest row, folded from a checkpoint
//! and the upserts and deletes after it, in the order they arrived.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow::compute::interleave;
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, SortField};

use crate::{Change, Error};

/// Batches a fold keeps rows of before it compacts them into one: a day of changes far larger
/// than the state costs about this much memory, not the day's size.
const COMPACT_BYTES: usize = 64 << 20;

/// A keyed table's state folded from its changes: each key's latest row.
pub struct Fold {
    schema: SchemaRef,
    key: Vec<usize>,
    rows: RowConverter,
    /// Batches holding the latest rows, and where each key's latest row is in them.
    batches: Vec<RecordBatch>,
    latest: HashMap<OwnedRow, (usize, usize)>,
    retained: usize,
    compact_at: usize,
}

impl Fold {
    pub fn new(schema: &SchemaRef, key: &[String]) -> Result<Self, Error> {
        Self::compacting_at(schema, key, COMPACT_BYTES)
    }

    fn compacting_at(schema: &SchemaRef, key: &[String], compact_at: usize) -> Result<Self, Error> {
        let key = key
            .iter()
            .map(|column| {
                schema
                    .index_of(column)
                    .map_err(|_| Error::Corrupt(format!("key column `{column}` is not a column")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields = key
            .iter()
            .map(|&i| SortField::new(schema.field(i).data_type().clone()))
            .collect();
        Ok(Self {
            schema: schema.clone(),
            key,
            rows: RowConverter::new(fields).map_err(arrow_error)?,
            batches: Vec::new(),
            latest: HashMap::new(),
            retained: 0,
            compact_at,
        })
    }

    /// Applies one batch: upserts set each of its keys' latest row, deletes remove their keys,
    /// and a checkpoint is the whole state from here (rows after it in upserts), replacing every
    /// key before it.
    pub fn apply(&mut self, batch: RecordBatch, change: Change) -> Result<(), Error> {
        let keys = self.keys(&batch)?;
        if change == Change::Checkpoint {
            self.batches.clear();
            self.latest.clear();
            self.retained = 0;
        }
        match change {
            Change::Checkpoint | Change::Upsert => {
                let at = self.batches.len();
                for (row, key) in keys.iter().enumerate() {
                    self.latest.insert(key.owned(), (at, row));
                }
                self.retained += batch.get_array_memory_size();
                self.batches.push(batch);
            }
            Change::Delete => {
                for key in keys.iter() {
                    self.latest.remove(&key.owned());
                }
            }
            Change::Rows => {
                return Err(Error::Corrupt(
                    "a keyed table's log holds an append table's rows".into(),
                ));
            }
        }
        // One batch is as compact as the state gets.
        if self.retained > self.compact_at && self.batches.len() > 1 {
            let state = self.state()?;
            self.apply(state, Change::Checkpoint)?;
        }
        Ok(())
    }

    /// Every key's latest row, in key order.
    pub fn state(&self) -> Result<RecordBatch, Error> {
        let mut latest: Vec<(&OwnedRow, &(usize, usize))> = self.latest.iter().collect();
        latest.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let indices: Vec<(usize, usize)> = latest.iter().map(|(_, at)| **at).collect();
        let columns = (0..self.schema.fields().len())
            .map(|column| {
                if indices.is_empty() {
                    let field = self.schema.field(column);
                    return Ok(new_null_array(field.data_type(), 0));
                }
                let arrays: Vec<&dyn Array> = self
                    .batches
                    .iter()
                    .map(|batch| batch.column(column).as_ref())
                    .collect();
                interleave(&arrays, &indices).map_err(arrow_error)
            })
            .collect::<Result<Vec<ArrayRef>, Error>>()?;
        RecordBatch::try_new(self.schema.clone(), columns).map_err(arrow_error)
    }

    fn keys(&self, batch: &RecordBatch) -> Result<arrow::row::Rows, Error> {
        let columns: Vec<ArrayRef> = self
            .key
            .iter()
            .map(|&i| Arc::clone(batch.column(i)))
            .collect();
        self.rows.convert_columns(&columns).map_err(arrow_error)
    }
}

fn arrow_error(error: arrow::error::ArrowError) -> Error {
    Error::Arrow(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef};

    use super::Fold;
    use crate::Change;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("sym", DataType::Utf8, true),
            Field::new("px", DataType::Float64, true),
        ]))
    }

    fn rows(rows: &[(&str, f64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.1))),
            ],
        )
        .unwrap()
    }

    fn state(fold: &Fold) -> Vec<(String, f64)> {
        let state = fold.state().unwrap();
        let syms = state.column(0).as_string::<i32>();
        let px = state.column(1).as_primitive::<Float64Type>();
        (0..state.num_rows())
            .map(|i| (syms.value(i).to_string(), px.value(i)))
            .collect()
    }

    /// Every key's latest value after a checkpoint and the changes after it, however often the
    /// fold compacts (every batch here, at 1 byte). A key from before the checkpoint is gone.
    #[test]
    fn a_fold_keeps_each_keys_latest_row() {
        for compact_at in [usize::MAX, 1] {
            let mut fold = Fold::compacting_at(&schema(), &["sym".into()], compact_at).unwrap();
            let changes = [
                (Change::Upsert, rows(&[("z", 9.0), ("a", 9.0)])),
                (
                    Change::Checkpoint,
                    rows(&[("a", 1.0), ("b", 2.0), ("c", 3.0)]),
                ),
                (Change::Upsert, rows(&[("b", 20.0), ("d", 4.0)])),
                (Change::Delete, rows(&[("a", 0.0), ("x", 0.0)])),
                (Change::Upsert, rows(&[("a", 10.0), ("b", 200.0)])),
                (Change::Delete, rows(&[("d", 0.0)])),
            ];
            for (change, batch) in changes {
                fold.apply(batch, change).unwrap();
            }
            assert_eq!(
                state(&fold),
                [("a".into(), 10.0), ("b".into(), 200.0), ("c".into(), 3.0)],
                "compacting at {compact_at}"
            );
        }
        let empty = Fold::new(&schema(), &["sym".into()]).unwrap();
        assert_eq!(empty.state().unwrap().num_rows(), 0);
    }
}
