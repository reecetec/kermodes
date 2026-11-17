//! `Session.publish`: a table's changes, written to a keyed stored table as they happen
//! (architecture.md §5.2).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::kernels::boolean::or;
use arrow::compute::kernels::cmp::distinct;
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use kermodes_core::{Changes, Table};
use kermodes_weir::Change;
use pyo3::prelude::*;

use crate::SourceWriter;

/// Cycles the writer may fall behind by before the publication stops, saying so, rather than
/// hold more of the engine's memory or slow its cycles to weir's pace. A restart catches up
/// exactly: a publication begins by replacing the table.
const BEHIND: usize = 1024;
/// The most one write carries, well under weir's limit for one batch.
const WRITE_BYTES: usize = 16 << 20;
/// How often an idle writer checks whether it was stopped.
const IDLE: Duration = Duration::from_millis(200);

/// A table being published (`Session.publish`); stops when this object goes, or with `stop()`.
#[pyclass(module = "kermodes", name = "Publication", frozen)]
pub struct Publication {
    state: Arc<State>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct State {
    stopped: AtomicBool,
    error: Mutex<Option<String>>,
}

impl State {
    fn fail(&self, error: String) {
        self.error
            .lock()
            .expect("publication poisoned")
            .get_or_insert(error);
        self.stopped.store(true, Ordering::Release);
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

#[pymethods]
impl Publication {
    /// Why the publication stopped on its own: the table failed, weir refused a write, or the
    /// writes fell too far behind the table. `None` while it runs.
    #[getter]
    fn error(&self) -> Option<String> {
        self.state
            .error
            .lock()
            .expect("publication poisoned")
            .clone()
    }

    /// Stops publishing once the write in progress, if any, is durable.
    fn stop(&self, py: Python<'_>) {
        self.state.stopped.store(true, Ordering::Release);
        let thread = self.thread.lock().expect("publication poisoned").take();
        if let Some(thread) = thread {
            let _ = py.detach(|| thread.join());
        }
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::Release);
    }
}

/// Publishes `table` through `writer` to a keyed stored table keyed by `key`: its current rows
/// replace the stored table's, then each cycle's changes follow as deletes and upserts.
pub(crate) fn publish(
    table: &Table,
    writer: SourceWriter,
    key: &[String],
) -> Result<Publication, kermodes_core::Error> {
    let schema = table.schema().clone();
    let key = key
        .iter()
        .map(|column| {
            schema.index_of(column).map_err(|_| {
                kermodes_core::Error::Schema(format!("the key column `{column}` is not a column"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let state = Arc::new(State::default());
    let (changed, changes) = mpsc::sync_channel::<Changes>(BEHIND);
    let listening = state.clone();
    table.on_changes(Box::new(move |told| {
        if listening.stopped() {
            return false;
        }
        let error = match told {
            Ok(changes) => match changed.try_send(changes) {
                Ok(()) => return true,
                Err(TrySendError::Disconnected(_)) => return false,
                Err(TrySendError::Full(_)) => {
                    format!("fell {BEHIND} cycles behind the table writing to weir")
                }
            },
            Err(failed) => failed.to_string(),
        };
        listening.fail(error);
        false
    }))?;
    let writing = state.clone();
    let thread = std::thread::Builder::new()
        .name("kermodes-publish".into())
        .spawn(move || {
            if let Err(e) = write(&changes, &writer, &schema, &key, &writing) {
                writing.fail(e);
            }
        })
        .map_err(|e| kermodes_core::Error::Unsupported(format!("cannot start publishing: {e}")))?;
    Ok(Publication {
        state,
        thread: Mutex::new(Some(thread)),
    })
}

/// Writes each cycle's changes as they come, until stopped or the table goes.
fn write(
    changes: &Receiver<Changes>,
    writer: &SourceWriter,
    schema: &SchemaRef,
    key: &[usize],
    state: &State,
) -> Result<(), String> {
    let mut first = true;
    loop {
        let told = match changes.recv_timeout(IDLE) {
            Ok(told) => told,
            Err(RecvTimeoutError::Timeout) if !state.stopped() => continue,
            Err(_) => return Ok(()),
        };
        if state.stopped() {
            return Ok(());
        }
        let writes = if first {
            first = false;
            opening(&told, schema).map_err(|e| e.to_string())?
        } else {
            writes(&told, schema, key).map_err(|e| e.to_string())?
        };
        // A cycle's writes go out together and share the store's fsyncs.
        for (batch, change) in writes {
            writer
                .send(change, &batch, None)
                .map_err(|e| e.to_string())?;
        }
        writer.flush().map_err(|e| e.to_string())?;
    }
}

/// The table's rows when publishing starts, replacing the stored table's: keys a publisher no
/// longer holds do not outlive its restart. One write replaces, the rest (a large table) upsert.
fn opening(
    current: &Changes,
    schema: &SchemaRef,
) -> Result<Vec<(RecordBatch, Change)>, ArrowError> {
    let mut rows = chunks(schema, &current.added)?.into_iter();
    let replacing = rows
        .next()
        .unwrap_or_else(|| RecordBatch::new_empty(schema.clone()));
    Ok(std::iter::once((replacing, Change::Checkpoint))
        .chain(rows.map(|batch| (batch, Change::Upsert)))
        .collect())
}

/// A cycle's changes as weir writes: deletes of the keys gone (removed rows, and the old key of
/// a modified row whose key changed), then upserts of the rows added or modified. Deletes go
/// first, so a key that moved from one row to another in the cycle ends up present.
fn writes(
    changes: &Changes,
    schema: &SchemaRef,
    key: &[usize],
) -> Result<Vec<(RecordBatch, Change)>, ArrowError> {
    let mut gone = changes.removed.clone();
    for (before, after) in &changes.modified {
        let mut moved = BooleanArray::from(vec![false; before.num_rows()]);
        for &column in key {
            moved = or(
                &moved,
                &distinct(before.column(column), after.column(column))?,
            )?;
        }
        if moved.true_count() > 0 {
            gone.push(filter_record_batch(before, &moved)?);
        }
    }
    let set = changes
        .added
        .iter()
        .chain(changes.modified.iter().map(|(_, after)| after));
    let set: Vec<RecordBatch> = set.cloned().collect();
    let deletes = chunks(schema, &gone)?
        .into_iter()
        .map(|b| (b, Change::Delete));
    let upserts = chunks(schema, &set)?
        .into_iter()
        .map(|b| (b, Change::Upsert));
    Ok(deletes.chain(upserts).collect())
}

/// `batches` as writes of at most `WRITE_BYTES` (about), without empty ones.
fn chunks(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, ArrowError> {
    let all = concat_batches(schema, batches)?;
    let rows = all.num_rows();
    if rows == 0 {
        return Ok(Vec::new());
    }
    let parts = all.get_array_memory_size().div_ceil(WRITE_BYTES).max(1);
    let per = rows.div_ceil(parts);
    Ok((0..rows)
        .step_by(per)
        .map(|start| all.slice(start, per.min(rows - start)))
        .collect())
}
