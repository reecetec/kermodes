//! A session owns one update graph: input tables, the tables derived from them, and cycles.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, new_null_array};
use arrow::compute::{interleave, interleave_record_batch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::agg::{Accumulator, AggBy, Call, agg_schema, call_error};
use crate::groups::{KeyKind, Membership, resolve};
use crate::join::{AsOfJoin, Join, JoinShape, NaturalJoin, Tracking};
use crate::ops::{Assignment, Edge, LastBy, Operator, Update, Where};
use crate::rolling::{Aggregate, Function, Input, Rolling, Span, Window};
use crate::sort::sorted;
use crate::storage::{Delta, End, Modified, RowKey, Rows, Storage};
use crate::types::check_types;
use crate::{Error, Expression, type_name};

static IDLE_HOOK: OnceLock<fn()> = OnceLock::new();

/// Registers `hook` to run whenever the engine goes idle: after a table is built or released,
/// and when a running clock has had no writes for a second (`CLOCK_WAIT`). The Python extension
/// registers its allocator's collection, so memory that work just freed goes back to the OS,
/// while a steady stream of cycles keeps reusing it. The first registration wins.
pub fn set_idle_hook(hook: fn()) {
    let _ = IDLE_HOOK.set(hook);
}

fn idle() {
    if let Some(hook) = IDLE_HOOK.get() {
        hook();
    }
}

/// An engine instance: one update graph and the tables in it. Clones share the graph.
#[derive(Clone, Default)]
pub struct Session {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    graph: RwLock<Graph>,
    /// Writes waiting for the next cycle, in arrival order.
    pending: Mutex<Vec<(usize, Write)>>,
    /// Signalled when writes are queued or the clock is told to stop.
    arrived: Condvar,
    /// The background clock started by `Session::start`, if running.
    clock: Mutex<Option<Clock>>,
    /// Tables whose last handle was dropped while the graph was busy; released the next time
    /// the graph is written.
    unheld: Mutex<Vec<usize>>,
}

/// A thread running a cycle whenever writes are pending. It holds the session only while it
/// waits a bounded time or runs a cycle, so dropping the last `Session` handle also ends it.
struct Clock {
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}

/// How long the clock waits for writes before checking whether its session is gone.
const CLOCK_WAIT: Duration = Duration::from_secs(1);

enum Write {
    Append(RecordBatch),
    Upsert(RecordBatch),
    /// Key columns only.
    Delete(RecordBatch),
}

#[derive(Default)]
struct Graph {
    /// In creation order, which is topological: a table is built only from existing tables.
    /// Released tables stay as empty placeholders, so ids never move.
    tables: Vec<TableState>,
    cycle: u64,
}

impl Graph {
    /// Adds a table; each table it is computed from gains a dependent.
    fn push(&mut self, state: TableState) -> usize {
        for parent in state.source.parents() {
            self.tables[parent].dependents += 1;
        }
        self.tables.push(state);
        self.tables.len() - 1
    }

    /// Marks the tables in `unheld` as having no handle left, then releases every table with
    /// neither a handle nor a dependent: its rows, operator state, and delta are freed, and the
    /// tables it was computed from lose a dependent, so an unused chain goes all at once.
    fn release(&mut self, unheld: &Mutex<Vec<usize>>) {
        let mut ids = std::mem::take(
            &mut *unheld
                .lock()
                .expect("ursid release queue poisoned by an earlier panic"),
        );
        for id in &ids {
            self.tables[*id].held = false;
        }
        while let Some(id) = ids.pop() {
            let table = &mut self.tables[id];
            if table.held || table.dependents > 0 || table.released {
                continue;
            }
            let parents: Vec<usize> = table.source.parents().collect();
            table.release();
            for parent in parents {
                self.tables[parent].dependents -= 1;
                ids.push(parent);
            }
        }
    }
}

/// Keeps a table alive while any clone of its `Table` exists. When the last one drops, the
/// table is released at once if the graph is free, else the next time the graph is written (a
/// cycle in progress finishes first); tables built on it keep it until they go too.
struct Handle {
    session: Session,
    id: usize,
}

impl Drop for Handle {
    fn drop(&mut self) {
        let inner = &self.session.inner;
        inner
            .unheld
            .lock()
            .expect("ursid release queue poisoned by an earlier panic")
            .push(self.id);
        // Never wait here: the dropping thread may be a reader, or a cycle may be running.
        if let Ok(mut graph) = inner.graph.try_write() {
            graph.release(&inner.unheld);
        }
        idle();
    }
}

/// How a table's rows change, from least to most constrained; each step lets the operators built
/// on the table keep less state.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Growth {
    /// Rows are added, removed, and modified.
    Changing,
    /// Rows are only added (a merge's, under keys that are not positions).
    AddOnly,
    /// Rows are only added, each keyed by its position: row order is key order, cycle after
    /// cycle. Such tables keep their rows as the chunks they arrive in.
    AppendOnly,
}

struct TableState {
    description: String,
    schema: SchemaRef,
    /// The table's rows; `None` for a table that computes them on read (see `Reader`).
    storage: Option<Storage>,
    refreshing: bool,
    growth: Growth,
    failure: Option<Error>,
    /// This cycle's changes, read by the tables built from this one.
    delta: Option<Delta>,
    /// The cycle that last changed this table's rows or failed it; 0 if none has.
    changed: u64,
    source: Source,
    /// Whether a `Table` handle still refers to this table.
    held: bool,
    /// Live tables computed from this one (static ones have their own rows and don't count).
    dependents: usize,
    /// Released: an empty placeholder that no handle or table refers to.
    released: bool,
}

enum Source {
    Append,
    Keyed(Box<Keyed>),
    Static,
    Derived {
        parent: usize,
        operator: Box<Operator>,
    },
    Join {
        left: usize,
        right: usize,
        operator: Box<Join>,
    },
    /// Every row of `inputs`, read through them: a merge stores nothing.
    Merge {
        inputs: Vec<usize>,
    },
}

impl Source {
    /// The tables this one is computed from; none for inputs and static tables.
    fn parents(&self) -> impl Iterator<Item = usize> + '_ {
        let (first, second, rest): (_, _, &[usize]) = match self {
            Source::Derived { parent, .. } => (Some(*parent), None, &[]),
            Source::Join { left, right, .. } => (Some(*left), Some(*right), &[]),
            Source::Merge { inputs } => (None, None, inputs),
            Source::Append | Source::Keyed(_) | Source::Static => (None, None, &[]),
        };
        first.into_iter().chain(second).chain(rest.iter().copied())
    }
}

const STORED: &str = "only joins and merges compute their rows; other tables store theirs";

/// Bits of a merged row's key that hold its input's key; the input's index takes the top 8.
const MERGE_KEY_BITS: u32 = 56;

/// The merged key of input `index`'s row `key`.
fn merged_key(index: usize, key: RowKey) -> Result<RowKey, ArrowError> {
    if key >> MERGE_KEY_BITS != 0 {
        return Err(ArrowError::ComputeError(format!(
            "merge: input row key {key} needs more than {MERGE_KEY_BITS} bits"
        )));
    }
    Ok(((index as RowKey) << MERGE_KEY_BITS) | key)
}

fn merged_keys(index: usize, keys: &[RowKey]) -> Result<Vec<RowKey>, ArrowError> {
    keys.iter().map(|key| merged_key(index, *key)).collect()
}

/// Inputs' changes as a merged table's: the same rows (shared, not copied) under merged keys and
/// the merged schema. `deltas` pairs each changed input's index with its delta.
fn merged_delta<'a>(
    schema: &SchemaRef,
    deltas: impl Iterator<Item = (usize, &'a Delta)>,
) -> Result<Delta, ArrowError> {
    let mut out = Delta::default();
    for (index, delta) in deltas {
        for rows in &delta.added {
            out.added.push(Rows {
                keys: merged_keys(index, &rows.keys)?,
                batch: rows.batch.clone().with_schema(schema.clone())?,
            });
        }
        for rows in &delta.removed {
            out.removed.push(Rows {
                keys: merged_keys(index, &rows.keys)?,
                batch: rows.batch.clone().with_schema(schema.clone())?,
            });
        }
        for modified in &delta.modified {
            out.modified.push(Modified {
                keys: merged_keys(index, &modified.keys)?,
                previous: modified.previous.clone().with_schema(schema.clone())?,
                current: modified.current.clone().with_schema(schema.clone())?,
            });
        }
    }
    Ok(out)
}

/// A table's current rows, wherever they are. Most tables store theirs. Two kinds store none: a
/// join whose right side ticks computes a row on read from its left row, the join's current match
/// for it, and that right row; a merge reads each row from the input it came from. Reads resolve
/// through earlier tables, so `tables` holds every table up to the one read.
#[derive(Clone, Copy)]
pub(crate) struct Reader<'a> {
    tables: &'a [TableState],
    id: usize,
}

enum Place<'a> {
    Stored(&'a Storage),
    Joined {
        join: &'a Join,
        left: Reader<'a>,
        right: Reader<'a>,
    },
    Merged {
        inputs: &'a [usize],
        schema: &'a SchemaRef,
    },
}

impl<'a> Reader<'a> {
    fn new(tables: &'a [TableState], id: usize) -> Self {
        Self { tables, id }
    }

    fn place(self) -> Place<'a> {
        let table = &self.tables[self.id];
        match (&table.storage, &table.source) {
            (Some(storage), _) => Place::Stored(storage),
            (
                None,
                Source::Join {
                    left,
                    right,
                    operator,
                },
            ) => Place::Joined {
                join: operator,
                left: Self::new(self.tables, *left),
                right: Self::new(self.tables, *right),
            },
            (None, Source::Merge { inputs }) => Place::Merged {
                inputs,
                schema: &table.schema,
            },
            (None, _) => unreachable!("{STORED}"),
        }
    }

    fn input(self, id: usize) -> Self {
        Self::new(self.tables, id)
    }

    pub fn len(self) -> usize {
        match self.place() {
            Place::Stored(storage) => storage.len(),
            Place::Joined { left, .. } => left.len(),
            Place::Merged { inputs, .. } => inputs.iter().map(|id| self.input(*id).len()).sum(),
        }
    }

    /// Current values of `keys`, in that order.
    pub fn gather(self, keys: &[RowKey]) -> Result<RecordBatch, ArrowError> {
        match self.place() {
            Place::Stored(storage) => storage.gather(keys),
            Place::Joined { join, left, right } => join.rows(&left.gather(keys)?, right),
            Place::Merged { inputs, schema } => {
                // Each key's input and position among the keys asked of that input.
                let mut wanted = vec![Vec::new(); inputs.len()];
                let mut at = Vec::with_capacity(keys.len());
                for key in keys {
                    let index = (key >> MERGE_KEY_BITS) as usize;
                    let Some(input_keys) = wanted.get_mut(index) else {
                        return Err(ArrowError::InvalidArgumentError(format!("no row {key}")));
                    };
                    at.push((index, input_keys.len()));
                    input_keys.push(key & ((1 << MERGE_KEY_BITS) - 1));
                }
                let batches = inputs
                    .iter()
                    .zip(&wanted)
                    .map(|(id, keys)| self.input(*id).gather(keys)?.with_schema(schema.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                if let [(only, _), ..] = at[..]
                    && at.iter().all(|(index, _)| *index == only)
                {
                    return Ok(batches[only].clone());
                }
                let batches: Vec<&RecordBatch> = batches.iter().collect();
                interleave_record_batch(&batches, &at)
            }
        }
    }

    /// The current values of `keys` in each of `columns`, one array per column, NULL where a key
    /// is `None`.
    pub fn columns_at(
        self,
        columns: &[usize],
        keys: &[Option<RowKey>],
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        if let Place::Stored(storage) = self.place() {
            return storage.columns_at(columns, keys);
        }
        let present: Vec<RowKey> = keys.iter().flatten().copied().collect();
        let rows = self.gather(&present)?;
        if present.len() == keys.len() {
            return Ok(columns.iter().map(|c| rows.column(*c).clone()).collect());
        }
        // Missing keys point at a one-row NULL array placed after the gathered rows.
        let mut next = 0;
        let indices: Vec<(usize, usize)> = keys
            .iter()
            .map(|key| match key {
                Some(_) => {
                    next += 1;
                    (0, next - 1)
                }
                None => (1, 0),
            })
            .collect();
        columns
            .iter()
            .map(|c| {
                let values = rows.column(*c);
                let null = new_null_array(values.data_type(), 1);
                interleave(&[values.as_ref(), null.as_ref()], &indices)
            })
            .collect()
    }

    /// The first `n` rows in key order.
    pub fn first_rows(self, n: usize) -> Result<RecordBatch, ArrowError> {
        self.gather(&self.edge_keys(End::First, n, None)?)
    }

    /// As `Storage::edge_keys`.
    pub fn edge_keys(
        self,
        end: End,
        n: usize,
        bound: Option<RowKey>,
    ) -> Result<Vec<RowKey>, ArrowError> {
        match self.place() {
            Place::Stored(storage) => Ok(storage.edge_keys(end, n, bound)),
            Place::Joined { left, .. } => left.edge_keys(end, n, bound),
            // Merged keys order by input first: walk the inputs from `end`, starting at the
            // bound's input, until `n` keys are found.
            Place::Merged { inputs, .. } => {
                let bound = bound.map(|b| {
                    let index = (b >> MERGE_KEY_BITS) as usize;
                    (index, b & ((1 << MERGE_KEY_BITS) - 1))
                });
                let (mut chunks, mut found) = (Vec::new(), 0);
                for step in 0..inputs.len() {
                    if found == n {
                        break;
                    }
                    let index = match end {
                        End::First => step,
                        End::Last => inputs.len() - 1 - step,
                    };
                    let within = match bound {
                        None => None,
                        Some((at, key)) if at == index => Some(key),
                        // Inputs on the near side of the bound's input are skipped entirely.
                        Some((at, _)) if (end == End::First) == (index < at) => continue,
                        Some(_) => None,
                    };
                    let keys = self
                        .input(inputs[index])
                        .edge_keys(end, n - found, within)?;
                    found += keys.len();
                    chunks.push(merged_keys(index, &keys)?);
                }
                if end == End::Last {
                    chunks.reverse();
                }
                Ok(chunks.concat())
            }
        }
    }

    /// Current rows in key order.
    pub fn snapshot(self) -> Result<Vec<RecordBatch>, ArrowError> {
        match self.place() {
            Place::Stored(storage) => storage.snapshot(),
            Place::Joined { join, left, right } => left
                .snapshot()?
                .iter()
                .map(|batch| join.rows(batch, right))
                .collect(),
            Place::Merged { inputs, schema } => {
                let mut batches = Vec::new();
                for id in inputs {
                    for batch in self.input(*id).snapshot()? {
                        batches.push(batch.with_schema(schema.clone())?);
                    }
                }
                Ok(batches)
            }
        }
    }

    /// Every current row as an addition: how a newly built table catches up with its input.
    pub fn as_added(self) -> Result<Delta, ArrowError> {
        match self.place() {
            Place::Stored(storage) => storage.as_added(),
            Place::Joined { join, left, right } => {
                let mut delta = left.as_added()?;
                for rows in &mut delta.added {
                    rows.batch = join.rows(&rows.batch, right)?;
                }
                Ok(delta)
            }
            Place::Merged { inputs, schema } => {
                // The inputs' key vectors are this call's own: re-key them in place.
                let mut out = Delta::default();
                for (index, id) in inputs.iter().enumerate() {
                    for mut rows in self.input(*id).as_added()?.added {
                        for key in &mut rows.keys {
                            *key = merged_key(index, *key)?;
                        }
                        rows.batch = rows.batch.with_schema(schema.clone())?;
                        out.added.push(rows);
                    }
                }
                Ok(out)
            }
        }
    }
}

/// A join's resolved columns: its shape, the left `on` columns, and the `at` columns if any.
struct JoinPlan {
    shape: JoinShape,
    left_on: Vec<usize>,
    at: Option<(usize, usize)>,
}

/// `"left=right"` as `("left", "right")`; a bare `"name"` names both sides.
fn pair(text: &str) -> (&str, &str) {
    match text.split_once('=') {
        Some((left, right)) => (left.trim(), right.trim()),
        None => (text.trim(), text.trim()),
    }
}

/// An input table with one row per key value: upserts replace, deletes remove.
struct Keyed {
    /// The key within upsert batches, which hold full rows.
    rows: KeyKind,
    /// The key within delete batches, which hold only the key columns.
    deletes: KeyKind,
    key_schema: SchemaRef,
    /// Live keys: an id per key, and per id the row key of its row.
    live: Membership,
    /// Per id, the cycle's last write to its key: an upserted `(batch, row)`, or `None` for a
    /// delete. Cleared as the cycle's delta is built.
    latest: Vec<Option<(usize, usize)>>,
    /// Per id, whether its key first appeared this cycle. Cleared likewise.
    new: Vec<bool>,
}

/// A table in a session. Cheap to clone; clones refer to the same table. The table lives while
/// any clone does, or while a live table is computed from it.
#[derive(Clone)]
pub struct Table {
    session: Session,
    id: usize,
    schema: SchemaRef,
    _alive: Arc<Handle>,
}

impl Session {
    pub fn new() -> Self {
        Self::default()
    }

    /// A refreshing table that receives rows through `Table::append`. Every column is nullable.
    pub fn input_table(&self, schema: &Schema) -> Result<Table, Error> {
        let schema = nullable(schema)?;
        Ok(self.add(TableState::new(
            "input".into(),
            schema,
            true,
            Growth::AppendOnly,
            Source::Append,
        )))
    }

    /// A refreshing table with one row per value of `key`; rows arrive through `Table::upsert`
    /// and leave through `Table::delete`. Every column is nullable.
    pub fn keyed_input_table(&self, schema: &Schema, key: &[&str]) -> Result<Table, Error> {
        if key.is_empty() {
            return Err(Error::Schema(
                "a keyed table needs at least one key column".into(),
            ));
        }
        let schema = nullable(schema)?;
        let columns = resolve(&schema, key)?;
        let key_schema = Arc::new(schema.project(&columns).map_err(schema_error)?);
        let keyed = Keyed {
            rows: KeyKind::new(&schema, columns.clone()).map_err(schema_error)?,
            deletes: KeyKind::new(&key_schema, (0..key.len()).collect()).map_err(schema_error)?,
            key_schema,
            live: Membership::new(&schema, columns, true, 0, 1).map_err(schema_error)?,
            latest: Vec::new(),
            new: Vec::new(),
        };
        let description = format!("input keyed by {}", key.join(", "));
        Ok(self.add(TableState::new(
            description,
            schema,
            true,
            Growth::Changing,
            Source::Keyed(Box::new(keyed)),
        )))
    }

    /// A table that never changes. Tables built from it are computed once, immediately.
    pub fn static_table(
        &self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Table, Error> {
        check_types(&schema)?;
        let batches = conform_all(&schema, &batches)?;
        let state = TableState::fixed("static".into(), schema, batches).map_err(schema_error)?;
        Ok(self.add(state))
    }

    /// Runs one cycle: applies pending writes to input tables, then updates every refreshing
    /// derived table in creation order. Returns the cycle number.
    pub fn step(&self) -> u64 {
        let pending = std::mem::take(&mut *self.pending());
        let mut graph = self.write();
        let cycle = graph.cycle + 1;
        let mut writes: Vec<Vec<Write>> = graph.tables.iter().map(|_| Vec::new()).collect();
        for (id, write) in pending {
            writes[id].push(write);
        }
        for (index, writes) in writes.into_iter().enumerate() {
            let (before, rest) = graph.tables.split_at_mut(index);
            let table = &mut rest[0];
            table.delta = None;
            if table.failure.is_some() {
                continue;
            }
            if table.source.parents().next().is_some() {
                // A computed table updates only while refreshing, and only on its parents' changes.
                if !table.refreshing {
                    continue;
                }
                let failed = table
                    .source
                    .parents()
                    .find_map(|p| before[p].failure.clone());
                if let Some(cause) = failed {
                    table.fail(Error::Upstream {
                        table: table.description.clone(),
                        cause: Arc::new(cause),
                    });
                    table.changed = cycle;
                    continue;
                }
                if table.source.parents().all(|p| before[p].delta.is_none()) {
                    continue;
                }
            }
            let unchanged = Delta::default();
            let delta_of = |id: usize| before[id].delta.as_ref().unwrap_or(&unchanged);
            let result = match &mut table.source {
                Source::Static => continue,
                Source::Append => Ok(appended(table.storage.as_ref().expect(STORED), writes)),
                Source::Keyed(keyed) => {
                    keyed.resolve(table.storage.as_ref().expect(STORED), writes)
                }
                Source::Derived { parent, operator } => operator.process(
                    Reader::new(before, *parent),
                    delta_of(*parent),
                    table.storage.as_ref().expect(STORED),
                ),
                Source::Join {
                    left,
                    right,
                    operator,
                } => operator.process(
                    Reader::new(before, *left),
                    delta_of(*left),
                    Reader::new(before, *right),
                    delta_of(*right),
                ),
                Source::Merge { inputs } => merged_delta(
                    &table.schema,
                    inputs
                        .iter()
                        .enumerate()
                        .filter_map(|(index, id)| before[*id].delta.as_ref().map(|d| (index, d))),
                ),
            };
            table.commit(result);
            if table.delta.is_some() || table.failure.is_some() {
                table.changed = cycle;
            }
        }
        graph.cycle = cycle;
        cycle
    }

    /// Starts running cycles in the background: one as soon as writes arrive, folding writes that
    /// arrive during a cycle into the next. With `min_period`, cycles start at least that far
    /// apart. Fails if a clock is already running; `stop` ends it.
    pub fn start(&self, min_period: Option<Duration>) -> Result<(), Error> {
        let mut clock = self.clock();
        if clock.is_some() {
            return Err(Error::Unsupported(
                "the session's clock is already running".into(),
            ));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let session = Arc::downgrade(&self.inner);
        let thread = thread::Builder::new()
            .name("ursid-clock".into())
            .spawn(move || {
                let mut last: Option<Instant> = None;
                loop {
                    let Some(inner) = session.upgrade() else {
                        return;
                    };
                    let session = Session { inner };
                    let ready = {
                        let pending = session.pending();
                        let (pending, _) = session
                            .inner
                            .arrived
                            .wait_timeout_while(pending, CLOCK_WAIT, |pending| {
                                pending.is_empty() && !stopped.load(Ordering::Acquire)
                            })
                            .expect("ursid write queue poisoned by an earlier panic");
                        !pending.is_empty()
                    };
                    if stopped.load(Ordering::Acquire) {
                        return;
                    }
                    if !ready {
                        // No writes for a whole wait: the engine is idle.
                        idle();
                        continue;
                    }
                    if let (Some(min_period), Some(last)) = (min_period, last) {
                        thread::sleep(
                            (last + min_period).saturating_duration_since(Instant::now()),
                        );
                    }
                    last = Some(Instant::now());
                    session.step();
                }
            })
            .map_err(|e| Error::Unsupported(format!("cannot start the clock thread: {e}")))?;
        *clock = Some(Clock { stop, thread });
        Ok(())
    }

    /// Stops the background clock, after any cycle in progress. Does nothing if none runs.
    pub fn stop(&self) {
        let Some(clock) = self.clock().take() else {
            return;
        };
        clock.stop.store(true, Ordering::Release);
        // Taking the queue lock orders the flag before a waiter's next check of it.
        drop(self.pending());
        self.inner.arrived.notify_all();
        clock.thread.join().expect("ursid clock thread panicked");
    }

    pub fn is_running(&self) -> bool {
        self.clock().is_some()
    }

    fn clock(&self) -> MutexGuard<'_, Option<Clock>> {
        self.inner
            .clock
            .lock()
            .expect("ursid clock poisoned by an earlier panic")
    }

    fn add(&self, state: TableState) -> Table {
        let schema = state.schema.clone();
        let id = self.write().push(state);
        idle();
        Table::new(self, id, schema)
    }

    /// How many tables the session holds: those with a handle, and those live tables are
    /// computed from. Dropping a table's last handle releases it unless others need it.
    pub fn table_count(&self) -> usize {
        self.read().tables.iter().filter(|t| !t.released).count()
    }

    fn read(&self) -> RwLockReadGuard<'_, Graph> {
        self.inner
            .graph
            .read()
            .expect("ursid graph lock poisoned by an earlier panic")
    }

    /// The graph, for changes; tables whose last handle dropped meanwhile are released first.
    fn write(&self) -> RwLockWriteGuard<'_, Graph> {
        let mut graph = self
            .inner
            .graph
            .write()
            .expect("ursid graph lock poisoned by an earlier panic");
        graph.release(&self.inner.unheld);
        graph
    }

    fn pending(&self) -> MutexGuard<'_, Vec<(usize, Write)>> {
        self.inner
            .pending
            .lock()
            .expect("ursid write queue poisoned by an earlier panic")
    }
}

impl TableState {
    /// A static table holding `batches`, keyed by position.
    fn fixed(
        description: String,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Self, ArrowError> {
        let mut state = TableState::new(
            description,
            schema,
            false,
            Growth::AppendOnly,
            Source::Static,
        );
        let mut next = 0;
        let added = batches
            .into_iter()
            .map(|batch| {
                let count = batch.num_rows() as RowKey;
                next += count;
                Rows {
                    keys: (next - count..next).collect(),
                    batch,
                }
            })
            .collect();
        state.storage.as_mut().expect(STORED).apply(&Delta {
            added,
            ..Delta::default()
        })?;
        Ok(state)
    }

    fn new(
        description: String,
        schema: SchemaRef,
        refreshing: bool,
        growth: Growth,
        source: Source,
    ) -> Self {
        let storage = match &source {
            Source::Join { operator, .. } if operator.computes_rows() => None,
            Source::Merge { .. } => None,
            _ if growth == Growth::AppendOnly => Some(Storage::dense(schema.clone())),
            _ => Some(Storage::sparse(schema.clone())),
        };
        Self {
            description,
            schema,
            storage,
            refreshing,
            growth,
            failure: None,
            delta: None,
            changed: 0,
            source,
            held: true,
            dependents: 0,
            released: false,
        }
    }

    /// Frees everything but the description: no handle or live table refers to this one.
    fn release(&mut self) {
        self.released = true;
        self.storage = None;
        self.source = Source::Static;
        self.delta = None;
        self.failure = None;
    }

    /// Finishes building a derived table: its first rows are applied, so their delta goes. A
    /// table that never refreshes never runs its operator again, so the operator's state (join
    /// indexes, aggregation shards, group tables) goes too, leaving only the rows, and it no
    /// longer needs the tables it was computed from. (A join that computes its rows always
    /// refreshes: its right side ticks.)
    fn settle(&mut self) {
        self.delta = None;
        if !self.refreshing {
            self.source = Source::Static;
        }
    }

    fn fail(&mut self, error: Error) {
        self.failure = Some(error);
        self.delta = None;
    }

    /// Applies a cycle's computed changes, or keeps the error: a failed table stops updating
    /// and every table built from it fails too.
    fn commit(&mut self, result: Result<Delta, ArrowError>) {
        let applied = result.and_then(|delta| {
            if !delta.is_empty() {
                if let Some(storage) = &mut self.storage {
                    storage.apply(&delta)?;
                }
                self.delta = Some(delta);
            }
            Ok(())
        });
        if let Err(error) = applied {
            let message = error.to_string();
            self.fail(Error::Compute {
                table: self.description.clone(),
                message,
            });
        }
    }
}

fn appended(storage: &Storage, writes: Vec<Write>) -> Delta {
    let mut next = storage.next_key();
    let added = writes
        .into_iter()
        .filter_map(|write| match write {
            Write::Append(batch) => Some(batch),
            Write::Upsert(_) | Write::Delete(_) => None,
        })
        .map(|batch| {
            let count = batch.num_rows() as RowKey;
            next += count;
            Rows {
                keys: (next - count..next).collect(),
                batch,
            }
        })
        .collect();
    Delta {
        added,
        ..Delta::default()
    }
}

impl Keyed {
    /// Nets a cycle's upserts and deletes per key: the last write to a key wins, and a key
    /// written and deleted within the cycle never appears.
    fn resolve(&mut self, storage: &Storage, writes: Vec<Write>) -> Result<Delta, ArrowError> {
        let mut upserts: Vec<RecordBatch> = Vec::new();
        for write in writes {
            let (kind, batch, upserting) = match write {
                Write::Upsert(batch) => (&self.rows, batch, true),
                Write::Delete(batch) => (&self.deletes, batch, false),
                Write::Append(_) => continue,
            };
            let encoded = kind.encode(&batch)?;
            let keys = kind.keys(&batch, encoded.as_ref());
            let index = upserts.len();
            for row in 0..batch.num_rows() {
                let id = self.live.group(
                    keys.hash(row),
                    |key| keys.matches(row, key),
                    || keys.owned(row),
                ) as usize;
                if id >= self.latest.len() {
                    self.latest.resize(id + 1, None);
                    self.new.resize(id + 1, false);
                }
                self.latest[id] = upserting.then_some((index, row));
            }
            if upserting {
                upserts.push(batch);
            }
        }
        for id in self.live.take_created() {
            self.new[id as usize] = true;
        }

        let (mut added_keys, mut added_at) = (Vec::new(), Vec::new());
        let (mut modified_keys, mut modified_at) = (Vec::new(), Vec::new());
        let mut removed = Vec::new();
        for id in self.live.take_touched() {
            let index = id as usize;
            let slot = self.live.slot[index];
            match (
                std::mem::take(&mut self.new[index]),
                self.latest[index].take(),
            ) {
                (false, Some(at)) => {
                    modified_keys.push(slot);
                    modified_at.push(at);
                }
                (false, None) => {
                    removed.push(slot);
                    self.live.release(id);
                }
                (true, Some(at)) => {
                    added_keys.push(slot);
                    added_at.push(at);
                }
                (true, None) => self.live.release(id),
            }
        }

        let batches: Vec<&RecordBatch> = upserts.iter().collect();
        let mut delta = Delta::default();
        if !added_keys.is_empty() {
            delta.added.push(Rows {
                keys: added_keys,
                batch: interleave_record_batch(&batches, &added_at)?,
            });
        }
        if !modified_keys.is_empty() {
            delta.modified.push(Modified {
                previous: storage.gather(&modified_keys)?,
                current: interleave_record_batch(&batches, &modified_at)?,
                keys: modified_keys,
            });
        }
        if !removed.is_empty() {
            delta.removed.push(Rows {
                batch: storage.gather(&removed)?,
                keys: removed,
            });
        }
        Ok(delta)
    }
}

impl Table {
    fn new(session: &Session, id: usize, schema: SchemaRef) -> Self {
        Self {
            session: session.clone(),
            id,
            schema,
            _alive: Arc::new(Handle {
                session: session.clone(),
                id,
            }),
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn description(&self) -> String {
        self.session.read().tables[self.id].description.clone()
    }

    pub fn is_refreshing(&self) -> bool {
        self.session.read().tables[self.id].refreshing
    }

    /// Queues rows for the next cycle. Columns are matched by name; all batches are checked
    /// before any is queued.
    pub fn append(&self, batches: &[RecordBatch]) -> Result<(), Error> {
        self.expect_source("append", |s| matches!(s, Source::Append))?;
        let batches = conform_all(&self.schema, batches)?;
        self.queue(batches.into_iter().map(Write::Append));
        Ok(())
    }

    /// Queues rows that replace the current row with the same key, or add one.
    pub fn upsert(&self, batches: &[RecordBatch]) -> Result<(), Error> {
        self.expect_source("upsert", |s| matches!(s, Source::Keyed(_)))?;
        let batches = conform_all(&self.schema, batches)?;
        self.queue(batches.into_iter().map(Write::Upsert));
        Ok(())
    }

    /// Queues the removal of the rows with these keys; `batches` hold only the key columns.
    pub fn delete(&self, batches: &[RecordBatch]) -> Result<(), Error> {
        self.expect_source("delete", |s| matches!(s, Source::Keyed(_)))?;
        let key_schema = match &self.session.read().tables[self.id].source {
            Source::Keyed(keyed) => keyed.key_schema.clone(),
            _ => unreachable!("checked above"),
        };
        let batches = conform_all(&key_schema, batches)?;
        self.queue(batches.into_iter().map(Write::Delete));
        Ok(())
    }

    /// Rows for which `predicate` is true. NULL counts as false.
    pub fn filter(&self, predicate: &str) -> Result<Table, Error> {
        let expression = Expression::predicate(predicate, &self.schema)?;
        // Over an input that only gains rows, `where` numbers its own rows.
        let add_only = self.add_only();
        self.derive(
            format!("where({predicate})"),
            self.schema.clone(),
            if add_only {
                Growth::AppendOnly
            } else {
                Growth::Changing
            },
            Operator::Where(Where::new(expression, add_only)),
        )
    }

    /// Adds or replaces columns, each `name = expression`. Later assignments see earlier ones.
    pub fn update(&self, assignments: &[&str]) -> Result<Table, Error> {
        if assignments.is_empty() {
            return Err(Error::Unsupported(
                "update needs at least one `name = expression`".into(),
            ));
        }
        let mut schema = self.schema.clone();
        let mut bound = Vec::with_capacity(assignments.len());
        for text in assignments {
            let (assignment, next) = assign(&schema, text)?;
            bound.push(assignment);
            schema = next;
        }
        let update = Update {
            assignments: bound,
            columns: (0..schema.fields().len()).collect(),
            output: schema.clone(),
        };
        self.derive(
            format!("update({})", assignments.join(", ")),
            schema,
            self.growth(),
            Operator::Update(update),
        )
    }

    /// Only the listed columns, in that order: each an existing column's name or
    /// `name = expression` (as in `update`; later entries see earlier ones).
    pub fn select(&self, columns: &[&str]) -> Result<Table, Error> {
        if columns.is_empty() {
            return Err(Error::Unsupported(
                "select needs at least one column".into(),
            ));
        }
        let mut schema = self.schema.clone();
        let (mut bound, mut keep) = (Vec::new(), Vec::with_capacity(columns.len()));
        for text in columns {
            let index = match schema.index_of(text.trim()) {
                Ok(index) => index,
                Err(_) => {
                    let (assignment, next) = assign(&schema, text)?;
                    let index = assignment.position;
                    bound.push(assignment);
                    schema = next;
                    index
                }
            };
            if keep.contains(&index) {
                return Err(Error::Schema(format!(
                    "select lists column `{}` twice",
                    schema.field(index).name()
                )));
            }
            keep.push(index);
        }
        let output = Arc::new(schema.project(&keep).map_err(schema_error)?);
        let select = Update {
            assignments: bound,
            columns: keep,
            output: output.clone(),
        };
        self.derive(
            format!("select({})", columns.join(", ")),
            output,
            self.growth(),
            Operator::Update(select),
        )
    }

    /// Rows ordered by `by`, each entry `"name"` or `"name desc"` (`"name asc"` is the default),
    /// earlier entries first. NULL sorts as the largest value; ties keep the input's order. The
    /// table must be static: ticking sort arrives with SQL `ORDER BY`.
    pub fn sort(&self, by: &[&str]) -> Result<Table, Error> {
        if by.is_empty() {
            return Err(Error::Unsupported("sort needs at least one column".into()));
        }
        let keys = by
            .iter()
            .map(|entry| {
                let words: Vec<&str> = entry.split_whitespace().collect();
                let (name, descending) = match words[..] {
                    [name] => (name, false),
                    [name, order] if order.eq_ignore_ascii_case("asc") => (name, false),
                    [name, order] if order.eq_ignore_ascii_case("desc") => (name, true),
                    _ => {
                        return Err(Error::Schema(format!(
                            "sort takes \"name\" or \"name desc\", not \"{entry}\""
                        )));
                    }
                };
                Ok((resolve(&self.schema, &[name])?[0], descending))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let description = format!("sort({})", by.join(", "));
        let mut graph = self.session.write();
        let table = &graph.tables[self.id];
        if let Some(cause) = &table.failure {
            return Err(Error::Upstream {
                table: description,
                cause: Arc::new(cause.clone()),
            });
        }
        if table.refreshing {
            return Err(Error::Unsupported(format!(
                "sort needs a static table, and `{}` ticks; ticking sort arrives with SQL ORDER BY",
                table.description
            )));
        }
        let compute = |e: ArrowError| Error::Compute {
            table: description.clone(),
            message: e.to_string(),
        };
        let rows = Reader::new(&graph.tables, self.id)
            .snapshot()
            .map_err(compute)?;
        let sorted = sorted(&self.schema, &rows, &keys).map_err(compute)?;
        let state =
            TableState::fixed(description.clone(), self.schema.clone(), sorted).map_err(compute)?;
        let id = graph.push(state);
        drop(graph);
        idle();
        Ok(Table::new(&self.session, id, self.schema.clone()))
    }

    /// The first `n` rows in key order (arrival order for appended rows), kept current as rows
    /// come and go.
    pub fn head(&self, n: usize) -> Result<Table, Error> {
        self.edge(End::First, n)
    }

    /// The last `n` rows in key order (the newest, for appended rows), kept current as rows come
    /// and go.
    pub fn tail(&self, n: usize) -> Result<Table, Error> {
        self.edge(End::Last, n)
    }

    fn edge(&self, end: End, n: usize) -> Result<Table, Error> {
        let name = match end {
            End::First => "head",
            End::Last => "tail",
        };
        // Rows leave as nearer ones arrive, so the output can lose rows whatever its input does.
        self.derive(
            format!("{name}({n})"),
            self.schema.clone(),
            Growth::Changing,
            Operator::Edge(Edge::new(end, n)),
        )
    }

    /// Each row with aggregates over a backward window of its group's rows (grouped by `by`;
    /// every row in one group without): count, sum, mean, std, min, or max, each
    /// `(name, "function(column)")`, NULL values ignored. The table must only append rows,
    /// since the window follows row order.
    pub fn rolling(
        &self,
        by: &[&str],
        window: Window<'_>,
        aggregations: &[(&str, &str)],
    ) -> Result<Table, Error> {
        if self.growth() != Growth::AppendOnly {
            return Err(Error::Unsupported(
                "rolling needs a table that only appends rows: an append-only input, or \
                 where/update over one"
                    .into(),
            ));
        }
        if aggregations.is_empty() {
            return Err(Error::Unsupported(
                "rolling needs at least one aggregation".into(),
            ));
        }
        let span = match window {
            Window::Rows(0) => {
                return Err(Error::Unsupported(
                    "a rolling window needs at least one row".into(),
                ));
            }
            Window::Rows(n) => Span::Rows(n),
            Window::Time { span, .. } if span <= 0 => {
                return Err(Error::Unsupported(
                    "a rolling time window needs a positive span".into(),
                ));
            }
            Window::Time { at, span } => {
                let at = resolve(&self.schema, &[at])?[0];
                let data_type = self.schema.field(at).data_type();
                if !matches!(
                    data_type,
                    DataType::Int32 | DataType::Int64 | DataType::Date32 | DataType::Timestamp(..)
                ) {
                    return Err(Error::Schema(format!(
                        "a rolling time window needs an integer, date, or timestamp column; `{}` \
                         is {}",
                        self.schema.field(at).name(),
                        type_name(data_type)
                    )));
                }
                Span::Time { at, span }
            }
        };
        let (mut inputs, mut aggregates) = (Vec::<Input>::new(), Vec::new());
        let mut fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for (name, spec) in aggregations {
            if fields.iter().any(|f| f.name() == name) {
                return Err(Error::Schema(format!("column `{name}` already exists")));
            }
            let call = Call::parse(spec, &self.schema)?;
            let function = match call.function {
                "count" => Function::Count,
                "sum" => Function::Sum,
                "mean" => Function::Mean,
                "std" => Function::Std,
                "min" => Function::Min,
                "max" => Function::Max,
                other => {
                    return Err(call_error(
                        spec,
                        format!(
                            "rolling supports count, sum, mean, std, min, and max, not `{other}`"
                        ),
                    ));
                }
            };
            let input_type = call.column.map(|c| self.schema.field(c).data_type());
            if matches!(function, Function::Min | Function::Max)
                && !matches!(
                    input_type,
                    Some(
                        DataType::Int32
                            | DataType::Int64
                            | DataType::Float64
                            | DataType::Date32
                            | DataType::Timestamp(..)
                    )
                )
            {
                return Err(call_error(
                    spec,
                    "rolling min and max need a number, date, or timestamp".into(),
                ));
            }
            let output = match (function, input_type) {
                (Function::Count, _) => DataType::Int64,
                (Function::Sum, Some(DataType::Float64)) => DataType::Float64,
                (Function::Sum, _) => DataType::Int64,
                (Function::Mean | Function::Std, _) => DataType::Float64,
                (Function::Min | Function::Max, Some(data_type)) => data_type.clone(),
                (Function::Min | Function::Max, None) => unreachable!("checked above"),
            };
            let input = call.column.map(|column| {
                inputs
                    .iter()
                    .position(|i| i.column == column)
                    .unwrap_or_else(|| {
                        let float = self.schema.field(column).data_type() == &DataType::Float64;
                        inputs.push(Input { column, float });
                        inputs.len() - 1
                    })
            });
            fields.push(Field::new(*name, output.clone(), true));
            aggregates.push(Aggregate {
                function,
                input,
                output,
            });
        }
        let schema = Arc::new(Schema::new(fields));
        let window_text = match window {
            Window::Rows(n) => format!("rows={n}"),
            Window::Time { at, span } => format!("time={span} at {at}"),
        };
        let named: Vec<String> = aggregations
            .iter()
            .map(|(name, spec)| format!("{name}={spec}"))
            .collect();
        let operator = Rolling::new(
            self.membership(by)?,
            KeyKind::new(&self.schema, resolve(&self.schema, by)?).map_err(schema_error)?,
            span,
            inputs,
            aggregates,
            schema.clone(),
        );
        self.derive(
            format!(
                "rolling([{}], {window_text}, {})",
                by.join(", "),
                named.join(", ")
            ),
            schema,
            Growth::AppendOnly,
            Operator::Rolling(Box::new(operator)),
        )
    }

    /// The last row (in arrival order) for each value of `columns`.
    pub fn last_by(&self, columns: &[&str]) -> Result<Table, Error> {
        let groups = self.membership(columns)?;
        let keys =
            KeyKind::new(&self.schema, resolve(&self.schema, columns)?).map_err(schema_error)?;
        self.derive(
            format!("last_by({})", columns.join(", ")),
            self.schema.clone(),
            Growth::Changing,
            Operator::LastBy(Box::new(LastBy::new(groups, keys, !self.add_only()))),
        )
    }

    /// One row per value of `columns`, with each `(name, "function(column)")` aggregation:
    /// count, sum, mean, std, min, max, first, last. `count(*)` counts rows.
    pub fn agg_by(&self, columns: &[&str], aggregations: &[(&str, &str)]) -> Result<Table, Error> {
        if aggregations.is_empty() {
            return Err(Error::Unsupported(
                "agg_by needs at least one aggregation".into(),
            ));
        }
        let group_columns = resolve(&self.schema, columns)?;
        let mut outputs = Vec::with_capacity(aggregations.len());
        for (name, spec) in aggregations {
            let (_, data_type): (Accumulator, DataType) = Accumulator::parse(spec, &self.schema)?;
            outputs.push(((*name).to_owned(), data_type));
        }
        let output = agg_schema(&self.schema, &group_columns, &outputs)?;
        let specs: Vec<&str> = aggregations.iter().map(|(_, spec)| *spec).collect();
        let operator = AggBy::new(
            &self.schema,
            group_columns,
            &specs,
            self.add_only(),
            output.clone(),
        )?;
        let named: Vec<String> = aggregations
            .iter()
            .map(|(name, spec)| format!("{name}={spec}"))
            .collect();
        let description = format!("agg_by([{}], {})", columns.join(", "), named.join(", "));
        self.derive(
            description,
            output,
            Growth::Changing,
            Operator::AggBy(operator),
        )
    }

    fn membership(&self, columns: &[&str]) -> Result<Membership, Error> {
        let columns = resolve(&self.schema, columns)?;
        Membership::new(&self.schema, columns, self.add_only(), 0, 1).map_err(schema_error)
    }

    /// Each row of this table with columns from the row of `right` whose `on` columns hold the
    /// same values, or NULLs if none does; a key with more than one right row fails the table.
    /// `on` entries are `name` or `left_name=right_name`. `joins` lists the right columns to add,
    /// as `name` or `new_name=right_name`; by default every right column not in `on`.
    pub fn natural_join(
        &self,
        right: &Table,
        on: &[&str],
        joins: Option<&[&str]>,
    ) -> Result<Table, Error> {
        let plan = self.join_plan(right, on, None, joins)?;
        // Group members go unused: the join keeps its own left rows (`Tracking`).
        let groups =
            Membership::new(&self.schema, plan.left_on, true, 0, 1).map_err(schema_error)?;
        let tracking = if !right.is_refreshing() {
            Tracking::None
        } else if self.add_only() {
            Tracking::Appended
        } else {
            Tracking::Changing
        };
        let description = format!("natural_join(on=[{}])", on.join(", "));
        let output = plan.shape.output.clone();
        let operator = Join::Natural(NaturalJoin::new(plan.shape, tracking, groups));
        self.join(right, description, output, operator)
    }

    /// Each row of this table with columns from the row of `right` whose `on` columns hold the
    /// same values and whose `at` time is the latest not after this row's, or NULLs if none;
    /// among equal times, the right row that arrived last. `at` is `name` or
    /// `left_name=right_name`, an integer, date, or timestamp column on both sides. `right` must
    /// only gain rows. `on` and `joins` are as for `natural_join`; `joins` defaults to every
    /// right column not in `on` or `at`.
    pub fn as_of_join(
        &self,
        right: &Table,
        on: &[&str],
        at: &str,
        joins: Option<&[&str]>,
    ) -> Result<Table, Error> {
        if !right.add_only() {
            return Err(Error::Unsupported(
                "as_of_join needs a right table that only gains rows: an append-only input, or \
                 where/update over one"
                    .into(),
            ));
        }
        let plan = self.join_plan(right, on, Some(at), joins)?;
        let (left_at, right_at) = plan.at.expect("planned with `at`");
        let left_at = KeyKind::new(&self.schema, vec![left_at]).map_err(schema_error)?;
        let right_at = KeyKind::new(&right.schema, vec![right_at]).map_err(schema_error)?;
        let groups =
            Membership::new(&self.schema, plan.left_on, true, 0, 1).map_err(schema_error)?;
        let description = format!("as_of_join(on=[{}], at={at})", on.join(", "));
        let output = plan.shape.output.clone();
        let tracking = right.is_refreshing();
        let operator = Join::AsOf(AsOfJoin::new(
            plan.shape, tracking, left_at, right_at, groups,
        ));
        self.join(right, description, output, operator)
    }

    /// Resolves a join's columns on both sides and its output schema.
    fn join_plan(
        &self,
        right: &Table,
        on: &[&str],
        at: Option<&str>,
        joins: Option<&[&str]>,
    ) -> Result<JoinPlan, Error> {
        let pairs: Vec<(&str, &str)> = on.iter().map(|text| pair(text)).collect();
        let left_names: Vec<&str> = pairs.iter().map(|p| p.0).collect();
        let right_names: Vec<&str> = pairs.iter().map(|p| p.1).collect();
        let left_on = resolve(&self.schema, &left_names)?;
        let right_on = resolve(&right.schema, &right_names)?;
        let same_type = |l: usize, r: usize, what: &str| {
            let (lt, rt) = (
                self.schema.field(l).data_type(),
                right.schema.field(r).data_type(),
            );
            if lt == rt {
                return Ok(());
            }
            Err(Error::Schema(format!(
                "{what} `{}` is {} on the left but `{}` is {} on the right; they must have the \
                 same type",
                self.schema.field(l).name(),
                type_name(lt),
                right.schema.field(r).name(),
                type_name(rt)
            )))
        };
        for (l, r) in left_on.iter().zip(&right_on) {
            same_type(*l, *r, "join key")?;
        }
        let at = match at {
            None => None,
            Some(text) => {
                let (l, r) = pair(text);
                let (l, r) = (
                    resolve(&self.schema, &[l])?[0],
                    resolve(&right.schema, &[r])?[0],
                );
                same_type(l, r, "time column")?;
                if !matches!(
                    self.schema.field(l).data_type(),
                    DataType::Int32
                        | DataType::Int64
                        | DataType::Date32
                        | DataType::Timestamp(_, _)
                ) {
                    return Err(Error::Schema(format!(
                        "as_of_join matches on an integer, date, or timestamp; `{}` is {}",
                        self.schema.field(l).name(),
                        type_name(self.schema.field(l).data_type())
                    )));
                }
                Some((l, r))
            }
        };
        let added: Vec<(String, usize)> = match joins {
            Some(joins) => joins
                .iter()
                .map(|text| {
                    let (name, column) = pair(text);
                    Ok((name.to_owned(), resolve(&right.schema, &[column])?[0]))
                })
                .collect::<Result<_, Error>>()?,
            None => (0..right.schema.fields().len())
                .filter(|c| !right_on.contains(c) && at.is_none_or(|(_, r)| r != *c))
                .map(|c| (right.schema.field(c).name().clone(), c))
                .collect(),
        };
        let mut fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for (name, column) in &added {
            if fields.iter().any(|f| f.name() == name) {
                return Err(Error::Schema(format!(
                    "column `{name}` is in both tables; rename the right one with \
                     joins=[\"right_{name}={name}\", ...]"
                )));
            }
            let data_type = right.schema.field(*column).data_type().clone();
            fields.push(Field::new(name, data_type, true));
        }
        let shape = JoinShape {
            left_keys: KeyKind::new(&self.schema, left_on.clone()).map_err(schema_error)?,
            right_keys: KeyKind::new(&right.schema, right_on).map_err(schema_error)?,
            right_columns: added.into_iter().map(|(_, c)| c).collect(),
            output: Arc::new(Schema::new(fields)),
        };
        Ok(JoinPlan { shape, left_on, at })
    }

    /// Builds a join of this table and `right`, computed from both tables' current rows. It
    /// ticks if either side does, and only gains rows if this side only gains rows and `right`
    /// never changes.
    fn join(
        &self,
        right: &Table,
        description: String,
        schema: SchemaRef,
        operator: Join,
    ) -> Result<Table, Error> {
        if !Arc::ptr_eq(&self.session.inner, &right.session.inner) {
            return Err(Error::Unsupported(
                "tables from different sessions cannot be joined".into(),
            ));
        }
        let mut graph = self.session.write();
        let (left, other) = (&graph.tables[self.id], &graph.tables[right.id]);
        if let Some(cause) = left.failure.as_ref().or(other.failure.as_ref()) {
            return Err(Error::Upstream {
                table: description,
                cause: Arc::new(cause.clone()),
            });
        }
        let source = Source::Join {
            left: self.id,
            right: right.id,
            operator: Box::new(operator),
        };
        let mut state = TableState::new(
            description,
            schema.clone(),
            left.refreshing || other.refreshing,
            // Over a static right side, rows are the left side's rows under its keys.
            if other.refreshing {
                Growth::Changing
            } else {
                left.growth
            },
            source,
        );
        let (left_rows, right_rows) = (
            Reader::new(&graph.tables, self.id),
            Reader::new(&graph.tables, right.id),
        );
        let result = left_rows.as_added().and_then(|left_added| {
            let right_added = right_rows.as_added()?;
            let Source::Join { operator, .. } = &mut state.source else {
                unreachable!("built as a join above");
            };
            operator.process(left_rows, &left_added, right_rows, &right_added)
        });
        state.commit(result);
        if let Some(error) = state.failure {
            return Err(error);
        }
        state.settle();
        let id = graph.push(state);
        drop(graph);
        idle();
        Ok(Table::new(&self.session, id, schema))
    }

    /// Every row of every one of `tables`, which need the same column names and types in the
    /// same order. Rows keep their table's order, tables follow one another, and nothing is
    /// copied: the merge reads each row from its table. It ticks if any table does, and a merge
    /// among `tables` contributes its own tables.
    pub fn merge(tables: &[&Table]) -> Result<Table, Error> {
        let Some(first) = tables.first() else {
            return Err(Error::Unsupported("merge needs at least one table".into()));
        };
        if tables
            .iter()
            .any(|t| !Arc::ptr_eq(&t.session.inner, &first.session.inner))
        {
            return Err(Error::Unsupported(
                "tables from different sessions cannot be merged".into(),
            ));
        }
        let columns = |schema: &Schema| -> Vec<(String, DataType)> {
            schema
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().clone()))
                .collect()
        };
        let listed = |schema: &Schema| -> String {
            schema
                .fields()
                .iter()
                .map(|f| format!("{} {}", f.name(), type_name(f.data_type())))
                .collect::<Vec<_>>()
                .join(", ")
        };
        if let Some(other) = tables
            .iter()
            .find(|t| columns(&t.schema) != columns(&first.schema))
        {
            return Err(Error::Schema(format!(
                "merge needs the same columns in every table, in the same order: ({}) and ({})",
                listed(&first.schema),
                listed(&other.schema)
            )));
        }
        let mut graph = first.session.write();
        let mut inputs = Vec::new();
        for table in tables {
            let state = &graph.tables[table.id];
            if let Some(cause) = &state.failure {
                return Err(Error::Upstream {
                    table: "merge".into(),
                    cause: Arc::new(cause.clone()),
                });
            }
            match &state.source {
                Source::Merge { inputs: theirs } => inputs.extend(theirs),
                _ => inputs.push(table.id),
            }
        }
        if inputs.len() > 1 << (RowKey::BITS - MERGE_KEY_BITS) {
            return Err(Error::Unsupported(format!(
                "merge takes at most {} tables",
                1 << (RowKey::BITS - MERGE_KEY_BITS)
            )));
        }
        let fields: Vec<Field> = first
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let nullable = tables.iter().any(|t| t.schema.field(i).is_nullable());
                field.as_ref().clone().with_nullable(nullable)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let state = TableState::new(
            format!("merge({} tables)", inputs.len()),
            schema.clone(),
            inputs.iter().any(|id| graph.tables[*id].refreshing),
            if inputs
                .iter()
                .all(|id| graph.tables[*id].growth >= Growth::AddOnly)
            {
                Growth::AddOnly
            } else {
                Growth::Changing
            },
            Source::Merge { inputs },
        );
        let id = graph.push(state);
        drop(graph);
        idle();
        Ok(Table::new(&first.session, id, schema))
    }

    /// The table's current rows in key order, or the error that failed it.
    pub fn snapshot(&self) -> Result<Vec<RecordBatch>, Error> {
        let graph = self.session.read();
        let table = &graph.tables[self.id];
        if let Some(error) = &table.failure {
            return Err(error.clone());
        }
        Reader::new(&graph.tables, self.id)
            .snapshot()
            .map_err(|e| Error::Compute {
                table: table.description.clone(),
                message: e.to_string(),
            })
    }

    /// The first `n` rows in key order, or the error that failed the table.
    pub fn first_rows(&self, n: usize) -> Result<RecordBatch, Error> {
        let graph = self.session.read();
        let table = &graph.tables[self.id];
        if let Some(error) = &table.failure {
            return Err(error.clone());
        }
        Reader::new(&graph.tables, self.id)
            .first_rows(n)
            .map_err(|e| Error::Compute {
                table: table.description.clone(),
                message: e.to_string(),
            })
    }

    /// The cycle that last changed this table's rows or failed it; 0 if none has.
    pub fn changed(&self) -> u64 {
        self.session.read().tables[self.id].changed
    }

    pub fn num_rows(&self) -> Result<usize, Error> {
        let graph = self.session.read();
        let table = &graph.tables[self.id];
        match &table.failure {
            Some(error) => Err(error.clone()),
            None => Ok(Reader::new(&graph.tables, self.id).len()),
        }
    }

    fn growth(&self) -> Growth {
        self.session.read().tables[self.id].growth
    }

    fn add_only(&self) -> bool {
        self.growth() >= Growth::AddOnly
    }

    fn expect_source(&self, action: &str, accepts: fn(&Source) -> bool) -> Result<(), Error> {
        let graph = self.session.read();
        let table = &graph.tables[self.id];
        if accepts(&table.source) {
            return Ok(());
        }
        Err(Error::Unsupported(format!(
            "`{}` does not accept {action}",
            table.description
        )))
    }

    fn queue(&self, writes: impl Iterator<Item = Write>) {
        self.session.pending().extend(writes.map(|w| (self.id, w)));
        self.session.inner.arrived.notify_all();
    }

    /// Builds a table from this one, computed from this table's current rows. Building on a
    /// static table finishes here; building on a refreshing one also updates it every cycle.
    fn derive(
        &self,
        description: String,
        schema: SchemaRef,
        growth: Growth,
        operator: Operator,
    ) -> Result<Table, Error> {
        let mut graph = self.session.write();
        let parent = &graph.tables[self.id];
        let source = Source::Derived {
            parent: self.id,
            operator: Box::new(operator),
        };
        let mut state = TableState::new(
            description,
            schema.clone(),
            parent.refreshing,
            growth,
            source,
        );
        if let Some(cause) = &parent.failure {
            return Err(Error::Upstream {
                table: state.description,
                cause: Arc::new(cause.clone()),
            });
        }
        let input = Reader::new(&graph.tables, self.id);
        let result = input.as_added().and_then(|everything| {
            let Source::Derived { operator, .. } = &mut state.source else {
                unreachable!("built as derived above");
            };
            operator.process(input, &everything, state.storage.as_ref().expect(STORED))
        });
        state.commit(result);
        if let Some(error) = state.failure {
            return Err(error);
        }
        state.settle();
        let id = graph.push(state);
        drop(graph);
        idle();
        Ok(Table::new(&self.session, id, schema))
    }
}

/// An input table's schema: supported types, unique names, every column nullable.
fn nullable(schema: &Schema) -> Result<SchemaRef, Error> {
    check_types(schema)?;
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    for (i, field) in fields.iter().enumerate() {
        if fields[..i].iter().any(|f| f.name() == field.name()) {
            return Err(Error::Schema(format!(
                "duplicate column `{}`",
                field.name()
            )));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn schema_error(error: ArrowError) -> Error {
    Error::Schema(error.to_string())
}

/// Binds `text`, `name = expression`, against `schema`: the assignment, and the schema after it
/// (the column replaced, or appended if new).
fn assign(schema: &SchemaRef, text: &str) -> Result<(Assignment, SchemaRef), Error> {
    let (name, expression) = Expression::assignment(text, schema)?;
    let field: FieldRef = Arc::new(Field::new(&name, expression.data_type().clone(), true));
    let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
    let position = match schema.index_of(&name) {
        Ok(index) => {
            fields[index] = field;
            index
        }
        Err(_) => {
            fields.push(field);
            fields.len() - 1
        }
    };
    let assignment = Assignment {
        input: schema.clone(),
        expression,
        position,
    };
    Ok((assignment, Arc::new(Schema::new(fields))))
}

/// Matches each batch's columns to `schema` by name and type; empty batches are dropped.
fn conform_all(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, Error> {
    batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| conform(schema, b))
        .collect()
}

fn conform(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let given = batch.schema();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let index = given.index_of(field.name()).map_err(|_| {
            Error::Schema(format!(
                "missing column `{}` ({})",
                field.name(),
                type_name(field.data_type())
            ))
        })?;
        let actual = given.field(index).data_type();
        if actual != field.data_type() {
            return Err(Error::Schema(format!(
                "column `{}` is {}, but the table expects {}",
                field.name(),
                type_name(actual),
                type_name(field.data_type())
            )));
        }
        columns.push(batch.column(index).clone());
    }
    let extra: Vec<&str> = given
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| schema.index_of(name).is_err())
        .collect();
    if !extra.is_empty() {
        return Err(Error::Schema(format!(
            "unexpected columns: {}",
            extra.join(", ")
        )));
    }
    let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &rows).map_err(schema_error)
}
