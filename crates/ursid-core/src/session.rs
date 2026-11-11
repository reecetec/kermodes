//! A session owns one update graph: input tables, the tables derived from them, and cycles.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow::compute::interleave_record_batch;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::agg::{Accumulator, AggBy, agg_schema};
use crate::groups::{KeyKind, Membership, resolve};
use crate::ops::{Assignment, LastBy, Operator, Update, Where};
use crate::storage::{Delta, Modified, RowKey, Rows, Storage};
use crate::types::check_types;
use crate::{Error, Expression, type_name};

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
    /// The background clock started by `Session::start`, if running.
    clock: Mutex<Option<Clock>>,
}

/// A thread running cycles on a period. It holds the session weakly, so dropping the last
/// `Session` handle also ends it.
struct Clock {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: JoinHandle<()>,
}

enum Write {
    Append(RecordBatch),
    Upsert(RecordBatch),
    /// Key columns only.
    Delete(RecordBatch),
}

#[derive(Default)]
struct Graph {
    /// In creation order, which is topological: a table is built only from existing tables.
    tables: Vec<TableState>,
    cycle: u64,
}

struct TableState {
    description: String,
    schema: SchemaRef,
    storage: Storage,
    refreshing: bool,
    /// Only ever gains rows; operators keep less state over such inputs.
    add_only: bool,
    failure: Option<Error>,
    /// This cycle's changes, read by the tables built from this one.
    delta: Option<Delta>,
    /// The cycle that last changed this table's rows or failed it; 0 if none has.
    changed: u64,
    source: Source,
}

enum Source {
    Append,
    Keyed(Box<Keyed>),
    Static,
    Derived {
        parent: usize,
        operator: Box<Operator>,
    },
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

/// A table in a session. Cheap to clone; clones refer to the same table.
#[derive(Clone)]
pub struct Table {
    session: Session,
    id: usize,
    schema: SchemaRef,
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
            true,
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
            false,
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
        let mut state = TableState::new("static".into(), schema, false, true, Source::Static);
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
        state
            .storage
            .apply(&Delta {
                added,
                ..Delta::default()
            })
            .map_err(schema_error)?;
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
            let result = match &mut table.source {
                Source::Static => continue,
                Source::Append => Ok(appended(&table.storage, writes)),
                Source::Keyed(keyed) => keyed.resolve(&table.storage, writes),
                Source::Derived { parent, operator } => {
                    if !table.refreshing {
                        continue;
                    }
                    let parent = &before[*parent];
                    if let Some(cause) = &parent.failure {
                        table.fail(Error::Upstream {
                            table: table.description.clone(),
                            cause: Arc::new(cause.clone()),
                        });
                        table.changed = cycle;
                        continue;
                    }
                    let Some(delta) = &parent.delta else {
                        continue;
                    };
                    operator.process(&parent.storage, delta, &table.storage)
                }
            };
            table.commit(result);
            if table.delta.is_some() || table.failure.is_some() {
                table.changed = cycle;
            }
        }
        graph.cycle = cycle;
        cycle
    }

    /// Starts running cycles in the background, one every `period` while writes are pending.
    /// Fails if a clock is already running; `stop` ends it.
    pub fn start(&self, period: Duration) -> Result<(), Error> {
        let mut clock = self.clock();
        if clock.is_some() {
            return Err(Error::Unsupported(
                "the session's clock is already running".into(),
            ));
        }
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let session = Arc::downgrade(&self.inner);
        let signal = stop.clone();
        let thread = thread::Builder::new()
            .name("ursid-clock".into())
            .spawn(move || {
                let (stopped, wake) = &*signal;
                loop {
                    let guard = stopped.lock().expect("ursid clock flag poisoned");
                    let (guard, _) = wake
                        .wait_timeout_while(guard, period, |stopped| !*stopped)
                        .expect("ursid clock flag poisoned");
                    if *guard {
                        return;
                    }
                    drop(guard);
                    let Some(inner) = session.upgrade() else {
                        return;
                    };
                    let session = Session { inner };
                    if !session.pending().is_empty() {
                        session.step();
                    }
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
        let (stopped, wake) = &*clock.stop;
        *stopped.lock().expect("ursid clock flag poisoned") = true;
        wake.notify_all();
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
        let mut graph = self.write();
        graph.tables.push(state);
        Table {
            session: self.clone(),
            id: graph.tables.len() - 1,
            schema,
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, Graph> {
        self.inner
            .graph
            .read()
            .expect("ursid graph lock poisoned by an earlier panic")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Graph> {
        self.inner
            .graph
            .write()
            .expect("ursid graph lock poisoned by an earlier panic")
    }

    fn pending(&self) -> MutexGuard<'_, Vec<(usize, Write)>> {
        self.inner
            .pending
            .lock()
            .expect("ursid write queue poisoned by an earlier panic")
    }
}

impl TableState {
    fn new(
        description: String,
        schema: SchemaRef,
        refreshing: bool,
        add_only: bool,
        source: Source,
    ) -> Self {
        let storage = if add_only {
            Storage::dense(schema.clone())
        } else {
            Storage::sparse(schema.clone())
        };
        Self {
            description,
            schema,
            storage,
            refreshing,
            add_only,
            failure: None,
            delta: None,
            changed: 0,
            source,
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
                self.storage.apply(&delta)?;
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
        let add_only = self.add_only();
        self.derive(
            format!("where({predicate})"),
            self.schema.clone(),
            add_only,
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
            let (name, expression) = Expression::assignment(text, &schema)?;
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
            bound.push(Assignment {
                input: schema.clone(),
                expression,
                position,
            });
            schema = Arc::new(Schema::new(fields));
        }
        let update = Update {
            assignments: bound,
            output: schema.clone(),
        };
        self.derive(
            format!("update({})", assignments.join(", ")),
            schema,
            self.add_only(),
            Operator::Update(update),
        )
    }

    /// The last row (in arrival order) for each value of `columns`.
    pub fn last_by(&self, columns: &[&str]) -> Result<Table, Error> {
        let groups = self.membership(columns)?;
        self.derive(
            format!("last_by({})", columns.join(", ")),
            self.schema.clone(),
            false,
            Operator::LastBy(LastBy::new(groups)),
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
        self.derive(description, output, false, Operator::AggBy(operator))
    }

    fn membership(&self, columns: &[&str]) -> Result<Membership, Error> {
        let columns = resolve(&self.schema, columns)?;
        Membership::new(&self.schema, columns, self.add_only(), 0, 1).map_err(schema_error)
    }

    /// The table's current rows in key order, or the error that failed it.
    pub fn snapshot(&self) -> Result<Vec<RecordBatch>, Error> {
        let graph = self.session.read();
        let table = &graph.tables[self.id];
        if let Some(error) = &table.failure {
            return Err(error.clone());
        }
        table.storage.snapshot().map_err(|e| Error::Compute {
            table: table.description.clone(),
            message: e.to_string(),
        })
    }

    /// The first `n` rows in key order, or the error that failed the table.
    pub fn head(&self, n: usize) -> Result<RecordBatch, Error> {
        let graph = self.session.read();
        let table = &graph.tables[self.id];
        if let Some(error) = &table.failure {
            return Err(error.clone());
        }
        table.storage.head(n).map_err(|e| Error::Compute {
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
            None => Ok(table.storage.len()),
        }
    }

    fn add_only(&self) -> bool {
        self.session.read().tables[self.id].add_only
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
    }

    /// Builds a table from this one, computed from this table's current rows. Building on a
    /// static table finishes here; building on a refreshing one also updates it every cycle.
    fn derive(
        &self,
        description: String,
        schema: SchemaRef,
        add_only: bool,
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
            add_only,
            source,
        );
        if let Some(cause) = &parent.failure {
            return Err(Error::Upstream {
                table: state.description,
                cause: Arc::new(cause.clone()),
            });
        }
        let result = parent.storage.as_added().and_then(|everything| {
            let Source::Derived { operator, .. } = &mut state.source else {
                unreachable!("built as derived above");
            };
            operator.process(&parent.storage, &everything, &state.storage)
        });
        state.commit(result);
        if let Some(error) = state.failure {
            return Err(error);
        }
        state.delta = None;
        graph.tables.push(state);
        Ok(Table {
            session: self.session.clone(),
            id: graph.tables.len() - 1,
            schema,
        })
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
