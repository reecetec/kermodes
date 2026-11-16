//! The replay clock (architecture.md §9.4): stored rows fed back through a graph in event-time
//! order, so the graph that runs live runs as a backtest over the same rows.
//!
//! A replay owns a session of its own, cycled only by `step`. Time moves on a grid of `step`
//! (multiples of it since the epoch, so a replay's cycles do not depend on where it starts):
//! each cycle feeds every row whose time is at or before its grid time, then runs the graph. A
//! stretch with no rows is skipped, not cycled through, so a night costs nothing.
//!
//! Rows arrive a date at a time (`Chunks`), so a replay holds one date of each table. An append
//! table's date is ordered by its event time (`ts`), ties in written order. A keyed table's date
//! is its log of changes in written order, as weir kept it (§5.2): reordering it would change its
//! state, so each change happens at its own time or, where that is earlier or missing (a
//! delete), at the change before's. Its first date's checkpoint is the state it starts from;
//! later dates' checkpoints restate what the log already built and are skipped. Time never goes
//! back, across dates either: a row timed before a row already fed arrives with it.
//!
//! A row with no time arrives with the row before it or, before any, with its date's first
//! timed row; a date with no times at all arrives at the epoch.
//!
//! The same rows and step give the same cycles and the same outputs.

use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, filter_record_batch, take_record_batch};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};

use crate::session::{Session, Table};
use crate::{Error, type_name};

/// A replayed table's rows, a date at a time, oldest first: each date's batches as written.
pub type Chunks = Box<dyn Iterator<Item = Result<Vec<RecordBatch>, Error>> + Send>;

/// What a replayed table is, and so how its rows are fed.
pub enum Kind<'a> {
    /// Rows are appended, in event-time order.
    Append,
    /// Rows are a keyed table's log: `op` names the column saying each row's change (`upsert`,
    /// `delete`, or `checkpoint`), which the replayed table does not have.
    Keyed { key: &'a [&'a str], op: &'a str },
}

/// A change in a keyed log.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Upsert,
    Delete,
}

/// The date of a table being fed: its rows in feeding order and each row's time.
struct Loaded {
    rows: RecordBatch,
    times: Vec<i64>,
    /// A keyed table's change per row; empty for an append table.
    ops: Vec<Op>,
    next: usize,
}

struct Source {
    table: Table,
    description: String,
    chunks: Chunks,
    /// The event-time column's position in the chunks' batches.
    ts: usize,
    /// The replayed table's columns' positions in the chunks' batches.
    columns: Vec<usize>,
    /// A keyed table's op column and key columns' positions in the chunks' batches.
    keyed: Option<(usize, Vec<usize>)>,
    loaded: Option<Loaded>,
    /// The time the latest row loaded is fed at: no later row is fed earlier.
    floor: i64,
    dates: usize,
}

pub struct Replay {
    session: Session,
    sources: Vec<Source>,
    step: i64,
    /// The latest cycle's grid time; `None` before the first.
    now: Option<i64>,
    cycles: u64,
    rows: u64,
}

impl Replay {
    /// A replay cycling every `step` of event time, in a session of its own.
    pub fn new(step: Duration) -> Result<Self, Error> {
        let step = i64::try_from(step.as_nanos())
            .ok()
            .filter(|step| *step > 0)
            .ok_or_else(|| {
                Error::Unsupported(format!("a replay's step is positive: not {step:?}"))
            })?;
        Ok(Self {
            session: Session::new(),
            sources: Vec::new(),
            step,
            now: None,
            cycles: 0,
            rows: 0,
        })
    }

    /// The session the replay cycles: build the graph on the tables `add` gives.
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// A table ticking with `chunks`' rows as the replay reaches each one's time in `ts`, a
    /// timestamp column. `schema` is the chunks' batches' columns; the table has them less a
    /// keyed table's op column. Every table is added before the first `step`.
    pub fn add(
        &mut self,
        description: &str,
        schema: &SchemaRef,
        ts: &str,
        kind: Kind<'_>,
        chunks: Chunks,
    ) -> Result<Table, Error> {
        if self.now.is_some() {
            return Err(Error::Unsupported(format!(
                "{description}: a replay's tables are all added before it starts"
            )));
        }
        let position = |name: &str| {
            schema
                .index_of(name)
                .map_err(|_| Error::Schema(format!("{description} has no column `{name}`")))
        };
        let ts_at = position(ts)?;
        if !matches!(schema.field(ts_at).data_type(), DataType::Timestamp(_, _)) {
            return Err(Error::Unsupported(format!(
                "{description}'s `{ts}` is {}: a replay orders by a timestamp column",
                type_name(schema.field(ts_at).data_type())
            )));
        }
        let (table, columns, keyed) = match kind {
            Kind::Append => (
                self.session.input_table(schema)?,
                (0..schema.fields().len()).collect(),
                None,
            ),
            Kind::Keyed { key, op } => {
                let op_at = position(op)?;
                let columns: Vec<usize> = (0..schema.fields().len())
                    .filter(|at| *at != op_at)
                    .collect();
                let table_schema = schema.project(&columns).map_err(arrow_error(description))?;
                let key_at = key.iter().map(|k| position(k)).collect::<Result<_, _>>()?;
                (
                    self.session.keyed_input_table(&table_schema, key)?,
                    columns,
                    Some((op_at, key_at)),
                )
            }
        };
        self.sources.push(Source {
            table: table.clone(),
            description: description.to_owned(),
            chunks,
            ts: ts_at,
            columns,
            keyed,
            loaded: None,
            floor: i64::MIN,
            dates: 0,
        });
        Ok(table)
    }

    /// Runs the next cycle: feeds every row due by the next grid time that has any, then runs
    /// the graph. That grid time, or `None` once every row has been fed.
    pub fn step(&mut self) -> Result<Option<i64>, Error> {
        let Some(due) = self.peek()? else {
            return Ok(None);
        };
        // The grid time at or after `due`, and after the last cycle's.
        let at = due.div_euclid(self.step) * self.step;
        let at = if at < due { at + self.step } else { at };
        let at = self.now.map_or(at, |now| at.max(now + self.step));
        for source in &mut self.sources {
            self.rows += source.feed(at)? as u64;
        }
        self.session.step();
        self.now = Some(at);
        self.cycles += 1;
        Ok(Some(at))
    }

    /// Steps until every row has been fed. With `speed`, cycles are paced to that many times
    /// event time (2.0: an hour of rows in half an hour); without, as fast as they run. Stops
    /// after `cycles` cycles if given, so a caller can check for interruptions between calls.
    pub fn run(&mut self, speed: Option<f64>, cycles: Option<u64>) -> Result<bool, Error> {
        let started = Instant::now();
        let mut origin = None::<i64>;
        let mut ran = 0;
        while cycles.is_none_or(|cycles| ran < cycles) {
            if let Some(speed) = speed.filter(|speed| *speed > 0.0)
                && let Some(next) = self.peek()?
            {
                let origin = *origin.get_or_insert(next);
                let wait = Duration::from_secs_f64((next - origin) as f64 / 1e9 / speed);
                if let Some(rest) = wait.checked_sub(started.elapsed()) {
                    thread::sleep(rest);
                }
            }
            if self.step()?.is_none() {
                return Ok(true);
            }
            ran += 1;
        }
        Ok(false)
    }

    /// The time of the next row to be fed, without feeding it.
    fn peek(&mut self) -> Result<Option<i64>, Error> {
        let mut due = None::<i64>;
        for source in &mut self.sources {
            if let Some(time) = source.next_time()? {
                due = Some(due.map_or(time, |due| due.min(time)));
            }
        }
        Ok(due)
    }

    /// The latest cycle's grid time, in nanoseconds since the epoch; `None` before the first.
    pub fn time(&self) -> Option<i64> {
        self.now
    }

    pub fn cycles(&self) -> u64 {
        self.cycles
    }

    /// Rows fed so far, across tables.
    pub fn rows(&self) -> u64 {
        self.rows
    }
}

fn arrow_error(description: &str) -> impl Fn(arrow::error::ArrowError) -> Error + '_ {
    move |e| Error::Compute {
        table: description.to_owned(),
        message: e.to_string(),
    }
}

impl Source {
    /// The time of the next row to feed, loading dates until one has rows; `None` at the end.
    fn next_time(&mut self) -> Result<Option<i64>, Error> {
        loop {
            if let Some(loaded) = &self.loaded
                && loaded.next < loaded.times.len()
            {
                return Ok(Some(loaded.times[loaded.next]));
            }
            let Some(chunk) = self.chunks.next() else {
                self.loaded = None;
                return Ok(None);
            };
            self.loaded = Some(self.load(chunk?)?);
            self.dates += 1;
        }
    }

    /// A date's rows in feeding order, each with the time it is fed at.
    fn load(&mut self, batches: Vec<RecordBatch>) -> Result<Loaded, Error> {
        let failed = arrow_error(&self.description);
        let Some(first) = batches.first() else {
            return Ok(Loaded {
                rows: RecordBatch::new_empty(self.table.schema().clone()),
                times: Vec::new(),
                ops: Vec::new(),
                next: 0,
            });
        };
        let rows = concat_batches(&first.schema(), &batches).map_err(&failed)?;
        let raw = nanoseconds(rows.column(self.ts));
        // Where a row with no time goes when no row before it has one: with its date's first
        // timed row, or, in a date with no times at all, at the epoch.
        let start =
            |times: &mut dyn Iterator<Item = Option<i64>>, floor: i64| match times.flatten().next()
            {
                Some(time) => time.max(floor),
                None if floor == i64::MIN => 0,
                None => floor,
            };
        match self.keyed {
            None => {
                // Event-time order, ties as written; rows with no time come first.
                let mut order: Vec<u32> = (0..rows.num_rows() as u32).collect();
                order.sort_by_key(|at| raw[*at as usize].unwrap_or(i64::MIN));
                let rows =
                    take_record_batch(&rows, &UInt32Array::from(order.clone())).map_err(&failed)?;
                let base = start(&mut order.iter().map(|at| raw[*at as usize]), self.floor);
                // Sorted, so no row here is fed before one above it; one timed before rows
                // already fed arrives in the next cycle with whatever is due.
                let mut times = Vec::with_capacity(order.len());
                for at in order {
                    let time = raw[at as usize].unwrap_or(base);
                    self.floor = self.floor.max(time);
                    times.push(time);
                }
                let rows = rows.project(&self.columns).map_err(&failed)?;
                Ok(Loaded {
                    rows,
                    times,
                    ops: Vec::new(),
                    next: 0,
                })
            }
            Some((op_at, _)) => {
                let kinds = rows.column(op_at).as_string::<i32>();
                // Only the first date's checkpoint is a starting state.
                let keep: Vec<bool> = (0..rows.num_rows())
                    .map(|at| self.dates == 0 || kinds.value(at) != "checkpoint")
                    .collect();
                let base = start(
                    &mut (0..rows.num_rows())
                        .filter(|at| keep[*at])
                        .map(|at| raw[at]),
                    self.floor,
                );
                let mut ops = Vec::with_capacity(rows.num_rows());
                let mut times = Vec::with_capacity(rows.num_rows());
                for at in (0..rows.num_rows()).filter(|at| keep[*at]) {
                    ops.push(match kinds.value(at) {
                        "upsert" | "checkpoint" => Op::Upsert,
                        "delete" => Op::Delete,
                        other => {
                            return Err(Error::Compute {
                                table: self.description.clone(),
                                message: format!("a change `{other}` in its log"),
                            });
                        }
                    });
                    // A change with no time (a delete) happens with the change before it.
                    let time = raw[at].unwrap_or(self.floor.max(base)).max(self.floor);
                    self.floor = time;
                    times.push(time);
                }
                let rows = filter_record_batch(&rows, &keep.into()).map_err(&failed)?;
                Ok(Loaded {
                    rows,
                    times,
                    ops,
                    next: 0,
                })
            }
        }
    }

    /// Feeds every row due at or before `at`, loading the next date when one runs out: a date's
    /// first rows may be due already (a row timed before the last fed arrives with it). How many.
    fn feed(&mut self, at: i64) -> Result<usize, Error> {
        let mut fed = 0;
        while self.next_time()?.is_some_and(|time| time <= at) {
            fed += self.feed_loaded(at)?;
        }
        Ok(fed)
    }

    /// Feeds the loaded date's rows due at or before `at`; how many.
    fn feed_loaded(&mut self, at: i64) -> Result<usize, Error> {
        let Some(loaded) = &mut self.loaded else {
            return Ok(0);
        };
        let start = loaded.next;
        let end = start + loaded.times[start..].partition_point(|time| *time <= at);
        loaded.next = end;
        if start == end {
            return Ok(0);
        }
        let failed = arrow_error(&self.description);
        let Some((_, key_at)) = &self.keyed else {
            self.table
                .append(&[loaded.rows.slice(start, end - start)])?;
            return Ok(end - start);
        };
        // A keyed log's changes in order, a run of one kind at a time.
        let mut run = start;
        while run < end {
            let op = loaded.ops[run];
            let length = loaded.ops[run..end]
                .iter()
                .take_while(|o| **o == op)
                .count();
            let rows = loaded.rows.slice(run, length);
            match op {
                Op::Upsert => {
                    let rows = rows.project(&self.columns).map_err(&failed)?;
                    self.table.upsert(&[rows])?;
                }
                Op::Delete => {
                    let keys = rows.project(key_at).map_err(&failed)?;
                    self.table.delete(&[keys])?;
                }
            }
            run += length;
        }
        Ok(end - start)
    }
}

/// A timestamp column's values as nanoseconds since the epoch; `None` where NULL.
fn nanoseconds(values: &ArrayRef) -> Vec<Option<i64>> {
    use arrow::datatypes::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    let scale = |values: Vec<Option<i64>>, by: i64| -> Vec<Option<i64>> {
        values
            .into_iter()
            .map(|v| v.map(|v| v.saturating_mul(by)))
            .collect()
    };
    match values.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => scale(
            values
                .as_primitive::<TimestampSecondType>()
                .iter()
                .collect(),
            1_000_000_000,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => scale(
            values
                .as_primitive::<TimestampMillisecondType>()
                .iter()
                .collect(),
            1_000_000,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => scale(
            values
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .collect(),
            1_000,
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => values
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .collect(),
        other => unreachable!("Replay::add admits only timestamps, not {other}"),
    }
}
