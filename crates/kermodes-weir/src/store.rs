//! A store directory (architecture.md §16.6): the catalog, and weir's log of every table's
//! partitions, owned by one process at a time.
//!
//! ```text
//! {root}/lock                 held by the owning process
//! {root}/catalog.sqlite       the catalog
//! {root}/weir/{location}/{date}/{source}/seg-N.{arrows,idx}
//! {root}/weir/{location}/{date}/@checkpoint/seg-N.{arrows,idx}    a keyed table's day opening
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::new_null_array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use kermodes_core::type_name;
use nix::sys::statvfs::statvfs;
use serde_json::json;

use crate::catalog::{Catalog, Garbage, HistoryCommit, HistoryDate, HistoryFile, TableEntry};
use crate::keyed::Fold;
use crate::partition::{Partition, Stamp, in_order};
use crate::{Change, Date, Error, SourceId, TableName};

/// The branch a local store's tables live on (architecture.md §10.1).
pub const BRANCH: &str = "local";

/// Receives a table's batches in order, each with its stamp (change kind and ingest time): every
/// batch committed today (UTC) when it subscribes (a keyed table's starting with its
/// checkpoint), then each new one as it commits. Empty batches too: an empty checkpoint clears a
/// keyed table. Returning `false` unsubscribes it (its reader is gone, or too far behind).
/// `Pace` says whether it may wait for its reader before returning.
pub type Subscriber = Box<dyn FnMut(&RecordBatch, Stamp, Pace) -> bool + Send>;

/// Whether a subscriber may wait before taking the next batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pace {
    /// A batch read from disk while the subscriber catches up, with no lock held: waiting slows
    /// only this subscriber, which is how a slow reader holds back its own replay.
    MayWait,
    /// A batch delivered under the table's lock, as writes commit: waiting would hold up every
    /// writer, so a subscriber that cannot take it should return `false` instead.
    MustNotWait,
}

/// While more than this is left to catch up on, a subscriber reads it without the table's lock;
/// the rest it reads under the lock, then joins the writes, so none comes between and none
/// twice. Bounded in batches and in bytes, so writers wait for little either way and a
/// subscriber can take it all without waiting.
const CATCH_UP_UNDER_LOCK: usize = 64;
const CATCH_UP_UNDER_LOCK_BYTES: u64 = 4 << 20;

/// A keyed table gets a new checkpoint once this much has been written since its last (and at
/// least as much as that checkpoint holds): a subscriber's replay starts from the latest
/// checkpoint, so it reads about this much of changes however busy the day.
const CHECKPOINT_BYTES: usize = 64 << 20;

pub struct Store {
    root: PathBuf,
    /// Held for the store's life: the OS releases the lock when the process ends, however.
    _lock: File,
    catalog: Mutex<Catalog>,
    /// The catalog branch whose tables this store holds (§10.1).
    branch: String,
    logs: Mutex<HashMap<i64, Arc<Log>>>,
}

/// A table's log, and the signal that one of its source leases was released.
struct Log {
    state: Mutex<TableLog>,
    released: Condvar,
}

/// One table's partitions and subscribers. Its lock orders every write against every
/// subscription, so a subscriber sees each batch exactly once.
struct TableLog {
    entry: TableEntry,
    dir: PathBuf,
    /// By date, then stream (a keyed table's checkpoint first, then each source).
    partitions: BTreeMap<(String, Stream), Partition>,
    subscribers: Vec<Subscriber>,
    /// Sources whose lease a `Writer` holds: one writer per source (§7.4).
    writing: HashSet<SourceId>,
    /// The last ingest time given: each batch's is later, so ingest order is commit order.
    clock: i64,
    /// A keyed table's: bytes of changes written since its last checkpoint, and that
    /// checkpoint's size (unknown, so 0, after a restart).
    since_checkpoint: usize,
    checkpoint_bytes: usize,
    /// An append table's day in commit order, once a read by position has asked for it.
    positions: Option<Positions>,
}

impl TableLog {
    /// The next ingest time: now, or just after the last one if the clock has not moved on.
    fn tick(&mut self) -> i64 {
        self.clock = now_ns().max(self.clock + 1);
        self.clock
    }
}

/// An append table's day in commit order, for reads by position (§7.2): each batch's partition,
/// its index there, and the rows before it. Built from the index on first use; each commit
/// extends it, as ingest times rise across the table's partitions.
struct Positions {
    day: String,
    streams: Vec<Stream>,
    /// (stream, batch within its partition, rows before it)
    batches: Vec<(u32, u32, u64)>,
    rows: u64,
}

impl Positions {
    fn build(log: &TableLog, day: &str) -> Self {
        let mut streams = Vec::new();
        let mut batches = Vec::new();
        for ((date, stream), partition) in &log.partitions {
            if date != day {
                continue;
            }
            let at = streams.len() as u32;
            streams.push(stream.clone());
            for (index, (rows, ingest_ns)) in partition.batch_rows().enumerate() {
                batches.push((ingest_ns, at, index as u32, rows));
            }
        }
        // Ties (from before the table's clock) go to the earlier partition, as a replay's do.
        batches.sort_unstable_by_key(|&(ingest_ns, at, index, _)| (ingest_ns, at, index));
        let mut positions = Self {
            day: day.to_owned(),
            streams,
            batches: Vec::with_capacity(batches.len()),
            rows: 0,
        };
        for (_, at, index, rows) in batches {
            positions.batches.push((at, index, positions.rows));
            positions.rows += rows;
        }
        positions
    }

    fn push(&mut self, stream: &Stream, index: usize, rows: u64) {
        let at = match self.streams.iter().position(|known| known == stream) {
            Some(at) => at,
            None => {
                self.streams.push(stream.clone());
                self.streams.len() - 1
            }
        };
        self.batches.push((at as u32, index as u32, self.rows));
        self.rows += rows;
    }
}

impl Store {
    /// Opens the store in `root`, creating it if needed, with its catalog in SQLite beside its
    /// data: the `local` branch (§16.6). Fails if another process has it open.
    pub fn open(root: &Path) -> Result<Self, Error> {
        let lock = lock_dir(root)?;
        let catalog = Catalog::sqlite(&root.join("catalog.sqlite"))?;
        Ok(Self::new(root, lock, catalog, BRANCH))
    }

    /// Opens the store in `root` with its catalog in Postgres at `url`, as the cluster's weir:
    /// the tables of `branch`, which every client of that catalog sees alike, and lease epochs
    /// that rise across all of them. Fails if another process has `root` open.
    pub fn open_with_catalog(root: &Path, url: &str, branch: &str) -> Result<Self, Error> {
        let lock = lock_dir(root)?;
        let catalog = Catalog::postgres(url)?;
        Ok(Self::new(root, lock, catalog, branch))
    }

    fn new(root: &Path, lock: File, catalog: Catalog, branch: &str) -> Self {
        Self {
            root: root.to_owned(),
            _lock: lock,
            catalog: Mutex::new(catalog),
            branch: branch.to_owned(),
            logs: Mutex::new(HashMap::new()),
        }
    }

    /// Creates `name` with `schema`, or returns it if it exists with the same columns.
    pub fn create_table(&self, name: &TableName, schema: &Schema) -> Result<TableEntry, Error> {
        lock(&self.catalog).create(&self.branch, name, schema, &[], &[])
    }

    /// Creates `name` as a keyed table (§5.2): one row per value of the `key` columns, written as
    /// upserts and deletes. Returns it if it exists with the same columns and key.
    pub fn create_keyed_table(
        &self,
        name: &TableName,
        schema: &Schema,
        key: &[String],
    ) -> Result<TableEntry, Error> {
        if key.is_empty() {
            return Err(Error::Schema(format!(
                "a keyed table needs key columns; {name} has none"
            )));
        }
        lock(&self.catalog).create(&self.branch, name, schema, key, &[])
    }

    /// Creates `name` as an append table whose history sorts each date by the `sort` columns
    /// (§8.1), so reads that filter on them skip most of a date. Returns it if it exists with
    /// the same columns and sort.
    pub fn create_sorted_table(
        &self,
        name: &TableName,
        schema: &Schema,
        sort: &[String],
    ) -> Result<TableEntry, Error> {
        lock(&self.catalog).create(&self.branch, name, schema, &[], sort)
    }

    /// Every table, by name.
    pub fn tables(&self) -> Result<Vec<TableEntry>, Error> {
        lock(&self.catalog).list(&self.branch)
    }

    /// The store's directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The table called `name`.
    pub fn table(&self, name: &TableName) -> Result<TableEntry, Error> {
        lock(&self.catalog)
            .get(&self.branch, name)?
            .ok_or_else(|| Error::NotFound(format!("no table {name}; create it first")))
    }

    /// Every date of `name`'s history (§8), by date then file.
    pub fn history(&self, name: &TableName) -> Result<Vec<HistoryDate>, Error> {
        let id = self.table(name)?.id;
        lock(&self.catalog).history(id)
    }

    /// What every table takes, as JSON for the web UI's catalog (§15.2): per table, the rows
    /// and stream bytes of each date weir holds (today's, and past dates not yet dropped) and the
    /// rows, bytes, and files of each date of its history; and the store's volume, its size and
    /// the bytes free on it.
    pub fn stats_json(&self) -> Result<serde_json::Value, Error> {
        let mut tables = Vec::new();
        for entry in self.tables()? {
            let log = self.log(&entry.name)?;
            let mut weir: BTreeMap<String, (u64, u64)> = BTreeMap::new();
            for ((day, _), partition) in &lock(&log.state).partitions {
                let held = weir.entry(day.clone()).or_default();
                held.0 += partition.rows();
                held.1 += partition.cursor(0).bytes();
            }
            let mut history: BTreeMap<String, (u64, u64, u64)> = BTreeMap::new();
            for date in lock(&self.catalog).history(entry.id)? {
                let held = history.entry(date.date.to_string()).or_default();
                held.0 += date.rows;
                held.1 += date.bytes;
                held.2 += 1;
            }
            let columns: Vec<serde_json::Value> = entry
                .schema
                .fields()
                .iter()
                .map(|f| json!({"name": f.name(), "type": type_name(f.data_type())}))
                .collect();
            let weir: Vec<serde_json::Value> = weir
                .iter()
                .map(|(date, (rows, bytes))| json!({"date": date, "rows": rows, "bytes": bytes}))
                .collect();
            let history: Vec<serde_json::Value> = history
                .iter()
                .map(|(date, (rows, bytes, files))| {
                    json!({"date": date, "rows": rows, "bytes": bytes, "files": files})
                })
                .collect();
            tables.push(json!({
                "table": entry.name.to_string(),
                "kind": if entry.key.is_empty() { "append" } else { "keyed" },
                "key": entry.key,
                "sort": entry.sort,
                "columns": columns,
                "weir": weir,
                "history": history,
            }));
        }
        let volume = statvfs(&self.root).map_err(|e| {
            Error::Io(format!(
                "cannot read the volume of {}: {e}",
                self.root.display()
            ))
        })?;
        let unit = widen(volume.fragment_size());
        Ok(json!({
            "tables": tables,
            "volume": {
                "bytes": widen(volume.blocks()) * unit,
                "free": widen(volume.blocks_available()) * unit,
            },
        }))
    }

    /// The files of `name`'s history holding any date from `first` to `last`, by first date.
    pub fn history_files(
        &self,
        name: &TableName,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error> {
        let id = self.table(name)?.id;
        lock(&self.catalog).history_files(id, first, last)
    }

    /// Commits a write of `name`'s history in one transaction (§8.2). `Busy` when the history
    /// changed since the commit was planned: plan it again.
    pub fn commit_history(&self, name: &TableName, commit: &HistoryCommit) -> Result<(), Error> {
        let id = self.table(name)?.id;
        lock(&self.catalog).commit_history(id, commit, unix_seconds())
    }

    /// History files replaced at least `grace` ago, which no read can still be using.
    pub fn history_garbage(&self, grace: Duration) -> Result<Vec<Garbage>, Error> {
        lock(&self.catalog).garbage(unix_seconds() - grace.as_secs() as i64)
    }

    /// Forgets a garbage file once it is deleted.
    pub fn forget_garbage(&self, garbage: &Garbage) -> Result<(), Error> {
        lock(&self.catalog).forget_garbage(garbage)
    }

    /// A writer for `source`'s rows of `name`, holding the source's lease (§7.4) until it is
    /// dropped. One writer per source at a time: with `wait`, this waits for the current writer
    /// to go; without, it fails at once. Each lease has a new epoch, recorded with every batch.
    pub fn writer(&self, name: &TableName, source: &SourceId, wait: bool) -> Result<Writer, Error> {
        let log = self.log(name)?;
        let mut state = lock(&log.state);
        while state.writing.contains(source) {
            if !wait {
                return Err(Error::Busy(format!(
                    "{name} already has a writer for source `{source}`"
                )));
            }
            state = log
                .released
                .wait(state)
                .expect("kermodes store poisoned by an earlier panic");
        }
        state.writing.insert(source.clone());
        let entry = state.entry.clone();
        drop(state);
        // The source is ours from here: the epoch is taken outside the log's lock, so the
        // catalog's lock is never held inside it.
        // If taking the epoch fails, dropping this writer releases the source again.
        let mut writer = Writer {
            log,
            source: source.clone(),
            schema: entry.schema,
            name: entry.name,
            key: entry.key,
            epoch: 0,
        };
        writer.epoch = lock(&self.catalog).next_epoch(entry.id, source)?;
        Ok(writer)
    }

    /// `source`'s last sequence number for `name`, as its writer would resume from, and the
    /// epoch of its latest lease (`None` before its first writer).
    pub fn source_state(
        &self,
        name: &TableName,
        source: &SourceId,
    ) -> Result<(Option<u64>, Option<u64>), Error> {
        let log = self.log(name)?;
        let (table_id, last_seq) = {
            let state = lock(&log.state);
            (
                state.entry.id,
                newest(&state, source).and_then(Partition::last_seq),
            )
        };
        let epoch = lock(&self.catalog).epoch(table_id, source)?;
        Ok((last_seq, epoch))
    }

    /// Feeds `subscriber` every batch of `name` committed today (UTC), then each new one as it
    /// commits; returns the table's schema. A live table is intraday (§7.3): earlier days are
    /// history, read from the lake once merged, not replayed into every reader.
    ///
    /// The replay reads one batch at a time from disk, without the table's lock, in rounds until
    /// few are left; those it delivers under the lock before joining the writes. Memory is a
    /// batch, however large the day, and writers wait at most for those last few.
    pub fn subscribe(
        &self,
        name: &TableName,
        mut subscriber: Subscriber,
    ) -> Result<SchemaRef, Error> {
        let today = today();
        let log = self.log(name)?;
        // Where the replay starts in each partition: a keyed table's latest checkpoint and the
        // changes after it; every batch of an append table's.
        let mut delivered = {
            let mut state = lock(&log.state);
            ensure_checkpoint(&mut state, &today)?;
            starts(&state, &today)
        };
        loop {
            let state = lock(&log.state);
            let mut behind = Vec::new();
            let (mut left, mut left_bytes) = (0, 0);
            for (key, partition) in &state.partitions {
                let from = delivered.get(key).copied().unwrap_or(0);
                let committed = partition.committed();
                if key.0 >= today && committed > from {
                    let cursor = partition.cursor(from);
                    left += committed - from;
                    left_bytes += cursor.bytes();
                    behind.push((key.clone(), cursor, committed));
                }
            }
            let schema = state.entry.schema.clone();
            let under_lock = left <= CATCH_UP_UNDER_LOCK && left_bytes <= CATCH_UP_UNDER_LOCK_BYTES;
            // Held through the delivery when little is left, so no write comes between.
            let (held, pace) = if under_lock {
                (Some(state), Pace::MustNotWait)
            } else {
                drop(state);
                (None, Pace::MayWait)
            };
            let mut caught_up = Vec::with_capacity(behind.len());
            let mut cursors = Vec::with_capacity(behind.len());
            for (key, cursor, committed) in behind {
                caught_up.push((key, committed));
                cursors.push(cursor);
            }
            for read in in_order(cursors) {
                let (batch, stamp) = read?;
                if !subscriber(&batch, stamp, pace) {
                    return Ok(schema);
                }
            }
            if let Some(mut state) = held {
                state.subscribers.push(subscriber);
                return Ok(schema);
            }
            delivered.extend(caught_up);
        }
    }

    /// How many rows `name` holds today (UTC), and its rows `[start, start + count)` of them in
    /// commit order, as a replay delivers them, with `columns` only (every column when `None`).
    /// For viewers that show a table without holding it (§7.2). An append table's only: a keyed
    /// table's rows are its keys' latest values, which positions do not name.
    pub fn rows_at(
        &self,
        name: &TableName,
        start: u64,
        count: u64,
        columns: Option<&[String]>,
    ) -> Result<(u64, Vec<RecordBatch>), Error> {
        let today = today();
        let log = self.log(name)?;
        let mut guard = lock(&log.state);
        let state = &mut *guard;
        if !state.entry.key.is_empty() {
            return Err(Error::Schema(format!(
                "{name} is keyed: its rows are its keys' latest values, read by key, not position"
            )));
        }
        let projection =
            columns
                .map(|columns| {
                    columns
                        .iter()
                        .map(|column| {
                            state.entry.schema.index_of(column).map_err(|_| {
                                Error::Schema(format!("{name} has no column `{column}`"))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?;
        if state.positions.as_ref().is_none_or(|p| p.day != today) {
            let built = Positions::build(state, &today);
            state.positions = Some(built);
        }
        let positions = state.positions.as_ref().expect("built above");
        let total = positions.rows;
        let end = start.saturating_add(count).min(total);
        let mut rows = Vec::new();
        if start >= end {
            return Ok((total, rows));
        }
        // The last batch starting at or before `start`, then each one up to `end`, read as runs
        // of consecutive batches of one partition.
        let mut at = positions
            .batches
            .partition_point(|&(_, _, before)| before <= start)
            - 1;
        let mut runs: Vec<(u32, u32, u32, u64)> = Vec::new();
        while at < positions.batches.len() && positions.batches[at].2 < end {
            let (stream, index, before) = positions.batches[at];
            match runs.last_mut() {
                Some(run) if run.0 == stream && run.1 + run.2 == index => run.2 += 1,
                _ => runs.push((stream, index, 1, before)),
            }
            at += 1;
        }
        for (stream, first, count, mut before) in runs {
            let key = (today.clone(), positions.streams[stream as usize].clone());
            let partition = state
                .partitions
                .get(&key)
                .expect("a position's partition is held");
            for batch in partition.read_batches(first as usize, count as usize)? {
                let len = batch.num_rows() as u64;
                let from = start.max(before) - before;
                let to = end.min(before + len).saturating_sub(before);
                before += len;
                if from >= to {
                    continue;
                }
                let batch = batch.slice(from as usize, (to - from) as usize);
                rows.push(match &projection {
                    Some(columns) => batch
                        .project(columns)
                        .map_err(|e| Error::Arrow(e.to_string()))?,
                    None => batch,
                });
            }
        }
        Ok((total, rows))
    }

    /// The past dates weir still holds rows of for `name`: not yet merged, or merged and not yet
    /// dropped. Reads of history take them from weir, so it has no gap (§8.4).
    pub fn past_dates(&self, name: &TableName) -> Result<Vec<Date>, Error> {
        let log = self.log(name)?;
        let state = lock(&log.state);
        let mut dates: Vec<Date> = state
            .partitions
            .iter()
            .filter(|(_, partition)| partition.rows() > 0)
            .filter_map(|((day, _), _)| Date::parse(day).ok())
            .filter(|&date| past(date).is_ok())
            .collect();
        dates.dedup();
        Ok(dates)
    }

    /// The rows and stream bytes weir holds of a past `date` of `name`, over every source.
    pub fn date_rows(&self, name: &TableName, date: Date) -> Result<(u64, u64), Error> {
        past(date)?;
        let log = self.log(name)?;
        let state = lock(&log.state);
        let key = date.to_string();
        Ok(state
            .partitions
            .iter()
            .filter(|((day, _), _)| *day == key)
            .fold((0, 0), |(rows, bytes), (_, partition)| {
                (rows + partition.rows(), bytes + partition.cursor(0).bytes())
            }))
    }

    /// Every batch of a past `date` of `name` with its change kind, to `each`, in the order they
    /// were committed. Nothing writes a past date, so this reads without holding up writers.
    pub fn read_date(
        &self,
        name: &TableName,
        date: Date,
        each: &mut dyn FnMut(RecordBatch, Stamp) -> Result<(), Error>,
    ) -> Result<(), Error> {
        past(date)?;
        let log = self.log(name)?;
        let key = date.to_string();
        let cursors: Vec<_> = lock(&log.state)
            .partitions
            .iter()
            .filter(|((day, _), _)| *day == key)
            .map(|(_, partition)| partition.cursor(0))
            .collect();
        for read in in_order(cursors) {
            let (batch, stamp) = read?;
            each(batch, stamp)?;
        }
        Ok(())
    }

    /// Deletes weir's partitions of a past `date` of `name`, once its history holds the date
    /// whole (as many rows as weir has): weir never deletes rows its history does not hold.
    pub fn drop_date(&self, name: &TableName, date: Date) -> Result<(), Error> {
        past(date)?;
        let in_history: u64 = self
            .history(name)?
            .iter()
            .filter(|held| held.date == date)
            .map(|held| held.rows)
            .sum();
        let log = self.log(name)?;
        let mut state = lock(&log.state);
        // A keyed table's state must not depend on the date going: today's checkpoint holds it.
        ensure_checkpoint(&mut state, &Date::of_ns(now_ns()).to_string())?;
        let key = date.to_string();
        let rows: u64 = state
            .partitions
            .iter()
            .filter(|((day, _), _)| *day == key)
            .map(|(_, partition)| partition.rows())
            .sum();
        if rows != in_history {
            return Err(Error::Catalog(format!(
                "weir holds {rows} rows of {name} on {date} and its history {in_history}: \
                 merge the date before dropping it"
            )));
        }
        state.partitions.retain(|(day, _), _| *day != key);
        let dir = state.dir.join(&key);
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(&dir, e)),
        }
    }

    /// The table's log, opened (and its partitions recovered) on first use.
    fn log(&self, name: &TableName) -> Result<Arc<Log>, Error> {
        let entry = self.table(name)?;
        let mut logs = lock(&self.logs);
        if let Some(log) = logs.get(&entry.id) {
            return Ok(log.clone());
        }
        let dir = self.root.join("weir").join(&entry.location);
        let mut partitions = BTreeMap::new();
        for (date, stream) in partition_dirs(&dir)? {
            let path = dir.join(&date).join(stream.dir_name());
            partitions.insert(
                (date, stream),
                Partition::open(&path, entry.schema.clone())?,
            );
        }
        let clock = partitions
            .values()
            .filter_map(Partition::last_ingest_ns)
            .max()
            .unwrap_or(0);
        let log = Arc::new(Log {
            state: Mutex::new(TableLog {
                entry: entry.clone(),
                dir,
                partitions,
                subscribers: Vec::new(),
                writing: HashSet::new(),
                clock,
                since_checkpoint: 0,
                checkpoint_bytes: 0,
                positions: None,
            }),
            released: Condvar::new(),
        });
        logs.insert(entry.id, log.clone());
        Ok(log)
    }
}

/// Writes one source's rows of a table, holding the source's lease (§7.4) until dropped. Each
/// batch lands in the partition for the UTC date it arrives (its ingest time) and is durable
/// before it returns: an append table's rows by `write`, a keyed table's by `upsert` and
/// `delete`.
pub struct Writer {
    log: Arc<Log>,
    source: SourceId,
    schema: SchemaRef,
    name: TableName,
    /// A keyed table's key columns; empty for an append table.
    key: Vec<String>,
    epoch: u64,
}

impl Writer {
    /// The source's last sequence number: of its newest partition, since sequence numbers rise
    /// within a partition and a writer resumes from where its source left off.
    pub fn last_seq(&self) -> Option<u64> {
        let state = lock(&self.log.state);
        newest(&state, &self.source).and_then(Partition::last_seq)
    }

    /// This writer's lease: above every earlier writer's of the source, and recorded with each
    /// batch it writes.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Appends `batch` (columns matched by name to the table's) as `seq`, or the next number after
    /// the last; `false` when `seq` was already written (a resend), which writes nothing.
    pub fn write(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        if !self.key.is_empty() {
            return Err(Error::Schema(format!(
                "{} is a keyed table: upsert or delete its rows",
                self.name
            )));
        }
        self.append(&conform(&self.schema, batch)?, seq, Change::Rows)
    }

    /// Sets each row's key to that row, as `write` does for an append table.
    pub fn upsert(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.keyed("upsert")?;
        self.append(&conform(&self.schema, batch)?, seq, Change::Upsert)
    }

    /// Removes the keys of `batch`'s rows; it needs only the key columns.
    pub fn delete(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.keyed("delete")?;
        self.append(
            &keys_only(&self.schema, &self.key, batch)?,
            seq,
            Change::Delete,
        )
    }

    /// Replaces the table's state with `batch`: every key not in it is gone (a checkpoint). A
    /// publisher starts this way, so keys it no longer holds do not outlive its restart; a state
    /// larger than one batch follows on in upserts.
    pub fn replace(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.keyed("replace")?;
        self.append(&conform(&self.schema, batch)?, seq, Change::Checkpoint)
    }

    fn keyed(&self, action: &str) -> Result<(), Error> {
        if self.key.is_empty() {
            return Err(Error::Schema(format!(
                "{} is an append table: write its rows; only keyed tables {action}",
                self.name
            )));
        }
        Ok(())
    }

    fn append(&self, batch: &RecordBatch, seq: Option<u64>, change: Change) -> Result<bool, Error> {
        let mut guard = lock(&self.log.state);
        let log = &mut *guard;
        // The day's opening checkpoint is stamped before the write that brings it, then the
        // write: under the lock, so ingest times rise in commit order across sources. Twice only
        // if the clock passed midnight between.
        let ingest_ns = loop {
            let date = Date::of_ns(now_ns().max(log.clock + 1));
            ensure_checkpoint(log, &date.to_string())?;
            let ingest_ns = log.tick();
            if Date::of_ns(ingest_ns) == date {
                break ingest_ns;
            }
        };
        let date = Date::of_ns(ingest_ns).to_string();
        let seq = match seq {
            Some(seq) => seq,
            None => newest(log, &self.source)
                .and_then(Partition::last_seq)
                .map_or(1, |last| last + 1),
        };
        let key = (date, Stream::Source(self.source.clone()));
        if !log.partitions.contains_key(&key) {
            let path = log.dir.join(&key.0).join(key.1.dir_name());
            let partition = Partition::open(&path, log.entry.schema.clone())?;
            log.partitions.insert(key.clone(), partition);
        }
        let partition = log.partitions.get_mut(&key).expect("inserted above");
        if !partition.append(batch, seq, ingest_ns, self.epoch, change)? {
            return Ok(false);
        }
        let committed = partition.committed();
        if let Some(positions) = log.positions.as_mut().filter(|p| p.day == key.0) {
            positions.push(&key.1, committed - 1, batch.num_rows() as u64);
        }
        let stamp = Stamp { change, ingest_ns };
        log.subscribers
            .retain_mut(|subscriber| subscriber(batch, stamp, Pace::MustNotWait));
        if !log.entry.key.is_empty() {
            log.since_checkpoint += batch.get_array_memory_size();
            // Past the last checkpoint's own size too, so folding costs in proportion to the
            // changes written however large the state.
            if log.since_checkpoint >= CHECKPOINT_BYTES.max(log.checkpoint_bytes) {
                let mut fold = Fold::new(&log.entry.schema, &log.entry.key)?;
                fold_latest(log, &key.0, &mut fold)?;
                write_checkpoint(log, &key.0, &fold.state()?)?;
            }
        }
        Ok(true)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        lock(&self.log.state).writing.remove(&self.source);
        self.log.released.notify_all();
    }
}

/// Creates `root` if needed and takes its lock: one process owns a store directory.
fn lock_dir(root: &Path) -> Result<File, Error> {
    fs::create_dir_all(root).map_err(|e| Error::io(root, e))?;
    let lock_path = root.join("lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|e| Error::io(&lock_path, e))?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(TryLockError::WouldBlock) => Err(Error::Busy(format!(
            "{} is open in another process; to share it, serve it with `kermodes dev` and \
             connect to that",
            root.display()
        ))),
        Err(TryLockError::Error(e)) => Err(Error::io(&lock_path, e)),
    }
}

fn newest<'a>(log: &'a TableLog, source: &SourceId) -> Option<&'a Partition> {
    log.partitions
        .iter()
        .rev()
        .find(|((_, stream), _)| *stream == Stream::Source(source.clone()))
        .map(|(_, partition)| partition)
}

/// What a partition holds within a date: a keyed table's checkpoint, or one source's writes.
/// The checkpoint sorts first, so it is replayed first.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stream {
    Checkpoint,
    Source(SourceId),
}

impl Stream {
    /// `@` is in no source's name, so the checkpoint's directory cannot be a source's.
    fn dir_name(&self) -> &str {
        match self {
            Stream::Checkpoint => "@checkpoint",
            Stream::Source(source) => source.as_str(),
        }
    }
}

/// `{dir}/{date}/{stream}` directories that exist.
fn partition_dirs(dir: &Path) -> Result<Vec<(String, Stream)>, Error> {
    let mut found = Vec::new();
    let Ok(dates) = fs::read_dir(dir) else {
        return Ok(found);
    };
    for date in dates {
        let date = date.map_err(|e| Error::io(dir, e))?;
        let date_name = date.file_name().to_string_lossy().into_owned();
        for stream in fs::read_dir(date.path()).map_err(|e| Error::io(&date.path(), e))? {
            let stream = stream.map_err(|e| Error::io(&date.path(), e))?;
            let name = stream.file_name().to_string_lossy().into_owned();
            let stream = match name.as_str() {
                "@checkpoint" => Stream::Checkpoint,
                name => Stream::Source(SourceId::parse(name)?),
            };
            found.push((date_name.clone(), stream));
        }
    }
    Ok(found)
}

/// Gives a keyed table's day `today` its checkpoint, unless it has one: every key's latest row,
/// folded from the latest earlier day weir holds (§7.3). Under the table's lock, since nothing
/// may write the day before its checkpoint; it happens on the day's first write or
/// subscription, or before an earlier day is dropped.
fn ensure_checkpoint(log: &mut TableLog, today: &str) -> Result<(), Error> {
    let opening = (today.to_owned(), Stream::Checkpoint);
    if log.entry.key.is_empty()
        || log
            .partitions
            .get(&opening)
            .is_some_and(|partition| partition.committed() > 0)
    {
        return Ok(());
    }
    let mut fold = Fold::new(&log.entry.schema, &log.entry.key)?;
    let earlier = log
        .partitions
        .keys()
        .map(|(day, _)| day)
        .filter(|day| day.as_str() < today)
        .max()
        .cloned();
    if let Some(day) = earlier {
        fold_latest(log, &day, &mut fold)?;
    }
    write_checkpoint(log, today, &fold.state()?)
}

/// Appends `state` to `day`'s checkpoints. Subscribers are not told: a reader already following
/// the table has every row it repeats.
fn write_checkpoint(log: &mut TableLog, day: &str, state: &RecordBatch) -> Result<(), Error> {
    let ingest_ns = log.tick();
    let key = (day.to_owned(), Stream::Checkpoint);
    if !log.partitions.contains_key(&key) {
        let path = log.dir.join(day).join(Stream::Checkpoint.dir_name());
        let partition = Partition::open(&path, log.entry.schema.clone())?;
        log.partitions.insert(key.clone(), partition);
    }
    let partition = log.partitions.get_mut(&key).expect("inserted above");
    let seq = partition.last_seq().map_or(1, |last| last + 1);
    partition.append(state, seq, ingest_ns, 0, Change::Checkpoint)?;
    log.since_checkpoint = 0;
    log.checkpoint_bytes = state.get_array_memory_size();
    Ok(())
}

/// Folds `day` of a keyed table as it stands: its latest checkpoint, then the changes of its
/// sources committed after it, in commit order.
fn fold_latest(log: &TableLog, day: &str, fold: &mut Fold) -> Result<(), Error> {
    let cursors = latest(log, day)
        .into_iter()
        .map(|(key, from)| log.partitions[&key].cursor(from));
    for read in in_order(cursors.collect::<Vec<_>>()) {
        let (batch, stamp) = read?;
        fold.apply(batch, stamp.change)?;
    }
    Ok(())
}

/// Where `day`'s state starts in each of its partitions: the latest checkpoint, and in each
/// source the first change committed after it (all of a day without one).
fn latest(log: &TableLog, day: &str) -> Vec<((String, Stream), usize)> {
    let checkpoint = log
        .partitions
        .get(&(day.to_owned(), Stream::Checkpoint))
        .filter(|partition| partition.committed() > 0);
    let after = checkpoint
        .and_then(Partition::last_ingest_ns)
        .unwrap_or(i64::MIN);
    log.partitions
        .iter()
        .filter(|((date, _), _)| date == day)
        .map(|(key, partition)| {
            let from = match key.1 {
                Stream::Checkpoint => partition.committed().saturating_sub(1),
                Stream::Source(_) => partition.at_or_before(after),
            };
            (key.clone(), from)
        })
        .collect()
}

/// Where a subscription's replay of `today` starts in each partition: a keyed table's state as
/// of its latest checkpoint (`latest`), an append table's every batch.
fn starts(log: &TableLog, today: &str) -> BTreeMap<(String, Stream), usize> {
    if log.entry.key.is_empty() {
        return BTreeMap::new();
    }
    latest(log, today).into_iter().collect()
}

/// `batch` with the table's columns, matched by name: the same names, each of the same type.
pub fn conform(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let incoming = batch.schema();
    if incoming.fields().len() != schema.fields().len() {
        return Err(Error::Schema(format!(
            "expected columns ({}), got ({})",
            names(schema),
            names(&incoming)
        )));
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let index = incoming.index_of(field.name()).map_err(|_| {
                Error::Schema(format!(
                    "missing column `{}`; expected ({})",
                    field.name(),
                    names(schema)
                ))
            })?;
            let given = incoming.field(index).data_type();
            if given != field.data_type() {
                return Err(Error::Schema(format!(
                    "column `{}` is {given}, expected {}",
                    field.name(),
                    field.data_type()
                )));
            }
            Ok(batch.column(index).clone())
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::Schema(e.to_string()))
}

/// A delete's batch with the table's columns: its key columns as given (matched by name), every
/// other column null.
pub fn keys_only(
    schema: &SchemaRef,
    key: &[String],
    batch: &RecordBatch,
) -> Result<RecordBatch, Error> {
    let incoming = batch.schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| match incoming.index_of(field.name()) {
            Ok(index) if incoming.field(index).data_type() == field.data_type() => {
                Ok(batch.column(index).clone())
            }
            Ok(index) => Err(Error::Schema(format!(
                "column `{}` is {}, expected {}",
                field.name(),
                incoming.field(index).data_type(),
                field.data_type()
            ))),
            Err(_) if key.contains(field.name()) => Err(Error::Schema(format!(
                "a delete needs the key column `{}`",
                field.name()
            ))),
            Err(_) => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::Schema(e.to_string()))
}

fn names(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("kermodes store poisoned by an earlier panic")
}

/// `statvfs`'s counts as u64: 32 bits on some platforms (macOS), 64 on others (Linux).
fn widen(n: impl Into<u64>) -> u64 {
    n.into()
}

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

/// Today's UTC date, the day live readers are given.
fn today() -> String {
    Date::of_ns(now_ns()).to_string()
}

/// Today's date is still being written: only a past one is read whole, merged, or dropped.
fn past(date: Date) -> Result<(), Error> {
    let today = Date::of_ns(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i64),
    );
    if date >= today {
        return Err(Error::Busy(format!(
            "{date} is still being written: only past dates are merged"
        )));
    }
    Ok(())
}
