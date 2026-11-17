//! A store directory (architecture.md §16.6): the catalog, and weir's log of every table's
//! partitions, owned by one process at a time.
//!
//! ```text
//! {root}/lock                 held by the owning process
//! {root}/catalog.sqlite       the catalog
//! {root}/weir/log/            the shard log (`wal`): every write, in order, durable when acknowledged
//! {root}/weir/{location}/{date}/{source}/seg-N.{arrows,idx}           derived from the log
//! {root}/weir/{location}/{date}/@checkpoint/seg-N.{arrows,idx}    a keyed table's day opening
//! ```
//!
//! Writes go to one committer thread, which appends every write waiting to the log under one
//! fsync, then applies them in log order: to partition files, keyed checkpoints, and
//! subscribers, then acknowledges each. A flusher thread fsyncs the partition files every
//! second, records how far they hold the log, and has the log below that point deleted. Opening
//! the store replays the log from there.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::new_null_array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use kermodes_core::type_name;
use nix::sys::statvfs::statvfs;
use serde_json::json;

use crate::catalog::{Catalog, Garbage, HistoryCommit, HistoryDate, HistoryFile, TableEntry};
use crate::keyed::Fold;
use crate::partition::{Partition, Stamp, in_order};
use crate::pending::{Answer, InFlight, Pending};
use crate::shape::Shape;
use crate::wal::{self, Append, Logged, Wal};
use crate::{Change, Date, Error, SourceId, TableName};

/// The branch a local store's tables live on (architecture.md §10.1).
pub const BRANCH: &str = "local";

/// How often the partition files are flushed and the log below them deleted. Writes are durable
/// in the log when acknowledged, so this bounds only how much log a restart replays.
const FLUSH_EVERY: Duration = Duration::from_secs(1);

/// The most the committer gathers into one fsync: whatever is waiting, up to this.
const GROUP_BYTES: usize = 64 << 20;

/// The longest a group waits for writers that were in the last one (`commit`).
const GATHER: Duration = Duration::from_micros(100);

/// Receives a table's batches in order, each with its stamp (change kind and ingest time): every
/// batch committed today (UTC) when it subscribes (a keyed table's starting with its
/// checkpoint), then each new one as it commits. Empty batches too: an empty checkpoint clears a
/// keyed table. Returning `false` unsubscribes it (its reader is gone, or too far behind).
/// `Pace` says whether it may wait for its reader before returning.
pub type Subscriber = Box<dyn FnMut(&RecordBatch, Stamp, Pace) -> bool + Send>;

/// Told why a subscription ended when the store ends it: the table's columns changed in a way
/// its reader's cannot follow (a column it reads was widened, §5.4).
pub type Failed = Box<dyn FnOnce(String) + Send>;

/// A subscriber, and the columns it reads: the table's when it subscribed, so a reader keeps its
/// columns while the table's change.
struct Subscription {
    deliver: Subscriber,
    shape: Shape,
    failed: Option<Failed>,
}

impl Subscription {
    /// Delivers `batch`, of `written`'s columns; `false` once the subscription is over.
    fn deliver(&mut self, written: &Shape, batch: &RecordBatch, stamp: Stamp) -> bool {
        match written.convert(batch.clone(), &self.shape) {
            Ok(batch) => (self.deliver)(&batch, stamp, Pace::MustNotWait),
            Err(e) => {
                if let Some(failed) = self.failed.take() {
                    failed(e.to_string());
                }
                false
            }
        }
    }
}

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
    logs: Arc<Mutex<HashMap<i64, Arc<Log>>>>,
    /// To the committer thread, which logs and applies every write.
    requests: Sender<Request>,
    /// The last LSN applied to the partition files (not yet necessarily flushed).
    applied: Arc<AtomicU64>,
    /// The log's directory, where the flushed position is recorded.
    log_dir: PathBuf,
    committer: Option<JoinHandle<()>>,
    flusher: Option<(Arc<Stop>, JoinHandle<()>)>,
}

/// Tells the flusher to stop: set, then notified.
type Stop = (Mutex<bool>, Condvar);

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
    subscribers: Vec<Subscription>,
    /// Sources whose lease a `Writer` holds: one writer per source (§7.4).
    writing: HashSet<SourceId>,
    /// The last LSN applied to this table: what a checkpoint folded now holds.
    last_lsn: u64,
    /// The oldest position a subscription may resume after: the table's batches up to it may
    /// have gone with dates merged and dropped.
    resumable_from: u64,
    /// A keyed table's: bytes of changes written since its last checkpoint, and that
    /// checkpoint's size (unknown, so 0, after a restart).
    since_checkpoint: usize,
    checkpoint_bytes: usize,
    /// An append table's day in commit order, once a read by position has asked for it.
    positions: Option<Positions>,
}

impl TableLog {
    /// What the next flush must fsync of this table's partitions.
    fn unsynced(&mut self) -> Result<Vec<File>, Error> {
        let mut files = Vec::new();
        for partition in self.partitions.values_mut() {
            files.extend(partition.unsynced()?);
        }
        Ok(files)
    }
}

/// What the committer thread is asked.
enum Request {
    Write(Submitted),
    /// The partition files hold the log up to this LSN: the segments wholly below it, to delete.
    Trim(u64, SyncSender<Vec<PathBuf>>),
    Stop,
}

/// A write to log and apply, and where its outcome goes: written, or a resend (`false`).
struct Submitted {
    log: Arc<Log>,
    table: i64,
    /// The columns `batch` has: the table's when it was submitted.
    shape: Shape,
    source: SourceId,
    seq: u64,
    epoch: u64,
    change: Change,
    batch: RecordBatch,
    answer: Answer,
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
            for (index, (rows, order)) in partition.batch_rows().enumerate() {
                batches.push((order, at, index as u32, rows));
            }
        }
        // In commit order (`Stamp::order`); ties, from before the log, go to the earlier
        // partition, as a replay's do.
        batches.sort_unstable_by_key(|&(order, at, index, _)| (order, at, index));
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
        Self::new(root, lock, catalog, BRANCH)
    }

    /// Opens the store in `root` with its catalog in Postgres at `url`, as the cluster's weir:
    /// the tables of `branch`, which every client of that catalog sees alike, and lease epochs
    /// that rise across all of them. Fails if another process has `root` open.
    pub fn open_with_catalog(root: &Path, url: &str, branch: &str) -> Result<Self, Error> {
        let lock = lock_dir(root)?;
        let catalog = Catalog::postgres(url)?;
        Self::new(root, lock, catalog, branch)
    }

    /// Recovers every table's partitions as far as their files hold the log, replays the log
    /// from there, and starts the committer and the flusher.
    fn new(root: &Path, lock: File, mut catalog: Catalog, branch: &str) -> Result<Self, Error> {
        let weir = root.join("weir");
        let log_dir = weir.join("log");
        fs::create_dir_all(&log_dir).map_err(|e| Error::io(&log_dir, e))?;
        let flushed = wal::read_applied(&log_dir)?;
        let mut logs = HashMap::new();
        let mut clock = 0;
        for entry in catalog.list(branch)? {
            let log = open_log(&weir, entry, flushed, flushed)?;
            let id = {
                let state = lock_log(&log);
                clock = clock.max(last_ingest(&state));
                state.entry.id
            };
            logs.insert(id, log);
        }
        let wal = Wal::open(&log_dir, flushed, &mut |logged: Logged| {
            let log = logs.get(&logged.table).ok_or_else(|| {
                Error::Corrupt(format!(
                    "the log holds a write of table {}, which the catalog does not",
                    logged.table
                ))
            })?;
            clock = clock.max(logged.ingest_ns);
            apply(log, logged).map(drop)
        })?;
        let logs = Arc::new(Mutex::new(logs));
        let applied = Arc::new(AtomicU64::new(wal.next_lsn() - 1));
        // The replay's writes reach the disk now, so another crash need not replay them.
        flush(&logs, &log_dir, &applied)?;
        let (requests, received) = mpsc::channel();
        let committer = {
            let applied = applied.clone();
            std::thread::Builder::new()
                .name("kermodes-weir-commit".into())
                .spawn(move || commit(wal, clock, &applied, &received))
                .map_err(|e| Error::Io(format!("cannot start weir's committer: {e}")))?
        };
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let flusher = {
            let (logs, log_dir, applied, requests, stop) = (
                logs.clone(),
                log_dir.clone(),
                applied.clone(),
                requests.clone(),
                stop.clone(),
            );
            std::thread::Builder::new()
                .name("kermodes-weir-flush".into())
                .spawn(move || flush_every(&logs, &log_dir, &applied, &requests, &stop))
                .map_err(|e| Error::Io(format!("cannot start weir's flusher: {e}")))?
        };
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
            catalog: Mutex::new(catalog),
            branch: branch.to_owned(),
            logs,
            requests,
            applied,
            log_dir,
            committer: Some(committer),
            flusher: Some((stop, flusher)),
        })
    }

    /// Creates `name` with `schema`, or brings it to `schema`'s columns if it exists (a
    /// compatible change, §5.4) and returns it.
    pub fn create_table(&self, name: &TableName, schema: &Schema) -> Result<TableEntry, Error> {
        let entry = lock(&self.catalog).create(&self.branch, name, schema, &[], &[])?;
        self.adopt(&entry)?;
        Ok(entry)
    }

    /// Creates `name` as a keyed table (§5.2): one row per value of the `key` columns, written as
    /// upserts and deletes. Brings it to `schema`'s columns if it exists with the same key.
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
        let entry = lock(&self.catalog).create(&self.branch, name, schema, key, &[])?;
        self.adopt(&entry)?;
        Ok(entry)
    }

    /// Creates `name` as an append table whose history sorts each date by the `sort` columns
    /// (§8.1), so reads that filter on them skip most of a date. Brings it to `schema`'s
    /// columns if it exists with the same sort.
    pub fn create_sorted_table(
        &self,
        name: &TableName,
        schema: &Schema,
        sort: &[String],
    ) -> Result<TableEntry, Error> {
        let entry = lock(&self.catalog).create(&self.branch, name, schema, &[], sort)?;
        self.adopt(&entry)?;
        Ok(entry)
    }

    /// Takes up `entry`'s columns in its table's log, if it is open and they changed: segments
    /// written from now on hold them, and writers' rows are brought to them. Readers and
    /// writers already open keep their own.
    fn adopt(&self, entry: &TableEntry) -> Result<(), Error> {
        let Some(log) = lock(&self.logs).get(&entry.id).cloned() else {
            return Ok(());
        };
        let mut state = lock(&log.state);
        if state.entry.shape() == entry.shape() {
            return Ok(());
        }
        state.entry = entry.clone();
        for partition in state.partitions.values_mut() {
            partition.reshape(entry.shape())?;
        }
        Ok(())
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
            requests: self.requests.clone(),
            source: source.clone(),
            shape: entry.shape(),
            name: entry.name.clone(),
            key: entry.key,
            epoch: 0,
            last_seq: Mutex::new(None),
            in_flight: Arc::default(),
            gone: format!("the store holding {} closed", entry.name).into(),
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
    /// commits, in the table's columns as they are now, whatever they become; returns them. A
    /// live table is intraday (§7.3): earlier days are history, read from the lake once merged,
    /// not replayed into every reader. `failed` is told if the subscription has to end because
    /// the table's columns changed beyond what these can read.
    ///
    /// The replay reads one batch at a time from disk, without the table's lock, in rounds until
    /// few are left; those it delivers under the lock before joining the writes. Memory is a
    /// batch, however large the day, and writers wait at most for those last few.
    pub fn subscribe(
        &self,
        name: &TableName,
        subscriber: Subscriber,
        failed: Failed,
    ) -> Result<SchemaRef, Error> {
        self.follow(name, None, subscriber, failed)
    }

    /// As `subscribe`, for a reader that has every batch up to the log position `after`
    /// (`Stamp::lsn`): the table's batches after it, whatever their date, then each new one;
    /// a keyed table's checkpoints are not repeated. `Gone` if batches after it may have gone
    /// with a date merged and dropped: the reader must start again.
    pub fn resume(
        &self,
        name: &TableName,
        after: u64,
        subscriber: Subscriber,
        failed: Failed,
    ) -> Result<SchemaRef, Error> {
        self.follow(name, Some(after), subscriber, failed)
    }

    fn follow(
        &self,
        name: &TableName,
        after: Option<u64>,
        mut subscriber: Subscriber,
        failed: Failed,
    ) -> Result<SchemaRef, Error> {
        let today = today();
        let log = self.log(name)?;
        // Where the replay starts in each partition: a keyed table's latest checkpoint and the
        // changes after it, and every batch of an append table's; or, resuming, the first batch
        // after the reader's position.
        let (mut delivered, shape) = {
            let mut state = lock(&log.state);
            let delivered = match after {
                None => {
                    ensure_checkpoint(&mut state, &today, now_ns())?;
                    starts(&state, &today)
                }
                Some(after) if after < state.resumable_from => {
                    return Err(Error::Gone(format!(
                        "{name}'s rows after position {after} are no longer all held (merged \
                         dates are dropped): read the table again"
                    )));
                }
                Some(after) => state
                    .partitions
                    .iter()
                    .map(|(key, partition)| {
                        let from = match key.1 {
                            Stream::Checkpoint => partition.committed(),
                            Stream::Source(_) => partition.at_or_before((after, i64::MAX)),
                        };
                        (key.clone(), from)
                    })
                    .collect(),
            };
            (delivered, state.entry.shape())
        };
        loop {
            let state = lock(&log.state);
            let mut behind = Vec::new();
            let (mut left, mut left_bytes) = (0, 0);
            for (key, partition) in &state.partitions {
                let from = delivered.get(key).copied().unwrap_or(0);
                let committed = partition.committed();
                if (after.is_some() || key.0 >= today) && committed > from {
                    let cursor = partition.cursor(from);
                    left += committed - from;
                    left_bytes += cursor.bytes();
                    behind.push((key.clone(), cursor, committed));
                }
            }
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
            for read in in_order(cursors, &shape) {
                let (batch, stamp) = read?;
                if !subscriber(&batch, stamp, pace) {
                    return Ok(shape.schema);
                }
            }
            if let Some(mut state) = held {
                let schema = shape.schema.clone();
                state.subscribers.push(Subscription {
                    deliver: subscriber,
                    shape,
                    failed: Some(failed),
                });
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
            let shape = state.entry.shape();
            for batch in partition.read_batches(first as usize, count as usize, &shape)? {
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
        let (cursors, shape): (Vec<_>, _) = {
            let state = lock(&log.state);
            let cursors = state
                .partitions
                .iter()
                .filter(|((day, _), _)| *day == key)
                .map(|(_, partition)| partition.cursor(0))
                .collect();
            (cursors, state.entry.shape())
        };
        for read in in_order(cursors, &shape) {
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
        ensure_checkpoint(&mut state, &Date::of_ns(now_ns()).to_string(), now_ns())?;
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
        // A reader resuming from before the date's last batch would miss what goes.
        let gone = state
            .partitions
            .iter()
            .filter(|((day, stream), _)| *day == key && *stream != Stream::Checkpoint)
            .filter_map(|(_, partition)| partition.last_order())
            .map(|(lsn, _)| lsn)
            .max()
            .unwrap_or(0);
        state.resumable_from = state.resumable_from.max(gone);
        state.partitions.retain(|(day, _), _| *day != key);
        let dir = state.dir.join(&key);
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(&dir, e)),
        }
    }

    /// The table's log: opened when the store was, or (a table created since) on first use.
    fn log(&self, name: &TableName) -> Result<Arc<Log>, Error> {
        let entry = self.table(name)?;
        let mut logs = lock(&self.logs);
        if let Some(log) = logs.get(&entry.id) {
            return Ok(log.clone());
        }
        let id = entry.id;
        // Created since the store opened: no partitions yet, and nothing of it in the log.
        let log = open_log(&self.root.join("weir"), entry, u64::MAX, 0)?;
        logs.insert(id, log.clone());
        Ok(log)
    }
}

impl Drop for Store {
    /// Stops the flusher, then the committer (writes already sent are logged and answered,
    /// later ones refused), then flushes what was applied, so reopening replays nothing.
    fn drop(&mut self) {
        if let Some((stop, flusher)) = self.flusher.take() {
            let (stopped, wake) = &*stop;
            *lock(stopped) = true;
            wake.notify_all();
            let _ = flusher.join();
        }
        let _ = self.requests.send(Request::Stop);
        if let Some(committer) = self.committer.take() {
            let _ = committer.join();
        }
        if let Err(e) = flush(&self.logs, &self.log_dir, &self.applied) {
            eprintln!("kermodes weir: closing, cannot flush partition files: {e}");
        }
    }
}

/// A table's log: its partitions recovered as far as their files hold the log, `trusted`. With
/// no partitions it may resume readers only from `floor` on: whatever it held before may have
/// gone with dates merged and dropped.
fn open_log(weir: &Path, entry: TableEntry, trusted: u64, floor: u64) -> Result<Arc<Log>, Error> {
    let dir = weir.join(&entry.location);
    let mut partitions = BTreeMap::new();
    for (date, stream) in partition_dirs(&dir)? {
        let path = dir.join(&date).join(stream.dir_name());
        partitions.insert(
            (date, stream),
            Partition::open(&path, entry.shape(), trusted)?,
        );
    }
    let last_lsn = partitions
        .values()
        .filter_map(Partition::last_order)
        .map(|(lsn, _)| lsn)
        .max()
        .unwrap_or(0);
    // Its sources' batches below the first it holds may have gone with a dropped date, unless
    // it still holds batches from before the log (position 0): then nothing with a position
    // ever went.
    let sources = || {
        partitions
            .iter()
            .filter(|((_, stream), _)| *stream != Stream::Checkpoint)
            .map(|(_, partition)| partition)
    };
    let resumable_from = if sources().any(|p| p.at_or_before((0, i64::MAX)) > 0) {
        0
    } else {
        sources()
            .filter_map(Partition::first_lsn)
            .min()
            .map_or(floor, |first| first - 1)
    };
    Ok(Arc::new(Log {
        state: Mutex::new(TableLog {
            entry,
            dir,
            partitions,
            subscribers: Vec::new(),
            writing: HashSet::new(),
            last_lsn,
            resumable_from,
            since_checkpoint: 0,
            checkpoint_bytes: 0,
            positions: None,
        }),
        released: Condvar::new(),
    }))
}

fn lock_log(log: &Log) -> MutexGuard<'_, TableLog> {
    lock(&log.state)
}

/// The newest ingest time a table's partitions hold.
fn last_ingest(log: &TableLog) -> i64 {
    log.partitions
        .values()
        .filter_map(Partition::last_order)
        .map(|(_, ingest_ns)| ingest_ns)
        .max()
        .unwrap_or(0)
}

/// Applies a logged write to its table: the partition of its date and source (or, a resend,
/// nothing), the table's readers, and a keyed table's checkpoints. Whether it was written.
fn apply(log: &Log, logged: Logged) -> Result<bool, Error> {
    let mut guard = lock(&log.state);
    let state = &mut *guard;
    let date = Date::of_ns(logged.ingest_ns).to_string();
    // A keyed table's day opens with its checkpoint, before any change of the day.
    ensure_checkpoint(state, &date, logged.ingest_ns)?;
    // Logged in the columns the table had when it was written; applied in today's (§5.4).
    let current = state.entry.shape();
    let batch = logged.shape.convert(logged.batch, &current)?;
    let key = (date, Stream::Source(logged.source));
    if !state.partitions.contains_key(&key) {
        let path = state.dir.join(&key.0).join(key.1.dir_name());
        let partition = Partition::open(&path, current.clone(), u64::MAX)?;
        state.partitions.insert(key.clone(), partition);
    }
    let partition = state.partitions.get_mut(&key).expect("inserted above");
    let written = partition.append(
        &batch,
        logged.lsn,
        logged.seq,
        logged.ingest_ns,
        logged.epoch,
        logged.change,
    )?;
    if !written {
        return Ok(false);
    }
    state.last_lsn = logged.lsn;
    let committed = partition.committed();
    if let Some(positions) = state.positions.as_mut().filter(|p| p.day == key.0) {
        positions.push(&key.1, committed - 1, batch.num_rows() as u64);
    }
    let stamp = Stamp {
        change: logged.change,
        ingest_ns: logged.ingest_ns,
        lsn: logged.lsn,
    };
    state
        .subscribers
        .retain_mut(|subscription| subscription.deliver(&current, &batch, stamp));
    if !state.entry.key.is_empty() {
        state.since_checkpoint += batch.get_array_memory_size();
        // Past the last checkpoint's own size too, so folding costs in proportion to the
        // changes written however large the state.
        if state.since_checkpoint >= CHECKPOINT_BYTES.max(state.checkpoint_bytes) {
            let mut fold = Fold::new(&state.entry.schema, &state.entry.key)?;
            fold_latest(state, &key.0, &mut fold)?;
            write_checkpoint(state, &key.0, &fold.state()?, logged.ingest_ns)?;
        }
    }
    Ok(true)
}

/// The committer thread: takes every write waiting (up to `GROUP_BYTES`), stamps each with
/// the next ingest time (rising across every table, so dates and commit order agree), logs them
/// under one fsync, applies them in log order, and answers each. A failure to log or apply
/// leaves what is durable in the log, to replay on the next open, and refuses every write after
/// it: the partition files no longer follow the log.
///
/// Writers answered together come back together, a few microseconds apart: a group waits up to
/// `GATHER` for as many writes as the last one held, so they share the next fsync instead of
/// alternating between groups of one and the rest. A lone writer never waits.
fn commit(mut wal: Wal, mut clock: i64, applied: &AtomicU64, requests: &Receiver<Request>) {
    let mut broken: Option<String> = None;
    let mut stop = false;
    let mut last_group = 1;
    while !stop {
        let Ok(first) = requests.recv() else {
            return;
        };
        let gather_until = Instant::now() + GATHER;
        let mut group = Vec::new();
        let mut bytes = 0;
        let mut next = Some(first);
        while let Some(request) = next.take() {
            match request {
                Request::Write(submitted) => {
                    bytes += submitted.batch.get_array_memory_size();
                    group.push(submitted);
                }
                Request::Trim(flushed, reply) => {
                    let _ = reply.send(wal.trim(flushed));
                }
                Request::Stop => stop = true,
            }
            if !stop && bytes < GROUP_BYTES {
                next = requests.try_recv().ok().or_else(|| {
                    let left = gather_until.checked_duration_since(Instant::now())?;
                    (group.len() < last_group)
                        .then(|| requests.recv_timeout(left).ok())
                        .flatten()
                });
            }
        }
        if group.is_empty() {
            continue;
        }
        last_group = group.len();
        if let Some(why) = &broken {
            for submitted in group {
                submitted.answer.send(Err(Error::Io(format!(
                    "weir stopped taking writes: {why}; restart it"
                ))));
            }
            continue;
        }
        let units = merge(group, &mut clock);
        let appends: Vec<Append> = units
            .iter()
            .map(|unit| Append {
                table: unit.table,
                shape: &unit.shape,
                source: &unit.source,
                seq: unit.seq,
                epoch: unit.epoch,
                change: unit.change,
                ingest_ns: unit.ingest_ns,
                batch: &unit.batch,
            })
            .collect();
        let lsns = match wal.append(&appends) {
            Ok(lsns) => lsns,
            Err(e) => {
                broken = Some(e.to_string());
                for unit in units {
                    Answer::send_all(unit.answers, Err(Error::Io(e.to_string())));
                }
                continue;
            }
        };
        drop(appends);
        for (unit, lsn) in units.into_iter().zip(lsns) {
            let Unit {
                log,
                table,
                shape,
                source,
                seq,
                epoch,
                change,
                ingest_ns,
                batch,
                answers,
            } = unit;
            let outcome = match &broken {
                Some(why) => Err(Error::Io(format!(
                    "weir stopped taking writes: {why}; restart it"
                ))),
                None => {
                    let logged = Logged {
                        lsn,
                        table,
                        shape,
                        source,
                        seq,
                        epoch,
                        change,
                        ingest_ns,
                        batch,
                    };
                    let outcome = apply(&log, logged);
                    match &outcome {
                        Ok(_) => applied.store(lsn, Ordering::Release),
                        Err(e) => broken = Some(e.to_string()),
                    }
                    outcome
                }
            };
            Answer::send_all(answers, outcome);
        }
    }
}

/// Writes logged and applied as one batch: one write, or adjacent new writes of one source of a
/// table (`merge`).
struct Unit {
    log: Arc<Log>,
    table: i64,
    shape: Shape,
    source: SourceId,
    /// The last write's: a resend is recognised by `seq <= last`, so the unit stands for every
    /// write in it.
    seq: u64,
    epoch: u64,
    change: Change,
    ingest_ns: i64,
    batch: RecordBatch,
    answers: Vec<Answer>,
}

/// Stamps each write of a group with the next ingest time and merges the new writes of each
/// source of a table, of the same change, date, and columns, into one batch: what writing them
/// one by one would leave, at the cost of one log entry, one index record, and one delivery to
/// each reader. Producers sending many small writes so have them batched by the load, with no
/// batch size or linger to choose, however many write the table at once.
///
/// What stays ordered (§7.1): each writer's writes (one source of one table: a run ends at a
/// write of the source that cannot join it), and a checkpoint against every write of its table
/// (it ends every run of the table), which replaces all of them. Writes in one group were all
/// sent before any of them was answered, so any other order is one they could have arrived in.
/// A unit is logged where its last write was, stamped with that write's time, so ingest times
/// still rise with log position. A resend stays alone. Should joining a unit's batches fail,
/// its writes fail and nothing of them is logged.
fn merge(group: Vec<Submitted>, clock: &mut i64) -> Vec<Unit> {
    struct Open {
        unit: Submitted,
        more: Vec<RecordBatch>,
        answers: Vec<Answer>,
        ingest_ns: i64,
        date: Date,
        bytes: usize,
        /// The group position of its last write: where it is logged.
        at: usize,
    }
    // Each source's last seq per table and date, as the group's writes before this one leave it.
    let mut known: HashMap<(i64, SourceId, Date), Option<u64>> = HashMap::new();
    // Each source's unit still taking writes, per table.
    let mut runs: HashMap<(i64, SourceId), usize> = HashMap::new();
    let mut open: Vec<Open> = Vec::new();
    // One reading of the clock for the group, which is logged at one instant: its writes are
    // stamped a nanosecond apart from there.
    let now = now_ns();
    for (at, submitted) in group.into_iter().enumerate() {
        *clock = now.max(*clock + 1);
        let date = Date::of_ns(*clock);
        let last = known
            .entry((submitted.table, submitted.source.clone(), date))
            .or_insert_with(|| {
                lock(&submitted.log.state)
                    .partitions
                    .get(&(date.to_string(), Stream::Source(submitted.source.clone())))
                    .and_then(Partition::last_seq)
            });
        let resend = last.is_some_and(|last| submitted.seq <= last);
        if !resend {
            *last = Some(submitted.seq);
        }
        let bytes = submitted.batch.get_array_memory_size();
        let run = (submitted.table, submitted.source.clone());
        if submitted.change == Change::Checkpoint {
            runs.retain(|(table, _), _| *table != submitted.table);
        }
        let joins = runs.get(&run).map(|&i| &open[i]).is_some_and(|previous| {
            !resend
                && previous.unit.epoch == submitted.epoch
                && previous.unit.change == submitted.change
                && previous.date == date
                && previous.unit.shape == submitted.shape
                && previous.bytes + bytes <= GROUP_BYTES
        });
        if joins {
            let previous = &mut open[runs[&run]];
            let Submitted {
                seq, batch, answer, ..
            } = submitted;
            previous.unit.seq = seq;
            previous.more.push(batch);
            previous.answers.push(answer);
            previous.ingest_ns = *clock;
            previous.bytes += bytes;
            previous.at = at;
            continue;
        }
        if resend || submitted.change == Change::Checkpoint {
            runs.remove(&run);
        } else {
            runs.insert(run, open.len());
        }
        open.push(Open {
            unit: submitted,
            more: Vec::new(),
            answers: Vec::new(),
            ingest_ns: *clock,
            date,
            bytes,
            at,
        });
    }
    open.sort_unstable_by_key(|open| open.at);
    open.into_iter()
        .filter_map(|open| {
            let Submitted {
                log,
                table,
                shape,
                source,
                seq,
                epoch,
                change,
                batch,
                answer,
            } = open.unit;
            let mut answers = open.answers;
            answers.insert(0, answer);
            let batch = if open.more.is_empty() {
                batch
            } else {
                let schema = batch.schema();
                let batches = std::iter::once(&batch).chain(&open.more);
                match arrow::compute::concat_batches(&schema, batches) {
                    Ok(batch) => batch,
                    Err(e) => {
                        Answer::send_all(answers, Err(Error::Arrow(e.to_string())));
                        return None;
                    }
                }
            };
            Some(Unit {
                log,
                table,
                shape,
                source,
                seq,
                epoch,
                change,
                ingest_ns: open.ingest_ns,
                batch,
                answers,
            })
        })
        .collect()
}

/// Fsyncs every partition file written so far, then records that the files hold the log up to
/// what was applied before: what the next open replays from. That position.
fn flush(
    logs: &Mutex<HashMap<i64, Arc<Log>>>,
    log_dir: &Path,
    applied: &AtomicU64,
) -> Result<u64, Error> {
    let through = applied.load(Ordering::Acquire);
    let open: Vec<Arc<Log>> = lock(logs).values().cloned().collect();
    let mut files = Vec::new();
    for log in open {
        files.extend(lock(&log.state).unsynced()?);
    }
    // Outside every lock: writers and readers go on meanwhile.
    for file in files {
        file.sync_all()
            .map_err(|e| Error::Io(format!("cannot sync a partition file: {e}")))?;
    }
    wal::write_applied(log_dir, through)?;
    Ok(through)
}

/// The flusher thread: flushes every `FLUSH_EVERY`, then has the log below the flushed point
/// deleted, until stopped. A flush that fails is not retried: the files may have lost what the
/// failed fsync held, so the log is kept whole, to replay on the next open.
fn flush_every(
    logs: &Mutex<HashMap<i64, Arc<Log>>>,
    log_dir: &Path,
    applied: &AtomicU64,
    requests: &Sender<Request>,
    stop: &(Mutex<bool>, Condvar),
) {
    loop {
        let (stopped, wake) = stop;
        let guard = lock(stopped);
        let (guard, _) = wake
            .wait_timeout_while(guard, FLUSH_EVERY, |stopped| !*stopped)
            .expect("kermodes store poisoned by an earlier panic");
        if *guard {
            return;
        }
        drop(guard);
        let flushed = match flush(logs, log_dir, applied) {
            Ok(flushed) => flushed,
            Err(e) => {
                eprintln!(
                    "kermodes weir: cannot flush partition files ({e}); the log keeps every \
                     write until weir restarts"
                );
                return;
            }
        };
        let (reply, answer) = mpsc::sync_channel(1);
        if requests.send(Request::Trim(flushed, reply)).is_err() {
            return;
        }
        for path in answer.recv().unwrap_or_default() {
            if let Err(e) = fs::remove_file(&path) {
                eprintln!("kermodes weir: cannot delete {}: {e}", path.display());
            }
        }
    }
}

/// Writes one source's rows of a table, holding the source's lease (§7.4) until dropped. Each
/// batch lands in the partition for the UTC date it arrives (its ingest time): an append table's
/// rows by `write`, a keyed table's by `upsert` and `delete`. Those return once the batch is
/// durable; `send` returns at once, so many writes can be in flight and share an fsync.
pub struct Writer {
    log: Arc<Log>,
    requests: Sender<Request>,
    source: SourceId,
    /// The table's columns when the writer opened: its rows are matched to these, then brought
    /// to the table's columns as they are when written.
    shape: Shape,
    name: TableName,
    /// A keyed table's key columns; empty for an append table.
    key: Vec<String>,
    epoch: u64,
    /// The highest seq this writer has sent: the next unnumbered write's is one above. Its lock
    /// also keeps the writer's writes in order when shared between threads.
    last_seq: Mutex<Option<u64>>,
    in_flight: Arc<InFlight>,
    /// What a write says if the store closes before answering it.
    gone: Arc<str>,
}

impl Writer {
    /// The source's last sequence number written: of its newest partition, since sequence
    /// numbers rise within a partition and a writer resumes from where its source left off.
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
        self.send(Change::Rows, batch, seq)?.wait()
    }

    /// Sets each row's key to that row, as `write` does for an append table.
    pub fn upsert(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(Change::Upsert, batch, seq)?.wait()
    }

    /// Removes the keys of `batch`'s rows; it needs only the key columns.
    pub fn delete(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(Change::Delete, batch, seq)?.wait()
    }

    /// Replaces the table's state with `batch`: every key not in it is gone (a checkpoint). A
    /// publisher starts this way, so keys it no longer holds do not outlive its restart; a state
    /// larger than one batch follows on in upserts.
    pub fn replace(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(Change::Checkpoint, batch, seq)?.wait()
    }

    /// Sends `batch` as a `change` (as `write`, `upsert`, `delete`, or `replace` would) without
    /// waiting for it: its `Pending` says when it is durable. A writer's writes are logged in
    /// the order sent; sending waits only while `IN_FLIGHT_BYTES` of them are unanswered.
    pub fn send(
        &self,
        change: Change,
        batch: &RecordBatch,
        seq: Option<u64>,
    ) -> Result<Pending, Error> {
        let batch = match change {
            Change::Rows if !self.key.is_empty() => {
                return Err(Error::Schema(format!(
                    "{} is a keyed table: upsert or delete its rows",
                    self.name
                )));
            }
            Change::Rows => conform(&self.shape.schema, batch)?,
            Change::Upsert => {
                self.keyed("upsert")?;
                conform(&self.shape.schema, batch)?
            }
            Change::Delete => {
                self.keyed("delete")?;
                keys_only(&self.shape.schema, &self.key, batch)?
            }
            Change::Checkpoint => {
                self.keyed("replace")?;
                conform(&self.shape.schema, batch)?
            }
        };
        let mut last = lock(&self.last_seq);
        let (current, table, seq) = {
            let state = lock(&self.log.state);
            let seq = seq.unwrap_or_else(|| {
                last.or_else(|| newest(&state, &self.source).and_then(Partition::last_seq))
                    .map_or(1, |last| last + 1)
            });
            (state.entry.shape(), state.entry.id, seq)
        };
        // Rows of the columns this writer opened with, as the table's are now (§5.4).
        let batch = self.shape.convert(batch, &current)?;
        let (pending, answer) = self
            .in_flight
            .send(batch.get_array_memory_size(), &self.gone);
        self.requests
            .send(Request::Write(Submitted {
                log: self.log.clone(),
                table,
                shape: current,
                source: self.source.clone(),
                seq,
                epoch: self.epoch,
                change,
                batch,
                answer,
            }))
            .map_err(|_| Error::Io(self.gone.to_string()))?;
        *last = Some(last.map_or(seq, |last| last.max(seq)));
        Ok(pending)
    }

    /// Waits until every write sent is durable or refused; the first that failed since the
    /// last flush, as an error, whether or not its `Pending` was waited for.
    pub fn flush(&self) -> Result<(), Error> {
        self.in_flight.flush()
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
fn ensure_checkpoint(log: &mut TableLog, today: &str, ingest_ns: i64) -> Result<(), Error> {
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
    write_checkpoint(log, today, &fold.state()?, ingest_ns)
}

/// Appends `state`, the table as of the last LSN applied to it, to `day`'s checkpoints.
/// Subscribers are not told: a reader already following the table has every row it repeats.
fn write_checkpoint(
    log: &mut TableLog,
    day: &str,
    state: &RecordBatch,
    ingest_ns: i64,
) -> Result<(), Error> {
    let key = (day.to_owned(), Stream::Checkpoint);
    if !log.partitions.contains_key(&key) {
        let path = log.dir.join(day).join(Stream::Checkpoint.dir_name());
        let partition = Partition::open(&path, log.entry.shape(), u64::MAX)?;
        log.partitions.insert(key.clone(), partition);
    }
    let partition = log.partitions.get_mut(&key).expect("inserted above");
    let seq = partition.last_seq().map_or(1, |last| last + 1);
    partition.append(state, log.last_lsn, seq, ingest_ns, 0, Change::Checkpoint)?;
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
    for read in in_order(cursors.collect::<Vec<_>>(), &log.entry.shape()) {
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
        .and_then(Partition::last_order)
        .unwrap_or((0, i64::MIN));
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
