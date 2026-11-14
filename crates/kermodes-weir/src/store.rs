//! A store directory (architecture.md §16.6): the catalog, and weir's log of every table's
//! partitions, owned by one process at a time.
//!
//! ```text
//! {root}/lock                 held by the owning process
//! {root}/catalog.sqlite       the catalog
//! {root}/weir/{location}/{date}/{source}/seg-N.{arrows,idx}
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::catalog::{Catalog, Garbage, HistoryCommit, HistoryDate, HistoryFile, TableEntry};
use crate::partition::Partition;
use crate::{Date, Error, SourceId, TableName};

/// The branch a local store's tables live on (architecture.md §10.1).
pub const BRANCH: &str = "local";

/// Receives a table's batches in order: every batch committed today (UTC) when it subscribes,
/// then each new one as it commits. Returning `false` unsubscribes it (its reader is gone, or
/// too far behind). `Pace` says whether it may wait for its reader before returning.
pub type Subscriber = Box<dyn FnMut(&RecordBatch, Pace) -> bool + Send>;

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
    /// By date, then source: the order a subscriber receives existing batches in.
    partitions: BTreeMap<(String, SourceId), Partition>,
    subscribers: Vec<Subscriber>,
    /// Sources whose lease a `Writer` holds: one writer per source (§7.4).
    writing: HashSet<SourceId>,
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
        lock(&self.catalog).create(&self.branch, name, schema)
    }

    /// Every table, by name.
    pub fn tables(&self) -> Result<Vec<TableEntry>, Error> {
        lock(&self.catalog).list(&self.branch)
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
        let (table_id, schema) = (state.entry.id, state.entry.schema.clone());
        drop(state);
        // The source is ours from here: the epoch is taken outside the log's lock, so the
        // catalog's lock is never held inside it.
        // If taking the epoch fails, dropping this writer releases the source again.
        let mut writer = Writer {
            log,
            source: source.clone(),
            schema,
            epoch: 0,
        };
        writer.epoch = lock(&self.catalog).next_epoch(table_id, source)?;
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
        let today = Date::of_ns(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as i64),
        )
        .to_string();
        let log = self.log(name)?;
        // Batches delivered so far, by partition.
        let mut delivered: BTreeMap<(String, SourceId), usize> = BTreeMap::new();
        loop {
            let mut state = lock(&log.state);
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
            if left <= CATCH_UP_UNDER_LOCK && left_bytes <= CATCH_UP_UNDER_LOCK_BYTES {
                for (_, cursor, _) in behind {
                    if !cursor.read(|batch| subscriber(&batch, Pace::MustNotWait))? {
                        return Ok(schema);
                    }
                }
                state.subscribers.push(subscriber);
                return Ok(schema);
            }
            drop(state);
            for (key, cursor, committed) in behind {
                if !cursor.read(|batch| subscriber(&batch, Pace::MayWait))? {
                    return Ok(schema);
                }
                delivered.insert(key, committed);
            }
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
        for (date, source) in partition_dirs(&dir)? {
            let path = dir.join(&date).join(source.as_str());
            partitions.insert(
                (date, source),
                Partition::open(&path, entry.schema.clone())?,
            );
        }
        let log = Arc::new(Log {
            state: Mutex::new(TableLog {
                entry: entry.clone(),
                dir,
                partitions,
                subscribers: Vec::new(),
                writing: HashSet::new(),
            }),
            released: Condvar::new(),
        });
        logs.insert(entry.id, log.clone());
        Ok(log)
    }
}

/// Writes one source's rows of a table, holding the source's lease (§7.4) until dropped. Each
/// batch lands in the partition for the UTC date it arrives (its ingest time) and is durable
/// before `write` returns.
pub struct Writer {
    log: Arc<Log>,
    source: SourceId,
    schema: SchemaRef,
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
        let batch = conform(&self.schema, batch)?;
        let ingest_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i64);
        let date = Date::of_ns(ingest_ns).to_string();
        let mut guard = lock(&self.log.state);
        let log = &mut *guard;
        let seq = match seq {
            Some(seq) => seq,
            None => newest(log, &self.source)
                .and_then(Partition::last_seq)
                .map_or(1, |last| last + 1),
        };
        let key = (date, self.source.clone());
        if !log.partitions.contains_key(&key) {
            let path = log.dir.join(&key.0).join(self.source.as_str());
            let partition = Partition::open(&path, log.entry.schema.clone())?;
            log.partitions.insert(key.clone(), partition);
        }
        let partition = log.partitions.get_mut(&key).expect("inserted above");
        if !partition.append(&batch, seq, ingest_ns, self.epoch)? {
            return Ok(false);
        }
        log.subscribers
            .retain_mut(|subscriber| subscriber(&batch, Pace::MustNotWait));
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
        .find(|((_, s), _)| s == source)
        .map(|(_, partition)| partition)
}

/// `{dir}/{date}/{source}` directories that exist.
fn partition_dirs(dir: &Path) -> Result<Vec<(String, SourceId)>, Error> {
    let mut found = Vec::new();
    let Ok(dates) = fs::read_dir(dir) else {
        return Ok(found);
    };
    for date in dates {
        let date = date.map_err(|e| Error::io(dir, e))?;
        let date_name = date.file_name().to_string_lossy().into_owned();
        for source in fs::read_dir(date.path()).map_err(|e| Error::io(&date.path(), e))? {
            let source = source.map_err(|e| Error::io(&date.path(), e))?;
            let name = source.file_name().to_string_lossy().into_owned();
            found.push((date_name.clone(), SourceId::parse(&name)?));
        }
    }
    Ok(found)
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

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}
