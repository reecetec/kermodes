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
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::catalog::{Catalog, TableEntry};
use crate::partition::Partition;
use crate::{Error, SourceId, TableName};

/// The branch a local store's tables live on (architecture.md §10.1).
pub const BRANCH: &str = "local";

/// Receives a table's batches in order: every committed batch when it subscribes, then each new
/// one as it commits. Returning `false` unsubscribes it (its table is gone).
pub type Subscriber = Box<dyn FnMut(&RecordBatch) -> bool + Send>;

pub struct Store {
    root: PathBuf,
    /// Held for the store's life: the OS releases the lock when the process ends, however.
    _lock: File,
    catalog: Mutex<Catalog>,
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
    /// Opens the store in `root`, creating it if needed. Fails if another process has it open.
    pub fn open(root: &Path) -> Result<Self, Error> {
        fs::create_dir_all(root).map_err(|e| Error::io(root, e))?;
        let lock_path = root.join("lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| Error::io(&lock_path, e))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(Error::Busy(format!(
                    "{} is open in another process; to share it, serve it with `kermodes dev` \
                     and connect to that",
                    root.display()
                )));
            }
            Err(TryLockError::Error(e)) => return Err(Error::io(&lock_path, e)),
        }
        let catalog = Catalog::open(&root.join("catalog.sqlite"))?;
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
            catalog: Mutex::new(catalog),
            logs: Mutex::new(HashMap::new()),
        })
    }

    /// Creates `name` with `schema`, or returns it if it exists with the same columns.
    pub fn create_table(&self, name: &TableName, schema: &Schema) -> Result<TableEntry, Error> {
        lock(&self.catalog).create(BRANCH, name, schema)
    }

    /// Every table, by name.
    pub fn tables(&self) -> Result<Vec<TableEntry>, Error> {
        lock(&self.catalog).list(BRANCH)
    }

    /// The table called `name`.
    pub fn table(&self, name: &TableName) -> Result<TableEntry, Error> {
        lock(&self.catalog)
            .get(BRANCH, name)?
            .ok_or_else(|| Error::NotFound(format!("no table {name}; create it first")))
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

    /// Feeds `subscriber` every committed batch of `name`, then each new one as it commits;
    /// returns the table's schema.
    pub fn subscribe(
        &self,
        name: &TableName,
        mut subscriber: Subscriber,
    ) -> Result<SchemaRef, Error> {
        let log = self.log(name)?;
        let mut log = lock(&log.state);
        for partition in log.partitions.values() {
            for batch in partition.batches()? {
                if !subscriber(&batch) {
                    return Ok(log.entry.schema.clone());
                }
            }
        }
        log.subscribers.push(subscriber);
        Ok(log.entry.schema.clone())
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
        let date = utc_date(ingest_ns);
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
        log.subscribers.retain_mut(|subscriber| subscriber(&batch));
        Ok(true)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        lock(&self.log.state).writing.remove(&self.source);
        self.log.released.notify_all();
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

/// `YYYY-MM-DD` of a UTC instant in nanoseconds since the epoch.
fn utc_date(ns: i64) -> String {
    // Days to civil date (Howard Hinnant's `civil_from_days`).
    let days = ns.div_euclid(86_400_000_000_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("kermodes store poisoned by an earlier panic")
}

#[cfg(test)]
mod tests {
    use super::utc_date;

    #[test]
    fn utc_dates_follow_the_calendar() {
        let day = 86_400_000_000_000_i64;
        assert_eq!(utc_date(0), "1970-01-01");
        assert_eq!(utc_date(-1), "1969-12-31");
        assert_eq!(utc_date(1_767_225_600 * 1_000_000_000), "2026-01-01");
        // 2024 is a leap year; 2100 is not.
        assert_eq!(utc_date(19_782 * day), "2024-02-29");
        assert_eq!(utc_date(47_541 * day), "2100-03-01");
    }
}
