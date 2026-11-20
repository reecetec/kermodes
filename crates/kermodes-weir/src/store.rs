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
//! Writes go to one committer thread, which proposes every write waiting to Raft as one group
//! and persists Raft's entries in the log under one fsync; once a quorum of the shard's replicas
//! holds them (on its own, a shard of one), it applies them in log order: to partition files,
//! keyed checkpoints, and subscribers, then acknowledges each. Every replica applies the same
//! entries in the same order. A flusher thread fsyncs the partition files every second, records
//! how far they hold the log, and has the log below that point deleted. Opening the store
//! replays the log from there.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::new_null_array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use kermodes_core::type_name;
use raft::eraftpb::{
    ConfChangeSingle, ConfChangeType, ConfChangeV2, ConfState, Entry, EntryType, Message,
    MessageType, Snapshot,
};
use raft::protocompat::PbMessage;
use raft::{ProgressState, RawNode, SnapshotStatus, StateRole, Storage};
use serde_json::json;

use crate::catalog::{Catalog, Garbage, HistoryCommit, HistoryDate, HistoryFile, TableEntry};
use crate::disk::{self, DiskRef};
use crate::keyed::Fold;
use crate::partition::{Partition, Stamp, ToSync, Written, in_order, schema_header};
use crate::pending::{Answer, InFlight, Pending};
use crate::replica::{
    LogLimits, LogStore, Promotion, Replica, promotion, replica_id, replica_name, slot, transferee,
};
use crate::shape::Shape;
use crate::snapshot::{self, SnapshotSource, SnapshotWanted};
use crate::wal::{self, Append, Command, Logged, Wal};
use crate::{Change, Date, Error, SourceId, TableName};

/// The branch a local store's tables live on (architecture.md §10.1).
pub const BRANCH: &str = "local";

/// Where the store's disk records a failed fsync, with the boot it failed on
/// (`Disk::untrusted`).
const FSYNC_FAILED: &str = "weir/fsync-failed";

/// How often the partition files are flushed and the log below them deleted. Writes are durable
/// in the log when acknowledged, so this bounds only how much log a restart replays.
const FLUSH_EVERY: Duration = Duration::from_secs(1);

/// Raft's clock on a replica with its own threads: a leader's heartbeat every tick, and an
/// election once a follower hears from no leader for 10 to 20 ticks, 1 to 2 s.
const TICK: Duration = Duration::from_millis(100);

/// The most the committer gathers into one fsync: whatever is waiting, up to this.
const GROUP_BYTES: usize = 64 << 20;

/// The longest a group waits for writers that were in the last one (`commit`).
const GATHER: Duration = Duration::from_micros(100);

/// The most a leader keeps in memory of entries its followers have yet to take: past it, a
/// follower behind reads them back from the log.
const CACHE_BYTES: u64 = 64 << 20;

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

/// A table's file of its sources' last seqs on dropped dates (`read_floors`), and its format.
/// `@` is in no date, so it cannot be taken for one.
const FLOORS: &str = "@sources";
const FLOORS_HEADER: &str = "kermodes-weir-sources 1";

pub struct Store {
    root: PathBuf,
    /// Held for the store's life: the OS releases the lock when the process ends, however.
    _lock: File,
    catalog: Arc<Mutex<Catalog>>,
    /// The catalog branch whose tables this store holds (§10.1).
    branch: String,
    disk: DiskRef,
    logs: Arc<Mutex<HashMap<i64, Arc<Log>>>>,
    /// To the committer, which proposes, logs, and applies every write.
    requests: Sender<Request>,
    flusher: Arc<Flusher>,
    /// The last entry applied, its index and term (`Committer::position`).
    position: Arc<Mutex<(u64, u64)>>,
    clock: Clock,
    /// This replica's Raft ID, and what its committer says of the shard.
    replica_id: u64,
    shard: Arc<Shard>,
    /// The committer's messages to other replicas, until whoever carries them takes them
    /// (`take_messages`, `take_outgoing`).
    outgoing: Mutex<Option<Outgoing>>,
    driver: Driver,
}

/// A replica's messages for the other replicas of its shard: each one encoded, with the ID of the
/// replica it is for.
pub type Outgoing = Receiver<(u64, Vec<u8>)>;

/// Reads the time, in nanoseconds since the Unix epoch: the system's, or a simulation's.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// The system's clock.
pub fn system_clock() -> Clock {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i64)
    })
}

/// The machine's disk, as the store in `root` uses it: where a replica installs a snapshot.
pub fn machine_disk(root: &Path) -> DiskRef {
    disk::real(&root.join(FSYNC_FAILED))
}

/// What commits a store's writes and flushes its files.
enum Driver {
    /// Its own threads: the committer, which gathers each group of writes, the flusher, and on
    /// a replica of others, Raft's clock.
    Threads {
        committer: Option<JoinHandle<()>>,
        flusher: Option<(Arc<Stop>, JoinHandle<()>)>,
        ticker: Option<JoinHandle<()>>,
    },
    /// Whoever steps it (`Store::step`, `Store::flush_now`), and nothing else: a simulation,
    /// which must choose the order of everything that happens.
    Stepped {
        committer: Box<Mutex<Committer>>,
        received: Mutex<Receiver<Request>>,
    },
}

/// Tells the flusher and the ticker to stop: set, then notified.
type Stop = (Mutex<bool>, Condvar);

/// What the committer says of its shard, for the store's callers.
#[derive(Default)]
struct Shard {
    /// The leader's Raft ID, as this replica knows it: 0 while it knows of none.
    leader: AtomicU64,
    /// Whether this replica is among the shard's members, a voter or a learner: one that joins
    /// asks the leader until it is (`Store::join`).
    member: AtomicBool,
    /// Whether this replica votes: a member made a learner first is made a voter once it has
    /// caught up (`promotion`).
    voter: AtomicBool,
    /// The shard log's commit index, as this replica knows it (`Store::caught_up`).
    committed: AtomicU64,
    /// The snapshot this replica must install (`Store::snapshot_wanted`).
    wanted: Mutex<Option<SnapshotWanted>>,
    /// The shard's members as of the last entry applied, read with it (`Store::members`).
    members: Mutex<ConfState>,
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
    disk: DiskRef,
    /// The store's directory, durably there: a partition syncs the directories down from it.
    root: PathBuf,
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
    /// Each source's last seq on the dates weir has dropped (`@sources`): its partitions held it.
    floors: HashMap<SourceId, u64>,
    /// Each source's latest lease this replica applied: its epoch, the index of its entry.
    leases: HashMap<SourceId, u64>,
}

impl TableLog {
    /// What the next flush must fsync of this table's partitions.
    fn unsynced(&mut self) -> Result<Vec<ToSync>, Error> {
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
    Drop(Dropping),
    Lease(Leasing),
    /// The partition files hold the log up to this LSN: the segments wholly below it, to delete.
    Trim(u64, SyncSender<Vec<PathBuf>>),
    Stop,
    /// A message from another replica of the shard (`Store::receive`).
    Receive(Message),
    /// One tick of Raft's clock (`TICK`).
    Tick,
    /// Hand the lead to another replica (`Store::hand_over`).
    HandOver,
    /// Take a replica into the shard (`Store::join`).
    Join(u64),
}

/// A past date of a table to drop, its history holding it whole, and where the outcome goes.
struct Dropping {
    log: Arc<Log>,
    table: i64,
    date: Date,
    /// The rows its history holds: the date goes only if it still holds as many.
    rows: u64,
    /// The clock as it was asked: what today is, whose keyed checkpoint takes the date's state.
    now_ns: i64,
    answer: Answer,
}

/// A lease on a table's source to take (§7.4), and where the outcome goes: its entry
/// committed, its epoch the entry's index (`TableLog::leases`).
struct Leasing {
    log: Arc<Log>,
    table: i64,
    source: SourceId,
    answer: Answer,
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
        Self::new(
            root,
            lock,
            catalog,
            BRANCH,
            machine_disk(root),
            system_clock(),
            Replica::alone(),
            false,
        )
    }

    /// Opens the store in `root` with its catalog in Postgres at `url`, as the cluster's weir:
    /// the tables of `branch`, which every client of that catalog sees alike, and lease epochs
    /// that rise across all of them. Fails if another process has `root` open.
    pub fn open_with_catalog(root: &Path, url: &str, branch: &str) -> Result<Self, Error> {
        let lock = lock_dir(root)?;
        let catalog = Catalog::postgres(url)?;
        Self::new(
            root,
            lock,
            catalog,
            branch,
            machine_disk(root),
            system_clock(),
            Replica::alone(),
            false,
        )
    }

    /// As `open_with_catalog`, as `replica` of its shard: every replica shares the catalog,
    /// which is how a follower finds the tables the leader's writes name. Its threads commit,
    /// flush, and keep Raft's clock; its messages to other replicas are for the carrier beside
    /// it (`take_outgoing`, `receive`). Only the leader takes writes; one too far behind its
    /// shard's log stops and says what to copy (`snapshot_wanted`).
    pub fn open_shard(
        root: &Path,
        url: &str,
        branch: &str,
        replica: Replica,
    ) -> Result<Self, Error> {
        let lock = lock_dir(root)?;
        let catalog = Catalog::postgres(url)?;
        Self::new(
            root,
            lock,
            catalog,
            branch,
            machine_disk(root),
            system_clock(),
            replica,
            false,
        )
    }

    /// As `open`, with its files on `disk`, time read from `clock`, and no threads of its own:
    /// writes are committed by `step` and files flushed by `flush_now`, when the caller says.
    /// A simulation runs a store this way, choosing the order of everything that happens; a
    /// writer's `send` is answered by the next `step`, so its `write` (which waits) must not be
    /// used. The lock and the catalog stay in `root` on the machine's disk.
    pub fn open_stepped(root: &Path, disk: DiskRef, clock: Clock) -> Result<Self, Error> {
        let lock = lock_dir(root)?;
        let catalog = Catalog::sqlite(&root.join("catalog.sqlite"))?;
        Self::new(
            root,
            lock,
            catalog,
            BRANCH,
            disk,
            clock,
            Replica::alone(),
            true,
        )
    }

    /// As `open_stepped`, as `replica` of its shard, with the catalog every replica shares in
    /// SQLite at `catalog`. Its Raft clock goes on as `tick` says and its messages to other
    /// replicas go as whoever carries them says (`take_messages`, `receive`): a simulation runs
    /// a shard's replicas this way. Only the leader takes writes (`is_leader`).
    pub fn open_replica(
        root: &Path,
        disk: DiskRef,
        clock: Clock,
        catalog: &Path,
        replica: Replica,
    ) -> Result<Self, Error> {
        let lock = lock_dir(root)?;
        let catalog = Catalog::sqlite(catalog)?;
        Self::new(root, lock, catalog, BRANCH, disk, clock, replica, true)
    }

    /// Recovers every table's partitions as far as their files hold the log, replays the
    /// committed entries from there, starts Raft as `replica` (a shard of one leads at once,
    /// committing whatever the last run left uncommitted), and starts the committer and the
    /// flusher, or (`stepped`) leaves them to the caller. Not on a boot an fsync failed on
    /// (`Disk::untrusted`): until the machine restarts, what it reads may not be what the disk
    /// holds, and a power loss would take it away.
    #[allow(clippy::too_many_arguments)]
    fn new(
        root: &Path,
        lock: File,
        mut catalog: Catalog,
        branch: &str,
        disk: DiskRef,
        clock: Clock,
        mut replica: Replica,
        stepped: bool,
    ) -> Result<Self, Error> {
        let untrusted = disk.untrusted().map_err(|e| {
            Error::Io(format!(
                "cannot tell whether an fsync failed since the machine started: {e}"
            ))
        })?;
        if untrusted {
            return Err(Error::Io(format!(
                "{}: an fsync failed since the machine started, so its page cache may show what \
                 the disk does not hold; restart the machine, or move the volume to another, \
                 before opening weir here",
                root.display()
            )));
        }
        // A snapshot whose install was under way is finished; one never copied whole goes.
        snapshot::recover(&disk, root)?;
        let weir = root.join("weir");
        let log_dir = weir.join("log");
        // Which replica this is, recorded before anything here makes the log (`identify`).
        if !replica.is_alone() {
            replica = identify(&disk, root, &log_dir, &mut catalog, branch, replica)?;
        }
        let flushed = wal::read_applied(&disk, &log_dir)?;
        let mut logs = HashMap::new();
        let mut stamped = 0;
        for entry in catalog.list(branch)? {
            let log = open_log(&disk, root, entry, flushed, flushed)?;
            let id = {
                let state = lock_log(&log);
                stamped = stamped.max(last_ingest(&state));
                state.entry.id
            };
            logs.insert(id, log);
        }
        let mut tables = Tables {
            logs: Arc::new(Mutex::new(logs)),
            catalog: Arc::new(Mutex::new(catalog)),
            branch: branch.to_owned(),
            disk: disk.clone(),
            root: root.to_owned(),
            known: Vec::new(),
        };
        // Built before the replay: a drop it replays flushes through itself first.
        let applied_at = Arc::new(AtomicU64::new(flushed));
        let flusher = Arc::new(Flusher {
            disk: disk.clone(),
            logs: tables.logs.clone(),
            log_dir: log_dir.clone(),
            applied: applied_at.clone(),
            failed: Mutex::new(false),
        });
        // Entries the partition files hold stay in the log, not in memory.
        let (wal, recovered) = Wal::open(
            &disk,
            root,
            &log_dir,
            replica.limits.segment_bytes,
            flushed,
            flushed,
            &mut |logged: Logged| {
                stamped = stamped.max(logged.ingest_ns);
                let log = tables.log_for(logged.table, &logged.shape)?;
                apply(&log, logged).map(drop)
            },
        )?;
        // Recorded once its log is durable (`Wal::open` makes it so), before Raft starts: it
        // has not voted or acknowledged anything before the catalog knows it has a log to lose.
        if !replica.is_alone() {
            self::lock(&tables.catalog).start_replica(branch, slot(replica.id))?;
        }
        // The committed entries the partition files do not hold yet, before Raft starts.
        let from = flushed.max(recovered.start.0);
        let mut applied = from;
        // Changes of the shard's members are Raft's own: applied to it once it runs.
        let mut changes = Vec::new();
        for entry in &recovered.entries {
            if entry.index > from && entry.index <= recovered.state.commit {
                if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
                    changes.push(entry.clone());
                } else if !entry.data.is_empty() {
                    tables.apply(entry, &mut stamped, &flusher)?;
                }
                applied = entry.index;
            }
        }
        let mut store = LogStore::new(wal, recovered, applied, &replica);
        store.compact_cache(applied);
        let term = Storage::term(&store, applied).map_err(|e| {
            Error::Corrupt(format!(
                "{}: the term of index {applied}: {e}",
                log_dir.display()
            ))
        })?;
        let position = Arc::new(Mutex::new((applied, term)));
        let config = raft::Config {
            id: replica.id,
            election_tick: 10,
            heartbeat_tick: 1,
            applied,
            max_size_per_msg: 1 << 20,
            max_inflight_msgs: 256,
            // A leader cut off from a quorum steps down, and a replica cut off cannot force an
            // election when it comes back: etcd's and TiKV's settings.
            check_quorum: true,
            pre_vote: true,
            random_seed: replica.seed,
            ..raft::Config::default()
        };
        let mut node = RawNode::new(&config, store, &raft::default_logger()).map_err(|e| {
            Error::Corrupt(format!("{}: cannot start Raft: {e}", log_dir.display()))
        })?;
        // Those the log committed past the last it recorded applied.
        let recorded = node.store().conf_index();
        for entry in changes.iter().filter(|entry| entry.index > recorded) {
            change_members(&mut node, entry)?;
        }
        let logs = tables.logs.clone();
        let members = node.store().conf().clone();
        let id = replica.id;
        let shard = Arc::new(Shard {
            member: AtomicBool::new(members.voters.contains(&id) || members.learners.contains(&id)),
            voter: AtomicBool::new(members.voters.contains(&id)),
            members: Mutex::new(members),
            ..Shard::default()
        });
        let (outgoing, outgoing_from) = mpsc::channel();
        let catalog = tables.catalog.clone();
        applied_at.store(applied, Ordering::Release);
        let applied = applied_at;
        let mut committer = Committer {
            node,
            replica,
            clock: clock.clone(),
            stamped,
            tables,
            flusher: flusher.clone(),
            applied: applied.clone(),
            position: position.clone(),
            proposed: VecDeque::new(),
            waiting: Vec::new(),
            drops: Vec::new(),
            leases: Vec::new(),
            schemas: Vec::new(),
            outgoing,
            shard: shard.clone(),
            broken: None,
        };
        if committer.replica.is_alone() {
            committer
                .node
                .campaign()
                .map_err(|e| Error::Io(format!("weir cannot lead its shard: {e}")))?;
            committer.drive();
            if let Some(why) = committer.broken {
                return Err(Error::Io(why));
            }
        }
        // The replay's writes reach the disk now, so another crash need not replay them.
        flusher.flush()?;
        let (requests, received) = mpsc::channel();
        let replica_id = committer.replica.id;
        let alone = committer.replica.is_alone();
        let driver = if stepped {
            Driver::Stepped {
                committer: Box::new(Mutex::new(committer)),
                received: Mutex::new(received),
            }
        } else {
            let committer = std::thread::Builder::new()
                .name("kermodes-weir-commit".into())
                .spawn(move || commit_every(committer, &received))
                .map_err(|e| Error::Io(format!("cannot start weir's committer: {e}")))?;
            let stop = Arc::new((Mutex::new(false), Condvar::new()));
            let flusher = {
                let (flusher, requests, stop) = (flusher.clone(), requests.clone(), stop.clone());
                std::thread::Builder::new()
                    .name("kermodes-weir-flush".into())
                    .spawn(move || flush_every(&flusher, &requests, &stop))
                    .map_err(|e| Error::Io(format!("cannot start weir's flusher: {e}")))?
            };
            // A shard of one leads from its open, and has no one to hear from.
            let ticker = (!alone)
                .then(|| {
                    let (requests, stop) = (requests.clone(), stop.clone());
                    std::thread::Builder::new()
                        .name("kermodes-weir-tick".into())
                        .spawn(move || tick_every(&requests, &stop))
                        .map_err(|e| Error::Io(format!("cannot start weir's Raft clock: {e}")))
                })
                .transpose()?;
            Driver::Threads {
                committer: Some(committer),
                flusher: Some((stop, flusher)),
                ticker,
            }
        };
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
            catalog,
            branch: branch.to_owned(),
            disk,
            logs,
            requests,
            flusher,
            position,
            clock,
            replica_id,
            shard,
            outgoing: Mutex::new(Some(outgoing_from)),
            driver,
        })
    }

    /// A stepped store's (`open_stepped`) committer: commits every write sent so far as one
    /// group, answering each (a replica's, once a quorum holds them); how many. A store with
    /// threads commits by itself: none here.
    pub fn step(&self) -> usize {
        let Driver::Stepped {
            committer,
            received,
        } = &self.driver
        else {
            return 0;
        };
        let (mut group, mut drops) = (Vec::new(), Vec::new());
        for request in lock(received).try_iter() {
            match request {
                Request::Write(submitted) => group.push(submitted),
                Request::Drop(dropping) => drops.push(dropping),
                // Asked only of a committer thread; a lease, of the committer at once.
                Request::Trim(..)
                | Request::Stop
                | Request::Receive(_)
                | Request::Tick
                | Request::HandOver
                | Request::Join(_)
                | Request::Lease(_) => {}
            }
        }
        let count = group.len() + drops.len();
        if count > 0 {
            lock(committer).submit(group, drops);
        }
        count
    }

    /// A stepped replica's (`open_replica`) Raft clock: one tick. A leader sends heartbeats
    /// every tick; a replica that hears from no leader for 10 to 20 ticks starts an election.
    /// A store with threads keeps its own (`TICK`).
    pub fn tick(&self) {
        if let Driver::Stepped { committer, .. } = &self.driver {
            lock(committer).tick();
        }
    }

    /// As leader, hands the lead to the replica that holds the most of the log, before this
    /// one stops: Raft sends it what it lacks and has it stand at once, so writers wait for a
    /// round trip instead of an election. Writes are refused meanwhile, as by a replica that
    /// does not lead. Done once this replica no longer leads (`is_leader`); Raft gives up after
    /// an election timeout.
    pub fn hand_over(&self) {
        match &self.driver {
            Driver::Stepped { committer, .. } => lock(committer).hand_over(),
            // Gone with the committer: the store is closing, and there is nothing to hand.
            Driver::Threads { .. } => drop(self.requests.send(Request::HandOver)),
        }
    }

    /// As leader, takes replica `id` into the shard: a new incarnation of a slot whose disk was
    /// lost (§7.6), made a learner now and a voter, in place of the one it replaced, once it
    /// has caught up. Asked until it is a member (`is_member`); refused unless this replica
    /// leads.
    pub fn join(&self, id: u64) -> Result<(), Error> {
        if !self.is_leader() {
            return Err(self.not_leading());
        }
        match &self.driver {
            Driver::Stepped { committer, .. } => lock(committer).join(id),
            Driver::Threads { .. } => drop(self.requests.send(Request::Join(id))),
        }
        Ok(())
    }

    /// Whether this replica is among its shard's members, a voter or a learner.
    pub fn is_member(&self) -> bool {
        self.shard.member.load(Ordering::Acquire)
    }

    /// Whether this replica votes in its shard's elections and commits.
    pub fn is_voter(&self) -> bool {
        self.shard.voter.load(Ordering::Acquire)
    }

    /// This replica's Raft ID (`replica_id`): its slot and incarnation.
    pub fn replica_id(&self) -> u64 {
        self.replica_id
    }

    fn not_leading(&self) -> Error {
        Error::NotLeader(match self.shard.leader.load(Ordering::Acquire) {
            0 => format!(
                "weir {} does not lead its shard, and knows of no leader yet",
                replica_name(self.replica_id)
            ),
            leader => format!(
                "weir {} does not lead its shard: {} does",
                replica_name(self.replica_id),
                replica_name(leader)
            ),
        })
    }

    /// Takes a message another replica of the shard sent this one (`take_messages`).
    pub fn receive(&self, message: &[u8]) -> Result<(), Error> {
        let mut decoded = Message::default();
        decoded
            .merge_from_bytes(message)
            .map_err(|e| Error::Corrupt(format!("a message from another replica: {e}")))?;
        match &self.driver {
            Driver::Stepped { committer, .. } => lock(committer).receive(decoded),
            // Gone with the committer: the store is closing, and Raft tolerates a lost message.
            Driver::Threads { .. } => drop(self.requests.send(Request::Receive(decoded))),
        }
        Ok(())
    }

    /// The messages for other replicas since the last call, each with the replica's ID, for
    /// whoever carries them: a stepped replica's carrier. A shard of one sends none.
    pub fn take_messages(&self) -> Vec<(u64, Vec<u8>)> {
        lock(&self.outgoing)
            .as_ref()
            .map_or_else(Vec::new, |outgoing| outgoing.try_iter().collect())
    }

    /// Every message for other replicas from now on, each with the replica's ID, for the
    /// carrier that runs beside a replica with threads; `None` once taken.
    pub fn take_outgoing(&self) -> Option<Outgoing> {
        lock(&self.outgoing).take()
    }

    /// Whether this replica leads its shard: only the leader takes writes.
    pub fn is_leader(&self) -> bool {
        self.shard.leader.load(Ordering::Acquire) == self.replica_id
    }

    /// Whether this replica is ready to serve as its shard's: it votes (a new incarnation, once
    /// promoted), knows a leader, and has applied the log as far as it knows it committed. A
    /// rollout takes the next replica down only once this one is: one still a learner leaves
    /// the replica it replaced among the voters, so another going would stall writes.
    pub fn caught_up(&self) -> bool {
        self.is_voter()
            && self.shard.leader.load(Ordering::Acquire) != 0
            && self.applied() >= self.shard.committed.load(Ordering::Acquire)
    }

    /// The snapshot this replica must install before it can go on, when it is too far behind
    /// its shard's log: close the store, `install_snapshot` it from the replica it names, and
    /// open the store again.
    pub fn snapshot_wanted(&self) -> Option<SnapshotWanted> {
        *lock(&self.shard.wanted)
    }

    /// Installs, in the closed store in `root` on `disk`, the snapshot it wanted
    /// (`snapshot_wanted`), its partition files copied from `from`; opening it then goes on
    /// from there. Should it fail, or the process stop, the replica stays as it was until
    /// the copy was whole, and is handed a snapshot again.
    pub fn install_snapshot(
        root: &Path,
        disk: DiskRef,
        limits: LogLimits,
        wanted: &SnapshotWanted,
        from: &dyn SnapshotSource,
    ) -> Result<(), Error> {
        snapshot::install(&disk, root, limits.segment_bytes, wanted, from)
    }

    /// The last index of the shard log this replica has applied to its tables: how far behind
    /// the leader it is.
    pub fn applied(&self) -> u64 {
        self.flusher.applied.load(Ordering::Acquire)
    }

    /// What the flusher does every second, now: fsyncs the partition files, records how far
    /// they hold the log, and deletes the log's segments wholly below that.
    pub fn flush_now(&self) -> Result<(), Error> {
        let flushed = self.flusher.flush()?;
        let gone = match &self.driver {
            Driver::Stepped { committer, .. } => lock(committer).trim(flushed),
            Driver::Threads { .. } => trim(&self.requests, flushed),
        };
        delete_segments(&self.disk, gone);
        Ok(())
    }

    fn now_ns(&self) -> i64 {
        (self.clock)()
    }

    /// Today's UTC date, the day live readers are given.
    fn today(&self) -> String {
        Date::of_ns(self.now_ns()).to_string()
    }

    /// Today's date is still being written: only a past one is read whole, merged, or dropped.
    fn past(&self, date: Date) -> Result<(), Error> {
        if date >= Date::of_ns(self.now_ns()) {
            return Err(Error::Busy(format!(
                "{date} is still being written: only past dates are merged"
            )));
        }
        Ok(())
    }

    fn unix_seconds(&self) -> i64 {
        self.now_ns().div_euclid(1_000_000_000)
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

    /// Takes up `entry`'s columns in its table's log, if it is open (`adopt`).
    fn adopt(&self, entry: &TableEntry) -> Result<(), Error> {
        let Some(log) = lock(&self.logs).get(&entry.id).cloned() else {
            return Ok(());
        };
        adopt(&log, entry)
    }

    /// Every table, by name: as the catalog has them, or, while it cannot be reached, those
    /// this replica holds (`table`).
    pub fn tables(&self) -> Result<Vec<TableEntry>, Error> {
        match lock(&self.catalog).list(&self.branch) {
            Err(Error::Catalog(_)) if !lock(&self.logs).is_empty() => {
                let mut held = self.held();
                held.sort_by_key(|entry| entry.name.to_string());
                Ok(held)
            }
            listed => listed,
        }
    }

    /// The tables this replica holds, as it last took them up.
    fn held(&self) -> Vec<TableEntry> {
        let logs: Vec<Arc<Log>> = lock(&self.logs).values().cloned().collect();
        logs.iter()
            .map(|log| lock(&log.state).entry.clone())
            .collect()
    }

    /// The shard's members as of the last entry this replica applied: read under `position`,
    /// they go with the files a peer copies (`SnapshotFiles`).
    pub(crate) fn members(&self) -> ConfState {
        lock(&self.shard.members).clone()
    }

    /// The store's directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn disk(&self) -> &DiskRef {
        &self.disk
    }

    /// The last entry applied, its index and term, held while it is: what a peer copying this
    /// replica's files may read them as of (`SnapshotSource`).
    pub(crate) fn position(&self) -> &Mutex<(u64, u64)> {
        &self.position
    }

    /// The table called `name`: as the catalog has it, or, while the catalog cannot be reached
    /// (a Postgres outage), as this replica holds it, so that writing, reading, and failing over
    /// go on (§7.6); a table created or changed meanwhile waits for the catalog.
    pub fn table(&self, name: &TableName) -> Result<TableEntry, Error> {
        match lock(&self.catalog).get(&self.branch, name) {
            Ok(Some(entry)) => Ok(entry),
            Ok(None) => Err(Error::NotFound(format!("no table {name}; create it first"))),
            Err(unreachable @ Error::Catalog(_)) => self
                .held()
                .into_iter()
                .find(|entry| entry.name == *name)
                .ok_or(unreachable),
            Err(e) => Err(e),
        }
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
        let (bytes, free) = self.disk.space(&self.root).map_err(|e| {
            Error::Io(format!(
                "cannot read the volume of {}: {e}",
                self.root.display()
            ))
        })?;
        Ok(json!({
            "tables": tables,
            "volume": {
                "bytes": bytes,
                "free": free,
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
        lock(&self.catalog).commit_history(id, commit, self.unix_seconds())
    }

    /// History files replaced at least `grace` ago, which no read can still be using.
    pub fn history_garbage(&self, grace: Duration) -> Result<Vec<Garbage>, Error> {
        lock(&self.catalog).garbage(self.unix_seconds() - grace.as_secs() as i64)
    }

    /// Forgets a garbage file once it is deleted.
    pub fn forget_garbage(&self, garbage: &Garbage) -> Result<(), Error> {
        lock(&self.catalog).forget_garbage(garbage)
    }

    /// A writer for `source`'s rows of `name`, holding the source's lease (§7.4) until it is
    /// dropped. One writer per source at a time: with `wait`, this waits for the current writer
    /// to go; without, it fails at once. Each lease is an entry in the shard log, its epoch
    /// that entry's index, recorded with every batch: taking one needs no catalog.
    pub fn writer(&self, name: &TableName, source: &SourceId, wait: bool) -> Result<Writer, Error> {
        self.lease(name, source, wait)?.writer()
    }

    /// Takes the lease a writer holds (`writer`), its entry proposed: the writer once it
    /// commits. A stepped replica of others is stepped until it is ready.
    pub fn lease(
        &self,
        name: &TableName,
        source: &SourceId,
        wait: bool,
    ) -> Result<PendingWriter, Error> {
        // Its writes would be refused one by one: the writer goes to the leader instead.
        if !self.is_leader() {
            return Err(self.not_leading());
        }
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
        // The source is ours from here; if the lease fails, dropping the writer releases it.
        let writer = Writer {
            log: log.clone(),
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
        let gone: Arc<str> = Arc::from("weir closed before the lease committed: take it again");
        let (pending, answer) = Arc::new(InFlight::default()).send(0, &gone);
        let leasing = Leasing {
            log,
            table: entry.id,
            source: source.clone(),
            answer,
        };
        let stepped = match &self.driver {
            Driver::Stepped { committer, .. } => {
                lock(committer).submit_lease(leasing);
                true
            }
            Driver::Threads { .. } => {
                // Gone with the committer: the store is closing, and the answer says so.
                let _ = self.requests.send(Request::Lease(leasing));
                false
            }
        };
        Ok(PendingWriter {
            writer,
            pending,
            stepped,
        })
    }

    /// `source`'s last sequence number for `name`, as its writer would resume from, and the
    /// epoch of its latest lease this replica applied (`None` before one, since it opened).
    pub fn source_state(
        &self,
        name: &TableName,
        source: &SourceId,
    ) -> Result<(Option<u64>, Option<u64>), Error> {
        let log = self.log(name)?;
        let state = lock(&log.state);
        Ok((
            source_last(&state, source),
            state.leases.get(source).copied(),
        ))
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
        let today = self.today();
        let log = self.log(name)?;
        // Where the replay starts in each partition: a keyed table's latest checkpoint and the
        // changes after it, and every batch of an append table's; or, resuming, the first batch
        // after the reader's position.
        let (mut delivered, shape) = {
            let mut state = lock(&log.state);
            let delivered = match after {
                None => {
                    ensure_checkpoint(&mut state, &today, self.now_ns())?;
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
        let today = self.today();
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
            .filter(|&date| self.past(date).is_ok())
            .collect();
        dates.dedup();
        Ok(dates)
    }

    /// The rows and stream bytes weir holds of a past `date` of `name`, over every source.
    pub fn date_rows(&self, name: &TableName, date: Date) -> Result<(u64, u64), Error> {
        self.past(date)?;
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
        self.past(date)?;
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
    /// The drop is an entry of the shard log, so every replica drops the date where its log
    /// says, and a replay brings back none of it. A date weir no longer holds was dropped.
    pub fn drop_date(&self, name: &TableName, date: Date) -> Result<(), Error> {
        self.past(date)?;
        let in_history: u64 = self
            .history(name)?
            .iter()
            .filter(|held| held.date == date)
            .map(|held| held.rows)
            .sum();
        let log = self.log(name)?;
        let (table, held, rows) = {
            let state = lock(&log.state);
            let key = date.to_string();
            let of_date = || state.partitions.iter().filter(|((day, _), _)| *day == key);
            let rows: u64 = of_date().map(|(_, partition)| partition.rows()).sum();
            (state.entry.id, of_date().next().is_some(), rows)
        };
        if !held {
            return Ok(());
        }
        if rows != in_history {
            return Err(Error::Catalog(format!(
                "weir holds {rows} rows of {name} on {date} and its history {in_history}: \
                 merge the date before dropping it"
            )));
        }
        let gone: Arc<str> = Arc::from("weir closed before the drop committed: drop it again");
        let (pending, answer) = Arc::new(InFlight::default()).send(0, &gone);
        let dropping = Dropping {
            log,
            table,
            date,
            rows,
            now_ns: self.now_ns(),
            answer,
        };
        match &self.driver {
            Driver::Stepped { committer, .. } => {
                lock(committer).submit(Vec::new(), vec![dropping]);
                if !pending.is_answered() {
                    return Err(Error::Busy(format!(
                        "the drop of {name} on {date} is proposed, and done once a quorum of \
                         replicas holds it"
                    )));
                }
            }
            Driver::Threads { .. } => {
                self.requests
                    .send(Request::Drop(dropping))
                    .map_err(|_| Error::Io(gone.to_string()))?;
            }
        }
        pending.wait().map(drop)
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
        let log = open_log(&self.disk, &self.root, entry, u64::MAX, 0)?;
        logs.insert(id, log.clone());
        Ok(log)
    }
}

impl Drop for Store {
    /// Stops the flusher, then the committer (writes already sent are logged and answered,
    /// later ones refused), then flushes what was applied, so reopening replays nothing.
    fn drop(&mut self) {
        match &mut self.driver {
            Driver::Threads {
                committer,
                flusher,
                ticker,
            } => {
                if let Some((stop, flusher)) = flusher.take() {
                    let (stopped, wake) = &*stop;
                    *lock(stopped) = true;
                    wake.notify_all();
                    let _ = flusher.join();
                }
                if let Some(ticker) = ticker.take() {
                    let _ = ticker.join();
                }
                let _ = self.requests.send(Request::Stop);
                if let Some(committer) = committer.take() {
                    let _ = committer.join();
                }
            }
            Driver::Stepped { .. } => {
                self.step();
            }
        }
        if let Err(e) = self.flusher.flush() {
            eprintln!("kermodes weir: closing, cannot flush partition files: {e}");
        }
    }
}

/// A table's log: its partitions recovered as far as their files hold the log, `trusted`. With
/// no partitions it may resume readers only from `floor` on: whatever it held before may have
/// gone with dates merged and dropped.
fn open_log(
    disk: &DiskRef,
    root: &Path,
    entry: TableEntry,
    trusted: u64,
    floor: u64,
) -> Result<Arc<Log>, Error> {
    let dir = root.join("weir").join(&entry.location);
    let mut floors = read_floors(disk, &dir)?;
    let forgotten = settle_drops(disk, &dir, trusted)?;
    let mut partitions = BTreeMap::new();
    for (date, stream) in partition_dirs(disk, &dir)? {
        let path = dir.join(&date).join(stream.dir_name());
        let partition =
            Partition::open(disk, root, &path, entry.shape(), trusted, stream.written())?;
        partitions.insert((date, stream), partition);
    }
    for key in forgotten {
        forget_date(disk, &dir, &mut partitions, &mut floors, &key)?;
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
            disk: disk.clone(),
            root: root.to_owned(),
            dir,
            partitions,
            subscribers: Vec::new(),
            writing: HashSet::new(),
            last_lsn,
            resumable_from,
            since_checkpoint: 0,
            checkpoint_bytes: 0,
            positions: None,
            floors,
            leases: HashMap::new(),
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
    // A resend: at or below the source's last seq, whatever date that was written on.
    if source_last(state, &logged.source).is_some_and(|last| logged.seq <= last) {
        return Ok(false);
    }
    let date = Date::of_ns(logged.ingest_ns).to_string();
    // A keyed table's day opens with its checkpoint, before any change of the day.
    ensure_checkpoint(state, &date, logged.ingest_ns)?;
    // Logged in the columns the table had when it was written; applied in today's (§5.4).
    let current = state.entry.shape();
    let batch = logged.shape.convert(logged.batch, &current)?;
    let key = (date, Stream::Source(logged.source));
    if !state.partitions.contains_key(&key) {
        let path = state.dir.join(&key.0).join(key.1.dir_name());
        let partition = Partition::open(
            &state.disk,
            &state.root,
            &path,
            current.clone(),
            u64::MAX,
            Written::FromLog,
        )?;
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
            let through = state.last_lsn;
            write_checkpoint(state, &key.0, &fold.state()?, through, logged.ingest_ns)?;
        }
    }
    Ok(true)
}

/// Drops `log`'s partitions of a past `date` (`Store::drop_date`), its history holding them,
/// as the shard log says at the time `now_ns`: if they still hold the `rows` its history does.
/// Writes stamped on the date may commit after it was merged (proposed before midnight, under
/// a leader that lost the lead before they did): then the date stays, and is merged again.
fn apply_drop(
    log: &Log,
    date: Date,
    rows: u64,
    now_ns: i64,
    index: u64,
    flusher: &Flusher,
) -> Result<(), Error> {
    let key = date.to_string();
    {
        let mut guard = lock(&log.state);
        let state = &mut *guard;
        let held: u64 = state
            .partitions
            .iter()
            .filter(|((day, _), _)| *day == key)
            .map(|(_, partition)| partition.rows())
            .sum();
        if held != rows {
            return Ok(());
        }
        // A keyed table's state must not depend on the date going: today's checkpoint holds it.
        let today = Date::of_ns(now_ns).to_string();
        ensure_checkpoint(state, &today, now_ns)?;
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
        // The checkpoint is durable before the date it may be folded from goes: whichever
        // process wrote it, it cannot be folded again once the date is gone.
        if let Some(checkpoint) = state.partitions.get(&(today, Stream::Checkpoint)) {
            checkpoint.sync_now(&state.root)?;
        }
    }
    // The partition files hold everything up to the drop before the date goes, so no replay
    // starts before it: one would fold today's keyed state again without the date. The date
    // says first that it is going, at `index`: a process stopped before it went finishes on
    // opening (`settle_drops`), the drop being past where its replay starts.
    mark_drop(&log.state, &key, index)?;
    flusher.flush_through(index)?;
    let mut guard = lock(&log.state);
    let state = &mut *guard;
    forget_date(
        &state.disk,
        &state.dir,
        &mut state.partitions,
        &mut state.floors,
        &key,
    )
}

/// Takes up `entry`'s columns in its table's log, if they changed: segments written from now on
/// hold them, and writers' rows are brought to them. Readers and writers already open keep
/// their own.
fn adopt(log: &Log, entry: &TableEntry) -> Result<(), Error> {
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

/// Every table's log by ID, and what opens or updates them for the writes of any table that
/// come from another replica, or from the log on open.
struct Tables {
    logs: Arc<Mutex<HashMap<i64, Arc<Log>>>>,
    catalog: Arc<Mutex<Catalog>>,
    branch: String,
    disk: DiskRef,
    root: PathBuf,
    /// The columns each table's writes came in that its log has taken up (`log_for`).
    known: Vec<(i64, Shape)>,
}

impl Tables {
    /// Carries out what an entry says (`apply`, `apply_drop`). Whether it wrote.
    fn apply(
        &mut self,
        entry: &Entry,
        stamped: &mut i64,
        flusher: &Flusher,
    ) -> Result<bool, Error> {
        if entry.get_entry_type() != EntryType::EntryNormal {
            return Err(Error::Corrupt(format!(
                "the shard log holds a {:?} at index {}, which this version cannot apply",
                entry.get_entry_type(),
                entry.index
            )));
        }
        match wal::decode(entry.index, &entry.data)
            .map_err(|e| Error::Corrupt(format!("the shard log: {e}")))?
        {
            Command::Write(logged) => {
                *stamped = (*stamped).max(logged.ingest_ns);
                let log = self.log_for(logged.table, &logged.shape)?;
                apply(&log, logged)
            }
            Command::Drop {
                table,
                date,
                rows,
                now_ns,
            } => {
                let log = self.open(table)?;
                apply_drop(&log, date, rows, now_ns, entry.index, flusher).map(|()| false)
            }
            // Another replica's lease: its epoch is the entry's index.
            Command::Lease { table, source } => {
                let log = self.open(table)?;
                lock(&log.state).leases.insert(source, entry.index);
                Ok(false)
            }
        }
    }

    /// The log of `table`, whose write came in `shape`: opened if the table was created since
    /// this replica opened, its columns taken up if they changed since (§5.4), as the leader
    /// took them up when they did.
    fn log_for(&mut self, table: i64, shape: &Shape) -> Result<Arc<Log>, Error> {
        let log = self.open(table)?;
        if !self.known.iter().any(|(t, s)| *t == table && s == shape) {
            if lock(&log.state).entry.shape() != *shape {
                adopt(&log, &self.entry(table)?)?;
            }
            self.known.push((table, shape.clone()));
        }
        Ok(log)
    }

    /// The log of `table`, opened if the table was created since this replica opened.
    fn open(&mut self, table: i64) -> Result<Arc<Log>, Error> {
        if let Some(log) = lock(&self.logs).get(&table) {
            return Ok(log.clone());
        }
        // Created since: no partitions yet, and nothing of it applied.
        let log = open_log(&self.disk, &self.root, self.entry(table)?, u64::MAX, 0)?;
        Ok(lock(&self.logs).entry(table).or_insert(log).clone())
    }

    fn entry(&self, table: i64) -> Result<TableEntry, Error> {
        lock(&self.catalog)
            .list(&self.branch)?
            .into_iter()
            .find(|entry| entry.id == table)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "the log holds a write of table {table}, which the catalog does not"
                ))
            })
    }
}

/// What commits the store's writes: this replica's Raft, the log it persists, and the writes
/// it proposed, until their entries commit.
struct Committer {
    node: RawNode<LogStore>,
    replica: Replica,
    clock: Clock,
    /// The last ingest time stamped or applied: each write's is above it, whatever the clock
    /// says, so ingest times rise across every table, and dates and commit order agree,
    /// whichever replica leads.
    stamped: i64,
    tables: Tables,
    /// The last index applied to the partition files.
    applied: Arc<AtomicU64>,
    /// What a drop flushes the partition files with before the date goes (`apply_drop`).
    flusher: Arc<Flusher>,
    /// The last entry applied, its index and term, locked while an entry is: a peer copying the
    /// partition files reads them as of it (`snapshot`), and none changes them meanwhile.
    position: Arc<Mutex<(u64, u64)>>,
    /// Writes proposed, in index order, until their entries commit.
    proposed: VecDeque<Proposed>,
    /// Writes and drops taken while earlier ones were uncommitted, proposed once those are
    /// applied.
    waiting: Vec<Submitted>,
    drops: Vec<Dropping>,
    leases: Vec<Leasing>,
    /// The schema messages of the columns written lately, so each is encoded once.
    schemas: Vec<(Shape, Vec<u8>)>,
    /// Messages to other replicas, for whoever carries them (`Store::take_messages`).
    outgoing: Sender<(u64, Vec<u8>)>,
    /// What the store's callers read: the leader, and the snapshot to install.
    shard: Arc<Shard>,
    /// Why the committer stopped taking writes: a failure to log or apply leaves what is
    /// durable in the log, to replay on the next open, and the partition files no longer
    /// following it.
    broken: Option<String>,
}

/// What was proposed at `index` in `term`: done if the entry committed there is it.
struct Proposed {
    index: u64,
    term: u64,
    what: Proposal,
}

enum Proposal {
    Write(Unit),
    Drop(Dropping),
    Lease(Leasing),
}

impl Proposal {
    fn answers(self) -> Vec<Answer> {
        match self {
            Proposal::Write(unit) => unit.answers,
            Proposal::Drop(dropping) => vec![dropping.answer],
            Proposal::Lease(leasing) => vec![leasing.answer],
        }
    }
}

impl Committer {
    /// Takes writes and drops to propose (`propose`); each is answered once its entry is
    /// applied, or now if this replica cannot take it.
    fn submit(&mut self, group: Vec<Submitted>, drops: Vec<Dropping>) {
        if self.broken.is_some() || !self.is_leader() {
            // Each refused as what it is: not leading sends a writer to the next replica to
            // resend there; weir stopped says to restart it.
            let refused = || match &self.broken {
                Some(why) => stopped(why),
                None => self.not_leader(),
            };
            let answers = group.into_iter().map(|submitted| submitted.answer);
            for answer in answers.chain(drops.into_iter().map(|dropping| dropping.answer)) {
                answer.send(Err(refused()));
            }
            return;
        }
        // Mostly nothing waits (a shard of one, always): then the group is taken as it is.
        if self.waiting.is_empty() {
            self.waiting = group;
        } else {
            self.waiting.extend(group);
        }
        self.drops.extend(drops);
        self.drive();
    }

    /// Takes a lease to propose (`propose`): answered once its entry is applied, or now if
    /// this replica cannot take it.
    fn submit_lease(&mut self, leasing: Leasing) {
        let refused = match &self.broken {
            Some(why) => Some(stopped(why)),
            None => (!self.is_leader()).then(|| self.not_leader()),
        };
        match refused {
            Some(refused) => leasing.answer.send(Err(refused)),
            None => {
                self.leases.push(leasing);
                self.drive();
            }
        }
    }

    fn is_leader(&self) -> bool {
        self.node.raft.state == StateRole::Leader
    }

    fn tick(&mut self) {
        if self.broken.is_none() {
            self.node.tick();
            self.drive();
        }
    }

    fn receive(&mut self, message: Message) {
        // A message for another incarnation of this replica's slot (the one it replaced) is
        // not this one's to take.
        if self.broken.is_none() && message.to == self.replica.id {
            // One Raft cannot use (from a replica not of the shard) is dropped, as Raft says.
            let _ = self.node.step(message);
            self.drive();
        }
    }

    /// As leader, makes replica `id` a learner, unless it is a member already or another change
    /// of members is under way (`Store::join`).
    fn join(&mut self, id: u64) {
        let conf = self.node.store().conf();
        if self.broken.is_some()
            || !self.is_leader()
            || self.node.raft.has_pending_conf()
            || conf.voters.contains(&id)
            || conf.learners.contains(&id)
        {
            return;
        }
        let change = ConfChangeV2 {
            changes: vec![change_of(id, ConfChangeType::AddLearnerNode)],
            ..ConfChangeV2::default()
        };
        let _ = self.node.propose_conf_change(Vec::new(), change);
        self.drive();
    }

    /// As leader, makes a learner that has caught up a voter, removing the incarnation of its
    /// slot it replaced in the same change (`promotion`): one change of members at a time.
    fn promote(&mut self) {
        let conf = self.node.store().conf();
        let raft = &self.node.raft;
        if self.broken.is_some()
            || !self.is_leader()
            || raft.has_pending_conf()
            || !conf.voters_outgoing.is_empty()
        {
            return;
        }
        let learners = conf.learners.iter().filter_map(|&id| {
            let progress = raft.prs().get(id)?;
            Some((
                id,
                progress.state == ProgressState::Replicate,
                progress.matched,
            ))
        });
        let Some(Promotion { learner, replaced }) = promotion(
            self.replica.id,
            &conf.voters,
            learners,
            raft.raft_log.committed,
        ) else {
            return;
        };
        let mut changes = vec![change_of(learner, ConfChangeType::AddNode)];
        changes.extend(
            replaced
                .into_iter()
                .map(|id| change_of(id, ConfChangeType::RemoveNode)),
        );
        let change = ConfChangeV2 {
            changes,
            ..ConfChangeV2::default()
        };
        let _ = self.node.propose_conf_change(Vec::new(), change);
    }

    /// Does what Raft asks until it asks nothing more (`handle_ready`), then proposes the
    /// writes waiting if it can, and again.
    fn drive(&mut self) {
        self.promote();
        loop {
            while self.broken.is_none() && self.node.has_ready() {
                self.handle_ready();
            }
            if self.broken.is_some() || !self.propose() {
                break;
            }
        }
        // In memory past what is applied: entries followers are still taking, up to
        // `CACHE_BYTES`; one further behind reads them back from the log.
        let applied = self.applied.load(Ordering::Acquire);
        let keep = match self.followers_matched() {
            Some(matched) if self.node.store().cache_bytes() <= CACHE_BYTES => applied.min(matched),
            _ => applied,
        };
        self.node.mut_store().compact_cache(keep);
        self.shard
            .committed
            .store(self.node.raft.raft_log.committed, Ordering::Release);
        self.shard
            .leader
            .store(self.node.raft.leader_id, Ordering::Release);
        let conf = self.node.store().conf();
        let id = self.replica.id;
        self.shard.member.store(
            conf.voters.contains(&id) || conf.learners.contains(&id),
            Ordering::Release,
        );
        self.shard
            .voter
            .store(conf.voters.contains(&id), Ordering::Release);
    }

    /// As leader of others, the last index every follower holds.
    fn followers_matched(&self) -> Option<u64> {
        if !self.is_leader() || self.replica.is_alone() {
            return None;
        }
        let id = self.replica.id;
        self.node
            .raft
            .prs()
            .iter()
            .filter(|(peer, _)| **peer != id)
            .map(|(_, progress)| progress.matched)
            .min()
    }

    /// As leader of others, hands the lead to the voter holding the most of the log
    /// (`Store::hand_over`), of those heard from lately. Asked again while a hand-over is under
    /// way, it tries another: the one it was handing to may have gone since it last answered.
    fn hand_over(&mut self) {
        if self.broken.is_some() || !self.is_leader() {
            return;
        }
        let raft = &self.node.raft;
        let voters = raft.prs().conf().voters();
        let to = transferee(
            self.replica.id,
            raft.prs()
                .iter()
                .filter(|(id, progress)| {
                    voters.contains(**id)
                        && progress.recent_active
                        && raft.lead_transferee != Some(**id)
                })
                .map(|(id, progress)| (*id, progress.matched)),
        );
        if let Some(to) = to {
            self.node.transfer_leader(to);
            self.drive();
        }
    }

    /// Proposes the writes waiting, stamped and merged (`merge`) as one group, if this replica
    /// leads and has applied every entry before them: `merge` tells a resend by the last seq
    /// the tables hold, which entries not yet applied have not set. Whether it proposed.
    fn propose(&mut self) -> bool {
        let last = self.node.raft.raft_log.last_index();
        if (self.waiting.is_empty() && self.drops.is_empty() && self.leases.is_empty())
            || !self.is_leader()
            || self.applied.load(Ordering::Acquire) != last
        {
            return false;
        }
        let units = merge(
            std::mem::take(&mut self.waiting),
            &mut self.stamped,
            (self.clock)(),
        );
        for unit in units {
            let data = self.schema(&unit.shape).and_then(|schema| {
                wal::encode(&Append {
                    table: unit.table,
                    schema,
                    source: &unit.source,
                    seq: unit.seq,
                    epoch: unit.epoch,
                    change: unit.change,
                    ingest_ns: unit.ingest_ns,
                    batch: &unit.batch,
                })
            });
            self.offer(data, Proposal::Write(unit));
        }
        for dropping in std::mem::take(&mut self.drops) {
            let data = wal::encode_drop(
                dropping.table,
                dropping.date,
                dropping.rows,
                dropping.now_ns,
            );
            self.offer(Ok(data), Proposal::Drop(dropping));
        }
        for leasing in std::mem::take(&mut self.leases) {
            let data = wal::encode_lease(leasing.table, &leasing.source);
            self.offer(Ok(data), Proposal::Lease(leasing));
        }
        true
    }

    /// Proposes `data`, encoded for `what`, and keeps `what` until its entry commits.
    fn offer(&mut self, data: Result<Bytes, Error>, what: Proposal) {
        let proposed = data.and_then(|data| {
            self.node
                .propose(Bytes::new(), data)
                .map_err(|_| self.not_leader())
        });
        match proposed {
            Ok(()) => self.proposed.push_back(Proposed {
                index: self.node.raft.raft_log.last_index(),
                term: self.node.raft.term,
                what,
            }),
            Err(e) => Answer::send_all(what.answers(), Err(e)),
        }
    }

    /// The schema message of `shape`'s columns with their IDs.
    fn schema(&mut self, shape: &Shape) -> Result<&[u8], Error> {
        let at = match self.schemas.iter().position(|(known, _)| known == shape) {
            Some(at) => at,
            None => {
                // A table's columns change rarely: a few are enough to keep.
                if self.schemas.len() == 64 {
                    self.schemas.clear();
                }
                let message = schema_header(&shape.segment_schema())?;
                self.schemas.push((shape.clone(), message));
                self.schemas.len() - 1
            }
        };
        Ok(&self.schemas[at].1)
    }

    /// Persists the entries and state Raft has for the log under one fsync, sends its messages
    /// (a follower's only once persisted), and applies the entries committed.
    fn handle_ready(&mut self) {
        let mut ready = self.node.ready();
        if ready
            .ss()
            .is_some_and(|soft| soft.raft_state != StateRole::Leader)
        {
            self.step_down();
        }
        if !ready.snapshot().is_empty() {
            self.want(ready.snapshot());
            return;
        }
        let messages = ready.take_messages();
        // A snapshot is its index and term: sent, it is done as far as Raft is concerned, and
        // the follower goes back to taking entries once it has copied the files.
        let snapshots: Vec<u64> = messages
            .iter()
            .filter(|m| m.get_msg_type() == MessageType::MsgSnapshot)
            .map(|m| m.to)
            .collect();
        self.send(messages);
        let entries = ready.take_entries();
        // Raft's state goes with entries, at no cost, and alone only when its term or vote
        // changed. A commit index alone is not written: Raft learns it again, and whatever was
        // applied was committed (`LogStore::new`), while writing it would cost every group a
        // second write after its fsync.
        if (!entries.is_empty() || ready.must_sync())
            && let Err(e) = self.node.mut_store().persist(&entries, ready.hs())
        {
            self.stop(e.to_string());
            return;
        }
        self.send(ready.take_persisted_messages());
        self.apply(ready.take_committed_entries());
        let mut light = self.node.advance(ready);
        self.send(light.take_messages());
        self.apply(light.take_committed_entries());
        self.node.advance_apply();
        for to in snapshots {
            self.node.report_snapshot(to, SnapshotStatus::Finish);
        }
    }

    /// Raft hands this follower a snapshot: it wants the donor's partition files, keeping its
    /// own term and vote, and takes nothing more until they are installed and it is reopened.
    fn want(&mut self, given: &Snapshot) {
        let meta = given.get_metadata();
        let donor = given
            .data
            .get(..8)
            .map(|id| u64::from_le_bytes(id.try_into().expect("8 bytes")));
        let Some(from) = donor else {
            self.stop("a snapshot that names no replica to copy from".into());
            return;
        };
        *lock(&self.shard.wanted) = Some(SnapshotWanted {
            from,
            index: meta.index,
            raft_term: self.node.raft.term,
            vote: self.node.raft.vote,
        });
        self.stop(format!(
            "{} is too far behind {} to catch up from the log: it copies that replica's \
             partition files, from index {}",
            replica_name(self.replica.id),
            replica_name(from),
            meta.index
        ));
    }

    /// Applies committed entries in order, and answers the writes this replica proposed in
    /// them: written, a resend, or lost to another leader's entry at their index.
    fn apply(&mut self, entries: Vec<Entry>) {
        for entry in entries {
            if self.broken.is_some() {
                return;
            }
            let index = entry.index;
            while self.proposed.front().is_some_and(|mine| mine.index < index) {
                let lost = self.proposed.pop_front().expect("seen");
                Answer::send_all(lost.what.answers(), Err(self.lost()));
            }
            let mine = match self.proposed.front() {
                Some(mine) if mine.index == index => {
                    let mine = self.proposed.pop_front().expect("seen");
                    if mine.term == entry.term {
                        Some(mine.what)
                    } else {
                        Answer::send_all(mine.what.answers(), Err(self.lost()));
                        None
                    }
                }
                _ => None,
            };
            let held = self.position.clone();
            let mut position = lock(&held);
            let (outcome, answers) = match mine {
                // A change of the shard's members, Raft's own.
                None if entry.get_entry_type() == EntryType::EntryConfChangeV2 => (
                    change_members(&mut self.node, &entry).map(|members| {
                        *lock(&self.shard.members) = members;
                        false
                    }),
                    Vec::new(),
                ),
                Some(Proposal::Write(unit)) => {
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
                    let logged = Logged {
                        lsn: index,
                        table,
                        shape,
                        source,
                        seq,
                        epoch,
                        change,
                        ingest_ns,
                        batch,
                    };
                    (apply(&log, logged), answers)
                }
                Some(Proposal::Drop(dropping)) => {
                    let dropped = apply_drop(
                        &dropping.log,
                        dropping.date,
                        dropping.rows,
                        dropping.now_ns,
                        index,
                        &self.flusher,
                    );
                    (dropped.map(|()| true), vec![dropping.answer])
                }
                Some(Proposal::Lease(leasing)) => {
                    lock(&leasing.log.state)
                        .leases
                        .insert(leasing.source, index);
                    (Ok(true), vec![leasing.answer])
                }
                // A leader's first entry of its term.
                None if entry.data.is_empty() => (Ok(false), Vec::new()),
                None => (
                    self.tables.apply(&entry, &mut self.stamped, &self.flusher),
                    Vec::new(),
                ),
            };
            match &outcome {
                Ok(_) => {
                    *position = (index, entry.term);
                    self.applied.store(index, Ordering::Release);
                }
                Err(e) => self.stop(e.to_string()),
            }
            drop(position);
            Answer::send_all(answers, outcome);
        }
    }

    /// No longer the leader: the writes it proposed may commit or be overwritten, which it may
    /// never learn, and those waiting are for the leader.
    fn step_down(&mut self) {
        for mine in std::mem::take(&mut self.proposed) {
            Answer::send_all(mine.what.answers(), Err(self.lost()));
        }
        for submitted in std::mem::take(&mut self.waiting) {
            submitted.answer.send(Err(self.not_leader()));
        }
        for dropping in std::mem::take(&mut self.drops) {
            dropping.answer.send(Err(self.not_leader()));
        }
        for leasing in std::mem::take(&mut self.leases) {
            leasing.answer.send(Err(self.not_leader()));
        }
    }

    /// Stops taking writes, for `why`, failing those taken.
    fn stop(&mut self, why: String) {
        for mine in std::mem::take(&mut self.proposed) {
            Answer::send_all(mine.what.answers(), Err(stopped(&why)));
        }
        for submitted in std::mem::take(&mut self.waiting) {
            submitted.answer.send(Err(stopped(&why)));
        }
        for dropping in std::mem::take(&mut self.drops) {
            dropping.answer.send(Err(stopped(&why)));
        }
        for leasing in std::mem::take(&mut self.leases) {
            leasing.answer.send(Err(stopped(&why)));
        }
        self.broken = Some(why);
    }

    /// Deletes what the log need not keep (`LogStore::trim`), their files: for a shard of one,
    /// whatever the partition files hold; for a replica, what every follower holds (as their
    /// leader), and past `LogLimits::retain_bytes` of it what one far behind would need, which
    /// catches up from a peer's partition files instead.
    fn trim(&mut self, flushed: u64) -> Vec<PathBuf> {
        let needed = if self.replica.is_alone() {
            flushed
        } else {
            self.followers_matched().unwrap_or(0)
        };
        let retain = self.replica.limits.retain_bytes;
        self.node.mut_store().trim(flushed, needed, retain)
    }

    /// Hands `messages` to whoever carries them, encoded. One nobody takes is dropped, as a
    /// network would.
    fn send(&self, messages: Vec<Message>) {
        for message in messages {
            let mut bytes = Vec::with_capacity(message.encoded_len());
            message.encode(&mut bytes);
            let _ = self.outgoing.send((message.to, bytes));
        }
    }

    fn not_leader(&self) -> Error {
        let id = self.replica.id;
        let me = replica_name(id);
        let raft = &self.node.raft;
        Error::NotLeader(match (raft.leader_id, raft.lead_transferee) {
            (raft::INVALID_ID, _) => {
                format!("weir {me} does not lead its shard, and knows of no leader yet")
            }
            (leader, Some(to)) if leader == id => {
                format!(
                    "weir {me} is handing the lead of its shard to {}",
                    replica_name(to)
                )
            }
            (leader, _) => {
                format!(
                    "weir {me} does not lead its shard: {} does",
                    replica_name(leader)
                )
            }
        })
    }

    fn lost(&self) -> Error {
        Error::NotLeader(format!(
            "weir {} stopped leading its shard before the write committed, so it may or may not \
             be written: send it again, as a resend",
            replica_name(self.replica.id)
        ))
    }
}

/// The file in a replica's directory holding its Raft ID (`identify`).
const IDENTITY: &str = "replica-id";

/// Which replica `replica` is, given its slot as its ID: the incarnation its disk records, or,
/// on a disk that records none, its slot's first incarnation, unless the catalog says the slot
/// had a log (`replica_started`) and this disk has none: its disk was lost or replaced. The
/// replica it replaced may have voted, and acknowledged entries, with what was lost, so it
/// takes the slot's next incarnation (`Catalog::next_incarnation`), a new member: it takes up
/// the members as the shard began them (catching up from the log's start replays every change
/// since; a snapshot brings them as of its copy) and votes only once the shard makes it a
/// voter (§7.6). The ID is on the disk before Raft first runs; a log from before IDs were
/// recorded is the first incarnation's.
fn identify(
    disk: &DiskRef,
    root: &Path,
    log_dir: &Path,
    catalog: &mut Catalog,
    branch: &str,
    mut replica: Replica,
) -> Result<Replica, Error> {
    let at = slot(replica.id);
    let id = match wal::read_word(disk, &root.join(IDENTITY))? {
        Some(id) => id,
        None => {
            let lost = !disk.exists(log_dir) && catalog.replica_started(branch, at)?;
            let incarnation = if lost {
                catalog.next_incarnation(branch, at)?
            } else {
                0
            };
            let id = replica_id(at, incarnation);
            wal::write_word(disk, root, IDENTITY, id)?;
            id
        }
    };
    if slot(id) != at {
        return Err(Error::Corrupt(format!(
            "{}: the disk of {} serves as replica {at}",
            root.display(),
            replica_name(id)
        )));
    }
    replica.id = id;
    Ok(replica)
}

/// Applies the change of the shard's members `entry` holds to Raft and records what it left,
/// durably, before the entry counts as applied.
fn change_members(node: &mut RawNode<LogStore>, entry: &Entry) -> Result<ConfState, Error> {
    let mut change = ConfChangeV2::default();
    change.merge_from_bytes(&entry.data).map_err(|e| {
        Error::Corrupt(format!(
            "the shard log's change of members at {}: {e}",
            entry.index
        ))
    })?;
    let members = node.apply_conf_change(&change).map_err(|e| {
        Error::Corrupt(format!(
            "the shard log's change of members at {}: {e}",
            entry.index
        ))
    })?;
    node.mut_store()
        .persist_conf(entry.index, members.clone())?;
    Ok(members)
}

/// One change of a change of the shard's members: `node_id` added as a voter or a learner, or
/// removed.
fn change_of(node_id: u64, change_type: ConfChangeType) -> ConfChangeSingle {
    ConfChangeSingle {
        change_type,
        node_id,
    }
}

fn stopped(why: &str) -> Error {
    Error::Io(format!("weir stopped taking writes: {why}; restart it"))
}

/// The committer thread: takes every write waiting (up to `GROUP_BYTES`) and commits them as
/// one group, until stopped.
///
/// Writers answered together come back together, a few microseconds apart: a group waits up to
/// `GATHER` for as many writes as the last one held, so they share the next fsync instead of
/// alternating between groups of one and the rest. A lone writer never waits.
fn commit_every(mut committer: Committer, requests: &Receiver<Request>) {
    let mut stop = false;
    let mut last_group = 1;
    while !stop {
        let Ok(first) = requests.recv() else {
            return;
        };
        let gather_until = Instant::now() + GATHER;
        let mut group = Vec::new();
        let mut drops = Vec::new();
        let mut bytes = 0;
        let mut next = Some(first);
        while let Some(request) = next.take() {
            match request {
                Request::Write(submitted) => {
                    bytes += submitted.batch.get_array_memory_size();
                    group.push(submitted);
                }
                Request::Drop(dropping) => drops.push(dropping),
                Request::Lease(leasing) => committer.submit_lease(leasing),
                Request::Trim(flushed, reply) => {
                    let _ = reply.send(committer.trim(flushed));
                }
                Request::Stop => stop = true,
                Request::Receive(message) => committer.receive(message),
                Request::Tick => committer.tick(),
                Request::HandOver => committer.hand_over(),
                Request::Join(id) => committer.join(id),
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
        if !group.is_empty() || !drops.is_empty() {
            last_group = group.len().max(1);
            committer.submit(group, drops);
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
fn merge(group: Vec<Submitted>, stamped: &mut i64, now: i64) -> Vec<Unit> {
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
    // Each source's last seq per table, as the group's writes before this one leave it.
    let mut known: HashMap<(i64, SourceId), Option<u64>> = HashMap::new();
    // Each source's unit still taking writes, per table.
    let mut runs: HashMap<(i64, SourceId), usize> = HashMap::new();
    let mut open: Vec<Open> = Vec::new();
    // `now` is one reading of the clock for the group, which is logged at one instant: its
    // writes are stamped a nanosecond apart from there.
    for (at, submitted) in group.into_iter().enumerate() {
        *stamped = now.max(*stamped + 1);
        let date = Date::of_ns(*stamped);
        let last = known
            .entry((submitted.table, submitted.source.clone()))
            .or_insert_with(|| source_last(&lock(&submitted.log.state), &submitted.source));
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
            previous.ingest_ns = *stamped;
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
            ingest_ns: *stamped,
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

/// Fsyncs the partition files and records how far they hold the log: the store's, shared with
/// its flusher thread.
struct Flusher {
    disk: DiskRef,
    logs: Arc<Mutex<HashMap<i64, Arc<Log>>>>,
    log_dir: PathBuf,
    applied: Arc<AtomicU64>,
    /// Held while a flush runs (the flusher thread's, `flush_now`, a drop's), and set once one
    /// failed. The files a failed flush took to sync may not hold what they were given, and a
    /// later flush would not sync them again: none records a position until the store is opened
    /// again and replays the log from the last one recorded.
    failed: Mutex<bool>,
}

impl Flusher {
    /// Fsyncs every partition file written so far, then records that the files hold the log up
    /// to what was applied before: what the next open replays from. That position. Never after
    /// a flush failed, or any fsync through the store's disk: what that fsync held may not be
    /// on the disk, and a position past it would skip it on replay.
    fn flush(&self) -> Result<u64, Error> {
        let mut failed = lock(&self.failed);
        if *failed || self.disk.fsync_failed() {
            return Err(Error::Io(
                "an earlier flush or fsync failed: the partition files are flushed again once \
                 weir restarts"
                    .into(),
            ));
        }
        let flushed = self.sync_and_record();
        *failed = flushed.is_err();
        flushed
    }

    /// Flushes, recording that the files hold everything up to `index`, which is applied.
    fn flush_through(&self, index: u64) -> Result<(), Error> {
        self.applied.store(index, Ordering::Release);
        self.flush().map(drop)
    }

    fn sync_and_record(&self) -> Result<u64, Error> {
        let through = self.applied.load(Ordering::Acquire);
        // By table id: the same order every run, as a simulation replaying a seed needs.
        let mut open: Vec<(i64, Arc<Log>)> = lock(&self.logs)
            .iter()
            .map(|(id, log)| (*id, log.clone()))
            .collect();
        open.sort_unstable_by_key(|(id, _)| *id);
        let mut files = Vec::new();
        for (_, log) in open {
            files.extend(lock(&log.state).unsynced()?);
        }
        // Partitions share their parent directories: each is fsynced once.
        let mut dirs = HashSet::new();
        files.retain(|file| match file {
            ToSync::Dir(dir) => dirs.insert(dir.clone()),
            ToSync::File(_) => true,
        });
        // Outside every lock: writers and readers go on meanwhile.
        for file in files {
            file.sync(&self.disk)
                .map_err(|e| Error::Io(format!("cannot sync a partition file: {e}")))?;
        }
        wal::write_applied(&self.disk, &self.log_dir, through)?;
        Ok(through)
    }
}

/// A replica's Raft clock: a tick every `TICK`, until stopped.
fn tick_every(requests: &Sender<Request>, stop: &Stop) {
    let (stopped, wake) = stop;
    let mut guard = lock(stopped);
    loop {
        guard = wake
            .wait_timeout_while(guard, TICK, |stopped| !*stopped)
            .expect("kermodes store poisoned by an earlier panic")
            .0;
        // Stopped, or the committer is gone.
        if *guard || requests.send(Request::Tick).is_err() {
            return;
        }
    }
}

/// The flusher thread: flushes every `FLUSH_EVERY`, then has the log below the flushed point
/// deleted, until stopped. A flush that fails is the last (`Flusher::failed`): the log is kept
/// whole, to replay on the next open.
fn flush_every(flusher: &Flusher, requests: &Sender<Request>, stop: &(Mutex<bool>, Condvar)) {
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
        let flushed = match flusher.flush() {
            Ok(flushed) => flushed,
            Err(e) => {
                eprintln!(
                    "kermodes weir: cannot flush partition files ({e}); the log keeps every \
                     write until weir restarts"
                );
                return;
            }
        };
        delete_segments(&flusher.disk, trim(requests, flushed));
    }
}

/// Has the committer thread forget the log's segments wholly below `flushed`: their files.
fn trim(requests: &Sender<Request>, flushed: u64) -> Vec<PathBuf> {
    let (reply, answer) = mpsc::sync_channel(1);
    if requests.send(Request::Trim(flushed, reply)).is_err() {
        return Vec::new();
    }
    answer.recv().unwrap_or_default()
}

/// Deletes log segments the committer forgot, outside any lock.
fn delete_segments(disk: &DiskRef, paths: Vec<PathBuf>) {
    for path in paths {
        if let Err(e) = disk.remove_file(&path) {
            eprintln!("kermodes weir: cannot delete {}: {e}", path.display());
        }
    }
}

/// A writer whose lease is proposed (`Store::lease`): the writer once the lease commits.
pub struct PendingWriter {
    writer: Writer,
    pending: Pending,
    /// The committer is stepped by its caller: the lease commits only as it steps.
    stepped: bool,
}

impl PendingWriter {
    /// Whether the lease is answered: `writer` then returns at once.
    pub fn is_ready(&self) -> bool {
        self.pending.is_answered()
    }

    /// The writer, holding the lease once its entry commits; waits for it, unless its
    /// committer is stepped by the caller, who steps it until `is_ready` first.
    pub fn writer(self) -> Result<Writer, Error> {
        let PendingWriter {
            mut writer,
            pending,
            stepped,
        } = self;
        if stepped && !pending.is_answered() {
            return Err(Error::Busy(format!(
                "the lease on {} for source `{}` is proposed: step the replicas until it is ready",
                writer.name, writer.source
            )));
        }
        pending.wait()?;
        writer.epoch = lock(&writer.log.state)
            .leases
            .get(&writer.source)
            .copied()
            .expect("a lease applied records its epoch");
        Ok(writer)
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
    /// The highest seq this writer has sent, or (until it sends) the source's: the next
    /// unnumbered write's is one above. Its lock also keeps the writer's writes in order when
    /// shared between threads.
    last_seq: Mutex<Option<u64>>,
    in_flight: Arc<InFlight>,
    /// What a write says if the store closes before answering it.
    gone: Arc<str>,
}

impl Writer {
    /// The source's last sequence number written, on any date: a writer resumes from where its
    /// source left off.
    pub fn last_seq(&self) -> Option<u64> {
        source_last(&lock(&self.log.state), &self.source)
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
        let (current, table, seq, known) = {
            let state = lock(&self.log.state);
            // Where the source left off, until this writer has sent: a first write that resends
            // an old seq must not make the next unnumbered one reuse the numbers after it.
            let known = last.or_else(|| source_last(&state, &self.source));
            let seq = seq.unwrap_or_else(|| known.map_or(1, |last| last + 1));
            (state.entry.shape(), state.entry.id, seq, known)
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
        *last = Some(known.map_or(seq, |known| known.max(seq)));
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
        // Every write sent is logged or refused before the source is let go: a writer taking it
        // over numbers its writes after what the partitions hold, which must include these.
        let _ = self.in_flight.flush();
        lock(&self.log.state).writing.remove(&self.source);
        self.log.released.notify_all();
    }
}

/// Each source's last seq on a table's dropped dates, from the table's directory `dir`: none
/// before the first drop.
fn read_floors(disk: &DiskRef, dir: &Path) -> Result<HashMap<SourceId, u64>, Error> {
    let path = dir.join(FLOORS);
    let bytes = match disk.read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(Error::io(&path, e)),
    };
    let corrupt = || Error::Corrupt(format!("{}: unreadable", path.display()));
    let text = String::from_utf8(bytes).map_err(|_| corrupt())?;
    let mut lines = text.lines();
    if lines.next() != Some(FLOORS_HEADER) {
        return Err(corrupt());
    }
    lines
        .map(|line| {
            let (source, seq) = line.split_once(' ').ok_or_else(corrupt)?;
            Ok((
                SourceId::parse(source)?,
                seq.parse().map_err(|_| corrupt())?,
            ))
        })
        .collect()
}

/// Records `floors` in the table's directory `dir`, atomically and durably.
fn write_floors(disk: &DiskRef, dir: &Path, floors: &HashMap<SourceId, u64>) -> Result<(), Error> {
    let mut sorted: Vec<(&SourceId, &u64)> = floors.iter().collect();
    sorted.sort_unstable();
    let mut text = format!("{FLOORS_HEADER}\n");
    for (source, seq) in sorted {
        text.push_str(&format!("{} {seq}\n", source.as_str()));
    }
    let path = dir.join(FLOORS);
    let staged = dir.join(format!("{FLOORS}.new"));
    let mut file = disk
        .create(&staged, false)
        .map_err(|e| Error::io(&staged, e))?;
    file.write_all(text.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|e| Error::io(&staged, e))?;
    disk.rename(&staged, &path)
        .map_err(|e| Error::io(&path, e))?;
    disk.sync_dir(dir).map_err(|e| Error::io(dir, e))
}

/// In a date's directory, while it is being dropped: the drop's index (u64, little-endian).
const DROP_MARK: &str = "@drop";
const DROP_MARK_NEW: &str = "@drop.new";

/// Says durably that `key`, a date of `log`, is being dropped by the entry at `index`.
fn mark_drop(log: &Mutex<TableLog>, key: &str, index: u64) -> Result<(), Error> {
    let state = lock(log);
    let dir = state.dir.join(key);
    let staged = dir.join(DROP_MARK_NEW);
    let mut file = state
        .disk
        .create(&staged, false)
        .map_err(|e| Error::io(&staged, e))?;
    file.write_all(&index.to_le_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|e| Error::io(&staged, e))?;
    let path = dir.join(DROP_MARK);
    state
        .disk
        .rename(&staged, &path)
        .map_err(|e| Error::io(&path, e))?;
    state.disk.sync_dir(&dir).map_err(|e| Error::io(&dir, e))
}

/// Settles the drops of a table's dates (`mark_drop`) that a process stopped in: one the
/// partition files hold the log through (`flushed`) is past where the replay starts, so its
/// date is to be forgotten (`forget_date`), which it returns; one they do not is replayed,
/// and its mark goes.
fn settle_drops(disk: &DiskRef, dir: &Path, flushed: u64) -> Result<Vec<String>, Error> {
    let mut forgotten = Vec::new();
    let Ok(dates) = disk.list(dir) else {
        return Ok(forgotten);
    };
    for date in dates {
        let day = dir.join(&date);
        let mark = day.join(DROP_MARK);
        if Date::parse(&date).is_err() || !disk.exists(&mark) {
            continue;
        }
        let bytes = disk.read(&mark).map_err(|e| Error::io(&mark, e))?;
        let index = <[u8; 8]>::try_from(bytes.as_slice())
            .map(u64::from_le_bytes)
            .map_err(|_| Error::Corrupt(format!("{}: not a drop's index", mark.display())))?;
        if index <= flushed {
            forgotten.push(date);
        } else {
            disk.remove_file(&mark)
                .and_then(|()| disk.sync_dir(&day))
                .map_err(|e| Error::io(&mark, e))?;
        }
    }
    Ok(forgotten)
}

/// Forgets `key`, a dropped date of the table in `dir`. Each source's last seq on it outlives
/// it, durably first: `source_last` must not go back, or a resend of the date's writes would
/// be written again. Only then, since a replay of the date's writes takes those at or below
/// the floors for resends, and so only once no replay can start before the drop.
fn forget_date(
    disk: &DiskRef,
    dir: &Path,
    partitions: &mut BTreeMap<(String, Stream), Partition>,
    floors: &mut HashMap<SourceId, u64>,
    key: &str,
) -> Result<(), Error> {
    let mut raised = floors.clone();
    for ((day, stream), partition) in partitions.iter() {
        if let (true, Stream::Source(source), Some(last)) =
            (day == key, stream, partition.last_seq())
        {
            let floor = raised.entry(source.clone()).or_insert(last);
            *floor = (*floor).max(last);
        }
    }
    if raised != *floors {
        write_floors(disk, dir, &raised)?;
        *floors = raised;
    }
    partitions.retain(|(day, _), _| day != key);
    let day = dir.join(key);
    match disk.remove_dir_all(&day) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(Error::io(&day, e)),
    }
    // Gone durably: a power loss does not bring the date back.
    disk.sync_dir(dir).map_err(|e| Error::io(dir, e))
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

/// A source's last sequence number, over all its partitions and the dates dropped: sequence
/// numbers rise across a source's dates, so a write at or below it is a resend whatever its
/// date (§7.1).
fn source_last(log: &TableLog, source: &SourceId) -> Option<u64> {
    log.partitions
        .iter()
        .filter(|((_, stream), _)| matches!(stream, Stream::Source(s) if s == source))
        .filter_map(|(_, partition)| partition.last_seq())
        .max()
        .max(log.floors.get(source).copied())
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

    fn written(&self) -> Written {
        match self {
            Stream::Checkpoint => Written::Folded,
            Stream::Source(_) => Written::FromLog,
        }
    }
}

/// `{dir}/{date}/{stream}` directories that exist.
fn partition_dirs(disk: &DiskRef, dir: &Path) -> Result<Vec<(String, Stream)>, Error> {
    let mut found = Vec::new();
    let Ok(dates) = disk.list(dir) else {
        return Ok(found);
    };
    for date in dates {
        // Beside the dates, the table's own files (`@sources`).
        if Date::parse(&date).is_err() {
            continue;
        }
        let path = dir.join(&date);
        for name in disk.list(&path).map_err(|e| Error::io(&path, e))? {
            let stream = match name.as_str() {
                "@checkpoint" => Stream::Checkpoint,
                DROP_MARK | DROP_MARK_NEW => continue,
                name => Stream::Source(SourceId::parse(name)?),
            };
            found.push((date.clone(), stream));
        }
    }
    Ok(found)
}

/// Gives a keyed table's day `today` its checkpoint, unless it has one: every key's latest row,
/// folded from the latest earlier day weir holds (§7.3). Under the table's lock, since nothing
/// may write the day before its checkpoint; it happens on the day's first write or
/// subscription, or before an earlier day is dropped. It holds the table through the earlier
/// day's last batch, and says so: one rebuilt after a crash took the first (it is written after
/// the flush its position is covered by, so may not have been durable) is the same checkpoint,
/// before the day's changes.
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
    let mut through = 0;
    if let Some(day) = earlier {
        fold_latest(log, &day, &mut fold)?;
        through = log
            .partitions
            .iter()
            .filter(|((date, _), _)| *date == day)
            .filter_map(|(_, partition)| partition.last_order())
            .map(|(lsn, _)| lsn)
            .max()
            .unwrap_or(0);
    }
    write_checkpoint(log, today, &fold.state()?, through, ingest_ns)
}

/// Appends `state`, the table as of LSN `through`, to `day`'s checkpoints.
/// Subscribers are not told: a reader already following the table has every row it repeats.
fn write_checkpoint(
    log: &mut TableLog,
    day: &str,
    state: &RecordBatch,
    through: u64,
    ingest_ns: i64,
) -> Result<(), Error> {
    let key = (day.to_owned(), Stream::Checkpoint);
    if !log.partitions.contains_key(&key) {
        let path = log.dir.join(day).join(Stream::Checkpoint.dir_name());
        let partition = Partition::open(
            &log.disk,
            &log.root,
            &path,
            log.entry.shape(),
            u64::MAX,
            Written::Folded,
        )?;
        log.partitions.insert(key.clone(), partition);
    }
    let partition = log.partitions.get_mut(&key).expect("inserted above");
    let seq = partition.last_seq().map_or(1, |last| last + 1);
    partition.append(state, through, seq, ingest_ns, 0, Change::Checkpoint)?;
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

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("kermodes store poisoned by an earlier panic")
}
