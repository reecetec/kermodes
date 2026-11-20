//! weir under simulation (architecture.md §7.6): a stepped store on a simulated disk, driven by
//! a seed through writes, commits, flushes, clean restarts, processes killed (the page cache
//! staying), and power losses that keep a random part of what was not fsynced and tear the last
//! write. Whatever the seed, after every reopen the store holds exactly the writes it
//! acknowledged, each once and in order; and the same seed does exactly the same, so any
//! failure replays.
//!
//! Then a shard of three replicas, each on its own simulated disk, their Raft messages on a
//! simulated network that drops, duplicates, and reorders them and cuts a replica off, with
//! every replica's process killed or its power lost at any point, the leader's included.
//! Whenever they come to agree, every replica holds exactly the writes acknowledged, each once
//! and in order.
//!
//! Each test runs seeds 1 to 64; `KERMODES_SIM_SEEDS=N` runs 1 to N, for a longer search, and
//! `KERMODES_SIM_SEED=N` only seed N, to replay a failure.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_weir::{
    Change, Clock, Date, HistoryCommit, HistoryDate, HistoryFile, LogLimits, Pending,
    PendingWriter, Replica, SourceId, Store, TableName, Writer, replica_id, sim, slot,
};

const SOURCES: usize = 3;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-sim-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn ticks() -> TableName {
    TableName::parse("md.ticks").unwrap()
}

fn quotes() -> TableName {
    TableName::parse("md.quotes").unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ])
}

fn rows(k: Vec<i64>, v: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("k", Arc::new(Int64Array::from(k)) as ArrayRef),
        ("v", Arc::new(Int64Array::from(v))),
    ])
    .unwrap()
}

/// What the store was told and answered: the oracle.
#[derive(Default)]
struct Model {
    /// Each source's tick values acknowledged, in order (its `k` is the source).
    ticks: [Vec<i64>; SOURCES],
    /// The keyed table's state from the writes acknowledged: each source writes its own keys,
    /// so writers' order against each other does not matter, and a checkpoint is ordered
    /// against them all.
    quotes: BTreeMap<i64, i64>,
    /// Each writer's highest seq sent, by table: resends are drawn below it.
    sent: [[u64; SOURCES]; 2],
    /// Each writer's highest seq acknowledged, by table: where it resumes after a crash.
    acked: [[u64; SOURCES]; 2],
    /// Each (table, past date) whose history the catalog holds: committed once.
    merged: BTreeSet<(usize, Date)>,
    /// The ticks of each merged date, by source, as weir held them: read from history after.
    history: BTreeMap<Date, [Vec<i64>; SOURCES]>,
}

/// Each source's ticks in `batch`.
fn ticks_of(batch: &RecordBatch, into: &mut [Vec<i64>; SOURCES]) {
    let k = batch.column(0).as_primitive::<Int64Type>();
    let v = batch.column(1).as_primitive::<Int64Type>();
    for row in 0..batch.num_rows() {
        into[k.value(row) as usize].push(v.value(row));
    }
}

/// A write sent and not yet answered, with what it does if written.
struct Sent {
    pending: Pending,
    /// Its writer (table, source) and seq.
    by: (usize, usize, u64),
    /// `None` for a resend: answered `false`, nothing written.
    effect: Option<Effect>,
}

enum Effect {
    Ticks(usize, Vec<i64>),
    Quotes(Change, Vec<(i64, i64)>),
}

impl Model {
    fn apply(&mut self, effect: Effect) {
        match effect {
            Effect::Ticks(who, values) => self.ticks[who].extend(values),
            Effect::Quotes(change, rows) => {
                if change == Change::Checkpoint {
                    self.quotes.clear();
                }
                for (k, v) in rows {
                    if change == Change::Delete {
                        self.quotes.remove(&k);
                    } else {
                        self.quotes.insert(k, v);
                    }
                }
            }
        }
    }
}

/// Everything a fresh subscription to `name` is given.
fn read(store: &Store, name: &TableName) -> Vec<(Change, RecordBatch)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::downgrade(&seen);
    store
        .subscribe(
            name,
            Box::new(move |batch, stamp, _| {
                let Some(sink) = sink.upgrade() else {
                    return false;
                };
                sink.lock().unwrap().push((stamp.change, batch.clone()));
                true
            }),
            Box::new(|why| panic!("{why}")),
        )
        .unwrap();
    seen.lock().unwrap().clone()
}

/// What the store holds, against what it acknowledged: an append table's past dates read whole
/// (from history once merged, each date from one place), then today's as a live reader is given
/// it.
fn check(store: &Store, model: &Model, seed: u64, when: &str) {
    let mut held: [Vec<i64>; SOURCES] = Default::default();
    let mut dates: BTreeSet<Date> = store.past_dates(&ticks()).unwrap().into_iter().collect();
    dates.extend(model.history.keys().copied());
    for date in dates {
        match model.history.get(&date) {
            Some(merged) => {
                for (who, values) in merged.iter().enumerate() {
                    held[who].extend(values);
                }
            }
            None => store
                .read_date(&ticks(), date, &mut |batch, _| {
                    ticks_of(&batch, &mut held);
                    Ok(())
                })
                .unwrap(),
        }
    }
    for (_, batch) in read(store, &ticks()) {
        ticks_of(&batch, &mut held);
    }
    assert_eq!(held, model.ticks, "seed {seed}, {when}: ticks");
    let mut state = BTreeMap::new();
    for (change, batch) in read(store, &quotes()) {
        if change == Change::Checkpoint {
            state.clear();
        }
        let k = batch.column(0).as_primitive::<Int64Type>();
        let v = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            if change == Change::Delete {
                state.remove(&k.value(row));
            } else {
                state.insert(k.value(row), v.value(row));
            }
        }
    }
    assert_eq!(state, model.quotes, "seed {seed}, {when}: quotes");
}

/// A store and its writers, one per source and table.
struct Open {
    store: Store,
    writers: Vec<[Writer; 2]>,
}

/// Opens the store and its writers. Should the process crash while it recovers (a crash still
/// armed), the next recovers again from what that left. Should an fsync have failed
/// (`failing`), weir opens only once the machine restarts, which a power loss is.
fn open(root: &std::path::Path, disk: &sim::Disk, clock: &Clock, seed: u64, failing: bool) -> Open {
    let mut reboots = 0;
    loop {
        let crashes = disk.crashes();
        // Taking a writer's lease writes to the log, as a write does: a failed fsync stops weir
        // there too, and it is restarted.
        let opened = Store::open_stepped(root, disk.handle(), clock.clone()).and_then(|store| {
            store.create_table(&ticks(), &schema())?;
            store.create_keyed_table(&quotes(), &schema(), &["k".to_owned()])?;
            let writers = (0..SOURCES)
                .map(|who| {
                    let source = SourceId::parse(&format!("s{who}")).unwrap();
                    Ok([
                        store.writer(&ticks(), &source, false)?,
                        store.writer(&quotes(), &source, false)?,
                    ])
                })
                .collect::<Result<Vec<_>, kermodes_weir::Error>>()?;
            Ok(Open { store, writers })
        });
        match opened {
            Ok(at) => {
                disk.disarm();
                return at;
            }
            Err(_) if disk.crashes() > crashes => continue,
            // Each open fsyncs a few dozen times, one in 30 failing: a hundred restarts in a row
            // that all fail is an error that is not the disk's.
            Err(_) if failing && reboots < 100 => {
                reboots += 1;
                disk.power_loss();
            }
            Err(e) => panic!("seed {seed}: reopening: {e}"),
        }
    }
}

/// A write whose answer leaves its fate open (its process stopped while it was committed): its
/// writer (table, source), seq, and what it does if it reached the log.
type Maybe = ((usize, usize, u64), Option<Effect>);

/// Answers every write sent: the store has committed them (`step`). Those that failed because
/// the process stopped (`crashed`) may or may not be in the log: returned, to resolve on
/// reopening.
fn settle(
    sent: &mut Vec<Sent>,
    model: &mut Model,
    trace: &mut Vec<u64>,
    seed: u64,
    crashed: bool,
) -> Vec<Maybe> {
    let mut maybes = Vec::new();
    for Sent {
        pending,
        by: (table, who, seq),
        effect,
    } in sent.drain(..)
    {
        let written = match pending.wait() {
            Ok(written) => written,
            Err(_) if crashed => {
                maybes.push(((table, who, seq), effect));
                continue;
            }
            Err(e) => panic!("seed {seed}: a committed write failed: {e}"),
        };
        assert_eq!(
            written,
            effect.is_some(),
            "seed {seed}: the answer to table {table}, source {who}, seq {seq} (acked up to {})",
            model.acked[table][who]
        );
        trace.push(u64::from(written));
        if let Some(effect) = effect {
            model.acked[table][who] = model.acked[table][who].max(seq);
            model.apply(effect);
        }
    }
    maybes
}

/// After a crash, or weir stopping: the process is gone (its store and writers, every write it
/// had sent failing), and a new one opens the store. Of the writes whose fate was open, those
/// that reached the log are written, which their writers' last seq says: each source's writes
/// reach the log in the order sent, so a prefix of them did. Then the store must hold exactly
/// what was acknowledged and those.
#[allow(clippy::too_many_arguments)]
fn recover(
    at: Open,
    mut maybes: Vec<Maybe>,
    sent: &mut Vec<Sent>,
    model: &mut Model,
    trace: &mut Vec<u64>,
    root: &std::path::Path,
    disk: &sim::Disk,
    clock: &Clock,
    seed: u64,
    failing: bool,
) -> Open {
    // The store first, whose closing answers every write sent: refused if its process is gone,
    // committed if weir only stopped flushing. Then the writers, which wait for those answers.
    let Open { store, writers } = at;
    drop(store);
    drop(writers);
    maybes.extend(settle(sent, model, trace, seed, true));
    disk.disarm();
    let at = open(root, disk, clock, seed, failing);
    for ((table, who, seq), effect) in maybes {
        let held = at.writers[who][table].last_seq().unwrap_or(0);
        if let Some(effect) = effect.filter(|_| seq <= held) {
            model.acked[table][who] = model.acked[table][who].max(seq);
            model.apply(effect);
        }
    }
    // The writers resume from what the store holds: resends are drawn below it.
    model.sent = model.acked;
    check(&at.store, model, seed, "after a crash");
    at
}

/// Runs `seed` for `actions` actions, one fsync in `failing_syncs` failing (0: none); what it
/// left: the disk's fingerprint and the answers.
fn simulate(seed: u64, actions: usize, failing_syncs: u64) -> (u64, Vec<u64>) {
    let dir = TempDir::new();
    let root = dir.0.clone();
    let failing = failing_syncs > 0;
    let disk = sim::Disk::new(seed, failing_syncs, &root);
    // A minute before midnight UTC: runs cross a date.
    let now = Arc::new(AtomicI64::new(1_767_225_540_000_000_000));
    let clock: Clock = {
        let now = now.clone();
        Arc::new(move || now.load(Ordering::Relaxed))
    };
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut model = Model::default();
    let mut trace = Vec::new();
    let mut sent: Vec<Sent> = Vec::new();
    let mut value = 0_i64;
    let mut at = open(&root, &disk, &clock, seed, failing);
    for _ in 0..actions {
        now.fetch_add(1_000_000 * (1 + rng.below(500)) as i64, Ordering::Relaxed);
        // Now and then the process dies, or the power goes, in the middle of what the store
        // does next.
        let crashes = disk.crashes();
        match rng.below(16) {
            0 => disk.power_loss_after(rng.below(24)),
            1 => disk.kill_after(rng.below(24)),
            _ => {}
        }
        let crashed = || disk.crashes() > crashes;
        let mut maybes = Vec::new();
        // Weir stopped (a flush failed): it is restarted, as Kubernetes would.
        let mut stopped = false;
        match rng.below(100) {
            0..=59 => send(&mut rng, &mut model, &mut value, &at.writers, &mut sent),
            // Commit what was sent.
            60..=79 => {
                at.store.step();
                maybes = settle(
                    &mut sent,
                    &mut model,
                    &mut trace,
                    seed,
                    crashed() || failing,
                );
            }
            // Flush the partition files and trim the log.
            80..=89 => {
                let flushed = at.store.flush_now();
                assert!(
                    flushed.is_ok() || crashed() || failing,
                    "seed {seed}: flushing: {flushed:?}"
                );
                stopped = flushed.is_err();
            }
            // Restart cleanly: what was sent is committed first.
            90..=92 => {
                at.store.step();
                maybes = settle(
                    &mut sent,
                    &mut model,
                    &mut trace,
                    seed,
                    crashed() || failing,
                );
                if !crashed() && maybes.is_empty() {
                    drop(at);
                    at = open(&root, &disk, &clock, seed, failing);
                    check(&at.store, &model, seed, "after a restart");
                }
            }
            // Merge each table's past dates and drop weir's copy, as the merger does: the
            // catalog is told its history holds weir's rows of the date (the files are not the
            // point here), then weir deletes them.
            93..=94 => {
                'merge: for (table, name) in [ticks(), quotes()].iter().enumerate() {
                    for date in at.store.past_dates(name).unwrap() {
                        if !model.merged.contains(&(table, date)) {
                            if table == 0 {
                                let mut values: [Vec<i64>; SOURCES] = Default::default();
                                at.store
                                    .read_date(name, date, &mut |batch, _| {
                                        ticks_of(&batch, &mut values);
                                        Ok(())
                                    })
                                    .unwrap();
                                model.history.insert(date, values);
                            }
                            let rows = at.store.date_rows(name, date).unwrap().0;
                            let file = format!("{date}.parquet");
                            let commit = HistoryCommit {
                                written: vec![date],
                                replaced: Vec::new(),
                                files: vec![HistoryFile {
                                    name: file.clone(),
                                    first: date,
                                    last: date,
                                    rows,
                                    bytes: 0,
                                }],
                                dates: vec![HistoryDate {
                                    date,
                                    file,
                                    rows,
                                    bytes: 0,
                                }],
                            };
                            at.store.commit_history(name, &commit).unwrap();
                            model.merged.insert((table, date));
                        }
                        if let Err(e) = at.store.drop_date(name, date) {
                            assert!(crashed() || failing, "seed {seed}: dropping {date}: {e}");
                            stopped = !crashed();
                            break 'merge;
                        }
                    }
                }
            }
            // The process dies between two operations (`kill -9`).
            95..=96 => disk.kill(),
            // The power goes between two operations.
            _ => disk.power_loss(),
        }
        // A write that failed (an fsync with it) stopped weir as a failed flush does: it is
        // restarted, and opens only once the machine restarts.
        if crashed() || stopped || !maybes.is_empty() {
            at = recover(
                at, maybes, &mut sent, &mut model, &mut trace, &root, &disk, &clock, seed, failing,
            );
        }
        disk.disarm();
    }
    at.store.step();
    let maybes = settle(&mut sent, &mut model, &mut trace, seed, failing);
    if !maybes.is_empty() {
        at = recover(
            at, maybes, &mut sent, &mut model, &mut trace, &root, &disk, &clock, seed, failing,
        );
    }
    check(&at.store, &model, seed, "at the end");
    drop(at);
    (disk.fingerprint(&root), trace)
}

/// Sends a write by a random writer: new, or now and then one already sent (a resend).
fn send(
    rng: &mut Rng,
    model: &mut Model,
    value: &mut i64,
    writers: &[[Writer; 2]],
    sent: &mut Vec<Sent>,
) {
    let who = rng.below(SOURCES as u64) as usize;
    let table = usize::from(rng.below(2) == 1);
    let last = model.sent[table][who];
    let resend = last > 0 && rng.below(8) == 0;
    let seq = if resend {
        1 + rng.below(last)
    } else {
        model.sent[table][who] += 1;
        model.sent[table][who]
    };
    let (change, batch, effect) = if table == 0 {
        let n = 1 + rng.below(3) as usize;
        let values: Vec<i64> = (*value..*value + n as i64).collect();
        *value += n as i64;
        let batch = rows(vec![who as i64; n], values.clone());
        (Change::Rows, batch, Effect::Ticks(who, values))
    } else {
        let key = |n: u64| who as i64 * 4 + (n % 4) as i64;
        let n = rng.below(4);
        *value += 1;
        let (change, ks, vs) = match rng.below(10) {
            0 => (
                Change::Checkpoint,
                vec![key(n), key(n + 1)],
                vec![*value; 2],
            ),
            1..=3 => (Change::Delete, vec![key(n)], vec![0]),
            _ => (
                Change::Upsert,
                vec![key(n), key(n + 2)],
                vec![*value, -*value],
            ),
        };
        let effect = Effect::Quotes(change, ks.iter().copied().zip(vs.clone()).collect());
        (change, rows(ks, vs), effect)
    };
    // A new write unnumbered, as a producer sends one; a resend with its seq.
    let pending = writers[who][table]
        .send(change, &batch, resend.then_some(seq))
        .unwrap();
    sent.push(Sent {
        pending,
        by: (table, who, seq),
        effect: (!resend).then_some(effect),
    });
}

const REPLICAS: usize = 3;

/// Small enough that segments roll every few dozen writes. On even seeds a replica keeps no log
/// its partition files hold, so one behind catches up from a peer's files; on odd seeds it keeps
/// some, so one a little behind catches up from the log, read back from disk.
fn limits(seed: u64) -> LogLimits {
    LogLimits {
        segment_bytes: 4 << 10,
        retain_bytes: if seed.is_multiple_of(2) { 0 } else { 64 << 10 },
    }
}

/// A shard's replicas, each on its own simulated disk, and the network between them.
struct Shard {
    dir: PathBuf,
    seed: u64,
    clock: Clock,
    disks: Vec<sim::Disk>,
    /// Replica `i + 1`'s store at `i`; `None` while its process is down.
    stores: Vec<Option<Store>>,
    /// Messages sent and not yet delivered: from, to, and the message.
    wire: Vec<(u64, u64, Vec<u8>)>,
    /// The replica cut off from the others, if one is.
    cut: Option<u64>,
    /// The writers, one per source and table, on the replica (its index) that led when they
    /// opened.
    writers: Option<(usize, Vec<[Writer; 2]>)>,
    /// Snapshots installed so far: every other one is cut short by its process dying.
    installs: u64,
}

impl Shard {
    /// Starts the first `started` replicas (the others start when the shard next agrees), then
    /// creates the tables: a follower finds them when the first writes reach it.
    fn new(dir: &Path, seed: u64, clock: Clock, started: usize) -> Self {
        let disks = (0..REPLICAS)
            .map(|i| {
                let root = dir.join(format!("r{}", i + 1));
                fs::create_dir_all(&root).unwrap();
                sim::Disk::new(seed * 8 + i as u64, 0, &root)
            })
            .collect();
        let mut shard = Self {
            dir: dir.to_owned(),
            seed,
            clock,
            disks,
            stores: (0..REPLICAS).map(|_| None).collect(),
            wire: Vec::new(),
            cut: None,
            writers: None,
            installs: 0,
        };
        for i in 0..started {
            shard.start(i);
        }
        let store = shard.stores[0].as_ref().unwrap();
        store.create_table(&ticks(), &schema()).unwrap();
        store
            .create_keyed_table(&quotes(), &schema(), &["k".to_owned()])
            .unwrap();
        shard
    }

    /// Starts replica `i`'s process, which recovers from what its disk holds. Should it crash
    /// while it recovers (a crash still armed), the next recovers again from what that left.
    fn start(&mut self, i: usize) {
        let disk = &self.disks[i];
        let store = loop {
            let crashes = disk.crashes();
            match self.open(i) {
                Ok(store) => break store,
                Err(_) if disk.crashes() > crashes => continue,
                Err(e) => panic!("seed {}: replica {}: reopening: {e}", self.seed, i + 1),
            }
        };
        disk.disarm();
        self.stores[i] = Some(store);
    }

    /// Opens replica `i`'s store, as its process does when it starts.
    fn open(&self, i: usize) -> Result<Store, kermodes_weir::Error> {
        let replica = Replica {
            id: i as u64 + 1,
            voters: (1..=REPLICAS as u64).collect(),
            seed: Some(self.seed * 8 + i as u64),
            limits: limits(self.seed),
        };
        let root = self.dir.join(format!("r{}", i + 1));
        let catalog = self.dir.join("catalog.sqlite");
        Store::open_replica(
            &root,
            self.disks[i].handle(),
            self.clock.clone(),
            &catalog,
            replica,
        )
    }

    /// Replica `i`'s process is gone: its store first, whose closing answers every write it
    /// had, then the writers on it, which wait for those answers.
    fn stop(&mut self, i: usize) {
        drop(self.stores[i].take());
        if self.writers.as_ref().is_some_and(|(at, _)| *at == i) {
            self.writers = None;
        }
    }

    /// Puts what replica `i` has to send on the wire.
    fn collect(&mut self, i: usize) {
        if let Some(store) = &self.stores[i] {
            for (to, message) in store.take_messages() {
                self.wire.push((i as u64 + 1, to, message));
            }
        }
    }

    /// Delivers a message, unless its sender or receiver is cut off or its receiver is down.
    fn deliver(&mut self, (from, to, message): (u64, u64, Vec<u8>)) {
        if self.cut.is_some_and(|cut| cut == from || cut == to) {
            return;
        }
        let i = slot(to) as usize - 1;
        if let Some(store) = &self.stores[i] {
            store.receive(&message).unwrap();
            self.collect(i);
        }
    }

    /// One tick of every running replica's Raft clock.
    fn tick(&mut self) {
        for i in 0..REPLICAS {
            if let Some(store) = &self.stores[i] {
                store.tick();
            }
            self.collect(i);
        }
    }

    /// Every replica whose process died since `before` (each disk's crashes) is down.
    fn reap(&mut self, before: &[u64]) {
        for (i, &crashes) in before.iter().enumerate() {
            if self.disks[i].crashes() > crashes {
                self.stop(i);
            }
            self.disks[i].disarm();
        }
    }

    /// Every running replica too far behind the log to catch up from it copies the partition
    /// files of the replica its leader named, if that one is running, and starts again. One
    /// whose copy is cut short, or with no one to copy from, starts again as it was, and its
    /// leader hands it a snapshot again.
    fn catch_up(&mut self) {
        for i in 0..REPLICAS {
            let Some(wanted) = self.stores[i].as_ref().and_then(Store::snapshot_wanted) else {
                continue;
            };
            self.stop(i);
            let root = self.dir.join(format!("r{}", i + 1));
            if let Some(from) = &self.stores[slot(wanted.from) as usize - 1] {
                // Every other copy has its process die partway, at any of its operations
                // that change the disk, the swap's included (one with fewer runs whole).
                self.installs += 1;
                if self.installs.is_multiple_of(2) {
                    self.disks[i].kill_after(self.installs * 37 % 256);
                }
                let crashes = self.disks[i].crashes();
                let disk = self.disks[i].handle();
                let installed =
                    Store::install_snapshot(&root, disk, limits(self.seed), &wanted, from);
                assert!(
                    installed.is_ok() || self.disks[i].crashes() > crashes,
                    "seed {}: replica {} installing from {}: {installed:?}",
                    self.seed,
                    i + 1,
                    wanted.from
                );
            }
            self.disks[i].disarm();
            self.start(i);
        }
    }

    /// Every replica up and connected until they agree: one leader, every replica a voter (a new
    /// incarnation taken in by the leader, then promoted) and applied as far as it, for rounds
    /// on end, and every write sent answered. The leader.
    fn agree(&mut self, sent: &[Sent]) -> usize {
        for i in 0..REPLICAS {
            if self.stores[i].is_none() {
                self.start(i);
            }
        }
        self.cut = None;
        let mut last = None;
        let mut same = 0;
        for _ in 0..2_000 {
            for i in 0..REPLICAS {
                self.stores[i].as_ref().unwrap().step();
                self.collect(i);
            }
            self.tick();
            self.catch_up();
            while !self.wire.is_empty() {
                let message = self.wire.remove(0);
                self.deliver(message);
            }
            let stores: Vec<&Store> = self.stores.iter().map(|s| s.as_ref().unwrap()).collect();
            let leaders: Vec<usize> = (0..REPLICAS).filter(|&i| stores[i].is_leader()).collect();
            // A replica whose disk was lost asks the leader to take it in, as its process does.
            if let [leader] = leaders[..] {
                for store in &stores {
                    if !store.is_member() {
                        let _ = stores[leader].join(store.replica_id());
                    }
                }
            }
            let applied: Vec<u64> = stores.iter().map(|store| store.applied()).collect();
            let now = (leaders.clone(), applied.clone());
            let agreed = leaders.len() == 1
                && stores.iter().all(|store| store.is_voter())
                && applied.iter().all(|&a| a == applied[0])
                && sent.iter().all(|s| s.pending.is_answered());
            same = if agreed && last.as_ref() == Some(&now) {
                same + 1
            } else {
                0
            };
            if same == 2 {
                return leaders[0];
            }
            last = Some(now);
        }
        let state: Vec<String> = self
            .stores
            .iter()
            .map(|s| {
                let s = s.as_ref().unwrap();
                format!(
                    "id {} leader {} member {} voter {} applied {}",
                    s.replica_id(),
                    s.is_leader(),
                    s.is_member(),
                    s.is_voter(),
                    s.applied()
                )
            })
            .collect();
        panic!(
            "seed {}: the replicas never came to agree: {state:?}",
            self.seed
        );
    }

    /// Once the replicas agree (`agree`): of the writes whose fate was open, those that
    /// reached the log are written, which their writers' last seq on the leader says (each
    /// source's writes reach the log in the order sent, so a prefix of them did). The writers
    /// open again on the leader, and every replica must hold exactly what was acknowledged
    /// and those.
    fn resolve(
        &mut self,
        mut maybes: Vec<Maybe>,
        sent: &mut Vec<Sent>,
        model: &mut Model,
        trace: &mut Vec<u64>,
    ) {
        let mut leader = self.agree(sent);
        maybes.extend(answers(sent, model, trace, self.seed));
        self.writers = None;
        // A lease refused (the leader was handing over its lead, or lost it) is taken again
        // once the replicas agree again.
        let mut tries = 0;
        let writers = loop {
            match self.lease(leader) {
                Ok(writers) => break writers,
                Err(_) if tries < 10 => {
                    tries += 1;
                    leader = self.agree(sent);
                }
                Err(e) => panic!("seed {}: a lease: {e}", self.seed),
            }
        };
        for ((table, who, seq), effect) in maybes {
            let held = writers[who][table].last_seq().unwrap_or(0);
            if let Some(effect) = effect.filter(|_| seq <= held) {
                model.acked[table][who] = model.acked[table][who].max(seq);
                model.apply(effect);
            }
        }
        model.sent = model.acked;
        self.writers = Some((leader, writers));
        // As the merger would: a date written to after it was merged is merged again.
        self.merge(model);
        let held = |store: &Store| {
            let ticks = store.past_dates(&ticks()).unwrap();
            (ticks, store.past_dates(&quotes()).unwrap())
        };
        let first = held(self.stores[0].as_ref().unwrap());
        for (i, store) in self.stores.iter().enumerate() {
            let store = store.as_ref().unwrap();
            let when = format!("replica {} once agreed", i + 1);
            check(store, model, self.seed, &when);
            // Every replica dropped the same dates, where its log said to.
            assert_eq!(held(store), first, "seed {}: {when}: dates held", self.seed);
        }
    }

    /// Every writer's lease, taken on replica `leader` (`Store::lease`): its entries proposed,
    /// then the messages carried and the replicas stepped until they commit.
    fn lease(&mut self, leader: usize) -> Result<Vec<[Writer; 2]>, kermodes_weir::Error> {
        let store = self.stores[leader].as_ref().unwrap();
        let pending = (0..SOURCES)
            .map(|who| {
                let source = SourceId::parse(&format!("s{who}")).unwrap();
                Ok([
                    store.lease(&ticks(), &source, false)?,
                    store.lease(&quotes(), &source, false)?,
                ])
            })
            .collect::<Result<Vec<[PendingWriter; 2]>, kermodes_weir::Error>>()?;
        for _ in 0..100 {
            if pending.iter().flatten().all(PendingWriter::is_ready) {
                break;
            }
            for i in 0..REPLICAS {
                self.collect(i);
            }
            while !self.wire.is_empty() {
                let message = self.wire.remove(0);
                self.deliver(message);
            }
        }
        pending
            .into_iter()
            .map(|[on_ticks, on_quotes]| Ok([on_ticks.writer()?, on_quotes.writer()?]))
            .collect()
    }

    /// Merges each table's past dates on the writers' replica, as the merger does: a date whose
    /// rows in weir are not what its history holds (not merged yet, or written to since, by
    /// writes stamped on it that committed late) is merged, replacing what history held of it
    /// (the catalog told so; the files are not the point here). Then weir's copy is dropped,
    /// which every replica does once the drop commits, if the date still holds those rows.
    fn merge(&self, model: &mut Model) {
        let (at, _) = self.writers.as_ref().expect("resolved");
        let Some(store) = &self.stores[*at] else {
            return;
        };
        for (table, name) in [ticks(), quotes()].iter().enumerate() {
            let history = store.history(name).unwrap();
            for date in store.past_dates(name).unwrap() {
                let rows = store.date_rows(name, date).unwrap().0;
                let held = history.iter().find(|held| held.date == date);
                if held.map(|held| held.rows) != Some(rows) {
                    if table == 0 {
                        let mut values: [Vec<i64>; SOURCES] = Default::default();
                        store
                            .read_date(name, date, &mut |batch, _| {
                                ticks_of(&batch, &mut values);
                                Ok(())
                            })
                            .unwrap();
                        model.history.insert(date, values);
                    }
                    let file = format!("{date}-{rows}.parquet");
                    let commit = HistoryCommit {
                        written: vec![date],
                        replaced: held.map(|held| held.file.clone()).into_iter().collect(),
                        files: vec![HistoryFile {
                            name: file.clone(),
                            first: date,
                            last: date,
                            rows,
                            bytes: 0,
                        }],
                        dates: vec![HistoryDate {
                            date,
                            file,
                            rows,
                            bytes: 0,
                        }],
                    };
                    store.commit_history(name, &commit).unwrap();
                    model.merged.insert((table, date));
                }
                // Refused for now (the lead is being handed over, or lost): the next merge
                // drops it, as the merger's client tries the next replica.
                match store.drop_date(name, date) {
                    Ok(())
                    | Err(
                        kermodes_weir::Error::Busy(_)
                        | kermodes_weir::Error::Io(_)
                        | kermodes_weir::Error::NotLeader(_),
                    ) => {}
                    Err(e) => panic!("seed {}: dropping {name} on {date}: {e}", self.seed),
                }
            }
        }
    }
}

/// Takes the answers in so far: written as the model says, or a fate left open (a replica
/// refused it, lost the lead, or crashed), to resolve once the replicas agree.
fn answers(sent: &mut Vec<Sent>, model: &mut Model, trace: &mut Vec<u64>, seed: u64) -> Vec<Maybe> {
    let mut maybes = Vec::new();
    let mut waiting = Vec::new();
    for one in sent.drain(..) {
        if !one.pending.is_answered() {
            waiting.push(one);
            continue;
        }
        let Sent {
            pending,
            by: (table, who, seq),
            effect,
        } = one;
        let Ok(written) = pending.wait() else {
            maybes.push(((table, who, seq), effect));
            continue;
        };
        assert_eq!(
            written,
            effect.is_some(),
            "seed {seed}: the answer to table {table}, source {who}, seq {seq} (acked up to {})",
            model.acked[table][who]
        );
        trace.push(u64::from(written));
        if let Some(effect) = effect {
            model.acked[table][who] = model.acked[table][who].max(seq);
            model.apply(effect);
        }
    }
    *sent = waiting;
    maybes
}

/// Runs a shard of three for `seed`, `actions` actions; what it left: each disk's fingerprint,
/// and the answers.
fn simulate_shard(seed: u64, actions: usize) -> (Vec<u64>, Vec<u64>) {
    let dir = TempDir::new();
    let now = Arc::new(AtomicI64::new(1_767_225_540_000_000_000));
    let clock: Clock = {
        let now = now.clone();
        Arc::new(move || now.load(Ordering::Relaxed))
    };
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut model = Model::default();
    let mut trace = Vec::new();
    let mut sent: Vec<Sent> = Vec::new();
    let mut value = 0_i64;
    let mut shard = Shard::new(&dir.0, seed, clock, REPLICAS);
    shard.resolve(Vec::new(), &mut sent, &mut model, &mut trace);
    let mut lost = 0;
    for _ in 0..actions {
        now.fetch_add(1_000_000 * (1 + rng.below(500)) as i64, Ordering::Relaxed);
        let before: Vec<u64> = shard.disks.iter().map(sim::Disk::crashes).collect();
        let victim = rng.below(REPLICAS as u64) as usize;
        // Now and then a replica's process dies, or its power goes, in the middle of what it
        // does next.
        match rng.below(64) {
            0 => shard.disks[victim].power_loss_after(rng.below(24)),
            1 => shard.disks[victim].kill_after(rng.below(24)),
            _ => {}
        }
        match rng.below(100) {
            0..=34 => {
                let (_, writers) = shard.writers.as_ref().expect("resolved");
                send(&mut rng, &mut model, &mut value, writers, &mut sent);
            }
            // The writers' replica takes what they sent: proposes it, if it leads.
            35..=42 => {
                let (at, _) = shard.writers.as_ref().expect("resolved");
                let at = *at;
                shard.stores[at].as_ref().unwrap().step();
                shard.collect(at);
            }
            // Messages arrive, in any order; some are lost, some come twice.
            43..=74 => {
                for _ in 0..1 + rng.below(6) {
                    if shard.wire.is_empty() {
                        break;
                    }
                    let at = rng.below(shard.wire.len() as u64) as usize;
                    match rng.below(20) {
                        0 => drop(shard.wire.swap_remove(at)),
                        1 => shard.deliver(shard.wire[at].clone()),
                        _ => {
                            let message = shard.wire.swap_remove(at);
                            shard.deliver(message);
                        }
                    }
                }
            }
            75..=85 => shard.tick(),
            // The leader hands over its lead, as before a planned restart.
            86 => {
                if let Some(store) = &shard.stores[victim] {
                    store.hand_over();
                    shard.collect(victim);
                }
            }
            // A replica is cut off from the others, or the network heals.
            87..=88 => shard.cut = (victim > 0).then_some(victim as u64),
            89..=91 => {
                if let Some(store) = &shard.stores[victim] {
                    let flushed = store.flush_now();
                    let crashed = shard.disks[victim].crashes() > before[victim];
                    assert!(
                        flushed.is_ok() || crashed,
                        "seed {seed}: flushing: {flushed:?}"
                    );
                }
            }
            // A clean restart.
            92..=93 => {
                if shard.stores[victim].is_some() {
                    shard.stop(victim);
                    shard.start(victim);
                }
            }
            // The process dies between two operations (`kill -9`).
            94..=95 => shard.disks[victim].kill(),
            // The power goes between two operations.
            96 => shard.disks[victim].power_loss(),
            // Past dates merged, and weir's copy dropped.
            97 => shard.merge(&mut model),
            // A replica's disk is lost, and it starts on a new one: a new member of its shard.
            // Only while every replica votes: with two of three lost before the first is
            // replaced, what only they held is gone, which no shard of three survives.
            98 if shard
                .stores
                .iter()
                .all(|store| store.as_ref().is_some_and(Store::is_voter)) =>
            {
                lost += 1;
                shard.stop(victim);
                let root = dir.0.join(format!("r{}", victim + 1));
                shard.disks[victim] =
                    sim::Disk::new(seed * 8 + victim as u64 + 64 * lost, 0, &root);
            }
            // The replicas that are down start again.
            _ => {
                for i in 0..REPLICAS {
                    if shard.stores[i].is_none() {
                        shard.start(i);
                    }
                }
            }
        }
        shard.reap(&before);
        shard.catch_up();
        let maybes = answers(&mut sent, &mut model, &mut trace, seed);
        if !maybes.is_empty() || shard.writers.is_none() {
            shard.resolve(maybes, &mut sent, &mut model, &mut trace);
        }
    }
    shard.resolve(Vec::new(), &mut sent, &mut model, &mut trace);
    let fingerprints = (0..REPLICAS)
        .map(|i| shard.disks[i].fingerprint(&dir.0.join(format!("r{}", i + 1))))
        .collect();
    drop(shard);
    (fingerprints, trace)
}

/// A replica whose disk was lost or replaced comes back as its slot's next incarnation, a new
/// member: the leader takes it in as a learner, then makes it a voter in place of the
/// incarnation it replaced once it has caught up. A second disk lost after leaves a majority of
/// voters only because the first replacement was made one.
#[test]
fn a_replica_that_lost_its_disk_rejoins_as_a_new_member() {
    let dir = TempDir::new();
    let clock: Clock = Arc::new(|| 1_767_225_540_000_000_000);
    let mut shard = Shard::new(&dir.0, 1, clock, REPLICAS);
    let (mut sent, mut model, mut trace) = (Vec::new(), Model::default(), Vec::new());
    shard.resolve(Vec::new(), &mut sent, &mut model, &mut trace);
    for lost in [1, 2] {
        shard.stop(lost);
        let root = dir.0.join(format!("r{}", lost + 1));
        shard.disks[lost] = sim::Disk::new(99 + lost as u64, 0, &root);
        shard.resolve(Vec::new(), &mut sent, &mut model, &mut trace);
        let store = shard.stores[lost].as_ref().unwrap();
        assert_eq!(store.replica_id(), replica_id(lost as u64 + 1, 1));
        assert!(store.is_voter());
    }
}

/// A leader hands its lead to another replica without an election: no tick passes, so none
/// can time out, and another replica leads once the messages arrive.
#[test]
fn a_leader_hands_over_its_lead() {
    let dir = TempDir::new();
    let clock: Clock = Arc::new(|| 1_767_225_540_000_000_000);
    let mut shard = Shard::new(&dir.0, 1, clock, REPLICAS);
    let (mut sent, mut model, mut trace) = (Vec::new(), Model::default(), Vec::new());
    shard.resolve(Vec::new(), &mut sent, &mut model, &mut trace);
    let (was, _) = shard.writers.as_ref().expect("resolved");
    let was = *was;
    shard.stores[was].as_ref().unwrap().hand_over();
    shard.collect(was);
    for _ in 0..20 {
        while !shard.wire.is_empty() {
            let message = shard.wire.remove(0);
            shard.deliver(message);
        }
        for i in 0..REPLICAS {
            shard.stores[i].as_ref().unwrap().step();
            shard.collect(i);
        }
    }
    let leaders: Vec<usize> = (0..REPLICAS)
        .filter(|&i| shard.stores[i].as_ref().unwrap().is_leader())
        .collect();
    assert_eq!(leaders.len(), 1, "{leaders:?}");
    assert_ne!(leaders[0], was);
    // A write to the replica that no longer leads is refused as not leading, which sends a
    // remote writer to the leader to resend it, rather than failing it as refused for good.
    let (_, writers) = shard.writers.as_ref().expect("resolved");
    let pending = writers[0][0]
        .send(Change::Rows, &rows(vec![0], vec![1]), None)
        .unwrap();
    shard.stores[was].as_ref().unwrap().step();
    let refused = pending.wait();
    assert!(
        matches!(refused, Err(kermodes_weir::Error::NotLeader(_))),
        "{refused:?}"
    );
}

/// A replica that never had a log joins its shard however late, the others having elected a
/// leader and committed without it (a StatefulSet starts its pods one at a time), and catches
/// up: it has never voted, so it has no vote to have lost.
#[test]
fn a_replica_that_starts_late_joins_its_shard() {
    let dir = TempDir::new();
    let clock: Clock = Arc::new(|| 1_767_225_540_000_000_000);
    let mut shard = Shard::new(&dir.0, 1, clock, 2);
    for _ in 0..200 {
        for i in 0..2 {
            shard.stores[i].as_ref().unwrap().step();
            shard.collect(i);
        }
        shard.tick();
        while !shard.wire.is_empty() {
            let message = shard.wire.remove(0);
            shard.deliver(message);
        }
    }
    let first = shard.stores[0].as_ref().unwrap();
    assert!(first.applied() > 0, "two of three replicas commit");
    // Replica 3 starts as the shard agrees, which checks every replica holds the same.
    let (mut sent, mut model, mut trace) = (Vec::new(), Model::default(), Vec::new());
    shard.resolve(Vec::new(), &mut sent, &mut model, &mut trace);
    let applied: Vec<u64> = shard
        .stores
        .iter()
        .map(|store| store.as_ref().unwrap().applied())
        .collect();
    assert!(applied.iter().all(|&a| a == applied[0]), "{applied:?}");
}

/// The seeds each test runs (module docs).
fn seeds() -> std::ops::RangeInclusive<u64> {
    let number = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|n| n.parse::<u64>().expect("a seed number"))
    };
    if let Some(seed) = number("KERMODES_SIM_SEED") {
        return seed..=seed;
    }
    1..=number("KERMODES_SIM_SEEDS").unwrap_or(64)
}

#[test]
fn every_acknowledged_write_survives_any_crash_once_and_in_order() {
    for seed in seeds() {
        simulate(seed, 400, 0);
    }
}

/// After an fsync fails, Linux may never write what it held, while the process still reads it:
/// weir stops taking writes, and nothing it acknowledged is lost.
#[test]
fn a_failed_fsync_loses_nothing_acknowledged() {
    for seed in seeds() {
        simulate(seed, 400, 30);
    }
}

#[test]
fn the_same_seed_does_the_same() {
    for seed in [3, 17, 42] {
        assert_eq!(
            simulate(seed, 300, 30),
            simulate(seed, 300, 30),
            "seed {seed}"
        );
    }
}

#[test]
fn a_shard_of_three_loses_no_acknowledged_write_to_crashes_or_the_network() {
    for seed in seeds() {
        simulate_shard(seed, 600);
    }
}

#[test]
fn the_same_seed_does_the_same_to_a_shard() {
    for seed in [5, 23] {
        assert_eq!(
            simulate_shard(seed, 300),
            simulate_shard(seed, 300),
            "seed {seed}"
        );
    }
}
