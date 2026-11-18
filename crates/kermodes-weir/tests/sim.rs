//! weir under simulation (architecture.md §7.6): a stepped store on a simulated disk, driven by
//! a seed through writes, commits, flushes, clean restarts, processes killed (the page cache
//! staying), and power losses that keep a random part of what was not fsynced and tear the last
//! write. Whatever the seed, after every reopen the store holds exactly the writes it
//! acknowledged, each once and in order; and the same seed does exactly the same, so any
//! failure replays. Each test runs seeds 1 to 64; `KERMODES_SIM_SEEDS=N` runs 1 to N, for a
//! longer search.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_weir::{
    Change, Clock, Date, HistoryCommit, HistoryDate, HistoryFile, Pending, SourceId, Store,
    TableName, Writer, sim,
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
    let store = loop {
        let crashes = disk.crashes();
        match Store::open_stepped(root, disk.handle(), clock.clone()) {
            Ok(store) => break store,
            Err(_) if disk.crashes() > crashes => continue,
            // Each open fsyncs a few dozen times, one in 30 failing: a hundred restarts in a row
            // that all fail is an error that is not the disk's.
            Err(_) if failing && reboots < 100 => {
                reboots += 1;
                disk.power_loss();
            }
            Err(e) => panic!("seed {seed}: reopening: {e}"),
        }
    };
    disk.disarm();
    store.create_table(&ticks(), &schema()).unwrap();
    store
        .create_keyed_table(&quotes(), &schema(), &["k".to_owned()])
        .unwrap();
    let writers = (0..SOURCES)
        .map(|who| {
            let source = SourceId::parse(&format!("s{who}")).unwrap();
            [
                store.writer(&ticks(), &source, false).unwrap(),
                store.writer(&quotes(), &source, false).unwrap(),
            ]
        })
        .collect();
    Open { store, writers }
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
            // Send a write.
            0..=59 => {
                let who = rng.below(SOURCES as u64) as usize;
                let table = usize::from(rng.below(2) == 1);
                let last = model.sent[table][who];
                // Now and then one already sent: a resend.
                let resend = last > 0 && rng.below(8) == 0;
                let seq = if resend {
                    1 + rng.below(last)
                } else {
                    model.sent[table][who] += 1;
                    model.sent[table][who]
                };
                let (change, batch, effect) = if table == 0 {
                    let n = 1 + rng.below(3) as usize;
                    let values: Vec<i64> = (value..value + n as i64).collect();
                    value += n as i64;
                    let batch = rows(vec![who as i64; n], values.clone());
                    (Change::Rows, batch, Effect::Ticks(who, values))
                } else {
                    let key = |n: u64| who as i64 * 4 + (n % 4) as i64;
                    let n = rng.below(4);
                    value += 1;
                    let (change, ks, vs) = match rng.below(10) {
                        0 => (Change::Checkpoint, vec![key(n), key(n + 1)], vec![value; 2]),
                        1..=3 => (Change::Delete, vec![key(n)], vec![0]),
                        _ => (
                            Change::Upsert,
                            vec![key(n), key(n + 2)],
                            vec![value, -value],
                        ),
                    };
                    let effect =
                        Effect::Quotes(change, ks.iter().copied().zip(vs.clone()).collect());
                    (change, rows(ks, vs), effect)
                };
                // A new write unnumbered, as a producer sends one; a resend with its seq.
                let pending = at.writers[who][table]
                    .send(change, &batch, resend.then_some(seq))
                    .unwrap();
                sent.push(Sent {
                    pending,
                    by: (table, who, seq),
                    effect: (!resend).then_some(effect),
                });
            }
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

/// The seeds each test runs (module docs).
fn seeds() -> std::ops::RangeInclusive<u64> {
    let last = std::env::var("KERMODES_SIM_SEEDS")
        .ok()
        .map(|n| n.parse().expect("KERMODES_SIM_SEEDS is a number"))
        .unwrap_or(64);
    1..=last
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
