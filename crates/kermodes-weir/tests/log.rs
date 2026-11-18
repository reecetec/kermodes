//! The shard log (architecture.md §7.6) on real files and real processes: a writer killed with
//! `SIGKILL` at seeded points loses no write it was told was durable and writes none twice; and
//! a reader resuming after any position it was given gets exactly what came after it, before and
//! after the store restarts.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_weir::{Change, Error, SourceId, Stamp, Store, TableName};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-log-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ])
}

fn ticks() -> TableName {
    TableName::parse("md.ticks").unwrap()
}

fn quotes() -> TableName {
    TableName::parse("md.quotes").unwrap()
}

fn rows(k: Vec<i64>, v: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("k", Arc::new(Int64Array::from(k)) as ArrayRef),
        ("v", Arc::new(Int64Array::from(v))),
    ])
    .unwrap()
}

/// The `i`th write of each table: an append of ten values, and an upsert of key `i % 7`.
fn appended(i: i64) -> RecordBatch {
    let v: Vec<i64> = (i * 10..i * 10 + 10).collect();
    rows(vec![0; 10], v)
}

fn upserted(i: i64) -> RecordBatch {
    rows(vec![i % 7], vec![i])
}

/// Every batch a fresh subscription is given, with its stamp.
fn read(store: &Store, name: &TableName) -> Vec<(Stamp, RecordBatch)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::downgrade(&seen);
    store
        .subscribe(
            name,
            Box::new(move |batch, stamp, _| {
                let Some(sink) = sink.upgrade() else {
                    return false;
                };
                sink.lock().unwrap().push((stamp, batch.clone()));
                true
            }),
            Box::new(|why| panic!("{why}")),
        )
        .unwrap();
    seen.lock().unwrap().clone()
}

fn values(batches: &[(Stamp, RecordBatch)]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|(_, b)| b.column(1).as_primitive::<Int64Type>().values().to_vec())
        .collect()
}

/// A keyed table's state from its batches: key to value.
fn state(batches: &[(Stamp, RecordBatch)]) -> BTreeMap<i64, i64> {
    let mut state = BTreeMap::new();
    for (stamp, batch) in batches {
        if stamp.change == Change::Checkpoint {
            state.clear();
        }
        let k = batch.column(0).as_primitive::<Int64Type>();
        let v = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            match stamp.change {
                Change::Delete => {
                    state.remove(&k.value(row));
                }
                _ => {
                    state.insert(k.value(row), v.value(row));
                }
            }
        }
    }
    state
}

/// The keyed table after its first `n` upserts.
fn state_after(n: i64) -> BTreeMap<i64, i64> {
    (0..n).map(|i| (i % 7, i)).collect()
}

/// Run as a child process by `a_killed_writer_loses_nothing_acknowledged`: writes both tables,
/// each from where its own durable writes end, up to `WINDOW` writes of each in flight, printing
/// how many of both are acknowledged after each acknowledgement.
#[test]
#[ignore = "a child process of a_killed_writer_loses_nothing_acknowledged"]
fn writer_child() {
    const WINDOW: usize = 16;
    let Some(dir) = std::env::var_os("KERMODES_LOG_TEST_DIR") else {
        return;
    };
    let store = Store::open(Path::new(&dir)).unwrap();
    store.create_table(&ticks(), &schema()).unwrap();
    store
        .create_keyed_table(&quotes(), &schema(), &["k".to_owned()])
        .unwrap();
    let feed = SourceId::parse("feed").unwrap();
    let ticks_writer = store.writer(&ticks(), &feed, true).unwrap();
    let quotes_writer = store.writer(&quotes(), &feed, true).unwrap();
    let mut tick = ticks_writer.last_seq().unwrap_or(0) as i64;
    let mut quote = quotes_writer.last_seq().unwrap_or(0) as i64;
    let stdout = std::io::stdout();
    let mut in_flight = std::collections::VecDeque::new();
    loop {
        let sent_tick = ticks_writer
            .send(Change::Rows, &appended(tick), Some(tick as u64 + 1))
            .unwrap();
        let sent_quote = quotes_writer
            .send(Change::Upsert, &upserted(quote), Some(quote as u64 + 1))
            .unwrap();
        tick += 1;
        quote += 1;
        in_flight.push_back((tick.min(quote), sent_tick, sent_quote));
        if in_flight.len() == WINDOW {
            let (both, sent_tick, sent_quote) = in_flight.pop_front().unwrap();
            assert!(sent_tick.wait().unwrap() && sent_quote.wait().unwrap());
            let mut out = stdout.lock();
            writeln!(out, "acked {both}").unwrap();
            out.flush().unwrap();
        }
    }
}

#[test]
fn a_killed_writer_loses_nothing_acknowledged() {
    let dir = TempDir::new();
    // Seeded kill points, each a restart of the same store: the second and later rounds resume
    // where the killed process's durable writes end.
    for kill_after in [7_u64, 300, 41, 1200, 3] {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["writer_child", "--exact", "--ignored", "--nocapture"])
            .env("KERMODES_LOG_TEST_DIR", &dir.0)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut acked = 0;
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut first = None;
        while let Some(Ok(line)) = lines.next() {
            if let Some(n) = line.strip_prefix("acked ") {
                acked = n.parse::<u64>().unwrap();
                let first = *first.get_or_insert(acked);
                if acked >= first + kill_after {
                    break;
                }
            }
        }
        assert!(
            first.is_some_and(|first| acked >= first + kill_after),
            "the writer stopped before its kill point: {acked} acked"
        );
        // SIGKILL, mid-write: the next write may be logged or not, never half.
        child.kill().unwrap();
        child.wait().unwrap();

        let store = Store::open(&dir.0).unwrap();
        let got = values(&read(&store, &ticks()));
        let written = got.len() as u64 / 10;
        assert!(
            written >= acked,
            "kill after {kill_after}: {written} of {acked} acked"
        );
        assert_eq!(
            got,
            (0..written as i64 * 10).collect::<Vec<_>>(),
            "kill after {kill_after}: every write once, in order"
        );
        let quotes_state = state(&read(&store, &quotes()));
        // Each writer's writes are logged in the order sent, so the table holds its first `n`
        // upserts, all acknowledged ones among them; the two tables' unacknowledged writes
        // need not agree (§7.1).
        assert!(
            (acked as i64..acked as i64 + 1_000).any(|n| quotes_state == state_after(n)),
            "kill after {kill_after}: {quotes_state:?} after {acked} acked"
        );
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

/// What a resumed subscription is given, until it has caught up with everything written.
fn resumed(
    store: &Store,
    name: &TableName,
    after: u64,
) -> Result<Vec<(Stamp, RecordBatch)>, Error> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::downgrade(&seen);
    store.resume(
        name,
        after,
        Box::new(move |batch, stamp, _| {
            let Some(sink) = sink.upgrade() else {
                return false;
            };
            sink.lock().unwrap().push((stamp, batch.clone()));
            true
        }),
        Box::new(|why| panic!("{why}")),
    )?;
    let seen = seen.lock().unwrap().clone();
    Ok(seen)
}

/// Positions and values, to compare what two readers were given.
fn given(batches: &[(Stamp, RecordBatch)]) -> Vec<(u64, Change, Vec<i64>)> {
    batches
        .iter()
        .map(|(stamp, batch)| {
            (
                stamp.lsn,
                stamp.change,
                batch
                    .column(1)
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec(),
            )
        })
        .collect()
}

#[test]
fn a_reader_resumed_after_any_position_gets_exactly_what_followed() {
    for seed in 1..=8_u64 {
        let dir = TempDir::new();
        let mut rng = Rng(seed * 7919);
        let store = Store::open(&dir.0).unwrap();
        store.create_table(&ticks(), &schema()).unwrap();
        store
            .create_keyed_table(&quotes(), &schema(), &["k".to_owned()])
            .unwrap();
        let sources: Vec<SourceId> = ["a", "b", "c"]
            .iter()
            .map(|s| SourceId::parse(s).unwrap())
            .collect();
        let ticks_writers: Vec<_> = sources
            .iter()
            .map(|s| store.writer(&ticks(), s, false).unwrap())
            .collect();
        let quotes_writers: Vec<_> = sources
            .iter()
            .map(|s| store.writer(&quotes(), s, false).unwrap())
            .collect();
        for i in 0..200 {
            let who = rng.below(3) as usize;
            match rng.below(4) {
                0 => {
                    ticks_writers[who].write(&appended(i), None).unwrap();
                }
                1 => {
                    let key = rng.below(9) as i64;
                    quotes_writers[who]
                        .delete(&rows(vec![key], vec![0]), None)
                        .unwrap();
                }
                _ => {
                    quotes_writers[who]
                        .upsert(&rows(vec![rng.below(9) as i64], vec![i]), None)
                        .unwrap();
                }
            }
        }
        drop((ticks_writers, quotes_writers));
        let check = |store: &Store, when: &str| {
            for name in [ticks(), quotes()] {
                let full = read(store, &name);
                // Changes only: a resumed reader is not given checkpoints again.
                let changes: Vec<_> = full
                    .iter()
                    .filter(|(stamp, _)| stamp.change != Change::Checkpoint)
                    .cloned()
                    .collect();
                let positions: Vec<u64> = changes.iter().map(|(stamp, _)| stamp.lsn).collect();
                assert!(
                    positions.windows(2).all(|w| w[0] < w[1]),
                    "{when} {name}: positions rise"
                );
                for at in [0, 1, positions.len() / 2, positions.len() - 1] {
                    let after = positions[at];
                    let got = resumed(store, &name, after).unwrap();
                    assert_eq!(
                        given(&got),
                        given(&changes[at + 1..]),
                        "seed {seed} {when} {name}: resumed after {after}"
                    );
                }
            }
        };
        check(&store, "live");
        drop(store);
        let store = Store::open(&dir.0).unwrap();
        check(&store, "after reopening");
    }
}

/// Writes sent without waiting, many at once, are merged as the store logs them: the store
/// holds what writing them one at a time would leave, every write is answered as it would have
/// been (a resend `false`), and the log replays to the same after reopening. Seeded, with
/// appends, upserts, deletes, checkpoints and resends from three sources, flushed at random.
#[test]
fn writes_merged_in_flight_leave_what_writing_them_one_by_one_would() {
    for seed in 1..=8_u64 {
        let dir = TempDir::new();
        let mut rng = Rng(seed * 104_729);
        let store = Store::open(&dir.0).unwrap();
        store.create_table(&ticks(), &schema()).unwrap();
        store
            .create_keyed_table(&quotes(), &schema(), &["k".to_owned()])
            .unwrap();
        let sources: Vec<SourceId> = ["a", "b", "c"]
            .iter()
            .map(|s| SourceId::parse(s).unwrap())
            .collect();
        let ticks_writers: Vec<_> = sources
            .iter()
            .map(|s| store.writer(&ticks(), s, false).unwrap())
            .collect();
        let quotes_writers: Vec<_> = sources
            .iter()
            .map(|s| store.writer(&quotes(), s, false).unwrap())
            .collect();
        // The oracle: each write applied alone, in the order sent. Writers' unacknowledged
        // writes may be logged in any order but their own (§7.1), so each source writes its own
        // keys, and tags its ticks with its number.
        let mut last = [[0_u64; 3]; 2];
        let mut want_ticks: [Vec<i64>; 3] = [vec![], vec![], vec![]];
        let mut want_quotes = BTreeMap::new();
        let mut sent = Vec::new();
        for i in 0..2_000 {
            let who = rng.below(3) as usize;
            let table = usize::from(rng.below(3) != 0);
            // Mostly the next number; now and then one already written, a resend.
            let seq = if last[table][who] > 0 && rng.below(10) == 0 {
                1 + rng.below(last[table][who])
            } else {
                last[table][who] + 1
            };
            let fresh = seq > last[table][who];
            if fresh {
                last[table][who] = seq;
            }
            let pending = if table == 0 {
                let v: Vec<i64> = (i * 10..i * 10 + 10).collect();
                if fresh {
                    want_ticks[who].extend(&v);
                }
                ticks_writers[who]
                    .send(Change::Rows, &rows(vec![who as i64; 10], v), Some(seq))
                    .unwrap()
            } else {
                let mine = |n: u64| who as i64 * 3 + (n % 3) as i64;
                let n = rng.below(3);
                let (key, next) = (mine(n), mine(n + 1));
                // A checkpoint replaces every source's keys: it stays ordered against them all.
                let (change, batch) = match rng.below(20) {
                    0 => (Change::Checkpoint, rows(vec![key, next], vec![i, i])),
                    1..=5 => (Change::Delete, rows(vec![key], vec![0])),
                    _ => (Change::Upsert, rows(vec![key, next], vec![i, -i])),
                };
                if fresh {
                    let k = batch.column(0).as_primitive::<Int64Type>();
                    let v = batch.column(1).as_primitive::<Int64Type>();
                    if change == Change::Checkpoint {
                        want_quotes.clear();
                    }
                    for row in 0..batch.num_rows() {
                        if change == Change::Delete {
                            want_quotes.remove(&k.value(row));
                        } else {
                            want_quotes.insert(k.value(row), v.value(row));
                        }
                    }
                }
                quotes_writers[who].send(change, &batch, Some(seq)).unwrap()
            };
            sent.push((pending, fresh));
            if rng.below(100) == 0 {
                ticks_writers[who].flush().unwrap();
            }
        }
        for (n, (pending, fresh)) in sent.into_iter().enumerate() {
            assert_eq!(pending.wait().unwrap(), fresh, "seed {seed}: write {n}");
        }
        drop((ticks_writers, quotes_writers));
        let check = |store: &Store, when: &str| {
            let ticks_read = read(store, &ticks());
            let mut by_source = [vec![], vec![], vec![]];
            for (_, batch) in &ticks_read {
                let k = batch.column(0).as_primitive::<Int64Type>();
                let v = batch.column(1).as_primitive::<Int64Type>();
                for row in 0..batch.num_rows() {
                    by_source[k.value(row) as usize].push(v.value(row));
                }
            }
            assert_eq!(by_source, want_ticks, "seed {seed} {when}: ticks");
            let quotes_read = read(store, &quotes());
            assert_eq!(
                state(&quotes_read),
                want_quotes,
                "seed {seed} {when}: quotes"
            );
            // Dates and log order agree: ingest times rise with log position.
            for batches in [&ticks_read, &quotes_read] {
                let stamps: Vec<_> = batches
                    .iter()
                    .filter(|(stamp, _)| stamp.lsn > 0)
                    .map(|(stamp, _)| (stamp.lsn, stamp.ingest_ns))
                    .collect();
                assert!(
                    stamps
                        .windows(2)
                        .all(|w| w[0].0 < w[1].0 && w[0].1 < w[1].1),
                    "seed {seed} {when}: ingest times rise with the log"
                );
            }
        };
        check(&store, "live");
        drop(store);
        let store = Store::open(&dir.0).unwrap();
        check(&store, "after reopening");
    }
}

/// A writer let go with writes still in flight hands its source over only once they are
/// logged: the next writer numbers its writes after them, and none of its writes is taken for a
/// resend of theirs.
#[test]
fn a_writer_let_go_with_writes_in_flight_hands_over_after_them() {
    let dir = TempDir::new();
    let store = Arc::new(Store::open(&dir.0).unwrap());
    store.create_table(&ticks(), &schema()).unwrap();
    let feed = SourceId::parse("feed").unwrap();
    let mut want = Vec::new();
    for round in 0..5_i64 {
        let writer = store.writer(&ticks(), &feed, true).unwrap();
        for i in round * 2_000..(round + 1) * 2_000 {
            // Not waited for: dropping the writer is what has to wait.
            drop(
                writer
                    .send(Change::Rows, &rows(vec![0], vec![i]), None)
                    .unwrap(),
            );
            want.push(i);
        }
    }
    assert_eq!(values(&read(&store, &ticks())), want);
}
