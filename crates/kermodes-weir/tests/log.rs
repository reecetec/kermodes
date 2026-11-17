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

/// Run as a child process by `a_killed_writer_loses_nothing_acknowledged`: writes both tables in
/// turn until killed, printing how many of each are acknowledged after each acknowledgement.
#[test]
#[ignore = "a child process of a_killed_writer_loses_nothing_acknowledged"]
fn writer_child() {
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
    let mut i = ticks_writer.last_seq().unwrap_or(0) as i64;
    let stdout = std::io::stdout();
    loop {
        ticks_writer
            .write(&appended(i), Some(i as u64 + 1))
            .unwrap();
        quotes_writer
            .upsert(&upserted(i), Some(i as u64 + 1))
            .unwrap();
        i += 1;
        let mut out = stdout.lock();
        writeln!(out, "acked {i}").unwrap();
        out.flush().unwrap();
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
        // The upsert after the last acknowledged append may have landed too, or not.
        assert!(
            quotes_state == state_after(acked as i64)
                || quotes_state == state_after(acked as i64 + 1)
                || quotes_state == state_after(written as i64),
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
