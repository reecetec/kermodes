//! History of a real store in a real directory. Merging: rows move whole, small dates share a
//! file, a re-run changes nothing, weir never drops what history lacks. Reading: only the dates
//! asked for, unmerged ones from weir.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Date32Type, Field, Int64Type, Schema};
use kermodes_lake::{History, Lake, Merged, Weir, collect_garbage, merge, read_history};
use kermodes_weir::{Date, Error, SourceId, Store, TableName};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-merge-{}-{}",
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

fn ticks() -> TableName {
    TableName::parse("md.ticks").unwrap()
}

fn batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(values.to_vec())) as ArrayRef)])
        .unwrap()
}

fn d(day: u32) -> Date {
    Date::parse(&format!("2026-11-{day:02}")).unwrap()
}

fn today() -> Date {
    Date::of_ns(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64,
    )
}

/// A date, and the values each source wrote on it.
type Day<'a> = (Date, &'a [(&'a str, &'a [i64])]);

/// A store holding `days` as weir holds them after a restart: written today, then the day's
/// directory renamed to the date it stands for.
fn store_with(root: &Path, days: &[Day]) -> Store {
    let schema = Schema::new(vec![Field::new("n", DataType::Int64, true)]);
    for (date, sources) in days {
        let store = Store::open(root).unwrap();
        let location = store.create_table(&ticks(), &schema).unwrap().location;
        for (source, values) in *sources {
            let writer = store
                .writer(&ticks(), &SourceId::parse(source).unwrap(), false)
                .unwrap();
            writer.write(&batch(values), None).unwrap();
        }
        drop(store);
        let dir = root.join("weir").join(location);
        fs::rename(dir.join(today().to_string()), dir.join(date.to_string())).unwrap();
    }
    Store::open(root).unwrap()
}

/// Every `n` history holds of `first` to `last`, sorted.
fn history(store: &Store, lake: &Lake, first: Date, last: Date) -> Vec<i64> {
    let location = store.table(&ticks()).unwrap().location;
    let mut n = Vec::new();
    for file in store.history_files(&ticks(), first, last).unwrap() {
        for batch in lake.read(&location, &file.name, first, last, &[]).unwrap() {
            let batch = batch.unwrap();
            n.extend(batch.column(1).as_primitive::<Int64Type>().values().iter());
        }
    }
    n.sort_unstable();
    n
}

#[test]
fn past_dates_move_whole_into_history_and_out_of_weir() {
    let dir = TempDir::new();
    let store = store_with(
        &dir.0.join("store"),
        &[
            (d(1), &[("a", &[1, 2, 3]), ("b", &[4, 5])]),
            (d(2), &[("a", &[6, 7])]),
            (d(3), &[("a", &[8])]),
        ],
    );
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();

    let first = merge(&store, &lake, &ticks(), d(1)).unwrap();
    assert_eq!(
        first,
        Merged {
            rows: 5,
            files: 1,
            replaced: 0
        }
    );
    assert_eq!(history(&store, &lake, d(1), d(1)), [1, 2, 3, 4, 5]);
    assert_eq!(store.date_rows(&ticks(), d(1)).unwrap(), (0, 0));

    // The next small date joins the first in a new file; the old one waits as garbage.
    let second = merge(&store, &lake, &ticks(), d(2)).unwrap();
    assert_eq!(
        second,
        Merged {
            rows: 2,
            files: 1,
            replaced: 1
        }
    );
    let files = store.history_files(&ticks(), d(1), d(2)).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(
        (files[0].first, files[0].last, files[0].rows),
        (d(1), d(2), 7)
    );
    assert_eq!(history(&store, &lake, d(2), d(2)), [6, 7]);
    assert_eq!(history(&store, &lake, d(1), d(2)), [1, 2, 3, 4, 5, 6, 7]);
    let location = store.table(&ticks()).unwrap().location;
    let on_disk = |dir: &Path| {
        fs::read_dir(dir.join("lake").join(&location))
            .unwrap()
            .count()
    };
    assert_eq!(on_disk(&dir.0), 2);
    collect_garbage(&store, &lake, Duration::ZERO).unwrap();
    assert_eq!(on_disk(&dir.0), 1);

    // Again: weir no longer holds the date, and history is as it was.
    assert_eq!(
        merge(&store, &lake, &ticks(), d(1)).unwrap(),
        Merged::default()
    );
    assert_eq!(history(&store, &lake, d(1), d(2)), [1, 2, 3, 4, 5, 6, 7]);

    // Weir keeps what history lacks, and today is still being written.
    assert!(matches!(
        store.drop_date(&ticks(), d(3)),
        Err(Error::Catalog(_))
    ));
    assert_eq!(store.date_rows(&ticks(), d(3)).unwrap().0, 1);
    assert!(matches!(
        merge(&store, &lake, &ticks(), today()),
        Err(Error::Busy(_))
    ));
}

/// A merge that committed but stopped before weir dropped the date: run again, it only drops.
#[test]
fn a_merge_cut_short_after_its_commit_finishes_by_dropping() {
    let dir = TempDir::new();
    let root = dir.0.join("store");
    let store = store_with(&root, &[(d(1), &[("a", &[1, 2])])]);
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();
    merge(&store, &lake, &ticks(), d(1)).unwrap();
    // Put the date back in weir, as if the drop had never happened.
    drop(store);
    let store = store_with(&root, &[(d(1), &[("a", &[1, 2])])]);
    assert_eq!(store.date_rows(&ticks(), d(1)).unwrap().0, 2);
    // In both places: a read takes it from history alone.
    let read = read_history(&store, &lake, &ticks(), None, None, &[]).unwrap();
    assert_eq!((rows(&read).len(), read.explain.weir_dates), (2, 0));

    assert_eq!(
        merge(&store, &lake, &ticks(), d(1)).unwrap(),
        Merged::default()
    );
    assert_eq!(store.date_rows(&ticks(), d(1)).unwrap(), (0, 0));
    assert_eq!(history(&store, &lake, d(1), d(1)), [1, 2]);
    assert_eq!(store.history_files(&ticks(), d(1), d(1)).unwrap().len(), 1);
}

/// The same merge from another process, as a merge Job runs it: over Flight, against a served
/// store, with weir reading the date out and checking the drop.
#[test]
fn a_merge_over_flight_moves_dates_the_same() {
    let dir = TempDir::new();
    let store = Arc::new(store_with(
        &dir.0.join("store"),
        &[
            (d(1), &[("a", &[1, 2, 3]), ("b", &[4, 5])]),
            (d(2), &[("a", &[6, 7])]),
        ],
    ));
    let server = kermodes_flight::serve(store.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let client = kermodes_flight::Client::connect(&format!("grpc://{}", server.addr())).unwrap();
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();

    let merged = [d(1), d(2)].map(|date| merge(&client, &lake, &ticks(), date).unwrap());
    assert_eq!(
        merged,
        [
            Merged {
                rows: 5,
                files: 1,
                replaced: 0
            },
            Merged {
                rows: 2,
                files: 1,
                replaced: 1
            }
        ]
    );
    assert_eq!(history(&store, &lake, d(1), d(2)), [1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(store.date_rows(&ticks(), d(1)).unwrap(), (0, 0));
    let location = store.table(&ticks()).unwrap().location;
    collect_garbage(&client, &lake, Duration::ZERO).unwrap();
    let files = fs::read_dir(dir.0.join("lake").join(location))
        .unwrap()
        .count();
    assert_eq!(files, 1);
    assert!(matches!(
        merge(&client, &lake, &ticks(), today()),
        Err(Error::Busy(_))
    ));
}

/// Each row read: its `Date` (as days) and its `n`.
fn rows(history: &History) -> Vec<(i32, i64)> {
    let mut rows = Vec::new();
    for batch in &history.batches {
        let dates = batch.column(0).as_primitive::<Date32Type>();
        let n = batch.column(1).as_primitive::<Int64Type>();
        rows.extend(
            dates
                .values()
                .iter()
                .copied()
                .zip(n.values().iter().copied()),
        );
    }
    rows
}

/// Reads of history take only the dates asked for: from files, only those dates' row groups;
/// past dates not merged yet from weir, so there is no gap. The same in-process and over Flight.
#[test]
fn history_reads_only_the_dates_asked_for() {
    let dir = TempDir::new();
    let store = Arc::new(store_with(
        &dir.0.join("store"),
        &[
            (d(1), &[("a", &[1, 2, 3]), ("b", &[4, 5])]),
            (d(2), &[("a", &[6, 7])]),
            (d(3), &[("a", &[8, 9])]),
        ],
    ));
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();
    merge(&*store, &lake, &ticks(), d(1)).unwrap();
    merge(&*store, &lake, &ticks(), d(2)).unwrap();
    let server = kermodes_flight::serve(store.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let client = kermodes_flight::Client::connect(&format!("grpc://{}", server.addr())).unwrap();

    let (one, two, three) = (d(1).days(), d(2).days(), d(3).days());
    for weir in [&*store as &dyn Weir, &client] {
        let read = |first, last| read_history(weir, &lake, &ticks(), first, last, &[]).unwrap();

        let all = read(None, None);
        let mut expected: Vec<(i32, i64)> = [1, 2, 3, 4, 5].map(|n| (one, n)).to_vec();
        expected.extend([(two, 6), (two, 7), (three, 8), (three, 9)]);
        let mut got = rows(&all);
        got.sort_unstable();
        assert_eq!(got, expected);
        assert!(
            rows(&all).is_sorted_by_key(|&(date, _)| date),
            "in date order"
        );
        assert_eq!(
            (
                all.explain.files,
                all.explain.row_groups,
                all.explain.weir_dates
            ),
            ((1, 1), (2, 2), 1)
        );

        let second = read(Some(d(2)), Some(d(2)));
        assert_eq!(rows(&second), [(two, 6), (two, 7)]);
        assert_eq!(
            (
                second.explain.files,
                second.explain.row_groups,
                second.explain.weir_dates
            ),
            ((1, 1), (1, 2), 0)
        );
        assert!(second.explain.bytes.0 < second.explain.bytes.1);
        assert!(
            second
                .explain
                .to_string()
                .starts_with("1 of 1 files, 1 of 2 row groups"),
            "{}",
            second.explain
        );

        let third = read(Some(d(3)), None);
        assert_eq!(rows(&third), [(three, 8), (three, 9)]);
        assert_eq!((third.explain.files, third.explain.weir_dates), ((0, 1), 1));

        assert!(read(Some(d(4)), None).batches.is_empty());
    }
}

/// A keyed table's history is its log of changes, each row with its kind in `Op`: the same read
/// from weir before the merge as from the lake after it.
#[test]
fn a_keyed_tables_history_is_its_log_of_changes() {
    let dir = TempDir::new();
    let quotes = TableName::parse("md.quotes").unwrap();
    let schema = Schema::new(vec![Field::new("n", DataType::Int64, true)]);
    let store = Store::open(&dir.0).unwrap();
    let location = store
        .create_keyed_table(&quotes, &schema, &["n".to_string()])
        .unwrap()
        .location;
    let writer = store
        .writer(&quotes, &SourceId::parse("feed").unwrap(), false)
        .unwrap();
    writer.upsert(&batch(&[1, 2]), None).unwrap();
    writer.delete(&batch(&[1]), None).unwrap();
    drop((writer, store));
    let dir_of = dir.0.join("weir").join(location);
    fs::rename(
        dir_of.join(today().to_string()),
        dir_of.join(d(1).to_string()),
    )
    .unwrap();
    let store = Store::open(&dir.0).unwrap();
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();
    let log = |store: &Store| -> Vec<(String, i64)> {
        let history = read_history(store, &lake, &quotes, Some(d(1)), Some(d(1)), &[]).unwrap();
        let mut log = Vec::new();
        for batch in &history.batches {
            let ops = batch.column(1).as_string::<i32>();
            let n = batch.column(2).as_primitive::<Int64Type>();
            log.extend((0..batch.num_rows()).map(|i| (ops.value(i).to_string(), n.value(i))));
        }
        log
    };
    let before = log(&store);
    let want = [("upsert", 1), ("upsert", 2), ("delete", 1)];
    assert_eq!(before, want.map(|(op, n)| (op.to_string(), n)));
    assert_eq!(merge(&store, &lake, &quotes, d(1)).unwrap().rows, 3);
    assert_eq!(store.date_rows(&quotes, d(1)).unwrap(), (0, 0));
    assert_eq!(log(&store), before);
}
