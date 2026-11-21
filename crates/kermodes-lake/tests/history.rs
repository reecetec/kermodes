//! History of a real store in a real directory. Merging: rows move whole, small dates share a
//! file, a re-run changes nothing, weir never drops what history lacks. Reading: only the dates
//! asked for, unmerged ones from weir.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, UInt64Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::{DataType, Date32Type, Field, Int64Type, Schema, SchemaRef};
use bytes::Bytes;
use kermodes_lake::{
    History, HistoryParts, Lake, Merged, Reading, Weir, collect_garbage, column_bounds,
    history_parts, merge, read_history, write_history,
};
use kermodes_weir::{Date, Error, HistoryCommit, SourceId, Store, TableName};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

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
        for batch in lake
            .read(&location, &file.name, None, first, last, &[], None)
            .unwrap()
        {
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
    let read = read_history(&store, &lake, &ticks(), &Reading::default()).unwrap();
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

/// A reader holding no lake keys reads history where weir says each file is: the rows a read
/// with the lake's keys gives. A store that does not say leaves it nothing to read.
#[test]
fn a_reader_without_lake_keys_reads_the_files_weir_vends() {
    let dir = TempDir::new();
    let store = Arc::new(store_with(
        &dir.0.join("store"),
        &[(d(1), &[("a", &[1, 2, 3])]), (d(2), &[("a", &[4, 5])])],
    ));
    let lake = Arc::new(Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap());
    merge(store.as_ref(), &lake, &ticks(), d(1)).unwrap();
    let local = read_history(store.as_ref(), &lake, &ticks(), &Reading::default()).unwrap();
    assert_eq!(rows(&local).len(), 5);

    let reader = Lake::reader().unwrap();
    let server = kermodes_flight::serve(store.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let client = kermodes_flight::Client::connect(&format!("grpc://{}", server.addr())).unwrap();
    assert!(matches!(
        read_history(&client, &reader, &ticks(), &Reading::default()),
        Err(Error::NotFound(_))
    ));

    let vending = lake.clone();
    store.vend_history(Arc::new(move |location, file| vending.vend(location, file)));
    let read = read_history(&client, &reader, &ticks(), &Reading::default()).unwrap();
    assert_eq!(rows(&read), rows(&local));
    assert_eq!(read.explain, local.explain);
}

/// A batch job writes a past date straight into history, not through weir: the date then
/// holds exactly those rows, written again it holds the new ones in their place, and written
/// with none it holds none. Its neighbours are kept as they were.
#[test]
fn a_batch_job_writes_a_past_date_straight_into_history() {
    let dir = TempDir::new();
    let store = store_with(&dir.0.join("store"), &[(d(1), &[("a", &[1, 2])])]);
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();
    merge(&store, &lake, &ticks(), d(1)).unwrap();

    let written = write_history(
        &store,
        &lake,
        &ticks(),
        d(5),
        &[batch(&[10, 11]), batch(&[12])],
    );
    assert_eq!(
        written.unwrap(),
        Merged {
            rows: 3,
            files: 1,
            replaced: 1
        }
    );
    assert_eq!(history(&store, &lake, d(5), d(5)), [10, 11, 12]);
    assert_eq!(history(&store, &lake, d(1), d(5)), [1, 2, 10, 11, 12]);

    write_history(&store, &lake, &ticks(), d(5), &[batch(&[13])]).unwrap();
    assert_eq!(history(&store, &lake, d(1), d(5)), [1, 2, 13]);

    write_history(&store, &lake, &ticks(), d(6), &[batch(&[14])]).unwrap();
    assert_eq!(history(&store, &lake, d(6), d(6)), [14]);

    write_history(&store, &lake, &ticks(), d(5), &[]).unwrap();
    assert_eq!(history(&store, &lake, d(1), d(6)), [1, 2, 14]);
    let day = Reading {
        first: Some(d(5)),
        last: Some(d(5)),
        ..Reading::default()
    };
    let read = read_history(&store, &lake, &ticks(), &day).unwrap();
    assert_eq!(rows(&read), []);
}

/// A lake reads a date rewritten by a batch job as its new rows, though it read the date's
/// earlier file (and its footer) before: its rows, and its statistics for bounds, are the new
/// file's, whether read whole or in parts.
#[test]
fn a_date_rewritten_reads_as_its_new_file() {
    let dir = TempDir::new();
    let store = store_with(&dir.0.join("store"), &[(d(1), &[("a", &[1])])]);
    let lake = Arc::new(Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap());
    let bounds = column_bounds("n >= 15");
    let day = Reading {
        first: Some(d(5)),
        last: Some(d(5)),
        bounds: &bounds,
        ..Reading::default()
    };
    let read = |values: &[i64]| {
        write_history(&store, &lake, &ticks(), d(5), &[batch(values)]).unwrap();
        let whole = read_history(&store, &lake, &ticks(), &day).unwrap();
        let entry = store.table(&ticks()).unwrap();
        let parts = history_parts(&store, &lake, &entry, &day).unwrap();
        assert_eq!(
            concat(&parts.schema, &run_parts(&parts)),
            concat(&whole.schema, &whole.batches)
        );
        rows(&whole).into_iter().map(|(_, n)| n).collect::<Vec<_>>()
    };
    assert_eq!(read(&[10, 11, 12]), [] as [i64; 0]);
    assert_eq!(read(&[20]), [20]);
    assert_eq!(read(&[30, 31, 32, 33]), [30, 31, 32, 33]);
}

/// A read that needs only a table's first rows (a notebook's preview) stops once it has them,
/// in date order across the lake's files and weir's past dates.
#[test]
fn a_limited_read_takes_the_first_rows_in_date_order() {
    let dir = TempDir::new();
    let days: [Day; 4] = [
        (d(1), &[("a", &[1, 2])]),
        (d(2), &[("a", &[3, 4])]),
        (d(3), &[("a", &[5, 6])]),
        (d(4), &[("a", &[7, 8])]),
    ];
    let store = store_with(&dir.0.join("store"), &days);
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();
    merge(&store, &lake, &ticks(), d(1)).unwrap();
    merge(&store, &lake, &ticks(), d(2)).unwrap();

    let limited = |limit| {
        let first = Reading {
            limit: Some(limit),
            ..Reading::default()
        };
        read_history(&store, &lake, &ticks(), &first).unwrap()
    };
    let values =
        |history: &History| -> Vec<i64> { rows(history).into_iter().map(|(_, n)| n).collect() };
    // Whole batches: at least the rows asked for, and no part after the one that gave them.
    let three = limited(3);
    assert_eq!(values(&three), [1, 2, 3, 4]);
    assert_eq!(three.explain.weir_dates, 0);
    let five = limited(5);
    assert_eq!(values(&five), [1, 2, 3, 4, 5, 6]);
    assert_eq!(five.explain.weir_dates, 1);
    assert_eq!(values(&limited(100)), [1, 2, 3, 4, 5, 6, 7, 8]);
}

/// Each date is served from one place (§8): history takes no date that is not past, by the
/// store's clock however the write reaches it, nor one weir still holds; and only rows that are
/// the table's columns, of an append table.
#[test]
fn writing_history_is_refused_where_a_date_would_be_served_twice_or_the_rows_do_not_fit() {
    let dir = TempDir::new();
    let store = Arc::new(store_with(&dir.0.join("store"), &[(d(1), &[("a", &[1])])]));
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();
    let server = kermodes_flight::serve(store.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let client = kermodes_flight::Client::connect(&format!("grpc://{}", server.addr())).unwrap();

    for weir in [&*store as &dyn Weir, &client] {
        let today = write_history(weir, &lake, &ticks(), today(), &[batch(&[1])]);
        assert!(matches!(today, Err(Error::Busy(_))), "{today:?}");
        let held = write_history(weir, &lake, &ticks(), d(1), &[batch(&[1])]);
        assert!(matches!(held, Err(Error::Busy(_))), "{held:?}");
    }
    assert_eq!(store.history(&ticks()).unwrap(), []);

    let column =
        |name: &str, values: ArrayRef| RecordBatch::try_from_iter([(name, values)]).unwrap();
    let as_float = column("n", Arc::new(arrow::array::Float64Array::from(vec![1.0])));
    let other = column("m", Arc::new(Int64Array::from(vec![1])));
    let extra = RecordBatch::try_from_iter([
        ("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("m", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .unwrap();
    for rows in [as_float, other, extra] {
        let wrong = write_history(&*store, &lake, &ticks(), d(5), &[rows]);
        assert!(matches!(wrong, Err(Error::Schema(_))), "{wrong:?}");
    }

    let quotes = TableName::parse("md.quotes").unwrap();
    let schema = Schema::new(vec![Field::new("n", DataType::Int64, true)]);
    store
        .create_keyed_table(&quotes, &schema, &["n".to_string()])
        .unwrap();
    let keyed = write_history(&*store, &lake, &quotes, d(5), &[batch(&[1])]);
    assert!(matches!(keyed, Err(Error::Schema(_))), "{keyed:?}");
}

/// A read of some columns gives `Date` and those, in the table's order, from history files and
/// from past dates weir still holds alike; a column renamed since a file was written is read by
/// its new name; `EXPLAIN` counts the columns read.
#[test]
fn a_read_of_some_columns_reads_only_those() {
    let dir = TempDir::new();
    let root = dir.0.join("store");
    let wide = TableName::parse("md.wide").unwrap();
    let int = |values: &[i64]| Arc::new(Int64Array::from(values.to_vec())) as ArrayRef;
    let rows = |a: &[i64], b: &[i64], c: &[i64]| {
        RecordBatch::try_from_iter([("a", int(a)), ("b", int(b)), ("c", int(c))]).unwrap()
    };
    let store = Store::open(&root).unwrap();
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]);
    let location = store.create_table(&wide, &schema).unwrap().location;
    let lake = Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap();
    write_history(
        &store,
        &lake,
        &wide,
        d(1),
        &[rows(&[1, 2], &[10, 20], &[100, 200])],
    )
    .unwrap();
    // A past date still in weir: written today, then its directory renamed to the date.
    let writer = store
        .writer(&wide, &SourceId::parse("feed").unwrap(), false)
        .unwrap();
    writer.write(&rows(&[3], &[30], &[300]), None).unwrap();
    drop((writer, store));
    let weir = root.join("weir").join(&location);
    fs::rename(weir.join(today().to_string()), weir.join(d(2).to_string())).unwrap();
    let store = Store::open(&root).unwrap();
    // `c` renamed `z` after the file was written.
    let renamed = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        kermodes_weir::column("z", "int64 was c").unwrap(),
    ]);
    store.create_table(&wide, &renamed).unwrap();

    let columns = ["z".to_string(), "a".to_string(), "nowhere".to_string()];
    let some = Reading {
        columns: Some(&columns),
        ..Reading::default()
    };
    let read = read_history(&store, &lake, &wide, &some).unwrap();
    let names: Vec<&str> = read
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["Date", "a", "z"]);
    let mut got = Vec::new();
    for batch in &read.batches {
        assert_eq!(batch.schema(), read.schema);
        let (a, z) = (batch.column(1).as_primitive::<Int64Type>(), batch.column(2));
        for row in 0..batch.num_rows() {
            got.push((a.value(row), z.as_primitive::<Int64Type>().value(row)));
        }
    }
    assert_eq!(got, [(1, 100), (2, 200), (3, 300)]);
    assert_eq!(read.explain.columns, (3, 4));
    assert_eq!(read.explain.weir_dates, 1);
    let all = read_history(&store, &lake, &wide, &Reading::default()).unwrap();
    assert_eq!(all.explain.columns, (4, 4));
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
        let read = |first, last| {
            let dates = Reading {
                first,
                last,
                ..Reading::default()
            };
            read_history(weir, &lake, &ticks(), &dates).unwrap()
        };

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
        let day = Reading {
            first: Some(d(1)),
            last: Some(d(1)),
            ..Reading::default()
        };
        let history = read_history(store, &lake, &quotes, &day).unwrap();
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

/// Date `k`'s rows: `n` is `k` millions and the row's place in the date, `m` the place.
fn numbered(k: u32, count: usize) -> RecordBatch {
    let base = i64::from(k) * 1_000_000;
    RecordBatch::try_from_iter([
        (
            "n",
            Arc::new(Int64Array::from_iter_values(
                (0..count as i64).map(|i| base + i),
            )) as ArrayRef,
        ),
        (
            "m",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// Every part, all its columns, read on a thread of its own, started last first, and their
/// batches in part order.
fn run_parts(parts: &HistoryParts) -> Vec<RecordBatch> {
    let all: Vec<usize> = (0..parts.schema.fields().len()).collect();
    std::thread::scope(|scope| {
        let mut running: Vec<_> = (0..parts.len())
            .rev()
            .map(|part| {
                let all = &all;
                scope.spawn(move || {
                    let batches = parts.read(part, all, None).unwrap();
                    batches.collect::<Result<Vec<_>, _>>()
                })
            })
            .collect();
        running.reverse();
        running
            .into_iter()
            .flat_map(|run| run.join().unwrap().unwrap())
            .collect()
    })
}

fn concat(schema: &SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    arrow::compute::concat_batches(schema, batches).unwrap()
}

/// History read as parts, each on its own thread in any order, gives `read_history`'s rows in
/// its order: from files of several row groups and from past dates weir holds, with bounds and
/// columns, and through a reader holding no lake keys. A part read again, some of its columns or
/// some of its rows, is those of its rows. A limit takes only the leading parts.
#[test]
fn history_read_in_parallel_parts_is_history_read_in_order() {
    let dir = TempDir::new();
    let root = dir.0.join("store");
    let name = TableName::parse("md.parts").unwrap();
    let schema = Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("m", DataType::Int64, true),
    ]);
    // Two past dates still in weir: each written today, then its directory renamed to the date.
    for (k, count) in [(6, 3), (7, 4)] {
        let store = Store::open(&root).unwrap();
        let location = store.create_table(&name, &schema).unwrap().location;
        let writer = store
            .writer(&name, &SourceId::parse("feed").unwrap(), false)
            .unwrap();
        writer.write(&numbered(k, count), None).unwrap();
        drop((writer, store));
        let weir = root.join("weir").join(location);
        fs::rename(weir.join(today().to_string()), weir.join(d(k).to_string())).unwrap();
    }
    let store = Arc::new(Store::open(&root).unwrap());
    let entry = store.table(&name).unwrap();
    let lake = Arc::new(Lake::open(dir.0.join("lake").to_str().unwrap()).unwrap());
    // Two files written as a merge writes them, a row group a date: small dates would share one.
    let with_ids = Schema::new(
        entry
            .schema
            .fields()
            .iter()
            .zip(entry.ids.iter())
            .map(|(field, id)| {
                field
                    .as_ref()
                    .clone()
                    .with_metadata([("PARQUET:field_id".to_string(), id.0.to_string())])
            })
            .collect::<Vec<_>>(),
    );
    for days in [
        &[(1, 150_000), (2, 150_000), (3, 10)][..],
        &[(4, 140_000), (5, 5)],
    ] {
        let dates: Vec<Date> = days.iter().map(|&(k, _)| d(k)).collect();
        let rows = |date: Date, each: &mut dyn FnMut(RecordBatch) -> Result<(), Error>| {
            let &(k, count) = days.iter().find(|&&(k, _)| d(k) == date).unwrap();
            each(numbered(k, count))
        };
        let written = lake
            .write(&entry.location, &with_ids, &[], &dates, rows, u64::MAX)
            .unwrap();
        let commit = HistoryCommit {
            written: dates,
            replaced: Vec::new(),
            files: written.files,
            dates: written.dates,
        };
        store.commit_history(&name, &commit).unwrap();
    }

    // `read_history` with the lake's keys is the oracle.
    let same = |weir: &dyn Weir, parts_from: &Arc<Lake>, reading: &Reading| {
        let parts = history_parts(weir, parts_from, &entry, reading).unwrap();
        let read = read_history(&*store, &lake, &name, reading).unwrap();
        assert_eq!(parts.schema, read.schema);
        assert_eq!(
            concat(&parts.schema, &run_parts(&parts)),
            concat(&read.schema, &read.batches)
        );
        // Read again, a part's last column of all its rows, and all its columns of every
        // seventh row, are those of its rows read whole.
        let all: Vec<usize> = (0..parts.schema.fields().len()).collect();
        let last = all.len() - 1;
        let read = |part, columns: &[usize], rows: Option<&[u64]>| {
            let schema = Arc::new(parts.schema.project(columns).unwrap());
            let batches = parts.read(part, columns, rows).unwrap();
            concat(&schema, &batches.collect::<Result<Vec<_>, _>>().unwrap())
        };
        for part in 0..parts.len() {
            let whole = read(part, &all, None);
            assert_eq!(read(part, &[last], None), whole.project(&[last]).unwrap());
            let rows: Vec<u64> = (0..whole.num_rows() as u64).step_by(7).collect();
            let taken = take_record_batch(&whole, &UInt64Array::from(rows.clone())).unwrap();
            assert_eq!(read(part, &all, Some(&rows)), taken);
        }
        parts.len()
    };
    // A part a big date, or the small dates after one: three of the first file, two of the
    // second, one each of weir's dates.
    assert_eq!(same(&*store, &lake, &Reading::default()), 7);
    let bounds = column_bounds("n <= 4100000");
    let columns = ["m".to_string()];
    let some = Reading {
        first: Some(d(2)),
        bounds: &bounds,
        columns: Some(&columns),
        ..Reading::default()
    };
    assert!(same(&*store, &lake, &some) < 7);

    let limited = Reading {
        limit: Some(150_001),
        ..Reading::default()
    };
    let parts = history_parts(&*store, &lake, &entry, &limited).unwrap();
    assert_eq!(parts.len(), 2);
    let rows: usize = run_parts(&parts).iter().map(RecordBatch::num_rows).sum();
    assert!(rows >= 150_001);

    let vending = lake.clone();
    store.vend_history(Arc::new(move |location, file| vending.vend(location, file)));
    let server = kermodes_flight::serve(store.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let client = kermodes_flight::Client::connect(&format!("grpc://{}", server.addr())).unwrap();
    let reader = Arc::new(Lake::reader().unwrap());
    assert_eq!(same(&client, &reader, &Reading::default()), 7);
}

/// Byte ranges asked of a file server, by the file's path.
type Asked = Arc<Mutex<Vec<(String, Range<u64>)>>>;

/// Serves a lake's files over HTTP by byte range, as a store's presigned URLs do, logging each
/// range asked.
fn serve_ranges(root: PathBuf) -> (SocketAddr, Asked) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let asked = Asked::default();
    let log = asked.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let (root, log) = (root.clone(), log.clone());
            std::thread::spawn(move || answer(stream.unwrap(), &root, &log));
        }
    });
    (addr, asked)
}

/// Answers one `GET /path` with `Range: bytes=a-b` or `bytes=-n`, then closes.
fn answer(mut stream: TcpStream, root: &Path, log: &Mutex<Vec<(String, Range<u64>)>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let path = line.split_whitespace().nth(1).unwrap()[1..].to_owned();
    let mut range = String::new();
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).unwrap();
        if header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.eq_ignore_ascii_case("range")
        {
            range = value.trim().trim_start_matches("bytes=").to_owned();
        }
    }
    let bytes = fs::read(root.join(&path)).unwrap();
    let total = bytes.len() as u64;
    let (start, end) = match range.split_once('-').unwrap() {
        ("", suffix) => (total - suffix.parse::<u64>().unwrap(), total),
        (first, last) => (first.parse().unwrap(), last.parse::<u64>().unwrap() + 1),
    };
    log.lock().unwrap().push((path, start..end));
    write!(
        stream,
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{total}\r\nConnection: close\r\n\r\n",
        end - start,
        end - 1
    )
    .unwrap();
    stream
        .write_all(&bytes[start as usize..end as usize])
        .unwrap();
}

/// A top-N over a wide table of several files, read as its parts: its sort column of every
/// row, then every column of the winners alone. Read where weir vends the files, from a server
/// logging every range asked: the first read asks for only `Date` and the sort column, of every
/// row group; the second, of the row groups holding winners alone, for only the pages holding
/// them and the column chunks' dictionaries.
#[test]
fn a_top_n_reads_the_other_columns_of_its_winners_pages_alone() {
    const DATE_ROWS: i64 = 60_000;
    const ROWS: i64 = 3 * DATE_ROWS;
    const WIDE: usize = 4;
    let dir = TempDir::new();
    let name = TableName::parse("md.wide").unwrap();
    let mut fields = vec![Field::new("k", DataType::Int64, false)];
    fields.extend((0..WIDE).map(|i| Field::new(format!("w{i}"), DataType::Int64, false)));
    let schema = Schema::new(fields);
    let store = Arc::new(Store::open(&dir.0.join("store")).unwrap());
    let entry = store.create_table(&name, &schema).unwrap();
    let lake_dir = dir.0.join("lake");
    let lake = Lake::open(lake_dir.to_str().unwrap()).unwrap();
    let with_ids = Schema::new(
        schema
            .fields()
            .iter()
            .zip(entry.ids.iter())
            .map(|(field, id)| {
                field
                    .as_ref()
                    .clone()
                    .with_metadata([("PARQUET:field_id".to_string(), id.0.to_string())])
            })
            .collect::<Vec<_>>(),
    );
    // Row `i` of history has `k` = i * 7919 + 13 mod the rows, a permutation, so the largest are
    // scattered; each `w{j}` is `k` * 10 + j.
    let k_of = |i: i64| (i * 7_919 + 13) % ROWS;
    let date_rows = |date: Date| {
        let first = i64::from(date.days() - d(1).days()) * DATE_ROWS;
        let k: Vec<i64> = (first..first + DATE_ROWS).map(k_of).collect();
        let mut columns = vec![Arc::new(Int64Array::from(k.clone())) as ArrayRef];
        for j in 0..WIDE as i64 {
            let w = k.iter().map(|k| k * 10 + j);
            columns.push(Arc::new(Int64Array::from_iter_values(w)));
        }
        RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap()
    };
    // Two files: dates 1 and 2, then date 3; a row group a date, of pages of 20,000 rows.
    for dates in [vec![d(1), d(2)], vec![d(3)]] {
        let rows = |date: Date, each: &mut dyn FnMut(RecordBatch) -> Result<(), Error>| {
            each(date_rows(date))
        };
        let written = lake
            .write(&entry.location, &with_ids, &[], &dates, rows, u64::MAX)
            .unwrap();
        let commit = HistoryCommit {
            written: dates,
            replaced: Vec::new(),
            files: written.files,
            dates: written.dates,
        };
        store.commit_history(&name, &commit).unwrap();
    }
    let (addr, asked) = serve_ranges(lake_dir.clone());
    store.vend_history(Arc::new(move |location, file| {
        Ok(format!("http://{addr}/{location}/{file}"))
    }));
    let reader = Arc::new(Lake::reader().unwrap());
    let parts = history_parts(&*store, &reader, &entry, &Reading::default()).unwrap();
    let take_asked = || std::mem::take(&mut *asked.lock().unwrap());
    // The footers, read while planning.
    take_asked();

    // The sort column of every row, with the row's part and place in it.
    let k_at = parts.schema.index_of("k").unwrap();
    let mut keyed: Vec<(i64, usize, u64)> = Vec::new();
    for part in 0..parts.len() {
        let mut row = 0;
        for batch in parts.read(part, &[k_at], None).unwrap() {
            for &k in batch
                .unwrap()
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
            {
                keyed.push((k, part, row));
                row += 1;
            }
        }
    }
    assert_eq!(keyed.len() as i64, ROWS);
    let narrow = take_asked();
    keyed.sort_unstable_by_key(|&(k, ..)| std::cmp::Reverse(k));
    let winners = &keyed[..5];
    let all: Vec<usize> = (0..parts.schema.fields().len()).collect();
    let mut got = Vec::new();
    for part in 0..parts.len() {
        let mut rows: Vec<u64> = winners
            .iter()
            .filter(|&&(_, of, _)| of == part)
            .map(|&(_, _, row)| row)
            .collect();
        if rows.is_empty() {
            continue;
        }
        rows.sort_unstable();
        for batch in parts.read(part, &all, Some(&rows)).unwrap() {
            let batch = batch.unwrap();
            let k = batch.column(k_at).as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                for j in 0..WIDE {
                    let w = batch.column(k_at + 1 + j).as_primitive::<Int64Type>();
                    assert_eq!(w.value(row), k.value(row) * 10 + j as i64);
                }
                got.push(k.value(row));
            }
        }
    }
    let wide = take_asked();
    got.sort_unstable_by_key(|&k| std::cmp::Reverse(k));
    assert_eq!(got, (ROWS - 5..ROWS).rev().collect::<Vec<_>>());

    // Each file, by its path, in date order: its footer and page index.
    let files: Vec<(String, ParquetMetaData)> = store
        .history_files(&name, Date::MIN, Date::MAX)
        .unwrap()
        .iter()
        .map(|file| {
            let path = format!("{}/{}", entry.location, file.name);
            let bytes = Bytes::from(fs::read(lake_dir.join(&path)).unwrap());
            let footer = ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::Required)
                .parse_and_finish(&bytes)
                .unwrap();
            (path, footer)
        })
        .collect();
    // Each winner's file, row group, and row in it: a row group a date, in date order.
    let place = |k: i64| {
        let i = (0..ROWS).find(|&i| k_of(i) == k).unwrap();
        let group = (i / DATE_ROWS) as usize;
        let (file, group) = if group < 2 { (0, group) } else { (1, 0) };
        (file, group, i % DATE_ROWS)
    };
    let places: Vec<(usize, usize, i64)> = got.iter().map(|&k| place(k)).collect();
    // The file, row group, and column holding each range asked.
    let chunk = |path: &str, range: &Range<u64>| {
        let file = files.iter().position(|(p, _)| p == path).unwrap();
        let footer = &files[file].1;
        for group in 0..footer.num_row_groups() {
            for (column, chunk) in footer.row_group(group).columns().iter().enumerate() {
                let (start, length) = chunk.byte_range();
                if start <= range.start && range.end <= start + length {
                    return (file, group, column);
                }
            }
        }
        panic!("{path} {range:?} is in no column chunk");
    };
    let name_of = |file: usize, column: usize| {
        let schema = files[file].1.file_metadata().schema_descr();
        schema.column(column).name().to_owned()
    };

    let mut sorted = std::collections::BTreeSet::new();
    for (path, range) in &narrow {
        let (file, group, column) = chunk(path, range);
        let name = name_of(file, column);
        assert!(
            name == "Date" || name == "k",
            "the first read asked for {name}"
        );
        if name == "k" {
            sorted.insert((file, group));
        }
    }
    assert_eq!(
        sorted.len(),
        3,
        "the first read asked for `k` of every row group"
    );

    assert!(!wide.is_empty());
    for (path, range) in &wide {
        let (file, group, column) = chunk(path, range);
        let rows: Vec<i64> = places
            .iter()
            .filter(|&&(f, g, _)| (f, g) == (file, group))
            .map(|&(_, _, row)| row)
            .collect();
        assert!(
            !rows.is_empty(),
            "{range:?} of a row group holding no winner"
        );
        let footer = &files[file].1;
        let start = footer.row_group(group).column(column).byte_range().0;
        let pages = footer
            .page_index_for_row_group(group)
            .page_locations(column)
            .unwrap()
            .clone();
        let in_group = footer.row_group(group).num_rows();
        // The chunk's dictionary, before its first data page, then the pages holding winners.
        let mut allowed = Vec::new();
        allowed.push(start..pages[0].offset as u64);
        for (at, page) in pages.iter().enumerate() {
            let end = pages
                .get(at + 1)
                .map_or(in_group, |next| next.first_row_index);
            if rows
                .iter()
                .any(|&row| page.first_row_index <= row && row < end)
            {
                let offset = page.offset as u64;
                allowed.push(offset..offset + page.compressed_page_size as u64);
            }
        }
        assert!(
            allowed
                .iter()
                .any(|page| page.start <= range.start && range.end <= page.end),
            "{} asked {range:?}, beyond the pages of its winners {allowed:?}",
            name_of(file, column)
        );
    }
}
