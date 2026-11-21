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
use kermodes_lake::{
    History, Lake, Merged, Reading, Weir, collect_garbage, merge, read_history, write_history,
};
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
