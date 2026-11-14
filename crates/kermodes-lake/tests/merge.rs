//! Merging past dates from a real store into a real history directory: rows move whole, small
//! dates share a file, a re-run changes nothing, and weir never drops what history lacks.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_lake::{Lake, Merged, collect_garbage, merge};
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
        for batch in lake.read(&location, &file.name, first, last).unwrap() {
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

    assert_eq!(
        merge(&store, &lake, &ticks(), d(1)).unwrap(),
        Merged::default()
    );
    assert_eq!(store.date_rows(&ticks(), d(1)).unwrap(), (0, 0));
    assert_eq!(history(&store, &lake, d(1), d(1)), [1, 2]);
    assert_eq!(store.history_files(&ticks(), d(1), d(1)).unwrap().len(), 1);
}
