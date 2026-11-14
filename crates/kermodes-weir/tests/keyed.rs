//! Keyed tables in a real store: upserts and deletes, the checkpoint each day opens with, and the
//! state carried across days, however its writers and readers come and go.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use kermodes_weir::{
    Change, Date, Error, HistoryCommit, HistoryDate, HistoryFile, SourceId, Store, TableName,
};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-keyed-{}-{}",
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

fn quotes() -> TableName {
    TableName::parse("md.quotes").unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("px", DataType::Float64, true),
    ])
}

fn rows(rows: &[(&str, f64)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "sym",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "px",
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.1))),
        ),
    ])
    .unwrap()
}

fn keys(syms: &[&str]) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "sym",
        Arc::new(StringArray::from_iter_values(syms.iter().copied())) as ArrayRef,
    )])
    .unwrap()
}

fn today() -> Date {
    Date::of_ns(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64,
    )
}

fn store(root: &Path) -> Store {
    let store = Store::open(root).unwrap();
    store
        .create_keyed_table(&quotes(), &schema(), &["sym".to_string()])
        .unwrap();
    store
}

/// What a subscriber gets: each batch's change kind and its `(sym, px)` rows, px NaN when null.
fn subscribe(store: &Store) -> Vec<(Change, Vec<(String, f64)>)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    store
        .subscribe(
            &quotes(),
            Box::new(move |batch, stamp, _| {
                let change = stamp.change;
                let syms = batch.column(0).as_string::<i32>();
                let px = batch.column(1).as_primitive::<Float64Type>();
                let rows = (0..batch.num_rows())
                    .map(|i| {
                        let px = if px.is_null(i) { f64::NAN } else { px.value(i) };
                        (syms.value(i).to_string(), px)
                    })
                    .collect();
                sink.lock().unwrap().push((change, rows));
                true
            }),
        )
        .unwrap();
    seen.lock().unwrap().clone()
}

fn state(seen: &[(Change, Vec<(String, f64)>)]) -> Vec<(String, f64)> {
    assert_eq!(seen.len(), 1, "only the checkpoint: {seen:?}");
    assert_eq!(seen[0].0, Change::Checkpoint);
    seen[0].1.clone()
}

/// Renames today's directory of the table to `day`, as weir holds a day after a restart.
fn roll_to(root: &Path, store: Store, day: &str) {
    let location = store.table(&quotes()).unwrap().location;
    drop(store);
    let dir = root.join("weir").join(location);
    fs::rename(dir.join(today().to_string()), dir.join(day)).unwrap();
}

#[test]
fn keyed_writes_are_upserts_and_deletes_and_a_day_opens_with_its_state() {
    let dir = TempDir::new();
    let s = store(&dir.0);
    let feed = SourceId::parse("feed").unwrap();
    let writer = s.writer(&quotes(), &feed, false).unwrap();
    assert!(matches!(
        writer.write(&rows(&[("a", 1.0)]), None),
        Err(Error::Schema(_))
    ));
    writer
        .upsert(&rows(&[("a", 1.0), ("b", 2.0)]), None)
        .unwrap();
    // A delete needs only the key.
    writer.delete(&keys(&["a"]), None).unwrap();
    writer.upsert(&rows(&[("c", 3.0)]), None).unwrap();
    let ticks = TableName::parse("md.ticks").unwrap();
    s.create_table(&ticks, &schema()).unwrap();
    let append = s.writer(&ticks, &feed, false).unwrap();
    assert!(matches!(
        append.upsert(&rows(&[("a", 1.0)]), None),
        Err(Error::Schema(_))
    ));
    assert!(matches!(
        append.delete(&keys(&["a"]), None),
        Err(Error::Schema(_))
    ));
    drop(append);

    // Today so far: the (empty) state the day opened with, then each change as written.
    let seen = subscribe(&s);
    let kinds: Vec<Change> = seen.iter().map(|(change, _)| *change).collect();
    assert_eq!(
        kinds,
        [
            Change::Checkpoint,
            Change::Upsert,
            Change::Delete,
            Change::Upsert
        ]
    );
    assert!(seen[0].1.is_empty());
    assert_eq!(seen[2].1[0].0, "a");
    assert!(seen[2].1[0].1.is_nan(), "a delete's other columns are null");

    drop(writer);
    roll_to(&dir.0, s, "2026-11-01");
    let s = store(&dir.0);
    // The next day opens with every key's latest row, and nothing else.
    assert_eq!(
        state(&subscribe(&s)),
        [("b".to_string(), 2.0), ("c".to_string(), 3.0)]
    );
}

/// Two writers' changes fold in the order they arrived, not by their sources' names.
#[test]
fn changes_from_several_sources_fold_in_the_order_they_arrived() {
    let dir = TempDir::new();
    let s = store(&dir.0);
    let b = s
        .writer(&quotes(), &SourceId::parse("b").unwrap(), false)
        .unwrap();
    let a = s
        .writer(&quotes(), &SourceId::parse("a").unwrap(), false)
        .unwrap();
    b.upsert(&rows(&[("x", 1.0), ("y", 1.0)]), None).unwrap();
    a.upsert(&rows(&[("x", 2.0)]), None).unwrap();
    b.delete(&keys(&["y"]), None).unwrap();
    a.upsert(&rows(&[("y", 3.0)]), None).unwrap();
    drop((a, b));
    roll_to(&dir.0, s, "2026-11-01");
    let s = store(&dir.0);
    assert_eq!(
        state(&subscribe(&s)),
        [("x".to_string(), 2.0), ("y".to_string(), 3.0)]
    );
}

/// The state survives the day it came from leaving weir, even if nothing touched the table
/// since midnight: dropping the day gives today its checkpoint first.
#[test]
fn dropping_a_day_keeps_the_state_it_carried() {
    let dir = TempDir::new();
    let s = store(&dir.0);
    let writer = s
        .writer(&quotes(), &SourceId::parse("feed").unwrap(), false)
        .unwrap();
    writer
        .upsert(&rows(&[("a", 1.0), ("b", 2.0)]), None)
        .unwrap();
    drop(writer);
    roll_to(&dir.0, s, "2026-11-01");
    let s = store(&dir.0);
    let day = Date::parse("2026-11-01").unwrap();
    // Its history holds the day whole (as a merge would leave it), so weir may drop it.
    let (rows_held, bytes) = s.date_rows(&quotes(), day).unwrap();
    let held = |name: &str| HistoryFile {
        name: name.into(),
        first: day,
        last: day,
        rows: rows_held,
        bytes,
    };
    s.commit_history(
        &quotes(),
        &HistoryCommit {
            written: vec![day],
            replaced: vec![],
            files: vec![held("f")],
            dates: vec![HistoryDate {
                date: day,
                file: "f".into(),
                rows: rows_held,
                bytes,
            }],
        },
    )
    .unwrap();
    s.drop_date(&quotes(), day).unwrap();
    assert_eq!(s.date_rows(&quotes(), day).unwrap(), (0, 0));
    assert_eq!(
        state(&subscribe(&s)),
        [("a".to_string(), 1.0), ("b".to_string(), 2.0)]
    );
}

/// What a subscriber ends with applying what it was given in order, and the rows it was given.
fn replay(store: &Store) -> (std::collections::BTreeMap<String, f64>, usize, Vec<Change>) {
    let seen = Arc::new(Mutex::new((
        std::collections::BTreeMap::new(),
        0,
        Vec::new(),
    )));
    let sink = seen.clone();
    store
        .subscribe(
            &quotes(),
            Box::new(move |batch, stamp, _| {
                let change = stamp.change;
                let mut seen = sink.lock().unwrap();
                let (state, rows, kinds) = &mut *seen;
                if change == Change::Checkpoint {
                    state.clear();
                }
                let syms = batch.column(0).as_string::<i32>();
                let px = batch.column(1).as_primitive::<Float64Type>();
                for i in 0..batch.num_rows() {
                    let sym = syms.value(i).to_string();
                    match change {
                        Change::Delete => state.remove(&sym),
                        _ => state.insert(sym, px.value(i)),
                    };
                }
                *rows += batch.num_rows();
                kinds.push(change);
                true
            }),
        )
        .unwrap();
    let seen = seen.lock().unwrap();
    seen.clone()
}

/// A busy day gets checkpoints as it goes, so a new subscriber reads the latest state and the
/// changes after it, not the whole day.
#[test]
fn a_busy_day_gets_checkpoints_so_subscribers_start_near_now() {
    let dir = TempDir::new();
    let s = store(&dir.0);
    let writer = s
        .writer(&quotes(), &SourceId::parse("feed").unwrap(), false)
        .unwrap();
    const ROWS: usize = 1_000_000;
    let mut written = 0;
    for round in 0..6 {
        let values: Vec<(String, f64)> = (0..ROWS)
            .map(|i| (format!("s{}", i % 8), (round * ROWS + i) as f64))
            .collect();
        let pairs: Vec<(&str, f64)> = values.iter().map(|(s, p)| (s.as_str(), *p)).collect();
        writer.upsert(&rows(&pairs), None).unwrap();
        written += ROWS;
    }
    let (state, delivered, kinds) = replay(&s);
    let want: std::collections::BTreeMap<String, f64> = (ROWS - 8..ROWS)
        .map(|i| (format!("s{}", i % 8), (5 * ROWS + i) as f64))
        .collect();
    assert_eq!(state, want);
    assert_eq!(kinds[0], Change::Checkpoint);
    assert!(
        delivered < written / 2,
        "{delivered} of {written} rows replayed"
    );
}

/// Changes from several sources reach a new subscriber in the order they were committed, so it
/// ends with each key's latest value, not whichever source it happened to read last.
#[test]
fn subscribers_catch_up_on_several_sources_in_commit_order() {
    let dir = TempDir::new();
    let s = store(&dir.0);
    let b = s
        .writer(&quotes(), &SourceId::parse("b").unwrap(), false)
        .unwrap();
    let a = s
        .writer(&quotes(), &SourceId::parse("a").unwrap(), false)
        .unwrap();
    b.upsert(&rows(&[("x", 1.0), ("y", 1.0)]), None).unwrap();
    a.upsert(&rows(&[("x", 2.0)]), None).unwrap();
    b.delete(&keys(&["y"]), None).unwrap();
    a.upsert(&rows(&[("y", 3.0)]), None).unwrap();
    b.upsert(&rows(&[("x", 4.0)]), None).unwrap();
    let (state, _, _) = replay(&s);
    assert_eq!(
        state.into_iter().collect::<Vec<_>>(),
        [("x".to_string(), 4.0), ("y".to_string(), 3.0)]
    );
}
