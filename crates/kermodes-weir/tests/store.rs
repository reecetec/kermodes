//! The store as embedded processes use it, on real files: durability across reopening, recovery
//! from writes a crash cut short, subscriptions, and the rules on names, columns, and owners.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, StringArray, UInt8Array};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_weir::{Error, SourceId, Store, TableName};

/// A fresh directory under the system temp dir, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-weir-{}-{}",
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
        Field::new("n", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
    ])
}

fn ticks() -> TableName {
    TableName::parse("md.ticks").unwrap()
}

fn source(name: &str) -> SourceId {
    SourceId::parse(name).unwrap()
}

fn batch(values: std::ops::Range<i64>) -> RecordBatch {
    let syms: Vec<String> = values.clone().map(|v| format!("s{v}")).collect();
    RecordBatch::try_from_iter([
        (
            "n",
            Arc::new(Int64Array::from_iter_values(values)) as ArrayRef,
        ),
        ("sym", Arc::new(StringArray::from(syms))),
    ])
    .unwrap()
}

/// Every committed value of `n`, in the order a subscriber receives them. The subscriber holds
/// its sink weakly, as a live table's feed does, so it unsubscribes at the next batch after this.
fn values(store: &Store) -> Vec<i64> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::downgrade(&seen);
    store
        .subscribe(
            &ticks(),
            Box::new(move |batch| {
                let Some(sink) = sink.upgrade() else {
                    return false;
                };
                let n = batch.column(0).as_primitive::<Int64Type>();
                sink.lock().unwrap().extend(n.values().iter().copied());
                true
            }),
        )
        .unwrap();
    seen.lock().unwrap().clone()
}

/// The table's segment files in its only partition, `(index, stream)`, oldest first.
fn segments(root: &Path) -> Vec<(PathBuf, PathBuf)> {
    let mut found = Vec::new();
    let mut dirs = vec![root.join("weir")];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "idx") {
                found.push((path.clone(), path.with_extension("arrows")));
            }
        }
    }
    found.sort();
    found
}

fn append(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
}

#[test]
fn rows_survive_reopening_and_resends_are_dropped() {
    let dir = TempDir::new();
    {
        let store = Store::open(&dir.0).unwrap();
        store.create_table(&ticks(), &schema()).unwrap();
        let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
        assert_eq!(writer.last_seq(), None);
        assert!(writer.write(&batch(0..3), None).unwrap());
        assert!(writer.write(&batch(3..5), None).unwrap());
        assert!(writer.write(&batch(5..6), Some(10)).unwrap());
        // A resend of a sequence number already written changes nothing.
        assert!(!writer.write(&batch(100..200), Some(2)).unwrap());
        assert!(!writer.write(&batch(100..200), Some(10)).unwrap());
        assert_eq!(writer.last_seq(), Some(10));
    }
    let store = Store::open(&dir.0).unwrap();
    assert_eq!(values(&store), (0..6).collect::<Vec<_>>());
    let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
    assert_eq!(writer.last_seq(), Some(10));
    assert!(writer.write(&batch(6..7), None).unwrap());
    assert_eq!(writer.last_seq(), Some(11));
    assert_eq!(values(&store), (0..7).collect::<Vec<_>>());
}

#[test]
fn recovery_keeps_committed_batches_and_drops_what_a_crash_cut_short() {
    let dir = TempDir::new();
    {
        let store = Store::open(&dir.0).unwrap();
        store.create_table(&ticks(), &schema()).unwrap();
        let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
        for seq in 0..3 {
            writer.write(&batch(seq * 10..seq * 10 + 10), None).unwrap();
        }
    }
    // A crash mid-write: half a batch in the stream and part of its index record.
    let (index, stream) = segments(&dir.0).pop().unwrap();
    append(&stream, &[0xff; 300]);
    append(&index, &[0x7f; 20]);
    {
        let store = Store::open(&dir.0).unwrap();
        assert_eq!(values(&store), (0..30).collect::<Vec<_>>());
        let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
        assert_eq!(writer.last_seq(), Some(3));
        writer.write(&batch(30..40), None).unwrap();
    }
    // A crash while the last index record was being written: it fails its check, so that batch
    // was never committed and its writer, which got no acknowledgement, sends it again.
    let (index, _) = segments(&dir.0).pop().unwrap();
    let mut bytes = fs::read(&index).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    fs::write(&index, bytes).unwrap();
    let store = Store::open(&dir.0).unwrap();
    assert_eq!(values(&store), (0..30).collect::<Vec<_>>());
    let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
    assert_eq!(writer.last_seq(), Some(3));
    assert!(writer.write(&batch(30..40), Some(4)).unwrap());
    assert_eq!(values(&store), (0..40).collect::<Vec<_>>());
    drop(writer);
    drop(store);
    // A crash just after a new segment's files were created, before its first commit: the
    // segment is discarded and its number reused.
    let (index, stream) = segments(&dir.0).pop().unwrap();
    let name = |path: &Path, n: u32| {
        let extension = path.extension().unwrap().to_str().unwrap().to_owned();
        path.with_file_name(format!("seg-{n:06}.{extension}"))
    };
    let next = segments(&dir.0).len() as u32 + 1;
    fs::copy(&index, name(&index, next)).unwrap();
    let header = fs::read(&index).unwrap()[..8].to_vec();
    fs::write(name(&index, next), header).unwrap();
    fs::write(name(&stream, next), [0xab; 64]).unwrap();
    let store = Store::open(&dir.0).unwrap();
    assert_eq!(values(&store), (0..40).collect::<Vec<_>>());
    let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
    assert!(writer.write(&batch(40..50), None).unwrap());
    assert_eq!(values(&store), (0..50).collect::<Vec<_>>());
}

#[test]
fn subscribers_get_every_committed_batch_once_then_each_new_one() {
    let dir = TempDir::new();
    let store = Store::open(&dir.0).unwrap();
    store.create_table(&ticks(), &schema()).unwrap();
    let east = store.writer(&ticks(), &source("east"), false).unwrap();
    let west = store.writer(&ticks(), &source("west"), false).unwrap();
    east.write(&batch(0..2), None).unwrap();
    west.write(&batch(2..4), None).unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    store
        .subscribe(
            &ticks(),
            Box::new(move |batch| {
                let n = batch.column(0).as_primitive::<Int64Type>();
                sink.lock().unwrap().extend(n.values().iter().copied());
                true
            }),
        )
        .unwrap();
    // One that unsubscribes at its first new batch is not called again.
    let calls = Arc::new(AtomicU64::new(0));
    let counter = calls.clone();
    store
        .subscribe(
            &ticks(),
            Box::new(move |_| counter.fetch_add(1, Ordering::Relaxed) < 2),
        )
        .unwrap();
    west.write(&batch(4..5), None).unwrap();
    east.write(&batch(5..6), None).unwrap();
    // A resend is not delivered again.
    east.write(&batch(5..6), Some(2)).unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2, 3, 4, 5]);
    assert_eq!(
        calls.load(Ordering::Relaxed),
        3,
        "two existing batches, then one new"
    );
}

#[test]
fn one_process_owns_a_store_and_one_writer_owns_a_source() {
    let dir = TempDir::new();
    let store = Store::open(&dir.0).unwrap();
    assert!(matches!(Store::open(&dir.0), Err(Error::Busy(_))));
    store.create_table(&ticks(), &schema()).unwrap();
    let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
    assert!(matches!(
        store.writer(&ticks(), &source("feed"), false),
        Err(Error::Busy(_))
    ));
    let other = store.writer(&ticks(), &source("other"), false).unwrap();
    drop(writer);
    store.writer(&ticks(), &source("feed"), false).unwrap();
    drop(other);
    drop(store);
    Store::open(&dir.0).unwrap();
}

#[test]
fn a_waiting_writer_takes_the_lease_with_a_new_epoch() {
    let dir = TempDir::new();
    {
        let store = Arc::new(Store::open(&dir.0).unwrap());
        store.create_table(&ticks(), &schema()).unwrap();
        let first = store.writer(&ticks(), &source("feed"), false).unwrap();
        assert_eq!(first.epoch(), 1);
        first.write(&batch(0..2), None).unwrap();

        // A second writer waits for the first to go, then resumes after its last batch.
        let waiting = {
            let store = store.clone();
            std::thread::spawn(move || {
                let next = store.writer(&ticks(), &source("feed"), true).unwrap();
                (next.epoch(), next.last_seq())
            })
        };
        // Long enough that the second writer is usually waiting already; it is right either way.
        std::thread::sleep(std::time::Duration::from_millis(50));
        first.write(&batch(2..4), None).unwrap();
        drop(first);
        assert_eq!(waiting.join().unwrap(), (2, Some(2)));
    }

    // Epochs outlive the store's process: the next lease is above every earlier one.
    let store = Store::open(&dir.0).unwrap();
    let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
    assert_eq!(writer.epoch(), 3);
    writer.write(&batch(4..5), None).unwrap();

    // Each index record carries the epoch of the lease its batch was written under.
    let epochs: Vec<u64> = segments(&dir.0)
        .iter()
        .flat_map(|(index, _)| {
            let bytes = fs::read(index).unwrap();
            let (records, _) = bytes[8..].as_chunks::<48>();
            records
                .iter()
                .map(|record| u64::from_le_bytes(record[32..40].try_into().unwrap()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(epochs, [1, 1, 3]);
}

#[test]
fn names_definitions_and_columns_are_checked() {
    for bad in ["ticks", "md.Ticks", "md.ticks.x", "1md.ticks", "md."] {
        assert!(TableName::parse(bad).is_err(), "{bad}");
    }
    assert_eq!(
        TableName::parse("md_2.ticks_1m").unwrap().to_string(),
        "md_2.ticks_1m"
    );
    // The platform's tables are readable by name; user code cannot create or write them.
    assert!(TableName::parse("sys.deployments").is_ok());
    assert!(TableName::parse_user("sys.deployments").is_err());
    assert!(TableName::parse_user("md.ticks").is_ok());
    for bad in ["", "Feed", "-feed", "a/b", "../x"] {
        assert!(SourceId::parse(bad).is_err(), "{bad}");
    }

    let dir = TempDir::new();
    let store = Store::open(&dir.0).unwrap();
    let first = store.create_table(&ticks(), &schema()).unwrap();
    assert_eq!(first.location, format!("local/md/ticks.{}", first.id));
    // The same definition again is the same table; a different one is refused.
    assert_eq!(
        store.create_table(&ticks(), &schema()).unwrap().id,
        first.id
    );
    let other = Schema::new(vec![Field::new("n", DataType::Float64, true)]);
    let Err(Error::Schema(message)) = store.create_table(&ticks(), &other) else {
        panic!("a conflicting definition should fail");
    };
    assert!(
        message.contains("already exists with columns (n int64, sym utf8)"),
        "{message}"
    );
    let odd = Schema::new(vec![Field::new("b", DataType::UInt8, true)]);
    let name = TableName::parse("md.odd").unwrap();
    assert!(matches!(
        store.create_table(&name, &odd),
        Err(Error::Schema(_))
    ));
    assert!(matches!(
        store.writer(
            &TableName::parse("md.nope").unwrap(),
            &source("feed"),
            false
        ),
        Err(Error::NotFound(_))
    ));

    let writer = store.writer(&ticks(), &source("feed"), false).unwrap();
    // Columns match by name, in any order; anything else is refused before it is written.
    let reordered = RecordBatch::try_from_iter([
        ("sym", Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
        ("n", Arc::new(Int64Array::from(vec![7]))),
    ])
    .unwrap();
    assert!(writer.write(&reordered, None).unwrap());
    let wrong_type = RecordBatch::try_from_iter([
        ("n", Arc::new(UInt8Array::from(vec![1])) as ArrayRef),
        ("sym", Arc::new(StringArray::from(vec!["x"]))),
    ])
    .unwrap();
    assert!(matches!(
        writer.write(&wrong_type, None),
        Err(Error::Schema(_))
    ));
    let missing =
        RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(matches!(
        writer.write(&missing, None),
        Err(Error::Schema(_))
    ));
    assert_eq!(values(&store), vec![7]);
    let tables: Vec<String> = store
        .tables()
        .unwrap()
        .iter()
        .map(|t| t.name.to_string())
        .collect();
    assert_eq!(tables, ["md.ticks"]);
}

/// Past 64 MiB a segment stops taking batches and the next one starts; all of them are read back,
/// before and after reopening.
#[test]
fn full_segments_roll_over() {
    let dir = TempDir::new();
    let rows = 1 << 20;
    let schema = Schema::new(vec![Field::new("n", DataType::Int64, true)]);
    let big = |k: i64| {
        RecordBatch::try_from_iter([(
            "n",
            Arc::new(Int64Array::from_iter_values(k * rows..(k + 1) * rows)) as ArrayRef,
        )])
        .unwrap()
    };
    let name = TableName::parse("md.big").unwrap();
    let count = |store: &Store| {
        let total = Arc::new(AtomicU64::new(0));
        let sink = Arc::downgrade(&total);
        store
            .subscribe(
                &name,
                Box::new(move |batch| {
                    let Some(sink) = sink.upgrade() else {
                        return false;
                    };
                    let n = batch.column(0).as_primitive::<Int64Type>();
                    assert_eq!(n.value(0) as u64 % rows as u64, 0);
                    sink.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    true
                }),
            )
            .unwrap();
        total.load(Ordering::Relaxed)
    };
    {
        let store = Store::open(&dir.0).unwrap();
        store.create_table(&name, &schema).unwrap();
        let writer = store.writer(&name, &source("feed"), false).unwrap();
        for k in 0..10 {
            writer.write(&big(k), None).unwrap();
        }
        assert_eq!(count(&store), 10 * rows as u64);
    }
    assert_eq!(segments(&dir.0).len(), 2);
    let store = Store::open(&dir.0).unwrap();
    assert_eq!(count(&store), 10 * rows as u64);
}
