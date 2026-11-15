//! A store served on localhost, used by separate clients as separate processes would.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_flight::{Client, serve};
use kermodes_weir::{Change, Error, SourceId, Store, TableName};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-flight-{}-{}",
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

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
    ])
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

/// Waits (up to 10 s) until `seen` holds `want`: a subscriber's batches arrive on its own task.
fn eventually(seen: &Mutex<Vec<i64>>, want: &[i64]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while *seen.lock().unwrap() != want {
        assert!(Instant::now() < deadline, "saw {:?}", seen.lock().unwrap());
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn clients_share_a_served_store() {
    let dir = TempDir::new();
    let store = Arc::new(Store::open(&dir.0).unwrap());
    let server = serve(store, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("grpc://{}", server.addr());

    let producer = Client::connect(&url).unwrap();
    producer.create_table(&ticks(), &schema()).unwrap();
    let writer = producer
        .writer(&ticks(), &SourceId::parse("feed").unwrap(), false)
        .unwrap();
    assert_eq!(writer.last_seq(), None);
    assert!(writer.write(&batch(0..3), None).unwrap());

    // Another process sees what was written, then each new batch.
    let reader = Client::connect(&url).unwrap();
    let names: Vec<String> = reader
        .tables()
        .unwrap()
        .iter()
        .map(|(n, _)| n.to_string())
        .collect();
    assert_eq!(names, ["md.ticks"]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let lost = Arc::new(Mutex::new(None::<String>));
    let reason = lost.clone();
    // How many values the reader had when told it caught up: all the table held, no fewer.
    let at_caught_up = Arc::new(Mutex::new(None::<Vec<i64>>));
    let (had, ready) = (seen.clone(), at_caught_up.clone());
    let schema = reader
        .subscribe(
            &ticks(),
            Box::new(move |batch, _, _| {
                let n = batch.column(0).as_primitive::<Int64Type>();
                sink.lock().unwrap().extend(n.values().iter().copied());
                true
            }),
            Box::new(move || *ready.lock().unwrap() = Some(had.lock().unwrap().clone())),
            Box::new(move |why| *reason.lock().unwrap() = Some(why)),
        )
        .unwrap();
    assert_eq!(schema.field(1).name(), "sym");
    eventually(&seen, &[0, 1, 2]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while at_caught_up.lock().unwrap().is_none() {
        assert!(Instant::now() < deadline, "never told it caught up");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        at_caught_up.lock().unwrap().as_deref(),
        Some(&[0, 1, 2][..])
    );
    // Columns in another order are matched by name.
    let reordered = RecordBatch::try_from_iter([
        ("sym", Arc::new(StringArray::from(vec!["x"])) as ArrayRef),
        ("n", Arc::new(Int64Array::from(vec![3]))),
    ])
    .unwrap();
    assert!(writer.write(&reordered, None).unwrap());
    // A resend is acknowledged but not written, and not delivered again.
    assert!(!writer.write(&batch(100..101), Some(1)).unwrap());
    assert_eq!(writer.last_seq(), Some(2));
    eventually(&seen, &[0, 1, 2, 3]);

    // One writer per source across clients; the source is free again once its writer goes.
    let feed = SourceId::parse("feed").unwrap();
    assert!(matches!(
        reader.writer(&ticks(), &feed, false),
        Err(Error::Busy(_))
    ));
    drop(writer);
    let deadline = Instant::now() + Duration::from_secs(10);
    let resumed = loop {
        match reader.writer(&ticks(), &feed, false) {
            Ok(writer) => break writer,
            Err(Error::Busy(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10))
            }
            Err(e) => panic!("{e}"),
        }
    };
    assert_eq!(resumed.last_seq(), Some(2));
    assert!(resumed.write(&batch(4..5), None).unwrap());
    eventually(&seen, &[0, 1, 2, 3, 4]);

    // Errors keep their kind across the wire.
    let nope = TableName::parse("md.nope").unwrap();
    assert!(matches!(reader.table(&nope), Err(Error::NotFound(_))));
    let other = Schema::new(vec![Field::new("n", DataType::Float64, true)]);
    assert!(matches!(
        reader.create_table(&ticks(), &other),
        Err(Error::Schema(_))
    ));
    let wrong =
        RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    assert!(matches!(resumed.write(&wrong, None), Err(Error::Schema(_))));

    // A subscriber learns when the server goes rather than waiting forever.
    assert_eq!(*lost.lock().unwrap(), None);
    let mut server = server;
    server.stop();
    let deadline = Instant::now() + Duration::from_secs(10);
    while lost.lock().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the subscriber was never told");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A rolling handover between processes: the new writer waits for the source's lease, and resumes
/// after every batch the old one wrote, including those written while it waited.
#[test]
fn a_waiting_writer_resumes_after_the_last_writers_final_batch() {
    let dir = TempDir::new();
    let store = Arc::new(Store::open(&dir.0).unwrap());
    let server = serve(store, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("grpc://{}", server.addr());
    let feed = SourceId::parse("feed").unwrap();

    let old = Client::connect(&url).unwrap();
    old.create_table(&ticks(), &schema()).unwrap();
    let writer = old.writer(&ticks(), &feed, false).unwrap();
    assert_eq!(writer.epoch(), Some(1));
    assert!(writer.write(&batch(0..3), None).unwrap());

    let waiting = {
        let (url, feed) = (url.clone(), feed.clone());
        std::thread::spawn(move || {
            let new = Client::connect(&url).unwrap();
            let writer = new.writer(&ticks(), &feed, true).unwrap();
            (writer.epoch(), writer.last_seq())
        })
    };
    // Long enough that the new writer is usually waiting already; it is right either way.
    std::thread::sleep(Duration::from_millis(100));
    assert!(writer.write(&batch(3..4), None).unwrap());
    assert!(writer.write(&batch(4..5), None).unwrap());
    drop(writer);
    assert_eq!(waiting.join().unwrap(), (Some(2), Some(3)));
}

/// A write is one atomic batch over Flight up to 64 MiB, far past gRPC's 4 MiB default; a larger
/// one is refused before it is sent, as too large rather than as a lost connection a connector
/// would retry, and the writer goes on.
#[test]
fn a_write_up_to_the_batch_limit_lands_whole_and_a_larger_one_is_refused() {
    let dir = TempDir::new();
    let store = Arc::new(Store::open(&dir.0).unwrap());
    let server = serve(store.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let client = Client::connect(&format!("grpc://{}", server.addr())).unwrap();
    client.create_table(&ticks(), &schema()).unwrap();
    let writer = client
        .writer(&ticks(), &SourceId::parse("feed").unwrap(), false)
        .unwrap();

    // About 40 MiB, then about 80.
    assert!(writer.write(&batch(0..2_000_000), None).unwrap());
    let refused = writer.write(&batch(0..4_000_000), None).unwrap_err();
    assert!(matches!(refused, Error::TooLarge(_)), "{refused}");
    assert!(writer.write(&batch(2_000_000..2_000_010), None).unwrap());
    assert_eq!(writer.last_seq(), Some(2));

    let batches = Arc::new(Mutex::new(Vec::new()));
    let seen = batches.clone();
    store
        .subscribe(
            &ticks(),
            Box::new(move |batch, _, _| {
                seen.lock().unwrap().push(batch.num_rows());
                true
            }),
        )
        .unwrap();
    assert_eq!(*batches.lock().unwrap(), [2_000_000, 10]);
}

/// A reader whose history is far larger than the server's queue (about 150 MiB, against 16 MiB)
/// gets all of it, in order and once, while a writer goes on: the server's replay waits for the
/// reader instead of holding the day.
#[test]
fn a_long_history_reaches_a_remote_reader_once_while_writes_go_on() {
    const ROWS: i64 = 50_000;
    let dir = TempDir::new();
    let store = Arc::new(Store::open(&dir.0).unwrap());
    store.create_table(&ticks(), &schema()).unwrap();
    let writer = store
        .writer(&ticks(), &SourceId::parse("feed").unwrap(), false)
        .unwrap();
    for v in 0..60 {
        writer
            .write(&batch(v * ROWS..(v + 1) * ROWS), None)
            .unwrap();
    }
    let server = serve(store, "127.0.0.1:0".parse().unwrap()).unwrap();
    let reader = Client::connect(&format!("grpc://{}", server.addr())).unwrap();
    // Whether each value came once and in order, and how many came.
    let next = Arc::new(AtomicU64::new(0));
    let in_order = Arc::new(AtomicU64::new(1));
    let (expect, ordered) = (next.clone(), in_order.clone());
    reader
        .subscribe(
            &ticks(),
            Box::new(move |batch, _, _| {
                // A reader slower than the disk, so the server's queue fills and its replay waits.
                std::thread::sleep(Duration::from_millis(5));
                for &n in batch.column(0).as_primitive::<Int64Type>().values() {
                    if n as u64 != expect.fetch_add(1, Ordering::Relaxed) {
                        ordered.store(0, Ordering::Relaxed);
                    }
                }
                true
            }),
            Box::new(|| {}),
            Box::new(|why| panic!("lost the stream: {why}")),
        )
        .unwrap();
    for v in 60..70 {
        writer
            .write(&batch(v * ROWS..(v + 1) * ROWS), None)
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while next.load(Ordering::Relaxed) < 70 * ROWS as u64 {
        assert!(
            Instant::now() < deadline,
            "{} rows",
            next.load(Ordering::Relaxed)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(next.load(Ordering::Relaxed), 70 * ROWS as u64);
    assert_eq!(
        in_order.load(Ordering::Relaxed),
        1,
        "every value once, in order"
    );
}

/// A keyed table's upserts and deletes cross the wire with their kind: a remote reader gets the
/// day's checkpoint, then each change tagged, a delete written as its key alone.
#[test]
fn keyed_changes_cross_the_wire_with_their_kind() {
    let dir = TempDir::new();
    let store = Arc::new(Store::open(&dir.0).unwrap());
    let server = serve(store, "127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!("grpc://{}", server.addr());
    let quotes = TableName::parse("md.quotes").unwrap();
    let feed = SourceId::parse("feed").unwrap();

    let producer = Client::connect(&url).unwrap();
    producer
        .create_keyed_table(&quotes, &schema(), &["sym".to_string()])
        .unwrap();
    let writer = producer.writer(&quotes, &feed, false).unwrap();
    assert!(writer.upsert(&batch(0..3), None).unwrap());
    let key =
        RecordBatch::try_from_iter([("sym", Arc::new(StringArray::from(vec!["s1"])) as ArrayRef)])
            .unwrap();
    assert!(writer.delete(&key, None).unwrap());

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    // Kept: a client's subscriptions run on its runtime.
    let reader = Client::connect(&url).unwrap();
    reader
        .subscribe(
            &quotes,
            Box::new(move |batch, stamp, _| {
                let change = stamp.change;
                let n = batch.column(0).as_primitive::<Int64Type>();
                let syms = batch.column(1).as_string::<i32>();
                let rows: Vec<(Option<i64>, String)> = (0..batch.num_rows())
                    .map(|i| (n.is_valid(i).then(|| n.value(i)), syms.value(i).to_string()))
                    .collect();
                sink.lock().unwrap().push((change, rows));
                true
            }),
            Box::new(|| {}),
            Box::new(|_| {}),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.lock().unwrap().len() < 3 {
        assert!(Instant::now() < deadline, "saw {:?}", seen.lock().unwrap());
        std::thread::sleep(Duration::from_millis(5));
    }
    let seen = seen.lock().unwrap().clone();
    let kinds: Vec<Change> = seen.iter().map(|(change, _)| *change).collect();
    // The day's (empty) opening state crosses too: an empty checkpoint clears a reader's table.
    assert_eq!(kinds, [Change::Checkpoint, Change::Upsert, Change::Delete]);
    assert!(seen[0].1.is_empty());
    assert_eq!(seen[1].1.len(), 3);
    assert_eq!(seen[2].1, [(None, "s1".to_string())]);

    // A keyed table takes no plain rows, an append table no upserts.
    assert!(matches!(
        writer.write(&batch(5..6), None),
        Err(Error::Schema(_))
    ));
    producer.create_table(&ticks(), &schema()).unwrap();
    let append = producer.writer(&ticks(), &feed, false).unwrap();
    assert!(matches!(
        append.upsert(&batch(0..1), None),
        Err(Error::Schema(_))
    ));
}

/// A viewer reads rows by position over the wire as the store gives them: the day's count with
/// any window, only the columns asked for, a count alone, and a keyed table's refusal.
#[test]
fn rows_by_position_cross_the_wire() {
    let dir = TempDir::new();
    let store = Arc::new(Store::open(&dir.0).unwrap());
    let server = serve(store.clone(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let client = Client::connect(&format!("grpc://{}", server.addr())).unwrap();
    client.create_table(&ticks(), &schema()).unwrap();
    let writer = client
        .writer(&ticks(), &SourceId::parse("feed").unwrap(), false)
        .unwrap();
    for (from, to) in [(0, 3), (3, 4), (4, 9)] {
        writer.write(&batch(from..to), None).unwrap();
    }
    let ns = |rows: &[RecordBatch]| -> Vec<i64> {
        rows.iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect()
    };
    let (total, rows) = client.rows_at(&ticks(), 2, 4, None).unwrap();
    assert_eq!((total, ns(&rows)), (9, vec![2, 3, 4, 5]));
    let (_, local) = store.rows_at(&ticks(), 2, 4, None).unwrap();
    assert_eq!(rows, local, "as the store gives them");

    let only = ["sym".to_owned()];
    let (_, rows) = client.rows_at(&ticks(), 7, 10, Some(&only)).unwrap();
    let syms: Vec<_> = rows
        .iter()
        .flat_map(|b| {
            let syms = b.column(0).as_string::<i32>();
            (0..syms.len())
                .map(|i| syms.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        (rows[0].num_columns(), syms),
        (1, vec!["s7".to_owned(), "s8".to_owned()])
    );
    assert_eq!(
        client.rows_at(&ticks(), 0, 0, None).unwrap(),
        (9, Vec::new())
    );

    let keyed = TableName::parse("md.quotes").unwrap();
    client
        .create_keyed_table(&keyed, &schema(), &["sym".to_owned()])
        .unwrap();
    assert!(matches!(
        client.rows_at(&keyed, 0, 1, None),
        Err(Error::Schema(_))
    ));
}
