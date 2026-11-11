//! A store served on localhost, used by separate clients as separate processes would.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_flight::{Client, serve};
use kermodes_weir::{Error, SourceId, Store, TableName};

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
        .writer(&ticks(), &SourceId::parse("feed").unwrap())
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
    let schema = reader
        .subscribe(
            &ticks(),
            Box::new(move |batch| {
                let n = batch.column(0).as_primitive::<Int64Type>();
                sink.lock().unwrap().extend(n.values().iter().copied());
                true
            }),
        )
        .unwrap();
    assert_eq!(schema.field(1).name(), "sym");
    eventually(&seen, &[0, 1, 2]);
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
        reader.writer(&ticks(), &feed),
        Err(Error::Busy(_))
    ));
    drop(writer);
    let deadline = Instant::now() + Duration::from_secs(10);
    let resumed = loop {
        match reader.writer(&ticks(), &feed) {
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
}
