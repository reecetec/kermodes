//! The WebSocket connector against real feeds on localhost: a scripted one for the reading rules,
//! and the synthetic ticker into a real store.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef, TimeUnit};
use futures::SinkExt;
use kermodes_ingest::{Options, Stats, ticker, websocket};
use kermodes_weir::{SourceId, Store, TableName};
use tokio_tungstenite::tungstenite::Message;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]))
}

/// Serves each connection the next script of messages, then closes it; the last connection
/// stays open after its script.
fn scripted_feed(scripts: Vec<Vec<Message>>) -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let count = scripts.len();
            for (index, script) in scripts.into_iter().enumerate() {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                for message in script {
                    socket.send(message).await.unwrap();
                }
                if index + 1 == count {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                let _ = socket.close(None).await;
            }
        });
    });
    (url, handle)
}

fn wait_for(what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn feeds_are_read_tolerantly_and_reconnected() {
    let text = |s: &str| Message::Text(s.into());
    let (url, _feed) = scripted_feed(vec![
        vec![
            // Unknown fields are ignored.
            text(r#"{"sym":"a","price":1.5,"size":10,"ts":"2026-11-11T09:30:00Z","venue":"x"}"#),
            // An array is read record by record: a bad one is skipped alone, and missing fields
            // are NULL.
            text(r#"[{"sym":"b","price":2,"size":20},{"sym":"c","price":"high"},{"sym":"d"}]"#),
            text("not json {"),
            Message::Binary(br#"{"sym":"e","price":5.0,"size":50}"#.to_vec().into()),
        ],
        // The feed drops; the connector reconnects and goes on.
        vec![text(r#"{"sym":"f","price":6.0,"size":60}"#)],
    ]);
    let written = Arc::new(Mutex::new(Vec::<RecordBatch>::new()));
    let sink = written.clone();
    let options = Options {
        flush: Duration::from_millis(20),
        max_rows: 1_000,
    };
    let mut ingest = websocket(
        &url,
        schema(),
        options,
        Box::new(move |batch| {
            sink.lock().unwrap().push(batch.clone());
            Ok(true)
        }),
    )
    .unwrap();
    wait_for("five rows", || ingest.stats().rows == 5);
    ingest.stop();
    let stats = ingest.stats();
    assert_eq!(
        (
            stats.messages,
            stats.skipped,
            stats.reconnects,
            stats.failed_writes
        ),
        (5, 2, 1, 0),
        "{stats:?}"
    );

    let rows = concat_batches(&schema(), written.lock().unwrap().iter()).unwrap();
    let syms: Vec<&str> = rows.column(0).as_string::<i32>().iter().flatten().collect();
    assert_eq!(syms, ["a", "b", "d", "e", "f"]);
    let prices = rows.column(1).as_primitive::<Float64Type>();
    assert!(prices.is_null(2), "d has no price");
    assert_eq!(prices.value(1), 2.0);
    let ts = rows.column(3);
    assert_eq!(
        ts.as_primitive::<arrow::datatypes::TimestampNanosecondType>()
            .value(0),
        1_794_389_400_000_000_000,
        "2026-11-11T09:30:00Z"
    );
    assert!(ts.is_null(1));
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The M1 acceptance path, in-process: a synthetic ticker over WebSocket into weir.
#[test]
fn a_ticker_feed_lands_in_a_store() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = TempDir(std::env::temp_dir().join(format!(
        "kermodes-ingest-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let store = Store::open(&dir.0).unwrap();
    let name = TableName::parse("md.ticks").unwrap();
    store.create_table(&name, &schema()).unwrap();
    let writer = store
        .writer(&name, &SourceId::parse("ticker").unwrap())
        .unwrap();

    let feed = ticker("127.0.0.1:0".parse().unwrap(), 5_000, 50).unwrap();
    let mut ingest = websocket(
        &format!("ws://{}/", feed.addr()),
        schema(),
        Options::default(),
        Box::new(move |batch| writer.write(batch, None)),
    )
    .unwrap();
    wait_for("1,000 rows", || ingest.stats().rows >= 1_000);
    ingest.stop();
    let Stats {
        rows,
        skipped,
        failed_writes,
        ..
    } = ingest.stats();
    assert_eq!((skipped, failed_writes), (0, 0));

    let stored = Arc::new(AtomicU64::new(0));
    let nulls = Arc::new(AtomicU64::new(0));
    let (count, missing) = (stored.clone(), nulls.clone());
    store
        .subscribe(
            &name,
            Box::new(move |batch| {
                count.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                let null_count: usize = batch.columns().iter().map(|c| c.null_count()).sum();
                missing.fetch_add(null_count as u64, Ordering::Relaxed);
                true
            }),
        )
        .unwrap();
    assert_eq!(stored.load(Ordering::Relaxed), rows);
    assert_eq!(
        nulls.load(Ordering::Relaxed),
        0,
        "every ticker field is read"
    );
}
