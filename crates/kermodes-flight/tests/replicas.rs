//! Three replicas of a shard on localhost, sharing a catalog in Postgres, as three processes run
//! them (architecture.md §7.6): writes go through whichever leads, every replica holds them, a
//! replica stopping, the leader too, loses none that were acknowledged, and one whose disk was
//! lost rejoins as a new member. Postgres runs when `KERMODES_TEST_POSTGRES` names a server.

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow::array::{AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_flight::{Client, serve_replica};
use kermodes_weir::{Error, LogLimits, Replica, SourceId, TableName};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

const REPLICAS: usize = 3;

fn postgres() -> Option<String> {
    let Ok(url) = std::env::var("KERMODES_TEST_POSTGRES") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set KERMODES_TEST_POSTGRES: replicas share their catalog in Postgres"
        );
        eprintln!("KERMODES_TEST_POSTGRES is not set: skipping replicas");
        return None;
    };
    Some(url)
}

/// A replica running: what stops it, and its thread.
type Running = (Arc<AtomicBool>, JoinHandle<Result<(), Error>>);

/// A shard's replicas, each its store in its own directory and its server on its own port.
struct Shard {
    dir: PathBuf,
    catalog: String,
    /// The catalog branch of this test alone: its tables, and its shard.
    branch: String,
    addrs: Vec<SocketAddr>,
    running: Vec<Option<Running>>,
}

impl Shard {
    fn new(catalog: String) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(format!("kermodes-replicas-{name}"));
        let _ = fs::remove_dir_all(&dir);
        let addrs = (0..REPLICAS)
            .map(|_| {
                let free = TcpListener::bind("127.0.0.1:0").unwrap();
                free.local_addr().unwrap()
            })
            .collect();
        let mut shard = Self {
            dir,
            catalog,
            branch: format!("test-{name}"),
            addrs,
            running: (0..REPLICAS).map(|_| None).collect(),
        };
        for i in 0..REPLICAS {
            shard.start(i);
        }
        shard
    }

    fn peers(&self) -> Vec<String> {
        self.addrs
            .iter()
            .map(|addr| format!("grpc://{addr}"))
            .collect()
    }

    /// Every replica, as a client knows them.
    fn url(&self) -> String {
        let hosts: Vec<String> = self.addrs.iter().map(SocketAddr::to_string).collect();
        format!("grpc://{}", hosts.join(","))
    }

    fn start(&mut self, i: usize) {
        let stop = Arc::new(AtomicBool::new(false));
        let root = self.dir.join(format!("r{}", i + 1));
        let (catalog, branch, peers, addr) = (
            self.catalog.clone(),
            self.branch.clone(),
            self.peers(),
            self.addrs[i],
        );
        let stopping = stop.clone();
        // Segments of 4 KiB, none kept past what the files hold: a replica down while the
        // leader flushes comes back too far behind, and copies the leader's files.
        let replica = Replica {
            id: i as u64 + 1,
            voters: (1..=REPLICAS as u64).collect(),
            seed: None,
            limits: LogLimits {
                segment_bytes: 4 << 10,
                retain_bytes: 0,
            },
        };
        let running = thread::spawn(move || {
            serve_replica(&root, &catalog, &branch, replica, &peers, addr, &stopping)
        });
        self.running[i] = Some((stop, running));
    }

    fn stop(&mut self, i: usize) {
        if let Some((stop, running)) = self.running[i].take() {
            stop.store(true, Ordering::Release);
            running.join().unwrap().unwrap();
        }
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        for i in 0..REPLICAS {
            self.stop(i);
        }
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn ticks() -> TableName {
    TableName::parse("md.ticks").unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![Field::new("n", DataType::Int64, true)])
}

fn batch(values: std::ops::Range<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema()),
        vec![Arc::new(Int64Array::from_iter_values(values))],
    )
    .unwrap()
}

/// `done`, retried until it holds or `seconds` pass.
fn eventually<T>(seconds: u64, mut done: impl FnMut() -> Result<T, Error>) -> T {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        match done() {
            Ok(value) => return value,
            Err(e) if Instant::now() < deadline => {
                eprintln!("not yet: {e}");
                thread::sleep(Duration::from_millis(200));
            }
            Err(e) => panic!("never: {e}"),
        }
    }
}

/// Whether the replica at `addr` says it is ready (gRPC health), as Kubernetes asks it.
fn ready(addr: SocketAddr) -> bool {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let Ok(channel) = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
        else {
            return false;
        };
        HealthClient::new(channel)
            .check(HealthCheckRequest::default())
            .await
            .is_ok_and(|answer| answer.into_inner().status == ServingStatus::Serving as i32)
    })
}

/// Two disks lost in turn, while a writer writes: each replica comes back on an empty disk as
/// a new member, catches up from the others' files, and is made a voter, which its readiness
/// says. After the second, only the first replacement voting keeps a majority, so writes going
/// on and every replica holding them all prove it was.
#[test]
fn replicas_whose_disks_were_lost_rejoin_as_new_members() {
    let Some(catalog) = postgres() else {
        return;
    };
    let mut shard = Shard::new(catalog);
    let client = eventually(10, || Client::connect(&shard.url()));
    eventually(10, || client.create_table(&ticks(), &schema()));
    let source = SourceId::parse("feed").unwrap();
    let writer = eventually(10, || client.writer(&ticks(), &source, true));
    let mut written = 0;
    let mut write_some = |writer: &kermodes_flight::RemoteWriter| {
        for _ in 0..20 {
            assert!(writer.write(&batch(written..written + 10), None).unwrap());
            written += 10;
        }
    };
    write_some(&writer);
    for lost in [0, 2] {
        shard.stop(lost);
        fs::remove_dir_all(shard.dir.join(format!("r{}", lost + 1))).unwrap();
        write_some(&writer);
        shard.start(lost);
        let addr = shard.addrs[lost];
        eventually(30, || {
            if ready(addr) {
                Ok(())
            } else {
                Err(Error::Io(format!("replica {} is not ready yet", lost + 1)))
            }
        });
        write_some(&writer);
    }
    let total = written as u64;
    for (i, addr) in shard.addrs.iter().enumerate() {
        let replica = eventually(10, || Client::connect(&format!("grpc://{addr}")));
        let values = eventually(20, || {
            let (held, rows) = replica.rows_at(&ticks(), 0, total, None)?;
            if held != total {
                return Err(Error::Io(format!(
                    "replica {} holds {held} of {total}",
                    i + 1
                )));
            }
            Ok(rows
                .iter()
                .flat_map(|rows| rows.column(0).as_primitive::<Int64Type>().values().to_vec())
                .collect::<Vec<i64>>())
        });
        assert_eq!(
            values,
            (0..written).collect::<Vec<_>>(),
            "replica {}",
            i + 1
        );
    }
}

#[test]
fn a_shard_of_three_keeps_every_acknowledged_write_through_each_replica_stopping() {
    let Some(catalog) = postgres() else {
        return;
    };
    let mut shard = Shard::new(catalog);
    let client = eventually(10, || Client::connect(&shard.url()));
    eventually(10, || client.create_table(&ticks(), &schema()));
    let source = SourceId::parse("feed").unwrap();
    let writer = eventually(10, || client.writer(&ticks(), &source, true));
    let mut written = 0;
    let mut write_some = |writer: &kermodes_flight::RemoteWriter| {
        for _ in 0..20 {
            let sent = writer.write(&batch(written..written + 10), None).unwrap();
            assert!(sent, "a new write is written");
            written += 10;
        }
    };
    // Each replica in turn goes, while writes go on, and comes back once the others flushed
    // and trimmed their logs past it: it catches up by copying the leader's files. One of them
    // leads when it goes.
    for i in 0..REPLICAS {
        write_some(&writer);
        shard.stop(i);
        write_some(&writer);
        thread::sleep(Duration::from_millis(1500));
        write_some(&writer);
        shard.start(i);
    }
    write_some(&writer);
    let total = written as u64;
    // Every replica holds every write, once, in order.
    for (i, addr) in shard.addrs.iter().enumerate() {
        let replica = eventually(10, || Client::connect(&format!("grpc://{addr}")));
        let values = eventually(20, || {
            let (held, rows) = replica.rows_at(&ticks(), 0, total, None)?;
            if held != total {
                return Err(Error::Io(format!(
                    "replica {} holds {held} of {total}",
                    i + 1
                )));
            }
            Ok(rows
                .iter()
                .flat_map(|rows| rows.column(0).as_primitive::<Int64Type>().values().to_vec())
                .collect::<Vec<i64>>())
        });
        assert_eq!(
            values,
            (0..written).collect::<Vec<_>>(),
            "replica {}",
            i + 1
        );
    }
}
