//! `sys.deployments` (architecture.md §16.2): every few seconds the leader appends one row per
//! deployment with what it runs and how it is doing, so the web UI's Deployments tab, notebooks,
//! and alerting programs read one live table, its latest row per deployment by `last_by`.

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use kermodes_flight::{Client, RemoteWriter};
use kermodes_weir::{SourceId, TableName};

use crate::k8s::Rollout;
use crate::manifest::Resource;

/// What Kubernetes shows of one pod of a deployment.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Pod {
    pub node: Option<String>,
    pub ready: bool,
    pub started_ns: Option<i64>,
    pub restarts: i64,
    /// Why its container is waiting to run (`CrashLoopBackOff`, `ImagePullBackOff`, ...).
    pub waiting: Option<String>,
    /// The scheduler found no node for it.
    pub unschedulable: bool,
}

/// What Kubernetes shows of one deployment.
#[derive(Clone, Debug, PartialEq)]
pub struct Observed {
    pub rollout: Rollout,
    /// Its pods, leaving out those terminating.
    pub pods: Vec<Pod>,
    /// Cores and bytes in use across its pods, as metrics-server last measured them.
    pub cpu: f64,
    pub memory: i64,
    /// Its newest Warning event: reason and message, and when.
    pub warning: Option<(String, i64)>,
}

/// One word for how a deployment is doing, most pressing first: a run that cannot start says
/// why, ahead of the rollout's own state.
pub fn status(rollout: Rollout, pods: &[Pod]) -> &'static str {
    let waiting = |reasons: &[&str]| {
        pods.iter()
            .any(|p| p.waiting.as_deref().is_some_and(|w| reasons.contains(&w)))
    };
    if waiting(&["CrashLoopBackOff"]) {
        "Crash looping"
    } else if waiting(&["ImagePullBackOff", "ErrImagePull", "InvalidImageName"]) {
        "Image pull failing"
    } else if pods.iter().any(|p| p.unschedulable) {
        "Unschedulable"
    } else {
        match rollout {
            Rollout::Absent => "Not running",
            Rollout::Failed => "Rollout failed",
            Rollout::Ready => "Running",
            Rollout::Rolling if pods.iter().any(|p| p.ready) => "Rolling out",
            Rollout::Rolling => "Starting",
        }
    }
}

/// Cores in a Kubernetes CPU quantity: `2`, `0.5`, `250m`, and the `u` and `n` metrics-server
/// reports.
pub fn cores(quantity: &str) -> Option<f64> {
    // Divided rather than multiplied by a fraction, so `5u` is exactly 0.000005.
    let (digits, per_core) = match quantity.as_bytes().last()? {
        b'm' => (&quantity[..quantity.len() - 1], 1e3),
        b'u' => (&quantity[..quantity.len() - 1], 1e6),
        b'n' => (&quantity[..quantity.len() - 1], 1e9),
        _ => (quantity, 1.0),
    };
    digits.parse::<f64>().ok().map(|n| n / per_core)
}

/// Bytes in a Kubernetes memory quantity: `512Mi`, `300M`, `1000000`.
pub fn bytes(quantity: &str) -> Option<i64> {
    const SUFFIXES: [(&str, i64); 8] = [
        ("Ki", 1 << 10),
        ("Mi", 1 << 20),
        ("Gi", 1 << 30),
        ("Ti", 1 << 40),
        ("k", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
    ];
    for (suffix, scale) in SUFFIXES {
        if let Some(digits) = quantity.strip_suffix(suffix) {
            return digits.parse::<i64>().ok()?.checked_mul(scale);
        }
    }
    quantity.parse().ok()
}

/// One deployment's row.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub project: String,
    pub name: String,
    pub kind: String,
    pub status: String,
    pub update: String,
    /// The commit it runs, and a newer deployable one it does not run yet.
    pub commit: Option<String>,
    pub available: Option<String>,
    pub node: Option<String>,
    pub started_ns: Option<i64>,
    pub restarts: i64,
    pub cpu: f64,
    pub cpu_limit: f64,
    pub memory: i64,
    pub memory_limit: i64,
    pub event: Option<String>,
    pub event_ns: Option<i64>,
}

impl Row {
    /// `resource` of `project`, running `commit` while `latest` is its newest deployable commit,
    /// as Kubernetes shows it (`None`: nothing runs it).
    pub fn new(
        project: &str,
        resource: &Resource,
        commit: Option<&str>,
        latest: &str,
        observed: Option<&Observed>,
    ) -> Self {
        let pods = observed.map_or(&[][..], |o| &o.pods);
        // The run serving: a ready pod, else the newest there is.
        let serving = pods.iter().find(|p| p.ready).or_else(|| pods.last());
        Self {
            project: project.into(),
            name: resource.name.clone(),
            kind: resource.kind.clone(),
            status: status(observed.map_or(Rollout::Absent, |o| o.rollout), pods).into(),
            update: format!("{:?}", resource.update).to_lowercase(),
            commit: commit.map(str::to_owned),
            available: (commit != Some(latest)).then(|| latest.to_owned()),
            node: serving.and_then(|p| p.node.clone()),
            started_ns: serving.and_then(|p| p.started_ns),
            restarts: pods.iter().map(|p| p.restarts).sum(),
            cpu: observed.map_or(0.0, |o| o.cpu),
            cpu_limit: cores(&resource.cpu).unwrap_or(0.0),
            memory: observed.map_or(0, |o| o.memory),
            memory_limit: bytes(&resource.memory).unwrap_or(0),
            event: observed.and_then(|o| o.warning.as_ref().map(|(text, _)| text.clone())),
            event_ns: observed.and_then(|o| o.warning.as_ref().map(|(_, at)| *at)),
        }
    }
}

pub static TABLE: LazyLock<TableName> =
    LazyLock::new(|| TableName::parse("sys.deployments").expect("a valid table name"));

/// The table's columns. `cpu` and `memory` are what its pods use, `*_limit` what it declared
/// (requests and limits alike), `*_pct` the one as a share of the other.
pub fn schema() -> SchemaRef {
    let time = || DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("ts", time(), false),
        Field::new("project", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("update", DataType::Utf8, false),
        Field::new("commit", DataType::Utf8, true),
        Field::new("available", DataType::Utf8, true),
        Field::new("node", DataType::Utf8, true),
        Field::new("started", time(), true),
        Field::new("restarts", DataType::Int64, false),
        Field::new("cpu", DataType::Float64, false),
        Field::new("cpu_limit", DataType::Float64, false),
        Field::new("cpu_pct", DataType::Float64, false),
        Field::new("memory", DataType::Int64, false),
        Field::new("memory_limit", DataType::Int64, false),
        Field::new("memory_pct", DataType::Float64, false),
        Field::new("event", DataType::Utf8, true),
        Field::new("event_at", time(), true),
    ]))
}

/// `rows` as of `ts_ns`.
pub fn batch(ts_ns: i64, rows: &[Row]) -> RecordBatch {
    let text = |f: fn(&Row) -> Option<&str>| -> ArrayRef {
        Arc::new(rows.iter().map(f).collect::<StringArray>())
    };
    let time = |f: fn(&Row) -> Option<i64>| -> ArrayRef {
        Arc::new(
            rows.iter()
                .map(f)
                .collect::<TimestampNanosecondArray>()
                .with_timezone("UTC"),
        )
    };
    let share = |used: f64, limit: f64| {
        if limit > 0.0 {
            100.0 * used / limit
        } else {
            0.0
        }
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(vec![ts_ns; rows.len()]).with_timezone("UTC")),
        text(|r| Some(&r.project)),
        text(|r| Some(&r.name)),
        text(|r| Some(&r.kind)),
        text(|r| Some(&r.status)),
        text(|r| Some(&r.update)),
        text(|r| r.commit.as_deref()),
        text(|r| r.available.as_deref()),
        text(|r| r.node.as_deref()),
        time(|r| r.started_ns),
        Arc::new(rows.iter().map(|r| r.restarts).collect::<Int64Array>()),
        Arc::new(rows.iter().map(|r| r.cpu).collect::<Float64Array>()),
        Arc::new(rows.iter().map(|r| r.cpu_limit).collect::<Float64Array>()),
        Arc::new(
            rows.iter()
                .map(|r| share(r.cpu, r.cpu_limit))
                .collect::<Float64Array>(),
        ),
        Arc::new(rows.iter().map(|r| r.memory).collect::<Int64Array>()),
        Arc::new(rows.iter().map(|r| r.memory_limit).collect::<Int64Array>()),
        Arc::new(
            rows.iter()
                .map(|r| share(r.memory as f64, r.memory_limit as f64))
                .collect::<Float64Array>(),
        ),
        text(|r| r.event.as_deref()),
        time(|r| r.event_ns),
    ];
    RecordBatch::try_new(schema(), columns).expect("columns built to the schema")
}

/// Writes the batches sent to it to `sys.deployments` on weir at `weir`, on a thread of its own
/// (the Flight client blocks). It holds the table's `control` source while the sender lives,
/// so a new leader's writer waits for the old one's to go, and reconnects if weir restarts.
/// A batch that finds the queue full is dropped: the next one, seconds later, says the same.
pub fn writer(weir: String) -> SyncSender<RecordBatch> {
    let (sender, batches) = sync_channel(4);
    std::thread::Builder::new()
        .name("kermodes-control-report".into())
        .spawn(move || write_all(&weir, &batches))
        .expect("a thread for the report writer");
    sender
}

fn write_all(weir: &str, batches: &Receiver<RecordBatch>) {
    let source = SourceId::parse("control").expect("a valid source name");
    loop {
        let writer = match open(weir, &source) {
            Ok(writer) => writer,
            Err(e) => {
                eprintln!("kermodes control: cannot write {}: {e}", *TABLE);
                // Wait out weir, giving up once the leader no longer reports.
                match batches.recv_timeout(Duration::from_secs(5)) {
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                    _ => continue,
                }
            }
        };
        loop {
            let Ok(batch) = batches.recv() else { return };
            if let Err(e) = writer.write(&batch, None) {
                eprintln!("kermodes control: writing {} failed: {e}", *TABLE);
                break;
            }
        }
    }
}

fn open(weir: &str, source: &SourceId) -> Result<RemoteWriter, kermodes_weir::Error> {
    let client = Client::connect(weir)?;
    client.create_table(&TABLE, &schema())?;
    client.writer(&TABLE, source, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Rollout as Replacement, Update};

    fn pod(ready: bool, waiting: Option<&str>) -> Pod {
        Pod {
            ready,
            waiting: waiting.map(str::to_owned),
            ..Pod::default()
        }
    }

    #[test]
    fn status_says_what_is_most_pressing() {
        use Rollout::*;
        let ready = pod(true, None);
        let starting = pod(false, None);
        let crashing = pod(false, Some("CrashLoopBackOff"));
        let no_image = pod(false, Some("ImagePullBackOff"));
        let unplaced = Pod {
            unschedulable: true,
            ..Pod::default()
        };
        let cases = [
            (Ready, vec![ready.clone()], "Running"),
            (Absent, vec![], "Not running"),
            (Rolling, vec![starting.clone()], "Starting"),
            (
                Rolling,
                vec![ready.clone(), starting.clone()],
                "Rolling out",
            ),
            (Failed, vec![ready.clone(), starting], "Rollout failed"),
            // A new run that cannot start says why, even while the old one serves.
            (
                Rolling,
                vec![ready.clone(), crashing.clone()],
                "Crash looping",
            ),
            (Rolling, vec![ready.clone(), no_image], "Image pull failing"),
            (Rolling, vec![ready, unplaced], "Unschedulable"),
            (Failed, vec![crashing], "Crash looping"),
        ];
        for (rollout, pods, want) in cases {
            assert_eq!(status(rollout, &pods), want, "{rollout:?} {pods:?}");
        }
    }

    #[test]
    fn quantities_read_as_kubernetes_writes_them() {
        assert_eq!(cores("2"), Some(2.0));
        assert_eq!(cores("0.5"), Some(0.5));
        assert_eq!(cores("250m"), Some(0.25));
        assert!((cores("33912045n").unwrap() - 0.033_912_045).abs() < 1e-12);
        assert_eq!(cores("5u"), Some(0.000_005));
        assert_eq!(cores("fast"), None);
        assert_eq!(bytes("512Mi"), Some(512 << 20));
        assert_eq!(bytes("40112Ki"), Some(40112 << 10));
        assert_eq!(bytes("300M"), Some(300_000_000));
        assert_eq!(bytes("1000"), Some(1000));
        assert_eq!(bytes("2GB"), None);
    }

    #[test]
    fn a_row_says_what_runs_where_and_how_much_of_its_size_it_uses() {
        let resource = Resource {
            name: "ticks".into(),
            kind: "deployment".into(),
            update: Update::Auto,
            rollout: Replacement::Rolling,
            cpu: "250m".into(),
            memory: "256Mi".into(),
        };
        let observed = Observed {
            rollout: Rollout::Rolling,
            pods: vec![
                Pod {
                    node: Some("n1".into()),
                    ready: true,
                    started_ns: Some(7),
                    restarts: 2,
                    ..Pod::default()
                },
                Pod {
                    node: Some("n2".into()),
                    restarts: 1,
                    ..Pod::default()
                },
            ],
            cpu: 0.05,
            memory: 64 << 20,
            warning: Some(("BackOff: restarting".into(), 9)),
        };
        let row = Row::new("ticker", &resource, Some("a"), "b", Some(&observed));
        assert_eq!(
            (
                row.status.as_str(),
                row.available.as_deref(),
                row.node.as_deref()
            ),
            ("Rolling out", Some("b"), Some("n1")),
            "the ready run is the one serving"
        );
        assert_eq!(
            (row.restarts, row.started_ns, row.event_ns),
            (3, Some(7), Some(9))
        );
        let batch = batch(1, &[row]);
        let pct = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        };
        assert!((pct("cpu_pct") - 20.0).abs() < 1e-9);
        assert!((pct("memory_pct") - 25.0).abs() < 1e-9);

        // Nothing runs it, and it is current: no update, no usage.
        let idle = Row::new("ticker", &resource, Some("b"), "b", None);
        assert_eq!(
            (
                idle.status.as_str(),
                idle.available,
                idle.cpu,
                idle.restarts
            ),
            ("Not running", None, 0.0, 0)
        );
    }
}
