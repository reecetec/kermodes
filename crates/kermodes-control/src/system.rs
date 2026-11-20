//! The platform's own components (architecture.md §15.2): weir, Postgres, the controller, and the
//! web UI, as Kubernetes shows their pods. The Deployments tab reads them from the controller's
//! API, not from a `sys.*` table, so it can still say weir is down when weir is down.

use serde::Serialize;

use kermodes_weir::Date;

use crate::k8s::Rollout;
use crate::plan::{JobOutcome, MergeJob};

/// The components' `app.kubernetes.io/component` labels, in the order they are shown.
pub const COMPONENTS: [&str; 4] = ["weir", "database", "control", "ui"];

/// One pod of a component.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Instance {
    pub pod: String,
    /// Postgres's `primary` or `replica`.
    pub role: Option<String>,
    /// The image tag it runs.
    pub version: Option<String>,
    pub ready: bool,
    pub node: Option<String>,
    pub started_ns: Option<i64>,
    pub restarts: i64,
    /// Why a container of it is waiting to run (`CrashLoopBackOff`, ...).
    pub waiting: Option<String>,
    /// Cores and bytes in use (metrics-server), and what its containers are sized at (limits,
    /// else requests); `None` when they declare no size.
    pub cpu: f64,
    pub cpu_limit: Option<f64>,
    pub memory: i64,
    pub memory_limit: Option<i64>,
    /// Its volumes: weir's store; Postgres's data and WAL.
    pub disks: Vec<Disk>,
}

/// A volume of a pod: what it holds, the bytes in use, and its size when known.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Disk {
    /// `store` (weir's), `data` (Postgres's databases), or `wal` (Postgres's write-ahead log).
    pub name: &'static str,
    pub used: u64,
    pub size: Option<u64>,
}

/// A Postgres instance's disks from its CNPG metrics (Prometheus text): its databases, together,
/// on its data volume of `data_size` bytes, and its write-ahead log on its WAL volume. A disk
/// whose metric is missing is left out rather than shown empty.
pub fn postgres_disks(metrics: &str, data_size: Option<u64>) -> Vec<Disk> {
    let (mut databases, mut wal, mut wal_size) = (None, None, None);
    for line in metrics.lines().filter(|line| !line.starts_with('#')) {
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Some(value) = value.parse::<f64>().ok().filter(|v| v.is_finite()) else {
            continue;
        };
        if series.starts_with("cnpg_pg_database_size_bytes{") {
            *databases.get_or_insert(0.0) += value;
        } else if series == r#"cnpg_collector_pg_wal{value="size"}"# {
            wal = Some(value);
        } else if series == r#"cnpg_collector_pg_wal{value="volume_size"}"# {
            wal_size = Some(value as u64);
        }
    }
    let data = databases.map(|used| Disk {
        name: "data",
        used: used as u64,
        size: data_size,
    });
    let wal = wal.map(|used| Disk {
        name: "wal",
        used: used as u64,
        size: wal_size,
    });
    data.into_iter().chain(wal).collect()
}

/// weir's store volume, from its `stats` action's `volume`.
pub fn weir_disk(stats: &serde_json::Value) -> Option<Disk> {
    let size = stats["volume"]["bytes"].as_u64()?;
    let free = stats["volume"]["free"].as_u64()?;
    Some(Disk {
        name: "store",
        used: size.saturating_sub(free),
        size: Some(size),
    })
}

/// A component: its pods, how it is doing, and its newest Warning event.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Component {
    pub name: String,
    pub status: String,
    pub instances: Vec<Instance>,
    pub warning: Option<String>,
    pub warning_ns: Option<i64>,
}

/// Waiting reasons that mean a pod cannot run, not merely that it has not yet.
const FAILING: [&str; 6] = [
    "CrashLoopBackOff",
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CreateContainerError",
];

/// One word for how a component is doing, most pressing first: a pod that cannot run says why;
/// then its workload's rollout (`None` for Postgres, which the operator runs pod by pod), so an
/// upgrade reads as one rather than as a degraded component; then how many pods are ready.
pub fn status(instances: &[Instance], rollout: Option<Rollout>) -> String {
    if let Some(reason) = instances
        .iter()
        .filter_map(|i| i.waiting.as_deref())
        .find(|w| FAILING.contains(w))
    {
        return reason.into();
    }
    // Pods of two versions mean old ones are still going, even once the workload says it has
    // rolled out: until they are gone the component is still changing, not degraded.
    let mut versions = instances.iter().filter_map(|i| i.version.as_deref());
    let mixed = versions
        .next()
        .is_some_and(|first| versions.any(|v| v != first));
    match rollout {
        Some(Rollout::Failed) => return "Rollout failed".into(),
        Some(Rollout::Rolling) => return "Rolling out".into(),
        _ if mixed => return "Rolling out".into(),
        _ => {}
    }
    let ready = instances.iter().filter(|i| i.ready).count();
    if ready == instances.len() {
        "Running".into()
    } else if ready > 0 {
        "Degraded".into()
    } else {
        instances
            .iter()
            .find_map(|i| i.waiting.clone())
            .unwrap_or_else(|| "Starting".into())
    }
}

/// The lake as the Deployments tab shows it, from what the controller can see of it: whether
/// its store answered (`probe`), the merge Jobs that exist (a finished one lasts a day), and the
/// merges that have been due for an hour with no Job (`overdue`). Most pressing first; the
/// second value says which table and date, or why.
pub fn lake_status(
    probe: &Result<(), String>,
    jobs: &[MergeJob],
    overdue: &[(String, Date)],
) -> (String, Option<String>) {
    if let Err(why) = probe {
        return ("Unreachable".into(), Some(why.clone()));
    }
    if let Some(job) = jobs.iter().find(|j| j.outcome == JobOutcome::Failed) {
        return (
            "Merge failed".into(),
            Some(format!(
                "merging {} {} failed; it is tried again tomorrow",
                job.table, job.date
            )),
        );
    }
    if let Some((table, date)) = overdue.first() {
        return (
            "Merge overdue".into(),
            Some(format!(
                "{table} {date} has waited over an hour for its merge"
            )),
        );
    }
    if jobs.iter().any(|j| j.outcome == JobOutcome::Running) {
        return ("Merging".into(), None);
    }
    // Fine reads the same word as every other part that is fine
    ("Running".into(), None)
}

/// An image reference's tag (`registry/name:0.4.131` → `0.4.131`), without a digest; `None`
/// for an untagged reference.
pub fn image_tag(image: &str) -> Option<String> {
    let image = image.split('@').next()?;
    let (_, tag) = image.rsplit_once(':')?;
    // A colon before the last slash belongs to a registry port, not a tag.
    (!tag.contains('/') && !tag.is_empty()).then(|| tag.to_owned())
}

/// Whether a Warning event is a failed probe (`Unhealthy`) its pod has recovered from: the pod
/// is ready, and became so after the failure, as every pod does once it has started. A card
/// shows what is wrong now; the component's events keep every one.
pub fn recovered(reason: &str, at_ns: i64, ready_since_ns: Option<i64>) -> bool {
    reason == "Unhealthy" && ready_since_ns.is_some_and(|since| since >= at_ns)
}

#[cfg(test)]
mod tests {
    use super::{
        Disk, Instance, image_tag, lake_status, postgres_disks, recovered, status, weir_disk,
    };
    use crate::k8s::Rollout;
    use crate::plan::{JobOutcome, MergeJob};
    use kermodes_weir::Date;

    #[test]
    fn the_lake_says_what_is_wrong_most_pressing_first() {
        let day = Date::parse("2026-11-14").unwrap();
        let job = |outcome: JobOutcome| MergeJob {
            table: "md.ticks".into(),
            date: day,
            outcome,
        };
        let overdue = vec![("md.ticks".to_owned(), day)];
        let down: Result<(), String> = Err("connection refused".into());
        type Case<'a> = (
            &'a Result<(), String>,
            Vec<MergeJob>,
            &'a [(String, Date)],
            &'a str,
        );
        let cases: Vec<Case> = vec![
            (&Ok(()), vec![], &[], "Running"),
            (&Ok(()), vec![job(JobOutcome::Succeeded)], &[], "Running"),
            (&Ok(()), vec![job(JobOutcome::Running)], &[], "Merging"),
            (
                &Ok(()),
                vec![job(JobOutcome::Running)],
                &overdue,
                "Merge overdue",
            ),
            (
                &Ok(()),
                vec![job(JobOutcome::Running), job(JobOutcome::Failed)],
                &overdue,
                "Merge failed",
            ),
            (
                &down,
                vec![job(JobOutcome::Failed)],
                &overdue,
                "Unreachable",
            ),
        ];
        for (probe, jobs, overdue, expected) in cases {
            assert_eq!(
                lake_status(probe, &jobs, overdue).0,
                expected,
                "{jobs:?} {overdue:?}"
            );
        }
        assert_eq!(
            lake_status(&down, &[], &[]).1.as_deref(),
            Some("connection refused")
        );
    }

    #[test]
    fn a_component_says_what_is_wrong_then_whether_it_is_rolling_out_then_how_ready() {
        let instance = |ready: bool, waiting: Option<&str>| Instance {
            ready,
            waiting: waiting.map(str::to_owned),
            ..Instance::default()
        };
        let (up, down) = (instance(true, None), instance(false, None));
        let creating = instance(false, Some("ContainerCreating"));
        let crashing = instance(false, Some("CrashLoopBackOff"));
        let at = |version: &str, ready: bool| Instance {
            version: Some(version.into()),
            ready,
            ..Instance::default()
        };
        let cases: Vec<(Vec<Instance>, Option<Rollout>, &str)> = vec![
            (vec![up.clone(), up.clone()], None, "Running"),
            (
                vec![up.clone(), up.clone()],
                Some(Rollout::Ready),
                "Running",
            ),
            (vec![up.clone(), down.clone()], None, "Degraded"),
            (
                vec![up.clone(), down.clone()],
                Some(Rollout::Ready),
                "Degraded",
            ),
            // The old pod serves while the new one starts: an upgrade, not a degraded component
            (
                vec![up.clone(), creating.clone()],
                Some(Rollout::Rolling),
                "Rolling out",
            ),
            (
                vec![creating.clone()],
                Some(Rollout::Rolling),
                "Rolling out",
            ),
            (vec![up.clone()], Some(Rollout::Failed), "Rollout failed"),
            // A pod that cannot run says why, rollout or not
            (
                vec![up, crashing.clone()],
                Some(Rollout::Rolling),
                "CrashLoopBackOff",
            ),
            (vec![down.clone(), crashing], None, "CrashLoopBackOff"),
            (vec![creating], None, "ContainerCreating"),
            (vec![down], None, "Starting"),
            // The Deployment is done but the old pod has not gone yet
            (
                vec![at("0.4.144", true), at("0.4.145", true)],
                Some(Rollout::Ready),
                "Rolling out",
            ),
            (
                vec![at("0.4.145", true), at("0.4.145", false)],
                Some(Rollout::Ready),
                "Degraded",
            ),
        ];
        for (instances, rollout, expected) in &cases {
            assert_eq!(
                status(instances, *rollout),
                *expected,
                "{instances:?} {rollout:?}"
            );
        }
    }

    #[test]
    fn an_images_tag_is_its_version() {
        let cases = [
            (
                "registry.example.com/acme/kermodes:0.4.131",
                Some("0.4.131"),
            ),
            (
                "ghcr.io/cloudnative-pg/postgresql:17.2@sha256:abc",
                Some("17.2"),
            ),
            ("localhost:5000/kermodes", None),
            ("kermodes", None),
        ];
        for (image, tag) in cases {
            assert_eq!(image_tag(image).as_deref(), tag, "{image}");
        }
    }

    #[test]
    fn a_probe_failure_the_pod_has_recovered_from_is_not_a_warning() {
        // Failed while starting, ready since.
        assert!(recovered("Unhealthy", 10, Some(12)));
        // Failing now, or after it became ready, or not ready at all.
        assert!(!recovered("Unhealthy", 15, Some(12)));
        assert!(!recovered("Unhealthy", 10, None));
        // Not a probe failure.
        assert!(!recovered("BackOff", 10, Some(12)));
    }

    /// As a CNPG instance reports them (cnpg 1.25), with the lines around them it ignores.
    const METRICS: &str = r#"# HELP cnpg_collector_pg_wal Total size in bytes of WAL files
# TYPE cnpg_collector_pg_wal gauge
cnpg_collector_pg_wal{value="count"} 36
cnpg_collector_pg_wal{value="size"} 6.03979776e+08
cnpg_collector_pg_wal{value="slots_max"} NaN
cnpg_collector_pg_wal{value="volume_size"} 2.147483648e+09
cnpg_pg_database_size_bytes{datname="kermodes"} 8.615615e+06
cnpg_pg_database_size_bytes{datname="postgres"} 8.017599e+06
cnpg_pg_settings_setting{name="max_wal_size"} 1024
"#;

    #[test]
    fn a_postgres_instance_has_its_databases_and_its_wal_on_their_volumes() {
        let disk = |name, used, size| Disk { name, used, size };
        assert_eq!(
            postgres_disks(METRICS, Some(5 << 30)),
            [
                disk("data", 16_633_214, Some(5 << 30)),
                disk("wal", 603_979_776, Some(2 << 30)),
            ]
        );
        // WAL on the data volume: no WAL volume of its own to size.
        let shared = METRICS.replace("volume_size", "volume_unknown");
        assert_eq!(
            postgres_disks(&shared, None)[1],
            disk("wal", 603_979_776, None)
        );
        // No metrics, no disks: unknown is not empty.
        assert_eq!(postgres_disks("# nothing yet\n", Some(1)), []);
    }

    #[test]
    fn weirs_store_is_its_volume_less_what_is_free() {
        let stats = serde_json::json!({"tables": [], "volume": {"bytes": 1000, "free": 250}});
        assert_eq!(
            weir_disk(&stats),
            Some(Disk {
                name: "store",
                used: 750,
                size: Some(1000)
            })
        );
        assert_eq!(weir_disk(&serde_json::json!({"tables": []})), None);
    }
}
