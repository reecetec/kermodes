//! The platform's own components (architecture.md §15.2): weir, Postgres, the controller, and the
//! web UI, as Kubernetes shows their pods. The Deployments tab reads them from the controller's
//! API, not from a `sys.*` table, so it can still say weir is down when weir is down.

use serde::Serialize;

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

/// One word for how a component is doing: every pod ready, some, or none and why.
pub fn status(instances: &[Instance]) -> String {
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
    use super::{Disk, Instance, image_tag, postgres_disks, recovered, status, weir_disk};

    #[test]
    fn a_component_is_running_degraded_or_says_why_it_is_not() {
        let instance = |ready: bool, waiting: Option<&str>| Instance {
            ready,
            waiting: waiting.map(str::to_owned),
            ..Instance::default()
        };
        let cases: &[(&[Instance], &str)] = &[
            (&[instance(true, None), instance(true, None)], "Running"),
            (&[instance(true, None), instance(false, None)], "Degraded"),
            (
                &[
                    instance(false, None),
                    instance(false, Some("CrashLoopBackOff")),
                ],
                "CrashLoopBackOff",
            ),
            (&[instance(false, None)], "Starting"),
        ];
        for (instances, expected) in cases {
            assert_eq!(status(instances), *expected);
        }
    }

    #[test]
    fn an_images_tag_is_its_version() {
        let cases = [
            (
                "git.rcolclough.com/rcolclough/kermodes:0.4.131",
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
