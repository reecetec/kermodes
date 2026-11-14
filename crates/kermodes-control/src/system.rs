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
    use super::{Instance, image_tag, recovered, status};

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
}
