//! What the controller applies, as pure functions of a commit's manifest: whether a resource rolls
//! to a synced commit (its update policy), and the Kubernetes objects that run it (§16.2–16.3).
//!
//! Each resource is a Deployment of one replica running `kermodes run <resource>` from the
//! commit's code snapshot, which an init container fetches from the controller (pods never hold
//! git credentials, §11.7). Its pod template names the commit, so only a new commit changes it,
//! and Kubernetes rolls it as the resource asks: start the new run, wait until it is ready, stop
//! the old (§11.4). A PodDisruptionBudget keeps a drain from evicting a healthy run; the
//! controller moves it instead, by the same rollout (§17.1).

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use serde_json::{Value, json};

use crate::manifest::{Resource, Rollout, Update};

/// Where a project's commit runs.
#[derive(Clone, Debug)]
pub struct Target {
    pub project: String,
    /// The project's directory within its repository; empty for the root.
    pub path: String,
    pub commit: String,
    /// The image runs use: the platform's, until environment images exist (§10.6).
    pub image: String,
    /// weir's Flight URL, where runs keep their tables.
    pub weir: String,
    /// The controller's snapshot endpoint: `{snapshots}/{project}/{commit}` is the code as a tar.
    pub snapshots: String,
    pub pull_secrets: Vec<String>,
}

/// What to do with a resource when its project's mainline is at `synced`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Roll it to the synced commit.
    Apply,
    /// It runs the synced commit already.
    Current,
    /// An update is available, waiting for someone to roll it out (`update="manual"`).
    Hold,
}

/// A resource that never ran is applied whatever its policy; one that runs an older commit rolls
/// at once (`auto`) or waits to be rolled out (`manual`).
pub fn decide(update: Update, deployed: Option<&str>, synced: &str) -> Decision {
    match deployed {
        None => Decision::Apply,
        Some(commit) if commit == synced => Decision::Current,
        Some(_) => match update {
            Update::Auto => Decision::Apply,
            Update::Manual | Update::OnStart => Decision::Hold,
        },
    }
}

/// The Kubernetes name of a resource's objects: `{project}-{resource}`, a DNS label.
pub fn object_name(project: &str, resource: &str) -> Result<String, String> {
    let name = format!("{project}-{resource}").replace('_', "-");
    if name.len() > 63 {
        return Err(format!(
            "`{project}`'s resource `{resource}` makes the name {name}, longer than Kubernetes' 63 \
             characters; shorten one of them"
        ));
    }
    Ok(name)
}

/// The names of every resource's objects; two resources may not share one (`a_b`, `a-b`).
pub fn object_names(
    project: &str,
    resources: &[Resource],
) -> Result<BTreeMap<String, String>, String> {
    let mut names = BTreeMap::new();
    let mut taken = BTreeMap::new();
    for resource in resources {
        let name = object_name(project, &resource.name)?;
        if let Some(other) = taken.insert(name.clone(), resource.name.clone()) {
            return Err(format!(
                "`{other}` and `{}` both become {name} in Kubernetes; rename one",
                resource.name
            ));
        }
        names.insert(resource.name.clone(), name);
    }
    Ok(names)
}

/// The labels that select a resource's pods; they never change, so neither does a Deployment's
/// selector.
fn selector(project: &str, resource: &str) -> Value {
    json!({"kermodes.io/project": project, "kermodes.io/resource": resource})
}

/// The Deployment that runs `resource` at the target's commit, with one run, or none while it is
/// stopped: a stopped Deployment's template still follows commits, so starting it runs the
/// right one without another rollout.
pub fn deployment(target: &Target, resource: &Resource, name: &str, stopped: bool) -> Deployment {
    let selector = selector(&target.project, &resource.name);
    let mut labels = selector.clone();
    labels["app.kubernetes.io/name"] = json!(name);
    labels["app.kubernetes.io/managed-by"] = json!("kermodes-control");
    let mut pod_labels = labels.clone();
    pod_labels["kermodes.io/commit"] = json!(target.commit);
    let strategy = match resource.rollout {
        Rollout::Rolling => json!({
            "type": "RollingUpdate",
            "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0},
        }),
        Rollout::Recreate => json!({"type": "Recreate"}),
    };
    let run = json!({
        "name": "run",
        "image": target.image,
        "args": [
            "run", resource.name,
            "--project", project_dir(target),
            "--server", target.weir,
            "--ready-file", "/run/kermodes/ready",
        ],
        "env": [{"name": "PYTHONDONTWRITEBYTECODE", "value": "1"}],
        // Requests equal to limits: the scheduler places the run where it fits, it never takes
        // more than it asked for, and its use reads as a share of one number.
        "resources": {
            "requests": {"cpu": resource.cpu, "memory": resource.memory},
            "limits": {"cpu": resource.cpu, "memory": resource.memory},
        },
        // Ready once it can take over from the run it replaces (§11.4): connected, holding its
        // sources' leases or waiting for them.
        "readinessProbe": {
            "exec": {"command": ["test", "-e", "/run/kermodes/ready"]},
            "periodSeconds": 2,
        },
        "volumeMounts": [
            {"name": "code", "mountPath": "/code", "readOnly": true},
            {"name": "run", "mountPath": "/run/kermodes"},
        ],
        "securityContext": container_security(),
    });
    let object = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            "replicas": i32::from(!stopped),
            // Rollbacks are rollouts of an earlier commit, never `kubectl rollout undo`. One old
            // ReplicaSet is still kept: deleted the moment a rollout ends, its pod is still
            // shutting down, and the disruption budget's controller warns about a pod without
            // a controller on every rollout.
            "revisionHistoryLimit": 1,
            "selector": {"matchLabels": selector},
            "strategy": strategy,
            // A new run that never becomes ready fails its rollout; the old one keeps serving.
            "progressDeadlineSeconds": 600,
            "template": {
                "metadata": {"labels": pod_labels},
                "spec": pod(target, vec![snapshot(target)], vec![run], "Always"),
            },
        },
    });
    serde_json::from_value(object).expect("a Deployment built from known fields")
}

/// Stops a drain from evicting a resource's healthy run (§17.1). The drain waits while the
/// controller sees the cordon and rolls the run onto another node, and the old pod then leaves
/// through that rollout, which the budget does not block. A run that is not ready can always be
/// evicted, so a broken run never holds a node.
pub fn disruption_budget(project: &str, resource: &str, name: &str) -> PodDisruptionBudget {
    let object = json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": name,
            "labels": {
                "kermodes.io/project": project,
                "kermodes.io/resource": resource,
                "app.kubernetes.io/name": name,
                "app.kubernetes.io/managed-by": "kermodes-control",
            },
        },
        "spec": {
            "maxUnavailable": 0,
            "selector": {"matchLabels": selector(project, resource)},
            "unhealthyPodEvictionPolicy": "AlwaysAllow",
        },
    });
    serde_json::from_value(object).expect("a PodDisruptionBudget built from known fields")
}

/// Where a resource's Deployment runs.
#[derive(Clone, Debug)]
pub struct Placement {
    pub name: String,
    /// Its rollout is done: every run is at its template and ready.
    pub settled: bool,
    /// The nodes of its pods, leaving out pods already terminating.
    pub nodes: Vec<String>,
}

/// The Deployments to roll onto other nodes because a run sits on a cordoned one. A rollout in
/// progress already starts its new run elsewhere (the scheduler skips cordoned nodes), and one
/// that failed would only fail again, so only settled Deployments move.
pub fn to_move<'a>(placements: &'a [Placement], cordoned: &BTreeSet<String>) -> Vec<&'a str> {
    placements
        .iter()
        .filter(|p| p.settled && p.nodes.iter().any(|node| cordoned.contains(node)))
        .map(|p| p.name.as_str())
        .collect()
}

/// The Job that discovers what the target's commit declares: `kermodes manifest` on its code,
/// read back from the Job's log.
pub fn discovery_job(target: &Target) -> Job {
    let name = discovery_job_name(&target.project, &target.commit, &target.image);
    let discover = json!({
        "name": "discover",
        "image": target.image,
        "args": ["manifest", "--project", project_dir(target)],
        "env": [{"name": "PYTHONDONTWRITEBYTECODE", "value": "1"}],
        "volumeMounts": [{"name": "code", "mountPath": "/code", "readOnly": true}],
        "securityContext": container_security(),
    });
    let object = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": {
                "kermodes.io/project": target.project,
                "kermodes.io/commit": target.commit,
                "app.kubernetes.io/managed-by": "kermodes-control",
            },
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 3600,
            "activeDeadlineSeconds": 300,
            "template": {
                "metadata": {"labels": {"kermodes.io/project": target.project}},
                "spec": pod(target, vec![snapshot(target)], vec![discover], "Never"),
            },
        },
    });
    serde_json::from_value(object).expect("a Job built from known fields")
}

/// One Job per commit and platform image: the same commit read again by a new platform is a new
/// Job, never a finished one's log from the old platform.
pub fn discovery_job_name(project: &str, commit: &str, image: &str) -> String {
    let short = &commit[..commit.len().min(12)];
    let project: String = project.replace('_', "-").chars().take(33).collect();
    // FNV-1a: stable across Rust versions, unlike the standard hasher.
    let platform = image.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    format!("{project}-discover-{short}-{:06x}", platform & 0xff_ffff)
}

fn project_dir(target: &Target) -> String {
    if target.path.is_empty() {
        "/code".to_owned()
    } else {
        format!("/code/{}", target.path.trim_matches('/'))
    }
}

/// Fetches the commit's code into `/code` before anything runs.
fn snapshot(target: &Target) -> Value {
    json!({
        "name": "snapshot",
        "image": target.image,
        "args": [
            "fetch-snapshot",
            format!("{}/{}/{}", target.snapshots, target.project, target.commit),
            "/code",
        ],
        "volumeMounts": [{"name": "code", "mountPath": "/code"}],
        "securityContext": container_security(),
    })
}

fn pod(target: &Target, init: Vec<Value>, containers: Vec<Value>, restart: &str) -> Value {
    json!({
        "restartPolicy": restart,
        "imagePullSecrets": target
            .pull_secrets
            .iter()
            .map(|name| json!({"name": name}))
            .collect::<Vec<_>>(),
        "securityContext": {
            "runAsNonRoot": true,
            "runAsUser": 10001,
            "runAsGroup": 10001,
            "fsGroup": 10001,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "initContainers": init,
        "containers": containers,
        "volumes": [
            {"name": "code", "emptyDir": {}},
            {"name": "run", "emptyDir": {"medium": "Memory"}},
        ],
    })
}

fn container_security() -> Value {
    json!({
        "allowPrivilegeEscalation": false,
        "readOnlyRootFilesystem": true,
        "capabilities": {"drop": ["ALL"]},
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource(name: &str, update: Update, rollout: Rollout) -> Resource {
        Resource {
            name: name.into(),
            kind: "deployment".into(),
            update,
            rollout,
            cpu: "250m".into(),
            memory: "1Gi".into(),
        }
    }

    fn target(commit: &str) -> Target {
        Target {
            project: "ticker".into(),
            path: "examples/ticker".into(),
            commit: commit.into(),
            image: "registry/kermodes:0.3.0".into(),
            weir: "grpc://kermodes-weir:8731".into(),
            snapshots: "http://kermodes-control:8732/snapshots".into(),
            pull_secrets: vec!["pull".into()],
        }
    }

    #[test]
    fn update_policies_decide_when_a_commit_rolls() {
        use Decision::*;
        use Update::*;
        let cases = [
            // (policy, deployed, synced) -> decision
            (Auto, None, "b", Apply),
            (Manual, None, "b", Apply),
            (Auto, Some("b"), "b", Current),
            (Manual, Some("b"), "b", Current),
            (Auto, Some("a"), "b", Apply),
            (Manual, Some("a"), "b", Hold),
        ];
        for (update, deployed, synced, want) in cases {
            assert_eq!(
                decide(update, deployed, synced),
                want,
                "{update:?} {deployed:?} {synced}"
            );
        }
    }

    #[test]
    fn names_are_dns_labels_and_unique() {
        let ok = [resource("ticks", Update::Auto, Rollout::Rolling)];
        assert_eq!(
            object_names("ticker", &ok).unwrap()["ticks"],
            "ticker-ticks"
        );
        let clash = [
            resource("a_b", Update::Auto, Rollout::Rolling),
            resource("a-b", Update::Auto, Rollout::Rolling),
        ];
        assert!(object_names("p", &clash).is_err());
        assert!(object_name(&"p".repeat(40), &"r".repeat(30)).is_err());
    }

    #[test]
    fn a_commit_is_discovered_once_per_platform_in_a_dns_label() {
        let commit = "4e3ffa8e08d69a78837b330be99141a6901ba7b9";
        let a = discovery_job_name("ticker", commit, "registry/kermodes:0.3.5");
        let b = discovery_job_name("ticker", commit, "registry/kermodes:0.3.6");
        assert_ne!(a, b);
        assert_eq!(
            a,
            discovery_job_name("ticker", commit, "registry/kermodes:0.3.5")
        );
        assert!(a.starts_with("ticker-discover-4e3ffa8e08d6-"));
        let long = discovery_job_name(&"p".repeat(63), commit, "registry/kermodes:0.3.5");
        assert!(long.len() <= 63, "{long}");
    }

    #[test]
    fn a_deployment_rolls_only_on_a_new_commit_and_as_its_resource_asks() {
        let rolling = resource("ticks", Update::Auto, Rollout::Rolling);
        let a = deployment(&target("a"), &rolling, "ticker-ticks", false);
        let b = deployment(&target("b"), &rolling, "ticker-ticks", false);
        let spec = |d: &Deployment| d.spec.clone().unwrap();
        // The selector is stable across commits; only the template's commit changes.
        assert_eq!(spec(&a).selector, spec(&b).selector);
        let commit = |d: &Deployment| {
            spec(d).template.metadata.unwrap().labels.unwrap()["kermodes.io/commit"].clone()
        };
        assert_eq!((commit(&a), commit(&b)), ("a".into(), "b".into()));
        // Stopped: no run, and the same template, so starting it again rolls nothing.
        let stopped = deployment(&target("a"), &rolling, "ticker-ticks", true);
        assert_eq!(
            (spec(&a).replicas, spec(&stopped).replicas),
            (Some(1), Some(0))
        );
        assert_eq!(spec(&stopped).template, spec(&a).template);
        let strategy = spec(&a).strategy.unwrap();
        assert_eq!(strategy.type_.as_deref(), Some("RollingUpdate"));
        let rolling_update = strategy.rolling_update.unwrap();
        assert_eq!(
            (rolling_update.max_surge, rolling_update.max_unavailable),
            (
                Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(1)),
                Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(0))
            )
        );

        let recreate = resource("book", Update::Auto, Rollout::Recreate);
        let r = deployment(&target("a"), &recreate, "ticker-book", false);
        assert_eq!(
            spec(&r).strategy.unwrap().type_.as_deref(),
            Some("Recreate")
        );

        // The run: the resource, from the snapshot's project directory, ready by its file.
        let pod = spec(&a).template.spec.unwrap();
        let run = &pod.containers[0];
        let args = run.args.clone().unwrap();
        assert_eq!(
            &args[..4],
            ["run", "ticks", "--project", "/code/examples/ticker"]
        );
        assert!(run.readiness_probe.is_some());
        // It gets exactly what it asked for, no more and no less.
        let sizes = run.resources.clone().unwrap();
        let quantity = |q: &std::collections::BTreeMap<
            String,
            k8s_openapi::apimachinery::pkg::api::resource::Quantity,
        >,
                        k: &str| q[k].0.clone();
        for side in [sizes.requests.unwrap(), sizes.limits.unwrap()] {
            assert_eq!(
                (quantity(&side, "cpu"), quantity(&side, "memory")),
                ("250m".into(), "1Gi".into())
            );
        }
        let fetch = pod.init_containers.unwrap()[0].args.clone().unwrap();
        assert_eq!(fetch[1], "http://kermodes-control:8732/snapshots/ticker/a");
    }

    #[test]
    fn a_budget_guards_exactly_its_resources_runs_and_never_a_broken_one() {
        let ticks = resource("ticks", Update::Auto, Rollout::Rolling);
        let d = deployment(&target("a"), &ticks, "ticker-ticks", false);
        let budget = disruption_budget("ticker", "ticks", "ticker-ticks")
            .spec
            .unwrap();
        assert_eq!(
            Some(budget.selector.unwrap()),
            Some(d.spec.unwrap().selector)
        );
        assert_eq!(
            budget.max_unavailable,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(0))
        );
        assert_eq!(
            budget.unhealthy_pod_eviction_policy.as_deref(),
            Some("AlwaysAllow")
        );
    }

    #[test]
    fn only_settled_runs_on_cordoned_nodes_move() {
        let placement = |name: &str, settled: bool, nodes: &[&str]| Placement {
            name: name.into(),
            settled,
            nodes: nodes.iter().map(|n| n.to_string()).collect(),
        };
        let placements = [
            placement("on-cordoned", true, &["n1"]),
            placement("elsewhere", true, &["n2"]),
            placement("already-rolling", false, &["n1", "n2"]),
            placement("one-of-two-cordoned", true, &["n2", "n1"]),
            placement("no-pods", true, &[]),
        ];
        let cordoned = BTreeSet::from(["n1".to_string()]);
        assert_eq!(
            to_move(&placements, &cordoned),
            ["on-cordoned", "one-of-two-cordoned"]
        );
        assert!(to_move(&placements, &BTreeSet::new()).is_empty());
    }
}
