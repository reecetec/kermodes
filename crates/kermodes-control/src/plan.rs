//! What the controller applies, as pure functions of a commit's manifest: whether a resource rolls
//! to a synced commit (its update policy), and the Kubernetes objects that run it (§16.2–16.3).
//!
//! Each resource is a Deployment of one replica running `kermodes run <resource>` from the
//! commit's code snapshot, which an init container fetches from the controller (pods never hold
//! git credentials, §11.7). Its pod template names the commit, so only a new commit changes it,
//! and Kubernetes rolls it as the resource asks: start the new run, wait until it is ready, stop
//! the old (§11.4). A PodDisruptionBudget keeps a drain from evicting a healthy run; the
//! controller moves it instead, by the same rollout (§17.1).
//!
//! Past dates weir still holds are merged into history by a Job each (§8.3): which to start, and
//! what one looks like, are decided here too.

use std::collections::{BTreeMap, BTreeSet};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kermodes_weir::Date;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::manifest::{App, Resource, Rollout, Update};

/// Where history goes, as a merge Job needs it (§8.1): `KERMODES_HISTORY`, the S3 endpoint, and
/// the Secret holding `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for that bucket alone.
#[derive(Clone, Debug, Deserialize)]
pub struct History {
    pub url: String,
    pub endpoint: String,
    pub secret: String,
}

/// A merge Job that exists: the table and date it merges, whether it is still running, and
/// whether it ended in failure.
#[derive(Clone, Debug)]
pub struct MergeJob {
    pub table: String,
    pub date: Date,
    pub active: bool,
    pub failed: bool,
}

/// How long after a date's midnight (UTC) its merge waits: a write timestamped just before
/// midnight may still be committing to it.
pub const MERGE_GRACE_SECONDS: i64 = 300;

/// The merges to start at `now` (ns since the epoch): per table, the oldest past date weir still
/// holds whose midnight is the grace behind, unless one of the table's merges is still running
/// (merges of one table would only race each other's commits) or that date has a Job already. A
/// finished Job expires after a day, so a date whose merge failed gets another then; meanwhile
/// the next date goes ahead.
pub fn merges_due(
    now: i64,
    held: &[(String, Vec<Date>)],
    jobs: &[MergeJob],
) -> Vec<(String, Date)> {
    const NS_PER_DAY: i64 = 86_400_000_000_000;
    let mut due = Vec::new();
    for (table, dates) in held {
        if jobs.iter().any(|job| &job.table == table && job.active) {
            continue;
        }
        let next = dates
            .iter()
            .copied()
            .filter(|date| {
                let midnight = (i64::from(date.days()) + 1) * NS_PER_DAY;
                now >= midnight + MERGE_GRACE_SECONDS * 1_000_000_000
            })
            .filter(|date| {
                !jobs
                    .iter()
                    .any(|job| &job.table == table && job.date == *date)
            })
            .min();
        if let Some(date) = next {
            due.push((table.clone(), date));
        }
    }
    due
}

/// The Job that merges `date` of `table` from weir into history: `kermodes merge`, against weir
/// over Flight, writing to the history bucket with that bucket's credentials alone.
pub fn merge_job(
    history: &History,
    image: &str,
    weir: &str,
    pull_secrets: &[String],
    table: &str,
    date: Date,
) -> Job {
    let merge = json!({
        "name": "merge",
        "image": image,
        "args": ["merge", table, date.to_string(), "--server", weir],
        "env": [
            {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
            {"name": "KERMODES_HISTORY", "value": history.url},
            {"name": "AWS_ENDPOINT", "value": history.endpoint},
            {"name": "AWS_ALLOW_HTTP", "value": history.endpoint.starts_with("http://").to_string()},
            {"name": "AWS_REGION", "value": "us-east-1"},
        ],
        "envFrom": [{"secretRef": {"name": history.secret}}],
        // A merge streams the date through: a 1.5 GB day took 6 s and little memory.
        "resources": {
            "requests": {"cpu": "1", "memory": "2Gi"},
            "limits": {"cpu": "1", "memory": "2Gi"},
        },
        "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
        "securityContext": container_security(),
    });
    let object = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": merge_job_name(table, date),
            "labels": {
                "app.kubernetes.io/component": "merge",
                "app.kubernetes.io/managed-by": "kermodes-control",
                "kermodes.io/table": table,
                "kermodes.io/date": date.to_string(),
            },
        },
        "spec": {
            "backoffLimit": 2,
            "ttlSecondsAfterFinished": 86_400,
            "activeDeadlineSeconds": 3_600,
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/component": "merge"}},
                "spec": {
                    "restartPolicy": "Never",
                    "imagePullSecrets": pull_secrets
                        .iter()
                        .map(|name| json!({"name": name}))
                        .collect::<Vec<_>>(),
                    "securityContext": pod_security(),
                    "containers": [merge],
                    "volumes": [{"name": "tmp", "emptyDir": {}}],
                },
            },
        },
    });
    serde_json::from_value(object).expect("a Job built from known fields")
}

/// One Job per table and date: `merge-{table}-{date}`, the table's name shortened to fit and
/// told apart by a hash of the whole of it.
pub fn merge_job_name(table: &str, date: Date) -> String {
    let short: String = table.replace(['.', '_'], "-").chars().take(30).collect();
    // FNV-1a: stable across Rust versions, unlike the standard hasher.
    let hash = table.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    format!("merge-{short}-{date}-{:06x}", hash & 0xff_ffff)
}

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
        "securityContext": pod_security(),
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

/// The namespace's restricted Pod Security profile: not root, the default seccomp profile.
fn pod_security() -> Value {
    json!({
        "runAsNonRoot": true,
        "runAsUser": 10001,
        "runAsGroup": 10001,
        "fsGroup": 10001,
        "seccompProfile": {"type": "RuntimeDefault"},
    })
}

/// The artifacts store, where builds put what they produce: an S3 bucket of its own
/// (`s3://kermodes-artifacts`), its keys in the controller's environment as
/// `KERMODES_ARTIFACTS_ACCESS_KEY_ID` and `KERMODES_ARTIFACTS_SECRET_ACCESS_KEY`. Build Jobs never
/// hold them: each uploads to a presigned URL for its one object.
#[derive(Clone, Debug, Deserialize)]
pub struct Artifacts {
    pub url: String,
    pub endpoint: String,
}

/// How apps build: an image with Bun, and a Secret whose keys become each build's environment
/// (the registry tokens a project's `.npmrc` names).
#[derive(Clone, Debug, Deserialize)]
pub struct Builds {
    pub image: String,
    #[serde(default)]
    pub secret: Option<String>,
}

/// How one build of an app stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Build {
    Building,
    Built,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildRecord {
    pub app: String,
    pub commit: String,
    pub state: Build,
}

/// Where `app`'s build at `commit` is kept in the artifacts store: a tar of its files.
pub fn app_object(project: &str, app: &str, commit: &str) -> String {
    format!("apps/{project}/{app}/{commit}.tar")
}

/// What to do with a project's app builds.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BuildPlan {
    /// Apps to build at the newest deployable commit.
    pub start: Vec<String>,
    /// Builds no longer needed, as (app, commit).
    pub remove: Vec<(String, String)>,
}

/// When `latest` is the project's newest deployable commit and `apps` what it declares: each
/// app with no build at `latest` builds. Builds go when nothing can use them: those of apps the
/// commit no longer has, and finished ones older than the build an app serves. A build still
/// running stays until it ends. `builds` are newest commit first.
pub fn builds_due(apps: &[App], latest: &str, builds: &[BuildRecord]) -> BuildPlan {
    let declared = |name: &str| apps.iter().any(|app| app.name == name);
    let start = apps
        .iter()
        .filter(|app| {
            !builds
                .iter()
                .any(|b| b.app == app.name && b.commit == latest)
        })
        .map(|app| app.name.clone())
        .collect();
    let mut remove = Vec::new();
    for (index, build) in builds.iter().enumerate() {
        if build.state == Build::Building {
            continue;
        }
        let superseded = builds[..index]
            .iter()
            .any(|newer| newer.app == build.app && newer.state == Build::Built);
        if !declared(&build.app) || superseded {
            remove.push((build.app.clone(), build.commit.clone()));
        }
    }
    BuildPlan { start, remove }
}

/// The commit whose build of `app` is served: its newest finished one.
pub fn served_build<'a>(app: &str, builds: &'a [BuildRecord]) -> Option<&'a str> {
    builds
        .iter()
        .find(|b| b.app == app && b.state == Build::Built)
        .map(|b| b.commit.as_str())
}

/// How `app` reads in the Deployments tab: Running once its build at `latest` is served,
/// Building until then, Build failed (with why) if it failed; an earlier build serves meanwhile.
pub fn app_status(
    app: &str,
    latest: &str,
    builds: &[BuildRecord],
) -> (&'static str, Option<String>) {
    match builds.iter().find(|b| b.app == app && b.commit == latest) {
        Some(BuildRecord {
            state: Build::Built,
            ..
        }) => ("Running", None),
        Some(BuildRecord {
            state: Build::Failed(error),
            ..
        }) => ("Build failed", Some(error.clone())),
        _ => ("Building", None),
    }
}

/// The Job that builds `app` at the target's commit: the commit's code, `bun install
/// --frozen-lockfile` where its lockfile is (the project, when it is a workspace of apps and
/// shared packages, else the app), Vite's build under the app's address, and a tar of the
/// output uploaded to `upload`, a presigned URL for its one object.
pub fn build_job(target: &Target, app: &str, builds: &Builds, upload: &str) -> Job {
    // Project code runs here, with no credentials but the build Secret's.
    const SCRIPT: &str = r#"set -eu
mkdir -p /work/home /work/tmp
cp -R "$PROJECT_DIR" /work/project
cd /work/project
if [ -f package.json ]; then root=.; else root="apps/$APP"; fi
(cd "$root" && bun install --frozen-lockfile)
cd "apps/$APP"
bun run build --base "/apps/$PROJECT/$APP/" --outDir /work/out --emptyOutDir
tar -C /work/out -cf /work/app.tar .
bun -e 'const r = await fetch(process.env.UPLOAD, { method: "PUT", body: Bun.file("/work/app.tar") });
if (!r.ok) { console.error(`upload refused: ${r.status} ${await r.text()}`); process.exit(1); }'
echo "built $APP"
"#;
    let name = build_job_name(&target.project, app, &target.commit);
    let mut build = json!({
        "name": "build",
        "image": builds.image,
        "command": ["sh", "-c", SCRIPT],
        "env": [
            {"name": "PROJECT", "value": target.project},
            {"name": "APP", "value": app},
            {"name": "PROJECT_DIR", "value": project_dir(target)},
            {"name": "UPLOAD", "value": upload},
            {"name": "HOME", "value": "/work/home"},
            {"name": "TMPDIR", "value": "/work/tmp"},
            {"name": "BUN_INSTALL_CACHE_DIR", "value": "/work/cache"},
        ],
        // Vite's build of a dashboard: about a gigabyte at its peak.
        "resources": {
            "requests": {"cpu": "1", "memory": "2Gi"},
            "limits": {"cpu": "2", "memory": "2Gi"},
        },
        "volumeMounts": [
            {"name": "code", "mountPath": "/code", "readOnly": true},
            {"name": "work", "mountPath": "/work"},
        ],
        "securityContext": container_security(),
    });
    if let Some(secret) = &builds.secret {
        build["envFrom"] = json!([{"secretRef": {"name": secret}}]);
    }
    let mut spec = pod(target, vec![snapshot(target)], vec![build], "Never");
    spec["volumes"]
        .as_array_mut()
        .expect("a pod's volumes are a list")
        .push(json!({"name": "work", "emptyDir": {}}));
    let object = json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": {
                "app.kubernetes.io/component": "build",
                "app.kubernetes.io/managed-by": "kermodes-control",
                "kermodes.io/project": target.project,
                "kermodes.io/app": app,
                "kermodes.io/commit": target.commit,
            },
        },
        "spec": {
            "backoffLimit": 0,
            // Its log says why a build failed for a day.
            "ttlSecondsAfterFinished": 86_400,
            "activeDeadlineSeconds": BUILD_DEADLINE_SECONDS,
            "template": {
                // The app's logs are its newest build's: its pods are found by these.
                "metadata": {"labels": {
                    "app.kubernetes.io/component": "build",
                    "kermodes.io/project": target.project,
                    "kermodes.io/app": app,
                }},
                "spec": spec,
            },
        },
    });
    serde_json::from_value(object).expect("a Job built from known fields")
}

/// How long a build may run; its upload URL lasts as long.
pub const BUILD_DEADLINE_SECONDS: u64 = 1_200;

/// One Job per app and commit, a DNS label: `{project}-{app}-build-{commit}-{hash}`, the names
/// shortened to fit and told apart by a hash of all three.
pub fn build_job_name(project: &str, app: &str, commit: &str) -> String {
    let short = |name: &str| name.replace('_', "-").chars().take(18).collect::<String>();
    // FNV-1a: stable across Rust versions, unlike the standard hasher.
    let hash = [project, app, commit]
        .iter()
        .flat_map(|s| s.bytes().chain([0]))
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
    format!(
        "{}-{}-build-{}-{:06x}",
        short(project),
        short(app),
        &commit[..commit.len().min(12)],
        hash & 0xff_ffff
    )
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
            defined_at: None,
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

    fn day(text: &str) -> Date {
        Date::parse(text).unwrap()
    }

    /// Nanoseconds since the epoch at `hh:mm` UTC on 2026-11-14.
    fn at(hour: i64, minute: i64) -> i64 {
        let midnight = i64::from(day("2026-11-14").days()) * 86_400_000_000_000;
        midnight + (hour * 60 + minute) * 60_000_000_000
    }

    #[test]
    fn each_table_merges_its_oldest_settled_past_date_one_at_a_time() {
        let held = |table: &str, dates: &[&str]| {
            (table.to_string(), dates.iter().map(|d| day(d)).collect())
        };
        let job = |table: &str, date: &str, active: bool| MergeJob {
            table: table.into(),
            date: day(date),
            active,
            failed: false,
        };
        type Case<'a> = (
            &'a str,
            i64,
            Vec<(String, Vec<Date>)>,
            Vec<MergeJob>,
            Vec<(&'a str, &'a str)>,
        );
        let cases: Vec<Case> = vec![
            (
                "the oldest first",
                at(9, 0),
                vec![held("md.ticks", &["2026-11-13", "2026-11-12"])],
                vec![],
                vec![("md.ticks", "2026-11-12")],
            ),
            (
                "yesterday waits out the grace",
                at(0, 3),
                vec![held("md.ticks", &["2026-11-13"])],
                vec![],
                vec![],
            ),
            (
                "and then goes",
                at(0, 10),
                vec![held("md.ticks", &["2026-11-13"])],
                vec![],
                vec![("md.ticks", "2026-11-13")],
            ),
            (
                "one merge of a table at a time",
                at(9, 0),
                vec![held("md.ticks", &["2026-11-13"])],
                vec![job("md.ticks", "2026-11-12", true)],
                vec![],
            ),
            (
                "a date with a Job, finished, is not merged again while it lasts",
                at(9, 0),
                vec![held("md.ticks", &["2026-11-12", "2026-11-13"])],
                vec![job("md.ticks", "2026-11-12", false)],
                vec![("md.ticks", "2026-11-13")],
            ),
            (
                "tables go side by side",
                at(9, 0),
                vec![
                    held("md.ticks", &["2026-11-13"]),
                    held("md.quotes", &["2026-11-12"]),
                ],
                vec![job("md.other", "2026-11-12", true)],
                vec![("md.ticks", "2026-11-13"), ("md.quotes", "2026-11-12")],
            ),
            (
                "nothing held, nothing to do",
                at(9, 0),
                vec![held("md.ticks", &[])],
                vec![],
                vec![],
            ),
        ];
        for (case, now, held, jobs, due) in cases {
            let due: Vec<(String, Date)> =
                due.iter().map(|&(t, d)| (t.to_string(), day(d))).collect();
            assert_eq!(merges_due(now, &held, &jobs), due, "{case}");
        }
    }

    #[test]
    fn a_merge_job_is_named_validly_runs_restricted_and_reaches_only_the_history_bucket() {
        let history = History {
            url: "s3://kermodes-history".into(),
            endpoint: "http://rustfs:9000".into(),
            secret: "kermodes-history".into(),
        };
        let table = "research_desk.a_very_long_table_name_for_options_quotes";
        let job = merge_job(
            &history,
            "img:1",
            "grpc://kermodes-weir:8731",
            &["pull".into()],
            table,
            day("2026-11-12"),
        );

        let name = job.metadata.name.clone().unwrap();
        assert!(
            name.len() <= 63 && name.ends_with(char::is_alphanumeric),
            "{name}"
        );
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "{name}"
        );
        assert_ne!(
            name,
            merge_job_name(
                "research_desk.a_very_long_table_name_for_options_bonds",
                day("2026-11-12")
            )
        );

        let pod = job.spec.unwrap().template.spec.unwrap();
        let security = pod.security_context.unwrap();
        assert_eq!(security.run_as_non_root, Some(true));
        assert_eq!(security.seccomp_profile.unwrap().type_, "RuntimeDefault");
        let merge = &pod.containers[0];
        let container = merge.security_context.clone().unwrap();
        assert_eq!(container.allow_privilege_escalation, Some(false));
        assert_eq!(container.capabilities.unwrap().drop.unwrap(), ["ALL"]);
        assert_eq!(
            merge.args.clone().unwrap(),
            [
                "merge",
                table,
                "2026-11-12",
                "--server",
                "grpc://kermodes-weir:8731"
            ]
        );
        let env: BTreeMap<String, String> = merge
            .env
            .clone()
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.value.unwrap_or_default()))
            .collect();
        assert_eq!(env["KERMODES_HISTORY"], "s3://kermodes-history");
        assert_eq!(env["AWS_ALLOW_HTTP"], "true");
        let secret = merge.env_from.clone().unwrap()[0]
            .secret_ref
            .clone()
            .unwrap();
        assert_eq!(secret.name, "kermodes-history");
    }

    fn record(app: &str, commit: &str, state: Build) -> BuildRecord {
        BuildRecord {
            app: app.into(),
            commit: commit.into(),
            state,
        }
    }

    fn apps(names: &[&str]) -> Vec<App> {
        names
            .iter()
            .map(|name| App {
                name: (*name).into(),
            })
            .collect()
    }

    #[test]
    fn each_app_builds_once_per_commit_and_old_builds_go_once_superseded() {
        let failed = || Build::Failed("bun install: lockfile out of date".into());
        // Newest commit first, as the state lists them.
        let builds = [
            record("market", "c3", Build::Building),
            record("market", "c2", Build::Built),
            record("blotter", "c2", failed()),
            record("market", "c1", Build::Built),
            record("blotter", "c1", Build::Built),
            record("retired", "c1", Build::Built),
        ];
        let plan = builds_due(&apps(&["market", "blotter"]), "c3", &builds);
        // market is building c3 already; blotter has nothing at c3 yet.
        assert_eq!(plan.start, ["blotter"]);
        // market c1 is older than the c2 it serves; retired is no longer declared. blotter's
        // c1 still serves, its failed c2 says why it is not newer, and c3 is still building.
        assert_eq!(
            plan.remove,
            [
                ("market".to_owned(), "c1".to_owned()),
                ("retired".to_owned(), "c1".to_owned())
            ]
        );

        assert_eq!(served_build("market", &builds), Some("c2"));
        assert_eq!(served_build("blotter", &builds), Some("c1"));
        assert_eq!(app_status("market", "c3", &builds), ("Building", None));
        assert_eq!(
            app_status("blotter", "c2", &builds),
            (
                "Build failed",
                Some("bun install: lockfile out of date".into())
            )
        );
        // Not started yet: about to build.
        assert_eq!(app_status("blotter", "c3", &builds), ("Building", None));

        // Once c3 builds, it is served and c2 goes.
        let mut done = builds.to_vec();
        done[0].state = Build::Built;
        assert_eq!(served_build("market", &done), Some("c3"));
        assert_eq!(app_status("market", "c3", &done), ("Running", None));
        assert!(
            builds_due(&apps(&["market", "blotter"]), "c3", &done)
                .remove
                .contains(&("market".to_owned(), "c2".to_owned()))
        );
    }

    #[test]
    fn a_build_job_is_named_validly_runs_restricted_and_holds_only_its_upload_url() {
        let name = build_job_name(&"p".repeat(63), &"a".repeat(63), "4e0ea58b88651c2fd8f9");
        assert!(
            name.len() <= 63 && name.ends_with(char::is_alphanumeric),
            "{name}"
        );
        assert_ne!(
            build_job_name("ticker", "market", "c1"),
            build_job_name("ticker", "market", "c2")
        );

        let builds = Builds {
            image: "oven/bun:1".into(),
            secret: None,
        };
        let job = build_job(&target("4e0ea58b"), "market", &builds, "https://s3/signed");
        let pod = job.spec.unwrap().template.spec.unwrap();
        assert_eq!(pod.security_context.unwrap().run_as_non_root, Some(true));
        let build = &pod.containers[0];
        let security = build.security_context.clone().unwrap();
        assert_eq!(security.read_only_root_filesystem, Some(true));
        assert_eq!(security.allow_privilege_escalation, Some(false));
        let env: BTreeMap<String, String> = build
            .env
            .clone()
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.value.unwrap_or_default()))
            .collect();
        assert_eq!(env["UPLOAD"], "https://s3/signed");
        assert_eq!(env["APP"], "market");
        // No Secret unless one is configured: the upload URL is its only reach.
        assert!(build.env_from.is_none());
        // The code and a writable work directory; the root filesystem stays read-only.
        let mounts: Vec<&str> = build
            .volume_mounts
            .iter()
            .flatten()
            .map(|m| m.mount_path.as_str())
            .collect();
        assert_eq!(mounts, ["/code", "/work"]);

        let with_secret = Builds {
            secret: Some("kermodes-build".into()),
            ..builds
        };
        let job = build_job(
            &target("4e0ea58b"),
            "market",
            &with_secret,
            "https://s3/signed",
        );
        let build = &job.spec.unwrap().template.spec.unwrap().containers[0];
        let secret = build.env_from.clone().unwrap()[0]
            .secret_ref
            .clone()
            .unwrap();
        assert_eq!(secret.name, "kermodes-build");
    }
}
