//! The Kubernetes side (architecture.md §16.2): plain objects, server-side applied under the
//! field manager `kermodes-control`, in the namespace the controller runs in. No custom resource:
//! git and Postgres hold what should run, and labels tie each object to its project and resource.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Event, Node, Pod};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::Api;
use kube::api::{
    ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams, LogParams, Patch,
    PatchParams, PostParams,
};

use crate::plan::{MergeJob, Placement};
use crate::report::{self, Observed};
use crate::system;

const MANAGER: &str = "kermodes-control";
/// The platform's own pods, by their chart's (and the Postgres operator's) component label.
const COMPONENTS_SELECTOR: &str = "app.kubernetes.io/component in (weir,database,control,ui)";
/// How long discovery may take: fetching the snapshot and importing the project.
const DISCOVERY_DEADLINE: Duration = Duration::from_secs(360);

#[derive(Clone)]
pub struct Kube {
    deployments: Api<Deployment>,
    budgets: Api<PodDisruptionBudget>,
    jobs: Api<Job>,
    pods: Api<Pod>,
    nodes: Api<Node>,
    events: Api<Event>,
    /// metrics-server's pod usage (`metrics.k8s.io`).
    metrics: Api<DynamicObject>,
}

/// Where a resource's rollout stands, as Kubernetes reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rollout {
    /// Its pod at the applied template is ready.
    Ready,
    /// A new run is starting or catching up; the old one serves meanwhile.
    Rolling,
    /// The new run missed the progress deadline; the old one keeps serving.
    Failed,
    /// Nothing runs it.
    Absent,
}

impl Kube {
    pub async fn connect(namespace: &str) -> Result<Self, String> {
        let client = kube::Client::try_default()
            .await
            .map_err(|e| format!("cannot reach Kubernetes: {e}"))?;
        Ok(Self {
            deployments: Api::namespaced(client.clone(), namespace),
            budgets: Api::namespaced(client.clone(), namespace),
            jobs: Api::namespaced(client.clone(), namespace),
            pods: Api::namespaced(client.clone(), namespace),
            nodes: Api::all(client.clone()),
            events: Api::namespaced(client.clone(), namespace),
            metrics: Api::namespaced_with(
                client,
                namespace,
                &ApiResource::from_gvk_with_plural(
                    &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
                    "pods",
                ),
            ),
        })
    }

    /// What Kubernetes shows of each of the controller's Deployments, by name: rollout, pods,
    /// usage, and newest Warning event. Usage is missing (zero) while metrics-server is not
    /// answering; the rest does not depend on it.
    pub async fn observe(&self) -> Result<BTreeMap<String, Observed>, String> {
        let managed =
            ListParams::default().labels(&format!("app.kubernetes.io/managed-by={MANAGER}"));
        let mut observed: BTreeMap<String, Observed> = self
            .deployments
            .list(&managed)
            .await
            .map_err(|e| format!("cannot list deployments: {e}"))?
            .items
            .iter()
            .filter_map(|d| {
                let observed = Observed {
                    rollout: rollout_of(d),
                    pods: Vec::new(),
                    cpu: 0.0,
                    memory: 0,
                    warning: None,
                };
                Some((d.metadata.name.clone()?, observed))
            })
            .collect();
        // Each pod's Deployment, for its usage and events.
        let mut owner: BTreeMap<String, String> = BTreeMap::new();
        for pod in self
            .pods
            .list(&managed)
            .await
            .map_err(|e| format!("cannot list pods: {e}"))?
            .items
        {
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let (Some(name), Some(deployment)) = (
                pod.metadata.name.clone(),
                pod.metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("app.kubernetes.io/name").cloned()),
            ) else {
                continue;
            };
            if let Some(seen) = observed.get_mut(&deployment) {
                seen.pods.push(pod_state(&pod));
                owner.insert(name, deployment);
            }
        }
        if let Ok(usage) = self.metrics.list(&managed).await {
            for pod in usage.items {
                let Some(seen) = pod
                    .metadata
                    .name
                    .as_ref()
                    .and_then(|name| owner.get(name))
                    .and_then(|deployment| observed.get_mut(deployment))
                else {
                    continue;
                };
                for container in pod.data["containers"].as_array().into_iter().flatten() {
                    let usage = &container["usage"];
                    seen.cpu += usage["cpu"].as_str().and_then(report::cores).unwrap_or(0.0);
                    seen.memory += usage["memory"]
                        .as_str()
                        .and_then(report::bytes)
                        .unwrap_or(0);
                }
            }
        }
        for event in self
            .events
            .list(&ListParams::default())
            .await
            .map_err(|e| format!("cannot list events: {e}"))?
            .items
        {
            if event.type_.as_deref() != Some("Warning") {
                continue;
            }
            let Some(seen) = deployment_of(&event, &owner).and_then(|d| observed.get_mut(&d))
            else {
                continue;
            };
            let Some(at) = event_ns(&event) else { continue };
            if seen.warning.as_ref().is_none_or(|(_, newest)| at > *newest) {
                let text = format!(
                    "{}: {}",
                    event.reason.as_deref().unwrap_or("Warning"),
                    event.message.as_deref().unwrap_or_default().trim()
                );
                seen.warning = Some((text, at));
            }
        }
        Ok(observed)
    }

    /// A Deployment's events, newest first: its own, its ReplicaSets', and its pods'.
    pub async fn events(&self, name: &str) -> Result<Vec<serde_json::Value>, String> {
        let owner: BTreeMap<String, String> = self
            .pods_of(name)
            .await?
            .into_iter()
            .filter_map(|p| Some((p.metadata.name?, name.to_owned())))
            .collect();
        let events = self
            .events
            .list(&ListParams::default())
            .await
            .map_err(|e| format!("cannot list events: {e}"))?
            .items
            .into_iter()
            .filter(|e| deployment_of(e, &owner).as_deref() == Some(name));
        Ok(newest_first(events))
    }

    /// The platform's components as their pods show them, in `system::COMPONENTS` order; one
    /// without pods is left out. Usage is zero while metrics-server is not answering.
    pub async fn components(&self) -> Result<Vec<system::Component>, String> {
        let selector = ListParams::default().labels(COMPONENTS_SELECTOR);
        let pods = self
            .pods
            .list(&selector)
            .await
            .map_err(|e| format!("cannot list pods: {e}"))?
            .items;
        let mut usage: BTreeMap<String, (f64, i64)> = BTreeMap::new();
        if let Ok(metrics) = self.metrics.list(&selector).await {
            for pod in metrics.items {
                let Some(name) = pod.metadata.name.clone() else {
                    continue;
                };
                let used = usage.entry(name).or_default();
                for container in pod.data["containers"].as_array().into_iter().flatten() {
                    let measured = &container["usage"];
                    used.0 += measured["cpu"]
                        .as_str()
                        .and_then(report::cores)
                        .unwrap_or(0.0);
                    used.1 += measured["memory"]
                        .as_str()
                        .and_then(report::bytes)
                        .unwrap_or(0);
                }
            }
        }
        // Each component's pods, and every object its events can be about.
        let mut instances: BTreeMap<String, Vec<system::Instance>> = BTreeMap::new();
        let mut about: BTreeMap<String, String> = BTreeMap::new();
        let mut ready_since: BTreeMap<String, i64> = BTreeMap::new();
        for pod in pods
            .iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
        {
            let (Some(name), Some(component)) = (
                pod.metadata.name.clone(),
                label(pod, "app.kubernetes.io/component"),
            ) else {
                continue;
            };
            for object in objects_of(pod) {
                about.insert(object, component.clone());
            }
            if let Some(since) = ready_since_ns(pod) {
                ready_since.insert(name.clone(), since);
            }
            let (cpu, memory) = usage.get(&name).copied().unwrap_or_default();
            instances
                .entry(component)
                .or_default()
                .push(instance(pod, name, cpu, memory));
        }
        let mut warnings: BTreeMap<String, (String, i64)> = BTreeMap::new();
        for event in self
            .events
            .list(&ListParams::default())
            .await
            .map_err(|e| format!("cannot list events: {e}"))?
            .items
        {
            if event.type_.as_deref() != Some("Warning") {
                continue;
            }
            let (Some(object), Some(at)) = (event.involved_object.name.as_ref(), event_ns(&event))
            else {
                continue;
            };
            let Some(component) = about.get(object) else {
                continue;
            };
            let reason = event.reason.as_deref().unwrap_or("Warning");
            if system::recovered(reason, at, ready_since.get(object).copied()) {
                continue;
            }
            if warnings
                .get(component)
                .is_none_or(|(_, newest)| at > *newest)
            {
                let text = format!(
                    "{reason}: {}",
                    event.message.as_deref().unwrap_or_default().trim()
                );
                warnings.insert(component.clone(), (text, at));
            }
        }
        Ok(system::COMPONENTS
            .iter()
            .filter_map(|name| {
                let instances = instances.remove(*name)?;
                let warning = warnings.remove(*name);
                Some(system::Component {
                    name: (*name).to_owned(),
                    status: system::status(&instances),
                    instances,
                    warning_ns: warning.as_ref().map(|(_, at)| *at),
                    warning: warning.map(|(text, _)| text),
                })
            })
            .collect())
    }

    /// A component's events, newest first: its pods' and their owners'.
    pub async fn component_events(
        &self,
        component: &str,
    ) -> Result<Vec<serde_json::Value>, String> {
        let objects: BTreeSet<String> = self
            .component_pods(component)
            .await?
            .iter()
            .flat_map(objects_of)
            .collect();
        let events = self
            .events
            .list(&ListParams::default())
            .await
            .map_err(|e| format!("cannot list events: {e}"))?
            .items
            .into_iter()
            .filter(|e| {
                e.involved_object
                    .name
                    .as_ref()
                    .is_some_and(|name| objects.contains(name))
            });
        Ok(newest_first(events))
    }

    /// The last `tail` lines of a component's pod: `pod` if it is one of the component's, else
    /// a ready one, else the newest.
    pub async fn component_logs(
        &self,
        component: &str,
        pod: Option<&str>,
        tail: i64,
    ) -> Result<String, String> {
        let mut pods = self.component_pods(component).await?;
        pods.sort_by_key(|p| p.metadata.creation_timestamp.as_ref().map(|t| t.0));
        let chosen = match pod {
            Some(wanted) => pods
                .iter()
                .find(|p| p.metadata.name.as_deref() == Some(wanted))
                .ok_or_else(|| format!("{wanted} is not a {component} pod"))?,
            None => pods
                .iter()
                .find(|p| pod_state(p).ready)
                .or_else(|| pods.last())
                .ok_or_else(|| format!("no {component} pod runs"))?,
        };
        let name = chosen.metadata.name.clone().unwrap_or_default();
        let params = LogParams {
            container: chosen
                .spec
                .as_ref()
                .and_then(|s| s.containers.first())
                .map(|c| c.name.clone()),
            tail_lines: Some(tail),
            timestamps: true,
            ..LogParams::default()
        };
        self.pods
            .logs(&name, &params)
            .await
            .map_err(|e| format!("cannot read {name}'s log: {e}"))
    }

    async fn component_pods(&self, component: &str) -> Result<Vec<Pod>, String> {
        Ok(self
            .pods
            .list(
                &ListParams::default().labels(&format!("app.kubernetes.io/component={component}")),
            )
            .await
            .map_err(|e| format!("cannot list {component}'s pods: {e}"))?
            .items
            .into_iter()
            .filter(|p| p.metadata.deletion_timestamp.is_none())
            .collect())
    }

    /// The last `tail` lines of the serving run's log: its ready pod's, else its newest's.
    pub async fn logs(&self, name: &str, tail: i64) -> Result<String, String> {
        let mut pods = self.pods_of(name).await?;
        pods.sort_by_key(|p| p.metadata.creation_timestamp.as_ref().map(|t| t.0));
        let serving = pods
            .iter()
            .find(|p| pod_state(p).ready)
            .or_else(|| pods.last())
            .and_then(|p| p.metadata.name.clone())
            .ok_or_else(|| format!("nothing runs {name}"))?;
        let params = LogParams {
            container: Some("run".into()),
            tail_lines: Some(tail),
            timestamps: true,
            ..LogParams::default()
        };
        self.pods
            .logs(&serving, &params)
            .await
            .map_err(|e| format!("cannot read {serving}'s log: {e}"))
    }

    async fn pods_of(&self, name: &str) -> Result<Vec<Pod>, String> {
        Ok(self
            .pods
            .list(&ListParams::default().labels(&format!(
                "app.kubernetes.io/name={name},app.kubernetes.io/managed-by={MANAGER}"
            )))
            .await
            .map_err(|e| format!("cannot list {name}'s pods: {e}"))?
            .items)
    }

    pub async fn apply(&self, deployment: &Deployment) -> Result<(), String> {
        let name = deployment.metadata.name.as_deref().unwrap_or_default();
        self.deployments
            .patch(
                name,
                &PatchParams::apply(MANAGER).force(),
                &Patch::Apply(deployment),
            )
            .await
            .map_err(|e| format!("cannot apply {name}: {e}"))?;
        Ok(())
    }

    pub async fn apply_budget(&self, budget: &PodDisruptionBudget) -> Result<(), String> {
        let name = budget.metadata.name.as_deref().unwrap_or_default();
        self.budgets
            .patch(
                name,
                &PatchParams::apply(MANAGER).force(),
                &Patch::Apply(budget),
            )
            .await
            .map_err(|e| format!("cannot apply {name}'s disruption budget: {e}"))?;
        Ok(())
    }

    /// The names of a project's Deployments.
    pub async fn deployments(&self, project: &str) -> Result<Vec<String>, String> {
        let selector =
            format!("kermodes.io/project={project},app.kubernetes.io/managed-by={MANAGER}");
        Ok(self
            .deployments
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| format!("cannot list {project}'s deployments: {e}"))?
            .items
            .into_iter()
            .filter_map(|d| d.metadata.name)
            .collect())
    }

    /// Deletes a resource's Deployment and its disruption budget.
    pub async fn delete(&self, name: &str) -> Result<(), String> {
        let gone = |result: Result<_, kube::Error>| match result {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(format!("cannot delete {name}: {e}")),
        };
        gone(
            self.deployments
                .delete(name, &DeleteParams::default())
                .await
                .map(|_| ()),
        )?;
        gone(
            self.budgets
                .delete(name, &DeleteParams::default())
                .await
                .map(|_| ()),
        )
    }

    pub async fn rollout(&self, name: &str) -> Result<Rollout, String> {
        Ok(self
            .deployments
            .get_opt(name)
            .await
            .map_err(|e| format!("cannot read {name}: {e}"))?
            .as_ref()
            .map_or(Rollout::Absent, rollout_of))
    }

    /// How many runs a Deployment asks for; `None` when it does not exist.
    pub async fn replicas(&self, name: &str) -> Result<Option<i32>, String> {
        Ok(self
            .deployments
            .get_opt(name)
            .await
            .map_err(|e| format!("cannot read {name}: {e}"))?
            .map(|d| d.spec.and_then(|s| s.replicas).unwrap_or(1)))
    }

    /// The nodes marked unschedulable: cordoned, usually to be drained.
    pub async fn cordoned(&self) -> Result<BTreeSet<String>, String> {
        Ok(self
            .nodes
            .list(&ListParams::default())
            .await
            .map_err(|e| format!("cannot list nodes: {e}"))?
            .items
            .into_iter()
            .filter(|n| n.spec.as_ref().and_then(|s| s.unschedulable) == Some(true))
            .filter_map(|n| n.metadata.name)
            .collect())
    }

    /// Where each of the controller's Deployments runs.
    pub async fn placements(&self) -> Result<Vec<Placement>, String> {
        let managed =
            ListParams::default().labels(&format!("app.kubernetes.io/managed-by={MANAGER}"));
        let mut nodes: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for pod in self
            .pods
            .list(&managed)
            .await
            .map_err(|e| format!("cannot list pods: {e}"))?
            .items
        {
            // A terminating pod is leaving already; counting it would move its resource again.
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }
            let deployment = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("app.kubernetes.io/name").cloned());
            let node = pod.spec.and_then(|s| s.node_name);
            if let (Some(deployment), Some(node)) = (deployment, node) {
                nodes.entry(deployment).or_default().push(node);
            }
        }
        Ok(self
            .deployments
            .list(&managed)
            .await
            .map_err(|e| format!("cannot list deployments: {e}"))?
            .items
            .iter()
            .filter_map(|d| {
                let name = d.metadata.name.clone()?;
                Some(Placement {
                    settled: rollout_of(d) == Rollout::Ready,
                    nodes: nodes.remove(&name).unwrap_or_default(),
                    name,
                })
            })
            .collect())
    }

    /// Rolls a Deployment onto fresh pods at the same commit, as `kubectl rollout restart` does.
    pub async fn restart(&self, name: &str) -> Result<(), String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let patch = serde_json::json!({"spec": {"template": {"metadata": {"annotations": {
            "kermodes.io/restarted-at": now.to_string(),
        }}}}});
        self.deployments
            .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map_err(|e| format!("cannot restart {name}: {e}"))?;
        Ok(())
    }

    /// The merge Jobs that exist (`plan::merge_job`), and whether each is still running.
    pub async fn merge_jobs(&self) -> Result<Vec<MergeJob>, String> {
        let jobs = self
            .jobs
            .list(&ListParams::default().labels("app.kubernetes.io/component=merge"))
            .await
            .map_err(|e| format!("cannot list merge jobs: {e}"))?;
        Ok(jobs
            .items
            .into_iter()
            .filter_map(|job| {
                let labels = job.metadata.labels?;
                let table = labels.get("kermodes.io/table")?.clone();
                let date = kermodes_weir::Date::parse(labels.get("kermodes.io/date")?).ok()?;
                let finished = job
                    .status
                    .and_then(|status| status.conditions)
                    .unwrap_or_default()
                    .iter()
                    .any(|c| (c.type_ == "Complete" || c.type_ == "Failed") && c.status == "True");
                Some(MergeJob {
                    table,
                    date,
                    active: !finished,
                })
            })
            .collect())
    }

    /// Starts a Job; one of the same name already there is left as it is.
    pub async fn start(&self, job: &Job) -> Result<(), String> {
        match self.jobs.create(&PostParams::default(), job).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
            Err(e) => Err(format!("cannot start a job: {e}")),
        }
    }

    /// Runs a discovery Job (or waits for the one already running for this commit) and returns
    /// what it printed: the manifest, or why it failed.
    pub async fn discover(&self, job: &Job) -> Result<String, String> {
        let name = job.metadata.name.clone().unwrap_or_default();
        let existing = self
            .jobs
            .get_opt(&name)
            .await
            .map_err(|e| format!("cannot read job {name}: {e}"))?;
        if existing.is_none() {
            self.jobs
                .create(&PostParams::default(), job)
                .await
                .map_err(|e| format!("cannot start discovery: {e}"))?;
        }
        let deadline = Instant::now() + DISCOVERY_DEADLINE;
        loop {
            let status = self
                .jobs
                .get(&name)
                .await
                .map_err(|e| format!("cannot read job {name}: {e}"))?
                .status
                .unwrap_or_default();
            if status.succeeded.unwrap_or(0) > 0 {
                return self.log(&name).await;
            }
            if status.failed.unwrap_or(0) > 0 {
                let log = self.log(&name).await.unwrap_or_default();
                let tail: Vec<&str> = log.lines().rev().take(5).collect();
                let tail: Vec<&str> = tail.into_iter().rev().collect();
                return Err(format!("discovery failed: {}", tail.join("\n")));
            }
            if Instant::now() > deadline {
                return Err("discovery took longer than 6 minutes".into());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn log(&self, job: &str) -> Result<String, String> {
        let pods = self
            .pods
            .list(&ListParams::default().labels(&format!("job-name={job}")))
            .await
            .map_err(|e| format!("cannot find job {job}'s pod: {e}"))?;
        let pod = pods
            .items
            .into_iter()
            .filter_map(|p| p.metadata.name)
            .next()
            .ok_or_else(|| format!("job {job} has no pod"))?;
        let params = LogParams {
            container: Some("discover".into()),
            ..LogParams::default()
        };
        self.pods
            .logs(&pod, &params)
            .await
            .map_err(|e| format!("cannot read {pod}'s log: {e}"))
    }
}

/// Where a Deployment's rollout stands, from its status.
fn rollout_of(deployment: &Deployment) -> Rollout {
    let status = deployment.status.clone().unwrap_or_default();
    let deadline_passed = status.conditions.unwrap_or_default().iter().any(|c| {
        c.type_ == "Progressing" && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
    });
    let generation = deployment.metadata.generation.unwrap_or(0);
    let observed = status.observed_generation.unwrap_or(0) >= generation;
    let updated = status.updated_replicas.unwrap_or(0);
    let ready = status.ready_replicas.unwrap_or(0);
    let total = status.replicas.unwrap_or(0);
    if deadline_passed {
        Rollout::Failed
    } else if observed && updated >= 1 && ready >= 1 && total == updated {
        Rollout::Ready
    } else {
        Rollout::Rolling
    }
}

/// A pod as the report reads it: where it runs, whether it serves, and why it cannot.
fn pod_state(pod: &Pod) -> report::Pod {
    let status = pod.status.clone().unwrap_or_default();
    let containers = status.container_statuses.clone().unwrap_or_default();
    let run = containers.iter().find(|c| c.name == "run");
    let condition = |kind: &str| {
        status
            .conditions
            .iter()
            .flatten()
            .find(|c| c.type_ == kind)
            .cloned()
    };
    report::Pod {
        node: pod.spec.as_ref().and_then(|s| s.node_name.clone()),
        ready: condition("Ready").is_some_and(|c| c.status == "True"),
        started_ns: status
            .start_time
            .and_then(|t| i64::try_from(t.0.as_nanosecond()).ok()),
        restarts: containers.iter().map(|c| i64::from(c.restart_count)).sum(),
        // The run's container, or an init container stuck before it (the snapshot fetch).
        waiting: run
            .into_iter()
            .chain(status.init_container_statuses.iter().flatten())
            .find_map(|c| c.state.as_ref()?.waiting.as_ref()?.reason.clone())
            .filter(|reason| reason != "ContainerCreating" && reason != "PodInitializing"),
        unschedulable: condition("PodScheduled")
            .is_some_and(|c| c.status == "False" && c.reason.as_deref() == Some("Unschedulable")),
    }
}

/// The Deployment an event is about: its own, a ReplicaSet's (`{deployment}-{hash}`), or one of
/// the pods in `owner` (pod name to Deployment).
fn deployment_of(event: &Event, owner: &BTreeMap<String, String>) -> Option<String> {
    let object = &event.involved_object;
    let name = object.name.as_deref()?;
    match object.kind.as_deref() {
        Some("Pod") => owner.get(name).cloned(),
        Some("ReplicaSet") => name.rsplit_once('-').map(|(d, _)| d.to_owned()),
        _ => Some(name.to_owned()),
    }
}

/// When an event last happened, in nanoseconds since the epoch.
fn event_ns(event: &Event) -> Option<i64> {
    let at = event
        .last_timestamp
        .as_ref()
        .map(|t| t.0)
        .or_else(|| event.event_time.as_ref().map(|t| t.0))
        .or_else(|| event.first_timestamp.as_ref().map(|t| t.0))?;
    i64::try_from(at.as_nanosecond()).ok()
}

/// `events` as the API shows them, newest first, at most 100.
fn newest_first(events: impl Iterator<Item = Event>) -> Vec<serde_json::Value> {
    let mut events: Vec<(i64, serde_json::Value)> = events
        .filter_map(|e| {
            let at = event_ns(&e)?;
            let json = serde_json::json!({
                "type": e.type_,
                "reason": e.reason,
                "message": e.message.as_deref().map(str::trim),
                "count": e.count,
                "at_ns": at,
                "object": format!(
                    "{}/{}",
                    e.involved_object.kind.as_deref().unwrap_or_default(),
                    e.involved_object.name.as_deref().unwrap_or_default()
                ),
            });
            Some((at, json))
        })
        .collect();
    events.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
    events.into_iter().take(100).map(|(_, e)| e).collect()
}

fn label(pod: &Pod, key: &str) -> Option<String> {
    pod.metadata.labels.as_ref()?.get(key).cloned()
}

/// The objects a pod's events can be about: the pod, its owners (a StatefulSet, the Postgres
/// operator's Cluster, a ReplicaSet), and a ReplicaSet's Deployment (`{deployment}-{hash}`).
fn objects_of(pod: &Pod) -> Vec<String> {
    let mut objects: Vec<String> = pod.metadata.name.iter().cloned().collect();
    for owner in pod.metadata.owner_references.iter().flatten() {
        objects.push(owner.name.clone());
        if owner.kind == "ReplicaSet"
            && let Some((deployment, _)) = owner.name.rsplit_once('-')
        {
            objects.push(deployment.to_owned());
        }
    }
    objects
}

/// One pod of a component, using `cpu` cores and `memory` bytes.
fn instance(pod: &Pod, name: String, cpu: f64, memory: i64) -> system::Instance {
    let state = pod_state(pod);
    let containers = pod
        .spec
        .as_ref()
        .map_or(&[][..], |s| s.containers.as_slice());
    // Each container's limit, else its request: what it was sized at.
    let sized = |resource: &str| -> Vec<String> {
        containers
            .iter()
            .filter_map(|c| {
                let r = c.resources.as_ref()?;
                r.limits
                    .as_ref()
                    .and_then(|l| l.get(resource))
                    .or_else(|| r.requests.as_ref()?.get(resource))
                    .map(|q| q.0.clone())
            })
            .collect()
    };
    let (cpus, memories) = (sized("cpu"), sized("memory"));
    let status = pod.status.clone().unwrap_or_default();
    system::Instance {
        pod: name,
        role: label(pod, "cnpg.io/instanceRole"),
        version: containers
            .first()
            .and_then(|c| c.image.as_deref())
            .and_then(system::image_tag),
        ready: state.ready,
        node: state.node,
        started_ns: state.started_ns,
        restarts: state.restarts,
        waiting: status
            .container_statuses
            .iter()
            .flatten()
            .chain(status.init_container_statuses.iter().flatten())
            .find_map(|c| c.state.as_ref()?.waiting.as_ref()?.reason.clone())
            .filter(|reason| reason != "ContainerCreating" && reason != "PodInitializing"),
        cpu,
        cpu_limit: (!cpus.is_empty())
            .then(|| cpus.iter().map(|q| report::cores(q)).sum::<Option<f64>>())
            .flatten(),
        memory,
        memory_limit: (!memories.is_empty())
            .then(|| {
                memories
                    .iter()
                    .map(|q| report::bytes(q))
                    .sum::<Option<i64>>()
            })
            .flatten(),
    }
}

/// When a pod became ready, if it is ready now.
fn ready_since_ns(pod: &Pod) -> Option<i64> {
    let ready = pod
        .status
        .as_ref()?
        .conditions
        .as_ref()?
        .iter()
        .find(|c| c.type_ == "Ready" && c.status == "True")?;
    i64::try_from(ready.last_transition_time.as_ref()?.0.as_nanosecond()).ok()
}
