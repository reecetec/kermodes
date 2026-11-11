//! The Kubernetes side (architecture.md §16.2): plain objects, server-side applied under the
//! field manager `kermodes-control`, in the namespace the controller runs in. No custom resource:
//! git and Postgres hold what should run, and labels tie each object to its project and resource.

use std::time::{Duration, Instant};

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{DeleteParams, ListParams, LogParams, Patch, PatchParams, PostParams};

const MANAGER: &str = "kermodes-control";
/// How long discovery may take: fetching the snapshot and importing the project.
const DISCOVERY_DEADLINE: Duration = Duration::from_secs(360);

#[derive(Clone)]
pub struct Kube {
    deployments: Api<Deployment>,
    jobs: Api<Job>,
    pods: Api<Pod>,
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
            jobs: Api::namespaced(client.clone(), namespace),
            pods: Api::namespaced(client, namespace),
        })
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

    pub async fn delete(&self, name: &str) -> Result<(), String> {
        match self
            .deployments
            .delete(name, &DeleteParams::default())
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(format!("cannot delete {name}: {e}")),
        }
    }

    pub async fn rollout(&self, name: &str) -> Result<Rollout, String> {
        let Some(deployment) = self
            .deployments
            .get_opt(name)
            .await
            .map_err(|e| format!("cannot read {name}: {e}"))?
        else {
            return Ok(Rollout::Absent);
        };
        let status = deployment.status.unwrap_or_default();
        let deadline_passed = status.conditions.unwrap_or_default().iter().any(|c| {
            c.type_ == "Progressing" && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
        });
        let generation = deployment.metadata.generation.unwrap_or(0);
        let observed = status.observed_generation.unwrap_or(0) >= generation;
        let updated = status.updated_replicas.unwrap_or(0);
        let ready = status.ready_replicas.unwrap_or(0);
        let total = status.replicas.unwrap_or(0);
        Ok(if deadline_passed {
            Rollout::Failed
        } else if observed && updated >= 1 && ready >= 1 && total == updated {
            Rollout::Ready
        } else {
            Rollout::Rolling
        })
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
