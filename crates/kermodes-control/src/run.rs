//! The controller's loop (architecture.md §11.3): for each project, sync the mainline's head,
//! discover a new commit in a Job, and reconcile every resource by its update policy. A commit
//! whose discovery fails is never rolled out; what runs keeps running.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify};

use crate::git::{self, Credentials, Repository};
use crate::k8s::{Kube, Rollout};
use crate::manifest::Manifest;
use crate::plan::{self, Decision, Target};
use crate::state::State;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub namespace: String,
    /// The controller's Postgres (the catalog's).
    pub database: String,
    /// The image runs use.
    pub image: String,
    /// weir's Flight URL.
    pub weir: String,
    /// This controller's snapshot endpoint, as pods reach it.
    pub snapshots: String,
    /// Where the controller's HTTP API listens.
    pub listen: String,
    #[serde(default)]
    pub pull_secrets: Vec<String>,
    /// Required on `POST /hooks/sync`; without one, the webhook is off.
    #[serde(default)]
    pub webhook_secret: Option<String>,
    #[serde(default = "poll_seconds")]
    pub poll_seconds: u64,
    #[serde(default)]
    pub git_username: Option<String>,
    #[serde(default)]
    pub git_password: Option<String>,
    pub projects: Vec<ProjectConfig>,
}

fn poll_seconds() -> u64 {
    30
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    pub url: String,
    pub branch: String,
    #[serde(default)]
    pub path: String,
}

impl Config {
    pub fn parse(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|e| format!("an unreadable controller config: {e}"))
    }

    fn credentials(&self) -> Option<Credentials> {
        Some(Credentials {
            username: self.git_username.clone()?,
            password: self.git_password.clone()?,
        })
    }
}

pub struct Controller {
    pub config: Config,
    pub state: Mutex<State>,
    pub kube: Kube,
    /// Wakes the loop early (the sync webhook).
    pub wake: Notify,
    /// Per project, the mainline head last seen leaving the project's directory as it was, so
    /// it is not fetched again at every poll.
    unchanged: Mutex<HashMap<String, String>>,
}

/// Runs the controller on a runtime of its own until SIGTERM or SIGINT, blocking the caller.
pub fn run_blocking(config: Config) -> Result<(), String> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("kermodes-control")
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start: {e}"))?
        .block_on(run(config))
}

/// Runs the controller until SIGTERM or SIGINT.
pub async fn run(config: Config) -> Result<(), String> {
    let controller = Arc::new(Controller {
        state: Mutex::new(State::open(&config.database).await?),
        kube: Kube::connect(&config.namespace).await?,
        config,
        wake: Notify::new(),
        unchanged: Mutex::new(HashMap::new()),
    });
    let listener = tokio::net::TcpListener::bind(&controller.config.listen)
        .await
        .map_err(|e| format!("cannot listen on {}: {e}", controller.config.listen))?;
    let server = tokio::spawn(crate::http::serve(controller.clone(), listener));
    let mover = tokio::spawn(move_off_cordoned(controller.clone()));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| format!("cannot handle SIGTERM: {e}"))?;
    let poll = Duration::from_secs(controller.config.poll_seconds);
    loop {
        for project in &controller.config.projects {
            if let Err(e) = sync(&controller, project).await {
                eprintln!("kermodes control: {}: {e}", project.name);
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(poll) => {}
            _ = controller.wake.notified() => {}
            _ = terminate.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    server.abort();
    mover.abort();
    Ok(())
}

/// How often runs on cordoned nodes are looked for: a drain waits this long at most before the
/// controller starts moving them.
const CORDON_CHECK: Duration = Duration::from_secs(5);

/// Moves runs off cordoned nodes, so a drain never evicts a healthy run (§17.1): each run's
/// disruption budget blocks the eviction, and its Deployment rolls onto another node instead.
async fn move_off_cordoned(controller: Arc<Controller>) {
    loop {
        if let Err(e) = move_once(&controller.kube).await {
            eprintln!("kermodes control: moving runs off cordoned nodes: {e}");
        }
        tokio::time::sleep(CORDON_CHECK).await;
    }
}

async fn move_once(kube: &Kube) -> Result<(), String> {
    let cordoned = kube.cordoned().await?;
    if cordoned.is_empty() {
        return Ok(());
    }
    let placements = kube.placements().await?;
    for name in plan::to_move(&placements, &cordoned) {
        kube.restart(name).await?;
        eprintln!("kermodes control: {name}: moving off a cordoned node");
    }
    Ok(())
}

/// Syncs a project's mainline and reconciles its resources.
async fn sync(controller: &Controller, project: &ProjectConfig) -> Result<(), String> {
    let repo = Repository {
        url: project.url.clone(),
        branch: project.branch.clone(),
        path: project.path.clone(),
    };
    let credentials = controller.config.credentials();
    let head = {
        let (repo, credentials) = (repo.clone(), credentials.clone());
        tokio::task::spawn_blocking(move || git::head(&repo, credentials.as_ref()))
            .await
            .map_err(|e| e.to_string())??
    };
    if controller.unchanged.lock().await.get(&project.name) == Some(&head) {
        return reconcile(controller, project).await;
    }
    let known = controller
        .state
        .lock()
        .await
        .commit(&project.name, &head)
        .await?;
    let pending = match known {
        None => {
            let scratch = tempdir()?;
            let snapshot = tokio::task::spawn_blocking(move || {
                let result = git::snapshot(&repo, credentials.as_ref(), &scratch);
                let _ = std::fs::remove_dir_all(&scratch);
                result
            })
            .await
            .map_err(|e| e.to_string())??;
            let mut state = controller.state.lock().await;
            // A commit elsewhere in the repository is not a new version of the project: what
            // runs keeps running, with nothing to discover or roll out.
            let latest = state.latest(&project.name).await?;
            if latest.and_then(|c| c.tree).as_deref() == Some(snapshot.tree.as_str()) {
                drop(state);
                controller
                    .unchanged
                    .lock()
                    .await
                    .insert(project.name.clone(), snapshot.commit);
                return reconcile(controller, project).await;
            }
            state
                .add_commit(
                    &project.name,
                    &snapshot.commit,
                    &snapshot.tree,
                    &snapshot.tar,
                )
                .await?;
            drop(state);
            eprintln!(
                "kermodes control: {}: synced {}",
                project.name, snapshot.commit
            );
            Some(snapshot.commit)
        }
        // Discovery was interrupted (the controller restarted): run it again.
        Some(c) if c.manifest.is_none() && c.error.is_none() => Some(c.commit),
        Some(_) => None,
    };
    if let Some(commit) = pending {
        let result = discover(controller, project, &commit).await;
        if let Err(e) = &result {
            eprintln!(
                "kermodes control: {}: {commit} not deployable: {e}",
                project.name
            );
        }
        controller
            .state
            .lock()
            .await
            .discovered(&project.name, &commit, &result)
            .await?;
    }
    reconcile(controller, project).await
}

async fn discover(
    controller: &Controller,
    project: &ProjectConfig,
    commit: &str,
) -> Result<String, String> {
    let output = controller
        .kube
        .discover(&plan::discovery_job(&target(controller, project, commit)))
        .await?;
    let manifest = Manifest::parse(&output)?;
    if manifest.project != project.name {
        return Err(format!(
            "its kermodes.toml names the project `{}`, configured here as `{}`",
            manifest.project, project.name
        ));
    }
    plan::object_names(&project.name, &manifest.resources)?;
    Ok(output)
}

/// Brings a project's Deployments to its newest deployable commit, resource by resource.
async fn reconcile(controller: &Controller, project: &ProjectConfig) -> Result<(), String> {
    let mut state = controller.state.lock().await;
    let Some(latest) = state.latest_discovered(&project.name).await? else {
        return Ok(());
    };
    let manifest = Manifest::parse(latest.manifest.as_deref().unwrap_or_default())?;
    let names = plan::object_names(&project.name, &manifest.resources)?;
    let deployed = state.deployed(&project.name).await?;
    for resource in &manifest.resources {
        let name = &names[&resource.name];
        let running = deployed.get(&resource.name).map(String::as_str);
        match plan::decide(resource.update, running, &latest.commit) {
            Decision::Apply => {
                let target = target(controller, project, &latest.commit);
                controller
                    .kube
                    .apply(&plan::deployment(&target, resource, name))
                    .await?;
                state
                    .set_deployed(&project.name, &resource.name, &latest.commit)
                    .await?;
                eprintln!(
                    "kermodes control: {}: {} rolling to {}",
                    project.name, resource.name, latest.commit
                );
            }
            // Recreate what someone deleted, at the commit it ran.
            Decision::Current | Decision::Hold => {
                if controller.kube.rollout(name).await? == Rollout::Absent
                    && let Some(commit) = running
                {
                    let target = target(controller, project, commit);
                    controller
                        .kube
                        .apply(&plan::deployment(&target, resource, name))
                        .await?;
                }
            }
        }
        controller
            .kube
            .apply_budget(&plan::disruption_budget(
                &project.name,
                &resource.name,
                name,
            ))
            .await?;
    }
    // Resources the commit no longer declares stop.
    let wanted: BTreeSet<&String> = names.values().collect();
    for name in controller.kube.deployments(&project.name).await? {
        if !wanted.contains(&name) {
            controller.kube.delete(&name).await?;
            eprintln!("kermodes control: {}: {name} removed", project.name);
        }
    }
    for resource in deployed.keys() {
        if !names.contains_key(resource) {
            state.remove_deployed(&project.name, resource).await?;
        }
    }
    Ok(())
}

/// Rolls a resource out to its project's newest deployable commit now (`update="manual"`).
pub async fn roll_out(
    controller: &Controller,
    project: &str,
    resource: &str,
) -> Result<String, String> {
    let config = controller
        .config
        .projects
        .iter()
        .find(|p| p.name == project)
        .ok_or_else(|| format!("no project {project}"))?;
    let mut state = controller.state.lock().await;
    let latest = state
        .latest_discovered(project)
        .await?
        .ok_or_else(|| format!("{project} has no deployable commit"))?;
    let manifest = Manifest::parse(latest.manifest.as_deref().unwrap_or_default())?;
    let declared = manifest
        .resources
        .iter()
        .find(|r| r.name == resource)
        .ok_or_else(|| format!("{project} declares no resource {resource}"))?;
    let name = plan::object_name(project, resource)?;
    controller
        .kube
        .apply(&plan::deployment(
            &target(controller, config, &latest.commit),
            declared,
            &name,
        ))
        .await?;
    state
        .set_deployed(project, resource, &latest.commit)
        .await?;
    Ok(latest.commit)
}

/// Every project's resources: what each runs, what it could run, and how its rollout stands.
pub async fn status(controller: &Controller) -> Result<Value, String> {
    let mut projects = Vec::new();
    for project in &controller.config.projects {
        let mut state = controller.state.lock().await;
        let head = state.latest(&project.name).await?;
        let latest = state.latest_discovered(&project.name).await?;
        let deployed = state.deployed(&project.name).await?;
        drop(state);
        // The mainline's newest commit, when its discovery failed: what runs stays on `commit`.
        let failed = head.and_then(|head| {
            let error = head.error?;
            Some(json!({"commit": head.commit, "error": error}))
        });
        let mut resources = Vec::new();
        if let Some(latest) = &latest {
            let manifest = Manifest::parse(latest.manifest.as_deref().unwrap_or_default())?;
            for resource in &manifest.resources {
                let running = deployed.get(&resource.name);
                let name = plan::object_name(&project.name, &resource.name)?;
                let rollout = controller.kube.rollout(&name).await?;
                resources.push(json!({
                    "name": resource.name,
                    "update": format!("{:?}", resource.update).to_lowercase(),
                    "commit": running,
                    "available": (running != Some(&latest.commit)).then_some(&latest.commit),
                    "rollout": format!("{rollout:?}").to_lowercase(),
                }));
            }
        }
        projects.push(json!({
            "name": project.name,
            "url": project.url,
            "branch": project.branch,
            "path": project.path,
            "commit": latest.map(|c| c.commit),
            "failed": failed,
            "resources": resources,
        }));
    }
    Ok(json!({ "projects": projects }))
}

fn target(controller: &Controller, project: &ProjectConfig, commit: &str) -> Target {
    let config = &controller.config;
    Target {
        project: project.name.clone(),
        path: project.path.clone(),
        commit: commit.to_owned(),
        image: config.image.clone(),
        weir: config.weir.clone(),
        snapshots: config.snapshots.clone(),
        pull_secrets: config.pull_secrets.clone(),
    }
}

fn tempdir() -> Result<std::path::PathBuf, String> {
    let dir = std::env::temp_dir().join(format!(
        "kermodes-sync-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    Ok(dir)
}
