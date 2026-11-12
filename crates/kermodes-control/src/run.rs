//! The controller's loop (architecture.md §11.3): for each project, sync the mainline's head,
//! discover a new commit in a Job, and reconcile every resource by its update policy. A commit
//! whose discovery fails is never rolled out; what runs keeps running. Every replica serves the
//! HTTP API; only the leader (`leader`) runs the loops.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::git::{self, Credentials, Repository};
use crate::k8s::Kube;
use crate::leader::{Leadership, Wake};
use crate::manifest::{Manifest, Update};
use crate::plan::{self, Decision, Target};
use crate::report;
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

/// Runs the controller until SIGTERM or SIGINT: the HTTP API at once, and the loops whenever this
/// replica leads.
pub async fn run(config: Config) -> Result<(), String> {
    let controller = Arc::new(Controller {
        state: Mutex::new(State::open(&config.database).await?),
        kube: Kube::connect(&config.namespace).await?,
        config,
        unchanged: Mutex::new(HashMap::new()),
    });
    let listener = tokio::net::TcpListener::bind(&controller.config.listen)
        .await
        .map_err(|e| format!("cannot listen on {}: {e}", controller.config.listen))?;
    let server = tokio::spawn(crate::http::serve(controller.clone(), listener));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| format!("cannot handle SIGTERM: {e}"))?;
    loop {
        tokio::select! {
            reason = lead(&controller) => {
                eprintln!("kermodes control: not leading: {reason}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            _ = terminate.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    // Returning drops the leadership, which frees the lock for another replica at once.
    server.abort();
    Ok(())
}

/// Waits for the leader lock, then syncs, reconciles, moves runs off cordoned nodes, and reports
/// to `sys.deployments` until it is lost; returns why.
async fn lead(controller: &Controller) -> String {
    let (mut leadership, mut wake) = match Leadership::acquire(&controller.config.database).await {
        Ok(won) => won,
        Err(e) => return e,
    };
    eprintln!("kermodes control: leading");
    tokio::select! {
        reason = leadership.lost() => reason,
        () = sync_forever(controller, &mut wake) => unreachable!("syncs until stopped"),
        () = move_off_cordoned(controller) => unreachable!("moves until stopped"),
        () = report_forever(controller) => unreachable!("reports until stopped"),
    }
}

/// How often each deployment's row goes to `sys.deployments`.
const REPORT_EVERY: Duration = Duration::from_secs(5);

/// Appends a row per deployment to `sys.deployments` every few seconds. The writer goes with
/// this future, so the table's source passes to the next leader.
async fn report_forever(controller: &Controller) {
    let rows = report::writer(controller.config.weir.clone());
    loop {
        match report_once(controller).await {
            Ok(batch) => {
                let _ = rows.try_send(batch);
            }
            Err(e) => eprintln!("kermodes control: reporting: {e}"),
        }
        tokio::time::sleep(REPORT_EVERY).await;
    }
}

async fn report_once(controller: &Controller) -> Result<RecordBatch, String> {
    let observed = controller.kube.observe().await?;
    let mut rows = Vec::new();
    for project in &controller.config.projects {
        let (latest, deployed, stopped) = {
            let mut state = controller.state.lock().await;
            (
                state.latest_discovered(&project.name).await?,
                state.deployed(&project.name).await?,
                state.stopped(&project.name).await?,
            )
        };
        let Some(latest) = latest else { continue };
        let manifest = Manifest::parse(latest.manifest.as_deref().unwrap_or_default())?;
        for resource in &manifest.resources {
            let name = plan::object_name(&project.name, &resource.name)?;
            rows.push(report::Row::new(
                &project.name,
                resource,
                deployed.get(&resource.name).map(String::as_str),
                &latest.commit,
                stopped.contains(&resource.name),
                observed.get(&name),
            ));
        }
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?;
    let now = i64::try_from(now.as_nanos()).map_err(|e| e.to_string())?;
    Ok(report::batch(now, &rows))
}

/// Syncs every project now, then at each poll or sync request.
async fn sync_forever(controller: &Controller, wake: &mut Wake) {
    let poll = Duration::from_secs(controller.config.poll_seconds);
    loop {
        for project in &controller.config.projects {
            if let Err(e) = sync(controller, project).await {
                eprintln!("kermodes control: {}: {e}", project.name);
            }
        }
        tokio::select! {
            () = tokio::time::sleep(poll) => {}
            Some(()) = wake.recv() => {}
        }
        // Requests queued meanwhile are answered by the sync about to run.
        while wake.try_recv().is_ok() {}
    }
}

/// How often runs on cordoned nodes are looked for: a drain waits this long at most before the
/// controller starts moving them.
const CORDON_CHECK: Duration = Duration::from_secs(5);

/// Moves runs off cordoned nodes, so a drain never evicts a healthy run (§17.1): each run's
/// disruption budget blocks the eviction, and its Deployment rolls onto another node instead.
async fn move_off_cordoned(controller: &Controller) {
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
    let stopped = state.stopped(&project.name).await?;
    for resource in &manifest.resources {
        let name = &names[&resource.name];
        let running = deployed.get(&resource.name).map(String::as_str);
        let is_stopped = stopped.contains(&resource.name);
        match plan::decide(resource.update, running, &latest.commit) {
            Decision::Apply => {
                let target = target(controller, project, &latest.commit);
                controller
                    .kube
                    .apply(&plan::deployment(&target, resource, name, is_stopped))
                    .await?;
                state
                    .set_deployed(&project.name, &resource.name, &latest.commit)
                    .await?;
                eprintln!(
                    "kermodes control: {}: {} rolling to {}",
                    project.name, resource.name, latest.commit
                );
            }
            // The commit it runs stays applied. For an `auto` resource that carries platform
            // upgrades too: applying an unchanged Deployment is a no-op, and a new image rolls
            // it like a new commit. A `manual` one restarts only when someone rolls it out, so
            // it is applied only to recreate it after someone deleted it, or to stop or start it;
            // it takes a new image at its next rollout or start.
            Decision::Current | Decision::Hold => {
                let desired = i32::from(!is_stopped);
                if let Some(commit) = running
                    && (resource.update == Update::Auto
                        || controller.kube.replicas(name).await? != Some(desired))
                {
                    let target = target(controller, project, commit);
                    controller
                        .kube
                        .apply(&plan::deployment(&target, resource, name, is_stopped))
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
    let is_stopped = state.stopped(project).await?.contains(resource);
    controller
        .kube
        .apply(&plan::deployment(
            &target(controller, config, &latest.commit),
            declared,
            &name,
            is_stopped,
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
