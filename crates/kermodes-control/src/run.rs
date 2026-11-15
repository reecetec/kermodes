//! The controller's loop (architecture.md §11.3): for each project, sync the mainline's head,
//! discover a new commit in a Job, and reconcile every resource by its update policy. A commit
//! whose discovery fails is never rolled out; what runs keeps running. Every replica serves the
//! HTTP API; only the leader (`leader`) runs the loops.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::record_batch::RecordBatch;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::artifacts::Artifacts;
use crate::browse;
use crate::git::{self, Credentials, Repository};
use crate::k8s::Kube;
use crate::leader::{Leadership, Wake};
use crate::manifest::{Manifest, Update};
use crate::plan::{self, Build, Decision, Target};
use crate::report;
use crate::state::{Commit, State};
use crate::system;

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
    /// Where history goes; without it, nothing is merged.
    #[serde(default)]
    pub history: Option<plan::History>,
    /// Where builds put what they produce, and how apps build; without both, apps are not built.
    #[serde(default)]
    pub artifacts: Option<plan::Artifacts>,
    #[serde(default)]
    pub builds: Option<plan::Builds>,
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
    /// Deploy only commits whose checks (CI) passed, as the git host's commit status says.
    #[serde(default)]
    pub checks: bool,
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
    pub artifacts: Option<Artifacts>,
    /// Per project, the mainline head last seen leaving the project's directory as it was, so
    /// it is not fetched again at every poll.
    unchanged: Mutex<HashMap<String, String>>,
    /// The lake's last probe and when it ran, so a page polling the components reaches the
    /// store at most every `LAKE_PROBE_EVERY`.
    lake_probe: Mutex<Option<(Instant, Result<(), String>)>>,
    /// Per project, the head last found waiting for its checks, so the wait is logged once.
    waiting: Mutex<HashMap<String, String>>,
    /// Requests to the git host's API: commit statuses.
    http: reqwest::Client,
}

/// How long a probe of the lake's store stands, and how long one may take.
const LAKE_PROBE_EVERY: Duration = Duration::from_secs(30);
const LAKE_PROBE_DEADLINE: Duration = Duration::from_secs(2);
/// How long a merge may be due without a Job before the lake reads Merge overdue.
const MERGE_OVERDUE_NS: i64 = 3_600_000_000_000;

impl Controller {
    /// The lake as a component of the platform, when history is configured: its store probed,
    /// its merges (`system::lake_status`).
    pub async fn lake(&self) -> Option<system::Component> {
        let history = self.config.history.as_ref()?;
        let probe = self.probe_lake(&history.url).await;
        let jobs = self.kube.merge_jobs().await.unwrap_or_default();
        // Without weir's dates, nothing can be overdue: weir down shows on its own row.
        let held = past_dates(self.config.weir.clone())
            .await
            .unwrap_or_default();
        let overdue = plan::merges_due(now_ns() - MERGE_OVERDUE_NS, &held, &jobs);
        let (status, warning) = system::lake_status(&probe, &jobs, &overdue);
        Some(system::Component {
            name: "lake".into(),
            status,
            instances: Vec::new(),
            warning,
            warning_ns: None,
        })
    }

    async fn probe_lake(&self, url: &str) -> Result<(), String> {
        let mut last = self.lake_probe.lock().await;
        if let Some((at, result)) = last.as_ref()
            && at.elapsed() < LAKE_PROBE_EVERY
        {
            return result.clone();
        }
        let url = url.to_owned();
        // The lake runs on a runtime of its own, opened and dropped on a blocking thread.
        let probe = tokio::task::spawn_blocking(move || {
            kermodes_lake::Lake::open(&url)
                .and_then(|lake| lake.probe())
                .map_err(|e| e.to_string())
        });
        let result = match tokio::time::timeout(LAKE_PROBE_DEADLINE, probe).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!(
                "the store did not answer within {} s",
                LAKE_PROBE_DEADLINE.as_secs()
            )),
        };
        *last = Some((Instant::now(), result.clone()));
        result
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

/// Each table's past dates weir still holds, over Flight.
async fn past_dates(weir: String) -> Result<Vec<(String, Vec<kermodes_weir::Date>)>, String> {
    // The Flight client blocks on a runtime of its own.
    tokio::task::spawn_blocking(move || {
        let client = kermodes_flight::Client::connect(&weir).map_err(|e| e.to_string())?;
        client
            .tables()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(name, _)| {
                let dates = client.past_dates(&name).map_err(|e| e.to_string())?;
                Ok((name.to_string(), dates))
            })
            .collect::<Result<Vec<_>, String>>()
    })
    .await
    .map_err(|e| e.to_string())?
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
    let artifacts = config.artifacts.as_ref().map(Artifacts::open).transpose()?;
    let controller = Arc::new(Controller {
        state: Mutex::new(State::open(&config.database).await?),
        kube: Kube::connect(&config.namespace).await?,
        artifacts,
        config,
        unchanged: Mutex::new(HashMap::new()),
        lake_probe: Mutex::new(None),
        waiting: Mutex::new(HashMap::new()),
        http: reqwest::Client::new(),
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

/// Waits for the leader lock, then syncs, reconciles, moves runs off cordoned nodes, merges past
/// dates into history, and reports to `sys.deployments` until it is lost; returns why.
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
        () = merge_forever(controller) => unreachable!("merges until stopped"),
        () = report_forever(controller) => unreachable!("reports until stopped"),
        () = build_forever(controller) => unreachable!("builds until stopped"),
    }
}

/// How often the leader looks for past dates to merge.
const MERGE_EVERY: Duration = Duration::from_secs(60);

/// Starts a merge Job for each table's oldest past date weir still holds, as `plan::merges_due`
/// says, once a minute (§8.3).
async fn merge_forever(controller: &Controller) {
    let Some(history) = &controller.config.history else {
        return std::future::pending().await;
    };
    loop {
        if let Err(e) = merge_once(controller, history).await {
            eprintln!("kermodes control: merging: {e}");
        }
        tokio::time::sleep(MERGE_EVERY).await;
    }
}

async fn merge_once(controller: &Controller, history: &plan::History) -> Result<(), String> {
    let held = past_dates(controller.config.weir.clone()).await?;
    let jobs = controller.kube.merge_jobs().await?;
    let now = now_ns();
    for (table, date) in plan::merges_due(now, &held, &jobs) {
        let config = &controller.config;
        let job = plan::merge_job(
            history,
            &config.image,
            &config.weir,
            &config.pull_secrets,
            &table,
            date,
        );
        controller.kube.start(&job).await?;
        eprintln!("kermodes control: merging {table} {date}");
    }
    Ok(())
}

/// How often the leader settles finished builds and starts due ones.
const BUILD_EVERY: Duration = Duration::from_secs(5);

/// Builds each project's apps at its newest deployable commit and keeps what is served
/// (`plan::builds_due`), every few seconds.
async fn build_forever(controller: &Controller) {
    let (Some(artifacts), Some(builds)) = (&controller.artifacts, &controller.config.builds) else {
        return std::future::pending().await;
    };
    loop {
        for project in &controller.config.projects {
            if let Err(e) = build_once(controller, artifacts, builds, project).await {
                eprintln!("kermodes control: {}: building apps: {e}", project.name);
            }
        }
        tokio::time::sleep(BUILD_EVERY).await;
    }
}

async fn build_once(
    controller: &Controller,
    artifacts: &Artifacts,
    config: &plan::Builds,
    project: &ProjectConfig,
) -> Result<(), String> {
    let (latest, records) = {
        let mut state = controller.state.lock().await;
        (
            state.latest_discovered(&project.name).await?,
            state.builds(&project.name).await?,
        )
    };
    let Some(latest) = latest else { return Ok(()) };
    let manifest = Manifest::parse(latest.manifest.as_deref().unwrap_or_default())?;
    // A build ends with its Job; what it uploaded is what makes it built.
    for record in records.iter().filter(|r| r.state == Build::Building) {
        let job = plan::build_job_name(&project.name, &record.app, &record.commit);
        let Some(outcome) = controller.kube.job_outcome(&job, "build").await? else {
            continue;
        };
        let object = plan::app_object(&project.name, &record.app, &record.commit);
        let result = match outcome {
            Ok(_) if artifacts.exists(&object).await? => Ok(()),
            Ok(_) => Err("the build ended without uploading what it built".into()),
            Err(why) => Err(why),
        };
        match &result {
            Ok(()) => eprintln!(
                "kermodes control: {}: built {} at {}",
                project.name, record.app, record.commit
            ),
            Err(e) => eprintln!(
                "kermodes control: {}: building {} at {} failed: {e}",
                project.name, record.app, record.commit
            ),
        }
        controller
            .state
            .lock()
            .await
            .finish_build(&project.name, &record.app, &record.commit, &result)
            .await?;
    }
    let records = controller.state.lock().await.builds(&project.name).await?;
    let due = plan::builds_due(&manifest.apps, &latest.commit, &records);
    for app in &due.start {
        let object = plan::app_object(&project.name, app, &latest.commit);
        let ttl = Duration::from_secs(plan::BUILD_DEADLINE_SECONDS + 300);
        let upload = artifacts.upload_url(&object, ttl).await?;
        // Recorded first: a Job that never starts then reads as gone, and the build as failed.
        controller
            .state
            .lock()
            .await
            .start_build(&project.name, app, &latest.commit)
            .await?;
        let target = target(controller, project, &latest.commit);
        controller
            .kube
            .start(&plan::build_job(&target, app, config, &upload))
            .await?;
        eprintln!(
            "kermodes control: {}: building {app} at {}",
            project.name, latest.commit
        );
    }
    for (app, commit) in &due.remove {
        artifacts
            .delete(&plan::app_object(&project.name, app, commit))
            .await?;
        controller
            .state
            .lock()
            .await
            .remove_build(&project.name, app, commit)
            .await?;
    }
    Ok(())
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
        let builds = controller.state.lock().await.builds(&project.name).await?;
        let builds_apps = controller.artifacts.is_some() && controller.config.builds.is_some();
        for app in &manifest.apps {
            let (status, warning) = if builds_apps {
                plan::app_status(&app.name, &latest.commit, &builds)
            } else {
                let why = "this platform builds no apps: it has no artifacts store or builder";
                ("Not built", Some(why.to_owned()))
            };
            rows.push(report::Row::app(
                &project.name,
                &app.name,
                status,
                plan::served_build(&app.name, &builds),
                &latest.commit,
                warning,
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
            let checks = if project.checks {
                commit_checks(controller, project, &head).await?
            } else {
                plan::Checks::Passed
            };
            if checks == plan::Checks::Pending {
                // Looked at again at the next poll; nothing is fetched or recorded meanwhile.
                let mut waiting = controller.waiting.lock().await;
                if waiting.insert(project.name.clone(), head.clone()).as_ref() != Some(&head) {
                    eprintln!(
                        "kermodes control: {}: {head} waits for its checks",
                        project.name
                    );
                }
                drop(waiting);
                return reconcile(controller, project).await;
            }
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
            // Recorded as a commit that cannot deploy, as one whose discovery failed is.
            if let plan::Checks::Failed(why) = checks {
                eprintln!(
                    "kermodes control: {}: {} not deployable: {why}",
                    project.name, snapshot.commit
                );
                controller
                    .state
                    .lock()
                    .await
                    .discovered(
                        &project.name,
                        &snapshot.commit,
                        &controller.config.image,
                        &Err(why),
                    )
                    .await?;
                return reconcile(controller, project).await;
            }
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
            .discovered(&project.name, &commit, &controller.config.image, &result)
            .await?;
    }
    reconcile(controller, project).await
}

/// How `commit`'s checks stand, from the git host's combined commit status, read with the
/// project's git credentials.
async fn commit_checks(
    controller: &Controller,
    project: &ProjectConfig,
    commit: &str,
) -> Result<plan::Checks, String> {
    let url = plan::status_url(&project.url, commit)?;
    let mut request = controller
        .http
        .get(&url)
        .header("accept", "application/json")
        .header("user-agent", "kermodes-control");
    if let Some(credentials) = controller.config.credentials() {
        request = request.basic_auth(credentials.username, Some(credentials.password));
    }
    let response = request
        .send()
        .await
        .map_err(|e| format!("cannot read {commit}'s checks: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("{url} answered {}", response.status()));
    }
    let text = response
        .text()
        .await
        .map_err(|e| format!("cannot read {commit}'s checks: {e}"))?;
    let status: Value =
        serde_json::from_str(&text).map_err(|e| format!("{url} sent no status: {e}"))?;
    Ok(plan::checks_of(&status))
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

/// A manifest is what one platform reads of a commit. After an upgrade:
/// - the mainline's newest commit, if discovery failed under another platform, is tried again:
///   it may use what the new platform adds (a commit declaring sizes, read by a platform without
///   them), and if it now reads, it is what the project runs;
/// - the commit a project runs is read again, so what the new platform knows of it reaches its
///   runs; if the new platform cannot read it, the old manifest stays and the failure is logged.
///
/// Either way each commit is read once per platform.
async fn rediscover_if_stale(
    controller: &Controller,
    project: &ProjectConfig,
) -> Result<(), String> {
    let image = &controller.config.image;
    let stale = |c: &Commit| c.discovered_by.as_deref() != Some(image.as_str());
    let head = controller.state.lock().await.latest(&project.name).await?;
    if let Some(head) = head.filter(|c| c.error.is_some() && stale(c)) {
        let result = discover(controller, project, &head.commit).await;
        match &result {
            Ok(_) => eprintln!(
                "kermodes control: {}: {} reads under {image}",
                project.name, head.commit
            ),
            Err(e) => eprintln!(
                "kermodes control: {}: {} still not deployable under {image}: {e}",
                project.name, head.commit
            ),
        }
        controller
            .state
            .lock()
            .await
            .discovered(&project.name, &head.commit, image, &result)
            .await?;
    }
    let latest = controller
        .state
        .lock()
        .await
        .latest_discovered(&project.name)
        .await?;
    let Some(latest) = latest.filter(stale) else {
        return Ok(());
    };
    let result = discover(controller, project, &latest.commit).await;
    match &result {
        Ok(_) => eprintln!(
            "kermodes control: {}: {} read again by {image}",
            project.name, latest.commit
        ),
        Err(e) => eprintln!(
            "kermodes control: {}: {image} cannot read {}, which keeps its manifest: {e}",
            project.name, latest.commit
        ),
    }
    controller
        .state
        .lock()
        .await
        .rediscovered(&project.name, &latest.commit, image, result.ok().as_deref())
        .await
}

/// Brings a project's Deployments to its newest deployable commit, resource by resource.
async fn reconcile(controller: &Controller, project: &ProjectConfig) -> Result<(), String> {
    rediscover_if_stale(controller, project).await?;
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
                let source = match running {
                    Some(commit) => source(controller, project, commit, &resource.name).await?,
                    None => None,
                };
                resources.push(json!({
                    "name": resource.name,
                    "update": format!("{:?}", resource.update).to_lowercase(),
                    "commit": running,
                    "available": (running != Some(&latest.commit)).then_some(&latest.commit),
                    "rollout": format!("{rollout:?}").to_lowercase(),
                    "source": source,
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

/// The page showing where `resource` is declared at `commit` on the project's git host, or the
/// project's directory there when that commit's manifest does not say.
async fn source(
    controller: &Controller,
    project: &ProjectConfig,
    commit: &str,
    resource: &str,
) -> Result<Option<String>, String> {
    let stored = controller
        .state
        .lock()
        .await
        .commit(&project.name, commit)
        .await?;
    let defined_at = stored
        .and_then(|c| c.manifest)
        .and_then(|manifest| Manifest::parse(&manifest).ok())
        .and_then(|manifest| {
            let resource = manifest
                .resources
                .into_iter()
                .find(|r| r.name == resource)?;
            resource.defined_at
        });
    let directory = project.path.trim_matches('/');
    Ok(match defined_at {
        Some(at) => {
            let file = match directory {
                "" => at.file,
                directory => format!("{directory}/{}", at.file),
            };
            browse::source_url(&project.url, commit, &file, Some(at.line))
        }
        None => browse::source_url(&project.url, commit, directory, None),
    })
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
