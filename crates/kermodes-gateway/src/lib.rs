//! The gateway (architecture.md §4, §15.2): the web UI's server, and each app's. `serve` runs
//! the UI's: the built web app at `/`, and on `/ws` a WebSocket over which the app lists the
//! session's tables and opens them in the table viewer (§15.3). `serve_at` runs an app's own:
//! its built frontend and the same WebSocket under the app's path (`/apps/<project>/<name>/`),
//! which is where the cluster routes it. Authentication and Flight come with the cluster gateway
//! (M4).
//!
//! The tables are those published (`Server::publish`) and, given a store (`Stored`), every
//! stored table: listed from the catalog without reading rows, and opened live only while a
//! view shows it, so the server holds what is being looked at rather than everything.
//!
//! Messages from the app are JSON text: `{"type": "tables"}`, `{"type": "open", "view": id,
//! "table": name}`, `{"type": "series", "view": id, "table": name, "x": column, "y": [columns],
//! "where": expression, "points": n}` (a chart's points, `kermodes_core::series`; `where` and
//! `points` optional), `{"type": "close", "view": id}`, `{"type": "pause" | "resume", "view":
//! id}` (a paused view gets no windows until resumed, keeping its sort and filters), and the
//! viewer's own messages (`viewport`, `sort`, `filter`, `fetch`, as in `kermodes_core::viewer`)
//! with a `"view"` field. The server answers with JSON text (`tables`, which lists the tables
//! and names the views published for pages of their own; `state` for a view before each of its
//! windows; `error`, with `"retry": true` when the view has ended because its stored table could
//! not be read, as while the store restarts, and opening it again later may work) and binary
//! frames: a little-endian u32 header length, the header (JSON: `view`, `type` "rows",
//! "fetched", or "series", `id` for fetches, `start`, `total`, `cycle`), a u32 rows length, the
//! rows (Arrow IPC stream), then the rows' keys (little-endian u64 each; none for a series).
//! After every engine cycle, each open view whose table changed gets its window again; a
//! series at most every `SERIES_EVERY`, since each recomputes from all its table's rows.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequestParts, Path as UrlPath, RawQuery, Request as HttpRequest, State};
use axum::http::{HeaderMap, HeaderName, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use kermodes_core::series::Series;
use kermodes_core::viewer::{FETCH_LIMIT, Frame, Request, SortKey, Viewer};
use kermodes_core::{Session, Table, type_name};
use serde_json::{Value, json};
use tokio::sync::{oneshot, watch};

mod window;

pub use window::ReadRows;
use window::Window;

/// How long the cycle watcher waits before checking whether the server is stopping.
const CYCLE_WAIT: Duration = Duration::from_millis(500);
/// How long the controller may take to answer the programs page.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// How often each connection is pinged: proxies end connections idle for minutes (the cluster's
/// Envoy after five), and a hidden tab's paused views or a table that does not tick send nothing.
const KEEPALIVE: Duration = Duration::from_secs(30);
/// The least time between a series' frames: a chart redrawn twice a second looks live, and a
/// series of millions of rows costs tens of milliseconds a frame.
const SERIES_EVERY: Duration = Duration::from_millis(500);
/// How often the server refreshes the stored tables it lists and lets go of tables no view
/// shows.
const MAINTAIN_EVERY: Duration = Duration::from_secs(2);
/// How often views shown by position ask the store whether their table has grown.
const POLL_EVERY: Duration = Duration::from_millis(250);

/// A stored table as the app lists it: its name, and its columns' names and type names.
pub type Listed = (String, Vec<(String, String)>);

/// Lists every stored table.
pub type ListTables = Box<dyn Fn() -> Result<Vec<Listed>, String> + Send + Sync>;

/// Opens the stored table `name` live, fed by the store.
pub type OpenTable = Box<dyn Fn(&str) -> Result<Table, String> + Send + Sync>;

/// Whether the stored table `name` is shown by position: an append table's schema, or `None`
/// for a keyed table, whose rows are its keys' latest values (opened live instead).
pub type Positional = Box<dyn Fn(&str) -> Result<Option<SchemaRef>, String> + Send + Sync>;

/// What each stored table takes in the store and in history, and the store's volume, as JSON
/// (`kermodes_weir::Store::stats_json`).
pub type ReadStats = Box<dyn Fn() -> Result<Value, String> + Send + Sync>;

/// The stored tables a server can show, given by whoever holds the store connection: listed
/// for the app without reading their rows. An append table's view reads its window by position
/// and holds nothing else; a keyed table, or an append table being sorted or filtered, is opened
/// live only while a view needs it.
pub struct Stored {
    pub list: ListTables,
    pub open: OpenTable,
    pub positional: Positional,
    pub rows: ReadRows,
    pub stats: ReadStats,
    /// How long a table no view shows stays open, so a view opened again soon finds it at once.
    pub idle: Duration,
}

/// A running server; stops when dropped.
pub struct Server {
    addr: SocketAddr,
    shared: Arc<Shared>,
    shutdown: Option<oneshot::Sender<()>>,
    runtime: Option<JoinHandle<()>>,
    watcher: Option<JoinHandle<()>>,
    stopping: Arc<AtomicBool>,
}

struct Shared {
    /// Published tables by name, held until unpublished or the server stops.
    tables: Mutex<BTreeMap<String, Table>>,
    /// Views the app opens by name for a page of its own (the Deployments tab), not listed
    /// among the published tables.
    views: Mutex<BTreeMap<String, Table>>,
    /// Bumped when the listed tables change, so connections resend the list.
    catalog: watch::Sender<u64>,
    /// The last finished engine cycle.
    cycles: watch::Sender<u64>,
    /// The built web app (`index.html` and its assets), if any.
    app: Option<PathBuf>,
    /// Apps `kermodes dev` runs, by project and name: each one's dev server and process.
    apps: Mutex<BTreeMap<(String, String), AppServers>>,
    /// Requests to the controller and to apps' dev servers.
    client: Client<HttpConnector, Full<Bytes>>,
    /// `kermodes control`, whose API the programs page reads through this server.
    control: Option<Control>,
    stored: Option<Stored>,
    /// The stored tables last listed.
    listed: Mutex<Vec<Listed>>,
    /// Stored tables open for views, by name.
    live: Mutex<HashMap<String, Live>>,
    /// Numbers each opening of a stored table, so a view's hold on one let go (failed) never
    /// counts against the next.
    generations: AtomicU64,
}

/// Where `kermodes dev` runs an app: its dev server (Vite, the pages and hot reload) and its own
/// process (`kermodes run`, its tables on `<path>ws`). Both serve under the app's path.
#[derive(Clone)]
struct AppServers {
    dev: String,
    process: String,
}

struct Control {
    url: String,
}

/// A stored table open for views: how many hold it, and since when none has.
struct Live {
    table: Table,
    generation: u64,
    leases: usize,
    idle_since: Option<Instant>,
}

/// A view's hold on a stored table: the table stays open while any view holds it.
struct Lease {
    shared: Arc<Shared>,
    name: String,
    generation: u64,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let idle = self
            .shared
            .stored
            .as_ref()
            .map_or(Duration::ZERO, |s| s.idle);
        let mut live = self.shared.live();
        let Some(entry) = live
            .get_mut(&self.name)
            .filter(|entry| entry.generation == self.generation)
        else {
            return;
        };
        entry.leases -= 1;
        if entry.leases > 0 {
            return;
        }
        if idle.is_zero() {
            let gone = live.remove(&self.name);
            drop(live);
            drop(gone);
        } else {
            entry.idle_since = Some(Instant::now());
        }
    }
}

impl Shared {
    fn tables(&self) -> MutexGuard<'_, BTreeMap<String, Table>> {
        self.tables
            .lock()
            .expect("kermodes gateway tables poisoned by an earlier panic")
    }

    fn views(&self) -> MutexGuard<'_, BTreeMap<String, Table>> {
        self.views
            .lock()
            .expect("kermodes gateway views poisoned by an earlier panic")
    }

    fn apps(&self) -> MutexGuard<'_, BTreeMap<(String, String), AppServers>> {
        self.apps
            .lock()
            .expect("kermodes gateway apps poisoned by an earlier panic")
    }

    fn listed(&self) -> MutexGuard<'_, Vec<Listed>> {
        self.listed
            .lock()
            .expect("kermodes gateway listing poisoned by an earlier panic")
    }

    fn live(&self) -> MutexGuard<'_, HashMap<String, Live>> {
        self.live
            .lock()
            .expect("kermodes gateway live tables poisoned by an earlier panic")
    }

    /// Lets go of a stored table that failed, if it is still that opening of it, so the next
    /// view subscribes afresh.
    fn evict(&self, name: &str, generation: u64) {
        let mut live = self.live();
        if live.get(name).is_some_and(|e| e.generation == generation) {
            let gone = live.remove(name);
            drop(live);
            drop(gone);
        }
    }
}

/// Serves `session` on `addr` (port 0 picks a free one), with the web app in `app`, the programs
/// of `control` (`kermodes control`'s URL), and the tables of `stored`, if there are.
pub fn serve(
    session: Session,
    addr: SocketAddr,
    app: Option<PathBuf>,
    control: Option<String>,
    stored: Option<Stored>,
) -> io::Result<Server> {
    start(session, addr, "/", app, control, stored)
}

/// Serves an app's own process: its built frontend (`app`) and the session's tables, those it
/// publishes and those of `stored`, all under `base` (`/apps/<project>/<name>/`, ending in a
/// slash), where the cluster's route sends the app's requests unchanged.
pub fn serve_at(
    session: Session,
    addr: SocketAddr,
    base: &str,
    app: Option<PathBuf>,
    stored: Option<Stored>,
) -> io::Result<Server> {
    if !base.starts_with('/') || !base.ends_with('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("an app's path starts and ends with /, not {base}"),
        ));
    }
    start(session, addr, base, app, None, stored)
}

fn start(
    session: Session,
    addr: SocketAddr,
    base: &str,
    app: Option<PathBuf>,
    control: Option<String>,
    stored: Option<Stored>,
) -> io::Result<Server> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let shared = Arc::new(Shared {
        tables: Mutex::new(BTreeMap::new()),
        views: Mutex::new(BTreeMap::new()),
        catalog: watch::Sender::new(0),
        cycles: watch::Sender::new(0),
        app,
        apps: Mutex::new(BTreeMap::new()),
        client: Client::builder(TokioExecutor::new()).build_http(),
        control: control.map(|url| Control {
            url: url.trim_end_matches('/').to_owned(),
        }),
        stored,
        listed: Mutex::new(Vec::new()),
        live: Mutex::new(HashMap::new()),
        generations: AtomicU64::new(0),
    });
    let stopping = Arc::new(AtomicBool::new(false));
    let watcher = {
        let (shared, stopping) = (shared.clone(), stopping.clone());
        thread::Builder::new()
            .name("kermodes-gateway-cycles".into())
            .spawn(move || {
                let mut last = 0;
                while !stopping.load(Ordering::Acquire) {
                    let cycle = session.wait_cycle(last, CYCLE_WAIT);
                    if cycle > last {
                        last = cycle;
                        shared.cycles.send_replace(cycle);
                    }
                }
            })?
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("kermodes-gateway")
        .enable_all()
        .build()?;
    let (shutdown, stopped) = oneshot::channel::<()>();
    let router = if base == "/" {
        Router::new()
            .route("/ws", get(upgrade))
            .route("/api/projects", get(projects))
            .route("/api/catalog", get(catalog))
            .route("/api/components", get(components))
            .route("/api/components/{component}/{action}", get(component_get))
            .route(
                "/api/deployments/{project}/{resource}/{action}",
                get(deployment_get).post(deployment_post),
            )
            .route("/api/apps", get(apps_list))
            .route("/api/artifacts", get(artifacts))
            .route("/api/artifacts/prune", axum::routing::post(prune_artifacts))
            .route("/apps/{project}/{name}", get(app_slash))
            .route("/apps/{project}/{name}/", any(app_proxy))
            .route("/apps/{project}/{name}/{*path}", any(app_proxy))
            .route("/", get(index))
            .route("/{*path}", get(asset))
    } else {
        let slashless = base.trim_end_matches('/').to_owned();
        let page = base.to_owned();
        Router::new()
            .route(&format!("{base}ws"), get(upgrade))
            .route(
                &slashless,
                get(move || async move { axum::response::Redirect::permanent(&page) }),
            )
            .route(base, get(index))
            .route(&format!("{base}{{*path}}"), get(asset))
    }
    .with_state(shared.clone());
    let maintained = shared.clone();
    let runtime = thread::Builder::new()
        .name("kermodes-gateway".into())
        .spawn(move || {
            runtime.block_on(async move {
                tokio::spawn(maintain(maintained));
                let listener =
                    tokio::net::TcpListener::from_std(listener).expect("a bound listener converts");
                let _ = axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await;
            });
        })?;
    Ok(Server {
        addr,
        shared,
        shutdown: Some(shutdown),
        runtime: Some(runtime),
        watcher: Some(watcher),
        stopping,
    })
}

impl Server {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Makes `table` visible to the web app as `name`, replacing any table of that name. The
    /// server holds it until it is unpublished or the server stops.
    pub fn publish(&self, name: &str, table: &Table) {
        self.shared.tables().insert(name.to_owned(), table.clone());
        self.shared.catalog.send_modify(|v| *v += 1);
    }

    /// Removes the table or view `name`; whether there was one.
    pub fn unpublish(&self, name: &str) -> bool {
        let table = self.shared.tables().remove(name).is_some();
        let view = self.shared.views().remove(name).is_some();
        self.shared.catalog.send_modify(|v| *v += 1);
        table || view
    }

    pub fn published(&self) -> Vec<String> {
        self.shared.tables().keys().cloned().collect()
    }

    /// Serves `table` as the view `name`, which the app opens by name for a page of its own,
    /// without listing it among the published tables. The `tables` message names the views
    /// there are, so the app shows their pages.
    pub fn publish_view(&self, name: &str, table: &Table) {
        self.shared.views().insert(name.to_owned(), table.clone());
        self.shared.catalog.send_modify(|v| *v += 1);
    }

    /// Serves the app `name` of `project` at `/apps/<project>/<name>/`, as `kermodes dev` runs
    /// it: its tables' WebSocket (`<path>ws`) passes to its own process at `process`, and every
    /// other request and WebSocket (pages, hot reload) to its dev server at `dev`
    /// (`http://127.0.0.1:5173`), path and all, as the cluster's route sends them to its pod.
    pub fn serve_app(&self, project: &str, name: &str, dev: &str, process: &str) {
        let servers = AppServers {
            dev: dev.trim_end_matches('/').to_owned(),
            process: process.trim_end_matches('/').to_owned(),
        };
        self.shared
            .apps()
            .insert((project.to_owned(), name.to_owned()), servers);
    }

    /// Stops serving the app `name` of `project`; whether it was served.
    pub fn stop_app(&self, project: &str, name: &str) -> bool {
        self.shared
            .apps()
            .remove(&(project.to_owned(), name.to_owned()))
            .is_some()
    }

    /// Stops serving: open connections close, and published tables are let go.
    pub fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.stopping.store(true, Ordering::Release);
        for handle in [self.runtime.take(), self.watcher.take()]
            .into_iter()
            .flatten()
        {
            let _ = handle.join();
        }
        self.shared.tables().clear();
        self.shared.live().clear();
    }
}

/// Every `MAINTAIN_EVERY`: lists the stored tables again (telling connections when they
/// change; the last list stays while the store is away), and lets go of stored tables no view
/// has shown for the idle time, or that failed, so the next view subscribes afresh.
async fn maintain(shared: Arc<Shared>) {
    let Some(stored) = &shared.stored else {
        return;
    };
    let mut every = tokio::time::interval(MAINTAIN_EVERY);
    loop {
        every.tick().await;
        let lister = shared.clone();
        let listed =
            blocking(move || (lister.stored.as_ref().expect("checked above").list)()).await;
        if let Ok(listed) = listed {
            let changed = {
                let mut current = shared.listed();
                let changed = *current != listed;
                *current = listed;
                changed
            };
            if changed {
                shared.catalog.send_modify(|v| *v += 1);
            }
        }
        let now = Instant::now();
        let gone: Vec<Live> = {
            let mut live = shared.live();
            let names: Vec<String> = live
                .iter()
                .filter(|(_, e)| {
                    e.idle_since
                        .is_some_and(|since| now.duration_since(since) >= stored.idle)
                        || e.table.num_rows().is_err()
                })
                .map(|(name, _)| name.clone())
                .collect();
            names.iter().filter_map(|name| live.remove(name)).collect()
        };
        drop(gone);
    }
}

/// Why a stored table could not be opened for a view.
enum Refused {
    /// There is no such table.
    Missing,
    /// The store could not be read (it may be restarting): a later try may work.
    Unavailable(String),
}

/// The stored table `name` for a view, opened unless it is open already, and the view's hold
/// on it.
async fn lease(shared: &Arc<Shared>, name: &str) -> Result<(Table, Lease), Refused> {
    if shared.stored.is_none() || !shared.listed().iter().any(|(n, _)| n == name) {
        return Err(Refused::Missing);
    }
    let hold = |entry: &mut Live| {
        entry.leases += 1;
        entry.idle_since = None;
        let lease = Lease {
            shared: shared.clone(),
            name: name.to_owned(),
            generation: entry.generation,
        };
        (entry.table.clone(), lease)
    };
    if let Some(entry) = shared.live().get_mut(name) {
        return Ok(hold(entry));
    }
    // Opening waits on the store: off the async workers, without the lock.
    let opener = shared.clone();
    let wanted = name.to_owned();
    let table = blocking(move || (opener.stored.as_ref().expect("checked above").open)(&wanted))
        .await
        .map_err(Refused::Unavailable)?;
    let mut live = shared.live();
    // Another view may have opened it meanwhile: the first opening stays.
    let entry = live.entry(name.to_owned()).or_insert_with(|| Live {
        table,
        generation: shared.generations.fetch_add(1, Ordering::Relaxed),
        leases: 0,
        idle_since: None,
    });
    Ok(hold(entry))
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn index(State(shared): State<Arc<Shared>>) -> Response {
    file(&shared, Path::new("index.html")).await
}

/// Every project the controller syncs, with the commit it runs and any that failed discovery.
async fn projects(State(shared): State<Arc<Shared>>) -> Response {
    to_control(&shared, Method::GET, "/api/projects").await
}

/// Every stored table with what it takes in weir and in history, and weir's volume: the
/// Catalog tab's list (§15.2), read from the store, not from the tables' rows.
async fn catalog(State(shared): State<Arc<Shared>>) -> Response {
    if shared.stored.is_none() {
        return control_error(StatusCode::NOT_FOUND, "this server has no store");
    }
    let reader = shared.clone();
    match blocking(move || (reader.stored.as_ref().expect("checked above").stats)()).await {
        Ok(stats) => axum::Json(stats).into_response(),
        Err(e) => control_error(StatusCode::SERVICE_UNAVAILABLE, &e),
    }
}

/// The platform's components (weir, Postgres, the controller, the web UI), as Kubernetes shows
/// them: read from the controller, so the page can say weir is down while it is.
async fn components(State(shared): State<Arc<Shared>>) -> Response {
    to_control(&shared, Method::GET, "/api/components").await
}

/// The artifacts store's size and what each object is for.
async fn artifacts(State(shared): State<Arc<Shared>>) -> Response {
    to_control(&shared, Method::GET, "/api/artifacts").await
}

/// Deletes what nothing uses in the artifacts store.
async fn prune_artifacts(State(shared): State<Arc<Shared>>) -> Response {
    to_control(&shared, Method::POST, "/api/artifacts/prune").await
}

/// A component's `events`, or `logs` (`?pod=` one of its pods, `?tail=` lines).
async fn component_get(
    State(shared): State<Arc<Shared>>,
    UrlPath((component, action)): UrlPath<(String, String)>,
    RawQuery(query): RawQuery,
) -> Response {
    if !plain(&component) {
        return control_error(StatusCode::BAD_REQUEST, "not a component name");
    }
    // Only these parameters, re-encoded from their parsed values.
    let mut params = Vec::new();
    for (key, value) in query
        .as_deref()
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.split_once('='))
    {
        match key {
            "tail" if value.parse::<u32>().is_ok() => params.push(format!("tail={value}")),
            "pod" if plain(value) => params.push(format!("pod={value}")),
            _ => {}
        }
    }
    let path = match action.as_str() {
        "events" => format!("/api/components/{component}/events"),
        "logs" if params.is_empty() => format!("/api/components/{component}/logs"),
        "logs" => format!("/api/components/{component}/logs?{}", params.join("&")),
        _ => return control_error(StatusCode::NOT_FOUND, "no such component page"),
    };
    to_control(&shared, Method::GET, &path).await
}

/// A name, never a path: letters, digits, `-`, and `_`.
fn plain(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// A deployment's `events` or `logs` (`?tail=` lines).
async fn deployment_get(
    State(shared): State<Arc<Shared>>,
    UrlPath((project, resource, action)): UrlPath<(String, String, String)>,
    RawQuery(query): RawQuery,
) -> Response {
    let tail = query
        .as_deref()
        .and_then(|q| q.strip_prefix("tail="))
        .and_then(|t| t.parse::<u32>().ok());
    match (action.as_str(), tail) {
        ("events", _) => to_deployment(&shared, Method::GET, &project, &resource, "events").await,
        ("logs", None) => to_deployment(&shared, Method::GET, &project, &resource, "logs").await,
        ("logs", Some(tail)) => {
            let action = format!("logs?tail={tail}");
            to_deployment(&shared, Method::GET, &project, &resource, &action).await
        }
        _ => control_error(StatusCode::NOT_FOUND, "no such deployment page"),
    }
}

/// Rolls a deployment out to its project's newest deployable commit (`rollout`, for
/// `update="manual"`), starts it afresh at the same commit (`restart`), or stops or starts it.
async fn deployment_post(
    State(shared): State<Arc<Shared>>,
    UrlPath((project, resource, action)): UrlPath<(String, String, String)>,
) -> Response {
    match action.as_str() {
        "rollout" | "restart" | "stop" | "start" => {
            to_deployment(&shared, Method::POST, &project, &resource, &action).await
        }
        _ => control_error(StatusCode::NOT_FOUND, "no such deployment action"),
    }
}

async fn to_deployment(
    shared: &Shared,
    method: Method,
    project: &str,
    resource: &str,
    action: &str,
) -> Response {
    // Names, never paths: nothing else of the controller's API is reachable from here.
    if !plain(project) || !plain(resource) {
        return control_error(StatusCode::BAD_REQUEST, "not a project and resource name");
    }
    let path = format!("/api/deployments/{project}/{resource}/{action}");
    to_control(shared, method, &path).await
}

/// Passes a request to the controller and its answer back.
async fn to_control(shared: &Shared, method: Method, path: &str) -> Response {
    let Some(control) = &shared.control else {
        return control_error(
            StatusCode::NOT_FOUND,
            "no controller: deployments run under `kermodes control`",
        );
    };
    let request = axum::http::Request::builder()
        .method(method)
        .uri(format!("{}{path}", control.url))
        .body(Full::new(Bytes::new()))
        .expect("a request from a parsed URL");
    match tokio::time::timeout(CONTROL_TIMEOUT, shared.client.request(request)).await {
        Ok(Ok(response)) => {
            let (parts, body) = response.into_parts();
            Response::from_parts(parts, Body::new(body))
        }
        Ok(Err(e)) => control_error(
            StatusCode::BAD_GATEWAY,
            &format!("cannot reach the controller at {}: {e}", control.url),
        ),
        Err(_) => control_error(
            StatusCode::GATEWAY_TIMEOUT,
            &format!("the controller at {} did not answer", control.url),
        ),
    }
}

fn control_error(status: StatusCode, message: &str) -> Response {
    (status, axum::Json(json!({"error": message}))).into_response()
}

async fn asset(State(shared): State<Arc<Shared>>, UrlPath(path): UrlPath<String>) -> Response {
    let path = Path::new(&path);
    // Only plain relative paths inside the app directory.
    if !path.components().all(|c| matches!(c, Component::Normal(_))) {
        return StatusCode::NOT_FOUND.into_response();
    }
    file(&shared, path).await
}

async fn file(shared: &Shared, path: &Path) -> Response {
    const NOT_BUILT: &str = "the web app is not built: run `bun run build` in web/";
    let Some(app) = &shared.app else {
        return (StatusCode::NOT_FOUND, NOT_BUILT).into_response();
    };
    // Read on every request, so a build after the server started is served at once.
    match tokio::fs::read(app.join(path)).await {
        Ok(bytes) => ([(header::CONTENT_TYPE, content_type(path))], bytes).into_response(),
        Err(_) if path == Path::new("index.html") => {
            (StatusCode::NOT_FOUND, NOT_BUILT).into_response()
        }
        // A path with no extension is a page of the app's own router: its index answers it.
        Err(_) if path.extension().is_none() => {
            Box::pin(file(shared, Path::new("index.html"))).await
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// The apps this server runs dev servers for, by project and name.
async fn apps_list(State(shared): State<Arc<Shared>>) -> Response {
    let apps: Vec<Value> = shared
        .apps()
        .keys()
        .map(|(project, name)| json!({"project": project, "name": name}))
        .collect();
    axum::Json(apps).into_response()
}

/// An app's page needs the trailing slash, so its relative assets resolve under it.
async fn app_slash(UrlPath((project, name)): UrlPath<(String, String)>) -> Response {
    if !plain(&project) || !plain(&name) {
        return StatusCode::NOT_FOUND.into_response();
    }
    axum::response::Redirect::permanent(&format!("/apps/{project}/{name}/")).into_response()
}

/// A request for an app that `kermodes dev` runs: its tables' WebSocket passes to its process,
/// anything else to its dev server, with its path and query as they came (both serve under
/// `/apps/<project>/<name>/`), WebSockets with the subprotocol asked for. In the cluster, the
/// app's own route sends its requests to its pod, so any that reach this server name no app.
async fn app_proxy(State(shared): State<Arc<Shared>>, request: HttpRequest) -> Response {
    let uri = request.uri();
    let app = uri.path().strip_prefix("/apps/").and_then(|rest| {
        let mut parts = rest.splitn(3, '/');
        Some((parts.next()?.to_owned(), parts.next()?.to_owned()))
    });
    let Some((project, name)) = app.filter(|(project, name)| plain(project) && plain(name)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let tables = uri.path() == format!("/apps/{project}/{name}/ws");
    let Some(servers) = shared.apps().get(&(project, name)).cloned() else {
        return (StatusCode::NOT_FOUND, "no such app runs here").into_response();
    };
    let upstream = if tables { servers.process } else { servers.dev };
    let target = format!(
        "{upstream}{}",
        uri.path_and_query().map_or(uri.path(), |p| p.as_str())
    );
    let (mut parts, body) = request.into_parts();
    let wants_socket = parts
        .headers
        .get(header::UPGRADE)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
    if wants_socket {
        return match WebSocketUpgrade::from_request_parts(&mut parts, &()).await {
            Ok(ws) => app_socket(ws, &parts.headers, &target).await,
            Err(rejection) => rejection.into_response(),
        };
    }
    let Ok(body) = body.collect().await.map(|b| b.to_bytes()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let mut forwarded = axum::http::Request::builder()
        .method(parts.method)
        .uri(&target);
    for (name, value) in &parts.headers {
        if name != header::HOST && !hop_by_hop(name) {
            forwarded = forwarded.header(name, value);
        }
    }
    let Ok(forwarded) = forwarded.body(Full::new(body)) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match shared.client.request(forwarded).await {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            Response::from_parts(parts, Body::new(body))
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            format!("the app's dev server at {upstream} did not answer: {e}"),
        )
            .into_response(),
    }
}

/// Headers about one connection, which a proxy does not pass on.
fn hop_by_hop(name: &HeaderName) -> bool {
    [
        header::CONNECTION,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ]
    .contains(name)
        || name.as_str() == "keep-alive"
}

async fn app_socket(ws: WebSocketUpgrade, headers: &HeaderMap, target: &str) -> Response {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let Ok(mut request) = target.replacen("http", "ws", 1).into_client_request() else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if let Some(protocols) = headers.get(header::SEC_WEBSOCKET_PROTOCOL) {
        request
            .headers_mut()
            .insert(header::SEC_WEBSOCKET_PROTOCOL, protocols.clone());
    }
    let (upstream, response) = match tokio_tungstenite::connect_async(request).await {
        Ok(connected) => connected,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("the app's dev server refused the WebSocket: {e}"),
            )
                .into_response();
        }
    };
    let chosen = response
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let ws = match chosen {
        Some(protocol) => ws.protocols([protocol]),
        None => ws,
    };
    ws.on_upgrade(move |socket| relay(socket, upstream))
}

type Upstream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Passes messages both ways until either side closes.
async fn relay(mut browser: WebSocket, mut upstream: Upstream) {
    use tokio_tungstenite::tungstenite::{Message as Up, protocol::CloseFrame as UpClose};
    loop {
        tokio::select! {
            message = browser.recv() => {
                let Some(Ok(message)) = message else { break };
                let closing = matches!(message, Message::Close(_));
                let message = match message {
                    Message::Text(text) => Up::text(text.as_str()),
                    Message::Binary(bytes) => Up::Binary(bytes),
                    Message::Ping(bytes) => Up::Ping(bytes),
                    Message::Pong(bytes) => Up::Pong(bytes),
                    Message::Close(frame) => Up::Close(frame.map(|f| UpClose {
                        code: f.code.into(),
                        reason: f.reason.as_str().into(),
                    })),
                };
                if upstream.send(message).await.is_err() || closing {
                    break;
                }
            }
            message = upstream.next() => {
                let Some(Ok(message)) = message else { break };
                let closing = matches!(message, Up::Close(_));
                let message = match message {
                    Up::Text(text) => Message::text(text.as_str()),
                    Up::Binary(bytes) => Message::Binary(bytes),
                    Up::Ping(bytes) => Message::Ping(bytes),
                    Up::Pong(bytes) => Message::Pong(bytes),
                    Up::Close(frame) => Message::Close(frame.map(|f| CloseFrame {
                        code: f.code.into(),
                        reason: f.reason.as_str().into(),
                    })),
                    Up::Frame(_) => continue,
                };
                if browser.send(message).await.is_err() || closing {
                    break;
                }
            }
        }
    }
}

fn content_type(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}

async fn upgrade(ws: WebSocketUpgrade, State(shared): State<Arc<Shared>>) -> Response {
    ws.on_upgrade(move |socket| connection(socket, shared))
}

/// An open view. A stored append table is shown by position (`window`) unless it is sorted or
/// filtered; the engine's `viewer` shows everything else: published tables, keyed stored
/// tables, and an append table while it is sorted or filtered, held live by `lease` meanwhile. A
/// chart's view is a `series` instead, of a published table or a stored one held by `lease`.
struct Open {
    viewer: Option<Arc<Mutex<Viewer>>>,
    window: Option<Window>,
    series: Option<Arc<Mutex<Series>>>,
    /// What the window last sent reflects: the engine view's cycle, or a window's row count.
    sent: Option<u64>,
    /// When a series last sent its points.
    sent_at: Option<Instant>,
    paused: bool,
    lease: Option<Lease>,
    /// The highest sort or filter number applied while shown by position.
    applied: u64,
}

/// A view's reply: its frame, `None` once its table is gone, or why it failed.
type Reply = Result<Option<Frame>, String>;

impl Open {
    fn new() -> Self {
        Self {
            viewer: None,
            window: None,
            series: None,
            sent: None,
            sent_at: None,
            paused: false,
            lease: None,
            applied: 0,
        }
    }

    fn read(shared: &Shared) -> &ReadRows {
        &shared
            .stored
            .as_ref()
            .expect("a window shows a stored table")
            .rows
    }

    /// Whether the engine says when this view changes (after each cycle), rather than a poll.
    fn engine(&self) -> bool {
        self.viewer.is_some() || self.series.is_some()
    }

    /// The window as the view shows it now.
    async fn rows(&self, shared: &Arc<Shared>) -> Reply {
        if let Some(series) = &self.series {
            let series = series.clone();
            return blocking(move || lock(&series).frame())
                .await
                .map_err(|e| e.to_string());
        }
        match (&self.viewer, &self.window) {
            (Some(viewer), _) => {
                let viewer = viewer.clone();
                blocking(move || lock(&viewer).rows())
                    .await
                    .map_err(|e| e.to_string())
            }
            (None, Some(window)) => {
                let (window, shared) = (window.clone(), shared.clone());
                blocking(move || window.rows(Open::read(&shared)).map(Some)).await
            }
            (None, None) => unreachable!("a view shows a viewer or a window"),
        }
    }

    /// What says the window may have changed: an engine view's last changing cycle, or a
    /// window's row count; `None` once the table is gone or cannot be read.
    async fn changed(&self, shared: &Arc<Shared>) -> Option<u64> {
        if let Some(series) = &self.series {
            let series = series.clone();
            return blocking(move || lock(&series).changed()).await;
        }
        match (&self.viewer, &self.window) {
            (Some(viewer), _) => {
                let viewer = viewer.clone();
                blocking(move || lock(&viewer).changed()).await
            }
            (None, Some(window)) => {
                let (name, shared) = (window.name.clone(), shared.clone());
                blocking(move || {
                    Open::read(&shared)(&name, 0, 0)
                        .ok()
                        .map(|(total, _)| total)
                })
                .await
            }
            (None, None) => None,
        }
    }

    /// Answers a viewer message. A sort or filter of a window opens its table live, as only
    /// the engine sorts and filters; once neither applies, the view goes back to its window and
    /// lets the table go.
    async fn receive(&mut self, shared: &Arc<Shared>, request: Request) -> Reply {
        if self.series.is_some() {
            return Err("a chart's view is not sorted, filtered, or scrolled".to_owned());
        }
        if let (Request::Viewport { top, bottom }, Some(window)) = (&request, &mut self.window) {
            window.set_viewport(*top, *bottom);
        }
        if let Some(viewer) = self.viewer.clone() {
            let (reply, plain, applied) = blocking(move || {
                let mut viewer = lock(&viewer);
                let reply = viewer.receive(request);
                let plain = viewer.sort().is_empty() && viewer.filters().is_empty();
                (reply, plain, viewer.applied())
            })
            .await;
            if plain && self.window.is_some() && reply.is_ok() {
                (self.viewer, self.lease, self.applied) = (None, None, applied);
                return self.rows(shared).await;
            }
            return reply.map_err(|e| e.to_string());
        }
        let window = self
            .window
            .clone()
            .expect("a view shows a viewer or a window");
        match request {
            Request::Viewport { .. } => self.rows(shared).await,
            Request::Fetch { id, start, count } => {
                let shared = shared.clone();
                let count = count.min(FETCH_LIMIT) as u64;
                blocking(move || {
                    window
                        .fetch(Open::read(&shared), id, start as u64, count)
                        .map(Some)
                })
                .await
            }
            Request::Sort { by, number } if by.is_empty() => {
                self.applied = self.applied.max(number);
                self.rows(shared).await
            }
            Request::Filter { filters, number }
                if filters.values().all(|text| text.trim().is_empty()) =>
            {
                self.applied = self.applied.max(number);
                self.rows(shared).await
            }
            request => {
                let (table, lease) = match lease(shared, &window.name).await {
                    Ok(held) => held,
                    Err(Refused::Missing) => return Ok(None),
                    Err(Refused::Unavailable(why)) => return Err(why),
                };
                let viewer = Arc::new(Mutex::new(blocking(move || Viewer::new(&table)).await));
                let (held, viewport) = (viewer.clone(), window.viewport);
                let reply = blocking(move || {
                    let mut viewer = lock(&held);
                    if let Some((top, bottom)) = viewport {
                        viewer.receive(Request::Viewport { top, bottom })?;
                    }
                    viewer.receive(request)
                })
                .await;
                (self.viewer, self.lease) = (Some(viewer), Some(lease));
                reply.map_err(|e| e.to_string())
            }
        }
    }
}

/// The messages for a view's reply, and whether the view goes on: one showing a stored table
/// that failed (or was let go) ends with an error the app retries, and lets the table go so a
/// retry subscribes afresh.
fn deliver(shared: &Shared, id: u64, open: &mut Open, reply: Reply) -> (Vec<Message>, bool) {
    let stored = open
        .lease
        .as_ref()
        .map(|lease| lease.name.clone())
        .or_else(|| open.window.as_ref().map(|window| window.name.clone()));
    if let Some(name) = stored
        && !matches!(reply, Ok(Some(_)))
    {
        if let Some(lease) = &open.lease {
            shared.evict(&lease.name, lease.generation);
        }
        let why = match reply {
            Err(e) => e,
            _ => format!("{name} is gone"),
        };
        return (vec![retry(id, &why)], false);
    }
    if let Ok(Some(frame)) = &reply
        && frame.fetched.is_none()
    {
        open.sent = Some(frame.cycle);
        open.sent_at = Some(Instant::now());
    }
    (answer(id, open, reply), true)
}

/// Sends each view whose window may have changed its window again: engine views after a cycle
/// (`engine`), views shown by position when their table has grown.
async fn refresh(
    shared: &Arc<Shared>,
    views: &mut HashMap<u64, Open>,
    outgoing: &mut Vec<Message>,
    engine: bool,
) {
    let mut ended = Vec::new();
    for (id, open) in views.iter_mut() {
        // A series is checked on the poll too: one held back by `SERIES_EVERY` goes out then.
        if open.paused || (open.series.is_none() && open.engine() != engine) {
            continue;
        }
        if open.series.is_some() && open.sent_at.is_some_and(|at| at.elapsed() < SERIES_EVERY) {
            continue;
        }
        let changed = open.changed(shared).await;
        // Unchanged, or a published table let go (which keeps today's quiet).
        if changed == open.sent
            || (changed.is_none() && open.lease.is_none() && open.window.is_none())
        {
            continue;
        }
        let reply = open.rows(shared).await;
        let (messages, goes_on) = deliver(shared, *id, open, reply);
        outgoing.extend(messages);
        if !goes_on {
            ended.push(*id);
        }
    }
    for id in ended {
        views.remove(&id);
    }
}

async fn connection(mut socket: WebSocket, shared: Arc<Shared>) {
    let mut views: HashMap<u64, Open> = HashMap::new();
    let mut cycles = shared.cycles.subscribe();
    let mut catalog = shared.catalog.subscribe();
    let mut outgoing = vec![tables(&shared)];
    let mut keepalive =
        tokio::time::interval_at(tokio::time::Instant::now() + KEEPALIVE, KEEPALIVE);
    let mut poll = tokio::time::interval(POLL_EVERY);
    loop {
        for message in outgoing.drain(..) {
            if let Err(e) = socket.send(message).await {
                eprintln!("kermodes ui: a connection ended: cannot send: {e}");
                return;
            }
        }
        tokio::select! {
            received = socket.recv() => match received {
                Some(Ok(Message::Text(text))) => {
                    outgoing = handle(&shared, &mut views, &text).await;
                }
                Some(Ok(Message::Close(_))) | None => return,
                Some(Err(e)) => {
                    eprintln!("kermodes ui: a connection ended: {e}");
                    return;
                }
                Some(Ok(_)) => {}
            },
            Ok(()) = cycles.changed() => {
                cycles.borrow_and_update();
                refresh(&shared, &mut views, &mut outgoing, true).await;
            },
            _ = poll.tick() => {
                refresh(&shared, &mut views, &mut outgoing, false).await;
            },
            Ok(()) = catalog.changed() => {
                catalog.borrow_and_update();
                outgoing.push(tables(&shared));
            },
            _ = keepalive.tick() => {
                outgoing.push(Message::Ping(Default::default()));
            },
        }
    }
}

/// The replies to one message from the app.
async fn handle(shared: &Arc<Shared>, views: &mut HashMap<u64, Open>, text: &str) -> Vec<Message> {
    let message: Value = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(e) => return vec![error(None, &format!("not JSON: {e}"))],
    };
    let kind = message["type"].as_str().unwrap_or_default();
    let view = message["view"].as_u64();
    match (kind, view) {
        ("tables", _) => vec![tables(shared)],
        ("open", Some(id)) => {
            let name = message["table"].as_str().unwrap_or_default().to_owned();
            let published = shared.tables().get(&name).cloned();
            let published = published.or_else(|| shared.views().get(&name).cloned());
            let mut open = Open::new();
            if let Some(table) = published {
                open.viewer = Some(Arc::new(Mutex::new(
                    blocking(move || Viewer::new(&table)).await,
                )));
            } else {
                if shared.stored.is_none() || !shared.listed().iter().any(|(n, _)| *n == name) {
                    return vec![error(Some(id), &format!("no table `{name}`"))];
                }
                let (asker, wanted) = (shared.clone(), name.clone());
                let positional = blocking(move || {
                    (asker.stored.as_ref().expect("checked above").positional)(&wanted)
                })
                .await;
                match positional {
                    Ok(Some(schema)) => open.window = Some(Window::new(name, schema)),
                    Ok(None) => match lease(shared, &name).await {
                        Ok((table, lease)) => {
                            open.viewer = Some(Arc::new(Mutex::new(
                                blocking(move || Viewer::new(&table)).await,
                            )));
                            open.lease = Some(lease);
                        }
                        Err(Refused::Missing) => {
                            return vec![error(Some(id), &format!("no table `{name}`"))];
                        }
                        Err(Refused::Unavailable(why)) => return vec![retry(id, &why)],
                    },
                    Err(why) => return vec![retry(id, &why)],
                }
            }
            let rows = open.rows(shared).await;
            let (messages, goes_on) = deliver(shared, id, &mut open, rows);
            if goes_on {
                views.insert(id, open);
            }
            messages
        }
        ("series", Some(id)) => {
            let name = message["table"].as_str().unwrap_or_default().to_owned();
            let chart = match Chart::parse(&message) {
                Ok(chart) => chart,
                Err(e) => return vec![error(Some(id), &e)],
            };
            let mut open = Open::new();
            let published = shared.tables().get(&name).cloned();
            let table = match published.or_else(|| shared.views().get(&name).cloned()) {
                Some(table) => table,
                None => match lease(shared, &name).await {
                    Ok((table, lease)) => {
                        open.lease = Some(lease);
                        table
                    }
                    Err(Refused::Missing) => {
                        return vec![error(Some(id), &format!("no table `{name}`"))];
                    }
                    Err(Refused::Unavailable(why)) => return vec![retry(id, &why)],
                },
            };
            let built = blocking(move || {
                Series::new(
                    &table,
                    &chart.x,
                    &chart.y,
                    chart.filter.as_deref(),
                    chart.points,
                )
            })
            .await;
            match built {
                Ok(series) => open.series = Some(Arc::new(Mutex::new(series))),
                // A column it cannot draw or a `where` that does not compile: no retry helps.
                Err(e) => return vec![error(Some(id), &e.to_string())],
            }
            let reply = open.rows(shared).await;
            let (messages, goes_on) = deliver(shared, id, &mut open, reply);
            if goes_on {
                views.insert(id, open);
            }
            messages
        }
        ("close", Some(id)) => {
            views.remove(&id);
            Vec::new()
        }
        ("pause", Some(id)) => {
            if let Some(open) = views.get_mut(&id) {
                open.paused = true;
            }
            Vec::new()
        }
        ("resume", Some(id)) => {
            let Some(open) = views.get_mut(&id) else {
                return vec![error(Some(id), "no such view")];
            };
            open.paused = false;
            let reply = open.rows(shared).await;
            let (messages, goes_on) = deliver(shared, id, open, reply);
            if !goes_on {
                views.remove(&id);
            }
            messages
        }
        (_, Some(id)) => {
            let Some(open) = views.get_mut(&id) else {
                return vec![error(Some(id), "no such view")];
            };
            let request = match request(kind, &message) {
                Ok(Some(request)) => request,
                Ok(None) => return vec![error(Some(id), &format!("unknown message `{kind}`"))],
                Err(e) => return vec![error(Some(id), &e)],
            };
            let reply = open.receive(shared, request).await;
            let (messages, goes_on) = deliver(shared, id, open, reply);
            if !goes_on {
                views.remove(&id);
            }
            messages
        }
        _ => vec![error(None, &format!("unknown message `{kind}`"))],
    }
}

/// What a `series` message asks to chart.
struct Chart {
    x: String,
    y: Vec<String>,
    filter: Option<String>,
    points: usize,
}

impl Chart {
    /// Points when a chart does not say: about what a chart a few hundred pixels wide draws.
    const POINTS: usize = 2000;

    fn parse(message: &Value) -> Result<Self, String> {
        let x = message["x"].as_str().ok_or("`series` needs a column `x`")?;
        let y = match &message["y"] {
            Value::String(column) => vec![column.clone()],
            Value::Array(columns) => columns
                .iter()
                .map(|c| c.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or("`series` takes `y` as column names")?,
            _ => return Err("`series` needs a column or columns `y`".to_owned()),
        };
        Ok(Self {
            x: x.to_owned(),
            y,
            filter: message["where"].as_str().map(str::to_owned),
            points: message["points"]
                .as_u64()
                .map_or(Self::POINTS, |n| n as usize),
        })
    }
}

/// A viewer message, as `kermodes_core::viewer::Request`; `None` for other types.
fn request(kind: &str, message: &Value) -> Result<Option<Request>, String> {
    let number = |name: &str| {
        message[name]
            .as_u64()
            .map(|n| n as usize)
            .ok_or_else(|| format!("`{kind}` needs a number `{name}`"))
    };
    // A client before applied numbers sends none: 0, which never reads as pending.
    let request = message["request"].as_u64().unwrap_or(0);
    Ok(Some(match kind {
        "viewport" => Request::Viewport {
            top: number("top")?,
            bottom: number("bottom")?,
        },
        "sort" => Request::Sort {
            by: message["by"]
                .as_array()
                .ok_or("`sort` needs a list `by`")?
                .iter()
                .map(|key| {
                    Ok(SortKey {
                        column: key["column"]
                            .as_str()
                            .ok_or("a sort key needs a `column`")?
                            .to_owned(),
                        descending: key["descending"].as_bool().unwrap_or(false),
                    })
                })
                .collect::<Result<_, &str>>()?,
            number: request,
        },
        "filter" => Request::Filter {
            filters: message["filters"]
                .as_object()
                .ok_or("`filter` needs an object `filters`")?
                .iter()
                .map(|(column, text)| {
                    (column.clone(), text.as_str().unwrap_or_default().to_owned())
                })
                .collect(),
            number: request,
        },
        "fetch" => Request::Fetch {
            id: number("id")? as u64,
            start: number("start")?,
            count: number("count")?,
        },
        _ => return Ok(None),
    }))
}

/// A view's frame as messages: its state (for windows), then the binary frame; or the error.
fn answer(id: u64, open: &Open, reply: Reply) -> Vec<Message> {
    match reply {
        Ok(Some(frame)) => {
            let mut messages = Vec::with_capacity(2);
            if frame.fetched.is_none() {
                messages.push(state(id, open, &frame));
            }
            let kind = match (&frame.fetched, &open.series) {
                (Some(_), _) => "fetched",
                (None, Some(_)) => "series",
                (None, None) => "rows",
            };
            messages.push(binary(id, kind, frame));
            messages
        }
        Ok(None) => vec![error(Some(id), "the table is gone")],
        Err(e) => vec![error(Some(id), &e)],
    }
}

fn state(id: u64, open: &Open, frame: &Frame) -> Message {
    let status = |status: String| {
        json!({
            "type": "state",
            "view": id,
            "row_count": frame.total,
            "status": status,
            "sort": [],
            "filters": {},
            "filter_errors": {},
            "applied": open.applied,
        })
    };
    let state = match (&open.series, &open.viewer) {
        (Some(series), _) => status(lock(series).status(frame.cycle)),
        (None, Some(viewer)) => {
            let viewer = lock(viewer);
            let sort: Vec<Value> = viewer
                .sort()
                .iter()
                .map(|k| json!({"column": k.column, "descending": k.descending}))
                .collect();
            json!({
                "type": "state",
                "view": id,
                "row_count": frame.total,
                "status": viewer.status(frame.cycle),
                "sort": sort,
                "filters": viewer.filters(),
                "filter_errors": viewer.filter_errors(),
                "applied": viewer.applied(),
            })
        }
        (None, None) => status("ticking".to_owned()),
    };
    Message::Text(state.to_string().into())
}

fn binary(id: u64, kind: &str, frame: Frame) -> Message {
    let mut head = json!({
        "view": id,
        "type": kind,
        "start": frame.start,
        "total": frame.total,
        "cycle": frame.cycle,
    });
    if let Some(fetch) = frame.fetched {
        head["id"] = fetch.into();
    }
    let head = head.to_string();
    let mut bytes = Vec::with_capacity(8 + head.len() + frame.rows.len() + frame.keys.len());
    bytes.extend_from_slice(&(head.len() as u32).to_le_bytes());
    bytes.extend_from_slice(head.as_bytes());
    bytes.extend_from_slice(&(frame.rows.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&frame.rows);
    bytes.extend_from_slice(&frame.keys);
    Message::Binary(bytes.into())
}

fn tables(shared: &Shared) -> Message {
    let published = shared.tables();
    let mut tables: Vec<(String, Value)> = published
        .iter()
        .map(|(name, table)| {
            let columns: Vec<Value> = table
                .schema()
                .fields()
                .iter()
                .map(|f| json!({"name": f.name(), "type": type_name(f.data_type())}))
                .collect();
            let info =
                json!({"name": name, "columns": columns, "refreshing": table.is_refreshing()});
            (name.clone(), info)
        })
        .collect();
    for (name, columns) in shared.listed().iter() {
        if !published.contains_key(name) {
            let columns: Vec<Value> = columns
                .iter()
                .map(|(column, kind)| json!({"name": column, "type": kind}))
                .collect();
            let info = json!({"name": name, "columns": columns, "refreshing": true});
            tables.push((name.clone(), info));
        }
    }
    drop(published);
    tables.sort_by(|a, b| a.0.cmp(&b.0));
    let tables: Vec<Value> = tables.into_iter().map(|(_, info)| info).collect();
    let views: Vec<String> = shared.views().keys().cloned().collect();
    Message::Text(
        json!({"type": "tables", "tables": tables, "views": views})
            .to_string()
            .into(),
    )
}

/// An error ending view `id` that opening it again later may not have: its stored table could
/// not be read.
fn retry(id: u64, message: &str) -> Message {
    Message::Text(
        json!({"type": "error", "view": id, "message": message, "retry": true})
            .to_string()
            .into(),
    )
}

fn error(view: Option<u64>, message: &str) -> Message {
    Message::Text(
        json!({"type": "error", "view": view, "message": message})
            .to_string()
            .into(),
    )
}

/// A view's engine side: a viewer or a series.
fn lock<T>(view: &Mutex<T>) -> MutexGuard<'_, T> {
    view.lock()
        .expect("kermodes view poisoned by an earlier panic")
}

/// Runs engine work (which takes the session's locks) off the async workers.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .expect("kermodes gateway engine call panicked")
}
