//! The gateway (architecture.md §4, §15.2): the web UI's server. This first slice serves one
//! local session: the built web app, and on `/ws` a WebSocket over which the app lists the
//! session's published tables and opens them in the table viewer (§15.3). Authentication,
//! routing to worker pods, and Flight come with the cluster gateway (M4).
//!
//! Messages from the app are JSON text: `{"type": "tables"}`, `{"type": "open", "view": id,
//! "table": name}`, `{"type": "close", "view": id}`, and the viewer's own messages (`viewport`,
//! `sort`, `filter`, `fetch`, as in `kermodes_core::viewer`) with a `"view"` field. The server
//! answers with JSON text (`tables`, `state` for a view before each of its windows, `error`)
//! and binary frames: a little-endian u32 header length, the header (JSON: `view`, `type`
//! "rows" or "fetched", `id` for fetches, `start`, `total`, `cycle`), a u32 rows length, the
//! rows (Arrow IPC stream), then the rows' keys (little-endian u64 each). After every engine
//! cycle, each open view whose table changed gets its window again.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as UrlPath, RawQuery, State};
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use kermodes_core::viewer::{Frame, Request, SortKey, Viewer};
use kermodes_core::{Session, Table, type_name};
use serde_json::{Value, json};
use tokio::sync::{oneshot, watch};

/// How long the cycle watcher waits before checking whether the server is stopping.
const CYCLE_WAIT: Duration = Duration::from_millis(500);
/// How long the controller may take to answer the programs page.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// Bumped when the published set changes, so connections resend the list.
    catalog: watch::Sender<u64>,
    /// The last finished engine cycle.
    cycles: watch::Sender<u64>,
    /// The built web app (`index.html` and its assets), if any.
    app: Option<PathBuf>,
    /// `kermodes control`, whose API the programs page reads through this server.
    control: Option<Control>,
}

struct Control {
    url: String,
    client: Client<HttpConnector, Full<Bytes>>,
}

impl Shared {
    fn tables(&self) -> MutexGuard<'_, BTreeMap<String, Table>> {
        self.tables
            .lock()
            .expect("kermodes gateway tables poisoned by an earlier panic")
    }
}

/// Serves `session` on `addr` (port 0 picks a free one), with the web app in `app` and the
/// programs of `control`, `kermodes control`'s URL, if there is one.
pub fn serve(
    session: Session,
    addr: SocketAddr,
    app: Option<PathBuf>,
    control: Option<String>,
) -> io::Result<Server> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let shared = Arc::new(Shared {
        tables: Mutex::new(BTreeMap::new()),
        catalog: watch::Sender::new(0),
        cycles: watch::Sender::new(0),
        app,
        control: control.map(|url| Control {
            url: url.trim_end_matches('/').to_owned(),
            client: Client::builder(TokioExecutor::new()).build_http(),
        }),
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
    let router = Router::new()
        .route("/ws", get(upgrade))
        .route("/api/projects", get(projects))
        .route(
            "/api/deployments/{project}/{resource}/{action}",
            get(deployment_get).post(deployment_post),
        )
        .route("/", get(index))
        .route("/{*path}", get(asset))
        .with_state(shared.clone());
    let runtime = thread::Builder::new()
        .name("kermodes-gateway".into())
        .spawn(move || {
            runtime.block_on(async move {
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

    /// Whether a table of that name was published.
    pub fn unpublish(&self, name: &str) -> bool {
        let removed = self.shared.tables().remove(name).is_some();
        self.shared.catalog.send_modify(|v| *v += 1);
        removed
    }

    pub fn published(&self) -> Vec<String> {
        self.shared.tables().keys().cloned().collect()
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
    }
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
    let plain = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    };
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
    match tokio::time::timeout(CONTROL_TIMEOUT, control.client.request(request)).await {
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
        Err(_) => StatusCode::NOT_FOUND.into_response(),
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

/// An open view: its viewer and the cycle of the window last sent.
struct Open {
    viewer: Arc<Mutex<Viewer>>,
    sent: Option<u64>,
}

async fn connection(mut socket: WebSocket, shared: Arc<Shared>) {
    let mut views: HashMap<u64, Open> = HashMap::new();
    let mut cycles = shared.cycles.subscribe();
    let mut catalog = shared.catalog.subscribe();
    let mut outgoing = vec![tables(&shared)];
    loop {
        for message in outgoing.drain(..) {
            if socket.send(message).await.is_err() {
                return;
            }
        }
        tokio::select! {
            received = socket.recv() => match received {
                Some(Ok(Message::Text(text))) => {
                    outgoing = handle(&shared, &mut views, &text).await;
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => {}
            },
            Ok(()) = cycles.changed() => {
                cycles.borrow_and_update();
                for (id, open) in &mut views {
                    let viewer = open.viewer.clone();
                    let changed = blocking(move || lock(&viewer).changed()).await;
                    if changed.is_some() && changed != open.sent {
                        open.sent = changed;
                        let viewer = open.viewer.clone();
                        let reply = blocking(move || lock(&viewer).rows()).await;
                        outgoing.extend(answer(*id, &open.viewer, reply));
                    }
                }
            },
            Ok(()) = catalog.changed() => {
                catalog.borrow_and_update();
                outgoing.push(tables(&shared));
            },
        }
    }
}

/// The replies to one message from the app.
async fn handle(shared: &Shared, views: &mut HashMap<u64, Open>, text: &str) -> Vec<Message> {
    let message: Value = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(e) => return vec![error(None, &format!("not JSON: {e}"))],
    };
    let kind = message["type"].as_str().unwrap_or_default();
    let view = message["view"].as_u64();
    match (kind, view) {
        ("tables", _) => vec![tables(shared)],
        ("open", Some(id)) => {
            let name = message["table"].as_str().unwrap_or_default();
            let Some(table) = shared.tables().get(name).cloned() else {
                return vec![error(Some(id), &format!("no published table `{name}`"))];
            };
            let viewer = Arc::new(Mutex::new(blocking(move || Viewer::new(&table)).await));
            let rows = {
                let viewer = viewer.clone();
                blocking(move || lock(&viewer).rows()).await
            };
            let sent = rows.as_ref().ok().and_then(|f| f.as_ref()).map(|f| f.cycle);
            let replies = answer(id, &viewer, rows);
            views.insert(id, Open { viewer, sent });
            replies
        }
        ("close", Some(id)) => {
            views.remove(&id);
            Vec::new()
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
            let viewer = open.viewer.clone();
            let reply = blocking(move || lock(&viewer).receive(request)).await;
            if let Ok(Some(frame)) = &reply
                && frame.fetched.is_none()
            {
                open.sent = Some(frame.cycle);
            }
            answer(id, &open.viewer, reply)
        }
        _ => vec![error(None, &format!("unknown message `{kind}`"))],
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
    Ok(Some(match kind {
        "viewport" => Request::Viewport {
            top: number("top")?,
            bottom: number("bottom")?,
        },
        "sort" => Request::Sort(
            message["by"]
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
        ),
        "filter" => Request::Filter(
            message["filters"]
                .as_object()
                .ok_or("`filter` needs an object `filters`")?
                .iter()
                .map(|(column, text)| {
                    (column.clone(), text.as_str().unwrap_or_default().to_owned())
                })
                .collect(),
        ),
        "fetch" => Request::Fetch {
            id: number("id")? as u64,
            start: number("start")?,
            count: number("count")?,
        },
        _ => return Ok(None),
    }))
}

/// A view's frame as messages: its state (for windows), then the binary frame; or the error.
fn answer(
    id: u64,
    viewer: &Mutex<Viewer>,
    reply: Result<Option<Frame>, kermodes_core::Error>,
) -> Vec<Message> {
    match reply {
        Ok(Some(frame)) => {
            let mut messages = Vec::with_capacity(2);
            if frame.fetched.is_none() {
                messages.push(state(id, &lock(viewer), &frame));
            }
            messages.push(binary(id, frame));
            messages
        }
        Ok(None) => vec![error(Some(id), "the table is gone")],
        Err(e) => vec![error(Some(id), &e.to_string())],
    }
}

fn state(id: u64, viewer: &Viewer, frame: &Frame) -> Message {
    let sort: Vec<Value> = viewer
        .sort()
        .iter()
        .map(|k| json!({"column": k.column, "descending": k.descending}))
        .collect();
    Message::Text(
        json!({
            "type": "state",
            "view": id,
            "row_count": frame.total,
            "status": viewer.status(frame.cycle),
            "sort": sort,
            "filters": viewer.filters(),
            "filter_errors": viewer.filter_errors(),
        })
        .to_string()
        .into(),
    )
}

fn binary(id: u64, frame: Frame) -> Message {
    let mut head = json!({
        "view": id,
        "type": if frame.fetched.is_some() { "fetched" } else { "rows" },
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
    let tables: Vec<Value> = shared
        .tables()
        .iter()
        .map(|(name, table)| {
            let columns: Vec<Value> = table
                .schema()
                .fields()
                .iter()
                .map(|f| json!({"name": f.name(), "type": type_name(f.data_type())}))
                .collect();
            json!({"name": name, "columns": columns, "refreshing": table.is_refreshing()})
        })
        .collect();
    Message::Text(
        json!({"type": "tables", "tables": tables})
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

fn lock(viewer: &Mutex<Viewer>) -> MutexGuard<'_, Viewer> {
    viewer
        .lock()
        .expect("kermodes viewer poisoned by an earlier panic")
}

/// Runs engine work (which takes the session's locks) off the async workers.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .expect("kermodes gateway engine call panicked")
}
