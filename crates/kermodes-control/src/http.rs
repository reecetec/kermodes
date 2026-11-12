//! The controller's HTTP surface: code snapshots for runs' init containers, the sync webhook any
//! git host can call (§11.3), projects' status, and each deployment's events, logs, rollouts,
//! and restarts. It is reachable only inside its namespace until the gateway authenticates
//! callers (§16.4); the web UI reaches it through its own server.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use crate::plan;
use crate::run::{self, Controller};

pub async fn serve(controller: Arc<Controller>, listener: tokio::net::TcpListener) {
    let app = Router::new()
        .route("/snapshots/{project}/{commit}", get(snapshot))
        .route("/hooks/sync", post(sync))
        .route("/api/projects", get(projects))
        .route("/api/deployments/{project}/{resource}/events", get(events))
        .route("/api/deployments/{project}/{resource}/logs", get(logs))
        .route(
            "/api/deployments/{project}/{resource}/rollout",
            post(roll_out),
        )
        .route(
            "/api/deployments/{project}/{resource}/restart",
            post(restart),
        )
        .route("/api/deployments/{project}/{resource}/stop", post(stop))
        .route("/api/deployments/{project}/{resource}/start", post(start))
        .with_state(controller);
    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("kermodes control: the HTTP server stopped: {e}");
    }
}

/// The Kubernetes name of a configured project's resource.
fn deployment(
    controller: &Controller,
    project: &str,
    resource: &str,
) -> Result<String, (StatusCode, String)> {
    if !controller.config.projects.iter().any(|p| p.name == project) {
        return Err((StatusCode::NOT_FOUND, format!("no project `{project}`")));
    }
    plan::object_name(project, resource).map_err(|e| (StatusCode::BAD_REQUEST, e))
}

async fn events(
    State(controller): State<Arc<Controller>>,
    Path((project, resource)): Path<(String, String)>,
) -> Response {
    let name = match deployment(&controller, &project, &resource) {
        Ok(name) => name,
        Err(refused) => return refused.into_response(),
    };
    match controller.kube.events(&name).await {
        Ok(events) => axum::Json(events).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
    }
}

/// The serving run's last `tail` lines (default 300, at most 5,000).
async fn logs(
    State(controller): State<Arc<Controller>>,
    Path((project, resource)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let name = match deployment(&controller, &project, &resource) {
        Ok(name) => name,
        Err(refused) => return refused.into_response(),
    };
    let tail = query
        .get("tail")
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(300)
        .clamp(1, 5_000);
    match controller.kube.logs(&name, tail).await {
        Ok(text) => text.into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e).into_response(),
    }
}

/// Stops a deployment: no run until it is started, its template still following commits.
async fn stop(
    State(controller): State<Arc<Controller>>,
    Path((project, resource)): Path<(String, String)>,
) -> Response {
    set_stopped(&controller, &project, &resource, true).await
}

/// Starts a stopped deployment, at the commit its template follows.
async fn start(
    State(controller): State<Arc<Controller>>,
    Path((project, resource)): Path<(String, String)>,
) -> Response {
    set_stopped(&controller, &project, &resource, false).await
}

/// Records the desired state, then has the leader apply it (whichever replica took this).
async fn set_stopped(
    controller: &Controller,
    project: &str,
    resource: &str,
    stopped: bool,
) -> Response {
    if let Err(refused) = deployment(controller, project, resource) {
        return refused.into_response();
    }
    let mut state = controller.state.lock().await;
    if let Err(e) = state.set_stopped(project, resource, stopped).await {
        return (StatusCode::NOT_FOUND, e).into_response();
    }
    match state.request_sync().await {
        Ok(()) => {
            let verb = if stopped { "stopping" } else { "starting" };
            (StatusCode::ACCEPTED, format!("{resource} {verb}")).into_response()
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
    }
}

/// Fresh runs at the same commit (`kubectl rollout restart`), rolled as the resource rolls.
async fn restart(
    State(controller): State<Arc<Controller>>,
    Path((project, resource)): Path<(String, String)>,
) -> Response {
    let name = match deployment(&controller, &project, &resource) {
        Ok(name) => name,
        Err(refused) => return refused.into_response(),
    };
    match controller.kube.restart(&name).await {
        Ok(()) => (StatusCode::ACCEPTED, format!("{resource} restarting")).into_response(),
        Err(e) => (StatusCode::CONFLICT, e).into_response(),
    }
}

async fn snapshot(
    State(controller): State<Arc<Controller>>,
    Path((project, commit)): Path<(String, String)>,
) -> Response {
    match controller
        .state
        .lock()
        .await
        .snapshot(&project, &commit)
        .await
    {
        Ok(Some(tar)) => (
            [(header::CONTENT_TYPE, "application/x-tar")],
            Body::from(tar),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            format!("no snapshot of {project} at {commit}"),
        )
            .into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
    }
}

/// A push happened: sync now rather than at the next poll. Any git host that signs its webhooks
/// as GitHub, Gitea, and Forgejo do can call it: `X-Hub-Signature-256` is `sha256=` and the hex
/// HMAC-SHA256 of the body under the shared secret, so the secret itself never travels.
async fn sync(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let Some(secret) = &controller.config.webhook_secret else {
        return (
            StatusCode::NOT_FOUND,
            "the sync webhook has no secret configured",
        )
            .into_response();
    };
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok());
    if !signed(secret, &body, signature) {
        return (
            StatusCode::UNAUTHORIZED,
            "wrong or missing X-Hub-Signature-256",
        )
            .into_response();
    }
    // Whichever replica took the request, the leader syncs.
    match controller.state.lock().await.request_sync().await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
    }
}

/// Whether `signature` (an `X-Hub-Signature-256` value) signs `body` under `secret`.
fn signed(secret: &str, body: &[u8], signature: Option<&str>) -> bool {
    let Some(hex) = signature.and_then(|s| s.strip_prefix("sha256=")) else {
        return false;
    };
    if !hex.is_ascii() || hex.len() % 2 != 0 {
        return false;
    }
    let Ok(tag) = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
    else {
        return false;
    };
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    // Constant-time: a wrong guess reveals nothing about the right one.
    ring::hmac::verify(&key, body, &tag).is_ok()
}

async fn projects(State(controller): State<Arc<Controller>>) -> Response {
    match run::status(&controller).await {
        Ok(status) => axum::Json(status).into_response(),
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
    }
}

async fn roll_out(
    State(controller): State<Arc<Controller>>,
    Path((project, resource)): Path<(String, String)>,
) -> Response {
    match run::roll_out(&controller, &project, &resource).await {
        Ok(commit) => (
            StatusCode::ACCEPTED,
            format!("{resource} rolling to {commit}"),
        )
            .into_response(),
        Err(e) => (StatusCode::CONFLICT, e).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::signed;

    /// GitHub's published example (docs: "Validating webhook deliveries"), which Gitea and
    /// Forgejo compute the same way.
    #[test]
    fn only_the_secrets_signature_of_this_body_wakes_the_controller() {
        let secret = "It's a Secret to Everybody";
        let body = b"Hello, World!";
        let good = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
        assert!(signed(secret, body, Some(good)));
        assert!(!signed("another secret", body, Some(good)));
        assert!(!signed(secret, b"Hello, World?", Some(good)));
        assert!(!signed(secret, body, Some(&good["sha256=".len()..])));
        assert!(!signed(secret, body, Some("sha256=zz")));
        assert!(!signed(secret, body, Some("sha256=é7")));
        assert!(!signed(secret, body, None));
    }
}
