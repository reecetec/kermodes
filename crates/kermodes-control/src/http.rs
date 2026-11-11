//! The controller's HTTP surface: code snapshots for runs' init containers, the sync webhook any
//! git host can call (§11.3), project status, and manual rollouts (§11.4). It is reachable only
//! inside its namespace until the gateway authenticates callers (§16.4).

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use crate::run::{self, Controller};

pub async fn serve(controller: Arc<Controller>, listener: tokio::net::TcpListener) {
    let app = Router::new()
        .route("/snapshots/{project}/{commit}", get(snapshot))
        .route("/hooks/sync", post(sync))
        .route("/api/status", get(status))
        .route(
            "/api/projects/{project}/resources/{resource}/rollout",
            post(roll_out),
        )
        .with_state(controller);
    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("kermodes control: the HTTP server stopped: {e}");
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

/// A push happened: sync now rather than at the next poll. Any git host can call it with the
/// shared secret in `X-Kermodes-Secret`.
async fn sync(State(controller): State<Arc<Controller>>, headers: HeaderMap) -> Response {
    let Some(secret) = &controller.config.webhook_secret else {
        return (
            StatusCode::NOT_FOUND,
            "the sync webhook has no secret configured",
        )
            .into_response();
    };
    let given = headers
        .get("x-kermodes-secret")
        .and_then(|v| v.to_str().ok());
    if given != Some(secret.as_str()) {
        return (
            StatusCode::UNAUTHORIZED,
            "wrong or missing X-Kermodes-Secret",
        )
            .into_response();
    }
    controller.wake.notify_one();
    StatusCode::ACCEPTED.into_response()
}

async fn status(State(controller): State<Arc<Controller>>) -> Response {
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
