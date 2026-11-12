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
    controller.wake.notify_one();
    StatusCode::ACCEPTED.into_response()
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
