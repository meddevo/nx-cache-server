use crate::domain::storage::StorageProvider;
use crate::server::{error::ServerError, AppState};
use axum::{
    extract::{Request, State},
    http::Method,
    middleware::Next,
    response::Response,
};
use subtle::ConstantTimeEq;

/// One-line-per-request access log at INFO. This is intentionally separate
/// from the per-error structured logs in infra/aws.rs / server/error.rs: this
/// line proves *whether* a request happened and how it ended even when it
/// didn't hit an error path, and the response status lets an operator find
/// the matching detailed error log for any 4xx/5xx by hash+time.
pub async fn access_log_middleware(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let start = std::time::Instant::now();

    let response = next.run(request).await;

    let duration_ms = start.elapsed().as_millis();
    let status = response.status();
    // Cheap only: response Content-Length header, when the handler set one.
    let bytes = response
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    tracing::info!(
        method = %method,
        path = %path,
        status = status.as_u16(),
        duration_ms,
        bytes,
        "request completed"
    );

    response
}

pub async fn auth_middleware<T>(
    State(state): State<AppState<T>>,
    request: Request,
    next: Next,
) -> Result<Response, ServerError>
where
    T: StorageProvider,
{
    // Extract Bearer token from Authorization header
    let token = request
        .headers()
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        .and_then(|auth_value| auth_value.strip_prefix("Bearer "));

    // Return ServerError (not a bare StatusCode) so auth failures carry a
    // text/plain body. Nx rejects a bodyless 401 with "Misconfigured remote
    // cache endpoint: Requests should respond with text/plain on 401s."
    let token = match token {
        Some(t) => t,
        None => return Err(ServerError::Unauthorized),
    };

    // Constant-time comparisons for security. Both tokens are always
    // compared so timing does not reveal which one matched.
    let is_read_write = bool::from(
        token
            .as_bytes()
            .ct_eq(state.config.service_access_token.as_bytes()),
    );
    let is_read_only = state
        .config
        .read_only_access_token
        .as_deref()
        .is_some_and(|read_only| bool::from(token.as_bytes().ct_eq(read_only.as_bytes())));

    if !is_read_write && !is_read_only {
        return Err(ServerError::Unauthorized);
    }

    // The read-only token may only read; writes require the service access
    // token. This lets untrusted CI jobs (e.g. PR builds) use the cache
    // without being able to poison it (CVE-2025-36852 / CREEP).
    if !is_read_write && request.method() != Method::GET {
        return Err(ServerError::Forbidden);
    }

    Ok(next.run(request).await)
}
