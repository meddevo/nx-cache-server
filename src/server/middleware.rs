use crate::domain::storage::StorageProvider;
use crate::server::AppState;
use axum::{
    extract::{Request, State},
    http::StatusCode,
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
) -> Result<Response, StatusCode>
where
    T: StorageProvider,
{
    // Extract Bearer token from Authorization header
    let token = request
        .headers()
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        .and_then(|auth_value| auth_value.strip_prefix("Bearer "));

    let token = match token {
        Some(t) => t,
        None => return Err(StatusCode::UNAUTHORIZED),
    };

    // Constant-time comparison for security
    if !bool::from(
        token
            .as_bytes()
            .ct_eq(state.config.service_access_token.as_bytes()),
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}
