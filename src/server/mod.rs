pub mod error;
pub mod handlers;
pub mod middleware;
pub mod validation;

use crate::domain::{config::ServerConfig, storage::StorageProvider};
use axum::{
    http::{header::CONNECTION, HeaderValue},
    middleware::{from_fn, from_fn_with_state},
    routing::{get, put},
    Router,
};
use std::sync::Arc;
use tower_http::set_header::SetResponseHeaderLayer;

#[derive(Clone)]
pub struct AppState<T: StorageProvider> {
    pub storage: Arc<T>,
    pub config: Arc<ServerConfig>,
}

pub fn create_router<T: StorageProvider + Clone>(app_state: &AppState<T>) -> Router<AppState<T>> {
    let protected_routes = Router::new()
        .route("/v1/cache/{hash}", get(handlers::retrieve_artifact::<T>))
        .route("/v1/cache/{hash}", put(handlers::store_artifact::<T>))
        .route_layer(from_fn_with_state(
            app_state.clone(),
            middleware::auth_middleware::<T>,
        ));

    // Combine public and protected routes. The access log wraps everything
    // (including auth failures and /health) so every request produces a
    // status+duration line at INFO - see server/middleware.rs.
    Router::new()
        .route("/health", get(handlers::health_check)) // Public route - no auth required
        .merge(protected_routes)
        .layer(from_fn(middleware::access_log_middleware))
        // Force `Connection: close` on every response so the ALB never reuses a
        // backend keep-alive connection. This kills the keep-alive reuse race
        // that produced sporadic 502s: the target resets a pooled idle
        // connection just as the ALB dispatches a new request onto it, which
        // the ALB logs as target_status_code="-", response_processing_time=-1.
        // Costs one TCP+TLS handshake per request — negligible for a CI cache,
        // and Nx clients are short-lived anyway.
        .layer(SetResponseHeaderLayer::overriding(
            CONNECTION,
            HeaderValue::from_static("close"),
        ))
}

pub async fn run_server<T: StorageProvider + Clone>(
    storage: T,
    config: &ServerConfig,
) -> Result<(), std::io::Error> {
    let app_state = AppState {
        storage: Arc::new(storage),
        config: Arc::new(config.clone()),
    };

    let app = create_router::<T>(&app_state).with_state(app_state);
    let addr = std::net::SocketAddr::new(config.bind_address, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!("Server running on {}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}
