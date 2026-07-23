pub mod error;
pub mod handlers;
pub mod middleware;
pub mod probe_cache;
pub mod validation;

use crate::domain::{config::ServerConfig, storage::StorageProvider};
use crate::server::probe_cache::ProbeCache;
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
    /// Coalesces + short-TTL-caches GET existence probes (see probe_cache).
    pub probe: Arc<ProbeCache>,
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
        probe: Arc::new(ProbeCache::default()),
    };

    let app = create_router::<T>(&app_state).with_state(app_state);
    let addr = std::net::SocketAddr::new(config.bind_address, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!("Server running on {}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    //! Full-router tests: every response class the router can produce must
    //! carry `Connection: close` (the ALB keep-alive 502 fix is worthless if
    //! it only covers 200s), and auth failures must carry a text/plain body
    //! (Nx rejects bodyless 401/403s). These exercise the real layer stack -
    //! `.layer()` ordering in axum is subtle (last-added layer is outermost),
    //! so this is guarded by tests rather than by reading the code carefully.

    use super::*;
    use crate::domain::storage::StorageError;
    use async_trait::async_trait;
    use axum::{
        body::Body,
        http::{header, Request, StatusCode},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncRead;
    use tokio_util::io::ReaderStream;
    use tower::ServiceExt;

    #[derive(Clone, Copy)]
    enum ExistsBehavior {
        No,
        Yes,
        Fail,
    }

    #[derive(Clone)]
    struct MockStorage {
        exists: ExistsBehavior,
        store_calls: Arc<AtomicUsize>,
    }

    impl MockStorage {
        fn new(exists: ExistsBehavior) -> Self {
            Self {
                exists,
                store_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl StorageProvider for MockStorage {
        async fn exists(&self, _hash: &str) -> Result<bool, StorageError> {
            match self.exists {
                ExistsBehavior::No => Ok(false),
                ExistsBehavior::Yes => Ok(true),
                ExistsBehavior::Fail => Err(StorageError::OperationFailed),
            }
        }

        async fn store(
            &self,
            _hash: &str,
            _data: ReaderStream<impl AsyncRead + Send + Unpin>,
        ) -> Result<(), StorageError> {
            self.store_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn retrieve(
            &self,
            _hash: &str,
        ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError> {
            Ok(Box::new(std::io::Cursor::new(b"artifact".to_vec())))
        }
    }

    const RW_TOKEN: &str = "test-rw-token";
    const RO_TOKEN: &str = "test-ro-token";

    fn app(storage: MockStorage) -> Router {
        let state = AppState {
            storage: Arc::new(storage),
            config: Arc::new(ServerConfig {
                port: 3000,
                bind_address: "127.0.0.1".parse().unwrap(),
                service_access_token: RW_TOKEN.to_string(),
                read_only_access_token: Some(RO_TOKEN.to_string()),
                debug: false,
            }),
            probe: Arc::new(ProbeCache::default()),
        };
        create_router(&state).with_state(state)
    }

    fn assert_connection_close(response: &axum::response::Response) {
        assert_eq!(
            response
                .headers()
                .get(CONNECTION)
                .expect("Connection header must be present on every response"),
            "close",
            "SetResponseHeaderLayer must be outermost so every status code gets it"
        );
    }

    async fn assert_text_plain_nonempty_body(response: axum::response::Response) {
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .expect("auth failures must have a Content-Type"),
            "text/plain",
            "Nx requires text/plain on 401/403"
        );
        let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
        assert!(!body.is_empty(), "Nx rejects bodyless 401/403 responses");
    }

    #[tokio::test]
    async fn health_200_has_connection_close() {
        let response = app(MockStorage::new(ExistsBehavior::No))
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_connection_close(&response);
    }

    #[tokio::test]
    async fn missing_token_401_has_connection_close_and_text_plain_body() {
        let response = app(MockStorage::new(ExistsBehavior::No))
            .oneshot(Request::get("/v1/cache/abc123").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_connection_close(&response);
        assert_text_plain_nonempty_body(response).await;
    }

    #[tokio::test]
    async fn invalid_token_401_has_connection_close_and_text_plain_body() {
        let response = app(MockStorage::new(ExistsBehavior::No))
            .oneshot(
                Request::get("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, "Bearer wrong-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_connection_close(&response);
        assert_text_plain_nonempty_body(response).await;
    }

    #[tokio::test]
    async fn read_only_token_put_403_before_any_storage_call() {
        let storage = MockStorage::new(ExistsBehavior::No);
        let store_calls = storage.store_calls.clone();
        let response = app(storage)
            .oneshot(
                Request::put("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RO_TOKEN))
                    .body(Body::from("cache-poisoning attempt"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_connection_close(&response);
        assert_text_plain_nonempty_body(response).await;
        // CREEP mitigation: the handler (and thus any S3 PutObject or
        // CreateMultipartUpload) must never run for a read-only token.
        assert_eq!(store_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn read_only_token_can_get() {
        // GET now probes existence first (Mode B fix), so a hit needs exists=Yes.
        let response = app(MockStorage::new(ExistsBehavior::Yes))
            .oneshot(
                Request::get("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RO_TOKEN))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_connection_close(&response);
    }

    #[tokio::test]
    async fn get_missing_key_404_has_connection_close() {
        // The existence probe reports absent -> 404, without ever calling
        // `retrieve`. Nx treats 404 as a plain cache miss.
        let response = app(MockStorage::new(ExistsBehavior::No))
            .oneshot(
                Request::get("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RO_TOKEN))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_connection_close(&response);
    }

    #[tokio::test]
    async fn read_write_token_put_202_has_connection_close_and_stores() {
        let storage = MockStorage::new(ExistsBehavior::No);
        let store_calls = storage.store_calls.clone();
        let response = app(storage)
            .oneshot(
                Request::put("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RW_TOKEN))
                    .body(Body::from("artifact bytes"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_connection_close(&response);
        assert_eq!(store_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn conflict_409_has_connection_close() {
        let response = app(MockStorage::new(ExistsBehavior::Yes))
            .oneshot(
                Request::put("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RW_TOKEN))
                    .body(Body::from("artifact bytes"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_connection_close(&response);
    }

    #[tokio::test]
    async fn storage_failure_500_has_connection_close() {
        let response = app(MockStorage::new(ExistsBehavior::Fail))
            .oneshot(
                Request::put("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RW_TOKEN))
                    .body(Body::from("artifact bytes"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_connection_close(&response);
    }

    #[tokio::test]
    async fn invalid_hash_400_has_connection_close() {
        let response = app(MockStorage::new(ExistsBehavior::No))
            .oneshot(
                Request::get("/v1/cache/bad.hash!")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RW_TOKEN))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_connection_close(&response);
    }

    #[tokio::test]
    async fn unknown_route_404_has_connection_close() {
        let response = app(MockStorage::new(ExistsBehavior::No))
            .oneshot(Request::get("/no-such-route").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_connection_close(&response);
    }
}
