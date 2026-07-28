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
use std::time::Duration;
use tower_http::set_header::SetResponseHeaderLayer;

/// How often the self-probe checks that S3 is still reachable.
const SELF_PROBE_INTERVAL: Duration = Duration::from_secs(60);

/// Key the self-probe HeadObjects. It contains a `.`, which
/// [`validation::validate_hash`] rejects, so no client can create it through the
/// API — the probe therefore always exercises the cheap "absent" path: one
/// HeadObject returning 404, never a GetObject.
const SELF_PROBE_KEY: &str = "_selfprobe.nx-cache-server";

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

/// One S3 reachability check, logged with its duration.
///
/// Every other signal this server produces is request-driven, so a task that
/// loses its ability to reach S3 while CI is idle leaves no trace at all until
/// the next build. That blind spot is exactly what makes the observed
/// degradation ambiguous: one long-lived task produced thousands of 30s S3
/// timeouts while its freshly-started peers produced none, and we cannot tell
/// whether onset needs burst traffic or just uptime. This ticks regardless of
/// traffic and timestamps it.
///
/// Failures need no logging here: `exists()` already emits the structured
/// `S3 operation failed` ERROR line that the CloudWatch alarm counts (see
/// infra/aws.rs). Note the probe alone cannot *trip* that alarm — at one call a
/// minute it can contribute at most 5 errors per 5-minute period against a
/// threshold of 20 — so an idle-window onset is recorded for the morning, not
/// paged on. That is the intended trade for a dev CI cache.
async fn self_probe_once<T: StorageProvider>(storage: &T) {
    let start = std::time::Instant::now();
    let outcome = storage.exists(SELF_PROBE_KEY).await;
    tracing::info!(
        duration_ms = start.elapsed().as_millis(),
        reachable = outcome.is_ok(),
        "s3 self-probe"
    );
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

    // Synthetic S3 probe, independent of request traffic. `interval` fires its
    // first tick immediately, so there's a reachability line from startup.
    let probe_storage = app_state.storage.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SELF_PROBE_INTERVAL);
        loop {
            ticker.tick().await;
            self_probe_once(probe_storage.as_ref()).await;
        }
    });

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
        store_fails: bool,
        retrieve_fails: bool,
        store_calls: Arc<AtomicUsize>,
    }

    impl MockStorage {
        fn new(exists: ExistsBehavior) -> Self {
            Self {
                exists,
                store_fails: false,
                retrieve_fails: false,
                store_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn failing_store(mut self) -> Self {
            self.store_fails = true;
            self
        }

        fn failing_retrieve(mut self) -> Self {
            self.retrieve_fails = true;
            self
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
            if self.store_fails {
                return Err(StorageError::OperationFailed);
            }
            Ok(())
        }

        async fn retrieve(
            &self,
            _hash: &str,
        ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError> {
            if self.retrieve_fails {
                return Err(StorageError::OperationFailed);
            }
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
    async fn self_probe_key_is_unreachable_through_the_api() {
        // The probe relies on its key never existing, so it always takes the
        // cheap HeadObject-404 path. That holds only while no client can create
        // it — `.` is outside validate_hash's charset.
        assert!(validation::validate_hash(SELF_PROBE_KEY).is_err());
    }

    #[tokio::test]
    async fn self_probe_runs_against_healthy_and_failing_storage() {
        // Guards the wiring, not the logging: the probe must complete either way
        // and must never panic on a task that has lost S3 (it runs unattended
        // overnight, and a panicking spawned task would silence it permanently).
        self_probe_once(&MockStorage::new(ExistsBehavior::No)).await;
        self_probe_once(&MockStorage::new(ExistsBehavior::Fail)).await;
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
    async fn put_seeds_cache_so_next_get_is_a_hit() {
        // Storage reports exists=No, so a cold GET would 404. After a PUT seeds
        // the probe cache (mark_present), the next GET on the same instance must
        // read the key as present (200) - the PUT-mid-burst stale-404 fix.
        // Both oneshots share one AppState (probe is Arc), so the seed carries.
        let app = app(MockStorage::new(ExistsBehavior::No));
        let put = app
            .clone()
            .oneshot(
                Request::put("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RW_TOKEN))
                    .body(Body::from("artifact bytes"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::ACCEPTED);

        let get = app
            .clone()
            .oneshot(
                Request::get("/v1/cache/abc123")
                    .header(header::AUTHORIZATION, format!("Bearer {}", RO_TOKEN))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::OK, "seeded key must read as a hit, not 404");
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
    async fn put_stores_when_the_existence_probe_fails() {
        // A failed HeadObject used to 500 the PUT. We can't tell whether the key
        // is there, so store it: content-addressed keys make a duplicate write
        // byte-identical. Only a failed `store` is a real 500 now.
        let storage = MockStorage::new(ExistsBehavior::Fail);
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
    async fn write_failure_500_has_connection_close() {
        // The one remaining 5xx: the write itself failed, so Nx must not believe
        // the artifact was accepted.
        let response = app(MockStorage::new(ExistsBehavior::No).failing_store())
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
    async fn get_probe_failure_is_a_miss_not_a_500() {
        // A read that fails IS a cache miss. Nx recomputes on 404; on 500 it
        // aborts the whole run as a misconfigured endpoint (nrwl/nx#36107),
        // which is unrecoverable in the `<app>:serve` CI step.
        let response = app(MockStorage::new(ExistsBehavior::Fail))
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
    async fn get_retrieve_failure_is_a_miss_not_a_500() {
        // Same rule one step later: the key probed present but GetObject failed.
        let response = app(MockStorage::new(ExistsBehavior::Yes).failing_retrieve())
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
