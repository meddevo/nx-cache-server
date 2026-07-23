use crate::domain::storage::{StorageError, StorageProvider};
use crate::server::{error::ServerError, validation, AppState};
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use tokio_stream::StreamExt;

pub async fn store_artifact<T: StorageProvider>(
    Path(hash): Path<String>,
    State(state): State<AppState<T>>,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    validation::validate_hash(&hash)?;

    if state.storage.exists(&hash).await? {
        // Drain the request body before responding. Every response forces
        // `Connection: close` (the 502 fix): answering 409 while the client is
        // still uploading closes the socket under it, so reqwest reports
        // "error sending request" (a transport error, not a clean 409) and Nx
        // fails the build - that's the "Mode A" red-builds-on-write incident.
        // Reading the body to completion first lets the client finish before we
        // close. The key is content-addressed, so the duplicate we're
        // discarding is byte-identical to what's already stored.
        state.probe.mark_present(&hash);
        drain_body(body).await;
        return Ok((StatusCode::CONFLICT, "Cannot override an existing record"));
    }

    // No second existence check inside `store()` - this one already covered it
    // (removing the duplicate HeadObject saves ~one S3 call per PUT and closes
    // a TOCTOU window). A racing writer can only store byte-identical content
    // under the same content-addressed key, so a lost race is harmless.

    // Stream the request body straight into storage without buffering the
    // whole artifact in memory - `axum::body::to_bytes` (the old approach)
    // would defeat the point of infra/aws.rs's multipart streaming below it.
    // A read error here (client disconnected/reset) surfaces as
    // `std::io::Error` and is turned into `StorageError::ClientAbort` by the
    // storage layer, not a 500 - see infra/aws.rs and server/error.rs.
    let body_stream = body
        .into_data_stream()
        .map(|chunk| chunk.map_err(std::io::Error::other));
    let reader = tokio_util::io::StreamReader::new(body_stream);
    let reader_stream = tokio_util::io::ReaderStream::new(reader);

    state.storage.store(&hash, reader_stream).await?;
    // Seed the probe cache so same-instance GETs skip the HeadObject and never
    // read a stale 404 left by a probe that ran before this write.
    state.probe.mark_present(&hash);

    Ok((StatusCode::ACCEPTED, ""))
}

pub async fn retrieve_artifact<T: StorageProvider>(
    Path(hash): Path<String>,
    State(state): State<AppState<T>>,
) -> Result<impl IntoResponse, ServerError> {
    validation::validate_hash(&hash)?;

    // Probe existence through the single-flight + short-TTL cache first. Under
    // CI bursts the same *missing* keys get probed dozens of times; without
    // this each was a fresh S3 call (new connection + DNS lookup) that
    // stampeded the resolver and timed S3 out at 30s -> "Mode B" 500s. The
    // cache collapses that to ~one HeadObject per key per TTL. A confirmed-
    // present key then streams via GetObject as before (hits are ~4%, so the
    // extra HeadObject on the hit path is negligible).
    if !state.probe.present(&hash, || state.storage.exists(&hash)).await? {
        return Err(ServerError::Storage(StorageError::NotFound));
    }

    let reader = state.storage.retrieve(&hash).await?;
    let stream = tokio_util::io::ReaderStream::new(reader);
    let body = Body::from_stream(stream);

    Ok((
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        body,
    ))
}

pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Read and discard a request body to completion so the client finishes its
/// upload before we close the (forced `Connection: close`) socket. A read error
/// means the client already went away - nothing left to drain.
async fn drain_body(body: Body) {
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        if chunk.is_err() {
            break;
        }
    }
}
