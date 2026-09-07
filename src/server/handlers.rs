use crate::domain::storage::{StorageError, StorageProvider};
use crate::server::{error::ServerError, validation, AppState};
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::time::Duration;
use tokio_stream::StreamExt;

pub async fn store_artifact<T: StorageProvider>(
    Path(hash): Path<String>,
    State(state): State<AppState<T>>,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    if let Err(invalid) = validation::validate_hash(&hash) {
        // Drain first, same as the 409 below: the client is still uploading when
        // we reject the key, and answering with its body unread closes the socket
        // under it - it would see a write error instead of this 400.
        drain_body(body).await;
        return Err(invalid);
    }

    // A failed existence probe must not 500 - see `retrieve_artifact` for why a
    // 5xx from this server is disproportionately expensive. We don't know
    // whether the key is there, so take the honest branch and store it: keys are
    // content-addressed, so re-writing bytes that may already be present is a
    // byte-identical no-op. 409 is still returned when `exists` actually said
    // yes (no deviation from the Nx immutability contract), and a genuine 500
    // now means only one thing: the write itself failed.
    if state.storage.exists(&hash).await.unwrap_or(false) {
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
    let mut reader = tokio_util::io::StreamReader::new(body_stream);
    // Lend the reader rather than move it, so a failed store leaves the unread
    // remainder here to drain (the storage layer reads one part before its
    // first S3 call, so small artifacts are already consumed by then).
    let reader_stream = tokio_util::io::ReaderStream::new(&mut reader);

    match state.storage.store(&hash, reader_stream).await {
        Ok(()) => {}
        // The client went away mid-upload; nobody is listening for the answer.
        Err(StorageError::ClientAbort) => return Err(StorageError::ClientAbort.into()),
        // A failed write is answered 403, not 500. Nx's `store()` treats 403
        // (like 409) as "server declined, carry on" and returns Ok(false);
        // anything else is "Misconfigured remote cache endpoint", which
        // `cache.put` retries 6 times (re-uploading the artifact each time) and
        // then rejects, marking the task that just *succeeded* as failed. The
        // only cost of the 403 is a later cache miss for this hash. 200 would
        // also work but would make "200 in the access log" stop meaning
        // "bytes are in S3". Logged at ERROR in infra/aws.rs at the failure.
        Err(e) => {
            tracing::error!(
                hash,
                "cache STORE declined (storage failed: {e}); answering 403 so Nx keeps the task green"
            );
            let _ = tokio::time::timeout(
                DRAIN_TIMEOUT,
                tokio::io::copy(&mut reader, &mut tokio::io::sink()),
            )
            .await;
            // Same 403 the read-only token gets: exact `text/plain`, as Nx checks.
            return Err(ServerError::Forbidden);
        }
    }
    // Seed the probe cache so same-instance GETs skip the HeadObject and never
    // read a stale 404 left by a probe that ran before this write.
    state.probe.mark_present(&hash);

    // 200, not 202: the Nx client matches PUT success against exactly
    // StatusCode::OK. Anything else is a "Misconfigured remote cache endpoint"
    // error that makes Nx retry the upload into the 409 path above - every
    // write transferred the artifact twice (upstream PR #23).
    Ok((StatusCode::OK, ""))
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
    //
    // A read that fails is answered 404, never 500: a failed cache read *is* a
    // cache miss, and the two are not symmetric in cost. Nx handles 404 by
    // recomputing the task (it already does, for ~96% of reads); a 500 makes it
    // abort the whole run as a misconfigured endpoint (nrwl/nx#36107) even after
    // every task succeeded. In dytab's `nx run <app>:serve` step that is
    // unrecoverable - the failing build is spawned *inside* the serve executor,
    // so the CI tolerance guard has no exit code to launder, the executor parks
    // in watch mode, and the service never boots. Worst case here: we recompute
    // an artifact we already had. Errors are still logged at ERROR with the
    // operation and AWS request_id in infra/aws.rs - alarm on those, not on 5xx.
    let present = state
        .probe
        .present(&hash, || state.storage.exists(&hash))
        .await
        .unwrap_or(false);
    if !present {
        return Err(ServerError::Storage(StorageError::NotFound));
    }

    let reader = state
        .storage
        .retrieve(&hash)
        .await
        .map_err(|_| ServerError::Storage(StorageError::NotFound))?;
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

/// How long we go on reading a body we already know we're discarding. A CI
/// client uploading an artifact finishes in seconds; past this it is either
/// pathologically slow or trickling bytes to hold the connection open. Draining
/// is a courtesy to the client, so it must not become a way for one to pin a
/// connection indefinitely - the server has no other request timeout.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

/// Read and discard a request body to completion so the client finishes its
/// upload before we close the (forced `Connection: close`) socket. A read error
/// means the client already went away - nothing left to drain.
pub(crate) async fn drain_body(body: Body) {
    drain_body_within(body, DRAIN_TIMEOUT).await;
}

/// Split out so tests can bound it in milliseconds instead of waiting a minute.
/// Giving up just restores the old behaviour for that one client (it sees a write
/// error rather than the status), which is the point: bounded courtesy.
async fn drain_body_within(body: Body, limit: Duration) {
    let _ = tokio::time::timeout(limit, async move {
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            if chunk.is_err() {
                break;
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An unbounded drain would let one authenticated client hold a connection
    /// open forever by never finishing its upload.
    #[tokio::test]
    async fn drain_gives_up_on_a_body_that_never_ends() {
        let never_ends =
            Body::from_stream(tokio_stream::pending::<Result<Vec<u8>, std::io::Error>>());
        // Returns, rather than hanging the suite.
        drain_body_within(never_ends, Duration::from_millis(50)).await;
    }
}
