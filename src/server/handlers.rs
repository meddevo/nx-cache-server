use crate::domain::storage::StorageProvider;
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
        return Ok((StatusCode::CONFLICT, "Cannot override an existing record"));
    }

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

    Ok((StatusCode::ACCEPTED, ""))
}

pub async fn retrieve_artifact<T: StorageProvider>(
    Path(hash): Path<String>,
    State(state): State<AppState<T>>,
) -> Result<impl IntoResponse, ServerError> {
    validation::validate_hash(&hash)?;

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
