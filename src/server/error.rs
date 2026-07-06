use crate::domain::storage::StorageError;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Bad request")]
    BadRequest,

    #[error("Unauthorized")]
    Unauthorized,

    #[error("Internal server error")]
    InternalError,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            // Map domain errors to HTTP responses
            ServerError::Storage(StorageError::NotFound) => {
                (StatusCode::NOT_FOUND, "The record was not found")
            }
            ServerError::Storage(StorageError::AlreadyExists) => {
                (StatusCode::CONFLICT, "Cannot override an existing record")
            }
            // Client disconnected/reset the body stream mid-upload. nx treats
            // this as fatal regardless of status, but it must not be counted
            // (or paged on) as a server failure - it's a 4xx, not a 5xx, and
            // infra/aws.rs already logged it at WARN with byte-count context.
            ServerError::Storage(StorageError::ClientAbort) => {
                (StatusCode::BAD_REQUEST, "Client disconnected during upload")
            }

            // HTTP-specific errors
            ServerError::BadRequest => (StatusCode::BAD_REQUEST, "Bad request"),
            ServerError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized"),

            // Generic fallback. Storage::OperationFailed (S3/head/get/put/multipart
            // failures) is already logged at ERROR with operation, hash, and the
            // full SDK error/request_id in infra/aws.rs at the point of failure -
            // logging again here would just duplicate that without new detail.
            _ => {
                if !matches!(self, ServerError::Storage(_)) {
                    tracing::error!("Server error: {}", self);
                }
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            }
        };

        (status, [("Content-Type", "text/plain")], message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_abort_maps_to_4xx_not_5xx() {
        // Fix #2: a client disconnect must never look like a server failure
        // to nx (which treats any non-2xx/404/409 as fatal either way, but
        // operators/alerting must be able to tell the two apart).
        let response = ServerError::Storage(StorageError::ClientAbort).into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn s3_operation_failure_maps_to_500() {
        let response = ServerError::Storage(StorageError::OperationFailed).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn not_found_maps_to_404() {
        let response = ServerError::Storage(StorageError::NotFound).into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn already_exists_maps_to_409() {
        let response = ServerError::Storage(StorageError::AlreadyExists).into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
}
