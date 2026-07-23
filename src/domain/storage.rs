use async_trait::async_trait;
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Object not found")]
    NotFound,
    #[error("Object already exists")]
    AlreadyExists,
    #[error("Storage operation failed")]
    OperationFailed,
    /// The client disconnected/reset the request body stream mid-upload.
    /// This is distinct from `OperationFailed` because it is never our fault
    /// (nothing on the server side went wrong) and must not be reported to
    /// nx/operators as a 5xx - see server/error.rs mapping.
    #[error("Client aborted upload")]
    ClientAbort,
}

#[async_trait]
pub trait StorageProvider: Send + Sync + 'static {
    /// Check if an object exists at the given hash key
    async fn exists(&self, hash: &str) -> Result<bool, StorageError>;

    /// Store data stream to storage at the given hash key.
    ///
    /// The caller (the server handler) owns the existence check and only calls
    /// this for a key it believes absent, so implementations must not assume
    /// they're the first writer. Keys are content-addressed, so a racing
    /// overwrite is byte-identical and harmless - do not re-add a pre-write
    /// HeadObject here (that's the redundant call the handler already made).
    async fn store(
        &self,
        hash: &str,
        data: ReaderStream<impl AsyncRead + Send + Unpin>,
    ) -> Result<(), StorageError>;

    /// Retrieve object as a stream from storage
    /// Returns NotFound error if object doesn't exist
    async fn retrieve(&self, hash: &str)
        -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError>;
}
