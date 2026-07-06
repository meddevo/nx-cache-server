use async_trait::async_trait;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::environment::region::EnvironmentVariableRegionProvider;
use aws_config::imds::region::ImdsRegionProvider;
use aws_config::meta::region::future::ProvideRegion as ProvideRegionFuture;
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_config::profile::region::ProfileFileRegionProvider;
use aws_config::provider_config::ProviderConfig;
use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::SharedHttpClient;
use aws_sdk_s3::config::{Credentials, ProvideCredentials};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::{RequestId, RequestIdExt};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::{config::Region, Client, Config as S3Config};
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_http_client::{tls, Builder as HttpClientBuilder};
use clap::Parser;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

use crate::domain::{
    config::{ConfigError, ConfigValidator},
    storage::{StorageError, StorageProvider},
};

/// S3 multipart part size. Parts below this go through a single PutObject;
/// at/above it we switch to CreateMultipartUpload/UploadPart/Complete so we
/// never hold more than ~one part of a (potentially multi-GB) artifact in
/// memory at once. Must stay >= S3's 5MiB multipart part minimum.
const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

/// Logs an S3 SDK error with everything an operator needs from a single INFO
/// (well, ERROR)-level line: which operation, which cache object, the full
/// SDK error (service code/message via Debug), and the AWS request id(s) so
/// AWS support can look it up. This is the fix for the 2026-07-06 incident
/// where diagnosing a 500 required RUST_LOG=debug on the whole SDK.
fn log_s3_error<E, R>(operation: &str, hash: &str, err: &aws_sdk_s3::error::SdkError<E, R>)
where
    E: std::fmt::Debug,
    R: std::fmt::Debug,
    aws_sdk_s3::error::SdkError<E, R>: RequestId + RequestIdExt,
{
    tracing::error!(
        operation,
        hash,
        request_id = err.request_id(),
        extended_request_id = err.extended_request_id(),
        error = ?err,
        "S3 operation failed"
    );
}

/// HTTPS client backed by rustls + ring.
///
/// Avoids the SDK default (`aws-lc-rs` → `aws-lc-sys`), which needs a
/// C/CMake/NASM toolchain and broke cross-platform release builds. Disabling
/// `default-https-client` drops the SDK's auto connector, so this is wired
/// explicitly into the S3 client and the credential/region chains below.
fn https_client() -> SharedHttpClient {
    HttpClientBuilder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::Ring))
        .build_https()
}

#[derive(Parser, Debug, Clone)]
pub struct AwsStorageConfig {
    #[arg(
        long,
        env = "AWS_REGION",
        help = "AWS region (e.g., us-west-2). Auto-discovered from environment, AWS config, or EC2/ECS metadata if not provided"
    )]
    pub region: Option<String>,

    #[arg(
        long,
        env = "AWS_ACCESS_KEY_ID",
        help = "AWS access key ID. Optional - uses AWS credential provider chain (environment, config file, IAM roles) if not provided"
    )]
    pub access_key_id: Option<String>,

    #[arg(
        long,
        env = "AWS_SECRET_ACCESS_KEY",
        help = "AWS secret access key. Required if --access-key-id is provided"
    )]
    pub secret_access_key: Option<String>,

    #[arg(
        long,
        env = "AWS_SESSION_TOKEN",
        help = "AWS session token for temporary security credentials. Optional"
    )]
    pub session_token: Option<String>,

    #[arg(
        long,
        env = "S3_BUCKET_NAME",
        help = "S3 bucket name for cache storage"
    )]
    pub bucket_name: String,

    #[arg(
        long,
        env = "S3_ENDPOINT_URL",
        help = "Custom S3 endpoint URL (e.g., http://localhost:9000 for MinIO). Optional - uses AWS S3 if not provided"
    )]
    pub endpoint_url: Option<String>,

    #[arg(
        long,
        env = "S3_TIMEOUT",
        default_value = "30",
        help = "S3 operation timeout in seconds"
    )]
    pub timeout_seconds: u64,
}

impl ProvideRegion for AwsStorageConfig {
    fn region(&self) -> ProvideRegionFuture<'_> {
        let region = self.region.clone();
        ProvideRegionFuture::new(async move {
            // Rebuild the env -> profile -> IMDS chain with our client, since
            // `or_default_provider()` would have no transport without it.
            let provider_config = ProviderConfig::default().with_http_client(https_client());
            RegionProviderChain::first_try(region.map(Region::new))
                .or_else(EnvironmentVariableRegionProvider::new())
                .or_else(
                    ProfileFileRegionProvider::builder()
                        .configure(&provider_config)
                        .build(),
                )
                .or_else(
                    ImdsRegionProvider::builder()
                        .configure(&provider_config)
                        .build(),
                )
                .region()
                .await
        })
    }
}

impl ProvideCredentials for AwsStorageConfig {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFuture<'a>
    where
        Self: 'a,
    {
        match (self.access_key_id.as_ref(), self.secret_access_key.as_ref()) {
            (Some(access_key_id), Some(secret_access_key)) => {
                ProvideCredentialsFuture::ready(Ok(Credentials::new(
                    access_key_id,
                    secret_access_key,
                    self.session_token.clone(),
                    None,
                    "nx-cache-server",
                )))
            }
            _ => ProvideCredentialsFuture::new(async {
                // `DefaultCredentialsChain::build()` panics without a configured
                // connector once `default-https-client` is disabled.
                let provider_config = ProviderConfig::default().with_http_client(https_client());
                DefaultCredentialsChain::builder()
                    .configure(provider_config)
                    .region(self.clone())
                    .build()
                    .await
                    .provide_credentials()
                    .await
            }),
        }
    }
}

impl ConfigValidator for AwsStorageConfig {
    async fn validate(&self) -> Result<(), ConfigError> {
        if self.bucket_name.is_empty() {
            return Err(ConfigError::MissingField("S3_BUCKET_NAME"));
        }
        if let Some(endpoint_url) = &self.endpoint_url {
            if !endpoint_url.starts_with("http://") && !endpoint_url.starts_with("https://") {
                return Err(ConfigError::Invalid(
                    "S3 endpoint URL must start with http:// or https://",
                ));
            }
        }
        match (self.access_key_id.as_ref(), self.secret_access_key.as_ref()) {
            (Some(..), None) => return Err(ConfigError::MissingField("AWS_SECRET_ACCESS_KEY")),
            (None, Some(..)) => return Err(ConfigError::MissingField("AWS_ACCESS_KEY_ID")),
            _ => {}
        }
        if self.region().await.is_none() {
            return Err(ConfigError::MissingField("AWS_REGION"));
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct S3Storage {
    client: Client,
    bucket_name: String,
}

impl S3Storage {
    pub async fn new(config: &AwsStorageConfig) -> Result<Self, StorageError> {
        // Resolve region once - validation already ensured it exists
        let region = config.region().await.ok_or_else(|| {
            tracing::error!("AWS_REGION must be set");
            StorageError::OperationFailed
        })?;

        let mut s3_config_builder = S3Config::builder()
            .behavior_version_latest()
            .http_client(https_client())
            .region(region)
            .credentials_provider(config.clone())
            // Adaptive retry mode self-heals transient S3 blips (throttling,
            // 5xx, socket resets) inside the SDK instead of surfacing them to
            // nx as a fatal build error. operation_attempt_timeout bounds a
            // single attempt (~10s); operation_timeout (below, per-call) is
            // the overall per-S3-call budget - each UploadPart/PutObject call
            // gets its own budget, so large artifacts aren't capped by a
            // single 10s window across the whole multipart upload.
            .retry_config(RetryConfig::adaptive().with_max_attempts(3))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(std::time::Duration::from_secs(config.timeout_seconds))
                    .operation_attempt_timeout(std::time::Duration::from_secs(10))
                    .build(),
            );

        // Configure for custom S3-compatible endpoints (MinIO, Hetzner, etc.)
        if let Some(endpoint_url) = &config.endpoint_url {
            s3_config_builder = s3_config_builder
                .endpoint_url(endpoint_url)
                .force_path_style(true); // Required for most S3-compatible services
        }

        let s3_config = s3_config_builder.build();

        let client = Client::from_conf(s3_config);

        Ok(Self {
            client,
            bucket_name: config.bucket_name.clone(),
        })
    }
}

/// Reads from `data` into a buffer until it reaches `part_size` bytes or the
/// stream ends, returning `(buffer, eof)`. This is the boundary math that
/// decides simple-PutObject vs multipart in `store()` below, factored out so
/// it's unit-testable against a plain in-memory `AsyncRead` - no AWS SDK
/// mocking required. A stream read error means the client disconnected/reset
/// mid-upload, which is logged at WARN (not an S3/server failure) and
/// surfaced as `StorageError::ClientAbort` so the caller maps it to a 4xx.
async fn fill_part<R: AsyncRead + Send + Unpin>(
    data: &mut ReaderStream<R>,
    part_size: usize,
    hash: &str,
    bytes_received: &mut usize,
) -> Result<(Vec<u8>, bool), StorageError> {
    let mut buffer = Vec::with_capacity(part_size);
    while buffer.len() < part_size {
        match data.next().await {
            Some(Ok(chunk)) => {
                *bytes_received += chunk.len();
                buffer.extend_from_slice(&chunk);
            }
            Some(Err(e)) => {
                tracing::warn!(
                    hash,
                    bytes_received = *bytes_received,
                    error = %e,
                    "client aborted PUT body stream"
                );
                return Err(StorageError::ClientAbort);
            }
            None => return Ok((buffer, true)),
        }
    }
    Ok((buffer, false))
}

#[async_trait]
impl StorageProvider for S3Storage {
    async fn exists(&self, hash: &str) -> Result<bool, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(hash)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if matches!(e.as_service_error(), Some(HeadObjectError::NotFound(_))) {
                    return Ok(false);
                }
                log_s3_error("head", hash, &e);
                Err(StorageError::OperationFailed)
            }
        }
    }

    async fn store(
        &self,
        hash: &str,
        mut data: ReaderStream<impl AsyncRead + Send + Unpin>,
    ) -> Result<(), StorageError> {
        if self.exists(hash).await? {
            return Err(StorageError::AlreadyExists);
        }

        // Fill one part's worth of the body before deciding how to upload it.
        // Bodies that fit in a single part use plain PutObject; larger bodies
        // switch to multipart so we never buffer more than ~one part of a
        // (potentially multi-GB) artifact in memory (replaces the old
        // read-everything-into-a-Vec approach).
        let mut bytes_received: usize = 0;
        let (mut buffer, mut eof) =
            fill_part(&mut data, MULTIPART_PART_SIZE, hash, &mut bytes_received).await?;

        if eof {
            // Whole body fit in one part: simple PutObject, no multipart
            // bookkeeping (and nothing to ever abort/leak).
            let body = aws_sdk_s3::primitives::ByteStream::from(buffer);
            return self
                .client
                .put_object()
                .bucket(&self.bucket_name)
                .key(hash)
                .body(body)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| {
                    log_s3_error("put", hash, &e);
                    StorageError::OperationFailed
                });
        }

        // Body exceeds one part - go multipart. `buffer` is ~MULTIPART_PART_SIZE
        // bytes at this point (the loop above only exits early via `eof`,
        // handled above).
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(hash)
            .send()
            .await
            .map_err(|e| {
                log_s3_error("multipart-create", hash, &e);
                StorageError::OperationFailed
            })?;

        let upload_id = match create.upload_id() {
            Some(id) => id.to_string(),
            None => {
                tracing::error!(
                    operation = "multipart-create",
                    hash,
                    "S3 create_multipart_upload returned no upload_id"
                );
                return Err(StorageError::OperationFailed);
            }
        };

        let mut completed_parts: Vec<CompletedPart> = Vec::new();
        let mut part_number: i32 = 1;

        // Upload parts until the stream is drained. Any failure (client abort
        // or S3 error) breaks out of this block so we can always reach the
        // AbortMultipartUpload cleanup below - an incomplete multipart upload
        // left dangling in S3 is billed storage with nothing to show for it.
        let upload_result: Result<(), StorageError> = async {
            loop {
                let part_body = std::mem::take(&mut buffer);
                if !part_body.is_empty() {
                    let part_len = part_body.len();
                    let res = self
                        .client
                        .upload_part()
                        .bucket(&self.bucket_name)
                        .key(hash)
                        .upload_id(&upload_id)
                        .part_number(part_number)
                        .content_length(part_len as i64)
                        .body(aws_sdk_s3::primitives::ByteStream::from(part_body))
                        .send()
                        .await
                        .map_err(|e| {
                            log_s3_error("multipart-part", hash, &e);
                            StorageError::OperationFailed
                        })?;

                    let e_tag = res.e_tag().ok_or_else(|| {
                        tracing::error!(
                            operation = "multipart-part",
                            hash,
                            part_number,
                            "S3 upload_part response missing ETag"
                        );
                        StorageError::OperationFailed
                    })?;

                    completed_parts.push(
                        CompletedPart::builder()
                            .part_number(part_number)
                            .e_tag(e_tag)
                            .build(),
                    );
                    part_number += 1;
                }

                if eof {
                    break;
                }

                let (next_buffer, next_eof) =
                    fill_part(&mut data, MULTIPART_PART_SIZE, hash, &mut bytes_received).await?;
                buffer = next_buffer;
                eof = next_eof;
            }
            Ok(())
        }
        .await;

        if let Err(err) = upload_result {
            if let Err(abort_err) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket_name)
                .key(hash)
                .upload_id(&upload_id)
                .send()
                .await
            {
                // We're already returning an error; the abort failing on top
                // means S3 is left holding a dangling multipart upload (billed
                // storage, no object). Log loudly so it can be cleaned up
                // manually - see README for the recommended lifecycle rule
                // that makes this self-healing.
                log_s3_error("multipart-abort", hash, &abort_err);
            }
            return Err(err);
        }

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(hash)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            )
            .send()
            .await
            .inspect_err(|e| {
                log_s3_error("multipart-complete", hash, e);
            })
            .map_err(|_| StorageError::OperationFailed)?;

        Ok(())
    }

    async fn retrieve(
        &self,
        hash: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(hash)
            .send()
            .await
            .map_err(|e| {
                if matches!(e.as_service_error(), Some(GetObjectError::NoSuchKey(_))) {
                    return StorageError::NotFound;
                }
                log_s3_error("get", hash, &e);
                StorageError::OperationFailed
            })?;

        // Direct streaming - no buffering
        Ok(Box::new(result.body.into_async_read()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    /// An AsyncRead that always fails - simulates a client disconnecting or
    /// resetting the connection mid-upload, without needing a real socket or
    /// the AWS SDK.
    struct FailingReader;

    impl AsyncRead for FailingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("connection reset by peer")))
        }
    }

    #[tokio::test]
    async fn fill_part_returns_full_buffer_and_eof_when_body_smaller_than_part_size() {
        let cursor = std::io::Cursor::new(vec![1u8, 2, 3, 4, 5]);
        let mut stream = ReaderStream::new(cursor);
        let mut bytes_received = 0usize;

        let (buffer, eof) = fill_part(&mut stream, 1024, "test-hash", &mut bytes_received)
            .await
            .expect("should succeed");

        assert!(eof, "body smaller than part_size must report eof");
        assert_eq!(buffer, vec![1, 2, 3, 4, 5]);
        assert_eq!(bytes_received, 5);
    }

    #[tokio::test]
    async fn fill_part_stops_at_part_size_boundary_without_eof() {
        let data = vec![7u8; 30];
        let cursor = std::io::Cursor::new(data.clone());
        let mut stream = ReaderStream::new(cursor);
        let mut bytes_received = 0usize;

        let (buffer, eof) = fill_part(&mut stream, 10, "test-hash", &mut bytes_received)
            .await
            .expect("should succeed");

        // Must have flushed a full part and not yet hit the end of the stream.
        assert!(!eof);
        assert!(buffer.len() >= 10, "buffer should reach the part boundary");
        assert!(buffer.len() <= data.len());
        assert_eq!(bytes_received, buffer.len());
    }

    #[tokio::test]
    async fn fill_part_on_empty_body_reports_eof_with_empty_buffer() {
        let cursor = std::io::Cursor::new(Vec::<u8>::new());
        let mut stream = ReaderStream::new(cursor);
        let mut bytes_received = 0usize;

        let (buffer, eof) = fill_part(&mut stream, 1024, "test-hash", &mut bytes_received)
            .await
            .expect("should succeed");

        assert!(eof);
        assert!(buffer.is_empty());
        assert_eq!(bytes_received, 0);
    }

    #[tokio::test]
    async fn fill_part_maps_stream_read_error_to_client_abort_not_operation_failed() {
        let mut stream = ReaderStream::new(FailingReader);
        let mut bytes_received = 0usize;

        let result = fill_part(&mut stream, 1024, "test-hash", &mut bytes_received).await;

        // A client-side disconnect must map to ClientAbort (-> 4xx), never
        // OperationFailed (-> 500) - that's the whole point of fix #2.
        assert!(matches!(result, Err(StorageError::ClientAbort)));
    }

    #[test]
    fn multipart_part_size_meets_s3_minimum() {
        // S3 requires every part except the last to be >= 5MiB. This is a
        // `const`, so assert it via a const block to keep clippy happy while
        // still catching a future accidental shrink below the S3 minimum.
        const _: () = assert!(MULTIPART_PART_SIZE >= 5 * 1024 * 1024);
    }
}
