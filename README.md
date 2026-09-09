# Nx Remote Cache Server (meddevo fork)

[![Test](https://github.com/meddevo/nx-cache-server/actions/workflows/test.yml/badge.svg)](https://github.com/meddevo/nx-cache-server/actions/workflows/test.yml)
[![Nx smoke test](https://github.com/meddevo/nx-cache-server/actions/workflows/smoke.yml/badge.svg)](https://github.com/meddevo/nx-cache-server/actions/workflows/smoke.yml)
[![Image](https://github.com/meddevo/nx-cache-server/actions/workflows/docker.yml/badge.svg)](https://github.com/meddevo/nx-cache-server/actions/workflows/docker.yml)

A small Rust/Axum server that puts an S3 bucket behind the Nx self-hosted remote cache API. Single static binary, streams artifacts straight between the client and S3, a few MB of RAM.

This is meddevo's fork of [nxcite/nx-cache-server](https://github.com/nxcite/nx-cache-server). Fixes that are not specific to our deployment go upstream as well; what stays here is what we learned running it behind an ALB on ECS:

- **Never a 5xx toward Nx.** A failed write answers `403`, a failed read `404`, a failed existence check on PUT stores anyway. Nx retries any 5xx six times and then fails the task that just succeeded.
- **Refuses to start on a bucket that rejects writes**, so a broken IAM policy fails the deploy instead of every build.
- **S3 self-probe every 60s** so an idle task that lost S3 still leaves a trace in the logs.
- **Every 4xx/5xx is diagnosable at `RUST_LOG=nx_cache_server=info`.** No debug logging, ever.
- **`Connection: close` on every response.** Kills the ALB keep-alive reuse race that produced sporadic 502s. Costs one TCP+TLS handshake per request, which is nothing for a CI cache.
- **Bounded S3 calls**: 3s connect, 10s per attempt, 3 attempts, standard (not adaptive) retry mode.
- **Read-only token** for untrusted CI jobs (CREEP mitigation, also upstream).

The contract this server implements is the Nx client, `packages/nx/src/native/cache/http_remote_cache.rs` in nrwl/nx, not the OpenAPI document (which said `202` for a stored artifact while the client required `200`). `smoke.yml` runs real Nx clients (previous major, `latest`, `next`) against the built server on every push and weekly, and fails if the client ever retries a write.

## Running it

### Image

Built for `linux/arm64` on every push to `master` and on version tags:

```bash
docker pull ghcr.io/meddevo/nx-cache-server:master
docker run --rm -p 3000:3000 \
  -e S3_BUCKET_NAME=your-bucket \
  -e SERVICE_ACCESS_TOKEN=your-bearer-token \
  -e AWS_REGION=eu-central-1 \
  ghcr.io/meddevo/nx-cache-server:master
```

Tags: `master`, `sha-<commit>`, and `X.Y.Z` for releases. Re-add `linux/amd64` in `docker.yml` if an x86 consumer ever appears.

### Binary

```bash
cargo build --release --bin nx-cache-aws
./target/release/nx-cache-aws
```

The release workflow (`release.yml`, manual dispatch) builds Linux, macOS and Windows binaries and attaches them to a GitHub release.

### Configuration

Every option is an environment variable or a CLI flag (`--help` lists them); flags win.

```bash
# Required
export S3_BUCKET_NAME="your-s3-bucket-name"
export SERVICE_ACCESS_TOKEN="your-bearer-token"       # read-write token for trusted builds

# Optional
export READ_ONLY_ACCESS_TOKEN="your-ro-token"         # read-only token for untrusted CI jobs, see "Cache poisoning"
export S3_ENDPOINT_URL="http://localhost:9000"        # S3-compatible services (MinIO etc.); enables path-style addressing
export S3_TIMEOUT="30"                                # whole-operation timeout in seconds (default 30); connect is fixed
                                                      # at 3s, one attempt at 10s, 3 attempts. Values above 60 log a warning.
export PORT="3000"                                    # default 3000
export BIND_ADDRESS="0.0.0.0"                         # default 0.0.0.0; "::" for IPv6/dual-stack
export RUST_LOG="nx_cache_server=info"                # the only log level you should ever need

# AWS credentials and region: auto-discovered (IAM role, config files, SSO, EC2/ECS metadata).
# Set explicitly only when running outside AWS without a profile.
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."                        # temporary credentials only
```

### IAM

The task role needs, on the cache bucket's objects:

```
s3:GetObject
s3:PutObject
s3:AbortMultipartUpload
```

`HeadObject` is covered by `s3:GetObject`; `CreateMultipartUpload`, `UploadPart` and `CompleteMultipartUpload` by `s3:PutObject`. A missing `s3:PutObject` is caught at startup (see below); a missing `s3:GetObject` shows up as every read being a `404` with an `AccessDenied` at `ERROR` next to it.

### Startup and health

On boot the server writes one object, `_selfprobe.nx-cache-server`, to the bucket. A definite refusal (any 4xx: wrong policy, wrong credential, wrong bucket) stops the boot. An unreachable S3 or a 5xx is only logged, so an S3 blip during a task replacement cannot turn into a crash loop. The same key is then `HeadObject`ed every 60 seconds by the self-probe and overwritten in place on every later start; it is the only object the server creates that is not a cache entry.

```bash
curl http://localhost:3000/health   # "OK", no auth
```

`/health` only says the process is up. S3 reachability is in the log, one `s3 self-probe` INFO line per minute with `reachable=true|false` and `duration_ms`.

### Client

```bash
export NX_SELF_HOSTED_REMOTE_CACHE_SERVER="https://your-cache-host"
export NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN="your-bearer-token"   # SERVICE_ACCESS_TOKEN or READ_ONLY_ACCESS_TOKEN
```

See the [Nx documentation](https://nx.dev/recipes/running-tasks/self-hosted-caching#usage-notes) for the rest.

## Observability & Diagnosability

Operating rule: **`RUST_LOG=nx_cache_server=info` must be sufficient to fully diagnose any 4xx/5xx response.** Debug logging on the AWS SDK produces enough volume to rotate a small log window out in minutes on a busy server, which is exactly what happened during the incident that motivated this rule.

At INFO you get:

- **One access-log line per request** (method, path, status, `duration_ms`, and `bytes` when a `Content-Length` is set), including `/health` and auth failures.
- **One `s3 self-probe` line per minute**, see above.
- **One structured error line per S3 failure**, carrying `operation` (`startup-probe`/`head`/`get`/`put`/`multipart-create`/`multipart-part`/`multipart-complete`/`multipart-abort`), the cache hash, and the AWS SDK's request id(s) plus the full error detail. Enough for an AWS support case without re-running anything.
- **Client-side aborts are `WARN`, not `ERROR`**, and answered `400`. A client disconnecting mid-upload is not a server failure and must not page anyone.

What the status codes mean, and why they are not what a generic HTTP server would send:

| Situation | Response | Why |
|---|---|---|
| Artifact stored | `200` | The Nx client matches PUT success against exactly `200`. Anything else, including the OpenAPI doc's `202`, makes it re-upload. |
| Key already present | `409` | Nx: "not stored, carry on". Body is drained first so the client does not see a reset socket. |
| Read-only token on PUT | `403` | Nx: "not stored, carry on". |
| **S3 write failed** | `403` | Same as above. The alternative is a 5xx, which Nx retries six times (re-uploading each time) and then fails the task. Cost: one later cache miss. The S3 error is still at `ERROR`; alert on that line, not on the status. |
| **S3 read failed** | `404` | A failed read is a cache miss. Nx recomputes on 404; on a 5xx it aborts the whole run as a misconfigured endpoint ([nrwl/nx#36107](https://github.com/nrwl/nx/issues/36107)). |
| Existence check on PUT failed | store anyway | Keys are content-addressed, so a duplicate write is a byte-identical no-op. |
| Client disconnected mid-upload | `400` | Not a server failure. |

Every response carries `Connection: close`.

## Uploads: Multipart Streaming & S3 Lifecycle Cleanup

PUT bodies are streamed to S3 in ~8MiB parts. Small bodies use a single `PutObject`; anything larger uses multipart upload, so the server never holds more than roughly one part of a multi-GB artifact in memory. If a part fails (including the client disconnecting), the in-progress upload is aborted via `AbortMultipartUpload`.

**Recommended:** a process kill or network partition can still leave an incomplete multipart upload behind, and incomplete parts are billed storage with no object to show for it. Add an [S3 Lifecycle rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html#mpu-abort-incomplete-mpu-lifecycle-config) that expires them:

```json
{
  "Rules": [
    {
      "ID": "abort-incomplete-multipart-uploads",
      "Status": "Enabled",
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 3 }
    }
  ]
}
```

## Cache poisoning (CVE-2025-36852 / CREEP)

If untrusted contributors can run CI with cache **write** access (typically pull request builds), they can pre-seed the cache entry for a hash that a trusted branch will later compute, and the trusted build replays the poisoned artifact ([CVE-2025-36852, "CREEP"](https://nx.dev/blog/cve-2025-36852-critical-cache-poisoning-vulnerability-creep)). Write-once semantics do not prevent this: the attack writes *first*, it never overwrites.

Keep untrusted jobs read-only:

```bash
export SERVICE_ACCESS_TOKEN="your-rw-token"     # trusted builds (main/release): read-write
export READ_ONLY_ACCESS_TOKEN="your-ro-token"   # untrusted builds (PRs): read-only
```

Set `NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN` to the read-only token in PR pipelines and to the read-write token only in trusted-branch pipelines. The read-only token reads as usual and gets `403` on writes, which Nx treats as "not stored, carry on". Both tokens are compared in constant time.

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Router behaviour is tested in `src/server/mod.rs` against in-memory storage mocks; add cases there. See `AGENTS.md` for how changes to anything Nx sees are expected to be justified.
