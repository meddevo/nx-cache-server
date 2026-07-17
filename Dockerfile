# syntax=docker/dockerfile:1

# Build the AWS/S3 binary. A full rust image (not -slim) is used because `ring`
# still needs a C compiler; the project already dropped aws-lc-rs to avoid the
# heavier CMake/NASM toolchain.
FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
# The target-dir cache mount MUST be keyed per platform: in a multi-arch build
# the amd64 and arm64 stages run concurrently, and BuildKit cache mounts are
# shared by default (keyed only on the target path). Without the id, both
# cargos write target/release/nx-cache-aws at the same path and the `cp` can
# grab the other platform's (or a half-written) binary. The registry cache is
# arch-independent and cargo serialises access to it with file locks, so it
# stays shared.
ARG TARGETPLATFORM
RUN --mount=type=cache,id=cargo-target-${TARGETPLATFORM},target=/app/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release --bin nx-cache-aws && \
    cp target/release/nx-cache-aws /nx-cache-aws

# Distroless cc: glibc + ca-certificates (needed for rustls to trust S3's TLS
# roots), no shell. Matches the "static binary, no shell" runtime expectation.
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /nx-cache-aws /usr/local/bin/nx-cache-aws
# Ensure startup logs, the per-request access-log line, and the structured
# S3 error logs are emitted (all at INFO/ERROR - see server/middleware.rs).
ENV RUST_LOG=info
EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/nx-cache-aws"]
