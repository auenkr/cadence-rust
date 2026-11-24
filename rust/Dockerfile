# Rust Cadence server image, layered on the Go server image so that the file layout,
# user, ENV, EXPOSE, ENTRYPOINT (/docker-entrypoint.sh), CMD (/start-cadence.sh),
# dockerize template, config_template.yaml, dynamicconfig and schema are identical.
# Only /usr/local/bin/cadence-server is replaced (musl build so it runs on alpine).
# --auto-setup runs cadence-auto-setup.sh (cadence-sql-tool/cadence-cassandra-tool from
# the base image); the Go cadence-server binary is removed.
# Build (context = repo root):
#   docker build -f go/Dockerfile --target cadence-auto-setup -t cadence-go:ci go
#   docker build -f rust/Dockerfile --build-arg GO_IMAGE=cadence-go:ci -t cadence-rust:ci .
ARG GO_IMAGE=cadence-go:ci

FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev gcc g++ make cmake perl git pkgconf openssl-dev openssl-libs-static linux-headers
WORKDIR /src
COPY go/idls go/idls
COPY go/proto go/proto
COPY rust rust
WORKDIR /src/rust
# A/B with the old build: --build-arg RELEASE_LTO=off --build-arg RELEASE_CODEGEN_UNITS=16
ARG RELEASE_LTO=fat
ARG RELEASE_CODEGEN_UNITS=1
ENV OPENSSL_STATIC=1 OPENSSL_DIR=/usr CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=${RELEASE_LTO} CARGO_PROFILE_RELEASE_CODEGEN_UNITS=${RELEASE_CODEGEN_UNITS}
ARG PROFILE=0
# PROFILE=1 (perf runs only): frame pointers + symbols (no line tables: the line-tables LTO build killed the runner), same opt level.
RUN if [ "$PROFILE" = 1 ]; then export CARGO_PROFILE_RELEASE_STRIP=none RUSTFLAGS="-C force-frame-pointers=yes"; fi; \
    cargo build --release -p cadence-server -p cadence-history --features jemalloc-default,cadence-history/jemalloc-default -j2 \
    && sha256sum target/release/cadence-server target/release/cadence-history > /cadence-server.sha256

FROM ${GO_IMAGE} AS final
USER root
COPY --from=builder --chown=cadence:cadence /src/rust/target/release/cadence-server /usr/local/bin/cadence-server-real
COPY --from=builder --chown=cadence:cadence /src/rust/target/release/cadence-history /usr/local/bin/cadence-history
COPY --chown=cadence:cadence rust/docker/cadence-server-dispatch.sh /usr/local/bin/cadence-server
COPY --from=builder /cadence-server.sha256 /etc/cadence/cadence-server.sha256
COPY --chown=cadence:cadence rust/docker/cadence-auto-setup.sh /usr/local/bin/cadence-auto-setup.sh
ENV CADENCE_SCHEMA_HELPER=/usr/local/bin/cadence-auto-setup.sh
USER cadence
