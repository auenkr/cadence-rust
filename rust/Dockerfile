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

FROM rust:1-alpine AS chef
RUN apk add --no-cache musl-dev gcc g++ make cmake perl git pkgconf openssl-dev openssl-libs-static linux-headers
RUN cargo install cargo-chef --locked --version 0.1.68
WORKDIR /src

# Dependency-only layer: the recipe only changes when Cargo.toml/Cargo.lock change, so the
# (expensive) third-party crate compile is reused from the layer cache across source edits.
FROM chef AS planner
COPY go/idls go/idls
COPY go/proto go/proto
COPY rust rust
WORKDIR /src/rust
RUN cargo chef prepare --recipe-path /recipe.json

FROM chef AS builder
COPY go/idls go/idls
COPY go/proto go/proto
WORKDIR /src/rust
# A/B with the old build: --build-arg RELEASE_LTO=off --build-arg RELEASE_CODEGEN_UNITS=16
# Gates use RELEASE_LTO=thin RELEASE_CODEGEN_UNITS=16; benchmarks keep the fat/1 defaults.
ARG RELEASE_LTO=fat
ARG RELEASE_CODEGEN_UNITS=1
ENV CARGO_BUILD_JOBS=2 OPENSSL_STATIC=1 OPENSSL_DIR=/usr CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=${RELEASE_LTO} CARGO_PROFILE_RELEASE_CODEGEN_UNITS=${RELEASE_CODEGEN_UNITS}
COPY rust/vendor /src/rust/vendor
COPY --from=planner /recipe.json /recipe.json
RUN cargo chef cook --release --recipe-path /recipe.json -p cadence-server -p cadence-history --features jemalloc-default,cadence-history/jemalloc-default
COPY rust /src/rust
ARG PROFILE=0
ARG HEAP_PROFILE=0
# PROFILE=1 (perf runs only): frame pointers + symbols (no line tables: the line-tables LTO build killed the runner), same opt level.
RUN if [ "$PROFILE" = 1 ]; then export CARGO_PROFILE_RELEASE_STRIP=none RUSTFLAGS="-C force-frame-pointers=yes"; fi; \
    HF=""; if [ "$HEAP_PROFILE" = 1 ]; then export CARGO_PROFILE_RELEASE_STRIP=none; HF=",cadence-history/heap-profile"; fi; \
    cargo build --release -p cadence-server -p cadence-history --features jemalloc-default,cadence-history/jemalloc-default$HF \
    && sha256sum target/release/cadence-server target/release/cadence-history > /cadence-server.sha256

# Exportable binaries (same paths as the builder stage): `buildx build --target binaries --output type=local,dest=DIR`
# with a gha layer cache, then `--build-context builder=DIR` for the final image (GO_IMAGE stays in the local daemon).
FROM scratch AS binaries
COPY --from=builder /src/rust/target/release/cadence-server /src/rust/target/release/cadence-server
COPY --from=builder /src/rust/target/release/cadence-history /src/rust/target/release/cadence-history
COPY --from=builder /cadence-server.sha256 /cadence-server.sha256

FROM ${GO_IMAGE} AS final
USER root
COPY --from=builder --chown=cadence:cadence /src/rust/target/release/cadence-server /usr/local/bin/cadence-server-real
COPY --from=builder --chown=cadence:cadence /src/rust/target/release/cadence-history /usr/local/bin/cadence-history
COPY --chown=cadence:cadence rust/docker/cadence-server-dispatch.sh /usr/local/bin/cadence-server
COPY --from=builder /cadence-server.sha256 /etc/cadence/cadence-server.sha256
COPY --chown=cadence:cadence rust/docker/cadence-auto-setup.sh /usr/local/bin/cadence-auto-setup.sh
ENV CADENCE_SCHEMA_HELPER=/usr/local/bin/cadence-auto-setup.sh
USER cadence
