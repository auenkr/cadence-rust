# Rust Cadence server image, layered on the Go server image so that the file layout,
# user, ENV, EXPOSE, ENTRYPOINT (/docker-entrypoint.sh), CMD (/start-cadence.sh),
# dockerize template, config_template.yaml, dynamicconfig and schema are identical.
# Only /usr/local/bin/cadence-server is replaced (musl build so it runs on alpine).
# The Go binary is kept as cadence-server-go and used as the schema helper for
# --auto-setup until the schema updater is ported.
# Build (context = repo root):
#   docker build -f go/Dockerfile --target cadence-auto-setup -t cadence-go:ci go
#   docker build -f rust/Dockerfile --build-arg GO_IMAGE=cadence-go:ci -t cadence-rust:ci .
ARG GO_IMAGE=cadence-go:ci

FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev gcc g++ make cmake perl git
WORKDIR /src
COPY go/idls go/idls
COPY go/proto go/proto
COPY rust rust
WORKDIR /src/rust
ENV CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
RUN cargo build --release -p cadence-server -j2 \
    && sha256sum target/release/cadence-server > /cadence-server.sha256

FROM ${GO_IMAGE} AS final
USER root
RUN mv /usr/local/bin/cadence-server /usr/local/bin/cadence-server-go
COPY --from=builder --chown=cadence:cadence /src/rust/target/release/cadence-server /usr/local/bin/cadence-server
COPY --from=builder /cadence-server.sha256 /etc/cadence/cadence-server.sha256
ENV CADENCE_SCHEMA_HELPER=cadence-server-go
USER cadence
