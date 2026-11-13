# Benchmark-only image: package selects Matching/Frontend launcher or standalone History.
ARG GO_IMAGE=cadence-go:ci

FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev gcc g++ make cmake perl git pkgconf openssl-dev openssl-libs-static linux-headers
WORKDIR /src
COPY go/idls go/idls
COPY go/proto go/proto
COPY rust rust
WORKDIR /src/rust
ENV OPENSSL_STATIC=1 OPENSSL_DIR=/usr CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
ARG RUST_PACKAGE=cadence-server
RUN cargo build --release -p ${RUST_PACKAGE} -j2 \
    && cp target/release/${RUST_PACKAGE} /cadence-server \
    && sha256sum /cadence-server > /cadence-server.sha256

FROM ${GO_IMAGE} AS final
USER root
COPY --from=builder --chown=cadence:cadence /cadence-server /usr/local/bin/cadence-server
COPY --from=builder /cadence-server.sha256 /etc/cadence/cadence-server.sha256
COPY --chown=cadence:cadence rust/docker/cadence-auto-setup.sh /usr/local/bin/cadence-auto-setup.sh
ENV CADENCE_SCHEMA_HELPER=/usr/local/bin/cadence-auto-setup.sh
USER cadence
