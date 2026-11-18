# Benchmark-only image: package selects Matching/Frontend launcher or standalone History.
ARG GO_IMAGE=cadence-go:ci

FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev gcc g++ make cmake perl git pkgconf openssl-dev openssl-libs-static linux-headers
WORKDIR /src
COPY go/idls go/idls
COPY go/proto go/proto
COPY rust rust
WORKDIR /src/rust
# PROFILE=1 (perf runs only): line tables + frame pointers, same opt level.
ARG PROFILE=0
ENV OPENSSL_STATIC=1 OPENSSL_DIR=/usr CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
RUN if [ "$PROFILE" = 1 ]; then printf 'CARGO_PROFILE_RELEASE_DEBUG=line-tables-only\nCARGO_PROFILE_RELEASE_STRIP=none\nRUSTFLAGS="-C force-frame-pointers=yes"\n' > /profile.env; else : > /profile.env; fi
RUN rustc -Vv > /cadence-toolchain.txt
ARG RUST_PACKAGE=cadence-server
RUN set -a; . /profile.env; set +a; if [ "${RUST_PACKAGE}" = all ]; then \
      cargo build --release -p cadence-server -p cadence-history --features cadence-server/jemalloc-default,cadence-history/jemalloc-default -j2 && \
      cp target/release/cadence-server /cadence-server && cp target/release/cadence-history /cadence-history; \
    else cargo build --release -p ${RUST_PACKAGE} --features ${RUST_PACKAGE}/jemalloc-default -j2 && cp target/release/${RUST_PACKAGE} /cadence-server; fi \
    && sha256sum /cadence-server > /cadence-server.sha256

FROM ${GO_IMAGE} AS runtime
USER root
COPY --from=builder --chown=cadence:cadence /cadence-server /usr/local/bin/cadence-server
COPY --from=builder /cadence-toolchain.txt /etc/cadence/cadence-toolchain.txt
COPY --from=builder /cadence-server.sha256 /etc/cadence/cadence-server.sha256
COPY --chown=cadence:cadence rust/docker/cadence-auto-setup.sh /usr/local/bin/cadence-auto-setup.sh
ENV CADENCE_SCHEMA_HELPER=/usr/local/bin/cadence-auto-setup.sh
USER cadence

# All-Rust needs both launchers; reuse the exact same builder output.
FROM runtime AS history
COPY --from=builder --chown=cadence:cadence /cadence-history /usr/local/bin/cadence-server
FROM runtime AS final
