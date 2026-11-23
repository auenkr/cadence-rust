# cadence-rust

Cadence Rust is a Rust implementation of the Cadence workflow server (Frontend, Matching, and History) that existing Cadence SDKs and the Go Worker service can use unchanged, with the same Thrift/TChannel and gRPC APIs and persistence schemas for MySQL, PostgreSQL, and Cassandra.

## Status

- **Rust:** Frontend, Matching, and History.
- **Go:** Worker service.
- **Persistence:** MySQL, PostgreSQL, and Cassandra.
- **Project progress:** [progress dashboard](memory/progress-dashboard.html) and [test and validation evidence](memory/evidence/).

## Repository layout

- `rust/` — Rust server workspace, service implementations, configuration, SQL, and container build.
- `go/` — upstream Cadence Go reference implementation as a submodule, pinned at `bb4833c865311f2e22eb6dfe73cad52b53dbaf4b`.
- `go-overlay/` — project-owned Go tests and oracle changes applied to the reference checkout.
- `tests/` — SDK compatibility checks, mixed-service gates, recovery and soak harnesses, and fixtures.
- `Benchmark/` — fixed-arrival service benchmark drivers and report tools.
- `scripts/` — Go overlay application and compatibility/oracle helpers.
- `memory/` — migration notes, progress, and reproducible validation evidence.
- `cadence-web/` — pinned Cadence Web submodule, including its upstream Node gRPC client and IDL inputs.

## Getting started

Clone the repository and initialize its submodules, then apply the Go test overlay:

```sh
git clone --recursive https://github.com/auenkr/cadence-rust.git
cd cadence-rust
git submodule update --init --recursive
scripts/go-overlay.sh
```

Build the Rust server:

```sh
cargo build --release --manifest-path rust/Cargo.toml
```

The Docker Hub publish workflow publishes per-service images under the `auenkr` namespace: `auenkr/cadence-rust-frontend`, `auenkr/cadence-rust-matching`, `auenkr/cadence-rust-history`, and `auenkr/cadence-go-worker`. Tags default to the short commit SHA; the workflow can also publish `latest`.

## CI and compatibility workflows

- `rust-ci` — Rust formatting, Clippy, and workspace or selected-crate tests.
- `*-mixed-gate` — run a Rust service with the other Go services and a selected database to check SDK and service interoperability.
- `*-soak` — sustained workflow traffic with service restart and resource monitoring.
- `sdk-matching-benchmark` — paired Go/Rust fixed-arrival service and cluster benchmarks.
- `docker-hub-publish` — builds and publishes per-service Docker Hub images.

## Benchmarks

The [2026-11-21 benchmark rerun](memory/evidence/bench-rerun-20261121.md) measured roughly 40–50% lower CPU use and 12–38% lower memory use for Rust in the reported lanes. p99 latency was equal or better except Frontend, where it was 3% higher, within run-to-run noise. At 40 workflows per second, Go p99 was 580 ms and Rust p99 was 108 ms.

## License

Apache License 2.0. This project is derived from Cadence, maintained upstream by [Uber and cadence-workflow](https://github.com/cadence-workflow/cadence). See the upstream license in `go/LICENSE` and the Rust license files.
