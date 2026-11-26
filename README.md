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

The [2026-11-26 benchmark report](memory/benchmark-results-5f6504e05-20261126.md) compares Go and Rust (Frontend, Matching and History) on GitHub-hosted runners:

- **Throughput:** Rust handles 30–185% more workflows per second on MySQL and 28–38% more on PostgreSQL at concurrency 16 and 64.
- **Latency:** Rust's p50 is 15–67% lower in every configuration.
- **Workloads:** Rust gets 1.46× Go's throughput on activities, 1.40× on child workflows and 1.36× on long histories.
- **Resources:** Rust uses about 40–55% less CPU, and about half the memory for Frontend and Matching.
- **One workflow at a time, fixed rate:** Rust's p90 and p95 are 25–39% lower at every paced rate from 10 to 22 wf/s.
- **Recovery:** Rust becomes stable faster after a kill or a full restart.

Open gaps:

- With one workflow at a time and no rate cap, Rust's p90 is 94 ms against Go's 53 ms, caused by queueing at History's 20 rps limiter.
- On Cassandra, a full restart loses some workflows in Rust; the bug is being fixed.

## License

Apache License 2.0. This project is derived from Cadence, maintained upstream by [Uber and cadence-workflow](https://github.com/cadence-workflow/cadence). See the upstream license in `go/LICENSE` and the Rust license files.
