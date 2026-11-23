# Frontend Cassandra paging and dynamic config residuals, 2026-11-23

Branch: `fe-residual-8-dynconfig`. Code head: `1fcd8aef61730d8761f31fd9322f869c47102970`. Go baseline: pinned submodule `bb4833c865311f2e22eb6dfe73cad52b53dbaf4b` with `go-overlay/`.

## Cassandra paging

- Finding #8 in `fe-gap-wp-c2-20261123.md` now has unit coverage in `matching_domain_cassandra.rs`. An opaque driver state containing zero and non-UTF-8 bytes survives Cassandra response → API token → Cassandra request. A finished response produces an empty token, and an empty request token starts the query.
- The signed ListDomains path is implemented in `matching_domain_metadata.rs`, with delegation from `matching_domain_cassandra.rs`. The new unit test checks zero rejection at the persistence boundary, the maximum positive i32 selecting a paged request, and -1 and i32::MIN selecting the unpaged request while forwarding only a nonempty state. This covers the inputs to the vendored Scylla call. `session.rs::query_unpaged_with_paging_state` passes no page size and forwards the state into the Cassandra protocol request. The mixed SDK gate covers ordinary ListDomains pagination. It does not directly exercise the negative-size wire path.

## Dynamic config

The four-key list comes from `parity-fe-dynconfig-20261121.md`. The newer `dynconfig-filter-check-20261123.md` is a broader source scan for filter parity.

| Key | Go default and filter | Reader and Rust disposition |
|---|---|---|
| `frontend.throttledLogRPS` | 20, unfiltered | `service/frontend/config/config.go` → `service/frontend/service.go` → `common/resource/resource_impl.go` → `common/log/throttle.go`. Rust reads the same key from the production dynamic config overlay and shares the existing Go-compatible dynamic rate limiter across long-poll deadline diagnostics and the GetWorkflowExecutionHistory page-cap warning. |
| `frontend.pinotOptimizedQueryColumns` | Empty map, unfiltered | `service/frontend/service.go` supplies `common/persistence/pinot/pinot_visibility_store.go` with the query-validator config. Rust has Pinot visibility write publishing, with no Pinot query validator or read store. The key stays inert. |
| `system.enableLogCustomerQueryParameter` | false, domain name | `service/frontend/config/config.go` → `service/frontend/service.go` → `common/persistence/client/factory.go` → `common/persistence/visibility_hybrid_manager.go::logUserQueryParameters`, a sampled log for the Pinot response comparator. The helper has no callers in the pinned Go tree or overlays, and Rust has no comparator log path. The key stays inert. |
| `frontend.esVisibilityListMaxQPS` | Deprecated | Go marks it as never read. |

## GitHub Actions evidence

| Workflow and inputs | Tested head | Conclusion | Run |
|---|---|---|---|
| `rust-ci.yml`, `package=cadence-persistence` | `92a86dd8e658ac96db238c65258013360e20b224` (the paging commit, with identical persistence sources at code head) | Success. The three new paging tests passed. | https://github.com/auenkr/cadence-rust/actions/runs/45235657821 |
| `rust-ci.yml`, `package=cadence-frontend` | `1fcd8aef61730d8761f31fd9322f869c47102970` | Success. Formatting, Clippy, and tests passed. | https://github.com/auenkr/cadence-rust/actions/runs/45236037367 |
| `frontend-mixed-gate.yml`, `database=cassandra`, Kafka enabled by default | `1fcd8aef61730d8761f31fd9322f869c47102970` | Success. Logged `GATE SUMMARY failed: []`. | https://github.com/auenkr/cadence-rust/actions/runs/45236057000 |
| `rust-ci.yml`, `clean_target=true`, whole workspace | `1fcd8aef61730d8761f31fd9322f869c47102970` | Success. Fresh dedicated target, formatting, workspace Clippy followed by workspace tests. | https://github.com/auenkr/cadence-rust/actions/runs/45236707020 |

Cassandra mixed gate Rust binary SHA256: `a50bfcedee4714a9f8473705efc3710552536eb3b096081040abbc6cee26b687`. The runner built it from the recorded code head in `/tmp/cadence-cargo-target-fe-45236057000`.
