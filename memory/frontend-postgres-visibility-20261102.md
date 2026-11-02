# Frontend PostgreSQL visibility — 2026-11-02

Branch: `t3/frontend-postgres-visibility-gate`.

## Implementation

- PostgreSQL configured stores now select the configured separate `visibilityStore`, using its database, endpoint, credentials, TLS, connection attributes, pool sizes and lifetime. Task/domain/history persistence stays on the default store.
- `PostgresTaskStore::connect_visibility_only` shares pool construction but skips task-schema validation, because the visibility database contains no task tables. Launcher already installs the selected visibility store for Frontend.
- Frontend mixed workflow accepts `database=postgres` (MySQL remains default). Disposable resource-limited PostgreSQL 13 lane creates cadence and cadence_visibility; Go auto-setup installs schemas. Reuses all existing SDK start/complete/query/list-open/list-closed and Frontend/Matching restart checks, with Go History/Worker and no Go Frontend/Matching.
- Runner ubuntu-latest, timeout 85 minutes, small log artifacts retained three days, concurrency cancellation retained.

## Validation

No local Cargo, rustfmt or Go commands. Python syntax and shell syntax checks passed locally.

Initial head: `f33086628b5002460c901bbd0bbcc0062eb5147e`.
- Package CI (fresh target, runner formatting enabled): https://github.com/auenkr/cadence-rust/actions/runs/41417225715 — pending.
- PostgreSQL mixed gate: https://github.com/auenkr/cadence-rust/actions/runs/41417230281 — **failure** at the initial head. Binary SHA256: `4fa4d5cb4f00e8792478b8aec3d5dd02fbf5a1a950c90612d9e329f0a329911d`. Fresh dedicated target `/tmp/cadence-cargo-target-fe-41417230281`.

Reproduce:
```
gh workflow run rust-ci.yml --ref t3/frontend-postgres-visibility-gate -f package=cadence-matching -f clean_target=true
gh workflow run frontend-mixed-gate.yml --ref t3/frontend-postgres-visibility-gate -f rust_ref=t3/frontend-postgres-visibility-gate -f database=postgres
```

## Exact runtime blocker (not a passing PostgreSQL acceptance result)

Gate artifact `frontend-mixed-gate`, `logs/rust-frontend.log` reports immediately after "configured stores opened":
```
thread 'main' panicked at postgres-0.19.14/src/connection.rs:66:22:
Cannot start a runtime from within a runtime.
```
Then client destruction during panic unwinding produces a second panic and abort. Go History and Rust Matching listening checks passed; both Rust Frontend listening checks failed. SDK checks were never reached. This run is not evidence of SDK/visibility behavior or PostgreSQL service completion.

Configured stores are constructed on `spawn_blocking` in `launcher.rs`, successfully opening both task and visibility pools. The next Frontend startup domain preload calls synchronous `source.list_domains` directly on the Tokio runtime (`launcher.rs`, run_frontend domain preload). The postgres synchronous client internally drives its own runtime, which cannot run there. Existing RPC paths also directly invoke synchronous domain-manager persistence (`grpc.rs` DescribeDomain; `cadence-service-core/src/handlers.rs` domain CRUD). A startup-only change would leave RPCs exposed. Required follow-up: place synchronous Frontend Postgres domain/history/visibility operations and client teardown on a blocking boundary, then rerun this lane through all SDK/restart checks. No startup-only workaround or claim of completion was added.

Retained evidence: runner artifact contains `commit.txt`, `binary.sha256`, `gate.log`, `logs/steps.json` and Frontend backtrace; retention is three days. The immutable run/head/hash and failure text above remain after artifact expiry.
