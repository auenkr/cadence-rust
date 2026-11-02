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
- Package CI (fresh target, runner formatting enabled): https://github.com/auenkr/cadence-rust/actions/runs/41417225715 — **success**, head `f33086628b5002460c901bbd0bbcc0062eb5147e`; runner formatting patch empty, clippy and package tests passed.
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

## Final workspace validation

Fresh uncached workspace clippy followed by workspace tests dispatched on the same implementation (only this evidence note changed): https://github.com/auenkr/cadence-rust/actions/runs/41418563149, head `91ba98e434b2f03b0db3bfd25b339bc006a02750`. In progress at handoff; do not interpret package CI as a workspace pass.

## Nested-runtime fix — implementation follow-up

Implementation commit `b5b9e0cae` (CI formatting follow-up `9aae9101d`) isolates
all synchronous PostgreSQL domain transactions, including metadata reads,
domain CRUD, replication queue operations and audit-log operations, on a scoped
thread when invoked from a Tokio runtime. This retains synchronous APIs,
borrowed inputs, SQL, transaction ordering and timeout behavior. Unlike
`block_in_place`, this boundary also works with current-thread runtimes. It
follows the MySQL runtime-owner principle that driver runtime work must happen
outside the caller runtime. Existing Frontend history/visibility readers already
use `spawn_blocking`.

Pooled PostgreSQL client destruction now uses the same boundary both when the
pool is dropped and when a lease is discarded. Unit tests call domain/name,
list and metadata reads from `#[tokio::test]`, expecting ordinary database
connection errors against an unavailable local endpoint rather than a nested
runtime panic; a second test asserts the boundary has no active Tokio runtime.

All Rust formatting/build/test commands run only in Actions. Initial Matching
CI https://github.com/auenkr/cadence-rust/actions/runs/41418924849 generated the
format patch but was superseded/cancelled by the persistence-test dispatch;
it is not a passing CI result. Persistence CI:
https://github.com/auenkr/cadence-rust/actions/runs/41419395684 (head
`9aae9101d`; pending at this checkpoint). Mixed PostgreSQL gate:
https://github.com/auenkr/cadence-rust/actions/runs/41418956694 (implementation
head `b5b9e0cae`; pending at this checkpoint). Final outcomes and binary identity
are recorded below when available.

### Confirmed follow-up outcomes

- PostgreSQL mixed gate **success**, 34/34 steps, `RESULT failures=0`, gate
  exit 0: https://github.com/auenkr/cadence-rust/actions/runs/41418956694.
  Head `b5b9e0cae8bf30cba3934c408ec88ecf742b72e5`.
  Fresh dedicated target `/tmp/cadence-cargo-target-fe-41418956694`.
  Rust binary SHA256:
  `7a77873938cd1f8fa850ab02a97b1e2e270ce3695da262a77b8457a134ac95f9`.
  Domain registration/description over TChannel and gRPC, SDK workflow start,
  completion, signal/query, list-open/list-closed, and Frontend kill/restart
  recovery all passed with Rust Frontend + Matching and Go History/Worker.
  This lane explicitly exercises Frontend restart; do not infer a Matching
  restart from `sdk-restart-handshake` or claim full service parity.
- Persistence CI **success**: https://github.com/auenkr/cadence-rust/actions/runs/41419395684.
  Head `9aae9101da4133951844e749c57fd0367046e5dc`. Fresh target, clippy then
  tests: 377 passed, 13 ignored. Both `runtime_boundary_tests` passed.
- Formatting-only commit `9aae9101d` applies the Actions-generated patch;
  mixed-gate implementation is identical apart from whitespace.
- Matching package CI **success** (fresh target, clippy then tests):
  https://github.com/auenkr/cadence-rust/actions/runs/41420119179,
  head `9aae9101da4133951844e749c57fd0367046e5dc`.
- Frontend package CI dispatched:
  https://github.com/auenkr/cadence-rust/actions/runs/41421435035,
  head `9aae9101da4133951844e749c57fd0367046e5dc`; pending at this checkpoint.

### Final validation complete

- Frontend package CI **success**, fresh target, clippy then tests (149 passed):
  https://github.com/auenkr/cadence-rust/actions/runs/41421435035,
  head `9aae9101da4133951844e749c57fd0367046e5dc`.
- Final fresh uncached **workspace clippy followed by workspace tests: success**:
  https://github.com/auenkr/cadence-rust/actions/runs/41422156747,
  head `4df07d90ad30a7ffb6311e820a1aa0760d6e2d9b` (`clean_target=true`).
  Only evidence/dashboard documentation differs from the package-tested source.

The nested-runtime blocker is resolved and the PostgreSQL lane is green.
No local Cargo/rustfmt/Go commands were run. Branch remains
`t3/frontend-postgres-visibility-gate`; no merge or branch deletion performed.
