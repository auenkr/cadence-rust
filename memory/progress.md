# Progress log

- **2026-10-14 — Retries, pollers, follow-up decisions (96 tests, clippy clean).**
  Activity retries per retry policy (fatal/non-retriable/max-attempts stop;
  fresh Scheduled event + backoff requeue via background loop),
  `FailureOptions`/`heartbeatDetails` on fail requests, follow-up decision
  scheduled on terminal activity events (like Go transfer tasks), poller
  presence tracking surfaced in DescribeTaskList.
  Verified: retry unit tests + poller e2e.

- **2026-10-14 — Heartbeat, cancel, paged history (93 tests, clippy clean).**
  Activity heartbeat (`cancelRequested` reflects decision-driven cancel
  requests), respond-activity-canceled with `ActivityTaskCanceled` event,
  `cancel_requested_event_id` tracking. History pagination
  (`maximumPageSize`/`nextPageToken`, Go's Invalid NextPageToken error).
  Verified: store unit tests + e2e (heartbeat→cancel→page-walk).

- **2026-10-14 — Child workflows, queries, timers, admin (89 tests, clippy clean).**
  Decisions: ContinueAsNew (successor run, repointed tree), StartChild
  (initiated/started/failed events, duplicate detection), SignalExternal
  and RequestCancelExternal (deliver or record Failed), UpsertSearchAttributes
  (merge + event). Unified close path with child-completion propagation to
  open parents and parent-close-policy enforcement (terminate/cancel/abandon).
  Embedded direct-query flow (buffer → decision `query`+`queryId` →
  `queryResults` → answer, reject conditions). Timer engine (pending
  deadlines + 500ms server loop firing `TimerFired`). Admin: describe-task-list
  (backlogs), search-attributes, count, delete-domain; list filters
  (workflowId/type/closeStatus). Verified: 25 persistence + 11 e2e tests
  (worker, child, query, timer, admin flows) and live curl runs.
  Follow-ups: Thrift-binary/gRPC transport for real SDKs, durable
  SQL/NoSQL backends, activity retries/timeouts/heartbeats, reset,
  batch, schedules/cron, visibility query language, worker/history
  service internals.

- **2026-10-14 — Matching + worker loop (75 tests green, clippy clean).**
  `cadence-api`: new `tasks` module (decision/activity poll/respond
  types, all task event attributes incl. `DecisionTaskFailed`), history
  events extended. `cadence-persistence`: decision/activity state
  machine (FIFO queues per task list, scheduled→started→completed/failed,
  attempt tracking, Go-shaped JSON task tokens), decision application
  (8 decision types with Go-style validation), `signal_with_start`.
  Server: 7 new task routes with long-poll (`timeoutSecs`, 204 on empty)
  plus signal-with-start. Verified: unit + `e2e.rs` worker simulation
  (start → poll → schedule activity → poll → complete → complete
  workflow, 11-event chain) and live `curl` run ending COMPLETED.
  Follow-ups: Thrift-binary/gRPC transport for real SDKs, durable
  SQL/NoSQL backends, remaining decisions (child workflows, signals,
  continue-as-new), timers firing, retries/timeouts, reset/query/batch,
  schedules, worker/history internals.

- **2026-10-14 — Runnable standalone server (65 tests green, clippy clean).**
  New `cadence-persistence` crate (in-memory domain + workflow execution
  stores mirroring `go/common/persistence` semantics: request-ID
  idempotency, `WorkflowIDReusePolicy`, history append on every mutation).
  New axum HTTP JSON API in `cadence-server` (`/health`,
  `/api/v1/domains/*`, `/api/v1/workflows/*`) serving the full core
  lifecycle with Go-identical validation and error mapping; `start` binds
  a real port. `cadence-api` gained `service_types` (domain CRUD,
  describe/list, history events). Verified: workspace tests
  (28+4+10+10+9+4 incl. `tests/e2e.rs` full lifecycle) plus a live
  `curl` run (register → start → describe → signal → history →
  terminate → list-closed).
  Follow-ups: Thrift-binary/gRPC transport for real SDKs, matching
  (poll/respond task queues), durable SQL/NoSQL backends, remaining
  service methods (reset, query, batch, schedules), worker/history
  internals.

- **2026-10-14 — Rust foundation port (48 tests green, clippy clean).**
  `cadence-api`: all 32 Thrift enums from `go/common/types/shared.go`
  (Go-identical discriminants, `String()` output, int wire encoding),
  core structs with Go-identical serde names (`[]byte` as base64),
  error shapes, SDK-compatible task-token JSON, domain helpers
  (`IsActiveActive`, `GetAllClusters`, `GetFailoverVersionForAttribute`,
  `IsAConfigUpdateRequest`), frontend validation (`ValidateRetryPolicy`,
  `CheckExecution`, start-request checks), plus `tests/go_compat.rs`
  Go-JSON fixtures. `cadence-common` (new crate): exponential backoff
  with Go-identical formula/jitter, jitter helpers, retry driver —
  ported from `go/common/backoff`. `cadence-server`: Go-compatible YAML
  config (env expansion, CSV NoSQL hosts; verified against
  `go/config/development.yaml`), service registry + Go-shaped health,
  `start`/`version`/`check-config` CLI.
  Verified: `cargo test --workspace` (25+4+10+9), `cargo clippy`
  clean, `check-config` on real Go configs.
  Follow-ups: unknown-preserving enums, remaining `common/types`
  structs (history/matching/admin/replicator), per-service handlers,
  persistence layer; CI working directories still need the `go/` prefix.

- **2026-10-14 — Monorepo move.** Moved the entire Go codebase from repo
  root into `go/` via `git mv` (2945 tracked files, renames staged).
  Restored `.gitmodules` (→ `go/idls`) and `.github/` to root (required
  by git/GitHub). Scaffolded `rust/` Cargo workspace (`cadence-api`,
  `cadence-server`) and `memory/` docs. Verified: `cargo test`
  in `rust/`, Go smoke compile of `go/common/types/...`.
  Follow-ups: fix CI working directories to `go/`, refresh `go/AGENTS.md`
  layout paths, start Thrift type port in `cadence-api`.

## Thrift transport implemented (Rust)

- Added `crates/cadence-thrift`: Apache thrift binary protocol codec (strict envelopes + NoWire structs), generic value model, converters for domains, workflow lifecycle, history/tasks, decisions, pending info, errors.
- Method/exception table generated from IDL (`service.rs`).
- `cadence-server` YARPC Thrift-over-HTTP dispatch supports 20 methods; NoWire request handling; declared exceptions use `Rpc-Status: error` headers.
- Smoke test via Go Cadence SDK client: register domain, start workflow, signal workflow succeed against Rust server.
- Go wire fixtures in `cadence-thrift/tests/fixtures` + wire-compat tests byte-compare encoder against thriftrw.
