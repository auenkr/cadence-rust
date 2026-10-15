# Rust Cadence implementation

Work-in-progress Rust rewrite of the Cadence server. Constraints:

- No API breaking changes: wire protocol (Thrift) and persistence schemas
  must stay compatible with existing Cadence SDKs and data.
- No program-logic changes: port behavior faithfully from `../go/`.

## Crates

- `crates/cadence-api` — shared API types mirroring the Thrift IDL
  (`go/idls`, `go/.gen`, `go/common/types`):
  `enums` (all 32 Thrift enums, Go-identical discriminants/strings, int
  encoding on the wire), `types` (core structs with Go-identical serde
  names, `[]byte` as base64 like Go `encoding/json`), `errors`, `tasktoken`
  (SDK-compatible task-token JSON), `domain` (`IsActiveActive`,
  failover-version lookup, `IsAConfigUpdateRequest`), `validation`
  (frontend request checks, `ValidateRetryPolicy`). Includes
  `tests/go_compat.rs` fixtures produced by Go JSON shapes.
- `crates/cadence-common` — shared utilities ported from `go/common`:
  `backoff` (exponential retry policy with Go-identical formula/jitter),
  `jitter`, `retry` (retry-with-backoff driver).
- `crates/cadence-server` — server binary: Go-compatible YAML config
  (env-var expansion, CSV NoSQL hosts, flexible ports; verified against
  `go/config/development.yaml`), service registry with Go-shaped health
  status, `start`/`version`/`check-config` CLI.
- `crates/cadence-persistence` — persistence layer mirroring
  `go/common/persistence` manager semantics: in-memory domain store
  (create/describe/update/deprecate/list) and workflow execution store
  (start with request-ID idempotency + `WorkflowIDReusePolicy`,
  describe, signal, request-cancel, terminate, history append/read).
- HTTP JSON API (`crates/cadence-server/src/handlers.rs`, axum):
  `/health`, domain CRUD (`/api/v1/domains/*` incl. delete),
  workflow lifecycle
  (`/api/v1/workflows/start|describe|signal|request-cancel|terminate|history|list|signal-with-start|query|count`),
  task lists with long-poll
  (`/api/v1/tasks/poll-decision|respond-decision-complete|respond-decision-failed|poll-activity|respond-activity-complete|respond-activity-failed`),
  admin (`/api/v1/tasklists/describe`, `/api/v1/search-attributes`).
  Decision completion applies ScheduleActivity/Complete/Fail/CancelWorkflow/
  ContinueAsNew/StartChild/SignalExternal/RequestCancelExternal/
  UpsertSearchAttributes/StartTimer/CancelTimer/RecordMarker/
  RequestCancelActivity with Go-style validation and history events.
  Child workflows propagate completion to parents; parent-close policy
  (terminate/cancel/abandon) is enforced; timers fire via a background
  loop; direct queries flow through buffered query + `queryResults`.
  A worker loop can drive workflows to completion
  (see `tests/e2e.rs::worker_simulation`).
  Request/response shapes are the Thrift-compatible `cadence-api` types;
  errors map to `{"type","message"}` with status codes (400/404/409).
  Task tokens use the Go JSON `TaskToken` shape.
  History reads support `maximumPageSize`/`nextPageToken` paging.
  Activity heartbeat/cancel-requested/canceled lifecycle included.

## Commands

```bash
cargo check --workspace
cargo clippy --workspace --all-targets
cargo test --workspace
cargo run -p cadence-server -- check-config --config ../go/config/development.yaml
cargo run -p cadence-server -- start --port 8080
```

## Porting notes

- Enum numeric values follow the Go `iota` order; `Display` matches Go
  `String()` (including quirks like `START_TIMER_DUPLICATE_I_D`).
- Unknown enum values are currently rejected on deserialize; Go preserves
  raw ints — full unknown-preserving enums are a follow-up.
- `#[non_exhaustive]` on enums forces callers to handle future variants.

The server embeds development settings from `rust/config/development.yaml`
when `--config` is omitted. A built executable can start from an empty working
directory without the Go checkout. Pass `--config /path/to/development.yaml`
to load an existing Cadence YAML file explicitly. Default runtime persistence
is still in memory (or development snapshots with `--data-dir`); configured
production SQL/NoSQL backends remain unported.

Matching now has an independent SQLite task-store component in
`cadence-persistence::matching_sqlite`, using the original Go `tasks` and
`task_lists` schemas and `thriftrw`/`thriftrw_snappy` blobs. Bidirectional tests
exchange a real database file with Go's SQL TaskStore, including source-domain
activity row keys and target-domain lease fencing. Independent queue allocation,
reader/ack recovery, cancellation-safe History delivery and registry lifecycle
components are also available. These components are not yet
wired into Matching RPC handlers or persistence configuration; the running server
continues to use the embedded execution engine's task queues.
