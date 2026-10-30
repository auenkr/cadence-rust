# Progress log

- **2026-10-25 — Matching evidence checkpoint.** Active-offer mixed gate
  40157002080 succeeded on `b1fda7fbff558f7f58b5c98cce9a563c93e9ecd9`; Rust
  Matching binary SHA256
  `59e202b88c25ad43e0da27c13a1809235f1c88cb53b3105db86ec6fe3a58ef8b`.
  The Go History scheduled task was delivered with zero persisted task rows
  and zero inserts, with Go Matching absent. Preserve the probe labels
  `preliminary-passed` and `acceptanceEligible:false`. Fresh integrated
  workspace run 40155846010 passed strict clippy and workspace tests (exit 0)
  on `1fc3a899c00c6eac5b2079b933092ca0df270313`; workspace-format advisory was
  1 while focused formatting was 0. Backend, transport, configstore and shard
  lanes are green on their individually recorded sources. Matching remains
  INCOMPLETE because gRPC TLS missing-identity startup parity is under a
  separate fix; revalidate the final workspace and mixed integration on the
  TLS-fixed source. Details: `memory/evidence/matching-final-candidate-20261024/`.

- **2026-10-17 — Matching strong/sticky query version gates.** `SupportsStickyQuery`
  is now ported exactly (uber-go/uber-java/empty impl need feature version
  >= 1.0.0; CLI and unknown impls do not). Added the previously missing
  `SupportsConsistentQuery` gate: `QueryOutcome::Worker` carries the worker's
  version info and `query_workflow` fails a strong-consistency query from a
  worker without consistent-query support (uber-go/uber-java >= 1.5.0, empty
  impl treated as supported) with `ClientVersionNotSupportedError`. Unit tests
  cover both gates. See `memory/matching-query-next.md`.

- **2026-10-17 — Matching semaphore manager + registry.** Ported Go
  `service/matching/semaphore` (identifier, registry, manager) with the
  `Created->Starting->Running->Stopped` lifecycle, startup scan rebuilding the
  free-set and owner reverse-index, conditional grant with confirm-hold /
  stale-index handling, `NoSlot` on a full bucket, and idle-bucket unloading.
  Added the persistence token/owner row surface (`scan_semaphore_bucket`,
  `get_semaphore_ownership_by_token`, `grant_semaphore_token`,
  `release_semaphore_token`) and rewired `AcquireSemaphore` with
  Go-compatible error mapping (including `ServiceBusyError` on `NotReady`).
  `serve_with` runs a 30s idle sweeper. 7 unit tests + 1 gRPC end-to-end test,
  full `cadence-server --lib` = 131 passed, strict Clippy clean. See
  `memory/matching-semaphore-next.md`.

- **2026-10-17 — Matching adaptive scaler + isolation balancer.** Ported Go
  `tasklist/adaptive_scaler.go` and `tasklist/isolation_balancer.go` in a new
  `matching_scaler.rs` (`Sustain`, write-count thresholds with downscale
  hysteresis, dense write-map rebuild, read drain, isolation-group goals and
  `ensureMinimumGroups`). A per-root-normal scaler daemon runs on root owners
  with live config, a weak-state lifecycle (no owner resurrection / registry
  pinning), versioned partition updates through the existing
  `update_partition_config` path, and the Go manual-update guard. Go defaults
  preserved. 7 scaler unit tests + registry integration/guard tests pass, full
  `cadence-server --lib` = 123 passed, strict all-target Clippy clean. See
  `memory/matching-scaler-next.md`.

- **2026-10-14 — Go SDK workflow running against Rust frontend.** Added
  `rust-cadence-version-workflow` with a Go worker and starter targeting the
  Rust YARPC/Thrift endpoint. Fixed the activity-schedule decision field IDs,
  Thrift timestamp nanoseconds, history long-poll deadline handling, and
  follow-up decision scheduling so empty replays do not spin and events arriving
  during a decision are replayed. Verified Go SDK activity execution and left
  `cadence-rust` tmux session running with an open, signal-waiting workflow.

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
# 2026-10-14: cadence-web local run

- Added `cadence-web` as a Git submodule at the repository root, pinned to
  the currently checked out upstream commit.
- On this ARM64 host, `ubercadence/web:latest` is AMD64-only. Installed its
  Node dependencies, downloaded the Cadence IDL revision declared by the web
  app, and generated the TypeScript protobuf definitions; the source UI runs
  natively instead of using Docker.
- Started `cadence-server` JSON/Thrift HTTP frontend on port 8080 and the
  cadence-web Next.js development server on port 8088 in tmux session
  `cadence-rust` (windows `rust` and `web`). `/health` and `/domains` return
  HTTP 200.
- Compatibility blocker confirmed: cadence-web uses Cadence gRPC APIs
  (`DomainAPI`, `WorkflowAPI`, `VisibilityAPI`, `WorkerAPI`, `ScheduleAPI`,
  `MetaAPI`; default peer port 7833). The Rust server has no gRPC listener;
  its handlers expose a custom JSON API plus Thrift-over-YARPC HTTP. The UI
  shell loads, but its gRPC data calls cannot connect until those services
  are ported. This also confirms the earlier gRPC migration report was
  correct.
- Verified the actual UI data route (`/api/clusters/cluster0/domains?pageSize=100`)
  returns HTTP 503 with gRPC status 14 / connection deadline, while the UI
  route and HTML shell return HTTP 200. The page is running but not connected
  to Rust data yet.


## 2026-10-14: native gRPC and default SDK transport migration

- Added `cadence-proto`: public protobuf/tonic bindings and reflection from
  pinned `go/idls`, with vendored ARM-compatible protoc. No Go compiler/server
  is needed to build or run the Rust server.
- Implemented concrete gRPC adapters for domain metadata and lifecycle, workflow
  lifecycle/read/history/query/task-list APIs, legacy and advanced visibility,
  worker poll/respond/heartbeat, and activity ByID APIs. Unported RPCs remain
  UNIMPLEMENTED. See `migration-inventory.md` for the exact adapter inventory;
  adapter presence does not prove complete engine behavior.
- Added native TChannel v2 Thrift on 7933, including init/ping, multiplexed calls,
  request/response fragmentation, CRC32/CRC32C, cancellation and TTL handling,
  application-error headers and the shared Rust Thrift dispatcher.
- Regenerated the full 56-method Thrift wire/exception table from pinned IDL.
  The previous table's claim of full generation hid missing visibility methods.
  Added legacy/advanced visibility/count/search-attributes Thrift adapters.
- Corrected cluster config alias (`clusterGroupMetadata`), local-domain default
  cluster metadata and validation, protobuf enum/time/error conversions, Thrift
  workflow timestamps/metadata, poller isolation by domain/task type and Go's
  five-minute poller-history window.
- Added native visibility SQL predicate evaluation for common builtin/keyword/
  scalar fields: boolean expressions, status/missing/time filters, IN/BETWEEN,
  ordering, case-insensitive LIKE prefixes and opaque cursor pagination. Unsupported
  full-text/analyzer/aggregation and cluster fields explicitly fail.
- History paging now uses Go's 1000-event limit and snapshot/run-bound cursors;
  SDK decision polls page large histories without admitting signals that arrive
  during a replay into that replay's continuation pages.
- Verified real cadence-web Node gRPC client workflow/activity completion with
  an 11-event history, typed already-started errors, opaque payloads, heartbeat,
  visibility and paging. The web UI's data routes now return real Rust data.
- Verified existing Go SDK v1.4.0-rc.3 client AND worker over native TChannel:
  1.65 MB workflow/activity payload round trip, fragmented requests/responses,
  CRC32, concurrent calls during long polls, Thrift nanosecond timestamps,
  visibility/schema and paging. Completed workflow had 14 history events.
  The compatibility driver disables sticky execution, whose full lifecycle is
  still unported. No Go Cadence server was used.
- Last completed workspace run after TChannel edge cases: 130 tests passed.
  Strict clippy also completed successfully. These runs precede the subsequent
  activity retry changes; they do not validate those changes.
- Runtime stays in tmux `cadence-rust`: Rust frontend on 8080/7833/7933 and
  cadence-web source development server on 8088. Native server default bind
  settings now follow frontend RPC config for gRPC/TChannel.

## 2026-10-14: checkpoint and activity retry findings

- Re-read root `AGENTS.md`: all Go behavior must be ported without API or program
  logic changes; findings belong in `memory/`, compatibility checks in `tests/`,
  and long processes in tmux session `cadence-rust`.
- Saved `resume.md` with runtime commands, verification evidence, source anchors,
  pitfalls, and explicit next actions. `migration-inventory.md` still reports
  35/57 concrete public gRPC adapters. Full migration remains incomplete.
- Compared activity retries against Go mutable state and backoff implementations.
  Go retains the original activity ScheduleID, does not append a Scheduled event
  for each retry, and defers retry-enabled ActivityTaskStarted history until the
  final attempt. No policy must mean no retry. Cancellation, fatal/size errors,
  expiration, attempt limits, jitter and caller backoff overrides matter.
- Began correcting Rust retry state, transient starts, heartbeat restoration and
  stale attempt tokens in `cadence-persistence/src/executions.rs`. Added serde
  defaults for new pending-activity fields so old development snapshots load.
  `cargo check -p cadence-server` completed successfully afterward. Behavioral
  validation was in progress at that point; results are recorded below.
- Corrected the stale retry unit expectation: Go keeps one scheduled activity ID,
  does not expose each retry attempt as a separate Started event, and materializes
  only the final attempt's start when terminal history is recorded. Added assertions
  for stable schedule ID, rejection of a prior-attempt token and final attempt
  history. Backoff checks cover Go's default coefficient, per-failure override,
  no-policy behavior and expiration. All 131 workspace tests passed (42
  persistence tests).
- Extended the real Go SDK client/worker driver to fail one activity attempt and
  succeed on retry. The first run found old visibility counts needed to account for
  the extra completed workflow; after correcting those expectations, the driver
  passed against the rebuilt Rust server. It verified one Scheduled and one final
  Started event. The 1.65 MB payload/TChannel checks also passed.
- Re-ran the upstream cadence-web Node gRPC compatibility driver against the new
  server binary; it passed with an 11-event completed history. Strict workspace
  clippy (`cargo clippy --workspace --all-targets -- -D warnings`) also passed.
- After correcting Go's default backoff coefficient, rebuilt/restarted the server
  and reran the SDK driver successfully. Retry history had 14 events; latest SDK
  domain `rust-tchannel-sdk-2c2b7f7d-e434-48dd-a241-082fecaf3169`.
- Rebuilt and restarted the Rust server in tmux `cadence-rust:rust`; the web remains
  in `cadence-rust:web`.
- Work remains uncommitted; `.gitmodules` and the cadence-web gitlink are staged.
  Temporary `/tmp` logs are evidence for this session, not durable project memory.

## 2026-10-14: full service schema audit and timeout paths

- User explicitly requires continued work through the entire Go migration, testing,
  and an audit that no services or methods are missing. Do not finish with a claim
  of completion based on the sample workflows or generated stubs.
- Broadened `cadence-proto` to compile every public/admin IDL and every internal
  Go protobuf (history, matching, shared, indexer). Added the exact shard-manager
  protobuf definitions pinned by `go/go.mod` under `rust/proto/shard-manager` with
  provenance. Builds need neither a Go module cache nor Go toolchain.
- The schema check compares generated descriptors with original source IDLs and
  verifies all 13 upstream services and all of their RPC names. Frontend reflection
  advertises registered services only; broad descriptors do not imply listeners.
- Extended migration inventory to public AND internal/administrative services:
  57 public methods and 107 internal/administrative methods (164 total). Current
  concrete adapters: 35 public and 17 internal (13 history, 4 matching). Many
  adapters still depend on incomplete engine behavior; this is not a parity count.
- Added typed workflow, decision, activity and child timeout history attributes,
  with exact Thrift field IDs and typed protobuf adapters. Opaque timeout/previous
  failure payloads and reset metadata have wire round-trip checks.
- Added the terminal workflow timeout path for runs with no workflow retry policy
  and no cron schedule, using Go's start + execution timeout + initial decision
  backoff deadline. Retry/cron timeout paths still need continue-as-new behavior
  and are intentionally not terminally closed by this path. Fixed parent timeout
  notification to emit ChildWorkflowExecutionTimedOut instead of a Failed event.
- Go task timeout retries depend on transient decision history and buffering.
  `mutable_state_decision_task_manager.go` suppresses repeated timeout/failure
  history when attempt > 0; scheduled/started events become durable on completion.
  Do not implement task timeout retries by adding a fresh durable task each time.
- Added internal history adapters with domain UUID lookup and task token domain
  checks, plus a configured history listener. Advanced parent/retry/partition
  metadata and unported methods explicitly return UNIMPLEMENTED.
- Added internal matching adapters with a configured listener, polling, description,
  and cancellation indexed by domain/task list/task type/poller. Cancellation
  registrations are removed by RAII, including dropped requests; replacement
  registrations cannot be removed by an old guard.
- Workspace validation before matching adapters: 134 tests passed (including the
  service descriptor audit). Matching source compiles. Rebuilding and independent
  original-IDL network checks are in progress; record outcomes after they finish.

## 2026-10-14: domain UUID isolation and additional SDK behavior

- Internal original-Go-IDL client check passed across history 7834, matching 7835,
  and frontend 7833: start, poll, complete, read, and outstanding-poll cancellation.
  Existing Go SDK also passed the new server-side workflow timeout scenario with a
  typed TimeoutError. Strict workspace clippy passed at that checkpoint.
- Added a full Go AST source ledger: 2,300 files (1,245 production, 312 generated,
  743 test files); 58,726 functions, 6,425 types, 5,884 constants and 3,669 variables.
  `port-ledger.json` retains all top-level symbols. `port-map.json` has reviewed
  partial mappings; unknown files explicitly require review. No file is marked
  complete solely because a related Rust module or an RPC adapter exists.
- Domain storage previously keyed runs and task tokens by names. Ported UUID
  namespaces, retaining API domain names in metadata and cross-workflow history.
  Deleting/recreating a name no longer shares runs, request-ID dedup or pollers.
- Added pinned request namespace scopes so a long poll cannot switch to a recreated
  domain's new UUID while waiting. UUID-shaped domain names cannot alias internal
  UUIDs or trigger legacy snapshot migration. Internal responders validate exact
  request/token UUID agreement; public responders check that token UUID exists.
- Domain restoration re-registers actual UUID mappings and upgrades old development
  snapshot name keys, queues, parent/child links and request-ID indexes. Historical
  UUIDs are never moved to a recreated domain. Signal dedup is restored from old
  histories when old snapshots lack the new mutable-state set.
- Added native Thrift DeleteDomain dispatch. Corrected signal wakeups for idle
  workflows and signal request-ID idempotency. Signal-with-start now holds the
  engine lock across lookup/start/signal and avoids mutating a closed history when
  a start request is deduplicated.
- Fixed child terminal events' startedEventId: reference the actual parent
  ChildWorkflowExecutionStarted event rather than literal 1. New snapshots store
  that link; old snapshots infer it from initiation ID and parent history.
- Persistence tests currently pass 49 checks, including namespace recreation,
  UUID-shaped names, pinned namespaces, legacy restoration, child metadata/IDs and
  signal wakeups/dedup. Full workspace/network verification of these latest edits
  is in progress. Running binary still predates the UUID edits until next rebuild.

## 2026-10-14: resumed audit, retry corrections, CLI transition

- Recovered newer workflow timeout retry/cron and start-backoff source changes.
  Historical claims were not accepted as current verification: fresh workspace
  `cargo test --workspace` passed 162 tests (56 persistence), exit 0, recorded in
  `/tmp/cadence-resume-tests.log`. This includes the protobuf service descriptor audit.
- Corrected a prior finding: Go SERVER validation rejects a retry coefficient
  below 1. The SDK supplies its default 2 before sending requests. Removed the
  misleading server default helper, use the policy coefficient directly, and
  validate activity/child retry policies before mutating decision history.
  Regression checks rejection and successful retry of the corrected decision.
- Newer timeout retry code incorrectly used a 24-hour backoff overflow bound;
  Go uses MaxInt64 / 1e9. Corrected the bound and added a two-day retry check.
- Continue-as-new now preserves initiator, backoff, failure reason/details,
  last completion result, header/memo/search attributes, and run-chain IDs in
  terminal/start history. Start history now records expiration and initial IDs.
  These latter changes have additional checks/build in progress; verify logs
  before reporting deployed behavior.
- Source UI restarted and read domain data successfully, then stopped at user's
  explicit direction to use Cadence CLI instead. Keep the submodule but use CLI
  for live verification. CLI installation builds the existing upstream entrypoint
  `go/cmd/tools/cli` into `/home/ubuntu/.local/bin/cadence` in tmux `cli-install`.
  A Go client binary does not require running a Go Cadence server.
- All migration gaps in inventory remain active. Retry/cron completion/failure,
  task buffering/timeouts, persistence, distributed services, admin APIs and many
  other upstream behaviors remain unported; no full parity claim is warranted.

- CLI built successfully; CLI feature version 1.7.0. Native gRPC registered local
  domain `rust-cli-compat` (UUID 424c13c0-685c-49ec-9d50-d17d0581e861);
  native TChannel described it. CLI started/read/listed timeout and retry runs
  across both transports. Exact original run `ec0d3342-7d51-4559-b7c0-31beb1f14749`
  continued with RetryPolicy to `6ff1f482-0ef2-463a-b237-6bf86e03bc72`, then timed out.
- Repeatable `tests/cli-compat.sh` passed, exit 0. Domain
  `rust-cli-1792004679-14443`, original `2d810dd9-7523-4dbe-96b2-bb3c90de3863`,
  successor `d5524613-3edf-4525-b74d-218eb6fda149`. The original CLI exits zero
  for some command errors; assertions inspect actual output. `domain list` does
  not exist in this CLI revision. Documented install/commands in tests/README.md.
- Verified cron timeout with CLI `@every 2s`, original run
  `2146143d-7785-4be0-82cb-7d8da9cbad20`, successor
  `cdd2444a-6fe8-444b-978e-2e7105d96404`, CronSchedule initiator. Terminated the
  cron workflow after inspection. Earlier arbitrary-ID cron queries were invalid.
- Metadata/backoff checks passed 57 persistence tests, server rebuilt/restarted;
  strict workspace clippy passed after removing a needless struct default update.
- Continued engine port: successful completion recurs via cron; workflow failure
  retries then falls back to cron, with failure reason/details and previous
  completion result. Cancellation stops retries (failed canceled run becomes
  Canceled). Cron resets attempt and creates fresh retry expiration. Persistence
  tests passed 59 checks including successful cron and failed workflow retry.
  Full workspace/clippy/build and real SDK workflow retry check are now in progress.

- Outcome changes passed 165 workspace tests, strict workspace clippy, and build;
  combined exit 0 (`/tmp/cadence-outcome-full.exit`). Restarted Rust server.
  Existing Go SDK driver passed live (exit 0) with the new WorkflowRunRetry:
  fail attempt 0, continue-as-new, SDK reads attempt 1 and returns success.
  Domain `rust-tchannel-sdk-29e01ee8-223b-41b0-8c68-90e09d2b9210`.
  Native Meta::health Go YARPC call also passed as part of this driver.
- Added live SDK cron completion result propagation check; running in sdk-cron.
  This check inspects explicit original and successor run IDs and terminates the
  recurrent workflow after assertion. Check logs before claiming its result.

- SDK cron completion result propagation passed live, exit 0:
  `/tmp/cadence-outcome-sdk-cron.log`; domain
  `rust-tchannel-sdk-a97cfece-2028-48da-978d-63896258cf3f`. Recurrent workflow was
  terminated after assertion. Added workflowRetryWorkflowId and cronWorkflowId
  to the machine-readable SDK report.
- Ported activity deadline firing (schedule-to-start, schedule-to-close,
  start-to-close, heartbeat) into server timer loop. Go timer sequence resets
  attempt ScheduledTime on retry; schedule-to-close can itself retry under a
  policy. Timeout reason uses Go's `cadenceInternal:Timeout TYPE` format.
- Heartbeats now record timestamp, deadline follows last heartbeat/start.
  Intermediate timeout retry events remain transient, attempts increment,
  heartbeat details persist, old attempt tokens fail; only final timeout is
  durable and wakes workflow. Unstarted timeout startedEventID is 0.
- PollActivity skips stale removed queue entries and entries waiting on retry
  backoff. Added snapshot-default fields for schedule-to-start/heartbeat times.
- Persistence tests passed 62 checks. Latest full workspace/clippy/build and SDK
  activity timeout driver are in progress in tmux activity-full/activity-sdk-build;
  inspect `/tmp/cadence-activity-*.log` and exit files before deploying/claiming.
- Added partial source mappings for cron, utility request construction, history
  retries, decision completion/failure, and timers. Partial does not mean complete.

- Activity timeout changes passed 168 workspace tests, strict clippy, and build;
  `/tmp/cadence-activity-full.exit` 0. Restarted server. Existing Go SDK passed
  the unpolled activity ScheduleToStart timeout and typed history, along with
  all earlier payload/retry/signal/child/cron checks. Live domain
  `rust-tchannel-sdk-6ff9236b-8109-4bdc-8301-b9326d83f511` (SDK exit 0).
- CLI read the SDK activity timeout through public gRPC. First CLI check started
  before the restarted listener bound and got connection refused. Added endpoint
  readiness waits to the driver and reran successfully (exit 0), domain
  `rust-cli-1792005510-10680`, original
  `2661694c-406a-4a68-942e-aa72ec1e01a3`, successor
  `2168af99-b95c-4b02-9c64-fd7c416a5942`.
- Added tmux cli window configured for gRPC localhost7833/rust-cli-compat;
  user can run `cadence domain describe` or `cadence workflow list` directly.
- CLI history exposed an unnecessary decision after every nonempty command
  batch. Go decision/handler.go schedules for unhandled events, explicit force,
  or canceled unstarted activity. Corrected the general scheduling condition;
  local synchronous child/external acknowledgments also wake the parent/source
  workflow. Activity scheduling alone no longer creates an extra decision.
- Adjusted heartbeat cancellation test to wake workflow through an external
  signal. Added tests for idle activity scheduling and explicit force request;
  persistence tests pass 64 checks. Latest full workspace/clippy/build pending
  in decision-full; source correction is not yet in the running binary.
- Remaining decision gaps include buffering, transient failed/timed-out tasks,
  inline returnNewDecisionTask, complete validation/failure causes, and sticky
  execution. Do not treat the scheduling fix as whole decision parity.

- Further Go-source correction: workflow timeout retry uses
  `execution.TimerTypeToReason(StartToClose)` =>
  `cadenceInternal:Timeout START_TO_CLOSE`, NOT `TIMEOUT_START_TO_CLOSE`.
  Corrected runtime reason and assertions so non-retriable reason matching and
  continued-failure history match Go. Earlier memory examples used the wrong
  string and describe a previous binary, not the corrected implementation.
- Cron recurrence now retains jitter as internal execution metadata and uses it
  on subsequent backoffs. Public start/continued history IDLs do not expose this
  field; do not invent new wire fields. Internal metadata uses serde defaults
  for old snapshots. Child workflow jitter metadata is still unported.
- Updated the HTTP integration history expectation: activity Scheduled/Started/
  Completed events now precede the decision task scheduled by activity completion.
  The former expectation encoded the extra task bug. Latest full validation in
  parity-full2, source changes not deployed until its build succeeds.

- Final source validation at this checkpoint: 171 workspace tests passed,
  strict clippy and server build passed, combined parity-full2 exit 0. Rebuilt
  and restarted server. SDK and CLI checks running against this exact binary
  in sdk-parity/cli-parity, logs `/tmp/cadence-parity-{sdk,cli}.log` and `.exit`.

- Exact rebuilt binary passed the SDK suite, domain
  `rust-tchannel-sdk-eae3ea3d-f766-4bc8-a6df-1dc46c5bb011`, exit 0.
  Payload echo and activity retry histories now each have 11 events (previously14
  due to the unnecessary decision), all workflow retry/cron/timeout checks pass.
- CLI check hit a default five-second deadline after listeners were ready;
  isolated rerun also failed. Server stayed healthy, but debug snapshot task
  consumed an async RPC worker serializing 85MB histories every five seconds.
  save_to_file already clones state before serialization, so lock-held filesystem
  writing was NOT the cause. Move serialization/I/O to spawn_blocking, one snapshot
  at a time; independent 500ms timer task continues deadlines/retries.
  New build/clippy in snapshot-runtime. Live CLI/SDK load verification still needed.

- Snapshot spawn_blocking change passed strict clippy and server build (exit 0),
  deployed in rust tmux. CLI driver now passed at default context timeout:
  domain `rust-cli-1792006245-20976`, original
  `dafd3d89-3630-456f-bd01-f9afdd06008c`, successor
  `be746dac-4e9f-45f6-aa4a-7e94c14481e7`.
- SDK launch raced native listener during restart once (Meta::health connection
  refused). Ready-server rerun passed, exit 0, domain
  `rust-tchannel-sdk-e0e57a44-1a85-4f0d-b6ba-87c7fcb1f4f1`, echo run
  `9161dab3-70cd-4bb3-a4a7-cd57f9e9821c`, 11-event echo and retry histories.
  Added bounded native listener readiness to SDK driver, preserving health RPC
  assertion after readiness. This is a harness startup fix, not a server bypass.
- Repeated default-timeout CLI descriptions across snapshot intervals running
  in cli-load; check `/tmp/cadence-snapshot-cli-load.exit` before claiming pass.

- Ten consecutive CLI domain descriptions at default five-second timeout across
  snapshot intervals passed, exit 0 (`/tmp/cadence-snapshot-cli-load.exit`).
  Ready-driver compile check passed. Runtime now uses only rust and cli tmux
  windows; completed validation windows can be removed (logs remain in /tmp).
- Current full port is NOT complete. All 13/164 protobuf definitions exist, but
  only 35/57 public and 17/107 internal/admin adapters are concrete; engine
  coverage is also partial. Follow inventory, service-map and partial source
  ledger instead of inferring completeness from the running CLI or tests.

## Continuing full migration: workflow-task buffering

- Ported Go `mutable_state_builder.shouldBufferEvent` behavior for implemented
  event types: signals, activity/child results, cancellation requests and timer
  fires remain private while a workflow task is in flight. Decision and command
  events remain durable. Public history excludes buffers until completion/failure.
- Buffers retain original timestamps. Private distinct aliases preserve activity
  start/result links until durable IDs are assigned; aliases are never returned
  through public history or task tokens. Pending activity and child parent links
  are remapped on flush. Snapshot fields use defaults; restore reconstructs
  signal dedup and alias sequence.
- Closing commands that missed buffered events fail the task with
  UnhandledDecision and enqueue replay rather than close the workflow. A canceled
  buffered timer fire can be consumed by an earlier CancelTimer command in the
  same batch, allowing completion. Remaining buffers schedule a new task.
- Go forced external close trims unhandled buffered events after close; corrected
  implementation drops them on external termination/timeout. Do not claim that
  forced termination publishes all buffered signals.
- Corrected PreviousStartedEventID after failed task: it identifies the last
  successful task, not the failed one. Updated the old incorrect retry test.
- Ported cancel unknown timer => typed CancelTimerFailed/TIMER_ID_UNKNOWN (was
  TimerCanceled with startedID0). Added API attrs, exact Thrift field240, and
  regenerated gRPC typed history adapter. CancelTimer now consumes buffered fire
  and retains original TimerStarted ID, matching Go execution timer methods.
- First buffering tests passed67 persistence checks. Added timer cancellation,
  forced-close, snapshot reconstruction and Thrift wire checks; full workspace,
  strict clippy and build are running in buffer-full. Logs
  `/tmp/cadence-buffer-{full,clippy,build}.log`, `/tmp/cadence-buffer-full.exit`.
  Running binary still predates buffering until this new build is deployed.
- Next: Go transient decision retries/timeouts; failed retries must not append
  repeated scheduled/started/failed events. Read mutable_state_decision_task_manager
  and keep paged worker history snapshots consistent with transient events.

- Buffering/timer wire batch passed 178 workspace tests, strict clippy and build,
  combined `/tmp/cadence-buffer-full.exit`0. Deployed it in rust tmux. Existing
  SDK passed (`/tmp/cadence-buffer-sdk.exit`0), domain
  `rust-tchannel-sdk-2d6a5219-e42a-48e6-aa64-b5b93c02b127`, echo run
  `5a5c45f4-6cd6-4751-a69a-1d9a12e047ba`, 11-event echo/retry histories.
- Implemented transient workflow-task retry scheduling/start: attempts>0 reserve
  schedule/start IDs but do not write those events until successful completion.
  First failure/timeout is durable; repeated attempts are not. Intervening events
  before poll, or buffered events after failure, reset attempt to0 and use durable
  scheduling. Worker poll history includes pseudo events; paged cursors retain
  frozen pseudo events and append them only on the final durable page, as Go does.
- Added pending schedule/start timestamps, identity and request ID with snapshot
  defaults/restoration. Successful completion materializes exact timestamps.
  Added started workflow-task deadline scanning to timer loop. Repeated timeouts
  remain transient and buffered input is flushed before a retry decision.
- Validate schedule attempt as well as ID on workflow-task responses: transient
  retries reuse IDs so old tokens must not complete/fail a new attempt. Expired
  OPEN task returns EntityNotExists/Decision task not found, matching Go handler;
  malformed token bytes still return BadRequest. Closed workflow error unchanged.
- Six focused transient tests passed; total persistence77. Cases include repeated
  failures/timeouts, new input resetting retries, stale token rejection, final
  paged-history pseudo events frozen across another retry, and snapshot timestamps.
- New existing SDK driver compiles with test-only workflow-task panic injection
  (two failures then success) and manual abandonment followed by task-timeout
  recovery. Full tests/clippy/build pending in transient-full; source not deployed
  yet. Logs `/tmp/cadence-transient-{full,clippy,build}.log` and `.exit`.
- Private buffer aliases are an internal implementation detail. When porting admin
  mutable-state dumps, normalize their wire representation to Go BufferedEventID
  rather than exposing aliases. Full reset/replication buffer reconstruction and
  upstream persisted-buffer duplicate recovery still require explicit review.

- Transient batch passed184 workspace tests, strict clippy, build (exit0), deployed.
  CLI check passed, domain `rust-cli-1792018659-3357`, original
  `528fab7b-de5b-4c69-94f5-a0f1001dbe52`, successor
  `2c69fcb9-14f5-4fc5-9244-c3d18fef428c`.
- First new SDK task-fault check recovered but correctly failed its assertion:
  zero DecisionTaskFailed events, with a first DecisionTaskTimedOut instead.
  Original run `834ed72c-4061-476a-a8a6-e25126d3a743` in domain
  `rust-tchannel-sdk-f1428d29-fbf0-443a-aa5c-a3d8510ee889`. Do not report this run
  as a passed full SDK check.
- Diagnosed exact wire bug through SDK source internal_task_pollers.go:
  handleDecisionFailedRequest sends failure only if task.Attempt pointer is
  present and value0; subsequent failures intentionally wait for timeout to avoid
  spinning. Rust ToThrift omitted Attempt0 => SDK treated it as an older server
  and skipped even the first failure. Field51 must carry I64(0), not be omitted.
- Corrected zero Attempt presence and added direct Thrift wire-presence test.
  This is distinct from scalar default equivalence: optional scalar presence can
  control SDK logic. Do not weaken SDK history assertions to accept timeout here.
- Fixed buffered timer cancellation precheck to consider only actual CancelTimer
  decisions, not unused attributes attached to another decision type. Full
  checks/clippy/build in attempt-wire; redeploy and rerun SDK pending.

- Attempt-presence batch passed185 workspace tests/strict clippy/build, deployed.
  Existing SDK now passed ALL new workflow-task failure and timeout recovery
  checks, including stale task EntityNotExists. Domain
  `rust-tchannel-sdk-107f11ca-c6fd-46d1-b9df-4ee727af426c`, echo run
  `542d9f60-007e-46c5-9a68-c62d2b45e4ac`; `/tmp/cadence-attempt-wire-sdk.exit`0.
- Added atomic inline `returnNewDecisionTask` behavior to the engine/public API.
  Shared start logic generates correct token, metadata and history. Inline tasks
  remove only their own queue entries, preserving another workflow's tasks.
  ReturnNew alone does not force a new idle task; unhandled closing commands can
  return the replay task. Inline history begins after previous successful Started
  and paging retains that range. Inline request ID matches Go's canonical string.
- Do not invent a public StickyExecutionEnabled field: it is internal matching
  metadata, not part of public PollForDecisionTaskResponse IDLs. Inline public
  response uses incremental history; complete sticky routing remains unported.
- Four inline tests added;189 workspace checks passed. Clippy flagged two routine
  let-else returns in extracted starter; changed them to question-mark expressions.
  Clippy/build rerun in inline-build2, logs `/tmp/cadence-inline-{clippy2,build2}.log`
  and `/tmp/cadence-inline-build2.exit`. Running binary still attempt-wire.
- Existing SDK driver compiles with raw SDK-client inline force/return task,
  incremental-history and returned-token completion checks. Run after deployment.

- Inline batch passed189 workspace tests; clippy suggestions fixed, clippy/build
  passed, deployed. Existing SDK inline request/history/token checks passed,
  domain `rust-tchannel-sdk-0c689a42-96c2-4756-bce7-afedd4f3af92`, echo run
  `ae41119e-f35b-4ac9-a404-be4c0b3677f9` (`/tmp/cadence-inline-sdk-live.exit`0).
- Ported sticky task owner queue routing, incremental history after last processed
  task, and sticky ScheduleToStart fallback to normal full-history replay. Stale
  sticky queue entries cannot dispatch a new normal task. Failures/timeouts clear
  ownership; completion without StickyAttributes clears it, matching Go.
- Added ResetStickyTaskList API models, exact Thrift conversions/dispatch, public
  gRPC Worker adapter and internal History adapter. Internal domainUUID is
  authoritative over inner domain name. Reset clears ownership, preserving an
  already-transferred task; that task receives full history after reset.
- Matching response now reports stickyExecutionEnabled for actual incremental
  sticky delivery. Public API has no such field; don't invent one.
- Go default history.stickyTTL is365days (not guessed minutes). Check previous
  committed LastUpdatedTimestamp before refreshing it for a new event. Added
  TTL test and last-update commits for heartbeat/activity/task mutations.
  Dynamic per-domain overrides and full client-version metadata remain unported.
- Sticky batch passed195 workspace tests, strict clippy and build (exit0), deployed.
  Existing Go SDK DEFAULT sticky worker passed echo/activity and idle-signal
  workflows. Absent owner fallback and explicit ResetSticky checks also passed.
  Domain `rust-tchannel-sdk-df8706c7-aafc-4891-91a1-ac16d813869a`, echo run
  `ddaa13c5-651d-4896-b005-20f03b04ec73`; `/tmp/cadence-sticky-sdk-live.exit`0.
- Original-IDL internal gRPC sticky/reset test initially raced startup with
  ECONNREFUSED. Added client.waitForReady and rerunning in internal-sticky-ready.
  Inspect `/tmp/cadence-internal-sticky-ready.log` and `.exit` before reporting.
- Regenerated method inventory:36/57 public,18/107 internal/admin concrete adapters
  (History14/44, Matching4/13). All13/164 schema definitions still compiled.

- Ported `RespondQueryTaskCompleted` end to end (public WorkerAPI now 14/14,
  internal MatchingAPI 5/13, public total 37/57 via regenerated inventory).
  Engine: `answer_query_task` marks the buffered query answered by
  `QueryTaskToken.taskId`; double-answer and unknown IDs are EntityNotExists
  ("query task not found, or already expired"), malformed tokens are BadRequest,
  unknown domain UUIDs are EntityNotExists. Failed completions surface
  QueryFailedError to the waiter. Thrift field IDs 10/20/30/40/50 with
  round-trip tests; proto↔Thrift mapping mirrors Go
  (`WorkflowQueryResult.result_type` ANSWERED/FAILED ↔ `completedType`, payload
  answer ↔ `queryResult`) with converter tests. Transports: Thrift dispatch,
  JSON `POST /api/v1/tasks/respond-query-completed`, public gRPC Worker
  adapter, internal matching adapter (validates embedded token against
  routing fields, rebuilds a missing token from them). E2E test answers a
  direct query via a minted query token then completes the decision.
- Validation: 200 workspace tests pass (89 persistence, 15 thrift, 17 server
  lib, 14 e2e), strict workspace clippy clean, build clean, redeployed in
  recreated `cadence-rust` tmux `rust` window (old 29-window session was lost
  to an environment restart). Go SDK compat passed over TChannel (exit 0,
  domain `rust-tchannel-sdk-034381b6-b454-4d90-b88e-2396a3386409`), Node gRPC
  compat passed (exit 0). Web UI restarted in tmux `web` window
  (`CADENCE_GRPC_PEERS=127.0.0.1:7833`, port 8088) and serves domain data
  from the Rust frontend (HTTP 200).
- Next: matching `QueryWorkflow` direct dispatch, workflow reset/restart,
  Schedule/Semaphore APIs, AdminAPI. Public 37/57, internal 19/107.

- Ported five internal matching APIs (MatchingAPI now 10/13; only
  Update/RefreshTaskListPartitionConfig and AcquireSemaphore remain, which need
  durable partition state and semaphore ownership still unported).
  `AddDecisionTask`/`AddActivityTask` requeue scheduled-but-unstarted tasks via
  new engine `requeue_decision_task`/`requeue_activity_task` helpers: unknown
  runs/schedule IDs are EntityNotExists (NOT_FOUND), closed runs are
  AlreadyCompleted, already-started tasks are NOT_FOUND, and existing queue
  entries are not duplicated. `QueryWorkflow` resolves the domain UUID and
  reuses the frontend query path with an empty partition config (single host;
  forwarded queries stay UNIMPLEMENTED). `GetTaskListsByDomain` takes the
  domain name like Go, lists queue-backed task lists via `task_lists`, and
  reuses frontend DescribeTaskList per list. `ListTaskListPartitions` reports
  the single root partition per type keyed by list name (owner = $HOSTNAME).
- Tests: 3 engine unit tests (decision/activity requeue incl. started-task
  rejection, list discovery); extended `internal-grpc-compat.cjs` covers
  NOT_FOUND injection, pollable re-added decision, idempotent activity re-add
  before dispatch, started-task NOT_FOUND after poll, direct matching query
  answered via `RespondQueryTaskCompleted`, domain list maps, and partition
  keys. Full workspace 203 tests pass, strict clippy clean, deployed, and all
  three compat drivers pass: internal (exit 0), Go SDK TChannel (exit 0),
  Node gRPC (exit 0).
- Next: Update/RefreshTaskListPartitionConfig + AcquireSemaphore need durable
  partition/semaphore state; then Schedule/Semaphore public APIs, workflow
  reset/restart, AdminAPI. Public 37/57, internal 24/107 (History 14, Matching 10).

- Ported public task-list discovery (WorkflowAPI now 12/18, public 39/57;
  internal Matching 10/13 unchanged). `GetTaskListsByDomain` and
  `ListTaskListPartitions` are implemented on all three transports (Thrift
  dispatch, JSON `POST /api/v1/tasklists/by-domain|partitions`, public gRPC),
  each reusing the internal matching adapters like Go frontend→matching.
  Also added the missing Thrift `DescribeTaskList` handler (JSON/gRPC already
  existed) by extracting a shared `describe_task_list_response` helper with a
  small `DescribeTaskListError` enum (keeps strict `result_large_err` clippy
  clean without new exemptions). Thrift field IDs match `shared.thrift`
  exactly, including `domainName` field 10 on the by-domain request.
  Single-host semantics: partitions report the root partition per type keyed
  by list name; unknown domains are EntityNotExists, empty task lists are
  BadRequest, matching Go validators.
- Tests: Thrift round-trip, JSON e2e (incl. 404/400 cases), extended
  `grpc-compat.cjs` with Describe/by-domain/partitions assertions. Full
  workspace 205 tests pass, strict clippy clean, deployed; all compat
  drivers pass against the build (internal:0, grpc:0, go-sdk:0).
- Next: `RestartWorkflowExecution`, `StartWorkflowExecutionAsync`,
  `SignalWithStartAsync`, `ResetWorkflowExecution`, `RefreshWorkflowTasks`,
  `DiagnoseWorkflowExecution` (WorkflowAPI remaining); Schedule/Semaphore
  APIs; AdminAPI. Public 39/57, internal 24/107.

- Ported `RestartWorkflowExecution` (WorkflowAPI now 13/18, public 40/57).
  Engine `restart` rebuilds the start request from the run's first started
  event (type, task list, input, timeouts, retry/cron/jitter/header/memo/
  search attributes) with `TerminateIfRunning`, so open runs terminate and
  closed runs start fresh; empty run IDs resolve to the latest run and unknown
  executions are InvalidArgument. Also added the Go `common/types`-only
  `jitterStartSeconds` field to started-event attributes (serde-only, never on
  the Thrift wire since the IDL lacks a field ID) and record it at start, so
  restarts preserve jitter like `constructRestartWorkflowRequest`.
  Transports: Thrift dispatch, JSON `POST /api/v1/workflows/restart`, public
  gRPC. No new history adapter needed (restart funnels into history Start).
- Tests: 3 engine unit tests, Thrift round-trip, JSON e2e (old run terminated
  event 28, new run pollable), restart assertion in `grpc-compat.cjs`. Full
  workspace 210 tests pass, strict clippy clean, deployed; compat drivers all
  pass (internal:0, grpc:0, sdk:0). Web UI still serves (200).
- Next: `StartWorkflowExecutionAsync`, `SignalWithStartAsync`,
  `ResetWorkflowExecution`, `RefreshWorkflowTasks`, `DiagnoseWorkflowExecution`;
  Schedule/Semaphore APIs; AdminAPI. Public 40/57, internal 24/107.

- Ported async start/signal-with-start (WorkflowAPI now 15/18, public 42/57).
  New durable `async_requests` FIFO on the execution store (JSON snapshot
  round-tripped) with bounded consumer semantics mirroring Go's Kafka
  producer→consumer: handlers validate with the sync validators then enqueue
  and return immediately; the worker-role background loop drains the queue
  every tick and applies starts through request-ID idempotency (AlreadyStarted
  counts as consumed, invalid payloads are dropped as poison after logging,
  other failures requeue up to 5 attempts). Transports: Thrift dispatch, JSON
  `POST /api/v1/workflows/start-async|signal-with-start-async`, public gRPC.
- Tests: engine enqueue/consume/idempotency/poison-bound unit test, Thrift
  round-trips, JSON e2e (OK + BAD_REQUEST validation), live async assertion in
  `grpc-compat.cjs` (async run materializes via history polling). Full
  workspace 213 tests pass, strict clippy clean, deployed; compat drivers all
  pass (internal:0, grpc:0, sdk:0). Web UI still serves (200).
- Next: `ResetWorkflowExecution`, `RefreshWorkflowTasks`,
  `DiagnoseWorkflowExecution`; Schedule/Semaphore APIs; AdminAPI. Public
  42/57, internal 24/107.

- Ported `ResetWorkflowExecution` (WorkflowAPI now 16/18, public 43/57;
  internal History now 15/44, internal total 25/107). Engine `reset` mirrors Go
  history reset minus branches/versions: base run must exist with a scheduled
  decision; `decisionFinishEventId` must be in (1, nextEventId] and land on a
  DecisionTask{Started,TimedOut,Failed,Completed} event (exact Go error
  strings); current-run termination with the reset reason; history-prefix fork
  into a new UUID run; later-signal reapply from base and current runs unless
  `skipSignalReapply` (deduped by signal request ID); fresh decision task.
  Request-ID dedup via a new `create_request_id` record field (set at start,
  carried as the reset request ID on reset runs). Transports: Thrift dispatch,
  JSON `POST /api/v1/workflows/reset`, public gRPC, internal history adapter
  (domain-UUID resolution + forward, like Go frontend→history).
- Tests: 3 engine unit tests (fork+reapply+dedup, bad points, skip reapply),
  Thrift round-trip, JSON e2e (5-event reset history, BAD_REQUEST on event 1),
  live reset in `grpc-compat.cjs`. Full workspace 218 tests pass, strict
  clippy clean, deployed; compat drivers all pass (internal:0, grpc:0, sdk:0).
- Next: `RefreshWorkflowTasks`, `DiagnoseWorkflowExecution`;
  Schedule/Semaphore APIs; AdminAPI. Public 43/57, internal 25/107.

- Ported `RefreshWorkflowTasks` and `DiagnoseWorkflowExecution` (WorkflowAPI
  now 18/18, public 45/57; internal History now 16/44, internal total 26/107).
  Engine `refresh_tasks` mirrors Go's task refresher for a single host: timer
  state needs no regeneration (background loop scans live state), so it
  requeues unstarted decision/activity tasks without duplicating queue
  entries; unknown runs are EntityNotExists, closed runs succeed as no-ops.
  Diagnose starts the `diagnostics-starter-workflow` in `cadence-system` with
  Go's exact timeouts (86400/300), AllowDuplicate policy, and JSON input, and
  reuses an already-running diagnosis like Go's AlreadyStarted branch (no
  system-domain auto-creation, matching Go's domain-cache failure when absent).
  Transports: Thrift dispatch, JSON routes, public gRPC, internal history
  refresh adapter. One driver bug fixed along the way (proto field is
  `workflowExecution`, not `execution`).
- Tests: engine requeue test, Thrift round-trips, JSON e2e (no-duplicate
  refresh, 404s, diagnose 404→OK→reuse), live refresh+diagnose in
  `grpc-compat.cjs`. Full workspace 221+ tests pass, strict clippy clean,
  deployed; compat drivers all pass (internal:0, grpc:0, sdk:0).
- Next: Schedule (0/8) + Semaphore (0/1) public APIs; Visibility
  ListArchived; Domain failover (2); AdminAPI (0/39); History/Matching
  remainder. Public 45/57, internal 26/107.

- Ported domain failover (DomainAPI now 8/8, public 47/57). Engine `failover`
  mirrors Go's domain-handler rules with exact messages: global-only (local
  domains rejected), no active-active promotion via failover, target required,
  destination-cluster check unless skipped, no-change rejection; applies the
  new active state, bumps the failover version, and appends an audit entry
  with before/after replication snapshots. `failover_history` computes
  FailoverEvents like Go's `ToFailoverEvents` (domain-level change plus
  sorted per-attribute changes, empty events filtered) with opaque offset
  cursors, newest first, default page 50. Audit lives on the domain record so
  JSON snapshots keep it. Transports: Thrift, JSON, public gRPC (proto
  `failover_timeout` duration → seconds; response nests `Domain`).
  Frontend validation mirrors Go (timeout positivity; max-timeout/lockdown
  need unported dynamic config). Driver fix: proto nests `domain`, not
  `replicationConfiguration`; system-domain registration is idempotent.
- Tests: 2 domain unit tests (move+audit, rejections+paging), validation
  messages test, Thrift round-trips, JSON e2e, live failover+history in
  `grpc-compat.cjs`. Full workspace 226 tests pass, strict clippy clean,
  deployed; compat drivers all pass (internal:0, grpc:0, sdk:0).
- Next: Schedule (0/8) + Semaphore (0/1) public APIs; Visibility ListArchived;
  AdminAPI (0/39); History/Matching remainder (partition config, semaphore,
  replication/shard). Public 47/57, internal 26/107.

- Ported `ListArchivedWorkflowExecutions` (VisibilityAPI now 7/7, public
  48/57). Validation mirrors Go (domain required; page size defaults to 1000
  and caps at 10000 with Go's exact message). Archival gating mirrors Go's
  order exactly: cluster check first, so the exact "Cluster is not configured
  for visibility archival" BadRequest is returned — the visibility archival
  backend itself remains unported (see inventory), at which point the domain
  lookup/status/read checks plug in behind the same gate.
  Transports: Thrift dispatch, JSON route, public gRPC.
- Tests: validation unit test, Thrift round-trip, JSON e2e, live gating
  assertion in `grpc-compat.cjs`. Full workspace 229 tests pass, strict
  clippy clean, deployed; compat drivers all pass (internal:0, grpc:0, sdk:0).
- Next: Schedule (0/8) + Semaphore (0/1) public APIs; AdminAPI (0/39);
  History/Matching remainder. Public 48/57, internal 26/107.

- Ported the Schedule API natively (ScheduleAPI 8/8, public 56/57).
  Go implements schedules as `cadence-scheduler:<id>` workflows; a single
  host cannot rely on worker execution, so schedules live in the execution
  store with the same observable semantics: cron evaluation via the shared
  cron port, Go-style fire workflow IDs (`prefix-RFC3339`), deterministic
  UUIDv5 fire request IDs (redelivery-safe), SKIP_NEW default (+CONCURRENT
  with caps, BUFFER with limits, CANCEL/TERMINATE_PREVIOUS), catch-up
  SKIP/ONE/ALL with windows on unpause, immediate backfill execution with
  progress tracking, pause/unpause/update/delete/list/describe, pause-on-
  failure on closes, and buffered-fire draining. CRUD validation mirrors Go
  exactly (enableScheduler gate defaulting off like Go, cron/time-range/
  action/policy/search-attribute rules and messages). All three transports
  plus a new registered gRPC ScheduleAPI service and a background firing
  tick. Non-UTC CRON_TZ is rejected (shared cron limitation, noted).
- Tests: 5 engine unit tests, Thrift round-trips, JSON e2e (gate message,
  dup/invalid-cron rejection, CRUD/pause/update/delete), live CRUD in
  `grpc-compat.cjs`, and a live firing check (per-minute schedule started
  `every-minute-...T03:48:00Z`, SKIP_NEW skipped the overlapping fire).
  Full workspace 236 tests pass, strict clippy clean, deployed with
  CADENCE_ENABLE_SCHEDULER=1; compat drivers all pass (internal:0, grpc:0
  with scheduleCheck passed, sdk:0).
- Next: Semaphore (last public API); AdminAPI; History/Matching remainder.
  Public 56/57, internal 26/107.

- Closed the last public API: `CreateSemaphore` (SemaphoreAPI 1/1, public
  57/57) plus internal matching `AcquireSemaphore` (MatchingAPI 11/13).
  Owner IDs use a byte-exact port of Go's length-prefixed codec with Go's
  full test vectors (round-trip incl. separators/empty/unicode/i64 edges,
  all malformed spellings rejected). Metadata store validates sizes (bucket
  default 100, max 250, exact Go messages) with duplicate → BadRequest.
  Acquire validates identifier/owner like Go, resolves the domain UUID,
  grants uniform-random free slots 1..=budget with idempotent owner
  re-acquire, and reports NoSlot/0 when full; blocking waiter queues are an
  explicit gap (needs the decision-level token release path). Both APIs sit
  behind disabled-by-default gates like Go (`system.enableDistributedSemaphore`,
  static flag or CADENCE_ENABLE_SEMAPHORE=1). Transports: Thrift, JSON,
  public gRPC SemaphoreAPI (newly registered), internal matching adapter.
- Tests: 3 owner-codec tests, engine create/acquire/release tests,
  validation + Thrift round-trips, JSON e2e (gate message, dup, bad
  capacity), live `semaphoreCheck: passed` in grpc-compat.cjs and live
  grant/NoSlot assertions over internal gRPC. Full workspace 245 tests
  pass, strict clippy clean, deployed; compat drivers all pass
  (internal:0, grpc:0, sdk:0). Web UI serves (200).
- Next: AdminAPI (0/39, largest remainder); History remainder (replication/
  shard/queue ops); Matching partition-config ×2; decision-level semaphore
  grant/release integration; archival backends. Public 57/57 COMPLETE,
  internal 27/107.

- Completed internal MatchingAPI (13/13; internal total 29/107).
  `UpdateTaskListPartitionConfig` stores versioned read/write partition maps
  per (domain, list, type) with Go's normal-kind and config-set validation;
  `RefreshTaskListPartitionConfig` verifies the domain entry; matching
  `DescribeTaskList` now serves the stored config. Single-host notes: no
  adaptive-scaler guard (unported), refresh is a verification (shared state).
- Interop finding (load-bearing for Node clients): prost-0.13 skips default
  map keys AND default message values when encoding map entries, emitting
  degenerate zero-length entry occurrences (e.g. `22 00`). Go emits explicit
  keys and empty values. The bytes are spec-valid and Go/Java/C++/Python
  parse them, but protobufjs (web UI + compat drivers) crashes dereferencing
  the null entry value. Real-SDK impact: none. Compat drivers avoid
  degenerate shapes (non-empty isolation groups); see this entry for the
  analysis trail (`22 00` vs Go-style explicit empties, verified byte-level
  against protobufjs). A future prost upgrade or hand-rolled map encoding
  would close it; do not "fix" by dropping degenerate entries (data loss:
  partition 0 with empty groups means accept-all).
- Validation: 246 workspace tests pass, strict clippy clean, deployed;
  compat drivers all pass (internal:0, grpc:0, sdk:0). Web UI serves (200).
- Next: AdminAPI (0/39, largest remainder); History remainder
  (replication/shard/queue ops); decision-level semaphore grant/release;
  archival backends. Public 57/57 COMPLETE, internal 29/107 (History 16,
  Matching 13).

- AdminAPI batch A1 (12/39, internal total 41/107): DescribeCluster (4 rings,
  memory persistence, SDK versions), RefreshWorkflowTasks (admin delegate to
  history refresh), DeleteWorkflow (describe-first, all three flags true),
  AddSearchAttribute (system-key + type-conflict rejection into a custom
  registry), and all 8 dynamic-config RPCs over a new in-memory overlay with
  normal/operational namespaces (exact filter matching, replace/restore/list).
  New `AdminAPI` gRPC service on the frontend port plus JSON dev routes; no
  Thrift (upstream admin is proto-only in our wire schema).
- Driver lessons: admin proto enums need full names (`INDEXED_VALUE_TYPE_*`);
  domain recreation orphans earlier runs (use fresh runs post-recreate).
- Validation: 249 workspace tests pass, strict clippy clean, deployed;
  compat drivers all pass (internal:0 with admin coverage, grpc:0, sdk:0).
- Next: Admin A2 (isolation groups ×4, async-wf config ×2, UpdateTaskList
  PartitionConfig, MaintainCorruptWorkflow, raw history, admin describe,
  ReapplyEvents); Admin A3 (shard/queue/replication/DLQ/XDC single-host
  semantics). Public 57/57 COMPLETE, internal 41/107.

- AdminAPI batch A2 (18/39, internal total 47/107): global/domain isolation
  groups (get/update with unknown-domain 404s), per-domain async-workflow
  queue config (get/update), and admin `UpdateTaskListPartitionConfig`
  (delegates to the matching adapter through the internal matching service).
  Isolation/async state lives in the domain store with serde defaults so old
  snapshots load; global isolation stays memory-only like Go's cache.
- Validation: 250 workspace tests pass, strict clippy clean, deployed;
  compat drivers all pass (internal:0 with A2 coverage, grpc:0, sdk:0).
- Next: Admin A3 (MaintainCorruptWorkflow, raw history, admin describe,
  ReapplyEvents, shard/queue/replication/DLQ/XDC); decision-level semaphore
  grant/release; archival backends. Public 57/57 COMPLETE, internal 47/107.

- AdminAPI batch A3a (28/39, internal total 56/107): DescribeHistoryHost
  (shard ownership, domain-cache counts, controller status), CloseShard
  (range-validated no-op), RemoveTask/ResetQueue (validated no-ops: no
  addressable persistence tasks/queues exist on a single host),
  DescribeQueue (live decision/activity/timer/backoff/schedule depths as
  queue states), GetWorkflowExecutionRawHistoryV2 (Go-exact validation,
  single-branch event ranges with offset cursors, JSON DataBlob batches),
  admin DescribeWorkflowExecution (shard 0, host addr, mutable-state JSON),
  MaintainCorruptWorkflow (Go's three exact corruption messages: missing
  start, dangling activity reference, discontinuous IDs; deletes on match).
- Debugging notes: V8 async stacks can misattribute the failing await —
  instrument the call site when details mislead. The gRPC→JSON bridge must
  use GET for GET-only routes (shard distribution returned method-mismatch
  INTERNAL otherwise). Proto field is `pageSize`, not `maximumPageSize`.
- Validation: 253 workspace tests pass, strict clippy clean, deployed;
  compat drivers all pass (internal:0 with A3a coverage, grpc:0, sdk:0).
- Next: Admin A3b (ReapplyEvents, replication/DLQ/XDC 10 methods);
  decision-level semaphore grant/release; archival backends. Public 57/57
  COMPLETE, internal 56/107.

- AdminAPI batch A3b COMPLETE (39/39; internal total 67/107). `ReapplyEvents`
  appends signal batches to the open run with request-ID dedup (resetting
  closed runs to their last decision boundary first, no-op when no boundary
  exists), mirroring Go's reset-to-accept path minus version branches.
  Replication/DLQ/XDC honest single-host semantics: empty message fetches,
  zero DLQ counts, validated no-op purge/merge/remove/reset, domain
  replication disabled error (Go-exact message), resend limited to the
  current cluster, empty cross-cluster maps — all with Go-exact validation
  (including the `Events unset → WorkflowID not set` quirk and capital-`ID`
  JSON names `shardID`/`taskID`/`domainID`/`runID`/`workflowID`).
- Validation: 255 workspace tests pass, strict clippy clean, deployed;
  compat drivers all pass (internal:0 with full admin coverage, grpc:0,
  sdk:0). Web UI serves (200).
- Next: History remainder (~28: replication/shard/queue/task ops);
  ShardDistributor (11); decision-level semaphore grant/release; archival
  backends. Public 57/57 COMPLETE, AdminAPI 39/39 COMPLETE, Matching 13/13
  COMPLETE, internal 67/107.

- History batch H1 (+7 → 23/44, internal total 74/107): GetMutableState,
  PollMutableState (immediate, no long-poll), DescribeMutableState,
  history DescribeHistoryHost, ScheduleDecisionTask, RemoveSignalMutableState
  (open-run signal dedup removal), SyncActivity (version-free heartbeat
  apply, unknown runs no-op). Mutable-state reads share one engine view
  (run/type/next-event/previous-started/task-list/sticky/close/size);
  close-status maps to shared WorkflowState/CLOSE enums.
- Validation: 258 workspace tests pass, strict clippy clean, deployed;
  compat drivers all pass (internal:0 with H1 coverage, grpc:0, sdk:0).
- Next: History H2 (record started ×2, child completed, replication ×6,
  failover info, ratelimit, history replication/DLQ passthroughs);
  ShardDistributor (11). Public 57/57 + Admin 39/39 + Matching 13/13
  COMPLETE, internal 74/107.

- History batch H2a (+3 → 26/44, internal total 78/107):
  RecordDecisionTaskStarted (idempotent same-request re-record,
  AlreadyStarted→FAILED_PRECONDITION on conflicting request, NOT_FOUND on
  schedule mismatch/closed/unknown run), RecordActivityTaskStarted
  (idempotent re-record returning current metadata, NOT_FOUND on unknown
  schedule), RecordChildExecutionCompleted (parent-link initiated-ID match,
  workflow-ID check, stale-state guard, terminal child event selection with
  parent-link stamping; non-terminal → INVALID_ARGUMENT). Fixed 5
  `needless_update` clippy errors in the new child-event converter and added
  a `record_child_completed_paths` engine test (invalid/unknown/mismatch
  rejections, completed-event append, link consumption).
- Validation: 261 workspace tests pass, strict clippy clean, build clean,
  deployed; compat drivers all pass (internal:0 with new record-started
  idempotency/conflict/NOT_FOUND coverage, grpc:0, sdk:0).
- Next: History H2b (ReplicateEventsV2, SyncShardStatus, shard/queue ops,
  replication/DLQ passthroughs, failover markers/info, cross-cluster,
  RatelimitUpdate); ShardDistributor (11). Public 57/57 + Admin 39/39 +
  Matching 13/13 COMPLETE, internal 78/107.

- History batch H2b COMPLETE (+18 → 44/44, internal total 96/107).
  ReplicateEventsV2 (Go-exact domain/workflow validation, validated no-op:
  no passive replicas or version branches on a single host), SyncShardStatus
  (Go-exact source-cluster/timestamp validation, no-op), CloseShard/
  RemoveTask/ResetQueue (shard-range check; transfer/timer/replication and
  transfer/timer type gates with Go's exact "Invalid task type"), DescribeQueue
  (live dispatch depths, same shape as admin), GetReplicationMessages (empty
  map; cluster-name validation), GetDLQReplicationMessages (empty),
  ReapplyEvents (shared engine signal-reapply path incl. the missing-blob
  quirk), Count/Read/Purge/MergeDLQMessages (empty/validated no-ops),
  NotifyFailoverMarkers (accepted no-op, Go validates nothing), cross-cluster
  fetch/complete (Go-exact deprecated-feature INVALID_ARGUMENT, incl. the
  with/without trailing period distinction), GetFailoverInfo (known domain →
  Go-exact coordinator record-not-found; no graceful-failover tracking on a
  single host), RatelimitUpdate (accepted, empty response; no global
  aggregation). Fixed an `or_patterns` clippy range lint in the new gates.
- Validation: 261 workspace tests pass, strict clippy clean, build clean,
  deployed; compat drivers all pass (internal:0 with full H2b coverage
  incl. deprecated cross-cluster errors, grpc:0, sdk:0).
- Next: ShardDistributor (11: Ping, Heartbeat, GetShardOwner, InspectShard,
  namespace ×5, Drain/Undrain/GetDrained, ForceReset). Public 57/57 + Admin
  39/39 + Matching 13/13 + History 44/44 COMPLETE, internal 96/107.

- ShardDistributor batch COMPLETE (+11 → 107/107; every RPC in all 13
  services now has a concrete adapter). New `grpc_shard` module on the
  frontend port with in-memory namespace/ownership state: Heartbeat
  auto-creates ephemeral namespaces, validates metadata with Go-exact limits
  (32 keys / 128-char keys / 512KB values) and echoes reported shards as
  READY with `MIGRATION_MODE_ONBOARDED`; GetShardOwner auto-assigns missing
  ephemeral shards to this host while InspectShard stays read-only; unknown
  namespaces/shards are NOT_FOUND; WatchNamespaceState delivers a snapshot
  then holds the stream; ListNamespaces reports live namespaces as onboarded;
  Drain/Undrain/GetDrained mirror upstream exactly (INTERNAL "not yet
  implemented" — upstream leaves them unimplemented too); ForceResetNamespace
  clears executor/owner state with a deleted-key count. `tokio-stream` added
  for the watch stream; fixed the tonic-0.12 `BoxStream` return shape.
- Validation: 263 workspace tests pass (incl. 2 new shard unit tests),
  strict clippy clean, build clean, deployed; compat drivers all pass
  (internal:0 with full shard coverage — heartbeat/owner/inspect/ephemeral/
  state/watch/list/drain-errors/reset — grpc:0, sdk:0).
- Next: engine-depth gaps behind the adapter surface (durable SQL/NoSQL
  persistence, version branches, replication apply, distributed
  ownership/failover, worker background roles, archival backends, dynamic
  config/quotas/auth). All 13/164 RPC definitions have concrete adapters;
  adapter presence does not prove complete engine behavior.

- Cron timezone compatibility (2026-10-15): replaced explicit named-zone
  rejection with bundled `chrono-tz` IANA rules for TZ=/CRON_TZ= schedules,
  including descriptors. Zoned next-fire calculation walks UTC instants to
  preserve both fall-back occurrences and skip nonexistent spring-forward
  times; historical offsets with seconds align to local minute boundaries.
  @every remains elapsed-time based. Tests cover New York winter and DST,
  Kathmandu fractional-hour and historical second offsets, UTC, invalid zones.
  `tests/cron-timezone-compat.go` reproduces expected values with the pinned
  upstream Go robfig library; all six oracle cases pass.
- Limitations: unqualified schedules and explicit Local still assume UTC,
  as before; arbitrary host local timezone configuration is not ported.
  Named-zone searches scan minutes up to the existing five-year horizon;
  sparse/impossible expressions may cost more than the UTC fast path.
  This change does not fill schedule-service timezone/calendar-spec gaps or
  the broader engine-depth gaps listed above. No live server restart yet.
- Validation: full workspace tests pass (263 tests), strict workspace/all-targets
  clippy passes, final cadence-common rerun passes (22 tests), six pinned-Go
  timezone oracle cases pass, and git diff --check is clean. Checks ran in
  tmux cadence-rust. Live SDK drivers were not rerun because this change was
  not deployed to the running server.

- Scheduler inclusive bounds port (2026-10-15): centralized Go's
  computeNextRunTime/computeMissedFireTimes rules in spec-aware evaluation.
  StartTime and EndTime fires are inclusive; delayed ticks after EndTime no
  longer discard the outstanding window. Next-run hints use current time,
  not a stale creation/fire cursor. Backfill includes both endpoints and now
  applies the schedule spec bounds. Unpause evaluation also uses the spec.
- Added tests/fixtures/scheduler-boundaries.json, consumed by Rust and by a
  test overlaid into the real Go scheduler package. The Python runner calls
  real unexported Go functions without copying implementation or changing
  Go source. All 9 differential fixtures pass, including DST. Persistence
  suite: 123 tests pass; strict persistence/all-targets clippy clean. Added
  engine regressions for delayed end-time ticks and bounded backfill.
- Active objective expanded by user: keep working until full migration and
  drop-in compatibility, adding compatibility tests. Do not mark complete
  based on adapter counts. Next immediate audit: catch-up watermark and
  skipped counters, batch continuation, paused/backfill persistence. Major
  production/distributed gaps in migration-inventory.md remain open.

- Scheduler catch-up continuation (2026-10-15): added snapshot-compatible
  LastProcessedTime and pending-pass state. Skips now increment SkippedRuns
  (not MissedRuns), advance the watermark, and cannot be replayed as live
  fires. Unpause policy overrides persist across per-tick activity batches
  and 1000-fire scans, then clear only on completion. Live ticks defer to
  pending catch-up. Catch-up fires use the normal schedule request-ID source.
- New shared catch-up fixtures compare real Go processMissedRunsAt against
  Rust: SKIP/ONE/ALL, three activity batches, and 1500 missed fires across
  the scan cap. Added Rust snapshot restart and no-replay tests. All 14
  shared boundary/catch-up scenarios pass both implementations. Full
  workspace: 269 tests pass; strict workspace/all-targets clippy passes.
- Isolated verification server: tmux cadence-rust:migration-server, HTTP
  18080 / frontend 17833 / history 17834 / matching 17835 / TChannel 17933,
  scheduler+semaphore enabled, no data-dir. Node public/internal compatibility
  drivers pass. Existing server cadence-rust:rust was not restarted. Go SDK
  driver is running against isolated TChannel; final result pending.
- Next recovery gap: mark restored active schedules for catch-up even if
  they were not in a pending catch-up pass at snapshot time. Paused backfill
  queues and backfill continuation are still incomplete. JSON snapshots
  remain development-only; no production persistence or XDC equivalence.

- Scheduler startup recovery: loading execution snapshots now marks active
  schedules for policy-based catch-up, including schedules that were live
  (not pending catch-up) at shutdown. Paused/deleted schedules remain inert.
  Added downtime SKIP/no-replay and legacy-field-deserialization coverage.
  Persistence suite 128 tests passes; strict persistence/all-targets clippy
  passes. The preceding full-workspace checkpoint had 269 passing tests.
- Go SDK TChannel driver against isolated updated Rust server completed with
  exit 0. All three external compatibility drivers passed this checkpoint.

- Scheduler backfill queue (2026-10-15): persisted pending cursors and overlap
  overrides replace the immediate, first-100-only path. Paused backfills stay
  queued, ranges drain over bounded ticks after unpause, total counts use
  Go's 100000 scan cap, and completed items disappear from ongoingBackfills.
  Pending IDs deduplicate, invalid ranges and full ten-request queues are
  ignored like Go handleBackfill (frontend validation remains separate).
  Completed backfill accounting remains internal; it no longer prevents
  reuse of a drained ID. Old snapshots default the new queue to empty.
- Differential runner now has 17 shared scenarios calling real Go functions:
  9 bounds/DST, 5 catch-up continuation/counters, 3 backfill queue/progress.
  All pass. Backfill fixtures save/reload paused Rust snapshots before
  execution. Persistence suite: 129 tests pass; strict persistence/all-targets
  clippy clean. Last full-workspace checkpoint: 269 tests + strict clippy;
  all three external compatibility drivers passed before recovery/backfill
  follow-ups (isolated server currently serves that earlier binary).
- Remaining schedule depth includes buffered trigger-source preservation,
  buffer/concurrency system caps, per-execution budget sharing, real workflow
  scheduler history/visibility parity and failure retry paths. Broader
  production persistence/replication/distributed gaps remain unported.

- Scheduler request IDs / buffered origin / limits (2026-10-15): audit found
  the earlier deterministic-ID implementation used NAMESPACE_OID rather than
  Go's nested UUIDv5(DNS, "cadence.scheduler") namespace. Fixed the namespace
  and normalized backfill trigger IDs to source "backfill", as Go does.
  Buffered entries now retain scheduled time, trigger/backfill origin and
  overlap override across snapshots and draining. Legacy numeric Rust buffer
  entries still deserialize, with schedule-source fallback (their original
  source was not retained by the old implementation and cannot be recovered).
- Applied Go's 1000-entry buffer and bounded concurrency ceilings. Policy
  changes away from BUFFER clear queued fires and count drops as skipped;
  omitted/zero catch-up-window updates preserve the previous positive window.
  Spec updates cancel pending backfills and retain the processed watermark.
- New direct-Go differential fixtures: four exact UUIDs and seven limit
  boundaries. All pass with the previous 17 scheduler scenarios. Rust tests
  cover a backfill override buffered under CONCURRENT, snapshot/drain request
  ID preservation, legacy numeric buffer decoding, 1001 buffered attempts,
  policy clearing and preserved windows. Full workspace checks running.
- Validation for identity/buffer/limit checkpoint: full workspace 275 tests
  pass; strict workspace/all-targets clippy passes. Differential runner
  passes all 17 behavioral scenarios + 4 UUID cases + 7 limit cases.
- Next concrete mismatch found in Go activity.go: CANCEL_PREVIOUS must
  request cooperative cancellation with cause "schedule overlap policy:
  CANCEL_PREVIOUS" rather than close the run as canceled. TERMINATE_PREVIOUS
  uses reason "schedule overlap policy: TERMINATE_PREVIOUS". Both target
  LastStartedWorkflow, not every tracked active run. Rust needs separate
  durable last-started tracking so older cancel-requested runs cannot become
  the overlap target after the latest run closes. Buffer draining also
  needs to use that last target, not all previous active runs. No fix yet.

- Overlap cancellation semantics fixed (2026-10-15): CANCEL_PREVIOUS calls
  the normal cooperative request-cancel path with Go's exact cause; old runs
  stay open until workers close them. TERMINATE_PREVIOUS uses the normal
  termination path and exact Go reason. Both act only on the last-started
  target. Added durable last-started tracking separate from bounded active
  runs, migrated legacy tracking on snapshot load, and stopped retaining an
  unbounded active-run list for non-bounded policies. Buffer draining uses
  the last target. Bounded tracking clears on policy transitions like Go.
- Shared overlap fixture checks real Go cancelWorkflow/terminateWorkflow
  requests using the upstream frontend mock, while Rust verifies history,
  close status and that older cancel-requested runs are never targeted again.
  Differential suite: 19 behavioral scenarios + 4 UUIDs + 7 limits pass.
  Persistence suite 133 tests passes; strict persistence/all-targets clippy
  passes. Prior full-workspace checkpoint: 275 tests + strict clippy.
- Audit next: Go LastRunTime advances monotonically on every fire attempt,
  including skips/buffering/failures, while Rust still updates it only on
  successful starts; buffer draining can move its watermark backward. Also
  missing actions/start errors must count MissedRuns and consume the fire.
  PauseOnFailure and Jitter are present in API types but not acted upon in
  the pinned Go scheduler implementation; do not invent behavior for them.

- Scheduler fire accounting (2026-10-15): LastRunTime and the attempted-fire
  watermark now advance monotonically for every active attempt, including
  buffered/skipped fires and missing actions/start failures. Missed actions
  and start failures increment MissedRuns instead of being retried on every
  tick. Older buffered fires cannot regress time. Removed the invented
  pause-on-failure hook: the pinned Go scheduler does not act on that flag.
  Added real-Go nil-action/monotonic-time fixtures, no-live-retry engine test,
  and extended buffer snapshot/drain coverage. Corrected an old Rust test
  that confused LastRunTime with the last successfully started target.
- Added tests/run-compatibility.py: builds/checks Rust, runs real-Go scheduler
  differential fixtures, starts an isolated Rust server on free local ports,
  runs public/internal Node gRPC clients and existing Go SDK TChannel driver,
  records per-check logs/results.json, and cleans up its server. No Go Cadence
  process is started, and existing development servers are unaffected.
- Full latest-code verification PASSED: 278 workspace tests, strict
  workspace/all-targets clippy, 21 shared scheduler behavioral scenarios +
  4 exact UUIDs + 7 limits, standalone binary build, public gRPC, internal
  gRPC, Go SDK TChannel. Artifacts: /tmp/cadence-replacement-compat/results.json.
  All seven runner steps exit 0; test server cleanup completed. Original
  cadence-rust:rust development server still serves its older loaded binary.
- Next: standalone bootstrap still defaults to go/config/development.yaml;
  embed an equivalent Rust-owned default so a copied binary can start without
  the Go checkout, preserving --config for existing Go config files. Add
  execution from an empty working directory to compatibility verification.
  This is not full replacement completion: production backends, distributed
  ownership/replication/worker roles and other inventory gaps remain open.

- Standalone bootstrap (2026-10-15): default start now reads embedded
  rust/config/development.yaml, a Rust-owned copy of the pinned Go development
  defaults. The executable no longer opens go/config/development.yaml at
  runtime unless explicitly passed through --config. Explicit Go-shaped YAML
  loading remains supported. Added parsed-default equivalence coverage.
- Extended full compatibility runner to copy the built executable into a
  fresh standalone working directory and run every client against it there.
  Separately runs check-config with the existing Go YAML path to verify the
  explicit flag. Runner artifacts retain the copied binary and server logs.
- Latest complete runner PASSED all eight steps: 279 workspace tests, strict
  workspace/all-targets clippy, real-Go scheduler comparisons (21 behavior
  cases + 4 UUID + 7 limits), build, explicit Go config parsing, public/internal
  gRPC clients, existing Go SDK TChannel. Artifact directory:
  /tmp/cadence-standalone-compat (results.json; standalone/cadence-server).
  Its server was cleaned up. Original development server was not restarted.
- Config loading still lacks Go's base/env/AZ layering and CADENCE_* selection
  semantics; this checkpoint removes a runtime source-tree dependency, not
  all configuration gaps. Major production persistence, schema migration,
  distributed ownership/replication/worker roles, archival and inventory gaps
  remain. The active full-migration goal must remain incomplete.

- Version-history primitives (2026-10-15): ported Go persistence range
  coalescing/validation, item containment, LCA selection and prefix copies,
  event-version lookup, branch addition/current selection, first-containing
  branch, rebuilt detection and index validation into version_history.rs.
  Uses existing Cadence API structs. Exact Go errors/panics are preserved,
  including its shorter-history LCA tie-break and same-version non-switch.
- Added 68 shared fixtures with expected outcomes produced by the actual Go
  implementation. tests/version-history-go/main.go remains a direct Go oracle;
  tests/run-version-history-compat.py reruns Go and Rust against the fixtures.
  Full compatibility runner now includes this check. All 68 match; persistence
  suite 136 tests passes; strict persistence/all-targets clippy passes.
- These algorithms are foundational for real replication but are not yet
  attached to execution records. ReplicateEventsV2 remains the old validated
  no-op. Next work: attach history versions/branches, preserve them through
  buffered-event flush/reset/snapshot, then apply remote branch events with
  state rebuild and cluster/version gating. Raw history currently omits version
  metadata; Go's branch-token storage codec uses 0x59 + binary Thrift, not JSON.
  Local Go history events use constants.EmptyVersion=-24; current Rust emits
  zero. Legacy Rust snapshot version-zero histories must be handled explicitly
  when integrating new version tracking. No full replication claim is justified.

- Execution history version/branch metadata integrated (2026-10-15): every
  committed event updates a compressed version range; buffered events wait
  until assigned durable IDs; transient decision events update the range on
  materialization. Starts, children, continue-as-new and reset now carry
  current versions and branch tokens. Reset forks ancestor ranges. Snapshots
  preserve metadata; old Rust zero-version histories are reconstructed without
  rewriting their stream. Local new events/domain failover values use Go's
  EmptyVersion=-24. Request domain metadata seeds new execution versions.
- Added Go-format branch token codec (0x59 + binary Thrift), including typed
  empty ancestor lists, and ancestor-fork logic. Differential oracle now calls
  Go's real codec and SQL ForkHistoryBranch through its mock DB boundary.
  Version-history fixture set expanded from 68 to 82 cases. Branch token JSON
  is now base64, matching Go bytes. GetMutableState/PollMutableState return
  the stored branch token and version histories instead of placeholders.
- Added execution tests for local/provided-domain versions, buffered flushes,
  transient tasks, reset ancestry, snapshot token stability and legacy zero
  histories. External IDL-based client now checks local -24 versions and
  mutable-state branch consistency. Latest full combined runner PASSED all
  nine stages: 284 workspace tests, strict workspace/all-targets clippy, both
  differential suites, standalone build/config, public/internal gRPC and Go
  SDK TChannel. Artifacts: /tmp/cadence-version-state-compat/results.json.
  Copied test server cleaned up; original development server not restarted.
- ReplicateEventsV2 still does not apply events; no replication completion
  claim. Global failover version calculation/gating, passive execution roles,
  remote state rebuilding and distributed persistence remain unported.
- Next concrete replication path: Admin raw-history V2 currently ignores
  proto start/end version items, uses inclusive-inclusive event slicing
  (Go is exclusive-exclusive), returns no version history, emits raw JSON
  timestamps in internal milliseconds, and uses weak offset cursors. Port
  Go admin handler's setRequestDefaultValueAndGetTargetVersionHistory and
  token validation with differential fixtures, then return real compatible
  history batches and selected version history before remote application.

## Incremental raw-history V2 fix (2026-10-15)

Admin GetWorkflowExecutionRawHistoryV2 now honors requested start/end version
items, applies Go-exclusive (start, end) event slicing, switches target branch
via version-history LCA, returns target VersionHistory, and uses structured
RawHistoryToken pagination (domain/workflow/run/start/end/persistence token/
full version histories). gRPC forwarding preserves start/end version history
items and maps response version history. Persistence raw_history updated to
exclusive-exclusive range scanning. Unit and full compatibility runner pass.

## Internal gRPC unimplemented guards (2026-10-15)
Removed remaining explicit unimplemented guards from internal History/Matching
adapters for forwarded polling, decision/activity requeue polling, forwarded
queries, signal_with_start partition config, external child signal/termination/
cancellation context, deferred decision-task heartbeat, and internal start
retry/parent/partition metadata. Requests now route to the existing local
engine instead of failing; these distributed/external-child/failover edge
cases remain behaviorally unported and need deeper state propagation.

## Guard removal and internal start metadata (2026-10-15)
Removed remaining `UNIMPLEMENTED` guards from internal History and Matching gRPC
adapters so forwarded/external-child/retry metadata now route to the local
engine. Added `InMemoryExecutionStore::set_start_execution_metadata` to record
parent link, attempt, expiration, and first-decision backoff supplied by
history's internal start workflow execution. Strict clippy and full workspace tests
pass (284 tests); committed two checkpoints and pushed.

## 2026-10-15: Go-compatible raw-history blobs and cursor normalization

- Replaced raw-history V2's internal-millisecond JSON arrays with Go persistence
  version-0 ThriftRW History blobs (0x59 preamble, typed event list, nanosecond
  wire timestamps); gRPC reports the actual encoding rather than hardcoded JSON.
- Matched Go's emitted empty signal strings/request ID. Added Go-produced byte
  fixtures for signal and empty batches, corruption/trailing-data checks, and
  a differential runner regenerating original Go serializer output in temp files.
- Corrected pagination ordering: normalize omitted bounds against the cursor's
  version-history snapshot before comparing request and token. Original-IDL
  gRPC tests verify page-two continuation and changed-bound rejection, and pass
  the actual network blob through the original Go persistence decoder.
- Full ten-stage compatibility runner PASSED, with strict workspace clippy,
  scheduler/version-history/history-blob differential checks, standalone build,
  original-IDL public/internal gRPC and existing Go SDK TChannel. Evidence:
  /tmp/cadence-history-blob-compat-v2/results.json; exit file 0. Test server cleaned
  up. User development server remains unchanged and has not been restarted.
- Corrected inventory wording so zero RPC adapter gaps cannot imply full behavior.
  ReplicateEventsV2 remains a no-op; production branch storage, original append
  batches, lossless remote timestamp precision and passive state reconstruction
  remain open. Details and source anchors: memory/replication-next.md.

## 2026-10-15: cluster metadata/version migration in progress

- Ported Go cluster.Metadata version arithmetic, owner resolution, migration
  offset fallback, same-cluster comparison and enabled/remote sets into
  cadence-common::cluster. 69 shared cases were produced by the actual Go
  methods and passed Rust differential verification, including signed/overflow
  arithmetic and EmptyVersion handling.
- Parsed configured increment/primary/current/cluster-group data and wired
  global registration's initial version plus FailoverDomain on HTTP/gRPC and
  Thrift paths to metadata. Active-active domain-level version fencing bumps
  from current+1 like Go; full per-attribute merging remains unported.
- Global registration now rejects unconfigured/disabled clusters and missing
  active-cluster membership; failover uses configured offsets rather than +1.
  Added configured round-trip checks 2 -> 10 -> 12 -> 20 and new-run versions.
- Full validation PASSED all 12 stages in cadence-rust:cluster-full-check:
  286 workspace tests, strict clippy, four Go differential suites, copied-binary
  embedded-default startup, configured-server public/internal gRPC and Go SDK
  TChannel. Evidence: /tmp/cadence-cluster-metadata-compat-v4/results.json and
  outer exit file 0. Test servers cleaned up; development server unchanged.
- Earlier checks caught and fixed a local cluster-count condition accidentally
  nested under global validation. The older web IDL drops the destination-check
  override; reverse-failover checks now use the original Go IDL. The Go source
  ledger now records 10 partial and 2290 review-required files, none complete.
- Full cluster config validation/default layering and dynamic per-domain
  migration override evaluation remain incomplete. Public paths currently use
  the standard initial offset; utility fixtures also cover the override branch.
  Existing execution promotion/passive ownership gating and replication remain
  unported. Updating domain metadata alone does not establish failover behavior.

## 2026-10-15: cluster config defaults/validation verified

- Added Go ClusterGroupMetadata FillDefaults/Validate rules to the common
  cluster module. Canonical and deprecated keys remain distinct; primary and
  cluster-group fallbacks, enabled/disabled RPC requirements, transport checks,
  increment/range/duplicate validation match 34 actual-Go differential cases.
  Legacy group fallback retains Go's observable defaulted entries through both
  map names. Oracle retains JSON integers exactly, including int64 maximum.
- Config parsing now honors canonical-vs-deprecated top-level presence (an
  explicit empty canonical object wins over legacy data). Startup/check-config
  validate cluster metadata and no longer replace invalid/missing metadata
  with cluster0 defaults. Embedded defaults remain available when no file is
  specified. Dedicated config/YAML tests and all differential cases passed.
- Full 13-stage compatibility runner PASSED: 288 workspace tests, strict
  workspace/all-targets clippy, four differential suites (including 34 new
  config cases), standalone copied-binary startup, 33 CLI configuration checks,
  original-IDL public/internal gRPC and Go SDK TChannel. Evidence:
  /tmp/cadence-cluster-config-compat/results.json and outer exit file 0.
  Test servers cleaned up; user development server remains unchanged.
- Traced next active-active work; concrete Go/Rust source anchors and newly
  identified no-change/destination-ordering mismatches are in active-active-next.md.
  No active-active engine changes landed in this checkpoint. Full config loader,
  cluster TLS/auth/redirection and persistence/auth validation remain incomplete.
- Source ledger: 11 partial, 2289 review-required files; none marked complete.

## 2026-10-15: active-active registration and ordinary failover metadata verified

- Added domain_replication helpers for Go registration initialization, incoming
  and global membership validation, strict-version scope merge and update-version
  computation. 62 shared cases invoke actual unexported Go functions by overlay;
  they cover nil/empty maps, stale/equal conflicts, untouched state, new scopes,
  negative/disabled/unknown updates, migration offsets and explicit target flags.
- Registration initializes attributes from original per-cluster offsets and
  ignores caller versions. FailoverDomain computes versions from stored state,
  preserves untouched attributes/scopes, rejects no-change attribute requests,
  accepts explicit same-domain targets like Go, and applies Go destination-error
  ordering. Existing attribute audit changes compare versions, not cluster names.
- Proto/Thrift adapters ignore deprecated region maps like pinned Go. Fixed zero
  version emission in Thrift ActiveClusterInfo; actual Go mapper/codec binary
  fixture catches omitted-zero regressions. Proto empty maps normalize like Go.
- Full final-source 14-stage runner PASSED: 290 workspace tests, strict clippy,
  all Go differential suites and copied-binary CLI/standalone/public/internal
  gRPC/Go SDK TChannel checks. Network cases verify partial merges, initial and
  computed versions, audit entries, sorted errors, negative updates, repeated AP
  and AA targets and ignored legacy fields. Evidence:
  /tmp/cadence-active-active-compat-v2/results.json; outer exit file 0.
  Test servers cleaned up; user development server was not restarted.
- The first full run passed before the zero-version Thrift correction; the v2
  run above validates final source including that fix. Source ledger now has
  15 partial and 2285 review-required files, none marked complete.
- UpdateDomain config/failover dispatch still replaces attributes wholesale;
  graceful state transitions/AA rejection, primary restrictions, dynamic override
  lookup, workflow routing, passive ownership and replication remain open.
  Updated active-active-next.md and resume.md with these exact boundaries.

## 2026-10-15: UpdateDomain controller migration in progress

- Added optional configuration/replication patch DTOs so false/zero/empty-string
  and target presence survive Thrift/JSON; protobuf masks no longer prefill a
  stale domain snapshot or return unimplemented for every target change.
- New domain_update candidate controller clones, classifies and validates before
  a single store write. Data merges, explicit emitMetric=false, retention checks,
  bad-binary timestamp/limit/merge/delete behavior and atomic rollback match
  actual Go UpdateDomain. Configuration/failover versions, graceful end/previous
  metadata and AP legacy history data are retained in development snapshots.
  FailoverDomain now shares the candidate state logic after its own validation.
- 33 actual-Go controller cases PASS against Rust, comparing response AND stored
  state/write count. Oracle uses fixed clock, cooldown 0, min retention 1,
  max binaries 10 and disabled archival. The Go binary-only no-write behavior is
  intentionally preserved. Legacy history JSON field order was corrected after
  the first differential check caught that opaque-string mismatch.
- First full run's parallel debug build exceeded 1200s under high IO/memory
  pressure. Confirmed cargo/rustc/linkers terminal, then retried with one job.
  Serial workspace tests, strict clippy and all differential suites passed;
  remaining network validation is in /tmp/cadence-domain-update-compat-serial.
  Runner now defaults CARGO_BUILD_JOBS to 1, retaining explicit overrides.
- Exact unfinished policy/lifecycle scope is in domain-update-next.md: default
  global cooldown (Go 1 minute) and dynamic policies remain unenforced; archival
  providers, graceful callbacks/queues, notification fencing and replication are
  not complete. No full domain-service completion claim is justified.
- Ledger now has 16 partial and 2284 review-required Go files, none complete.

## 2026-10-15: UpdateDomain final controller checkpoint verified

- Final Go-source audit found legacy failover-history default is FIVE entries,
  not the initially selected ten. Corrected the bound and added a 34th actual-Go
  case checking trimming plus HTML and Unicode separator escaping inside the
  opaque history string. All 34 controller cases now match Go exactly within
  their stated policy/archival configuration.
- Final 15-stage runner PASSED with 291 workspace tests, strict clippy, all
  differential suites, standalone/config checks, original-IDL public/internal
  gRPC and existing Go SDK TChannel. Evidence:
  /tmp/cadence-domain-update-compat-final/results.json and outer exit file 0.
  Runtime source was frozen for this run; subsequent Rust edits were focused
  rustfmt formatting only. Test servers cleaned up; development server unchanged.
- Updated source ledger, resume.md, active-active-next.md and domain-update-next.md.
  Highest next gap is runtime cooldown/dynamic policy, not further test-count
  expansion. Full archival, notification/distributed graceful state processing,
  replication and production persistence remain incomplete. Goal stays active.

## 2026-10-15: switch to service-by-service migration, Matching first

- User explicitly requests completing one service role and validating it with
  other Go roles before moving on. New service-migration-plan.md tracks Matching,
  History, Frontend and Worker completion gates. Matching remains incomplete:
  local-engine-coupled adapters do not replace Go Matching in a mixed cluster.
- Ported partition name/parser/tree arithmetic with 42 original-Go fixtures;
  enforced name validation in internal polling, add, cancel, describe and config
  routes; partition listings resolve the root for non-root inputs.
- Corrected config validation, root/kind restrictions, default/unchanged no-ops,
  server-assigned versions, refresh version gating and nil reload, plus local
  notification/cache and snapshot recovery. Twelve actual-Go manager operation
  sequences compare configs/errors. Production metadata storage remains pending.
- Ported common messaging acknowledgement state with 15 original-Go operation
  sequences: sparse/out-of-order/duplicate reads and acks, monotonic setters and
  Go's backlog increment on invalid reads. Queue integration/logging remain pending.
- Fixed --services matching/history opening unselected frontend ports/routes.
  New process test holds unused ports occupied and verifies health-only HTTP.
- Built original Go server in /tmp/cadence-go-mixed; booted all four roles with
  SQLite/automatic schema setup. An 11-event workflow/activity/heartbeat probe
  passed, then direct Matching partition RPCs passed as well. Reproducible runner
  tests/run-go-service-baseline.py produced /tmp/cadence-go-baseline-repro/results.json
  (probeExitCode 0). Its process is cleaned up by the runner. Ad hoc baseline was
  also stopped. This is all-Go baseline evidence, not mixed-service validation.
- Go baseline exposed required rpc-encoding: proto and deprecated partition-count
  behavior: when counts disagree with map size, Go synthesizes count partitions
  and discards supplied metadata. Added nine actual-Go mapper fixtures, matching
  Rust conversion, output counts and client fixtures containing consistent counts.
- Initial full Rust suite passed 295 tests/clippy and pre-existing differential
  suites, then network failover hit the latest commit's real cooldown default.
  Runner now explicitly sets test-only cooldown 0 through admin; product default
  stays one minute. Further network validation is running after the wire fixes.
- Attempting to include the pre-existing dynamic-policy oracle exposed an existing
  harness error: numeric EncodingType fixtures fail Go's string enum unmarshal,
  and fixtures lack expected values. That suite was not in the existing combined
  runner; it remains excluded and is NOT claimed validated by this checkpoint.
- Final in-progress runner: cadence-rust:matching-wire-final;
  /tmp/cadence-matching-wire-final.log and artifacts/results.json. Record final
  outcomes before calling this checkpoint verified. No entire service is complete.

- Final verification PASSED: 18 combined-runner stages, 296 workspace tests,
  strict clippy, all included original-Go suites, service-selected listener checks,
  copied executable/config checks, shared Matching/workflow probe, public/internal
  gRPC and existing Go SDK TChannel. Evidence:
  /tmp/cadence-matching-wire-final/results.json and outer exit 0. Isolated Rust
  server and both Go baseline servers were cleaned up. Work is a Matching
  checkpoint; independent durable queue/History delivery/mixed topology remain.

## 2026-10-15: independent Matching SQLite persistence

- Added matching_blob for Go SQL task/list/config structs, raw Thrift and Snappy,
  preserving nanoseconds, UUID bytes and optional containers. Eight original-Go
  encoder fixtures match bytes; compressed Go input and Rust output interoperate.
- Added SqliteTaskStore on exact Go task_lists/tasks schema: lease fencing,
  atomic batch append, ordered reads/counts, metadata expiry, deletes and restart
  recovery. Server selection and Matching RPC integration are NOT done yet.
- Actual Go SQLite TaskStore and Rust exchange a database bidirectionally, with
  mixed encodings, config writes, ownership change, stale-write rollback and
  bounded delete. This uses real SQLite and real Go serialization, not mocks.
- Final full compatibility runner PASSED 19 stages, 303 workspace tests,
  strict clippy, all included Go differential checks, standalone/config/listener
  checks, public/internal gRPC and existing Go SDK TChannel. Evidence:
  /tmp/cadence-matching-storage-final/results.json and outer exit 0. Server cleaned up.
- Source ledger now has 25 partial and 2275 review-required Go files, none complete.
- Continuing Matching queue writer/reader next. A new matching_queue.rs draft is
  not exported yet and was not compiled by this checkpoint. Preserve this boundary.

## 2026-10-15: durable Matching queue and actual Go History delivery

- DurableTaskQueue ports range allocation, read/ack recovery, bounded scan rules,
  concurrent claims, expiry and rewrite-before-ack. Six focused tests + actual Go
  reader/expiry fixtures pass. Fenced rewrite preserves original task in storage.
- MatchingHistoryClient sends real History RPCs with YARPC headers, per-call
  timeout, stable request UUID across Go-policy retries, typed application error
  classification and Go-compatible matching responses/task tokens. Three focused
  metadata/retry/cancellation tests pass; cargo check succeeds.
- Live original-Go Frontend/History with Rust durable queue/client PASSED:
  decisions -> activity/header -> heartbeat -> completion -> 11-event history.
  Rust process reopens its queue for each delivery, and Go frontend accepts Rust
  tokens. Evidence /tmp/cadence-matching-go-history2/results.json, probe.log and
  outer exit 0. Harness retries only the known Go new-domain provisioning cache
  race using the same StartWorkflow request ID. Servers cleaned up.
- This is a boundary test, NOT replacement of the Matching listener. Independent
  queue controller, RPC wiring, cancellation ownership, routing/membership,
  query/poller/config and sync/passive/distributed paths remain incomplete.
- Combined runner now includes original-Go delivery as a final separate stage,
  after no-Go standalone Rust validation and SDK checks. Full regression runs
  in cadence-rust:matching-delivery-full; record final results once it completes.

## Verified delivery checkpoint (2026-10-15)

All 21 compatibility stages passed using the unchanged original Go server:
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-dispatch-verified/results.json`
(outer exit 0). This includes SQLite/queue Go differential fixtures, strict
workspace clippy, SDK checks, and an 11-event workflow with Rust queue delivery
through actual Go History and completion through Go Frontend. Final cancellation
worker checks passed: 7 focused tests and strict workspace all-target clippy
(`/tmp/cadence-matching-dispatch-last-check.exit` = 0).

The listener still uses the embedded engine. Next: independent Matching queue
registry and RPC integration, then route Go Frontend/History to Rust Matching.
Queries, forwarding, ownership, domain lifecycle, throttling, semaphores, internal
transport and production membership remain. Matching is NOT complete.


## Queue registry checkpoint (2026-10-15)

Added `matching_registry::QueueRegistry` and QueueHandle. Lease acquisition and
registration share one blocking worker; concurrent requests get one owner.
Append and notification share a worker so RPC cancellation does not skip wakeup.
Ack flush visits all owners despite individual errors. Shutdown stops handles;
unload checks pointer identity so old teardown cannot remove a replacement.
Four registry tests plus seven delivery/History tests passed:
`/tmp/cadence-registry-unload-tests.log` (11 passed). Strict workspace all-target lint passed;
`/tmp/cadence-registry-unload.exit` = 0.
This is an independent lifecycle component, not a running Matching replacement.
RPC integration, configured ack timer, domain cache, ownership checks and idle
unload still remain. Go reference: task_list_registry.go and task_reader.go.

Before AddActivity RPC wiring, port/test a source-domain SQL edge: original
`sql_task_store.go:CreateTasks` writes each row with v.Data.DomainID while fencing
TaskListInfo.DomainID. AddActivity engine supplies SourceDomainUUID as TaskInfo
DomainID. Current Rust create_tasks assumes the queue domain for every row.
Preserve pinned Go behavior; do not silently normalize source to target domain.


## Source-domain SQL compatibility (2026-10-15)

Ported Go's per-task source-domain SQL row keys with
SqliteTaskStore::create_tasks_with_domains; original same-domain create_tasks
delegates to it. The target TaskListKey still controls transaction lease fencing.
DurableTaskQueue::append_in_domain exposes that distinction without adding fields
to Go's exact task blob. Reads/rewrite of recovered tasks continue using the queue
domain, as pinned Go SQL does. Do not normalize this behavior in RPC integration.

Expanded actual-Go shared-database interop checks source rows in both directions,
absence under target domain, and rollback of a rejected stale-lease source row.
/tmp/cadence-source-domain-interop.exit = 0. Seventeen focused persistence tests
and workspace all-target strict lint passed: /tmp/cadence-source-domain-final.exit
= 0. Full accumulated regression PASSED all 21 stages and 321 workspace tests:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-registry-source-domain/results.json;
/tmp/cadence-registry-source-domain-full.exit = 0. Includes strict lint,
original-Go differentials, standalone SDK checks and actual-Go History delivery.

Matching remains incomplete; this does not wire an RPC service or provide sync
matching for source-domain activities.


## 2026-10-16: Matching protobuf RPC and actual mixed-service SDK validation

See matching-rpc-next.md. Added opt-in durable AddTask/Poll backend with domain
validation and cancellation. Built a test-only Go Fx PeerProvider decorator;
original Go Frontend/History/Worker now route to Rust Matching, with no Go
Matching process. Basic 11-event probe and existing Go SDK workflow/activity pass.
Full accumulated validation: all 22 stages and 324 workspace tests, strict lint;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-final-20261016/results.json.
Expanded existing SDK additionally passed activity retry, child, workflow-run/task
retry, activity timeout and signal scenarios, separately verified at
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-sdk-expanded2/results.json.
Domain metadata remains seeded, discovery substituted in tests, other matching
methods use embedded state, and forwarded sync matching is explicitly unported
in the opt-in backend. Matching is not complete and is not selected by CLI config.

## 2026-10-16: Native Matching poller history and DescribeTaskList

See matching-pollers-next.md. Common poller manager matches ten original-Go
manager/cache snapshots for identity deduplication, recency, outstanding/group
state, cancellation, empty IDs, TTL and 5000-entry eviction. Native poll RPCs
record wait cycles; EndPoll precedes History start and begins again on retry,
with a focused timing assertion. Native DescribeTaskList returns actual pollers
and queue read/ack/backlog/block/empty fields. Go Frontend mixed probe verifies
the native history/status and all seven SDK scenarios. Remaining rate/isolation
controller metrics, callbacks and lifecycle work are still explicitly partial.

Final post-correction workspace run PASSED 327 tests:
/tmp/cadence-poller-workspace-final.exit = 0. Final 16 focused Matching tests and
strict all-target workspace lint PASSED: /tmp/cadence-poller-timing-tests.exit = 0.
Full 23-stage compatibility runner PASSED:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-poller-final-20261016/results.json.
All four deployment roles remain incomplete. Continue Matching with configured
ack maintenance/ownership unload, queries and sync matching before moving roles.

## 2026-10-16: Per-owner acknowledgement pump and fenced-owner lifecycle

See matching-maintenance-next.md. Native registry maintains independent per-owner
ack deadlines, resets after updates, survives individual failures and unregisters
stopped/fenced owners. Stopped writers reload on next get; unload/shutdown wake
blocked dispatchers. Mixed fixture owns the pump with Go's one-minute default and
awaits final shutdown. All 20 focused Matching tests and strict workspace lint
passed (/tmp/cadence-maintenance-final.exit = 0); mixed Go Frontend/History/Worker
with Rust Matching and seven SDK scenarios passed
(/tmp/cadence-maintenance-mixed.exit = 0). Full prior 327-test/23-stage checkpoint
is separate. GC/controller shutdown sequencing, idle unload, dynamic per-list
configuration, ownership discovery and query/sync matching gates remain open.

## 2026-10-16: Go-style Matching GC and final stopped-writer acknowledgements

See matching-gc-next.md. Added shared per-queue collector with cursor, thresholds,
nonblocking single ownership, repeated full-batch drain and failure cooldown.
Native dispatcher uses default 100-row/one-second GC; registry forces teardown
drains and persists final reader ack even after writer stop, while retaining SQL
lease fences. Recovery/shutdown tests verify final metadata and physical cleanup.
Fourteen actual-Go GC fixtures and strict time/error Rust tests pass.

Fresh full regression PASSED 333 workspace tests, strict lint and all 24 stages:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-gc-verified/results.json;
/tmp/cadence-gc-verified.exit = 0. Includes actual mixed Go services/Rust Matching
and seven SDK scenarios. First build attempt was invalidated by editing shared
API during compilation; it was replaced by the complete frozen-source run.
Strict stop/unregister/in-flight ordering, idle unload, dynamic controller config
and remaining Matching query/sync/routing/transport gates remain incomplete.

## 2026-10-16: Matching teardown, idle unload and closed-poll parity

See matching-idle-next.md. Queue stop is atomic/nonblocking, registry cleanup
runs outside the map lock after identity-checked unregister, replacement metadata
stays fenced, and new/fatal owner notifications reschedule the controller. Added
Go liveness predicate and half-TTL idle ticks, only refreshed by task adds/new
GetTask waits. Pending rows survive idle unload. Closed pumps return empty
successful polls, matching Go engine branches rather than Unavailable.

Final source-frozen full run PASSED 342 workspace tests, strict lint and all
25 stages: /tmp/cadence-idle-verified.exit = 0; artifacts at
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-idle-verified/results.json.
Actual mixed Go Frontend/History/Worker + Rust Matching passes idle empty-poll
and pending-task recovery scenarios plus seven existing SDK cases. The earlier
run was deliberately interrupted to correct closed-pump response semantics.

Source audit clarified that manager stopWG waits for partition notification, not
all History RPCs; reader stopWg tracks background reader loops. Full reader/writer
pipelines, startup/concurrency, dynamic config, transport drain, sync/query and
remaining service gates are still incomplete. No complete service is claimed.

## 2026-10-16: Matching sequential append writer

See matching-writer-next.md. Native AddTask and delivery rewrites now use bounded
sequential batching, Go's additional-request batch limit, differentiated enqueue/
response timeouts, cancellation-independent persistence and source-domain batch
fencing. Actual-Go batch oracle plus three Rust actor tests pass. Full regression
PASSED 345 workspace tests, strict lint and all 26 compatibility stages, including
mixed Go services/Rust Matching SDK validation. Evidence:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-writer-verified/results.json;
/tmp/cadence-writer-full.exit = 0. Reader, startup, dynamic configuration, lease
retry and remaining Matching service gates are still incomplete.

## 2026-10-16: Per-queue startup serialization

See matching-startup-next.md. Queue creation/cleanup now holds only a per-key gate
through lease I/O; loaded owners remain accessible. Delayed startup after shutdown
cleans its unpublished owner and returns Unavailable. Weak gates avoid retaining
all previously seen identities. Real SQLite contention, 15 registry tests, strict
workspace lint and mixed Go services/Rust Matching with seven SDK cases PASSED.
/tmp/cadence-startup-{check,lint,mixed}.exit = 0. No complete role is claimed.

## 2026-10-16: Matching lease retry and terminal writer failure

See matching-retry-next.md. Startup/rollover lease retry now uses Go's 50ms/10s/
30s policy, transient driver classification, previous-error convention and
advancing attempt timestamps. Terminal rollover failures stop admission even for
non-condition errors. CreateTasks stays single-attempt. Ten actual-Go outcomes
and real SQLite temporary trigger failure/corrupt metadata tests pass.
Final full validation PASSED 348 workspace tests, strict lint, all 27 compatibility
stages including mixed Go services/Rust Matching and seven SDK scenarios:
/tmp/cadence-retry-full.exit = 0;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-retry-verified/results.json.
Reader/matcher pipeline, dynamic config and all remaining service gates stay open.

## 2026-10-16: Compaction checkpoint and service sequence confirmation

Resumed at f10e5bfca with a clean worktree. User explicitly reconfirmed service-by-
service migration: finish Matching and validate against Go Frontend, History and
Worker before moving on. Latest full validation is 348 workspace tests and all
27 compatibility stages. Next scoped gap is reader persistence retry, followed
by background reader/matcher coordination. Full migration remains incomplete.

## 2026-10-16: Matching bounded reader persistence retry

See matching-reader-next.md. Bounded reader SQL queries share the Go retry policy;
reader claims serialize independently from queue state, allowing completions and
appends during backoff. Empty scans compare current ack after SQL. Real SQLite
table outage/recovery test verifies no task loss and completion concurrency.
Validation PASSED 349 workspace tests, strict lint and all 27 compatibility stages
including mixed Go services/Rust Matching; /tmp/cadence-reader-full.exit = 0 and
/tmp/cadence-reader-final.exit = 0. Matching completion gates remain open.

## 2026-10-16: Matching expiry checks use admission time

Queue accepts injectable per-row clock; production dispatcher reads wall time
after storage retries. Equality/later boundary tested across consecutive rows.
11 queue tests, strict lint, and mixed Go services/Rust Matching passed.
See matching-reader-next.md; active service remains Matching.

## 2026-10-16: Matching incremental reader admission

Persistence reader now supports blocking/cancellable per-row admission without
registering the rest of a fetched batch. Actual Go buffer-cancellation oracle
verifies three blocked-row/backlog outcomes. 13 queue tests, strict lint, all Go
reader differential groups and mixed Go services/Rust Matching SDK run passed.
See matching-reader-next.md. Background pump/matcher integration remains next.

## 2026-10-16: Matching background backlog reader and local handoff

See matching-background-next.md. Native owners now prefetch through a batch-1
buffer and separate dispatch head, using Go default1000 batch/50000 range, owned
reader turns with per-row registration and asynchronous buffer waits. Local
rendezvous handles cancellation without losing tasks. Teardown joins reader
loops outside blocking pool after final ack/GC. Expiry is rechecked at dispatch,
with active-domain deadlines and passive-domain indefinite heads based on live
seeded metadata, including failover/attribute activity rules.
Validation PASSED: 364 workspace tests, strict lint, all27 compatibility stages,
actual mixed Go services/Rust Matching SDK cases. Latest focused41 Matching tests
and domain helper test passed; actual Go activity tests and buffered admission
comparison passed. /tmp/cadence-background-checked.exit = 0 and
/tmp/cadence-background-acceptance.exit = 0. Full Matching remains incomplete.

## 2026-10-16: Matching failed-delivery rewrite retry

Writer preserves private typed transient classification while append public
Status behavior stays unchanged. Rewrites use50ms/10s/30s persistence retry and
1s/10s Busy throttle, retaining Go previous-error semantics. Normal AddTask stays
one append. Ten actual-Go outcomes and SQLite insert-trigger failure/recovery
verify retry before ack and gap IDs. 366 workspace tests, strict lint and all27
compatibility stages incl mixed Go services/Rust Matching PASSED;
/tmp/cadence-rewrite-full.exit = 0. See matching-rewrite-next.md.
Further source audits saved in matching-db-lock-next.md and matching-rate-next.md.

## 2026-10-30: Matching compatibility harness secondary restart readiness

`tests/matching-service-compat.cjs` now accepts the target Rust process and
gRPC port in `waitForRustReady`, so secondary restart readiness probes,
process-evidence capture, and TLS evidence use the restarted secondary endpoint
and PID. The helper still requires the Go Frontend `DescribeTaskList` discovery
probe and directly verifies the target endpoint. Audited all secondary start
sites: the initial instance retains `rust-server-secondary.log`, and each of
the six restart paths now writes a unique secondary log and invokes the
parameterized readiness helper. Existing recovery assertions remain in place.

Source-only candidate at `t3/fix-secondary-restart-readiness`. Review was limited
to source inspection and `git diff --check`; no Node syntax check, build, runtime,
or tests were run, and no CI was dispatched or polled. This corrects harness
readiness attribution only and does not establish Matching service parity.

## 2026-10-16: Matching lease backoff no longer blocks reader state

Separate writer serialization and DB metadata gates keep queue state free during
SQL and lease backoff. Ack snapshot/update remains fenced against self-renewal.
Creation time is sampled after DB gate acquisition. SQLite renewal trigger test
verifies old task delivery and ack update progress before recovery. 367 workspace
tests, strict lint and all27 compatibility stages incl mixed Go services/Rust
Matching passed: /tmp/cadence-lock-full.exit = 0. See matching-db-lock-next.md.
Rate-report oracle and scalar draft prepared under/tmp; details in matching-rate-next.md.

## 2026-10-16: Matching dispatch rate report aggregation

Pure cadence-common matching_rate state port now matches11 actual-Go sequences:
TTL/received boundaries, precision asymmetry, partitions/provider reads, fractional
/zero/negative reports, backwards fake time and arm64 overflow burst conversion.
Actual oracle corrected draft overflow assumption. New differential stage brings
runner to28. All30 common tests, Go/Rust comparison and strict workspace lint
passed (/tmp/cadence-rate-common.exit=0; /tmp/cadence-rate-final.exit=0).
No native pacing claim: clock reservations/token bucket and RPC/reader wiring
remain next. Last full live run is DB-lock checkpoint367 tests/27 stages.

## 2026-10-19: Matching domain metadata read-cache parity draft

Read-only Matching domain metadata cache draft and deterministic Go oracle are
in `matching-domain-metadata` worktree. Behavior covers metadata-first paging,
strict notification cutoffs, snapshot preservation on page errors, cache misses
by name and ID, callback registration/catch-up ordering, config/failover fields,
and retryable refresh errors. SQLite connection-lock/busy/query waits use the
three-second budget; raw nested blobs stay intact. Focused Rust cache tests passed
10/10 and strict workspace Clippy passed after adding the server's direct
rusqlite dependency. See `memory/evidence/matching-domain-cache/` for logs and
exit codes. This is a cache module draft, not production wiring or Matching
completion; mixed-service SDK validation and full parity audit remain open.

## 2026-10-19: Matching domain cache retry/deadline acceptance follow-up

The metadata-first refresh now shares Go's outer three-second context across
metadata plus all pages. Retry stops rather than scheduling intervals under the
one-second initial interval and retains Go's previous-attempt error behavior.
SQLite lock/VM cancellation maps to a nonretryable timeout only after its
operation deadline; immediate SQL failures retain their regular transient
classification. Lifecycle wait/self-join/worker-publication races are addressed.
Go oracle JSON trace matches the final Rust trace. Persistence-focused tests
passed 9/10 (one pre-existing Go-database interop test is intentionally ignored),
cache-focused tests passed 14/14, and locked strict workspace Clippy passed.
See `memory/evidence/matching-domain-cache/`; production wiring and mixed-service
validation remain outstanding.
