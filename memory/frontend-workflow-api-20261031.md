# Frontend workflow API port — 2026-10-31

## Current branch and skeleton state

- Implementation checkout: `t3/frontend-workflow-api` at `94e79ff98`.
- `frontend-core` is a separate local worktree at `/home/auenkr/.t3/worktrees/cadence-rust/t3-57c063ab`, also at `94e79ff98`.
- `git fetch origin frontend-core` failed because there is no remote ref named `frontend-core`.
- The frontend crate currently contains only its launcher placeholder, `lib.rs`, and `main.rs`; neither worktree has the promised core handler/module files yet.
- The project instructions call for keeping implementation under `rust/crates/cadence-frontend/src/` and not colliding with the core skeleton.

## Landed in this checkpoint

- Added `wrappers/clusterredirection.rs` with Go policy names, V1/V2/V3 allowlists, deprecated-domain allowed API set, policy parsing, and pure forwarding selection (strong consistency, active-active domain, all-domain target match and selected API fallback).
- Added policy unit tests for the exact allowlist version deltas, all-domain fallback behavior, and deprecated-domain API list.
- Exposed the module through `wrappers/mod.rs` and `lib.rs`.

## Validation and evidence

- `git diff --check` passed.
- No Cargo command was run locally, per requirement.
- CI has not yet been dispatched; no run URL, head SHA, or conclusion exists for this checkpoint.
- This module is an initial pure-policy slice and does not establish full cluster redirection parity. The stateful Go behavior (domain cache, active-cluster manager lookups, config gates, fallback on `DomainNotActiveError`, logging, and metrics) remains to be connected to the frontend handler interfaces.

## Remaining work

- Reconcile with the `frontend-core` handler skeleton when it lands; import `handler_workflow*.rs` without overwriting its existing files.
- Port throttling, metering/error translation, version checking, access control, redirect call wrappers, and Thrift/gRPC wrapper behavior against the concrete Rust handler, config, errors, metrics, and transport APIs.
- Port `go/service/frontend/api/handler.go` workflow methods and mirror their Go tests, including workflow lifecycle, polling/responding, visibility, history, and list/scan/count behavior.
- Run the requested `rust-ci.yml` CI dispatch for `cadence-frontend`, record the run URL, exact head SHA and conclusion here, and report any parity gaps before claiming completion.
- Mixed Go/Rust SDK compatibility and restart/recovery gates remain outstanding.

## Go handler method inventory (initial)

`go/service/frontend/api/handler.go` implements `WorkflowHandler` lifecycle and
health, diagnostic execution, both poll methods, heartbeat/heartbeat-by-ID,
activity and decision completion/failure/cancellation, query completion,
sync and async start/signal-with-start, workflow signal, history retrieval,
terminate/reset/cancel/restart, search attributes, query, describe, archived
history conversion/validation, and cluster info. The major user-facing method
locations are `StartWorkflowExecution` (1652), `GetWorkflowExecutionHistory`
(1836), `SignalWorkflowExecution` (2114), `SignalWithStartWorkflowExecution`
(2270), terminate/reset/cancel/restart (2455-2570), query (2643), and describe
(2713). List/scan/count and task-list endpoints are implemented in adjacent
frontend API files and still need a separate inventory. This is a source map,
not a completeness audit.

CI run [41172629964](https://github.com/auenkr/cadence-rust/actions/runs/41172629964)
was dispatched for `cadence-frontend` at head
`ab5b2fae8285dd9b73326c1ec4119b2eb1d67839`. Last observed state: in progress;
no conclusion available yet.

## User-directed continuation — core rebase and implementation

The earlier missing-skeleton observations above are historical and superseded.
On 2026-10-31, fetched `origin/frontend-core` at `4fe22eea2` and rebased this
branch onto it. The rebase kept core's `run()` launcher, its full wrapper module
list, and added the policy slice. The rebased task branch was pushed with
`--force-with-lease`.

`origin/frontend-core2` now points to `e268cf3c5`; its request validator and
generated History/Matching gRPC client additions were inspected as the
parallel worker's current interface. Do not duplicate those changes. As of this
save, that client commit is not merged into the task branch.

Current uncommitted implementation work:

- Stateful cluster forwarding now models deprecated-domain rejection,
  local/global routing, active-active workflow selection, policy allowlists,
  strong consistency, and one retry to a different active cluster from a
  `DomainNotActiveError`. It has local unit tests for selected forwarding,
  fallback, and deprecated-domain behavior.
- Rate limiting now models the user/worker/worker-poll/visibility/async
  policy classes, poll wait/deadline behavior, caller bypass, exact Go busy
  message, and sticky task-list base-name selection.
- Metered error normalization/counter mapping, access control decisions and
  latency hooks, version-check exemptions, and Thrift/gRPC service metadata
  now have isolated modules and focused unit tests.
- `handler_workflow.rs` no longer returns an unconditional InternalServiceError
  for every operation. It names 40 Go workflow, worker, visibility, and
  task-list operations and dispatches requests through `WorkflowBackend`.

Still required before parity: wire these wrapper policies into a real ordered
handler decorator; port the body of every Go `WorkflowHandler` method rather
than forwarding generic JSON through `WorkflowBackend`; implement all Go
validation/token/history/visibility/archive transformations and exact metrics
tags; and add a production backend implementation using the shared History,
Matching, persistence, domain-cache, and visibility interfaces. The current
`frontend-core2` changes only provide validator/client primitives. The needed
interface surface is a typed workflow-engine/backend with History calls for
start/signal/cancel/terminate/reset/history/query/respond/heartbeat, Matching
poll and task-list calls, visibility list/scan/count calls, and injected domain
cache/config/metrics/archive/task-token services. Confirm the concrete owner
interfaces before adding overlapping client or persistence code.

The wrapper tests and new handler tests have not been run locally (Cargo is
forbidden on the dev host). `git diff --check` is source-only. CI run
[41174711275](https://github.com/auenkr/cadence-rust/actions/runs/41174711275)
tested SHA `48b30c7d16c76d325b3e318ab5eb1771f205999d` and failed in Clippy
because the three skeleton domain/admin/schedule handlers still import the
compatibility `unimplemented_operation` helper and a test backend used
`RefCell` despite the `Sync` contract. Both compile errors were fixed in the
current working tree. A fresh `rust-ci.yml` dispatch is required after pushing
these fixes; no green CI evidence covers the current changes.

## Follow-up implementation checkpoint

- `origin/frontend-core` was fetched and confirmed as an ancestor of this
  branch (`git rev-list --left-right --count origin/frontend-core...HEAD` was
  `0 6`). `git rebase origin/frontend-core` completed as an up-to-date no-op;
  no frontend-core commits were dropped.
- Checkpoint `a26cf95b4` (wrapper semantics/history-token helpers) and
  `563282d4f` (handler validation) were pushed to
  `origin/t3/frontend-workflow-api`.
- Handler validation now preserves Go ordering for history requests, requires
  query/query type after execution validation, checks diagnose's required run
  ID, validates heartbeat token presence, and checks heartbeat by ID
  workflow/activity fields before calling the backend. Tests cover query and
  diagnose validation order.
- The handler now runs the generated ratelimit wrapper's request preflight
  before handler validation: nil/domain checks for user, async, visibility and
  worker-poll operations; token presence for token-based worker operations;
  and domain checks for worker by-ID and sticky-reset methods. Quota policy
  evaluation still needs a live decorator/config injection.
- The authorization request filter strips hidden payload fields from nested
  async requests as Go's `json:"-"` fields do. Metering normalizes errors
  outside Go's typed error switch to the generic internal error and increments
  failures.
- Async start and signal-with-start wrappers carry an explicit nested
  `request` in the Cadence Thrift shape. Handler validation, rate-limit
  preflight, authorization attributes and metered tags now unwrap that request
  for Go's promoted domain/workflow/task-list fields while preserving the
  original envelope for dispatch.
- Merged `origin/frontend-core2` at `981e17c690d108d42a2ce2a2eb0f3cba394d9de8`
  (its package CI run
  [41176185814](https://github.com/auenkr/cadence-rust/actions/runs/41176185814)
  concluded `success`) to use its owned request-validator methods and generated
  History/Matching client interfaces. List/scan/count, open/closed/archived
  visibility, task-list, sticky-reset and refresh handlers now invoke the
  corresponding validator entry points. Page-size defaults are copied back
  into the original JSON request without dropping unknown SDK fields.
- Added handler tests for list page-size mutation and unknown-field retention,
  open-list validation, task-list validation, and rate-limit wrapper preflight.
- Added a quota-dimension extractor for the generated rate-limit wrapper. It
  uses nested async request fields, sticky task-list base names, and decodes
  task-token requests before resolving their domain UUID through an injected
  `DomainNameLookup`.
- Added `allow_operation` to apply Go's generated behavior: denied user,
  visibility, async and poll requests return the rate-limit error, while worker
  calls count the limit but ignore denial. History dispatch now decodes a
  supplied continuation token and returns Go's invalid-token and run-ID
  mismatch errors before backend invocation.
- CI run
  [41175955180](https://github.com/auenkr/cadence-rust/actions/runs/41175955180)
  at `a26cf95b4e110bb4a2ec5902771d4e2a7552e0b4` was cancelled when a newer
  branch run superseded it. Run
  [41176066100](https://github.com/auenkr/cadence-rust/actions/runs/41176066100)
  at `563282d4fa10e1c4cc29659722cfd9d99e6b337c` was in progress at this save;
  it does not include later handler changes. No green CI conclusion exists for
  the current head yet. Local `git diff --check` passes; Cargo remains unrun
  locally.

## Remaining parity blockers

- `Handler::dispatch` still relies on the generic `WorkflowBackend::invoke`
  seam. No concrete production implementation translates all workflow
  operations into History, Matching, visibility and domain-cache calls, so the
  operation catalogue is not functional service behavior.
- The backend interface needed by this handler must support domain-name/ID
  lookups; typed History operations (poll mutable state, history pages/raw
  history, start/signal/cancel/terminate/reset/query/describe,
  heartbeat/respond, sticky reset); Matching poll/respond/task-list operations;
  visibility list/scan/count/filter APIs; plus archival, dynamic config,
  token serialization, shutdown state and metrics. Coordinate these with the
  frontend-core2 owner rather than duplicate client or persistence code.
- List/scan/count and task-list Go endpoints are catalogued but their handler
  transformations are not ported: visibility query validation, domain
  resolution, filter feature gates, archival configuration/URI checks,
  execution-time normalization, and Matching request wrapping remain gaps.
- Wrapper modules hold policy helpers but are not composed into the live
  transport handler/decorator order. The gRPC status helper is not wired into a
  registered Workflow API gRPC service, and Thrift declared-exception
  selection still needs a real dispatch path.
- Full Go method body behavior, broad Go-mirroring tests, mixed Go/Rust SDK
  compatibility, failure/restart/recovery validation, and a green CI result
  for the current source remain outstanding. Do not mark Frontend complete.

## Follow-up after user's continuation — 2026-10-31

- The branch is `t3/frontend-workflow-api` at the `frontend-core` base with
  `frontend-core2` merged. The remote branch had no worktree changes before
  this continuation.
- CI run
  [41176804275](https://github.com/auenkr/cadence-rust/actions/runs/41176804275)
  tested `4d66d043d471db8584cf47cd3aa6683c6b81ccc6` and concluded `failure`.
  Clippy found stale calls to `RequestValidator::validate_start` and
  `validate_execution` after frontend-core2 changed that API. The handler now
  calls the shared `cadence_api::validation` Go-parity helpers directly.
- `Handler::dispatch` now composes the metered wrapper around rate-limit
  preflight, request validation, and backend invocation. It emits operation
  request/timing metrics and per-request tags, normalizes typed/uncategorized
  errors with Go's counter mapping, and allows a metrics sink to be injected.
  Added tests for validation-error metrics/tags and normalization of an
  uncategorized backend failure. These source changes have not yet been run by
  CI; `git diff --check` is the only local source check.
- Continued the Go handler audit and added missing task-list validation to both
  poll methods, plus required workflow/activity ID validation to heartbeat and
  all three activity respond-by-ID methods. Regression tests mirror Go's
  required-field error order.
- CI run
  [41177269241](https://github.com/auenkr/cadence-rust/actions/runs/41177269241)
  tested `1437d2e5c11a6758b54710cdb4b08bddbd879008` and concluded `failure`.
  Clippy found `validate_request` needed a mutable JSON value for the existing
  Go-compatible page-size default mutation. The parameter is now mutable; the
  latest source still needs an exact-head package run.
- CI run
  [41177546268](https://github.com/auenkr/cadence-rust/actions/runs/41177546268)
  tested `e29a3c2ada2a66d2960d15a3fdddd0abca522073` and concluded `failure`.
  Clippy then reported three needless borrows in Thrift wrapper unit tests;
  those test references were simplified, with another exact-head run required.
- CI formatting run
  [41178031363](https://github.com/auenkr/cadence-rust/actions/runs/41178031363)
  tested `618fda8a3bf5b4f5c840cc69e1b40068973a6a96` with
  `format_frontend_files=true` and concluded `failure` in tests (formatting and
  Clippy passed after the CI rustfmt step). Retrieved its
  `frontend-format-patch` artifact and applied the runner-generated formatting
  patch locally. Two tests failed: history run-ID mismatch used a malformed
  request run UUID (Go validates it first), and the Thrift test assumed a
  WorkflowService `Health` method that does not exist. Fixed the UUID fixture
  and now test the valid `GetSearchAttributes` method's undeclared BadRequest
  exception behavior. A normal exact-head CI run is required.
- Next exact-head CI must be dispatched after pushing the validator API fix and
  metering integration. No green CI result covers the current source.
- Handler bodies still do not execute Go's workflow-engine transformations:
  backend calls are currently generic `WorkflowBackend::invoke` operations.
  The production runtime currently routes through shared `cadence-service-core`
  handlers and does not instantiate this Frontend crate handler. Needed
  interfaces remain domain/cache/config, generated History/Matching typed
  calls, visibility query validation/storage/archive, task-token codec, and
  caller context for version/rate-limit/auth. The frontend-core2 owner owns
  clients and persistence; request those missing interfaces from that owner
  rather than duplicating them. Stateful redirect, authorization, quota,
  transport conversion, mixed-cluster compatibility and SDK recovery tests
  still need live wiring and parity tests.

## Exact-head CI checkpoint — wrappers and validation

- `rust-ci.yml` package run
  [41178373998](https://github.com/auenkr/cadence-rust/actions/runs/41178373998)
  tested head `0e76f49f10ffab8cb22ca0a8d99ebfa5eab86ec3` and concluded
  `success` (Clippy and tests).
- This confirms the current helper policies and validation compile and pass
  their tests. It does not close the Go parity task: the registered Rust
  service still uses the shared `cadence_matching::launcher::run()` path, and
  the Frontend `Handler` is not instantiated by that runtime. The generic
  backend dispatch also does not yet implement the History, Matching,
  visibility, archival, async producer, or domain-cache transformations in
  `go/service/frontend/api/handler.go`.
- Source audit: Go `WorkflowHandler` has 55 methods in `handler.go`; the Rust
  operation catalogue has 40 workflow/worker/visibility/task-list methods,
  omitting health-related signatures outside that list and broader admin/domain
  and schedule surfaces. Generated Go ratelimited wrapper has 57 methods and
  its preflight/quota policy is not attached to a live Rust handler. The Go
  constructor order is access control -> optional cluster redirection ->
  metering -> rate limiting -> version check -> API handler. Rust currently
  meters dispatch and performs validation but has no caller metadata or
  configured authorizer/version/quota/redirector in its live path.
- Remaining required interfaces for the API bodies: domain lookup (name/UUID
  and domain config), shutdown state and dynamic config, visibility query
  validation and visibility manager/archive provider, async request producer
  and ThriftRW encoder, task-token context/serializer, caller headers/identity,
  and typed outbound History/Matching method calls. The latter two client
  wrappers only expose generated gRPC clients; they do not yet provide the
  operation adapters needed by these handler methods.

## Wrapper dispatch integration checkpoint

- Added caller metadata and access-control, rate-limit, and version-check hooks
  to `Handler`. Access control runs outside metering; rate-limit preflight and
  quota policy run before version checking; both run inside metering, matching
  the Go constructor/decorator order for these layers.
- Added `HandlerAuthorization` and `HandlerRateLimit` adapters so the existing
  Go-parity attribute and quota policies can be installed directly on the
  handler. Rate-limit adapter resolves token domain IDs through the injected
  `DomainNameLookup`, preserves sticky task-list quota names, and applies
  worker-poll waiting/bypass semantics.
- Added an outer cluster-redirection hook to `Handler`, a target-cluster backend
  invocation seam, an async forwarding adapter that reuses the existing
  stateful policy, active-active selection and DomainNotActive fallback, and a
  handler integration test. The runtime must supply a `RedirectRequestResolver`
  backed by its domain cache/config/active-cluster manager; that resolver is
  still not implemented in the Frontend production graph.
- Added `grpc::to_status`, which converts the mapped Cadence code and original
  message into one tonic status at the transport boundary.
- CI run
  [41179224019](https://github.com/auenkr/cadence-rust/actions/runs/41179224019)
  is testing head `ad0cd7e6d97903ec192b3cb4a6041a9c6b54daf2`; it was `in_progress`
  at this save. Runs [41179195098](https://github.com/auenkr/cadence-rust/actions/runs/41179195098),
  [41179109690](https://github.com/auenkr/cadence-rust/actions/runs/41179109690),
  [41179068665](https://github.com/auenkr/cadence-rust/actions/runs/41179068665),
  [41178916064](https://github.com/auenkr/cadence-rust/actions/runs/41178916064),
  and [41178803702](https://github.com/auenkr/cadence-rust/actions/runs/41178803702)
  were cancelled when newer same-branch runs superseded them. The latest
  completed success remains run 41178373998 at `0e76f49` and does not cover
  these wrapper changes.
- Still incomplete: all API methods continue through the generic backend
  `invoke` seam, so Go's typed History/Matching/visibility/archival/async
  transformations and responses are not ported. List/scan/count and task-list
  methods currently validate requests but do not resolve domain IDs, validate
  visibility queries, select filter-specific persistence calls, or implement
  archival behavior. Thrift/gRPC helpers are not registered as Frontend
  transports; `run()` still selects the shared Matching launcher. The Frontend
  production runtime does not instantiate this handler or its middleware.

## Handler parity additions — history pagination and visibility query

- Ported GetWorkflowExecutionHistory page-size behavior: non-positive sizes
  use the configured `frontend.historyMaxPageSize` value, and requests above Go's
  fixed `GetHistoryMaxPageSize` (1000) are clamped. The handler builder exposes
  the configured value; domain-filtered dynamic lookup and Go's warning log
  still need runtime config/metrics wiring.
- Added an injected `VisibilityQueryValidator` hook and dispatch integration for
  CountWorkflowExecutions, ScanWorkflowExecutions, and ListWorkflowExecutions.
  The validated query is put back into the outbound request without dropping
  other fields. This separates the query dialect/search-attribute dependency
  from the shared request validator.
- Package CI run
  [41179465854](https://github.com/auenkr/cadence-rust/actions/runs/41179465854)
  is testing source head `5bf5a6df0b331bae11334e0059e55e9ad753e4ab`; it remains
  `in_progress` at the time of this note. This run includes the history
  page-size and visibility-query code. No newer Rust source changes are pushed
  after that head; subsequent work should wait for this run or dispatch a fresh
  package workflow.

## User-directed rebase continuation — 2026-10-31

- Fetched `origin/frontend-core` at `4fe22eea2` and rebased
  `t3/frontend-workflow-api` onto it. Rebase completed without conflicts and
  preserved the existing wrapper/handler commits. Rewritten branch pushed with
  `--force-with-lease`; latest workflow-api head before the shutdown follow-up
  was `2abc9cc3f6295c6a503a353bc910bcb3bcb7797e`.
- Added `Handler::stop` and atomic shutdown gating in the workflow dispatcher,
  mirroring `WorkflowHandler.Stop` and the `validate.ErrShuttingDown` message.
  Health remains callable during drain. A unit test covers rejection and the
  Health exception. Source-only `git diff --check` passed; Cargo was not run on
  this host.
- Follow-up commit `4d433e93aee93112d3f6c26a7609fe73729489fd` was pushed.
  CI run
  [41179864590](https://github.com/auenkr/cadence-rust/actions/runs/41179864590)
  targets this exact head; latest observed state: pending. Older run
  [41179724089](https://github.com/auenkr/cadence-rust/actions/runs/41179724089)
  targets `2abc9cc3f6295c6a503a353bc910bcb3bcb7797e` and was still in progress
  when superseded; it is not evidence for the latest source.
- Remaining parity gaps are unchanged and material: workflow calls still use
  the generic `WorkflowBackend::invoke` seam rather than Go-equivalent typed
  History/Matching/visibility/archival/async calls; list/task-list bodies only
  validate and forward public requests; runtime does not register this handler
  or install its wrappers. Needed interfaces include domain lookup/config,
  typed History and Matching method adapters, visibility persistence/archive
  operations, async ThriftRW producer and request context metadata. The
  parallel `frontend-core2` branch currently has a later tip than the already
  incorporated validator/client changes; do not merge blindly, coordinate if
  API interfaces change.
- This is progress evidence, not a parity completion claim. Continue porting
  service-specific request/response behavior and its Go-mirroring tests, then
  rerun exact-head package CI and record the run URL, head SHA and conclusion.
- Redirection metric review found that Go increments its forwarding-policy
  counter once per API invocation, including a retry after `DomainNotActive`.
  Rust previously emitted one `ForwardRecord` per attempt. The implementation
  now emits one record, carries Go's fallback `active_cluster` tag, preserves
  the initial target tag, and records final success. Added assertions for both
  synchronous and asynchronous retry paths.
- Exact-head package run
  [41180045387](https://github.com/auenkr/cadence-rust/actions/runs/41180045387)
  tested `cfb892d59e77f282c3bf8c59b535b7171cfc1bca` and failed Clippy: the
  redirect closure had an unnecessary `mut`, and the handler redirect test
  expected a plain mutex instead of the shared `Arc<Mutex<_>>`. Both are fixed
  in the current worktree; the next package run must validate those fixes.
- Package run
  [41180338585](https://github.com/auenkr/cadence-rust/actions/runs/41180338585)
  tested `fc44957b70bb1ad2b59b2ee89bacd2dd74c4d0d4` and passed Clippy after
  those fixes, but failed the full Frontend rustfmt check. Go's `HealthStatus`
  string audit also corrected the API response text to `OK`, `WarmingUp`, and
  `ShuttingDown`; that source adjustment is in the current worktree. Use the
  workflow's `format_frontend_files=true` mode to retrieve the canonical patch,
  then rerun normal package CI on the formatted pushed head.
- Formatting run
  [41180617761](https://github.com/auenkr/cadence-rust/actions/runs/41180617761)
  tested `41dabb27448981fe4be686d819d2f3ec8bec08c8`. The runner-generated
  `frontend-format-patch` artifact was applied. Its Clippy and formatting steps
  passed; 54/58 unit tests passed. The four failures were three stale tests
  assuming Health called the backend or returned the unmetered shutdown error,
  plus an invalid run UUID in the redirect test fixture. Those expectations
  are corrected in the current worktree; normal package CI is still required.

## Workflow backend request mapping continuation

- Added an injectable synchronous `WorkflowDomainLookup` seam and applied Go's
  internal request mapping for `DescribeTaskList`, `ListTaskListPartitions`,
  `GetTaskListsByDomain`, `ResetStickyTaskList`, `CountWorkflowExecutions`,
  `ScanWorkflowExecutions`, `ListWorkflowExecutions`, and the open/closed
  visibility list APIs. Visibility requests now carry the resolved UUID,
  normalized page size/token and the validated query or selected filter fields
  expected by the downstream persistence adapter.
- Added Go-mirroring tests for task-list describe and visibility count mapping.
  `git diff --check` passes. No Cargo command was run locally.
- Production must install `WorkflowDomainLookup`; until the Frontend runtime
  graph does so, the bootstrap handler intentionally preserves the public
  request shape. The backend must route these operation names to Matching or
  visibility persistence and decode the Go internal request shapes. The
  archive reader, workflow History request transformations (including task
  tokens, raw/archive history, restart, async ThriftRW publishing), and actual
  service transport registration/runtime remain unimplemented in this slice.
- Package CI run
  [41180945218](https://github.com/auenkr/cadence-rust/actions/runs/41180945218)
  validates `40a3e084f25c1e315569d876db58a3d55ebeb834`; it was still running at
  the time this change was prepared. It predates the new request mapping and
  is not validation evidence for this source. Dispatch formatting and package
  CI on the next pushed head and record its final conclusion.
- Added History envelopes for workflow signal, terminate, reset, cancel,
  query, and describe calls, plus task-token decoding/envelopes for heartbeat,
  activity/decision completion/failure/cancel, and query-task completion.
  Added a Go-mirroring heartbeat token envelope test and corrected Describe's
  public execution field to `execution`. These follow-up edits are not yet
  committed or covered by CI at this save. Activity-by-ID token construction,
  detailed task completion behavior (limits/version normalization/decision
  response reconstruction), polling retry/isolation logic and workflow start
  request construction remain open.

## Follow-up handler implementation — 2026-10-31

- Confirmed `origin/frontend-core` (`4fe22eea2`) is already an ancestor of
  `t3/frontend-workflow-api`; the branch is therefore based on the landed
  skeleton and required no additional rebase commit. `frontend-core2`'s
  request-validator and generated History/Matching clients are present in the
  checked-out skeleton; no client or persistence files were changed here.
- Pushed token-envelope work as `68506a418887cba9994f2a954547124a21e4b034`.
  Package CI run
  [41181623921](https://github.com/auenkr/cadence-rust/actions/runs/41181623921)
  targets that exact SHA; it was still `in_progress` at the latest check.
- Continued operation-specific request mapping for Go's `StartWorkflowExecution`,
  `SignalWithStartWorkflowExecution`, and `RefreshWorkflowTasks` calls. Added
  Go-mirroring tests for the first two request envelopes. Implemented local
  `GetClusterInfo` using Go SDK compatibility constants (`1.7.0`/`1.5.0`),
  including its Go behavior of remaining available during shutdown, with a
  corresponding test. `git diff --check` passed; no Cargo command ran locally.
- These edits remain uncommitted and are not covered by CI yet. Workflow
  implementation remains incomplete: generic `WorkflowBackend::invoke` is
  still a test/bootstrap seam rather than concrete History/Matching/visibility
  routing. Required missing interfaces are (1) History API operations and
  multi-call history pagination/raw/archive logic, (2) Matching poll and
  query-completion calls with poll IDs, isolation, retries and response
  conversion, (3) visibility operations with dynamic filter policy and
  persistence response mapping, (4) domain config/cache metadata for size,
  identity and archival checks, (5) async ThriftRW publisher and caller header
  propagation, (6) search-attribute registry and cluster membership/version
  metadata. In particular, Restart, Diagnose, both async start calls, search
  attributes, poll completion reconstruction, archived history/listing, and
  list filters still need their Go method bodies. The wrapper policies are
  implemented as independent helpers and are partly wired in `Handler`, but
  transport-specific Thrift/gRPC conversion and runtime production wiring are
  still not proven.
- Final result for run 41181623921: `failure` at Clippy on
  `68506a418887cba9994f2a954547124a21e4b034`; it flagged identical branches in
  a workflow execution-field selection. The condition was simplified. The
  run did not reach Rust tests and does not validate the uncommitted follow-up.
- Corrected the RefreshWorkflowTasks internal request to preserve Go's
  historical `DomainUIID` JSON spelling and its nested `request` field, based
  on `go/common/types/history.go`. This is included in the pending next CI
  checkpoint.
- Added a live search-attribute provider seam and local GetSearchAttributes
  response construction from Go's `frontend.validSearchAttributes` behavior,
  plus a unit test. Production config must inject this provider; absent one,
  the bootstrap/test backend path remains available.
- Added a Go-envelope test matrix for Signal, Terminate, Reset, Cancel and
  Query History operations. These changes were pushed at
  `7b742b5fc554a4ad3457b85b196b215256392b91`. CI run
  [41181996079](https://github.com/auenkr/cadence-rust/actions/runs/41181996079)
  is in progress against that exact source SHA; no conclusion is available
  yet. It includes formatting patch mode.
- Run 41181996079 finished `failure`: Clippy and formatting passed; tests
  reported 66 passed and two test-fixture assertions failed. The Describe
  fixture's run ID was not a UUID accepted by the Go-equivalent validator, and
  the search-attribute enum's Rust JSON encoding is numeric (`1` for Keyword),
  not its Thrift text name. Both assertions were corrected locally; they still
  need a pushed exact-head CI rerun.
- Superseded run [41181861730](https://github.com/auenkr/cadence-rust/actions/runs/41181861730)
  for `c0fd8529700a44c5f8eafb690325ea89a8f65c7f` was cancelled after the next
  code checkpoint was pushed, so it is not validation evidence.

## Workflow implementation continuation — 2026-10-31

- The current `t3/frontend-workflow-api` HEAD before this continuation was
  `2fb3fa9269f8217c9b084e0382805786e35f5526`, already based on
  `origin/frontend-core`; `origin/frontend-core2` is at
  `def795959ee2b7771c34644a2c6a227fef2748ad`. The branch therefore required no
  additional rebase. No frontend-core2 client/persistence source was modified.
- Exact-head CI run
  [41182354458](https://github.com/auenkr/cadence-rust/actions/runs/41182354458)
  checked `2fb3fa9269f8217c9b084e0382805786e35f5526` and concluded `failure` at
  Frontend rustfmt. Formatting is being requested through the workflow's
  `format_frontend_files=true` mode; no Cargo/rustfmt command was run locally.
  Formatting CI run
  [41182558949](https://github.com/auenkr/cadence-rust/actions/runs/41182558949)
  was superseded and cancelled when the follow-up commit was pushed.
- Added `VisibilityFilterPolicy` to the workflow handler so filtered open and
  closed visibility requests can consult Go's live
  `DisableListVisibilityByFilter(domain)` setting after request validation and
  before backend dispatch. A denied filtered request returns Go's exact
  `BadRequestError` text, `No permission to do this operation.` New tests cover
  the denial and the unfiltered path. `git diff --check` passed; these tests
  require the next package CI run.
- This continuation is incomplete. The Generic `WorkflowBackend::invoke`
  remains only an operation seam and no production implementation is installed
  by the shared launcher. Stateful wrappers and operation-specific JSON
  shaping do not by themselves establish production Thrift/gRPC dispatch or
  all method parity. Archive reads, history pagination/raw/archive retrieval,
  poll/respond transformation, restart/diagnose behavior, visibility result
  routing/filter configuration, and async ThriftRW publishing still need the
  corresponding injected service interfaces and implementations. No current
  source has a green exact-head CI result yet.
- Formatting/Clippy CI run
  [41182701977](https://github.com/auenkr/cadence-rust/actions/runs/41182701977)
  tested exact head `f2e7317685d9aac6e39c5c6ad925b9f738823627`: rustfmt patch
  generation, Clippy, and formatting passed; tests failed 68 passed / 2 failed.
  Both new list-gate tests used lower-camel `startTimeFilter`, but the pinned
  Thrift-compatible Rust request deserializer names the field `StartTimeFilter`,
  so request validation correctly rejected the fixtures before reaching the
  policy. The fixtures now use the Go JSON field spelling. The same follow-up
  adds an injectable per-domain `HistoryPageSizePolicy`, matching Go's dynamic
  `HistoryMaxPageSize(domain)` lookup, with a regression test; these corrections
  were included in the next CI checkpoint.
- Formatting/package CI run
  [41183050866](https://github.com/auenkr/cadence-rust/actions/runs/41183050866)
  tested `7ded9c1ebeaad2175091e6bc9e8f666e4427e0b2` and concluded `success`.
  Clippy, format check and all Frontend unit tests passed. The runner patch only
  made rustfmt changes; it was applied locally and is being pushed as a separate
  formatting checkpoint, followed by normal exact-head package CI.
- Normal package CI run
  [41183294319](https://github.com/auenkr/cadence-rust/actions/runs/41183294319)
  targeted exact head `430259a93046d7f2d1dbb915597200fb35daf534`, but was
  cancelled after the next code checkpoint was pushed while its Go raw-byte
  oracle step was running. It has no conclusion and is not validation evidence.
- Continued the Go `QueryWorkflow` body audit. Added an injectable dynamic
  `WorkflowQueryPolicy` and Go's exact `DisallowQuery` error. It runs after
  domain/execution validation and before checking the Query payload, preserving
  Go validation precedence; the regression test checks this ordering. The
  regression test still needs format/package CI.
- Pushed query-policy implementation and test at
  `19156796c49a117e828af8433b7ddb2a9299edea`. Formatting/package CI run
  [41183386357](https://github.com/auenkr/cadence-rust/actions/runs/41183386357)
  tested this exact SHA and concluded `success`; runner formatting changes were
  applied locally and pushed in `bb9a33c0f0e49a0c8dcc205db02d6251a826e6d9`;
  normal package CI is recorded below.
- Normal package CI run
  [41183655544](https://github.com/auenkr/cadence-rust/actions/runs/41183655544)
  validated formatted exact head `bb9a33c0f0e49a0c8dcc205db02d6251a826e6d9`
  with conclusion `success` (Clippy, formatting, and Frontend unit tests).
  This confirms the dynamic list-filter/query policies and page-size behavior
  compile and pass their tests, but does not establish full Frontend parity.
- `origin/frontend-core2` advanced beyond the validator/client commit already
  incorporated in this branch. Its later commits currently concern lifecycle
  drain and configured persistence bootstrap. They were not merged into this
  workflow slice to avoid overlapping the worker-owned lifecycle/persistence
  work; re-audit its final public interfaces before integration.

## Restart workflow and isolation continuation — 2026-10-31

- Reconfirmed `origin/frontend-core` is an ancestor of
  `t3/frontend-workflow-api`; `git rebase origin/frontend-core` completed as
  an up-to-date no-op after temporarily stashing the restart edit.
- Committed the initial `RestartWorkflowExecution` history/start mapping at
  `8928642d7e8bccd2de245348413be9546c4f08bc` and pushed it. CI run
  [41184196087](https://github.com/auenkr/cadence-rust/actions/runs/41184196087)
  tested that exact head and concluded `success` (Clippy, formatting, tests).
  This run does not cover the following uncommitted isolation-group change.
- Added an `IsolationGroupState` injection point and Go's drained-group
  `BadRequestError` for Start, async Start, Signal, SignalWithStart, async
  SignalWithStart, and Restart operations, with a Go-mirroring Restart test.
  State lookup errors fail open, as Go does. The implementation still needs
  the runtime config/state provider and request-context isolation group value.
  Go's poll methods use `waitUntilIsolationGroupHealthy`; that timeout/wait
  behavior is not implemented here.
- Current remote `origin/frontend-core2` has advanced with lifecycle work and
  overlapping edits to the handler source; it was inspected but not merged.
  Its owner remains responsible for persistence/client/lifecycle wiring.

### Remaining parity and interfaces

The workflow handler still routes most operation bodies through the generic
`WorkflowBackend::invoke` seam. That seam is useful for operation-specific
request mapping tests but is not a production History/Matching/visibility
adapter. Missing behavior includes History pagination/raw/archive retrieval,
archived visibility, Diagnose, poll response shaping and isolation waits,
respond-task reconstruction, async ThriftRW encoding/publishing with caller
headers, and complete response/error normalization. Runtime wiring must provide
an outbound backend (or typed clients for History, Matching and visibility),
a domain cache/config and isolation-state provider, an async ThriftRW producer,
archival reader/config, and task-token/poll-ID services. No mixed Go/Rust SDK
compatibility or recovery gate has been run for this branch. Do not report
Frontend parity until these are implemented and validated.

## Isolation-group policy checkpoint — 2026-10-31

- Added Go's fail-closed-on-drained / fail-open-on-lookup-error behavior for
  Start, async Start, Signal, SignalWithStart, async SignalWithStart, and
  Restart. Request metadata now carries the isolation group, and the runtime
  must inject the dynamic enable/state provider. Poll isolation waiting remains
  open.
- Formatting/package CI run
  [41184510610](https://github.com/auenkr/cadence-rust/actions/runs/41184510610)
  tested exact HEAD `f7e74a256cf1e2e349f3f8cda93668355c45909e` and concluded
  `success` (Clippy, format, tests). A later async ThriftRW implementation is
  not included in that run.

## Async queue and Diagnose continuations

- Added Go-compatible async start/signal-with-start ThriftRW request-envelope
  encoding, queue metadata (`request_type` 0/1, `ThriftRW`, workflow partition
  key, original headers), an injectable publisher interface, and tests that
  decode the queued start payload. Production must implement producer lookup,
  publishing, queue metrics and transport-header extraction through that
  interface.
- Initial async checkpoint `d836011f814514c7fa6f2a48d87d1360cef0e1b7` failed
  CI run [41184753862](https://github.com/auenkr/cadence-rust/actions/runs/41184753862)
  at Clippy with two borrow-lifetime errors while extracting domain/partition
  strings. The strings have since been cloned before the request drops; this
  repair and Diagnose work remain unvalidated.
- Started porting `DiagnoseWorkflowExecution`: construct the same
  `cadence-system` start request, JSON-marshaled diagnostic input field names,
  24h/5m timeouts and AllowDuplicate policy, and treat
  WorkflowExecutionAlreadyStarted as a successful response carrying its run
  ID. A Go-mirroring request test is pending CI.
- Run [41185052893](https://github.com/auenkr/cadence-rust/actions/runs/41185052893)
  tested exact SHA `bb4247fe61d64c864a641244386a8a62eccce657`: Clippy and
  formatting passed; 76/78 tests passed. The async publisher fixture lacked
  Go-required `requestId`, and one Diagnose expectation had a mistyped run ID.
  Both fixtures are corrected; CI rerun is pending. Diagnose now also checks
  the system domain's isolation state before its internal Start call.
- CI run [41185389191](https://github.com/auenkr/cadence-rust/actions/runs/41185389191)
  tested exact SHA `3165f28eff7ccf0b8484fbb7d5ef9c2b5d697ef4`: Clippy and
  formatting passed; 76/78 tests passed. The async fixture needed valid
  start/decision timeouts, and the Cadence Thrift JSON spelling is
  `workflowIdReusePolicy` (lowercase `d`), correcting both Diagnose and
  Restart. These fixes await CI.
- Package CI run [41185708448](https://github.com/auenkr/cadence-rust/actions/runs/41185708448)
  validated exact SHA `a1d170efd58e84cecb5ee15e2b6b38c1c968e748` and concluded
  `success` (Clippy, format, tests). It validated async request IDs/timeouts,
  Diagnose and Restart's `workflowIdReusePolicy` JSON spelling. The subsequent
  Diagnose field-order fix (Go serializes `Domain`, `Identity`, `WorkflowID`,
  `RunID` in struct order) and duplicate-start regression test need another CI
  run.
- Package CI run [41185964550](https://github.com/auenkr/cadence-rust/actions/runs/41185964550)
  validated exact HEAD `91a9a1a8268f6f42cc775454ffcc6f2e98710183` and concluded
  `success` (Clippy, formatting, all 80 Frontend unit tests). It covers async
  queue encoding/publisher dispatch, Diagnose field order and duplicate-start
  handling, Restart's corrected JSON field, and isolation group checks.

## Diagnose exactness and query payload checks — 2026-10-31

- Added an injectable per-domain workflow-query blob-size limit. Query args are
  decoded as Thrift byte payloads, domain lookup runs first, and oversized
  requests return Go's `Blob data size exceeds limit.` error before dispatch.
  The new regression test is awaiting CI.
- Diagnose input now serializes a struct in Go's field order, and the duplicate
  `WorkflowExecutionAlreadyStarted` case returns the existing diagnostic run.
- Run [41185964550](https://github.com/auenkr/cadence-rust/actions/runs/41185964550)
  completed `success` for exact SHA `91a9a1a8268f6f42cc775454ffcc6f2e98710183`
  (Clippy, format, 81 tests). This is prior to the new query-size policy and
  duplicate-start regression; the next run must validate them.
- Query payload policy CI run
  [41186429646](https://github.com/auenkr/cadence-rust/actions/runs/41186429646)
  tested exact SHA `3fa99c9a55666922bf884783c8dd57cb10f4668d` and concluded
  `success` (Clippy, format, tests). It includes the per-domain query argument
  limit and exact BadRequest test. It does not validate a production visibility
  or History backend.

## Workflow continuation — 2026-10-31 (blob limits and visibility mapping)

- Reconfirmed `origin/frontend-core` is an ancestor of this branch: `git
  rev-list --left-right --count origin/frontend-core...HEAD` returned `0 69`
  before these incremental commits. No core commit was dropped.
- Added Go event-blob limit accounting for the Go call sites implemented here:
  Query args, Signal input, synchronous Start input plus memo map, and each of
  the two synchronous SignalWithStart payloads. Memo size uses Go's exact map
  overhead constant (48 bytes). Removed speculative checks for async queue
  methods and Terminate after source comparison showed those Go methods do not
  call `CheckEventBlobSizeLimit`.
- Added Go-mirroring tests for Signal rejection before dispatch, memo overhead,
  and filtered open-visibility mapping to the persistence request fields.
- The initial expected memo total was wrong (test output: actual 55, expected
  57); corrected the fixture. Exact-head CI run
  [41187401382](https://github.com/auenkr/cadence-rust/actions/runs/41187401382)
  tested `d78e835134d307733706b480f21b82821cdb896d` and concluded `success`
  (Clippy, formatting, and all 83 Frontend unit tests). Prior run
  [41187035561](https://github.com/auenkr/cadence-rust/actions/runs/41187035561)
  tested `87bb3d148278e51d7003eeed7b3eec4ead01d857` and failed only that
  incorrect expectation (82 passed, 1 failed). No Cargo was run locally.

### Still open; this branch is not complete Frontend parity

- `Handler` still delegates to generic `WorkflowBackend::invoke`; the shared
  launcher does not instantiate this handler as the production Workflow API.
  The existing History and Matching modules are generated gRPC client shells,
  not method adapters.
- `GetWorkflowExecutionHistory` still lacks Go's PollMutableState/page loop,
  persistence/raw-history retrieval, long-poll behavior, transient decisions,
  close-event filtering, archive selection/read, and exact continuation-token
  reconstruction. `ListArchivedWorkflowExecutions` still lacks archival config,
  URI/provider lookup and execution-time normalization. Visibility response
  conversion remains backend-dependent.
- Poll/respond methods still need Go's task-token/poll-ID reconstruction,
  matching long-poll cancellation and isolation-group wait behavior, response
  shaping, and oversized worker payload failure-to-activity conversion.
  Workflow blob checking currently implements error thresholds but not Go's
  warning thresholds, event size timer/histogram, violation tags, or operation
  metrics.
- Runtime integration must provide typed History calls (including mutable
  state, history pages, start/signal/cancel/terminate/reset/query/respond/
  heartbeat/sticky reset), Matching poll/respond/task-list calls, visibility
  list/scan/count calls, archived history/visibility readers, domain cache and
  config, search-attribute validation, task-token serializer, caller transport
  metadata, poll isolation waits, and event-blob metrics. `frontend-core2`
  owns History/Matching clients and persistence wiring; these adapters should
  be added against that worker's final interfaces, without duplicating those
  owned files.
- Wrapper policy modules and decorator hooks have unit tests, but the shared
  runtime does not compose them into the actual registered Thrift/gRPC
  WorkflowService. Mixed Go/Rust SDK, failure, restart and recovery validation
  has not run. The green package CI is compile/unit evidence only.

## Follow-up — blob warning observation and poll envelopes

- Rechecked `origin/frontend-core2` at `32de07ee3`; its only commit since the
  merge is a memory/documentation update. The merged History/Matching client
  shells remain the current shared interface. No interface or persistence
  source change was available to merge at this checkpoint.
- Merged-head CI run
  [41189188057](https://github.com/auenkr/cadence-rust/actions/runs/41189188057)
  concluded `success` for exact source SHA
  `cb6d9b07b284f3e2a042394f368788661b310e1b`.
- Added `WorkflowBlobSizeMeasurement` observer dimensions and warning/error
  outcomes around the Go-equivalent payload size checks. Commit
  `322010a62920f31e48881061f4042baa07962a47` is pushed; package CI
  [41189493958](https://github.com/auenkr/cadence-rust/actions/runs/41189493958)
  was cancelled when a newer source head was dispatched; it has no conclusion
  and is not validation evidence.
- Poll activity/decision now prepares the Go Matching envelope with resolved
  `domainUUID`, unique UUID `pollerID`, nested public `pollRequest`, and the
  caller isolation group. A regression test covers those fields and fresh IDs.
  This change is committed and pushed as
  `9a42e581bbbcf6dd66f825bb158be49d615cb1f3`. Package CI
  [41189699286](https://github.com/auenkr/cadence-rust/actions/runs/41189699286)
  was still `in_progress` at this save and will be superseded by the typed
  Matching client request methods described below.
- Added typed Matching client calls for both long-poll RPCs and explicit
  `CancelOutstandingPoll`, with builders for Go's internal request envelopes
  and a constructor unit test. These edits are uncommitted and unvalidated.
- Remaining critical gaps are unchanged: generated History/Matching client
  shells are not operation adapters; the production launcher does not register
  `Handler` or compose its wrappers; poll cancellation/health-wait and response
  shaping, history/archive paging, archived visibility, and typed persistence
  calls remain absent. No Frontend + Go History + Rust Matching SDK gate has
  been added or run. Current package CI validates only the handler's unit-test
  backend and is not mixed-service proof.
