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
