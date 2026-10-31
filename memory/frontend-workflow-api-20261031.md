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
