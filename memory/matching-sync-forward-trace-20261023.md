# Matching sync-offer forward trace (2026-10-23)

Question (from `memory/evidence/matching-forward-source-domain-20261023/notes.md`): does the
Rust Add path forward a fresh task to the parent partition when the child has no local poller,
the way Go's `matcher.Offer` -> `forwarder.ForwardTask` does? If so, with what source domain?

## Answer

Yes. The notes' "Open" item is wrong: Rust has a call site. It is `sync_match_chain`
(`grpc_matching.rs:1358`), called from `add_decision_task` (`grpc_matching.rs:2200`) and
`add_activity_task` (`grpc_matching.rs:2415`). The activity SourceDomainUUID case is reachable from
RPC. The earlier "no call site" conclusion came from looking only at `forward_backlog_task`, which
serves the backlog reader path.

Field-by-field, the Rust remote sync forward equals Go `forwarder.go:139-168`. Rate limiting and
the parent-busy handling also match in control flow. One metric-only gap remains (below). No
program logic change was needed. A unit test pins the request fields.

## Go reference

- `go/service/matching/tasklist/matcher.go:150` `Offer`. No local poller, then
  `case token := <-tm.fwdrAddReqTokenC()` at `:219`, `tm.fwdr.ForwardTask(ctx, task)` at `:222`.
  Success returns `true` (`:224`). `ErrForwarderSlowDown` increments
  `SyncMatchForwardTaskThrottleErrorPerTasklist` (`:230-231`). Any error returns `false`, so the
  caller persists the task.
- `go/service/matching/tasklist/forwarder.go:117` `ForwardTask`:
  sticky kind -> `ErrTaskListKind` (`:119`); no parent -> `ErrNoParent` (`:124`);
  `limiter.Allow()` false -> `ErrForwarderSlowDown` (`:127`).
- `forwarder.go:139-153` decision arm: `DomainUUID = task.Event.DomainID` (`:142`),
  `Source = &task.source` (`:150`), `ForwardedFrom = taskListID.GetName()` (`:151`),
  `PartitionConfig` (`:152`), `Kind = fwdr.taskListKind`, `TaskList.Name = parent`.
- `forwarder.go:154-168` activity arm: `DomainUUID = taskListID.GetDomainID()` (`:156`),
  `SourceDomainUUID = task.Event.DomainID` (`:157`), `Source` (`:165`), `ForwardedFrom` (`:166`),
  `PartitionConfig` (`:167`).
- `forwarder.go:298` `handleErr`: only `ServiceBusyError` maps to `ErrForwarderSlowDown`.
- `go/service/matching/tasklist/task_list_manager.go:849`
  `newInternalTask(params.TaskInfo, nil, params.Source, params.ForwardedFrom, true, ...)`.
- `go/service/matching/handler/engine.go:644` AddActivityTask:
  `TaskInfo.DomainID = request.GetSourceDomainUUID()`, so `task.Event.DomainID` is the workflow
  (source) domain. `engine.go:568` AddDecisionTask: `DomainID = domainID = request.GetDomainUUID()`.

## Rust trace

- `grpc_matching.rs:2063` `add_decision_task`. Source domain is `r.domain_id` (`:2142`). Sync call
  `sync_match_chain(ForwardedAdd{ domain_id: &r.domain_id, source, backlog: r.source == 2,
  task_source: r.source, ... })` at `:2200`.
- `grpc_matching.rs:2282` `add_activity_task`. Source domain is `r.source_domain_id` parsed at
  `:2362-2364`. Sync call at `:2415-2427`, with `domain_id: &r.domain_id` (list domain) and
  `source` (task's own domain).
- `grpc_matching.rs:1358-1587` `sync_match_chain`:
  - Entry partition tries local pollers first (`:1413-1424`). Ancestors and forwarded adds use
    `try_sync_match_forwarded`.
  - Parent name via `name.parent(degree)` (`:1466`). Sticky (`list.kind == 2`) or root
    -> `None` (`:1456-1465`).
  - Forward permit: `try_forward_task_observed` (`:1476`). `NoToken` -> `None`. `RateLimited`
    -> `sync_forward_throttled` metric (`:1486`) then `None`.
  - `forwarded_from = list.name` (`:1495`) before `list.name = parent` (`:1496`).
  - Remote decision request (`:1516-1526`): `domain_id: source` (`:1518`), `source: task_source`,
    `forwarded_from`, `partition_config`, `schedule_to_start_timeout`.
  - Remote activity request (`:1534-1545`): `domain_id: domain_id` (list domain, `:1535`),
    `source_domain_id: source` (`:1536`), `forwarded_from` (`:1542`), `source`, `partition_config`.
  - Result: `.ok()` (`:1559-1561`). An error means not matched, so the caller enqueues locally
    (or fails if forwarded), matching Go's `return false`.
- `matching_registry.rs:313` `try_forward_task_observed`: token first, then rate, like Go's
  `ForwardTask` order (`Allow` only after the token).
- `matching_forward.rs:20` `ForwardError::classify` maps ServiceBusy to SlowDown. It is not used on
  the sync path.

## Field comparison (sync-offer remote forward)

| Field | Go | Rust | Match |
|---|---|---|---|
| decision DomainUUID | task.Event.DomainID (workflow domain) | `source` = `r.domain_id` | yes |
| activity DomainUUID | taskListID domain | `domain_id` = `r.domain_id` (list) | yes |
| activity SourceDomainUUID | task.Event.DomainID (`engine.go:644`) | `source` = `r.source_domain_id` | yes |
| TaskList name | parent | `parent` | yes |
| TaskList kind | fwdr.taskListKind | request list kind (same kind) | yes |
| ForwardedFrom | taskListID.GetName() | `forwarded_from` (child name) | yes |
| Source | task.source | `task_source` = `r.source` | yes |
| ScheduleID / ScheduleToStart | task.Event | `r.schedule_id`, `r.schedule_to_start_timeout` | yes |
| PartitionConfig | task.Event.PartitionConfig | `data.partition_config` | yes |
| sticky / no parent | ErrTaskListKind / ErrNoParent | `None` | yes (no token consumed) |
| forward rate | `limiter.Allow` -> SlowDown | `ForwardRate::allow` -> RateLimited | yes |
| forward token | `fwdrAddReqTokenC` | `forward_tasks.try_acquire` | yes |
| parent ServiceBusy | counter `SyncMatchForwardTaskThrottleErrorPerTasklist` (`matcher.go:230`) | not counted (`result.ok()`) | **metric gap** |

## Gap left open

- Metric only: a parent `ServiceBusy` during a sync forward increments the Go counter
  (`matcher.go:230-231`), but the Rust sync path does not (`grpc_matching.rs:1559-1561`). Control
  flow is identical (both fall back). Fixing it means classifying the forward result with
  `ForwardError::classify` and calling `sync_forward_throttled`, plus a metric-observing test.
  Not done here.

## Test added

- `rust/crates/cadence-matching/src/matching_rpc_tests.rs`:
  `matching_rpc_sync_offer_forward_sends_go_forwarder_fields_to_parent`, with a
  `RecordingAddParent` gRPC server. The child's membership maps the root `queue` to the recording
  parent. The child has no poller, so each Add RPC takes the remote sync forward. The test asserts
  the decision and activity fields above and that the child persists nothing. The activity case
  uses a second domain as the task's own domain, so the DomainUUID / SourceDomainUUID split is
  observable.

## Evidence

Run from `/mnt/storage/service/github/wt/syncfwd/rust`, `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared`, via `cargo-safe`.

- Baseline (before test edit), `/tmp/sf-base.log`, exit 0: `442 passed; 0 failed; 2 ignored`.
- Final: see the results section below.
