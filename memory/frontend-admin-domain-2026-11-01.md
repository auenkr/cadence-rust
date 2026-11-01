# Frontend Admin/Domain/Schedule continuation — 2026-11-01

Branch `fe-admin-domain` (from `frontend-domain-admin` @ c39bcd19a). Continues
`frontend-domain-admin-2026-10-31.md`. Go Worker is used (not ported), so the
Admin/Domain/Schedule APIs it calls must work against the Rust Frontend.

## Findings
- The wire-level Admin surface (44 gRPC methods in `cadence-matching/src/grpc_admin.rs`,
  JSON handlers in `cadence-service-core/src/admin.rs`) already exists for the
  single-host monolith. The Thrift table (`cadence-thrift/src/service.rs`) only
  has WorkflowService/MatchingService/Meta; there is no Thrift AdminService.
- `cadence-frontend` Admin was primitives only (dynamic config, search attrs,
  describe cluster). Go's History-proxy methods were missing.

## Slices
1. `handler_admin_history.rs` + History client methods: CloseShard, RemoveTask,
   ResetQueue, DescribeQueue, DescribeHistoryHost, ReapplyEvents (Go validation
   order), GetReplicationMessages, GetDLQReplicationMessages, Count/Read/Purge/
   Merge DLQ delegation. Unit tests for validation.
2. `scheduler_wire.rs`: Go-`encoding/json`-compatible payloads for the scheduler
   workflow (input, update/pause/unpause/backfill signals, describe query decode,
   error normalisation). Rust API types use nanos ints and numeric enums; Go uses
   RFC3339, ns Durations and string enums, so these are converted explicitly.

## CI evidence
- Run https://github.com/auenkr/cadence-rust/actions/runs/41314536592 (package=cadence-frontend), head 17cd77b68, conclusion: success (format, clippy, tests).
- Earlier runs 41313528769 (fmt), 41313759228 (clippy result_large_err), 41314148334 (fmt) failed and were fixed.

## Remaining gaps
- Wire the History-proxy methods and scheduler_wire into the live transport
  (Thrift/gRPC handlers still use the local schedule store in service-core).
- Schedule handlers: start/signal/query via WorkflowHandler (owned by
  `fe-workflow-service`), DescribeSchedule retry loop (5 attempts, 1s),
  DeleteSchedule signal→terminate fallback, ListSchedules via visibility.
- Admin: DLQ domain/replication switch, CountDLQ merge with domain DLQ,
  isolation groups, async-workflow config, real membership in DescribeCluster,
  search-attribute persistence.
- Domain gaps from the 2026-10-31 handoff: read-through on cache miss,
  Cassandra audit rows, FailoverInfo in DescribeDomainResponse, create/update/
  deprecate/delete audit rows, domain replication queue.

## Domain + Admin(describe-host) + Schedule slices (branch fe-admin-domain)

CI (full workspace clippy + tests): https://github.com/auenkr/cadence-rust/actions/runs/41322656303
head 0826c242681a80a3e87f94b245280b9f03def636 — conclusion: success.

### Domain (done)
- Audit rows for create/update/deprecate/delete (+ existing failover), best effort like Go `updateDomainAuditLog`.
  Left ungated by `EnableDomainAuditLogging` (Rust failover audit was already ungated).
- `DescribeDomainResponse.failoverInfo` (thrift field 60) from `failover_end_time`.
- SQL `DomainMetadataPersistence` now delegates get/list so a cold cache reads Go rows (fixed
  previously failing `sqlite_domain_manager_survives_cache_restart`).
- Remaining: audit identity/identity_type from request context.

### Admin
- `DescribeHistoryHost` validates selector and delegates to History (single-host fallback).
- Still placeholders in service-core admin.rs: get_replication_messages, get_dlq_replication_messages,
  cross-cluster tasks, describe_shard_distribution (single host), replication-type DLQ read/purge/merge
  (needs History replication-task proto conversion), ReapplyEvents domain lookup, DescribeCluster real
  membership, search-attribute persistence, isolation groups/async config persistence.
- Pre-existing stale tests on the base branch were fixed (config store-name tests, admin e2e
  expectations for History delegation without a History client).

### Schedule (done)
- `scheduler_wire` moved to cadence-service-core; new `schedule_flow.rs`.
- create → start `cadence-scheduler:<id>` workflow (AllowDuplicate, 10y exec timeout, 10s decision);
  AlreadyStarted → BadRequest "schedule %q already exists in domain %q".
- update/pause/unpause/backfill → signal with Go JSON payloads, EntityNotExists normalized.
- delete → signal, already-gone = success, transient (ServiceBusy/Internal) returned, else terminate fallback.
- describe → probe + query (NotCompletedCleanly), 5x1s retry on ContinuedAsNew / "decision task".
- list → open scheduler workflows, prefix stripped, internal search attrs filtered, default page 10.
- Not yet validated against a real Go Worker scheduler workflow (mixed-cluster gate pending).

## Admin transport slice (YARPC AdminService)
- cadence-thrift method table now includes all 37 `AdminService` procedures (generator updated); `Rpc-Procedure: AdminService::X` keeps its qualifier so admin `DescribeWorkflowExecution` is not shadowed by WorkflowService.
- New YARPC arms bridging to `crate::admin::*`: CloseShard, RemoveTask, ReapplyEvents, MaintainCorruptWorkflow, Get/Update Global/Domain IsolationGroups, Get/Update DomainAsyncWorkflowConfiguraton, admin DescribeWorkflowExecution, DescribeHistoryHost, DescribeShardDistribution (+ Thrift converters). Declared-exception mapping via `bridge_response`.
- CI: run https://github.com/auenkr/cadence-rust/actions/runs/41324390824, head 8672abd81, conclusion success (full workspace).
- Remaining gaps: Thrift converters/arms for GetReplicationMessages, GetDLQReplicationMessages, ResendReplicationTasks, GetCrossClusterTasks, RespondCrossClusterTasksCompleted, CountDLQMessages (not in the pinned admin.thrift subset used by SDK/Worker); replication-type DLQ read/purge/merge still need History proxy + proto<->API replication task conversion; DescribeCluster real membership; search-attribute persistence; audit identity from request context; no mixed Go-Worker validation yet.
