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
(see below; updated per run)

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
