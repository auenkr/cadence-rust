# Service-by-service migration (2026-10-15)

User requires completing one of the four Go service roles, validating it with
remaining Go services, then proceeding to the next. This supersedes the earlier
feature-by-feature cross-service migration order. All four roles remain incomplete.

Order: Matching -> History -> Frontend -> Worker (system/background).
Shared infrastructure is implemented as required by the active service. Generated
RPC services are not equivalent to these four deployment roles.

## Active: Matching

Source: go/service/matching (handler, tasklist, poller, config, semaphore).
Default Rust grpc_matching delegates polls into the embedded frontend/execution
engine and AddTask looks up local workflow state. An opt-in durable queue backend
now routes AddTask/poll protobuf RPCs independently of local workflow state. Go Matching accepts queued
references and calls History.Record*TaskStarted on delivery. Consequently current
Rust Matching is not yet a complete replacement. The basic mixed-service SDK
probe now passes with no Go Matching process (see matching-rpc-next.md).

Independent components now verified: Go-format SQLite store, durable queue core,
Go History client, cancellation-safe delivery worker and basic queue registry.
Both RPC/History boundary and actual Go Frontend/History/Worker routing probes
now exercise the Rust Matching listener. The latter includes the existing SDK.
Native owners now have a background reader, bounded local buffer, cancellation-safe
local backlog handoff and reader-loop shutdown joins (matching-background-next.md).
These basic workflows do not satisfy all replacement gates below.
Source-domain SQL writes are now checked against the original Go TaskStore.

Completion gates:

1. Exact task-list identity, partition configuration and version/cache semantics.
2. Matching-owned durable task storage: leases/range IDs, ack/read levels,
   append/read/delete, expiry and restart recovery using Go persistence schemas.
3. Sync/async matching, delivery to Go History over RPC, retry/error classification,
   task token generation, cancellation/deadlines and task acknowledgement.
4. Query delivery, poller history, sticky availability, rate limits, forwarding,
   partition routing/scaling, isolation groups, semaphores.
5. Membership/ownership and standalone Matching lifecycle, including transport,
   health and graceful shutdown. No frontend/history engine required in-process.
6. Mixed-service harness: Go Frontend + Go History + Go Worker with Rust Matching;
   existing SDK workflows, activities, queries, cancellations, retries/timeouts,
   sticky and partitioned lists, concurrent pollers, crash/restart and failover.
   Compare expected histories/errors and replay against all-Go baseline.

No gate is complete solely because a listener or adapter exists. Original-Go
function differential fixtures complement mixed-service tests; they do not replace
mixed-service integration. The full compatibility runner now includes a real
mixed Go Frontend/History/Worker + Rust Matching stage with seven SDK scenarios.
Its discovery override and seeded domain metadata are test-only.

Gate-item progress recorded in memory (all validated by the full 29-stage runner
unless noted):

- gate 1: write-partition count from `matching.numTasklistWritePartitions`
  (matching-partitions-next.md).
- gate 4: rate limits (matching-rate-next.md), durable query delivery with a
  mixed `queryDispatch` scenario (matching-query-next.md), sticky availability
  fail-fast (matching-sticky-next.md), arrival QPS tracker and
  `DescribeTaskList.newTasksPerSecond` (matching-qps-next.md).
- gate 1 config: taskListName/taskType scoping now supported for write-partition
  counts and QPS windows (matching-config-next.md).
- gate 4 observations: durable polls return LoadBalancerHints; Describe returns
  configured isolation-group QPS and poller counts, and polls retain group
  attribution (matching-isolation-next.md). Latest full runner:29 stages and395
  workspace tests passed, strict all-target lint clean.
- gate 4 backlog isolation: independent group buffers/handoffs, live drain and
  poller-recency/age/partition checks, shared fallback verified by actual-Go
  fixtures and mixed isolationDispatch probe. Full30 stages pass;401 workspace
  tests and strict lint pass (matching-isolation-dispatch-next.md).
- gate 4 still open: forwarding, partition routing/scaling, isolation enable
  owner reload/state/balancing and semaphores. Backlog routing is not the full gate.
- gate 6 still open: cancellations, retries/timeouts beyond the SDK probe,
  partitioned lists, concurrent pollers, crash/restart and failover, and the
  baseline history/error comparison.
- gate 5 open: membership/ownership, standalone Matching lifecycle; the
  production standalone server still serves Matching from the in-memory engine
  (`Matching::new`), and the durable matcher is wired only in the test/mixed
  harness. Wiring the durable matcher into the standalone server is the end-state
  requirement.

## Next: History

Mutable-state/history engine, task/timer/replication processors, durable branches,
shard ownership, resets, active/passive failover and cross-cluster recovery. Validate
Rust History with Go Matching/Frontend/Worker before moving on.

## Then: Frontend

Route through real service clients, complete domain/cache/archival policies,
visibility backends, auth, quotas, transport/error parity and operational behavior.
Validate with migrated services and Go Worker, with Go/Java SDK replay coverage.

## Finally: Worker

System workflows, scanners, replication/archival/indexing/batch/background jobs,
service lifecycle and production recovery. Then validate a complete Rust cluster
with no Go Cadence process and all supported production backend schemas.
