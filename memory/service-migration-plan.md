# Service-by-service migration (2026-10-15)

User requires completing one of the four Go service roles, validating it with
remaining Go services, then proceeding to the next. This supersedes the earlier
feature-by-feature cross-service migration order. All four roles remain incomplete.

Order: Matching -> History -> Frontend -> Worker (system/background).
Shared infrastructure is implemented as required by the active service. Generated
RPC services are not equivalent to these four deployment roles.

## Active: Matching

Source: go/service/matching (handler, tasklist, poller, config, semaphore).
Current Rust grpc_matching delegates polls into the embedded frontend/execution
engine and AddTask looks up local workflow state. Go Matching accepts queued
references and calls History.Record*TaskStarted on delivery. Consequently current
Rust Matching cannot replace Go Matching in a mixed Go/Rust cluster.

Independent components now verified: Go-format SQLite store, durable queue core,
Go History client, cancellation-safe delivery worker and basic queue registry.
The 11-event live probe uses Go Frontend/History and Rust queue delivery, but
bypasses the Matching RPC listener. It does not satisfy the replacement gate.
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
mixed-service integration. The existing full compatibility runner boots only a
Rust server and SDK clients, so its pass does not meet gate 6.

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
