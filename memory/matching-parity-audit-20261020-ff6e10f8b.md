# Matching parity refresh: candidate ff6e10f8b (2026-10-20)

Read-only source audit at candidate HEAD `ff6e10f8bf71997ea87bdfb1fbd574ba80e48970` in `t3/matching-shard-processor-integration`. The candidate worktree was clean before this report. No implementation changes, builds or tests were made for this audit.

The earlier `matching-production-gap-audit-20261018.md` predates the current Matching config wiring. Its claims that task delete batch size, append batch/capacity/timeout, RecordTaskStarted timeout and shutdown drain providers are absent are stale. Those providers and sampling points now appear in `grpc_matching.rs`, `matching_writer.rs`, `matching_runtime.rs` and persistence queue code. This is a source observation only; their behavior was not revalidated at this exact candidate.

## Candidate status

**Implemented in source, validation boundary not established here:** Matching has durable task queues, SQLite operations, provider-based live queue settings, domain metadata integration, forwarding/query/cancel paths, and Matching Thrift/gRPC surfaces. Existing evidence in memory covers selected SQLite codec/schema interop and adapter tests, but this refresh ran no gates and does not transfer evidence to candidate HEAD automatically.

**Still missing at candidate:** production assembly for Ringpop ownership/discovery, History peer discovery and shard-distributor runtime integration is absent from this candidate revision. This statement applies only to `ff6e10f8b`; a newer dirty runtime worktree has since added these pieces (see below). Matching metrics do not have the Go-equivalent external metrics sink and instrumentation families. The production task queue and domain metadata backends remain SQLite-only. The production in-memory execution/semaphore state is not backed by Go-equivalent semaphore persistence.

**Not a completion claim:** forwarding, query, cancellation and config providers existing in source do not prove end-to-end SDK behavior, discovery, failure recovery or service parity. No current-candidate acceptance run was performed.

## Persistence backend parity

Rust Matching persistence is concretely SQLite-backed: `cadence-persistence` depends on `rusqlite` and `matching_sqlite.rs` contains task-list/task operations. The domain metadata boundary is in `matching_domain_metadata.rs`. The earlier audit's task/domain resolver paths reject SQL plugins outside SQLite and NoSQL stores. Go has MySQL and PostgreSQL task stores in `go/common/persistence/sql/sql_task_store.go` and plugin implementations under `go/common/persistence/sql/sqlplugin/{mysql,postgres}/task.go`. Schemas are `go/schema/mysql/v8/cadence/schema.sql` and `go/schema/postgres/cadence/schema.sql`. Cassandra's model is separate in `go/schema/cassandra/cadence/schema.cql` and is not covered by a SQL implementation.

Prioritize a backend-neutral Rust boundary before adding drivers:

1. Define `MatchingTaskStore` and `MatchingDomainMetadataSource` traits around the operations actually consumed by the durable queue/cache. Keep SQLite as an adapter and make it possible for runtime construction to supply a selected adapter.
2. Implement MySQL and PostgreSQL adapters against the existing Cadence schema and configured `defaultStore`, connection settings, pool options, `nShards` and `useMultipleDatabases`. Preserve Go's database selection/hash behavior; do not create private tables or apply schema migrations.
3. Preserve transaction fences: lease reads/locks the task-list row, compares the range ID, then increments under transaction; task insertion and task-list range verification are atomic; GC deletes acknowledged rows in bounded batches; reads preserve Go's ordered task-ID range and page behavior. Map stale/missing lease outcomes to the same conditional error class, separately from transient/database failures.
4. Match domain metadata layout and encoding: shard ID 54321, Go Thrift blob decoding, notification-version lookup, and UUID paging. PostgreSQL uses UUID `BYTEA`; MySQL uses `BINARY(16)`; column/layout details vary by schema.
5. Treat Cassandra as a distinct follow-up: its row/table model and conditional LWT semantics do not follow from the SQL adapters. Semaphore persistence also needs a separate backend availability audit; Go's Cassandra semaphore support does not imply SQL token-store support.

## Newer runtime worktree (outside candidate)

The separate dirty worktree `/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider` at `4ae464b80` contains uncommitted runtime assembly in `main.rs` and `matching_runtime.rs`: Ringpop membership, History peer resolution, shard-distributor executor/spectator, ownership/routing, live config reload and lifecycle hooks. Its existing evidence is under `memory/evidence/matching-main-gate-20261020-a7f2603a2/` and the Ringpop evidence tree. These files and artifacts belong to another source revision and are dirty; this audit did not modify or rerun them. Do not report those runtime pieces as absent across all worktrees, and do not treat that evidence as validation of candidate `ff6e10f8b`.

The newer runtime currently rejects shard-distributor configuration without Ringpop and rejects `system.enableGRPCOutbound=false`, since TChannel History outbound is not implemented. That leaves TChannel History fallback as an explicit parity item even in the newer runtime snapshot.

## Priorities and evidence needed

1. Complete configured MySQL/PostgreSQL Matching task and domain persistence, preserving Go schemas, shard routing, transaction fences, errors and codecs. Keep Cassandra separate.
2. Validate the newer production runtime wiring on a frozen source revision, then run the mixed cluster with Go Frontend/History/Worker and no Go Matching. Cover SDK workflows, service-to-service routing, restart/recovery, pending backlog, lost ownership/failover, cancellation and query behavior.
3. Add/verify semaphore persistence against backends actually supported by Go; add a Matching metrics sink with Go-compatible names/tags and relevant RPC, queue, writer, History and ownership instrumentation.
4. Record reproducible command lines, source revision, per-stage exit status and artifacts in memory. Matching remains incomplete until the backend matrix, runtime behavior and mixed-cluster failure/recovery audit are complete. Do not move to History migration before then.

