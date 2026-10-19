# Matching persistence production parity audit (2026-10-19)

Read-only source audit for Matching storage and recovery. Root reference is
`a7e33258d`, a docs-only checkpoint after `f6e6363d7`; the latter contains
the preceding Matching shard-distributor production gap audit. This audit's
selected worktree is `t3/matching-history-peer-validation` at `08265d333`. Also
inspected the uncommitted production-cache draft in
`/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider`, branch
`t3/matching-runtime-owner` at `6011effad` plus its working-tree changes. No
build, test, or implementation command was run.

## What is already proven

The Rust SQLite store is a meaningful interop slice, not an unverified format
port. `rust/crates/cadence-persistence/src/matching_sqlite.rs:302-307` compares
the two Matching table definitions to Go's SQLite schema. The source supports
both Go encodings (`thriftrw`, `thriftrw_snappy`) and reads the row's stored
`data_encoding` (`matching_sqlite.rs:82-92, 120-122, 214-245`).
`memory/matching-storage-next.md` records passing bidirectional real-Go SQLite
interop for task and task-list blobs, both codecs, leases, stale-write fencing,
partition metadata and recovery. Those results establish the tested Go SQLite
schema/codec path; they do not establish production backend selection or
cross-database migration.

The current Rust store implements the SQLite queue operations used by its
durable queue: lease, metadata update, atomic task append, bounded ordered read,
size, delete, bounded GC and task-list removal
(`matching_sqlite.rs:95-278`). Lease and queue-range behavior has focused tests,
and the implementation explicitly preserves SQL lease conventions
(`matching_sqlite.rs:125-161`). I found no new serialization defect in that
tested SQLite path, so these are evidence boundaries rather than rewrite tasks.

## Production gaps

**Rust does not select the configured Go Matching task persistence.** Go's
Matching service receives the resource TaskManager together with the other
production dependencies (`go/service/matching/service.go:100-118`). Go's
persistence factory creates TaskManager over the configured default store and
wraps it with error injection, a configured rate limiter and persistence
metrics (`go/common/persistence/client/factory.go:190-207,684-718`). The store
factory accepts SQL, NoSQL and sharded NoSQL configurations
(`factory.go:690-713`).

At root, Rust chooses durable matching queues only when `--data-dir` is set;
otherwise it starts the in-memory runtime. With a data directory it opens or
creates `<data-dir>/matching.db` as a SQLite store, always writes
`thriftrw_snappy`, and uses that direct store for the runtime
(`rust/crates/cadence-server/src/main.rs:742-785`). This is separate from the
Go-configured SQL/Cassandra task store. The cache draft wires configured
persistence into a live domain metadata cache, but leaves the task queue on the
same standalone `matching.db` path and SQLite constructor
(`/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/rust/crates/cadence-server/src/main.rs:777-826`).
That draft advances domain metadata integration; it does not implement or
select the Matching task backend.

**Rust implements neither Go SQL shard behavior beyond the single-shard case.**
Go has two distinct shard concepts here. Its SQL task store's `nShards` pages
through persisted task-list `shard_id` values in `ListTaskList`
(`go/common/persistence/sql/sql_task_store.go:41,49-60,328-342`). Separately,
per-task-list operations calculate a shard index from
`FarmHash32(domainID + "_" + taskList)` and `GetTotalNumDBShards()` for physical
database routing (`sql_task_store.go:87-94`;
`go/common/persistence/sql/sqlplugin/dbSharding.go:44-47`); SQL config supports
multiple databases (`go/common/config/config.go:329-347`). Rust currently
hard-codes persisted row `shard_id=0` and keeps one SQLite connection, with no
physical database routing (`matching_sqlite.rs:44-58,82-86,144-145,272-273`).
The existing Go interop evidence uses one SQLite database and shard zero. It
therefore establishes neither persisted task-list shard pagination nor
configured physical database routing for multi-shard SQL.

**Cassandra/NoSQL has no Rust task-store implementation.** Go config accepts
NoSQL and ShardedNoSQL, with task-list-to-shard routing through configured
`taskListHashing.shardOrder` (`go/common/config/config.go:216-219,260-299`;
`go/common/persistence/nosql/sharding_policy.go:72-78,98-109`). Go's Cassandra
backend stores typed columns and partition configuration in its CQL row model,
starts task-list ranges at 1, uses conditional updates for the lease and
conditional logged-batch insertion for task rows, and can apply Cassandra TTL
(`go/common/persistence/nosql/nosql_task_store.go:94-179,291-359`;
`go/common/persistence/nosql/nosqlplugin/cassandra/tasks.go:37-46,178-205,342-411`).
Its `ListTaskList` and `GetOrphanTasks` calls are explicitly unsupported, so
these should not be described as Cassandra-only Rust regressions
(`nosql_task_store.go:253-260,68-72`). Rust SQLite's Thrift blobs and SQL
transactions are not a drop-in Cassandra representation or LWT implementation.

**A Rust restart recovers only the selected Rust SQLite file.** `open()` uses
`SQLITE_OPEN_READ_WRITE` and checks for the two expected tables; `create()`
installs only their DDL in a new database (`matching_sqlite.rs:49-80`). The
root binary uses a Rust-named `matching.db`; it does not discover the
pre-existing Go TaskManager database or any configured SQL/Cassandra shard.
When pointed manually at a Go SQLite DB, the passing single-shard interop
establishes that tasks can be read and written there, but no runtime config or
cutover path currently selects that file. There is no import/migration tool for
MySQL, Postgres or Cassandra state. A same-process or rollback plan also needs
to establish who owns leases during cutover; a DB copy without preserved
`range_id`, `ack_level`, task IDs, task rows and `data_encoding` is not a safe
restart migration.

**Persistence surface is narrower than Go's SQL TaskManager.** Rust's store
has no `list_task_list` pagination or SQL orphan-task scan. Go SQL implements
both; listing encodes a sharded cursor with Go Gob, and orphan scanning returns
task keys (`go/common/persistence/sql/sql_task_store.go:312-380,586-608`). These
are lower priority than the active queue path but matter to a general-purpose
TaskManager and cleanup/recovery tools. Cassandra Go also leaves those two
operations unsupported, so preserve backend-specific behavior if portability
is implemented instead of inventing a universal success path.

**Retry and error classification are currently SQLite-specific.** Rust's
`matching_retry` retries every `StorageError::Sql` for up to the configured
30-second exponential policy and stops retrying non-SQL and condition failures
(`rust/crates/cadence-persistence/src/matching_retry.rs:7-45`). The durable
queue uses this for opening/renewing leases and each bounded task read
(`matching_queue.rs:174-176,227-240,135-139`); task append, ack metadata and
partition metadata writes are direct store calls (`matching_queue.rs:253-283,497-539`).
Go Matching uses `persistence.IsTransientError` with its persistence retry
policy for lease and read paths (`go/service/matching/tasklist/task_writer.go:205-229`;
`task_reader.go:145-148,226-245`). Go's store layer also has backend-specific
condition/consistency handling; Cassandra retries delete at a lower
consistency level only for consistency failures
(`go/common/persistence/nosql/nosqlplugin/cassandra/tasks.go:318-339`). The
existing retry fixture validates a bounded set of outcomes, but it does not
establish SQL-driver/Cassandra classification parity. Retain that fixture and
add backend-error cases when adding configured stores; do not broaden all
storage failures into retries without checking Go classification.

## Prioritized bounded work

1. **Make the queue store a runtime dependency.** Define a small async/sync
   `MatchingTaskStore` boundary for the queue's actual calls, keep SQLite as the
   test/development adapter, and remove the `--data-dir` gate as the only way
   to get restartable queues. Have Matching runtime construction receive the
   task store explicitly from production config. Document whether it uses Go's
   default Task store, then wire the configured backend rather than creating a
   parallel private file. The cache draft's domain metadata integration can
   stay independent.
2. **Complete configured SQL compatibility.** Implement Go's DB-shard hash and
   configured `nShards`/multiple-database routing before claiming SQL parity;
   add a backend adapter for the SQL drivers Cadence actually configures, with
   the same schema and transaction fencing. Keep the SQLite schema/codec oracle
   as regression coverage. Validate opening a Go-created single-shard DB and a
   multi-shard fixture through the configured runtime, including stale leases
   and process restart.
3. **Port the configured Cassandra backend as its own slice.** Mirror the
   CQL task-list sentinel row, initial range 1, LWT create/renew/delete,
   consistency fallback, configured task-list shard order, TTL limits, and
   partition-config map conversions. Exercise lost lease, competing owners,
   expired task, Cassandra restart/reconnect and persisted queue recovery
   against the real Go schema/provider or a compatibility fake backed by Go
   fixtures. Reuse Go's unsupported-operation results for ListTaskList and
   GetOrphanTasks until those features are actually ported.
4. **Lock down error/recovery contracts.** Compare `IsTransientError` and
   backend-specific retry paths with Rust's retry boundaries for lease, read,
   append, ack and metadata updates. Add exact error mapping for condition
   failures and tests for uncertain write outcomes so task IDs remain reserved
   and acknowledged rows are not lost. Add an operational cutover/recovery
   test that starts from a Go-owned configured store, removes Go Matching,
   restarts Rust against the same selected backend, and proves old backlog and
   ack/range state resume correctly.
5. **Finish the SQL manager surface and instrumentation.** Port SQL task-list
   pagination/Gob cursor compatibility and orphan enumeration where required;
   preserve Cassandra's explicit unsupported responses. Apply configured
   persistence QPS/error-injection/metrics wrappers at the matching task-store
   boundary, since the Go factory supplies these around TaskManager operations
   (`go/common/persistence/client/factory.go:197-207`).

## 2026-10-19 configured-store wiring follow-up (source frozen pending build slot)

Runtime construction now resolves the explicit production
`persistence.defaultStore` and uses its SQLite file for Matching task queues;
the private `--data-dir/matching.db` and in-memory paths remain for embedded
development only. The resolver carries Go SQLite's `rwc`, 60-second busy
timeout, WAL, and fixed nanosecond time-format defaults, accepts only supported
writer options, and rejects unsupported SQL plugins, NoSQL, multi-shard or
multiple-database layouts, in-memory DBs, read-only mode, and unknown URI
attributes. Opening the configured store validates Go task-list/task primary
keys and required columns without installing any schema. `connectAddr` remains
ignored as it is for Go's SQLite plugin.

The mixed Go/Rust probe has been extended in source to use the configured DB
for both directions: Go's TaskStore reads task-list metadata written by Rust,
and Go's TaskStore inserts a distinct activity task ID 2 after Rust creates
the matching queue and a first Rust task at ID 1. A fresh Go task list can
restore ack level 0, so ID 0 would be skipped by Rust's strict
`task_id > read_level` scan. The corrected probe first polls and starts the
valid Rust-enqueued activity (advancing the reader), then polls for a second,
distinct Go-enqueued activity at ID 2 and asserts its activity ID and input.
It then waits up to five seconds for Rust's asynchronous GC to remove Go row
2. Both activities use valid scheduled events produced by Go Frontend/History.
The compatibility runner passes the selected DB to the Go probe and checks
that production startup did not create the private data-dir database.

This follow-up is **source-only and unvalidated**: History currently owns the
shared build slot, so no compile, build, or tests have run for this delta. The
probe's distinct Go task delivery and consumed-row assertion, plus the Rust
wiring, must be exercised after the slot is released. Even when that passes, this is only evidence for one
single-shard SQLite backend. It does not establish configured multi-shard SQL,
Cassandra, every SQL plugin, or overall Matching service parity. Do not move
to History migration until the configured-store probe, full Matching parity
audit, and mixed Go/Rust failure/restart/recovery validation have reproducible
passing evidence.

## Production mixed Ringpop probe staged (source-only, not acceptance evidence)

`tests/run-go-service-baseline.py --rust-matching --rust-production-binary <path>`
now selects an ordinary Go server build and starts only Go Frontend, History,
and Worker. That mode does not compile or use the Go Fx discovery overlay. It
generates a Rust Matching config pointing at Go's auto-setup SQLite database
and the same Ringpop bootstrap hosts, then launches the Rust server as only
Matching. The probe waits for a public Go Frontend `DescribeTaskList` call to
resolve Matching, which will require the original Go `PeerProvider` to discover
the Rust member by its Ringpop `serviceName`/gRPC labels. The restart mode
SIGKILLs only Rust and uses the existing SDK to resume queued work. Production
mode also tests SIGTERM shutdown while a matching task is persisted, restarts
Rust against the same database, and checks SDK completion. Adapter mode retains
the deterministic stop-file/idle-unload tests.

This runner mode is deliberately staged. Current Rust `main.rs` does not yet
assemble/start the new `MatchingRingpopProvider` or membership lifecycle, so
the Go provider will not discover the Rust process until that wiring lands.
The mode has not been built or run and must not be recorded as passing. The
test-only static PeerProvider route remains useful adapter evidence but cannot
substitute for production discovery. The production mixed gate must also replace
the current loopback-only Matching-to-History client with a discovered
`MatchingHistoryPeerResolver`/`RingpopHistoryMembership` before claiming
service parity. Existing adapter-only idle-unload behavior remains an explicit
coverage gap for the production process probe.

## Timestamped configured-store validation runner (source-only)

`tests/run-matching-configured-store-validation.py` is the reproducible runner
for the configured SQLite source delta. It records UTC start/end times, Git
HEAD, server binary path and SHA-256 hashes of relevant source, probe and audit
files before and after validation. It emits a separate log and status per
independent gate, plus `source-freeze.json` and `results.json` in a unique
timestamped artifact directory. The gates cover (1) persistence SQLite
options/schema and store-operation tests, (2) server configured-store option
resolution, (3) a locked `cadence-server` build, (4) the Go/native-Thrift
configured two-way probe, and (5) strict Clippy for `cadence-persistence` and
`cadence-server` (`-D warnings`). The compatibility probe verifies
Rust task-list data through Go persistence and Go activity task ID 2 delivery
plus subsequent Rust-side task-list readback/collection. Run with
`python3 tests/run-matching-configured-store-validation.py --target-dir /path/to/shared/target`.
The server binary defaults to `<target-dir>/debug/cadence-server`; `--server-bin`
can override it when reusing a previously built binary.
Cargo commands use `--locked`, default to one build job, and inherit an existing
`CARGO_TARGET_DIR`; `--target-dir` can select the shared warmed target explicitly.

This is a source-only checkpoint until the validation runner has completed.
It is not acceptance evidence until all five gates pass with matching
before/after source hashes; the staged production
Ringpop probe remains separately blocked on production provider assembly.

## Focused configured-store probe evidence (2026-10-19)

The native configured-store runner passed after isolating the two startup-only
checks from the production data directory and strengthening the live Go
readback to inspect the Rust-created activity task payload before Rust
delivered it. The exact command was:

```sh
python3 tests/run-matching-thrift-compat.py \
  --server-bin /home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target/debug/cadence-server \
  --artifacts-dir artifacts/matching-configured-store-20261019T220052Z
```

The command exited successfully. It ran Go overlay compile diagnostics, Go
TaskStore seed/readback checks, all ten generated-client native Matching
Thrift methods over TChannel, HTTP, HTTP with trailing slash and TLS, Rust
task-list and task-row interoperability, the Go-enqueued task ID 2 delivery
and collection assertion, dynamic failover checks, graceful drain health,
plain HTTP routing checks, TLS startup checks, and embedded/listener-failure
startup checks. The live task-list input is preserved in
`artifacts/matching-configured-store-20261019T220052Z/rust-configured-task-list.txt`.
The production negative assertion passed: the explicit configured-store
server did not create `artifacts/matching-configured-store-20261019T220052Z/data/matching.db`.
Embedded startup wrote only under `embedded-data`, and listener failure used
`listener-failure-data`.

At tested HEAD `5ead1b419e2b09d5109aa27b9c691da474829e0d`, the source hashes
were:

| File | SHA-256 |
| --- | --- |
| `tests/matching-thrift-go_test.go` | `e49161b140a8c0e3ac6bb9527c5be5f853bd47d549045f270b6efe1e640d37ac` |
| `tests/run-matching-thrift-compat.py` | `bbeb74e27497cd5708b1d04a50a6fce339d0621ed1104f6f651bf53b40fb90ba` |
| `rust/crates/cadence-server/src/config.rs` | `00f0dd3153eb5084164277321e5eb94944108543b0738e02b4b0f0e5e412cf6a` |
| `rust/crates/cadence-server/src/lib.rs` | `053aea7397291306fe6e7dd595f772e6c6ea672bbea25ceee4a9aad8bd856cce` |
| `rust/Cargo.lock` | `a9995876e7e851652c6781142c2439b5d71e4903518df03b66a5047537d2b129` |

This focused runner is evidence for configured single-shard SQLite persistence,
native Matching transports and the listed lifecycle paths. It does not test
the production Go Frontend/History/Worker mixed-cluster Ringpop discovery path,
Matching-to-History discovery, or establish Matching service completion. The
separate staged production mixed probe remains unvalidated until production
main assembles the Matching Ringpop membership/provider lifecycle and the
discovered History peer resolver.

The Rust-side SQLite/options/build/Clippy gates passed independently at
`9619136057cfd3419d802068fd9bb1f8fa0da974`; their machine-readable run record
reported exit 0 for those four gates and is preserved as
`historical-run-results-failed-probe.json` in
`memory/evidence/matching-configured-store-20261019T220052Z/`. Its Go probe
failed at that earlier source revision and is explicitly retained only as
historical failure evidence. The focused final probe was consumed
interactively (no raw stdout log was captured); it still requires a tee-captured
rerun when the build slot is available. The source freeze manifest records the
exact source hashes for the successful run state.
