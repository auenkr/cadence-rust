# Matching Cassandra validation runbook

Run the prepared sequence only after the shared build lane is released. Keep it
in tmux session `cadence-rust`; the runner is intentionally sequential and stops
at the first failed phase. It writes commands, logs, and per-phase exit codes to
`memory/evidence/matching-cassandra-validation-<UTC timestamp>/`.

```sh
tmux new-session -d -s cadence-rust
tmux send-keys -t cadence-rust 'cd /home/ubuntu/github/auenkr/cadence-rust && tests/run-matching-cassandra-validation.sh' Enter
```

Before pulling or starting the Cassandra image, the script checks disk and
available memory, tool availability, Docker daemon access, and that its named
container does not already exist. Defaults require 18,000,000 KB free disk and
4,000,000 KB `MemAvailable`; override with `CADENCE_CASSANDRA_MIN_FREE_KB` and
`CADENCE_CASSANDRA_MIN_MEM_KB` only after checking the actual target and Docker
image/cache size. The default image is pinned to `cassandra:4.1.7`; override it
with `CADENCE_CASSANDRA_IMAGE` only when validating another documented Go
compatible Cassandra version. Docker publishes only on loopback. The script
creates a one-node disposable keyspace and applies the checked-in Go schema;
it does not modify a user's existing database. Use a fresh container name and
test keyspace. Preserve failed containers and evidence until the failure is
understood.

The first invocation regenerates `rust/Cargo.lock` separately and stops if the
lock differs from HEAD. Review the recorded diff for the `scylla` 1.9.0 local
path patch and its transitive dependency changes, commit the reviewed lockfile,
then rerun the script. It verifies the locked Scylla package is version 1.9.0
with no registry source before any `--locked` build. `rust/Cargo.toml` maps
`scylla` to `rust/vendor/scylla-1.9.0`; vendor provenance and the narrow TLS
server-name patch are recorded in
[`CADENCE-PATCHES.md`](../rust/vendor/scylla-1.9.0/CADENCE-PATCHES.md).

The sequential gates are Cassandra config/host-selection tests (including
hostpool expiry and retry), dedicated-runtime shutdown, Go-compatible Matching
Cassandra adapter tests, shared semaphore persistence tests, Matching
semaphore RPC behavior, all persistence targets (which compiles the vendored
driver and every backend), strict persistence Clippy, and the Matching server
build. Then the runner starts Cassandra 4.1.7, applies
`go/schema/cassandra/cadence/schema.cql`, and executes the existing Go seed →
Rust lease/write/reconnect/read → Go verification harness. Use the resulting
phase logs and Go assertions as narrow adapter interoperability evidence.

Source inspection at this worktree HEAD confirms that
`open_configured_default_store` already selects `CassandraMatchingTaskStore`
and `CassandraDomainMetadataStore` for a Cassandra default store. The old claim
above that the production task-store factory was missing is stale. Startup is
wired through `rust/crates/cadence-server/src/main.rs` and
`rust/crates/cadence-server/src/matching_task_store.rs`. This source change also
wires the Cassandra semaphore factory into production startup, selecting the
configured Cassandra metadata/token tables rather than in-memory state.
Neither finding is runtime-validated.

The final runner now calls the production gate with `--cassandra`. It writes
Go's Cassandra zone config and Rust's production config with the same default
and visibility keyspaces, starts original Go Frontend/History/Worker and two
Rust Matching nodes, and starts no Go Matching. It registers a domain through
Go Frontend, traverses Go domain pages using opaque continuation tokens, and
runs the existing SDK workflow across idle eviction and Matching owner
crash/recovery. A Go gocql observer reads task-list range and pending/cleared
rows directly from the Go `tasks` table. It also creates one unique
capacity-one semaphore through the Go Frontend `CreateSemaphore` API, probes
concurrent acquires through Rust Matching, verifies the owner/token row with
Go gocql, and retries that owner after a Matching crash. The injected persistence
observer retains the existing SQLite query path for other runner modes; in
Cassandra mode it uses Go CQL for pending/cleared task rows and range IDs in
the SIGKILL, optional process-restart, and SIGTERM drain/recovery branches.
The offline `node tests/matching-task-observer-self-check.cjs` verifies that
each backend selects only its own observer. Evidence and server logs go below
the selected artifact directory. The adapter-only Go seed → Rust persistence
→ Go verify phase remains separate.

The gate is implemented but not yet executed in this source-only lane. Run
`tests/run-matching-cassandra-validation.sh` after the shared telemetry-owned
build lane is released. It applies the existing Go cadence and visibility
schemas, then records the configured mixed gate under
`memory/evidence/matching-cassandra-validation-<UTC timestamp>/`. Matching
remains incomplete until the script passes and the resulting evidence is
audited; do not begin History before that service parity decision.

The runner's reproducible environment is an unauthenticated loopback Cassandra
4.1.7 container, Go/Rust schema files from this checkout, Go, Node with
`cadence-web/node_modules` installed, and the already-reviewed Rust server
binary from the preceding build phases. It requires the disk/memory preflight
thresholds above and a free container name. For an externally managed
instance, set `CADENCE_CASSANDRA_CONTACT_POINTS`,
`CADENCE_CASSANDRA_PORT`, `CADENCE_CASSANDRA_KEYSPACE`, and
`CADENCE_CASSANDRA_VISIBILITY_KEYSPACE`; the runner still owns Go/Rust config
generation and schema application. The current harness does not configure
Cassandra authentication or TLS.
