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

The current final runner invocation is still **not** a Cassandra service gate.
`tests/run-go-service-baseline.py` generates a Rust SQLite default store and
starts Go Frontend/History/Worker with the SQLite zone. Its SDK failure and
restart coverage proves only the SQLite configuration. Do not describe the
script as Cassandra mixed-service validation and do not mark Matching
complete from it. A real gate must configure Go Frontend/History/Worker and
Rust Matching against the same pre-existing Go Cassandra schema/keyspace,
seed domains through Go services, start no Go Matching, and capture SDK
workflow plus task, semaphore, paging, failure, restart, and recovery
assertions. The adapter harness is separate and does not substitute for that
gate. Runtime/build validation is pending the shared telemetry-owned heavy
lane; until the mixed gate has reproducible evidence in `memory/evidence/`,
Matching remains incomplete and History must not begin.
