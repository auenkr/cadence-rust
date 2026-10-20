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

Finally, the runner invokes the existing two-Rust-node failure/recovery gate:
original Go Frontend, History, and Worker stay up; no Go Matching is started;
the existing Go SDK exercises workflows across idle eviction, owner crash,
restart, and recovery. This gate currently uses the service's SQLite task
store, because the production Matching task-store factory does not yet wire
`CassandraMatchingTaskStore`. It therefore cannot establish Cassandra-backed
Matching service parity. Cassandra semaphore persistence likewise has adapter
coverage and compilation, but no end-to-end service binding test. Keep both as
explicit parity gaps until production wiring and a durable Cassandra-backed
mixed Go/Rust SDK/restart test exist. Do not mark Matching complete from this
script alone; finish the service audit and remaining failure, restart, and
recovery scenarios first.
