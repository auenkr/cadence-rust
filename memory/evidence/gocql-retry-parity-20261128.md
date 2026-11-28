# gocql client retry/timeout/consistency parity (2026-11-28)

Go: `go/common/persistence/nosql/nosqlplugin/cassandra/{plugin.go,constants.go}`, `gocql/{client.go,session.go}`, gocql `b225f9b53fa1` (`NewCluster`, `queryExecutor.do`).
Rust: `rust/crates/cadence-persistence/src/cassandra.rs` (`CassandraSessionConfig`, `connect`).

| Aspect | Go | Rust before | Status |
|---|---|---|---|
| Timeout | cfg or 10s | 10s default, cfg override | match |
| ConnectTimeout | cfg or 2s | 2s default, cfg override | match |
| Consistency | cfg or LOCAL_QUORUM; ParseConsistency | same | match |
| SerialConsistency | cfg or LOCAL_SERIAL | same | match |
| NumConns | MaxConns>0 else gocql default 2 | same | match |
| Port / hosts | cfg, env fallback, comma list | same | match |
| Host selection | ""/tokenaware,roundrobin -> TokenAware(RR); roundrobin; hostpool-epsilon-greedy; else error | same set, errors on unknown | match |
| DC / region filter | region (first 3 chars of DC) overrides datacenter | same | match |
| **RetryPolicy** | ClusterConfig.RetryPolicy unset (nil): first error returned, no retry (`rt == nil` in `queryExecutor.do`) | scylla default `DefaultRetryPolicy` retried (next host / same host) | **FIXED**: `GocqlNilRetryPolicy` always `DontRetry`; speculative execution explicitly None |
| ProtoVersion | cfg or 4, passed to gocql | only v4 accepted, else startup error | documented limitation (scylla driver speaks v4 only; default identical) |
| Reconnection policy | gocql Constant(3 retries, 1s), ReconnectInterval 60s | scylla driver built-in reconnect | documented driver difference, not config-driven in Go |
| Session refresh on ErrNoConnections | `session.refresh()` recreates session (min 5s interval) | not implemented | residual gap (not a retry-policy item; noted for follow-up) |

Unit tests added in `session_config_tests`: Go defaults, option overrides, nil retry policy session creation.

## CI evidence
Head SHA d462a73f35e927f471cd51670b7f0ecf19e85ee3.
- rust-ci (package=cadence-persistence): https://github.com/auenkr/cadence-rust/actions/runs/46108705540 — **success** (head d462a73f3).
- matching-cassandra-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/46108707851 — **failure** at `Go baseline probe failed` (RuntimeError in tests/run-go-service-baseline.py, probe.log not retrievable from step log). The same workflow also failed on branch gate-sweep-20261128 (run 46108484364) 3 min earlier without this change; last success was 2026-11-23 (run 45220115542). So failure is not attributable to this change but the gate gives NO passing evidence for it. Rust binary SHA256 not recorded (gate failed before reporting).

# Cassandra gates after merging master (head 4554a10f974c9ace6b4c98ea503e18f6a8e4e45b)
Merged origin/master (fix-cassandra-domain-typecheck) into gocql-retry-parity with no conflicts. All runs were dispatched on `gocql-retry-parity`.

| Run | Conclusion | Binary SHA256 |
|---|---|---|
| rust-ci cadence-persistence [46124074415](https://github.com/auenkr/cadence-rust/actions/runs/46124074415) | success | - |
| matching-cassandra-mixed-gate [46124076775](https://github.com/auenkr/cadence-rust/actions/runs/46124076775) | success | cadence-matching 82d103acb1158a6b599c971708ab6ca28ad2b7835177b2677c5c485393bb40fe |
| history-mixed-gate cassandra [46124079364](https://github.com/auenkr/cadence-rust/actions/runs/46124079364) | success | cadence-server d3ff7e139a590ecb991fa663288d95949f925d920bc653bab3d262e7f001f72d, cadence-history 599b9bf76aa0ed71e0f22353c8fef9a6fa94fd87c23a0fc2cab6f8123ee3350a |
| history-restartgate cassandra [46124081888](https://github.com/auenkr/cadence-rust/actions/runs/46124081888) | success | release cadence-server 73063f9886396fe72650ded8abbeac7be986ecca282274622bb12ece2c00734a, cadence-history bd7c98c94baa2a9b32187510515a8588177a3e9ec3cf55d45ccc3cc3eff8cb76 |
