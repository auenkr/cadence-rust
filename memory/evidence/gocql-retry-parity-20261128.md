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
- rust-ci (package=cadence-persistence): https://github.com/auenkr/cadence-rust/actions/runs/46108705540 — still queued when this was written; conclusion NOT yet recorded.
- matching-cassandra-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/46108707851 — still queued; conclusion NOT yet recorded.
