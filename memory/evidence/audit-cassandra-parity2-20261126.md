# Cassandra parity audit 2 (2026-11-26) — IN PROGRESS

Scope: history_v2 (tree/node), error mapping; remaining files listed under "Not yet audited".

| Statement / behavior | Go | Rust | Result |
|---|---|---|---|
| upsert history_node text | nosqlplugin/cassandra/history_events_cql.go:26 | history_v2_cassandra.rs:19 | match |
| read nodes text, bounds `>= min AND < max` | history_events_cql.go:30, history_events.go:78 | history_v2_cassandra.rs:20,150 | match |
| range delete nodes | history_events_cql.go:33 | history_v2_cassandra.rs:21 | match |
| insert history_tree (6 cols; fork_time = ms-truncated, created_time = raw) | history_events_cql.go:36, history_events.go:56 | history_v2_cassandra.rs:69-79 (`ts()` ms for both) | minor: created_time not ns-precise (Cassandra timestamp is ms, no observable diff) |
| tree+node insert = LOGGED batch, default consistency | history_events.go:54 | history_v2_cassandra.rs:81,144 | match |
| delete branch = LOGGED batch, CL ALL | history_events.go:102,110 | history_v2_cassandra.rs:256 (`all=true`) | match |
| read tree: page size 100 loop, ancestors sorted by end_node_id, begin ids derived | history_events.go:147,209 | history_v2_cassandra.rs:195,209 | match |
| scan all trees: unsharded only; fork_time from column | nosql_history_store.go:483-505 | history_v2_cassandra.rs:259 | match |
| append guard `node_id < begin` -> InvalidPersistenceRequest | nosql_history_store.go:67 | history_v2_cassandra.rs:131 | match |
| read: skip txn < last, error on node <= last | nosql_history_store.go:149-178 | history_v2_cassandra.rs:164-172 | match |
| fork ancestors selection | nosql_history_store.go:241-268 | history_branch.rs fork_request | match |
| delete branch ranges/out-of-order break/last-remaining | nosql_history_store.go:412-464 | history_v2_cassandra.rs:238-255 | match |
| Error mapping: write timeout / conn closed / deadline -> `persistence.TimeoutError` | nosql/utils.go:47, gocql/public/client.go:50 | was `Internal` everywhere; now history_execution_cassandra.rs `internal` -> `PersistenceError::Timeout` | **mismatch fixed** |
| Error mapping: overloaded (0x1001) -> `ServiceBusyError` | utils.go:51, client.go:68 | now `ServiceBusy` for execution/history-node/visibility stores | **mismatch fixed** |
| Error mapping: Unavailable w/ LWT/consistency msg -> `DBUnavailableError` | utils.go:57, client.go:76 | classified (`cassandra_errors.rs`) but no `DBUnavailable` variant in PersistenceError/HistoryReadError/VisibilityError, still `Internal` | **open** (needs new variant + frontend mapping) |
| Not-found -> EntityNotExists | utils.go:41 | paths already return EntityNotExists explicitly | not re-verified |
| HistoryReadError/VisibilityError Timeout | utils.go:47 | no Timeout variant; stays `Internal` | **open** |

Fix: `rust/crates/cadence-persistence/src/cassandra_errors.rs` (classifier + unit tests), wired into `internal()` in history_execution_cassandra.rs, history_cassandra.rs, visibility_cassandra.rs (history_v2 reuses execution `internal`).

## Not yet audited
history_visibility_cassandra.rs, visibility_cassandra.rs statements/TTL, matching_domain_cassandra.rs, semaphores_cassandra_native*.rs, history_execution_cassandra.rs read paths, session defaults in cassandra.rs (defaults LOCAL_QUORUM/LOCAL_SERIAL, 10s timeout seen at cassandra.rs:751-754).

## CI evidence
Head SHA df74191019b01961118231dcf7a68c14b2502373
- rust-ci cadence-persistence: success https://github.com/auenkr/cadence-rust/actions/runs/45751576076
- rust-ci workspace: success https://github.com/auenkr/cadence-rust/actions/runs/45752198612
- recovery-timing database=cassandra kill_history,kill_matching 5 reps: success, lost=0 Go (876/876) and Rust (860/860) https://github.com/auenkr/cadence-rust/actions/runs/45752211702
- Rust cadence-server SHA256 9494fc18278ebef99748c4f4c4de569903d1c51c1bfce2a63184c3e7ed6b5457 (cadence-history 4f62107baa955290e8f84cfeb79e535829b86c66273f791fe0539c8cfe801c24)
