# Cassandra parity audit 4 (2026-11-26) — DONE (items marked open remain)

| Behavior | Go | Rust | Result |
|---|---|---|---|
| Unavailable(LWT/consistency) -> DBUnavailableError | nosql/utils.go:57 | `PersistenceError::DbUnavailable`, `HistoryReadError::DbUnavailable`, `VisibilityError::DbUnavailable` (cassandra_errors classifier) | **fixed**; frontend/history map it to InternalService (Go has no typed API error for it), retried as transient in engine_ndc `is_transient` (Go IsTransientError) |
| Timeout for history_node / visibility reads | nosql/utils.go:47 | `HistoryReadError::Timeout`, `VisibilityError::Timeout` | **fixed**; mapped to internal at frontend |
| Unit tests | | `read_errors_map_like_go_convert_common_errors` in history_cassandra.rs, visibility_cassandra.rs | added |
| Visibility insert/update TTL, maxCassandraTTL=157680000 selects no-TTL template | cassandra/visibility.go:43,91; constants.go:39 | history_visibility_cassandra.rs:16,121-154 | match |
| Session consistency LOCAL_QUORUM / serial LOCAL_SERIAL | gocql/session.go:71 (from config) | cassandra.rs:751 defaults | match (config-driven) |
| Request timeout default | gocql Timeout from config (0 = gocql default) | 10s when unset (cassandra.rs:753) | **open**: differs only when config omits timeout |
| Retry policy | gocql default SimpleRetryPolicy | scylla default policy, no override | **open** (not changed; needs design) |
| matching_domain_cassandra.rs, semaphores_cassandra_native*.rs, history_execution_cassandra.rs read paths | | | **not audited this round** |

## CI evidence
Head SHA 23dd7aba4293110bc3fd6c4a9071100afee114a8
- rust-ci cadence-persistence: success https://github.com/auenkr/cadence-rust/actions/runs/45758135874
- rust-ci workspace: success https://github.com/auenkr/cadence-rust/actions/runs/45758139402
- recovery-timing cassandra kill_history,cold_restart 3 reps: **success**, lost=0 Go (936/936), Rust (845/845) https://github.com/auenkr/cadence-rust/actions/runs/45764843398
  - first completion median ms: kill_history go 13 / rust 10; cold_restart go 11044 / rust 9461
- Earlier attempt https://github.com/auenkr/cadence-rust/actions/runs/45759874474 FAILED: Rust lost 1/895 (rep 2 rust, kill_history). Not reproduced in the rerun; not investigated (flaky or real loss unknown) — open.
- Rust SHA256: cadence-server 5ec25532c2967f5e620f9b0519a791449ebcd9aa4d26735c301e7d23a40bd2bd, cadence-history fb401c5de6b92cd1b60194a062bb668e824b45a0e33938f11402284741d47b7c
