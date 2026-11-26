# Cassandra parity audit 4 (2026-11-26) — IN PROGRESS

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
