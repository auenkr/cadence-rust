# history-review3fix (2026-11-14)

Branch `history-review3fix`, head `bc19b456931f544ce79de88b9b8f31c2d160eb02`. Fixes the combined findings of history-review3 (Sol + Sonnet).

## Changes
1. **Timestamps = nanoseconds at creation** (Go parity). `push_event` stores ns; all engine_* call sites pass ns; removed ms<->ns heuristics (`archive_nanos`, es_visibility `nanos()`, cloud_archiver scaling, `events_millis_to_nanos`); thrift `history_blob::decode` / `ToThrift` pass through; frontend archive_readers no longer converts.
2. **Archive enums as Go JSON text**: `cadence_api::enums::to_go_json_value` (thread-local flag making `cadence_enum!` serialize string names); used by `go_history_batch` in archiver_wiring.
3. **Kafka visibility producer**: TLS/SASL (`kafka_auth.rs`, config `KafkaTls`/`KafkaSasl`), retry max 3, eager connect at startup (rskafka `transport-tls`).
4. **Shard controller**: Weak cell references break the Arc cycle that retained retired shards.
5. **Shard start/close race**: `Lifecycle` handshake, engine stop claimed exactly once.
6. **Transfer-active `schedule_decision`**: no-op when a decision is pending (Go parity). NDC `schedule_decision` guard NOT added: decision already failed before scheduling, so no pending decision can exist.
7. **Forwarding deadlines audit**: matching add_task (2s), parent_close_policy_client, archival signaler, replication `decorate` already set TTL. `forward_start` lacked one -> added 2s (TASK_RPC_CALL_TIMEOUT). `replication_clients.rs:242` and `launcher.rs:241` only build channels; calls go through the deadline-setting paths above.

Notes: Cargo.lock looks stale for cadence-history (CI is not `--locked`). No s3-archival workflow exists; S3 is covered by `history-mixed-gate.yml -f s3_archival=true`.

## CI evidence (repo auenkr/cadence-rust; all `GATE SUMMARY failed: []`)
- rust-ci first run 43485063206 failed (thrift test expected ms->ns); fixed; rerun 43488023962 **success** (head bc19b45).
- mixed gate grpc two_history: mysql 43485096386, postgres 43485103239, cassandra 43485113416 - success
- mixed gate mysql s3_archival: 43485126946 - success
- ES visibility: 43485070223 - success
- mixed gate tchannel two_history: mysql 43489417417, postgres 43489423046, cassandra 43489428373 - success
(Gate runs used the commit before the test-only thrift fix.)
