# History queue v2 + S3/GCS archival gate (2026-11-14)
Branch history-queuev2-gate. Workflow history-mixed-gate.yml gained inputs `queue_v2` (sets history.enableTimerQueueV2/TransferQueueV2 true in the Rust dynamic config unless development.yaml already defines them, which it does; both default true) and `gcs` (with s3_archival: Rust History GCS archival phase `rgcs-*` against fake-gcs-server, retention 0, Frontend serves archived history over tchannel+grpc).
Fixes along the way (gate only): duplicate `gstorage` YAML key when s3+gcs both on; duplicate dynamic-config keys (parse error silently dropped all overrides). No Rust bugs found.
Head SHA 9efe657591da73307c022d65e88f84a214f0fa51. All runs: GATE SUMMARY failed: [].

| Lane | Run | Result |
|---|---|---|
| mysql/grpc queue_v2 | https://github.com/auenkr/cadence-rust/actions/runs/43568262634 | success |
| postgres/grpc queue_v2 | https://github.com/auenkr/cadence-rust/actions/runs/43568265556 | success |
| cassandra/tchannel queue_v2 | https://github.com/auenkr/cadence-rust/actions/runs/43568268351 | success |
| mysql/grpc s3_archival+gcs+queue_v2 (rs3-* and rgcs-*) | https://github.com/auenkr/cadence-rust/actions/runs/43568271612 | success |
