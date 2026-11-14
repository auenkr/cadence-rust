# History swap gate (Go History <-> Rust History on one DB) — 2026-11-13

Branch `history-swap`, head `daf66a84c0854193e58e2a07caaa57ccfd060c2f` (merged origin/master).
Rust History binary SHA256 (debug, CI): `4f788e1c31a82b4850a9d86731ab5d0356d84a97b25de50c7034451190874b33`
(artifact `rust-history-binary.sha256`, same on mysql/postgres/cassandra).

## Gate
- Workflow `.github/workflows/history-swap.yml` (push on `history-swap` + dispatch with `database`, `outbound`), harness `tests/gates/run-history-swap.py`, SDK driver `tests/history-swap/main.go`.
- Services: Go Worker, Rust Matching, Rust Frontend stay up; History is swapped Go -> Rust -> Go -> Rust (SIGTERM, restart on same DB).
- Workflow: timer -> activity -> child -> signal s1 -> timer -> activity -> signal s2 -> done.
  1. Go: start A(8). 2. Rust: start B(4), advance A, finish A 0..4, start C(3). 3. Go: finish remaining A, B, C; start+finish D(3). 4. Rust: dump.
- Asserts: all 36 workflows (incl. children) complete, Go-read and Rust-read histories identical (`differ: []`), log scan clean (no corruption/decode/panic; benign "DLQ handler started" and Go "data-corruption-workflow" startup lines are ignored).

## Results (all PASS)
- Swap run https://github.com/auenkr/cadence-rust/actions/runs/43460051158 — mysql, postgres, cassandra all success, 36 workflows, no diff.
- rust-ci https://github.com/auenkr/cadence-rust/actions/runs/43460107575 success.
- history-mixed-gate x12 (db x two_history x outbound), all success: 43460112651, 43460119207, 43460125777, 43460130679, 43460136654, 43460142449, 43460147531, 43460154054, 43460160110, 43460166236, 43460172076, 43460178628.

## Rust bugs the gate proved (fixed minimally, per Go)
Go codec (`serializerImpl`) prefixes DataBlobs with byte 0x59 (never a valid thrift ttype), snappy wraps preamble+struct. Plain sqlblobs structs have none.
1. Rust could not read Go shard rows: ProcessingQueueStates / failover markers blobs (shard fields 42/50/55, encodings 44/51/56) -> `read_versioned_blob` honours the encoding and the preamble (`sqlblobs.rs`). Writers now emit the preamble (`versioned_blob`).
2. Cassandra shard UDT: Go writes zero-length timestamps (e.g. `timer_ack_level`); Scylla typed decode failed. `get_shard` now uses read-only `ShardUdtRead { range_id }` (`history_shard_cassandra.rs`).
3. Rust could not decode Go VersionHistories (`unknown ttype 89`): `Encoding::decode` strips a leading 0x59; added `Encoding::encode_versioned`.
4. Go could not read Rust rows (`DeserializeBatchEvents ... Invalid binary encoding version`): Rust wrote these blobs without the preamble. Now `encode_versioned`: VersionHistories (`mutable_state_bridge.rs` x2, `engine_close.rs`), auto-reset points (`engine_decision.rs`, `mutable_state_methods_decision.rs`), active-cluster selection policy (`mutable_state_methods_started.rs`), child-initiated event (`engine_decision_ops.rs`).

## Harness notes
- `kill()` falls back to SIGKILL if SIGTERM does not stop Rust History within 30s (seen on postgres).
- Earlier failing runs (for the record): 43445695184, 43450580417, 43454264344, 43456649216 (log-scan false positives only).
