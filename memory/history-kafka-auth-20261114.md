# History authenticated Kafka gate (2026-11-14)

## Scope

The relevant end-to-end lane is `.github/workflows/history-es-visibility.yml`. It runs Rust History's advanced visibility Kafka producer, Go Worker's visibility indexer consumer, and the existing SDK gate. `tests/gates/run-frontend-mysql-lane.sh` starts Kafka; `tests/gates/run-frontend-gate.py` writes the shared Go-compatible persistence YAML and Rust async queue YAML.

`memory/history-auditgaps-20261114.md` marks authenticated Kafka as open. `memory/history-review3fix-20261114.md` records the Rust-side TLS/SASL implementation in `kafka_auth.rs` and Kafka config fields, but previous evidence exercised only plaintext Kafka.

## Changes for the authenticated lane

- Added `kafka_auth` workflow input; false retains the existing plaintext broker lane.
- When true, the Kafka container has an externally published SASL/PLAIN listener and a plaintext internal listener for broker administration/topic creation. Container limits remain 1 GiB memory/swap, 1 CPU, 2048 PIDs.
- The gate uses test-only credentials `cadence` / `cadence-password`; it supplies Go-compatible `kafka.sasl` YAML for Rust History and the Go Worker, and SASL settings for the async workflow queue so the rest of the gate can still run.
- TLS was not added; SASL/PLAIN is the required lane for this pass.

## Validation and CI evidence

- Local validation: `git diff --check` passed. Per instructions, no local Cargo, Go, or Docker commands were run.
- Authenticated SASL/PLAIN lane: pending CI dispatch.
- Run URL, head SHA, conclusion, and the `GATE SUMMARY failed: []` evidence will be recorded here after the run completes.
