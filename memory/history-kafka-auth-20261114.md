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

### First CI setup failure and correction

- Initial SASL attempt: https://github.com/auenkr/cadence-rust/actions/runs/43575836926 (head `3246804d49b6c2b8a0487c10d26e2aaa546dcc3f`), failed during Kafka startup before the gate. The Apache Kafka entrypoint requires `KAFKA_OPTS` to reference a JAAS file for a SASL listener; its log reported `SASL is enabled` then `/etc/kafka/docker/configure: line 18: !1: unbound variable` because `KAFKA_OPTS` was unset.
- Updated the lane to generate and mount a temporary KafkaServer JAAS file and set `KAFKA_OPTS`; the gate harness and shell both pass static syntax validation. The corrected SASL run is pending.
- Second SASL attempt: https://github.com/auenkr/cadence-rust/actions/runs/43576647189 (head `f90461d5bbc717f2f0bd261a947f593913ff9c0b`), failed during broker startup. The broker loaded the JAAS system property but reported a JAAS configuration `No such file or directory`; the generated file was mode 0600, which is not readable by the Kafka image's `appuser`. The lane now makes the temporary JAAS file world-readable for the container and removes it during cleanup. No Rust/Go application connection was attempted in this run.
