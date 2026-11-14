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
- Third SASL attempt: https://github.com/auenkr/cadence-rust/actions/runs/43577404535 (head `5e2fd2918e0a69098942667efa00ab9eed0849be`), failed during Kafka broker startup with `No serviceName defined in either JAAS or Kafka config`; Kafka selected its default GSSAPI mechanism despite the listener-specific SASL environment setting. Added the broker-wide `KAFKA_SASL_ENABLED_MECHANISMS=PLAIN` setting alongside the listener setting. The next SASL run will verify broker startup and the gate traffic.

## Final SASL/PLAIN evidence

- Lane: `history-es-visibility.yml`, `es_flavor=elasticsearch`, `two_history=false`, `kafka_auth=true`.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/43578304254
- Run conclusion: **success**; head SHA: `d256e7c0549364e9676d337cdca0369750269b30`.
- The CI log contains the required exact line: `GATE SUMMARY failed: []`.
- The gate reports `kafka-broker-stopped` and `kafka-broker-restarted` passed, `es-index-has-documents` passed, and `go-worker-alive-after-es-sdk` passed. `gate/es-final-docs.json` records 114 ES documents, confirming Rust History's authenticated Kafka visibility writes were consumed by the Go Worker indexer.
- Rust binary SHA256 from the clean runner target: `cadence-server` `807e8001ed0fa01e0d7087333fa9dd40a81ea4d4a8cfa1a3eb57dbb01f2b60f3`; `cadence-history` `d9adf548e552462983734c9f774ca8f01837798242a3db835f2b379670c5d897`.
- Authenticated Kafka image: `apache/kafka:3.8.0`; SASL/PLAIN uses test-only `cadence` / `cadence-password`. No TLS mode was added, so there is no TLS lane to report.
- The three earlier failed attempts (runs 43575836926, 43576647189, 43577404535) were Kafka broker test-fixture setup failures before the end-to-end gate; their fixes are recorded above. None reported a Rust Kafka application defect.
- Validation commands run locally: `git diff --check`, Python syntax compilation, and `bash -n`; no local Cargo, Go, or Docker commands were run.
