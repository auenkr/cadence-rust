# Frontend parity audit follow-up

Branch `parity-audit-gaps` starts at `a075a66fbfe2987a7ab7296cba26247d61625959`.

## Admin DeleteWorkflow and MaintainCorruptWorkflow

Configured Frontend instances resolve the execution through the owning History shard and use the database mutable state to select the history branches. The admin operation deletes history branches, execution and current-execution rows, and then visibility if both execution writes succeeded. The maintenance operation runs the ordinary Frontend describe and history reads and invokes deletion only for Go's three corruption messages.

The TChannel results from the first MySQL and PostgreSQL passes exposed a Go CLI panic when it decoded an omitted false boolean. Both Rust Thrift responses now send all three flags, matching the Go mapper. The successful persistence and healthy-maintenance probes from that attempt appear in the same logs. [MySQL](https://github.com/auenkr/cadence-rust/actions/runs/44764289633) · [PostgreSQL](https://github.com/auenkr/cadence-rust/actions/runs/44764313319)

## SQL failover-history cursors

Rust emits the Go SQL audit log gob record with its binary `time.Time` and event ID. Rust also reads the previous Rust text cursor so in-flight pages can continue after an upgrade. The Rust fixture test compares emitted bytes and decodes the original Go bytes. The Go fixture generator ran on the hosted runner on head `9db52df1d`. [Successful fixture generation and persistence CI](https://github.com/auenkr/cadence-rust/actions/runs/44748578276)

## PostgreSQL async restart investigation

Downloaded the gate and service logs from the failed run at `549606b58bcd2ca512d11436935357fb1813de2b`. Rust server SHA256 was `e8333343a664b5fe88fa74d899f5585c31848c65f2339ae6dfe5cea3b688a4fa`. [Failed PostgreSQL gate](https://github.com/auenkr/cadence-rust/actions/runs/44744989279)

- `sdk-async.log` reports 40/40 requests accepted before retry and identifies restart workflow 22 as missing.
- `go-history-worker.log` shows the Go consumer had the message on Kafka partition 0, offset 9. At 13:40:23.017 UTC it logged `start workflow execution failed after all attempts: code:deadline-exceeded message:context deadline exceeded`.
- At 13:40:23.051 UTC Go History logged `create-wf-execution` with `context canceled` for the same workflow and warned that the failed execution write could leave an orphaned history branch. This places the observed failure in the Go worker/History start path after Rust delivered the message to Kafka.
- Go Worker then failed to publish to the unavailable `cadence-async-gate-dlq` topic. The logs show a downstream mixed-gate failure after Kafka delivery and provide no evidence of a Rust producer defect.

## Mixed-cluster validation

The Frontend gate probe starts a fresh workflow, checks that MaintainCorruptWorkflow keeps healthy executions, deletes it through the admin gRPC API, and checks that History can no longer load the execution. It also checks the errors for missing execution and unknown domain.

The fresh validation used source head `2ee7dbda719cf85e03f7e5144b691a4a060d69e3`. All four mixed gates exited 0 with `GATE SUMMARY failed: []`. Both PostgreSQL samples accepted 40/40 async requests before retry and recovered all 40 workflows across Frontend restart.

| Validation | Conclusion | Rust binary SHA256 | Run |
| --- | --- | --- | --- |
| Fresh workspace formatting, Clippy and tests | Success | N/A | [44766594797](https://github.com/auenkr/cadence-rust/actions/runs/44766594797) |
| Frontend MySQL with Kafka and Elasticsearch | Success | `5bfc0c19694724ea94b90c3ed7ddd2208e3fa966794516012e3d215b2eb23d81` | [44766601305](https://github.com/auenkr/cadence-rust/actions/runs/44766601305) |
| Frontend PostgreSQL with Kafka, sample 1 | Success | `79f6f07c2ed5293d2e03bdef9dcbdc28f0c53041d6cdadb10f471e1fff30b634` | [44766606908](https://github.com/auenkr/cadence-rust/actions/runs/44766606908) |
| Frontend PostgreSQL with Kafka, sample 2 | Success | `12e7799c28af502937a1b27cdbacb42013301d7db1d2e0ce1859158826acb2f5` | [44766613119](https://github.com/auenkr/cadence-rust/actions/runs/44766613119) |
| History MySQL with two Rust History hosts | Success | Server `c358739e658bb727c32f5193cb027912dd7ee661c10d789af1e373d62102bd04`, History `d9c523a56b091d5d68b508be0ccb5630b67a3819dd933ddce1ebfb0a8a70deaf` | [44766621557](https://github.com/auenkr/cadence-rust/actions/runs/44766621557) |

The gates cover the healthy maintenance response, CLI compatibility and durable deletion. They do not inject corrupt mutable state or exercise a dual advanced-visibility migration.

## Thrift missing-execution error kind (branch `parity-deleteworkflow-thrift`)

The earlier note conflated two handlers. Go admin `DeleteWorkflow` with no execution returns the pointer `validate.ErrRequestNotSet` (`*types.BadRequestError`, "Request is nil."), which `adh.error` passes through and both mappers convert, so a declared `BadRequestError` is correct on Thrift and `INVALID_ARGUMENT` on gRPC (the gate probe `delete-workflow-no-execution` asserts this). Only `MaintainCorruptWorkflow` returns a by-value error, which is untyped (`UNKNOWN`); Rust Thrift already maps it untyped since commit `438e6f143` (see `parity-maintain-corrupt.md`). No behavior change was needed. Added unit test `admin_missing_execution_thrift_tests` in `yarpc.rs` pinning both kinds. CI (rust-ci.yml, package cadence-service-core, head `74923a618`, success): https://github.com/auenkr/cadence-rust/actions/runs/45710837581 (earlier failed runs were test-construction mistakes: 45708879920, 45710184511).
