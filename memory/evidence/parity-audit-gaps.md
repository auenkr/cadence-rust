# Frontend parity audit follow-up

Branch `parity-audit-gaps` starts at `a075a66fbfe2987a7ab7296cba26247d61625959`.

## SQL failover-history cursors

Rust emits the Go SQL audit log gob record with its binary `time.Time` and event ID. Rust also reads the previous Rust text cursor so in-flight pages can continue after an upgrade. The Rust fixture test compares emitted bytes and decodes the original Go bytes. The Go fixture generator ran on the hosted runner on head `9db52df1d`. [Successful fixture generation and persistence CI](https://github.com/auenkr/cadence-rust/actions/runs/44748578276)

## PostgreSQL async restart investigation

Downloaded the gate and service logs from the failed run at `549606b58bcd2ca512d11436935357fb1813de2b`. Rust server SHA256 was `e8333343a664b5fe88fa74d899f5585c31848c65f2339ae6dfe5cea3b688a4fa`. [Failed PostgreSQL gate](https://github.com/auenkr/cadence-rust/actions/runs/44744989279)

- `sdk-async.log` reports 40/40 requests accepted before retry and identifies restart workflow 22 as missing.
- `go-history-worker.log` shows the Go consumer had the message on Kafka partition 0, offset 9. At 13:40:23.017 UTC it logged `start workflow execution failed after all attempts: code:deadline-exceeded message:context deadline exceeded`.
- At 13:40:23.051 UTC Go History logged `create-wf-execution` with `context canceled` for the same workflow and warned that the failed execution write could leave an orphaned history branch. This places the observed failure in the Go worker/History start path after Rust delivered the message to Kafka.

## Mixed-cluster validation

The Frontend gate probe starts a fresh workflow, checks that MaintainCorruptWorkflow keeps healthy executions, deletes it through the admin gRPC API, and checks that History can no longer load the execution. It also checks the errors for missing execution and unknown domain.

## Final validation on head `2ee7dbda719cf85e03f7e5144b691a4a060d69e3`

Earlier heads failed: compile errors (`admin_delete` result alias, missing `Arc` import, `result_large_err` clippy) were fixed on `b652ea2da`/`da29c3baf`; gates on `da29c3baf` then failed `sdk-api-admin-workflow-fix-corruption-tchannel` because the Go CLI mapper dereferences all three bool fields, which Rust omitted when false (runs 44764743900, 44764757891). Fixed by `2ee7dbda7` (always emit all three flags, as Go's From mapper does).

- rust-ci workspace (clippy + tests): [44766594797](https://github.com/auenkr/cadence-rust/actions/runs/44766594797) success
- Frontend gate PostgreSQL run 1: [44766613119](https://github.com/auenkr/cadence-rust/actions/runs/44766613119) `GATE SUMMARY failed: []`
- Frontend gate PostgreSQL run 2: [44766606908](https://github.com/auenkr/cadence-rust/actions/runs/44766606908) `GATE SUMMARY failed: []`
- Frontend gate MySQL: [44766601305](https://github.com/auenkr/cadence-rust/actions/runs/44766601305) `GATE SUMMARY failed: []`
- History mixed gate MySQL: [44766621557](https://github.com/auenkr/cadence-rust/actions/runs/44766621557) `GATE SUMMARY failed: []`

The PostgreSQL lost-message failure did not recur in two reruns, consistent with the Go-side timeout finding above.
