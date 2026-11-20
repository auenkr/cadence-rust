# MaintainCorruptWorkflow / DeleteWorkflow parity follow-up

Branch `parity-maintain-corrupt`, validated head `a73c9a3597ea5bf2f96510a1bf86b341a6561452`.

## 1. Missing execution error
Go `MaintainCorruptWorkflow` returns `types.BadRequestError{...}` by value; the Thrift and gRPC mappers (`errorutils.ConvertError` with `*types.BadRequestError`) only convert the pointer, so the error is untyped on both transports. Rust gRPC already returned `UNKNOWN`. Rust Thrift now returns an untyped transport error with the message (`AppError::untyped` in `rust/crates/cadence-service-core/src/yarpc.rs`). There is no Thrift wire probe; the gRPC probe `maintain-corrupt-no-execution` passes.

## 2. Corrupt-state gate case (`tests/gates/admin_probe.py`)
The gate starts a workflow, sends two signals (3 history batches), deletes the `node_id=3` `history_node` row in the database, then:
- Frontend `GetWorkflowExecutionHistory` returns `INTERNAL cadence internal error, msg: corrupted history event batch, eventID is not continuous`.
- `MaintainCorruptWorkflow` returns an empty response and keeps the run. This matches Go: the admin handler compares exact messages (`corruptWorkflowErrorList`), but its Frontend client receives the metered wrapper text (`go/service/frontend/wrappers/metered/metered.go` handleErr prefixes `cadence internal error, msg:`), so Go never deletes. Derived from Go source, not from a Go-Frontend run.
- Admin `DeleteWorkflow` then deletes history, execution and visibility, describe returns `NOT_FOUND`, and zero history rows remain.
Cassandra is skipped (history_node delete needs the branch id). No dual advanced-visibility case: the harness has no dual-visibility mode.

## Validation
| Validation | Result | Rust binary SHA256 | Run |
| --- | --- | --- | --- |
| rust-ci cadence-service-core (head 438e6f143) | success | n/a | [44770220437](https://github.com/auenkr/cadence-rust/actions/runs/44770220437) |
| rust-ci workspace (head a73c9a359) | success | n/a | [44774425751](https://github.com/auenkr/cadence-rust/actions/runs/44774425751) |
| Frontend MySQL gate | success | `064fefc13943f1460b47a4d270bf436e7b8bdb017f5bd008bea7b33eb6d8164f` | [44774387259](https://github.com/auenkr/cadence-rust/actions/runs/44774387259) |
| Frontend PostgreSQL gate | success | `e4212d81ab3da1b49190a13ef8c9eda7a14a530d9f0a07596e982e1e52ffe8c0` | [44774406283](https://github.com/auenkr/cadence-rust/actions/runs/44774406283) |

Earlier gate runs 44770240195 / 44770261043 failed only because the first version of the gate case expected deletion; 44772507422 (MySQL diagnostic) showed the prefixed error.
