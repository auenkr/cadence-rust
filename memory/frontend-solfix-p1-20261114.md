# Frontend P1 implementation — 2026-11-14

Branch `frontend-solfix-p1`, based on origin/master. Scope: SOL #1–#5 and Opus LOW-1 only. No local Cargo, Go or Docker commands.

Initial implementation commit: `ab8e495ca7d366ddb0903414c6a9244ff1dce2d1` (all six findings).
Follow-up `458304e0b`: preserve standalone local-engine ByID fallback and exercise the unqualified live Thrift WorkflowService hook name.

- #1: both gRPC async operations dispatch the complete wrapper request via the installed production WorkflowApi, retaining GRPC_CALLER headers/deadline and propagating publication errors. Handler-path regression covers both methods and success/error.
- #2: all four gRPC activity ByID operations dispatch their original ByID model and operation to the production hook without resolving a local token. Local standalone fallback retained. Handler regression checks a remote-only activity and heartbeat response.
- #3: gRPC domain DLQ read/purge use the same extracted core implementations as Thrift; merge uses existing core merge implementation. Bounds, paging, tasks, errors retained. Handler regression covers nonempty paged reads, purge persistence and merge failure through gRPC plus Thrift read dispatch.
- #4: ReleaseSemaphore attributes added to shared Decision, Thrift field 140, protobuf input/output oneof. Completion-handler regression checks forwarding and both codec roundtrips.
- #5: WorkflowExecutionInfo timestamp unit is now Unix nanoseconds in shared model/Thrift/protobuf; standalone engine converts milliseconds at its to_info boundary. Live Thrift list regression covers fractional start/execution/close and absent close.
- LOW-1: HistoryProxy forwards original History tonic::Status in AdminError::History (code/message/details retained), rather than coercing it to InternalServiceError. Proxy regression calls an actual test History gRPC endpoint returning a typed status.

## Validation (in progress)

Initial head ab8e495ca:
- Frontend CI (also generates formatting patch): https://github.com/auenkr/cadence-rust/actions/runs/43595254822
- service-core CI: https://github.com/auenkr/cadence-rust/actions/runs/43595257521 — failed formatting before compile.
- matching CI (adapter unit tests): https://github.com/auenkr/cadence-rust/actions/runs/43595260811 — failed formatting before compile.

Formatting is generated on CI, selected only for files changed by this task. Final package runs and MySQL mixed gate pending; no parity completion claimed yet.
