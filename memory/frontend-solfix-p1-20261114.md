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

## Final validation — PASS

Tested source head: **`a79d200ba28f7c85614342a58d497d2dfc8a40e5`**. Every run below concluded `success`; all Cargo operations ran on GitHub Actions standard Ubuntu runners with `clean_target=true` for package runs. No Cargo/Go/Docker ran locally.

- Frontend clippy + tests: https://github.com/auenkr/cadence-rust/actions/runs/43596453003 — 261 unit tests passed.
- service-core clippy + tests: https://github.com/auenkr/cadence-rust/actions/runs/43596455860 — success.
- matching clippy + tests (live gRPC adapters): https://github.com/auenkr/cadence-rust/actions/runs/43596458346 — 1001 unit tests passed (2 ignored), plus integration tests passed.
- persistence clippy + tests (timestamp boundary consumers): https://github.com/auenkr/cadence-rust/actions/runs/43596461045 — 434 unit tests passed (14 ignored), plus integration tests passed.
- MySQL Rust Frontend mixed gate, Kafka enabled: https://github.com/auenkr/cadence-rust/actions/runs/43596463901 — **success**, log contains **`GATE SUMMARY failed: []`**. Existing workflow runs Rust Frontend + Rust Matching with original Go History/Worker and SDK/CLI compatibility, restart/API/IPv6 probes.
- Mixed binary SHA256: **`8c3eb2651280fdedf192b2cb4f9409548c54af104d962d46fd52bf5b2eccd996`**, built in `/tmp/cadence-cargo-target-fe-43596463901`.

Durable evidence: `memory/evidence/frontend-solfix-p1-20261114/` contains official run identities/conclusions, binary SHA256, mixed gate log, and package test log excerpts. The final documentation commit does not change tested Rust sources.

### Finding-to-commit/test map

All six implementation fixes originate in `ab8e495ca7d366ddb0903414c6a9244ff1dce2d1`; follow-ups are listed individually:

| Finding | Follow-up commits | Passing live-path regression |
|---|---|---|
| SOL P1 #1 | bb31de525 (CI formatting) | `grpc::p1_handler_tests::async_starts_publish_through_original_hook_with_context_and_errors` |
| SOL P1 #2 | 458304e0b (standalone fallback), e1fcd0637 (format), a79d200ba (test fixture) | `grpc::p1_handler_tests::by_id_operations_reach_history_hook_without_local_activity` |
| SOL P1 #3 | bb31de525 (CI formatting) | `grpc_admin::domain_dlq_handler_tests::domain_dlq_grpc_and_thrift_use_persistence_and_propagate_merge_errors` |
| SOL P1 #4 | bb31de525 (CI formatting) | `grpc::p1_handler_tests::release_semaphore_decision_reaches_completion_hook_and_roundtrips_codecs` |
| SOL P1 #5 | 458304e0b (Thrift test method), a58973775 (local visibility query boundary), a79d200ba (timeout test unit) | `workflow_visibility::timestamp_handler_tests::thrift_list_handler_returns_exact_nanosecond_timestamps` |
| Opus LOW-1 | e1fcd0637 (box original status; clippy) | `handler_admin_history::history_error_passthrough_tests::admin_history_forwarding_preserves_original_status_details_and_metadata` |

### Failed/superseded iterations

- Initial Frontend run 43595254822 reached clippy and rejected large AdminError; fixed by boxing the original status in e1fcd0637.
- 43595633694 / 43595636455 / 43595639290 and 43595695814 / 43595698415 / 43595701166 were superseded/cancelled while iterating.
- a58973775: Frontend 43595887747 and core 43595890073 passed. Matching 43595892668 rejected a redundant default struct update; persistence 43595885189 exposed an existing timeout test consuming the now-nanosecond API timestamp as milliseconds. Both test fixtures corrected in a79d200ba, final runs above passed.
- Superseded mixed runs 43595703859 / 43595894997 were cancelled by branch concurrency when the final gate was dispatched.

Limits: the existing mixed gate is broad compatibility evidence, not a newly added restart-recovery or nonempty-domain-DLQ SDK scenario dedicated to each finding. New focused tests exercise original operation dispatch/context/error behavior, paginated nonempty domain-DLQ read and purge/merge error forwarding, release-decision codecs and exact visibility wire timestamps. No service-completion claim, merge or branch deletion.
