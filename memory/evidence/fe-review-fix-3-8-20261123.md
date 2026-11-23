# FE review fixes 3-8 (branch fe-review-fix-3-8), 2026-11-23

Source: `fe-final-review-sol-20261123.md` / `fe-final-review-opus-20261123.md` agreed list. No Cargo/Go/Docker was run locally.

| # | Item | Status | Commit subject / test |
| - | ---- | ------ | --------------------- |
| 3 | History retry policy | Done | "Frontend History retry: port FrontendRetry predicate..." Tests in `workflow_backend::history_retry_tests` (predicate, 50ms/10s/30s timings, owner-from-error redirect, WorkflowID rate-limit busy not retried, caller deadline). `PollMutableState` now goes through `retry_history_call`. |
| 4 | Nested calls use inner handler | Done | `WorkflowApi::call_inner` + `Handler::dispatch_inner_with_metadata` (skips auth, redirect, rate limit, version check, metering). schedule_flow and DeleteDomain list check use it. Test: `schedule_flow` CapturingApi records `OUTER:` for `call`; existing test asserts no outer calls. |
| 5 | Async Kafka TLS serverName | Done | Verifier wrapper verifies cert against configured name (rskafka has no SNI override, so SNI still uses the dialed host). Test `server_name_override_is_applied_to_verification`. |
| 6 | Outbound gRPC/TChannel selector | REMAINING | Needs gRPC clients plus thrift<->proto mapping for ~26 History/Matching methods in workflow_backend/workflow_matching. Not started; too large to do safely in this task. |
| 7 | RemoveTask no type | Done | `Request is nil.` checked before shard validation. Test `remove_task_without_type_returns_request_not_set_before_shard_check`. |
| 8 | Worker-poll stage-ordered wait | Done | `TokenBucketPolicy::wait` consumes task-list, domain, global stages in order. Test `wait_consumes_stages_in_order_and_keeps_earlier_tokens_on_timeout`. |

## CI evidence (head 396cb19575de7998b61f8e12e6e133c8d547ec1b)
- rust-ci `package=cadence-frontend`: https://github.com/auenkr/cadence-rust/actions/runs/45177380947 success
- rust-ci `package=cadence-service-core` (earlier SHA 4376b608 + fmt fix; retry fix only touched frontend): https://github.com/auenkr/cadence-rust/actions/runs/45176283873 success
- frontend-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/45177406490 success, `GATE SUMMARY failed: []`
- history-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/45177414273 success

Earlier failed runs: 45176264206 (2 retry tests used std Instant under paused tokio time; fixed in 396cb195).
