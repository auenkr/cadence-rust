# history-decisiongaps2 (2026-11-11)

Branch `history-decisiongaps2` (from origin/master).

## Done
1. Local activity dispatch: `request_local_dispatch` added to `cadence_api::tasks::ScheduleActivityTaskDecisionAttributes` (thrift field 90, gRPC convert both ways; the unimplemented rejection was removed). `EngineOps::schedule_activity` returns `ActivityLocalDispatchInfo` when `enable_activity_local_dispatch_by_domain && request_local_dispatch` and skips the activity transfer task. `start_local_activity` records ActivityTaskStarted, the token and timestamps are populated by the existing TaskHandler path, and `completed_run` fills `activities_to_dispatch_locally`.
   Note: `rust/tools/generate-grpc-decisions.py` still emits the old rejection; the generated `decisions.rs` was edited by hand.
2. Advanced-visibility search-attr task: `add_binary_checksum_if_not_exists` calls `gen_search_attr_task` when a new checksum is recorded and `core.advanced_visibility_writing()` is true.
3. Client version: `CLIENT_HEADERS` tokio task-local (`ClientHeaders`: cadence-client-library-version / -feature-version / -implementation) set in grpc.rs and tchannel.rs around RespondDecisionTaskCompleted. `completed_run` stores them on execution info (`set_client_info`) and uses `supports_consistent_query` (Go version checker, uber-go/uber-java >= 1.5.0) for the buffered-query version check (fail-open when headers are absent).
4. Unit tests in engine_decision_ops.rs: local dispatch, no dispatch without flag, binary-checksum search-attr task, client info, consistent-query checker.

## CI
- rust-ci cadence-history, head f36fff082: success https://github.com/auenkr/cadence-rust/actions/runs/43058858963
- history-mixed-gate mysql/grpc/two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43060154371 (see result below)

## Remaining
- Thrift RespondDecisionTaskCompleted `activitiesToDispatchLocally` in the response is already mapped; no end-to-end local-activity SDK run was done.
- Fix generate-grpc-decisions.py to match the hand edit.

## Gate result and root cause (appended)
- mysql gate run 43060154371 on f36fff08: FAILED (sdk-complete-echo/describe-closed/complete-child: activity START_TO_CLOSE). Root cause: Rust frontend/API dropped `activitiesToDispatchLocally` from the history response, so locally dispatched (already started) activities never reached the SDK.
- Fix (commits 9cbd0ce, f899dad): cadence-api `ActivityLocalDispatchInfo` + response field; thrift/proto/JSON conversions; frontend workflow_matching.rs and cadence-matching grpc.rs pass-through; persistence in-memory response fills the field.
- rust-ci run 43063568361 on f899dad8dbc959d20f06f0d5dbd8e1bb0532c583: success (full workspace).
- mysql gate run 43065856244 (https://github.com/auenkr/cadence-rust/actions/runs/43065856244) on f899dad8: success.
- Remaining: rust/tools/generate-grpc-decisions.py still emits the old request_local_dispatch rejection (decisions.rs hand-edited).
