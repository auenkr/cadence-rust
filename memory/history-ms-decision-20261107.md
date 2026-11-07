# history-ms-decision (2026-11-07)

Branch `history-ms-decision`. Files: `rust/crates/cadence-history/src/execution/mutable_state_methods_decision.rs`, `mutable_state_methods_started.rs` (registered in `execution/mod.rs`).
CI (rust-ci, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/42338026648 — success on code head (commit "Fix started-event field types").

## Ported (direct port on `TypedMutableState`, Go sentinels -23/-24; explicit `DecisionEnv` replaces timeSource/config)
Decision: get_decision_info, update_decision, delete_decision, fail_decision, has_processed_or_pending_decision, get_previous_started_event_id, get_pending_decision, get_in_flight_decision, get_decision_schedule_to_start_timeout, is_sticky_task_list_enabled, create_transient_decision_events, add_first_decision_task_scheduled, add_decision_task_scheduled_event(+_as_heartbeat), replicate_decision_task_scheduled/transient_scheduled/started/completed/timed_out/failed, add_decision_task_started/completed/timed_out/schedule_to_start_timeout/reset_timeout/failed_event, check_resettable, add_binary_checksum_if_not_exists.
Started: replicate_workflow_execution_started_event.

## Gaps
- Task generation (decision schedule/start, delayed decision, workflow start, record-started, search-attr) not wired; `DecisionEnv.needs_search_attr_tasks` flag only.
- `add_workflow_execution_started_event` and `..._for_continue_as_new` NOT ported (need HistoryBuilder request plumbing); started replicate lacks events-cache write, auto_reset_points rollover, active_cluster_selection_policy, insert Start workflow request.
- `check_resettable` lacks semaphore checks; auto_reset_points blob codec not wired (kept in `DecisionEnv.auto_reset_points`); reset WorkflowRequest stores only request id.
- No logging/metrics (critical decision attempts). Existing trait-based `mutable_state_decision_task_manager.rs` (0-based sentinels) left untouched.
- Sticky schedule-to-start timeout unit assumed nanos.
- No unit tests added.
