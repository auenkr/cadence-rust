# History gap: timer mutable state for TimerActiveDepsImpl (branch history-gap-timermut, 2026-11-07)

Backed by new `wiring/assembly/engine_timer.rs` on `EngineMutableState` (not TypedMutableState):
- Inner now loads/persists user timers (`snapshot.timer_infos`, upsert/delete in `engine_close.rs`; `TimerInfo.task_id` = Go TaskStatus). Close transaction now also runs `create_next_user_timer` (Go `createNextUserTimer`, timer task type 2).
- Real: user/activity timer sequences (Go timer_sequence.go ordering), user_timer_info, resurrected_timers (history scan), delete_user_timer, add_timer_fired_event, update_activity (timer status), retry_activity, activity/decision(start-to-close, schedule-to-start) timed-out events, terminate_decision_attempts_exceeded, search-attr transfer task (type 8), workflow retry backoff, cron backoff (no jitter), timeout_workflow, activity_scheduled_domain (from scheduled event), partition_config, schedule_to_start_timeout/timer_task_status.
- Still TODO(link): `retry_workflow`, `update_workflow_execution_with_new_as_active` (continue-as-new run creation), activity `last_heartbeat_timeout_visibility_in_seconds` (not in persistence ActivityInfo), cron jitter, retry_activity failure options. Adapter still not wired into launcher until retry_workflow lands.
- No unit tests; validated by CI only.

## Evidence
rust-ci package=cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/42341390829 code head 8ac2743dbdce4847be68e855ec80ed7a33ff0dd4 conclusion: success (clippy + tests).
