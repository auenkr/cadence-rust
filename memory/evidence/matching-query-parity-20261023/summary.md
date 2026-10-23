# Matching query parity (A1-A4), 2026-10-23
Scope `-p cadence-server` (host overloaded; single cargo job rule).
Exit codes: check 0, clippy `-D warnings --all-targets` 0, test 0 (451 passed, 0 failed).
- A1: parent ServiceBusy/ResourceExhausted on query forward -> keep waiting locally (`query_forward_slow_down`; test `matching_query_forward_slow_down_only_for_service_busy`).
- A2: standby PollForQuery forwards poll to remote/service parent (`TaskDispatcher::forward_query_poll`). No dedicated test (needs 2-partition standby harness); in-process dispatcher parent not forwarded to avoid claiming backlog. UNVERIFIED end to end.
- A3: invalid-isolation-group counter only when reader isolation enabled (test `matching_reader_isolation_enabled_only_with_groups`). Parent-path check not added.
- A4: removed poll-side emission; reader Err branch counts + eprintln except "task not started" (test `matching_reader_counts_unknown_dispatch_errors_but_not_not_started`).
