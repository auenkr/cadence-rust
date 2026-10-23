# Matching PARTIAL upgrade B (map rows 9–16) — 2026-10-24

Source map: `memory/matching-go-test-map-20261023.md` (not edited). Branch `matching-partial-upgrade-b-20261024` from origin/master `bafd65a77`.

Row selection: PARTIAL rows of the per-test section, in document order, excluding engine_test / engine_integration_test / matcher_test / forwarder_test / task_completer_test / membership tests. The go/common section is file-level and was not in the per-test list, so it was not counted. Filtered list rows 9–16:

| # | Map row (Go file) |
|---|---|
| 9 | handler/handler_test.go TestAddActivityTask / TestAddDecisionTask / TestPollForActivityTask / TestPollForDecisionTask |
| 10 | liveness/liveness_test.go TestEventLoop_Noop |
| 11 | liveness/liveness_test.go TestEventLoop_Shutdown |
| 12 | poller/manager_test.go TestManager_HistoryCallback |
| 13 | poller/manager_test.go TestManager_CancelPoll |
| 14 | poller/manager_test.go TestManager_HasPollerFromIsolationGroupAfter / TestManager_HasPollerAfter |
| 15 | poller/manager_test.go TestManager_GetCount / TestManager_ListInfo / TestManager_GetCountByIsolationGroup |
| 16 | semaphore/semaphore_manager_test.go TestStartBuildsTheFreeSetAndTheHeldIndexAcrossPages |

## Table

Status: old → new. Run URL: see the final line of this file.

| Map row | Go test (line) | Rust test(s) | Old → new | Notes |
|---|---|---|---|---|
| 9 | handler_test TestAddActivityTask (167) | `tests/matching_partial_upgrade_b.rs::go_handler_add_activity_task_rate_limited_returns_service_busy` | PARTIAL → PARTIAL | Rate-limit ported (denied add persists nothing). Not ported: success PartitionConfig echo (Go engine mock) and "AddActivityTask failed" → InternalServiceError(engine message) (no engine seam). |
| 9 | handler_test TestAddDecisionTask (235) | `go_handler_add_decision_task_rate_limited_returns_service_busy` | (same) | Same gaps as above, decision path. |
| 9 | handler_test TestPollForActivityTask (303) | `go_handler_poll_for_activity_task_rate_limited_returns_service_busy`, `go_handler_long_polls_without_deadline_return_context_timeout_not_set`, `go_handler_poll_for_activity_task_returns_added_task_token` (#[ignore]) | (same) | Success case is an ignored gap: after a durable add persists one row, a 2s poll returns an empty token (root cause not isolated). Not ported: "PollForActivityTask failed" (no engine seam). |
| 9 | handler_test TestPollForDecisionTask (380) | `go_handler_poll_for_decision_task_rate_limited_returns_service_busy`, `go_handler_long_polls_without_deadline_return_context_timeout_not_set` | (same) | Decision success and engine-failure not ported (as above). |
| 10 | liveness_test TestEventLoop_Noop (89) | `matching_registry.rs::liveness_event_loop_noop_keeps_marked_owner_loaded` (unchanged) | PARTIAL → PARTIAL | Analogue: Rust drives `sweep_idle`; production has a due-based idle loop, not a ttl/2 ticker. Existing assertions reviewed, no edit. |
| 11 | liveness_test TestEventLoop_Shutdown (105) | `matching_registry.rs::liveness_event_loop_shutdown_unloads_owner_at_ttl` (strengthened) | PARTIAL → PARTIAL | Added: owner still loaded at TTL−1 ns, unloaded at TTL, later sweep is a no-op (Go CAS once). Ticker lag [TTL,1.5TTL] still unasserted. |
| 12 | poller TestManager_HistoryCallback (38) | `tests/matching_partial_upgrade_b.rs::poller_start_without_identity_records_no_history` (new); `poller_history_callback_fires_on_start_and_end_with_identity` (#[ignore]) | PARTIAL → PARTIAL | Callback counter absent from `cadence_common::poller::Manager` (gap marked). |
| 13 | poller TestManager_CancelPoll (58) | `poller_cancel_invokes_registered_callback_per_poll` (new); `poller_cancel_reports_whether_the_poll_is_known` (#[ignore]) | PARTIAL → PARTIAL | Callback counts for happy/repeated/unknown ported. `cancel` returns `()`, Go returns bool (gap marked). |
| 14 | poller TestManager_HasPollerFromIsolationGroupAfter (103) and TestManager_HasPollerAfter (177) | `poller_has_group_after_matches_go_cases` (6 cases), `poller_has_after_matches_go_cases` (5 cases); registry `has_poller_after_reports_outstanding_or_finished_pollers` (added finished-at-query-time asserts) | PARTIAL → PORTED | Same timestamps as Go; boundary cases use strict `after`. Previously weak (only i64::MIN). |
| 15 | poller TestManager_GetCount (240) | `poller_count_keeps_expired_entries_like_go_size` | PARTIAL → PORTED | Go `Size` does not evict; Rust `count()` matches. |
| 15 | poller TestManager_ListInfo (257) | `poller_list_info_matches_go_cases` | (same) | Newest-first order, empty-identity exclusion, no-pollerID case. |
| 15 | poller TestManager_GetCountByIsolationGroup (332) | `poller_count_by_group_matches_go_cases` | (same) | Go zero `after` mapped to `i64::MIN`; "some expired" uses `START+1m-1ns`. |
| 16 | semaphore_manager_test TestStartBuildsTheFreeSetAndTheHeldIndexAcrossPages (180) | `matching_semaphore_parity_tests.rs::matching_semaphore_start_builds_free_and_held_indexes` (strengthened: free set = {1,3}) | PARTIAL → PARTIAL | Page tokens still absent from `SemaphorePersistence::scan` (existing ignored gap `matching_semaphore_scan_threads_page_tokens`). |

Totals for the 8 rows: 2 PARTIAL→PORTED (rows 14, 15), 6 remain PARTIAL.

New tests: 15 functions in `tests/matching_partial_upgrade_b.rs` (12 active, 3 `#[ignore]` gap markers: poller callback, CancelPoll bool, and the activity poll success token). In-file edits: 3 existing tests strengthened (`liveness_event_loop_shutdown_unloads_owner_at_ttl`, `has_poller_after_reports_outstanding_or_finished_pollers`, `matching_semaphore_start_builds_free_and_held_indexes`). No production code changed.

## Verification

Not compiled or run locally (per instruction). Verification is the GitHub Actions run below (`rust-ci.yml`, package `cadence-matching`, clippy `-D warnings` then `cargo test`).

Run: _pending_
