# Matching Go test function map (resolves section B of matching-v6-ignored-tests-audit-20261027.md)

Read-only review. No source changes, nothing run locally (Cargo/Go only on CI). This does NOT claim whole-service completion.
Mapping is by behavior, not file name. Classes: COVERED, EQUIV (fixture/equivalent source only, row-level not mirrored), TEST-GAP (behavior exists, Go-equivalent test absent), MOCK (Go-specific mock/suite shape, no Rust analog needed).
Rust paths are under `rust/crates/cadence-matching/src` unless noted.

## tasklist/isolation_balancer_test.go
- `TestAdjustWritePartitions` (29 cases): COVERED. `matching_scaler_go_tests.rs` `matching_isolation_balancer_*`, one-for-one, plus an extra `..._ensure_minimum_single_pass_keeps_donor`.

## tasklist/adaptive_scaler_test.go
- `TestAdaptiveScalerRun` (17 cases): COVERED, with a caveat. `matching_adaptive_scaler_*` drive `Scaler::tick*` and `adjust_read_partitions_async` through the test helper `run_scaler_cycle`, a test-side replica of Go `run()` metric collection. They do not drive production `scaler_update` / `describe_scaled_partition` (`matching_registry.rs` ~1815 / ~1732).
- `TestAdaptiveScalerLifecycle`: COVERED. `matching_adaptive_scaler_lifecycle_start_stop_is_idempotent`.
- GAP (TEST-GAP): there is no per-case test of the production collector (describe error resets sustains, isolation aggregation, empty_check). Only `scaler_describe_panic_is_a_collection_failure_and_the_tick_returns` (~3476) and the e2e `matching_rpc_adaptive_scaler_upscales_write_partitions_on_load` (`matching_rpc_tests.rs:2947`) exist.

## tasklist/task_completer_test.go (`TestCompleteTaskIfStarted`, 16 cases)
- 14 of 16: COVERED by `matching_standby.rs` `go_*` tests (domain active, decision/activity not started, missing execution <24h / >24h, workflow closed, decision no pending / schedule less / equal-and-started, activity no match / started, no execution info, unsupported task type), plus `task_started` pure tests.
- "error - could not get domain by ID from cache" and "error - could not fetch workflow execution from history service": TEST-GAP. The retry loop exists in `checker`, but there is no test for retry exhaustion returning the original error. Related partial tests: `standby_cancelled_checker_counts_one_completion_failure`, `standby_completion_failure_excludes_not_started_but_counts_young_missing`. The Rust retry constants (1s/10s/300s) were not compared with Go `CreateTaskCompleterRetryPolicy`. Verify before claiming parity.
- The Go `retryPolicyMaxAttempts=3` attempt-count policy is a MOCK seam (the production Go policy is time-based).

## tasklist/forwarder_test.go (`ForwarderTestSuite`)
- TestForwardTaskError, TestForwardDecisionTask, TestForwardActivityTask, TestForwardTaskRateExceeded: COVERED (`matching_dispatch.rs` `matching_forward_*`).
- TestForwardQueryTaskError, TestForwardQueryTask, TestForwardQueryTaskRateNotEnforced: COVERED (`matching_query_forward_family_tests.rs`; ErrTaskListKind is unreachable from OfferQuery, which the test comment notes).
- TestForwardPollError, TestForwardPollForDecision, TestForwardPollForActivity: COVERED (`matching_forward_poll_*`).
- TestMaxOutstandingConcurrency (contention, token_leak) and TestMaxOutstandingConfigUpdate: COVERED (`matching_forward.rs` `matching_forward_max_outstanding_concurrency`, `..._config_update_resizes_generation`, `..._tokens_replace_generation_without_inflation`).

## tasklist/task_reader_test.go
- `TestDispatchSingleTaskFromBuffer` (11): EQUIV/COVERED by behavior in `matching_reader.rs` (throttled retry, unknown vs not-started counting, dispatch timeout keeps row unacked, known/default isolation group, expired rows, stop). The `(breakDispatch, breakRetries)` return tuple is a MOCK shape.
- `TestGetDispatchTimeout` (5): COVERED. `cadence-common/src/task_isolation.rs` `original_go_isolation_policy_fixtures`.
- `TestTaskPump` (2): COVERED. `matching_reader_pump_falls_back_to_default_for_unknown_isolation_group`, `matching_reader_dispatches_new_task_to_waiting_poller_then_acks`.
- `TestGetTasksPumpConditionFailedUnloadsTaskList`: COVERED. `matching_registry.rs` `ack_condition_failure_unloads_owner_without_stop_signal`.
- `TestTaskReaderBatchSizeValidation` (4): COVERED. `matching_reader_batch_size_fallback_sizes_group_buffer_like_go`.

## tasklist/task_list_limiter_test.go (`TestTaskListLimiter`, 12 cases)
- EQUIV. `tests/fixtures/matching-rate.json` (11 cases) via `cadence-common/src/matching_rate.rs` `matching_rate_original_go_report_sequences`, plus `matching_limiter.rs` `report_divides_by_live_partition_count`, `report_limit_returns_go_rate_gauge_updates`, `default_limit_and_report_divide_by_partitions`. The 12 Go rows are not individually named or asserted (11 fixture cases vs 12 Go rows). TEST-GAP at row level.

## liveness/liveness_test.go (in `go/service/matching/liveness/`)
- TestIsAlive_No/Yes, TestMarkAlive_Noop/Updated: COVERED by fixture `cadence-common/src/liveness.rs` `original_go_liveness_fixtures` (`tests/fixtures/matching-liveness.json`). The Rust type is pull-based, with explicit `now`. The mocked clock is a MOCK shape.
- TestEventLoop_Noop / TestEventLoop_Shutdown: COVERED. `matching_registry.rs` `liveness_event_loop_noop_keeps_marked_owner_loaded`, `liveness_event_loop_shutdown_unloads_owner_at_ttl`, `liveness_idle_pump_unloads_owner_between_ttl_and_one_and_a_half_ttl`.

## handler/membership_test.go
- `TestGetTaskListManager_OwnerShip` (not-owned, LookupError, WhoAmIError): PARTIAL. Related tests: `matching_runtime.rs` `installed_shard_ownership_check_matches_notfound_shutdown_and_transient_errors`, `matching_registry.rs` `matching_membership_guards_creation_and_unloads_moved_owners`, `matching_membership.rs` `empty_ring_ownership_errors_wrap_lookup_failure_like_go_engine`. TEST-GAP: no direct LookupError/WhoAmIError rows were confirmed.
- TestMembershipSubscriptionRecoversAfterPanic: COVERED. `matching_rpc_tests.rs` `matching_rpc_membership_loop_panic_ends_loop_without_error`.
- TestShutDownNonOwnedSemaphoreManagersReportsAPanic: COVERED. `matching_rpc_membership_semaphore_stop_panic_is_contained`.
- TestGetTasklistManagerShutdownScenario: COVERED (`installed_shard_ownership_check…` shutdown gives Aborted; `get_or_create_semaphore_manager_refused_while_engine_shuts_down`).
- TestShutDownNonOwnedSemaphoreManagers (owned bucket stays loaded, moved one stops): TEST-GAP. The existing Rust test unloads both. Behavior is in `grpc_matching.rs:1237` `maintain_membership_until`.
- TestShutDownNonOwnedSemaphoreManagersSurfacesALookupFailure (lookup error unloads nothing): TEST-GAP. The implementation logs and breaks without unloading, but no test pins it.
- TestSubscriptionAndShutdown, TestSubscriptionAndErrorReturned, TestSubscribeToMembershipChangesQuitsIfSubscribeFails: MOCK. Rust `membership.subscribe()` is an infallible watch channel with no domain-change-callback unregister. The WhoAmI failure during event handling is only logged and untested.

## handler/engine_integration_test.go (`matchingEngineSuite`)
- Empty and short-context polls (activity/decision): COVERED (`matching_rpc_empty_polls_reserve_tailroom_and_read_live_expiration`, `tests/matching_longpoll_expiry_parity.rs`).
- PollActivity/DecisionWithExpiredContext: COVERED (`matching_long_polls_with_nearly_expired_deadline_return_internal_like_go`, `..._reject_missing_transport_deadlines_with_bad_request`).
- TestOnlyUnloadMatchingInstance: COVERED (`stale_teardown_cannot_remove_replacement_owner`).
- TestQueryWorkflow: COVERED (`tests/matching_engine_go_tests_b.rs` `query_workflow_*`, `matching_rpc_delivers_query_to_decision_poller`).
- TestMatchingPollMetricOracle: COVERED (`matching_dispatch_records_poll_events_from_claimed_task_and_real_poll_context`).
- TestAddTaskTrafficMetricOracle: COVERED at unit level. `cadence-service-core/src/matching_metrics.rs` has a unit test around line 1795 that records decision, sticky and forwarded `add_task_request` calls and asserts exactly one `cadence_tasklist_request` AddTask counter, with tags including host `oracle-host`. Sticky and forwarded are excluded. No handler-level test confirms the call site fires only after domain resolution and before manager lookup (minor TEST-GAP).
- TestAddActivityTasks / TestAddDecisionTasks (+Forwarded): COVERED (`matching_rpc_forwarded_add_never_persists_and_reports_remote_failure`, `matching_dispatch_*` forward tests).
- TestAddAndPoll* (+Isolation): COVERED (`go_engine_add_then_poll_decision_tasks_delivers_each_task_and_drains_store`, `matching_rpc_isolation_*`, `matching_reader_isolation_*`).
- TestSyncMatch* (+Isolation): COVERED (`matching_sync.rs` `sync_match_*`, `matching_dispatch.rs` sync match tests).
- TestAddTaskAfterStartFailure: COVERED (`matching_dispatch_transient_failure_rewrites_before_successful_redelivery`, `matching_dispatch_typed_terminal_error_drops_without_rewrite`, persistence `matching_queue_failure_rewrite_is_durable_before_ack`).
- TestUnload{Activity,Decision}TasklistOnIsolationConfigChange: COVERED (`matching_rpc_isolation_toggle_returns_empty_poll_and_replaces_owner`, `..._stops_owner_before_admitting_add`).
- TestDrain{Activity,Decision}BacklogNoPollersIsolationGroup: COVERED (`matching_rpc_isolation_policy_rechecks_drains_and_poller_recency`).
- TestMultipleEngines{Activities,Decisions}RangeStealing: EQUIV. Related tests: `replacement_can_load_before_old_cleanup_and_keeps_its_lease` and persistence `matching_queue_range_id_renews_per_block_and_fences_stale_writer`. No named two-engine scenario test was confirmed (TEST-GAP).
- TestConcurrentAddAndPoll* (Activities, ActivitiesWithZeroDispatch, Decisions, 3 Isolation variants): TEST-GAP (unconfirmed). No Rust test mirroring the Go scenario (1000 tasks, concurrent adders and pollers) was found. Only partial coverage from `concurrent_load_acquires_one_lease_and_isolates_task_types` and the semaphore concurrency tests.
- TestConfigDefaultHostName: EQUIV. `matching_host_name()` OnceLock at `grpc_matching.rs:30`, no direct test confirmed (minor TEST-GAP).

## Correction to the v6 audit
- A3's claim that `go_history_tchannel_wire_oracle_consumes_reviewed_fixture` no longer exists is stale for the current tree. It is an active `#[test]` at `matching_history_tchannel.rs:2009`, and CI regenerates the fixture from the pinned Go ThriftRW generator and compares it. A1/A2/C were not re-verified.

## Confirmed gaps summary
No unimplemented behavior was found. All gaps are test coverage. Behavior should still be verified on CI before any parity claim.
1. Task completer retry exhaustion rows (domain cache error, history fetch error): `matching_standby.rs` `checker`. Also compare the retry constants with Go.
2. Membership: owned-bucket-stays-loaded and lookup-failure-unloads-nothing (`grpc_matching.rs:1237`); OwnerShip LookupError/WhoAmIError rows.
3. Production scaler collector (`matching_registry.rs` `scaler_update` / `describe_scaled_partition`) has no per-case tests.
4. Limiter: 12 Go rows are not individually asserted (fixture has 11).
5. Engine-level scenarios: TestConcurrentAddAndPoll* (all variants), TestMultipleEngines*RangeStealing, TestConfigDefaultHostName.

MOCK shapes (not gaps): gomock/testify suites, mocked clocks, `retryPolicyMaxAttempts`, the `dispatchSingleTaskFromBuffer` return tuple, and the fallible membership subscribe / domain-callback unregister.
