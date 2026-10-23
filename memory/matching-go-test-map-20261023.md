# Matching Go test → Rust test map (authoritative, 2026-10-23)

Branch `matching-test-inventory-final-20261023` (from origin/master `d03e99474`). Read-only on code; nothing was compiled or run here.
This file replaces the per-test tables in `matching-go-test-inventory-20261023.md`, `-port2-`, `-port3-`, `matching-tlm-test-inventory-20261023.md`,
`matching-test-parity-gaps-20261023.md`, and the handler/persistence inventories on their branches. Where they disagree, this file wins.

## Method and verification level (read first)

- Go side: every `func Test*` and every testify suite method (`func (s *X) Test*`) under `go/service/matching/**/*_test.go` was listed; `t.Run`/table rows were counted heuristically (shown as "~N sub").
  Suite runners (`TestMatcherSuite`, `TestForwarderSuite`, `TestHandlerSuite`, `TestMatchingEngineSuite`, `TestLivenessSuite`) are excluded from counts (they run the methods).
- Rust side: all `#[test]`/`#[tokio::test]` function names were extracted with `git grep` from origin/master and from the branches below. **Every Rust test name cited here was grepped and exists** (checked by script), with these exceptions found in the old inventories, corrected here:
  - `matching_query_without_poller_waits_for_deadline` does not exist; real name is `matching_rpc_query_without_poller_waits_for_deadline`.
  - `matching_rpc_partition_config_rpcs_reject_non_normal_kinds` does not exist in any ref; the real test is `go_handler_refresh_partition_config_rejects_missing_tasklist_and_non_normal_kinds` (branch only).
  - `sync_match_invalid_isolation_group_poll_gets_no_task` (PG-3) and `sync_match_non_blocking_poll_with_no_tasks_returns_immediately` (PG-2) are described in the port3 inventory as `#[ignore]`d tests but **exist on no branch or master**. The port3 inventory overstates; those two Go tests are MISSING here.
  - the port2 `#[ignore]` on `matching_forward_activity_task_fields_match_go_forwarder` is gone on master; the test asserts `request.source_domain_id == source_domain` and `domain_id == list_domain`, so PG-1 looks fixed (assertion read, test not run).
- "Assertion read" level: I read the body of `sync_match_local_timeout_reports_no_match_when_poller_finishes_late`, `liveness_event_loop_shutdown_unloads_owner_at_ttl`, `matching_sync_match_failure_reports_busy_without_matching`, `matching_forward_config_update_resizes_generation`, `has_poller_after_reports_outstanding_or_finished_pollers`, `matching_forward_activity_task_fields_match_go_forwarder`, plus the ignore markers of every `#[ignore]` in cadence-matching and the semaphore/isolation/tlm evidence notes. Other mappings are by name + the worker's evidence notes; they were not re-read line by line. A COVERED/PORTED status therefore means "a test with this scenario exists and the owning inventory/evidence says it passed in CI or locally", not an independent audit.
- Tests run status: pass evidence is in the evidence directories / CI runs cited by the old inventories (e.g. rust-ci runs 40015060031, 40015106530, 40015151528, 40014990614, 40015006344). I did not re-run anything.
- Weak tests found while reading: `has_poller_after_reports_outstanding_or_finished_pollers` only queries with `i64::MIN`, so Go's "poller finished before the query time" branch is not tested (map as PORTED, but weaker than Go). `liveness_event_loop_*` drive `sweep_idle` directly, not a TTL/2 ticker (Rust has no liveness daemon), so they are analogues, counted PARTIAL.
- Refs used (not merged to master unless stated): `origin/master` + `origin/matching-tlm-batchread-20261023` (tlm 14,15,16,25 and queue tests), `-tlm-gc-partcfg-` (tlm 17, 20), `-tlm-isogroup-` (tlm 10, 12), `-handler-tests-`, `-engine-integration-tests-` (contains handler-tests commits + 2 engine tests), `-persistence-tests-`. `matching-isolation-scaler-tests-20261023` and `matching-semaphore-tests-20261023` are already in master (`matching_scaler_go_tests.rs`, `matching_semaphore_parity_tests.rs`). `matching-dyncfg-defaults` and `matching-metrics-blocked` add no Go-test ports. Mark **(B)** = exists only on a branch, not on master.

Status key: PORTED = new/near-1:1 Rust test of the Go scenario; COVERED = an existing Rust test (often a Go-fixture test) asserts the same behavior; PARTIAL = related Rust test(s) exist, Go scenario/subtests not all asserted; CANNOT-PORT(reason) = no Rust unit-level API/seam (this is an API-shape reason, **not** proof of behavioral parity); MISSING = no Rust test (or only an `#[ignore]` gap marker).

## Summary: go/service/matching (260 Go test functions, 24 files)

| Go file | funcs | PORTED | COVERED | PARTIAL | CANNOT-PORT | MISSING |
|---|---|---|---|---|---|---|
| config/config_test.go | 2 | 0 | 0 | 2 | 0 | 0 |
| handler/context_test.go | 3 | 0 | 0 | 1 | 2 | 0 |
| handler/engine_test.go | 22 | 0 | 8 | 14 | 0 | 0 |
| handler/engine_integration_test.go | 35 | 3 (B) | 1 | 31 | 0 | 0 |
| handler/handler_test.go | 19 | 0 | 6 | 11 | 2 | 0 |
| handler/membership_test.go | 9 | 0 | 1 | 6 | 2 | 0 |
| liveness/liveness_test.go | 6 | 0 | 4 | 2 | 0 | 0 |
| poller/manager_test.go | 7 | 0 | 0 | 7 | 0 | 0 |
| semaphore/identifier_test.go | 5 | 2 | 2 | 0 | 1 | 0 |
| semaphore/semaphore_registry_test.go | 6 | 4 | 2 | 0 | 0 | 0 |
| semaphore/semaphore_manager_test.go | 31 | 23 | 1 | 3 | 0 | 4 |
| tasklist/identifier_test.go | 4 | 0 | 3 | 0 | 1 | 0 |
| tasklist/isolation_balancer_test.go | 1 (~30 sub) | 1 | 0 | 0 | 0 | 0 |
| tasklist/task_reader_test.go | 5 | 0 | 0 | 5 | 0 | 0 |
| tasklist/adaptive_scaler_test.go | 2 | 1 | 0 | 0 | 0 | 1 |
| tasklist/task_list_registry_test.go | 3 | 0 | 1 | 2 | 0 | 0 |
| tasklist/task_test.go | 1 | 0 | 0 | 0 | 0 | 1 |
| tasklist/forwarder_test.go | 12 | 8 | 0 | 0 | 4 | 0 |
| tasklist/task_list_limiter_test.go | 1 | 0 | 0 | 1 | 0 | 0 |
| tasklist/task_list_manager_test.go | 28 | 11 (5 are B) | 1 | 14 | 2 | 0 |
| tasklist/task_completer_test.go | 1 | 0 | 1 | 0 | 0 | 0 |
| tasklist/matcher_test.go | 53 | 13 | 0 | 11 | 27 | 2 |
| tasklist/shard_processor_test.go | 3 | 3 | 0 | 0 | 0 | 0 |
| wrappers/thrift/thrift_handler_test.go | 1 (~10 sub) | 0 | 0 | 1 | 0 | 0 |
| **TOTAL** | **260** | **69** (61 on master + 8 (B)) | **31** | **111** | **41** | **8** |

69+31+111+41+8 = 260. Only 100 of 260 (PORTED+COVERED) have an equivalent Rust test; 111 are PARTIAL and 41 have no unit-level Rust seam. There is no `task_gc_test.go` in Go.

### go/common Matching-related tests (file-level, subtests not individually mapped)

See section "go/common" at the bottom. It is file-level only (no per-function counts, so it is not in the totals above); I did not read those Go tests' bodies.

## Every MISSING (8)

| Go test | What is unverified |
|---|---|
| semaphore_manager_test TestNewManagerValidatesItsParams (136) | Rust `Manager::new` is infallible, clamps TTL; no param validation. `matching_semaphore_new_manager_validates_params` is `#[ignore]`d gap marker. |
| semaphore_manager_test TestAcquireHonorsItsDeadlineWhileStarting (389) | acquire has no deadline/ctx; waits forever during startup. Ignored gap `matching_semaphore_acquire_honors_deadline_while_starting`. |
| semaphore_manager_test TestGrantStopsRetryingOnceTheDeadlineHasPassed (577) | no cancel check between grant attempts. Ignored gap `matching_semaphore_grant_stops_retrying_at_deadline`. |
| semaphore_manager_test TestGrantRejectsAnUnrecognizedWriteOutcome (699) | `GrantOutcome` exhaustive, cannot inject Unknown. Ignored `matching_semaphore_grant_rejects_unknown_write_outcome`. |
| adaptive_scaler_test TestAdaptiveScalerLifecycle (87) | Scaler has no Start/Stop handle; `#[ignore] ..._lifecycle_start_stop_is_idempotent_gap` is an empty body. Also production trailing-drain wiring gap (`matching_adaptive_scaler_production_drain_describes_trailing_partitions`, ignored) inside TestAdaptiveScalerRun (PORTED otherwise). |
| task_test TestNewInternalTask (36, ~8 sub) | no Rust test of internal-task construction (source/forward/started flags). Never assessed in any inventory. |
| matcher_test Test_pollNonBlocking_NoTasks (1224) | non-blocking poll with no tasks must return ErrNoTasks immediately; Rust poll parks. Documented as gap PG-2 but no test exists anywhere. |
| matcher_test TestIsolationPollFailure (673) | Go rejects an unknown isolation group with an error; Rust maps unknown group to default and parks (PG-3). No test exists anywhere. |

## Every PARTIAL (111) — shortest description of the unverified behavior

config_test: TestNewConfig (~66 keys; only ~7 helper getters tested, no key→field table); TestGetTasksBatchSizeValidation (raw 0/-5 pass-through not asserted).
context_test: TestHandleErrKnowErrors (only BadRequest→invalid_argument and ManagerError mapping, ~15 Go cases).
engine_test (14): ErrIfShardOwnershipLost (8 cases; only typed not-owned + policy recheck); IsExcludedFromShardDistributor; IsShuttingDown (indirect); GetTasklistsNotOwned; ShutDownTasklistsNotOwned; UpdateTaskListPartitionConfig (8 cases); RefreshTaskListPartitionConfig (6); domainChangeCallback; registerDomainFailoverCallback; RefreshWorkflowTasks (9 cases; only the thrift fixture); SetupExecutorWithEmptyConfig; ShardDistributorOnboarded (4); UnloadSemaphoreManager (explicit unload of one); StopSemaphoreManagers (shutdown-all).
engine_integration (31, all via RPC-level analogues, none a 1:1 port): poll empty/short-context ×4; OnlyUnloadMatchingInstance; PollForDecisionTasks; AddActivity/DecisionTasksForwarded; AddTaskTrafficMetricOracle; MatchingPollMetricOracle; AddAndPollActivityTasks; AddAndPoll*Isolation ×2; SyncMatch ×4; ConcurrentAddAndPoll ×6; Poll*WithExpiredContext ×2; MultipleEngines*RangeStealing ×2; AddTaskAfterStartFailure; Unload*TasklistOnIsolationConfigChange ×2; Drain*BacklogNoPollersIsolationGroup ×2; ConfigDefaultHostName (no host-name case). Unverified: the end-to-end Go scenario (real persistence + real pollers, counts, metric tag oracles).
handler_test (11): NewHandler; Start; Stop; AddActivityTask/AddDecisionTask/PollForActivityTask/PollForDecisionTask (3–4 rate-limit/error subcases each); DescribeTaskList, ListTaskListPartitions, UpdateTaskListPartitionConfig, RefreshTaskListPartitionConfig (only rate-limited/validation subcases ported, (B) handler-tests).
membership_test (6): SubscriptionAndShutdown; SubscriptionAndErrorReturned; SubscribeToMembershipChangesQuitsIfSubscribeFails; GetTasklistManagerShutdownScenario; ShutDownNonOwnedSemaphoreManagers; ...SurfacesALookupFailure.
liveness_test: TestEventLoop_Noop, TestEventLoop_Shutdown (analogues via `sweep_idle`; TTL/2 ticker lag [TTL,1.5TTL] unasserted).
poller/manager_test (all 7): HistoryCallback; CancelPoll; HasPollerFromIsolationGroupAfter (7 sub); HasPollerAfter (6 sub); GetCount; ListInfo (4 sub); GetCountByIsolationGroup (5 sub). Rust `cadence_common::poller::original_go_poller_state_snapshots` (fixture, steps start/end/cancel/advance/bulk) plausibly covers counts/info but which snapshot fields it asserts was not checked, so not upgraded to COVERED.
semaphore_manager: StartBuildsTheFreeSetAndTheHeldIndexAcrossPages (no page tokens; `scan_threads_page_tokens` ignored gap); StartSkipsRowsItCannotClassify (nil/unknown rows unrepresentable; ignored); ALateStopUnregistersOnlyItself (Manager::stop has no registry callback; ignored).
task_reader: DispatchSingleTaskFromBuffer (12 sub); GetDispatchTimeout (6); TaskPump (3); GetTasksPumpConditionFailedUnloadsTaskList; TaskReaderBatchSizeValidation (5).
task_list_registry: RegisterLookupAndUnregister (gauge, replace); Filters (all/by-domain/by-name).
task_list_limiter: TestTaskListLimiter (13 sub; only zero-burst and terminal-burst paced wait).
task_list_manager (14): DeliverBufferTasks (rate-limiter branch); DescribeTaskList (only no-status case of 15 sub); QueriesPerSecond (zero-QPS only; 42.5 case needs injectable clock); AddTaskStandby (standby forward branch); MisconfiguredZoneDoesNotBlock (dispatch-side redirect, `dispatched[""]`); TaskWriterShutdown (`taskWriter.Stop` path); TaskListManagerGetTaskBatch (second manager + row-count GC assertions approximated; (B)); TaskExpiryAndCompletion (GC counts (B) ported, expiry-while-polling half only via reader test); RefreshTaskListPartitionConfig (DB refresh and DB failure cases); Refresh/UpdateTaskListPartitionConfigConcurrency ×2 (per-manager lock order); ManagerStart_RootPartition and _NonRootPartition (direct Start partition assertions); DispatchTask ((B) success path only; MustOffer-error and standby TaskCompleter branches not).
matcher_test (11): RemoteSyncMatch, IsolationRemoteSyncMatch, RemoteSyncMatchBlocking, IsolationRemoteSyncMatchBlocking (composed `forward_backlog_task` + in-process root; Go's local-offer-then-forward step not asserted; "Blocking" is a delay variant, no TaskSource distinction); SyncMatchFailure (Go Offer returns no error; Rust returns `SlowDown`); PollersDisconnectedAfterDisconnectBlockedPollers (stop signal analogue; "parent ctx not cancelled" unasserted); Offer_NoTimeoutAsyncMatchedNoError, Offer_AsyncMatchedNoError (same test `offer_without_waiting_poller_is_not_queued`); OfferOrTimeout_AsyncMatchNotMatched; OfferQuery_ContextExpired (withdraw analogue, no ctx.Canceled); Test_poll_ContextExpired (stop analogue).
thrift_handler_test: TestThriftHandler (10 sub; no per-method routing table test).

## CANNOT-PORT (41) — behavior still unverified at unit level

context_test TestHandleErrNil, TestHandleErrUncategorizedError (no generic `handleErr`; error mapping is per-RPC). handler_test TestNewHandlerContext, TestDomainName (Rust `domain_name` returns Err; Go swallows). membership_test TestMembershipSubscriptionRecoversAfterPanic, ...ReportsAPanic (Go panic recovery). semaphore/identifier TestLogTags (zap tags). tasklist/identifier TestTaskListIDToString (no Display). task_list_manager TestTaskListString, TestGetNumPartitions (NotPanics toggle). forwarder: TestForwardQueryTaskError, TestForwardQueryTask, TestForwardQueryTaskRateNotEnforced, TestForwardPollError (query forwarding is inline in `grpc_matching.rs`; the ErrNoParent/ErrTaskListKind poll path does not exist as a unit; the closest RPC tests are `matching_rpc_child_query_forwards_to_parent_poller` and `matching_rpc_child_poll_forwards_to_parent_backlog`).
matcher_test (27): RateLimitHandling, Offer_RateLimited, RatelimitBehavior (17 sub), MustOfferRemoteRateLimit (matcher has no limiter; limiter covered in `matching_limiter.rs` only); MustOfferLocalMatch, IsolationMustOfferLocalMatch, MustOfferRemoteMatch, IsolationMustOfferRemoteMatch, MustOffer_ContextExpiredFirstAttempt, _ContextExpiredAfterFirstAttempt, _LocalMatchAfterChildCtxExpired, _LocalMatchAfterForwardError, _ContextExpiredAfterForwardError (no Rust MustOffer; retry loop in `matching_reader.rs`, reader tests at ~1532/1554/1579 cover similar semantics but are not 1:1); QueryRemoteSyncMatch, QueryRemoteSyncMatchError, OfferQuery_ForwardError, RemotePollForQuery (inline in grpc_matching.rs); RemotePoll (only the mixed gate `matching_remote_forward_reaches_go_yarpc_peer`); PollersConnectedAfterDisconnectBlockedPollersSetCancelContext; Offer_NoTimeoutSyncMatchedError (`offer` returns bool); Test_pollOrForward_* ×5 and Test_fwdrPollReqTokenC (Go-private helpers); Test_LocalSyncMatch_AutoConfigHint (no AutoConfigHint in Rust poll result).
These need either an RPC-level fixture with a mocked parent or a production-API change to assert; the old claim "no Rust analogue" does not mean Rust behavior matches Go.

## Per-test map

Format: Go test (line, ~sub) | Rust test(s) `file::name` (all verified to exist) | status | evidence.

### config/config_test.go
| TestNewConfig (45, ~66) | matching_config.rs::go_defaults_when_unset, configured_values_override_defaults, wrong_type_falls_back_to_default, activity_sync_match_wait_defaults_to_go_50ms | PARTIAL | inventory-20261023 |
| TestGetTasksBatchSizeValidation (211, ~5) | none asserting raw pass-through | PARTIAL | inventory |

### handler/context_test.go
| TestHandleErrNil (38) | none | CANNOT-PORT(no generic handleErr) | gaps-20261023 |
| TestHandleErrKnowErrors (48, ~15) | grpc.rs / grpc_matching.rs error mapping tests | PARTIAL | |
| TestHandleErrUncategorizedError (129) | none | CANNOT-PORT | |

### handler/engine_test.go (22)
| TestGetTaskListsByDomain (73, ~9) | matching_rpc_tests.rs::matching_rpc_domain_listing_uses_loaded_matching_managers, matching_domain_listing_filters_kinds_and_collapses_partition_roots | COVERED | |
| TestListTaskListPartitions (240, ~4) | matching_rpc_lists_configured_write_partitions, matching_rpc_scopes_partition_counts_to_task_list_and_type | COVERED | |
| TestCancelOutstandingPoll (353, ~3) | matching_rpc_cancel_outstanding_poll_checks_task_list_ownership | COVERED | |
| TestErrIfShardOwnershipLost (421, ~8) | matching_rpc_tests.rs::durable_queue_preserves_typed_not_owned_error_from_shutdown_policy; matching_registry.rs::shard_policy_accepts_local_assignment_and_rechecks_before_publish | PARTIAL | |
| TestIsExcludedFromShardDistributor (530, ~6) | matching_shard_routing.rs::excluded_and_missing_spectator_routes_use_live_ring_snapshot, excluded_cached_owner_skips_ring_check_but_new_owner_does_not | PARTIAL | |
| TestRespondQueryTaskCompleted (585, ~3) | matching_query.rs::offer_is_delivered_once_and_completed_by_task_id, completing_an_unknown_task_reports_not_found | COVERED | |
| TestQueryWorkflow (637, ~5) | matching_rpc_delivers_query_to_decision_poller, matching_rpc_query_without_poller_waits_for_deadline | COVERED | |
| TestWaitForQueryResult (780, ~6) | matching_query.rs::failed_query_reports_the_worker_message, internal_error_fails_the_waiting_query | COVERED | |
| TestIsShuttingDown (918) | matching_runtime.rs::shutdown_drain_provider_reads_live_unfiltered_value | PARTIAL | |
| TestGetTasklistsNotOwned (942) | matching_registry.rs::matching_membership_guards_creation_and_unloads_moved_owners | PARTIAL | |
| TestShutDownTasklistsNotOwned (978) | matching_registry.rs::assigned_eligible_owner_unloads_when_local_processor_snapshot_is_lost | PARTIAL | |
| TestUpdateTaskListPartitionConfig (1025, ~8) | matching_rpc_manual_partition_update_rejected_while_scaler_enabled, matching_rpc_native_partition_cache_updates_refreshes_and_recovers | PARTIAL | |
| TestRefreshTaskListPartitionConfig (1249, ~6) | matching_rpc_partition_update_refreshes_remote_read_owner_and_ignores_write_only_owner; matching_registry.rs::scaled_partition_from_response_reads_owner_describe_like_go | PARTIAL | |
| Test_domainChangeCallback (1434, ~2) | matching_failover_callback_excludes_sticky_activity_and_other_domains, matching_failover_callback_disconnects_sticky_decision_and_keeps_manager | PARTIAL | |
| Test_registerDomainFailoverCallback (1595, ~2) | matching_rpc_domain_failover_releases_blocked_polls | PARTIAL | |
| TestRefreshWorkflowTasks (1712, ~9) | matching_history_tchannel.rs::refresh_workflow_tasks_matches_nested_thrift_request_fixture | PARTIAL | |
| TestSetupExecutorWithEmptyConfig (1910) | matching_shard_distributor.rs::pinned_executor_heartbeat_assignment_and_drain_contract | PARTIAL | |
| TestShardDistributorOnboarded (1935, ~4) | same + spectator_waits_for_initial_snapshot_and_returns_cached_owner | PARTIAL | |
| TestGetOrCreateSemaphoreManager (2055, ~8) | matching_semaphore.rs::matching_semaphore_registry_shares_one_manager_per_bucket | COVERED | |
| TestUnloadSemaphoreManager (2214) | matching_semaphore.rs::matching_semaphore_sweep_unloads_idle_buckets | PARTIAL | |
| TestStopSemaphoreManagers (2236) | matching_semaphore_registry_register_lookup_and_unregister | PARTIAL | |
| TestAcquireSemaphore (2285, ~4) | matching_rpc_semaphore_acquire_grants_and_reports_no_slot, matching_rpc_sql_semaphore_matches_go_unsupported_backend_error | COVERED | |

### handler/engine_integration_test.go (35 suite methods)
| TestAddActivityTasks (592), TestAddDecisionTasks (596) | matching_rpc_tests.rs::go_engine_add_decision_and_activity_tasks_persist_every_task (B engine-integration-tests, 7092b3bd7) | PORTED (B) | branch commit |
| TestAddAndPollDecisionTasks (709) | matching_rpc_tests.rs::go_engine_add_then_poll_decision_tasks_delivers_each_task_and_drains_store (B) | PORTED (B) | |
| TestQueryWorkflow (438) | matching_rpc_delivers_query_to_decision_poller, matching_rpc_child_query_forwards_to_parent_poller | COVERED | |
| TestPollForActivityTasksEmptyResult (214) / ...Decision (218) | matching_rpc_empty_polls_reserve_tailroom_and_read_live_expiration / matching_rpc_in_memory_empty_polls_reserve_tailroom | PARTIAL | |
| ...EmptyResultWithShortContext (222, 229) | matching_long_polls_reject_missing_transport_deadlines_with_bad_request; matching_dispatch.rs::matching_dispatch_empty_long_poll_expires_without_delivery | PARTIAL | |
| TestOnlyUnloadMatchingInstance (236) | matching_registry.rs::stale_teardown_cannot_remove_replacement_owner | PARTIAL | |
| TestPollForDecisionTasks (281) | matching_rpc_decision_poll_preserves_isolation_group_history | PARTIAL | |
| TestMatchingPollMetricOracle (535) | matching_rpc_reports_arrival_qps_by_isolation_group; matching_metrics.rs::poll_match_latency_emits_go_timer_and_histogram_pairs, poller_gauge_uses_go_task_list_dimensions | PARTIAL | |
| TestAddActivityTasksForwarded (600), ...Decision (604) | matching_rpc_child_add_sync_matches_parent_without_duplicate_persistence; matching_rpc_forwarded_add_never_persists_and_reports_remote_failure | PARTIAL | |
| TestAddTaskTrafficMetricOracle (611) | matching_metrics.rs::add_task_counter_matches_go_dimensions_and_exclusions | PARTIAL | |
| TestAddAndPollActivityTasks (713) | matching_rpc_cross_host_poll_add_and_cancellation | PARTIAL | |
| TestAddAndPollDecisionTasksIsolation (717) / Activity (721) | matching_rpc_isolation_toggle_returns_empty_poll_and_replaces_owner; matching_reader.rs::matching_reader_isolation_buffers_avoid_cross_group_head_blocking | PARTIAL | |
| TestSyncMatchActivityTasks (786) / Decision (790) | matching_sync.rs::sync_match_waits_for_history_completion_and_preserves_original_metadata / sync_match_zero_wait_claims_registered_poller_and_timeouts_withdraw_offers | PARTIAL | |
| TestSyncMatch*Isolation (794, 798) | matching_sync.rs::sync_match_does_not_deliver_to_other_or_unknown_groups | PARTIAL | |
| TestConcurrentAddAndPollActivities (942) / Decisions (950) | matching_registry.rs::concurrent_load_acquires_one_lease_and_isolates_task_types | PARTIAL | |
| ...ActivitiesWithZeroDispatch (946) / ...Isolation (958) | matching_writer.rs::zero_capacity_handoffs_are_fifo_and_never_overwrite_waiters | PARTIAL | |
| ...ActivitiesIsolation (954) / DecisionsIsolation (962) | matching_reader.rs::matching_reader_isolation_buffers_avoid_cross_group_head_blocking / matching_reader_isolation_timeout_reselects_without_rewriting_storage | PARTIAL | |
| TestPollActivityWithExpiredContext (1088) / Decision (1092) | matching_dispatch.rs::matching_dispatch_empty_long_poll_expires_without_delivery / matching_dispatch_background_timeout_unregisters_wait_before_next_task | PARTIAL | |
| TestMultipleEnginesActivitiesRangeStealing (1127) / Decisions (1131) | matching_rpc_native_partition_concurrent_versions_and_fencing | PARTIAL | |
| TestAddTaskAfterStartFailure (1218) | matching_registry.rs::slow_startup_does_not_block_loaded_owner_or_shutdown | PARTIAL | |
| TestUnloadActivityTasklistOnIsolationConfigChange (1264) / Decision (1268) | matching_rpc_isolation_toggle_stops_owner_before_admitting_add | PARTIAL | |
| TestDrainActivityBacklogNoPollersIsolationGroup (1326) / Decision (1330) | matching_rpc_isolation_policy_rechecks_drains_and_poller_recency | PARTIAL | |
| TestConfigDefaultHostName (1493) | matching_config.rs::go_defaults_when_unset (no host name case) | PARTIAL | |

### handler/handler_test.go (19 methods)
| TestHealth (153) | matching_meta_health_waits_for_start_on_both_transports | COVERED | |
| TestQueryWorkflow (457), TestRespondQueryTaskCompleted (511), TestCancelOutstandingPoll (554) | same as engine rows | COVERED | |
| TestGetTaskListsByDomain (712, ~7) | matching_rpc_quotas_classify_shared_protocol_handlers_and_preserve_count_only_calls + domain listing tests | COVERED | handler-test-inventory |
| TestHandlerAcquireSemaphore (921, ~6) | matching_rpc_semaphore_acquire_grants_and_reports_no_slot | COVERED | |
| TestDescribeTaskList (598) | matching_rpc_durable_describe_tracks_activity_poller_and_queue_status; go_handler_describe_task_list_rate_limited_returns_service_busy (B) | PARTIAL | CI 40015060031 |
| TestListTaskListPartitions (656) | go_handler_list_task_list_partitions_rate_limited_returns_service_busy (B) | PARTIAL | |
| TestUpdateTaskListPartitionConfig (854) | go_handler_update_task_list_partition_config_rate_limited_returns_service_busy (B); matching_rpc_manual_partition_update_rejected_while_scaler_enabled | PARTIAL | |
| TestRefreshTaskListPartitionConfig (804) | go_handler_refresh_partition_config_rejects_missing_tasklist_and_non_normal_kinds (B) | PARTIAL | |
| TestNewHandler (123), TestStart (129), TestStop (140) | matching_grpc_listener_registers_meta_health_and_gates_startup; matching_meta_health_waits_for_start_on_both_transports; matching_registry.rs::shutdown_persists_ack_and_stops_previously_returned_handles | PARTIAL | |
| TestAddActivityTask (167), TestAddDecisionTask (235), TestPollForActivityTask (303), TestPollForDecisionTask (380) | matching_rpc_durable_activity_preserves_source_domain_and_type; matching_rpc_durable_add_does_not_require_local_workflow_state; matching_rpc_cross_host_poll_add_and_cancellation; matching_rpc_decision_poll_preserves_isolation_group_history | PARTIAL | |
| TestNewHandlerContext (160), TestDomainName (771) | none | CANNOT-PORT | |

### handler/membership_test.go (9)
| TestGetTaskListManager_OwnerShip (61, ~4) | matching_registry.rs::matching_membership_guards_creation_and_unloads_moved_owners | COVERED | |
| TestSubscriptionAndShutdown (189) | matching_membership_lifecycle.rs::shutdown_self_evicts_before_stop_and_clears_placement | PARTIAL | |
| TestSubscriptionAndErrorReturned (222), TestSubscribeToMembershipChangesQuitsIfSubscribeFails (285) | matching_membership_lifecycle.rs::startup_failures_stop_provider_and_start_is_idempotent | PARTIAL | |
| TestGetTasklistManagerShutdownScenario (325) | matching_registry.rs::shutdown_tolerates_lost_task_list_ownership | PARTIAL | |
| TestShutDownNonOwnedSemaphoreManagers (374) | matching_registry.rs::matching_membership_guards_creation...; semaphore sweep | PARTIAL | |
| ...SurfacesALookupFailure (448) | matching_semaphore.rs::matching_semaphore_missing_semaphore_is_not_ready_after_scan | PARTIAL | |
| TestMembershipSubscriptionRecoversAfterPanic (169), ...ReportsAPanic (423) | none | CANNOT-PORT(panic) | |

### liveness/liveness_test.go (6)
| TestIsAlive_No (64), _Yes (70), TestMarkAlive_Noop (76), _Updated (83) | cadence-common/src/liveness.rs::original_go_liveness_fixtures | COVERED | |
| TestEventLoop_Noop (89) | matching_registry.rs::liveness_event_loop_noop_keeps_marked_owner_loaded | PARTIAL | analogue |
| TestEventLoop_Shutdown (105) | matching_registry.rs::liveness_event_loop_shutdown_unloads_owner_at_ttl (read: mark_alive(0), sweep at TTL, asserts stopped+unregistered) | PARTIAL | analogue |

### poller/manager_test.go (7) — all PARTIAL
| HistoryCallback (38) | matching_rpc_child_poll_receives_parent_query_and_preserves_completion_partition | PARTIAL |
| CancelPoll (58) | matching_rpc_cancel_without_matching_poll_returns_ok, matching_rpc_duplicate_poller_id_replaces_entry_without_cancelling_first; cadence-common poller.rs::original_go_poller_state_snapshots | PARTIAL |
| HasPollerFromIsolationGroupAfter (103), HasPollerAfter (177) | matching_registry.rs::has_poller_after_reports_outstanding_or_finished_pollers (weak: only i64::MIN); matching_rpc_isolation_policy_rechecks_drains_and_poller_recency | PARTIAL |
| GetCount (240), ListInfo (257), GetCountByIsolationGroup (332) | cadence-common poller.rs::original_go_poller_state_snapshots, history_size_counts_unique_identities_and_evicts_expired_entries; matching_rpc_reports_arrival_qps_by_isolation_group | PARTIAL |

### semaphore/ (42 functions)
| identifier: TestNewIdentifier (16) | matching_semaphore_identifier_matches_go_validation | COVERED |
| TestParseRequestOwner (59, ~4) | cadence-common semaphore.rs::parse_owner_rejects_malformed, owner_string_round_trip_matches_go, length_prefix_counts_bytes (inventory previously said NOT-ASSESSED; the tests exist) | COVERED |
| TestRingKey (88), TestIdentifierIsUsableAsAMapKey (101) | matching_semaphore_ring_key_separates_buckets, matching_semaphore_identifier_is_usable_as_a_map_key | PORTED |
| TestLogTags (116) | none | CANNOT-PORT(zap) |
| registry: RegisterLookupAndUnregister (38), UnregisterIgnoresAStaleManager (62), AllManagersIsASnapshot (85), BuildsOnceUnderConcurrentCallers (146) | matching_semaphore_registry_register_lookup_and_unregister, matching_semaphore_unregister_ignores_a_stale_manager, matching_semaphore_all_managers_is_a_snapshot, matching_semaphore_concurrent_callers_share_one_manager | PORTED |
| registry: BuildsOnlyWhenThereIsNothingHeld (101), HoldsNothingWhenTheBuildFails (129) | matching_semaphore_registry_shares_one_manager_per_bucket; matching_semaphore_missing_semaphore_is_not_ready_after_scan; matching_semaphore_parity_tests.rs::matching_semaphore_failed_load_unregisters_and_allows_fresh_retry | COVERED |
| manager (all in matching_semaphore_parity_tests.rs unless noted; branch evidence matching-semaphore-tests-20261023: 37 passed, 7 ignored) | | |
| NewManagerValidatesItsParams (136, ~8) | ..._new_manager_validates_params (#[ignore]) | MISSING |
| StartBuildsTheFreeSetAndTheHeldIndexAcrossPages (180) | ..._start_builds_free_and_held_indexes; ..._scan_threads_page_tokens (#[ignore]) | PARTIAL |
| StartKeepsAClaimedSlotOutOfTheFreeSet (206) | ..._start_excludes_owner_claim_even_when_token_is_free | PORTED |
| StartSkipsRowsItCannotClassify (227) | ..._start_skips_malformed_known_rows; ..._scan_skips_nil_and_unknown_rows (#[ignore]) | PARTIAL |
| StartLeavesStateUntouchedWhenTheScanFails (254) | ..._scan_failure_leaves_indexes_untouched | PORTED |
| SecondStartIsANoOp (282) / AFailedLoadUnregistersTheManager (295) | ..._second_start_is_noop / ..._failed_load_unregisters_and_allows_fresh_retry | PORTED |
| AcquireRejectsAnEmptyOwnerID (307) | matching_semaphore.rs::matching_semaphore_rejects_empty_owner_without_touching_free_set | PORTED |
| AcquireBeforeTheBucketIsUsable (320, ~4) | ..._acquire_before_bucket_usable | PORTED |
| AcquireHonorsItsDeadlineWhileStarting (389) | ..._acquire_honors_deadline_while_starting (#[ignore]) | MISSING |
| StartDoesNotWaitOnALoadInFlight (420) | ..._second_start_does_not_wait_for_scan | PORTED |
| AcquireHandsBackWhatTheGrantDecided (454, ~3) / AcquireSurfacesAFailedGrant (496) | ..._acquire_passes_through_grant_result / ..._acquire_surfaces_failed_grant | PORTED |
| GrantOnAFreeSlot (509), RetriesADifferentSlot (537), GivesUpAfterMaxAttempts (562) | ..._grant_on_free_slot_records_request_and_hold, ..._grant_retries_different_slot_when_taken, ..._grant_gives_up_after_max_attempts | PORTED |
| GrantStopsRetryingOnceTheDeadlineHasPassed (577) | ..._grant_stops_retrying_at_deadline (#[ignore]) | MISSING |
| GrantWhenTheWriteSaysTheOwnerAlreadyHolds (600, ~3), ...NoToken (651), ReturnsTheSlotWhenTheWriteFails (679) | ..._write_already_held_returns_reserved_slot, ..._already_held_without_token_is_internal_error, ..._failed_writes_do_not_drain_free_set | PORTED |
| GrantRejectsAnUnrecognizedWriteOutcome (699) | ..._grant_rejects_unknown_write_outcome (#[ignore]) | MISSING |
| GrantWhenTheReverseIndexIsStale (716, ~4), SurfacesAConfirmingReadFailure (772), DropStaleHold (786, ~3) | ..._stale_reverse_index_free_taken_and_missing, ..._confirming_read_failure_is_preserved, ..._drop_stale_hold_only_returns_proven_free_slot | PORTED |
| UnreserveIsIdempotent (829) | matching_semaphore.rs::matching_semaphore_unreserve_is_idempotent | PORTED |
| FreeSetSurvivesConcurrentReserveAndUnreserve (854), ConcurrentGrantsHandOutDistinctSlots (880), LifecycleUnderConcurrentGrants (919) | ..._free_set_survives_concurrent_reserve_unreserve, ..._concurrent_grants_are_distinct, ..._lifecycle_under_concurrent_grants | PORTED |
| AnIdleManagerUnloadsItselfAndLeavesTheRegistry (981) | matching_semaphore.rs::matching_semaphore_sweep_unloads_idle_buckets | COVERED |
| AcquireKeepsAManagerLoaded (999) | ..._acquire_refreshes_idle_time | PORTED |
| ALateStopUnregistersOnlyItself (1028) | ..._late_stop_unregisters_only_itself (#[ignore]); matching_semaphore_unregister_ignores_a_stale_manager | PARTIAL |
Two Rust-side fixes were made on that branch (failed grant unreserves ID; startup free set built only from scanned rows); `matching_semaphore_load_rebuilds_index_from_persisted_holds` is an older related test.

### tasklist/
| identifier: TestValidTaskListNames (33), TestTaskListParentName (71), TestInvalidTasklistNames (111) | cadence-common task_list.rs::matching_identifier_go_fixtures (+ matching_partition_mapper_go_fixtures) | COVERED |
| TestTaskListIDToString (133) | none | CANNOT-PORT(no Display) |
| isolation_balancer: TestAdjustWritePartitions (56, 30 sub) | matching_scaler_go_tests.rs::matching_isolation_balancer_* (30 tests; evidence matching-isolation-scaler-tests-20261023, CI 81cbcbff0) | PORTED |
| adaptive_scaler: TestAdaptiveScalerRun (100, ~18 sub) | matching_scaler_go_tests.rs::matching_adaptive_scaler_* (19 incl. ignored lifecycle + ignored production trailing-drain wiring gap) | PORTED |
| TestAdaptiveScalerLifecycle (87) | ..._lifecycle_start_stop_is_idempotent_gap (#[ignore], empty body) | MISSING |
| task_reader: TestDispatchSingleTaskFromBuffer (57, ~12) | matching_reader.rs::matching_reader_local_head_start_does_not_forward | PARTIAL |
| TestGetDispatchTimeout (244, ~6) | matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads | PARTIAL |
| TestTaskPump (303, ~3) | matching_reader_prefetch_backpressure_and_stop_preserve_unacked_rows | PARTIAL |
| TestGetTasksPumpConditionFailedUnloadsTaskList (443) | matching_registry.rs::fatal_owner_wakeup_does_not_wait_for_ack_interval | PARTIAL |
| TestTaskReaderBatchSizeValidation (464, ~5) | matching_reader.rs read_batch_size (no raw 0/-5 test) | PARTIAL |
| task_list_registry: RegisterLookupAndUnregister (30) | matching_registry.rs::stale_teardown_cannot_remove_replacement_owner, task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls | PARTIAL |
| TestTaskListRegistryManagersGaugeOracle (78) | task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls | COVERED |
| TestTaskListRegistry_Filters (102, ~3) | matching_shard_processor.rs::start_is_noop_and_stop_is_exact_name_across_domain_and_type; matching_registry.rs::describe_by_domain_* | PARTIAL |
| task_test: TestNewInternalTask (36, ~8) | none | MISSING |
| forwarder: TestForwardTaskError (77) | matching_dispatch.rs::matching_forward_task_error_is_no_parent_or_task_list_kind | PORTED |
| TestForwardDecisionTask (86), TestForwardActivityTask (110) | matching_forward_decision_task_fields_match_go_forwarder, matching_forward_activity_task_fields_match_go_forwarder (PG-1 no longer ignored) | PORTED |
| TestForwardTaskRateExceeded (135) | matching_forward_task_rate_exceeded_after_two_forwards | PORTED |
| TestForwardPollForDecision (203), TestForwardPollForActivity (231) | matching_forward_poll_for_decision_fields_match_go_forwarder, matching_forward_poll_for_activity_fields_match_go_forwarder | PORTED |
| TestMaxOutstandingConcurrency (259), TestMaxOutstandingConfigUpdate (311) | matching_forward.rs::matching_forward_max_outstanding_concurrency, matching_forward_config_update_resizes_generation (read) | PORTED |
| TestForwardQueryTaskError (148), TestForwardQueryTask (159), TestForwardQueryTaskRateNotEnforced (178), TestForwardPollError (192) | none | CANNOT-PORT |
| task_list_limiter: TestTaskListLimiter (15, ~13) | matching_limiter.rs::zero_burst_reports_throttled_without_consuming, wait_paces_after_terminal_burst_until_tokens_replenish | PARTIAL |
| task_completer: TestCompleteTaskIfStarted (68, ~17) | matching_standby.rs::standby_activity_requires_started_or_absent_activity, matching_standby_original_go_completion_decisions | COVERED |
| shard_processor: TestNewShardProcessorFailsWithEmptyParams (52), TestGetShardReport (59), TestSetShardStatus (68) | matching_shard_processor.rs::shard_processor_factory_rejects_empty_shard_key, shard_processor_initial_report_is_ready_with_zero_load, shard_processor_set_status_is_reflected_in_report | PORTED |

### tasklist/task_list_manager_test.go (28)
| 1 DeliverBufferTasks (127) | matching_reader.rs::stop_joins_pump_blocked_on_buffered_task_without_poller | PARTIAL |
| 2 TaskListString (155) | none | CANNOT-PORT |
| 3 DeliverBufferTasks_NoPollers (164) | stop_joins_pump_blocked_on_buffered_task_without_poller | PORTED |
| 4 ReadLevelForAllExpiredTasksInBatch (181) | matching_reader_read_level_advances_past_expired_rows_without_ack | PORTED |
| 5 RegistryNotification (272) | matching_registry.rs::unload_notifies_registry_once_and_second_unload_is_noop (read) | PORTED |
| 6 DescribeTaskList (304, ~15) | describe_by_domain_lists_loaded_root_without_config_or_pollers | PARTIAL |
| 7 QueriesPerSecond (457, ~3) | loaded_owner_reports_zero_qps_before_arrivals | PARTIAL |
| 8 CheckIdleTaskList (497, ~3) | idle_check_unloads_only_after_full_ttl_without_activity | PORTED |
| 9 AddTaskStandby (590) | append_after_writer_stop_fails_without_persisting; matching_standby.rs::standby_decision_uses_schedule_order_and_started_state | PARTIAL |
| 10 RateLimitErrorsFromTasklistDispatch (646) | matching_reader.rs::matching_reader_retries_throttled_dispatch_without_acking (B tlm-isogroup, CI 40014990614) | PORTED (B) |
| 11 MisconfiguredZoneDoesNotBlock (671) | matching_sync.rs::sync_match_invalid_isolation_group_poll_falls_back_to_default_group (poll side) | PARTIAL |
| 12 GetIsolationGroupForTask (709, 15 cases) | cadence-common task_isolation.rs::go_get_isolation_group_for_task_table (B tlm-isogroup) | PORTED (B) |
| 13 TaskWriterShutdown (943) | append_after_writer_stop_fails_without_persisting; matching_writer.rs::matching_writer_zero_capacity_times_out_unaccepted_and_keeps_accepted_write_on_stop | PARTIAL |
| 14 TaskListManagerGetTaskBatch (974) | cadence-persistence matching_queue.rs::matching_queue_get_task_batch_bounds_follow_read_range (B tlm-batchread) | PARTIAL (B) |
| 15 ReaderPumpAdvancesAckLevelAfterEmptyReads (1104) | matching_queue.rs::matching_queue_empty_reads_advance_ack_over_renewed_ranges, matching_queue_range_id_renews_per_block_and_fences_stale_writer (B) | PORTED (B) |
| 16 GetTaskBatch_ReadBatchDone (1196) | matching_queue.rs::matching_queue_get_task_batch_done_only_at_max_read_level (B) | PORTED (B) |
| 17 TaskExpiryAndCompletion (1241) | matching_queue.rs::matching_queue_gc_thresholds_drive_stored_row_counts (B gc-partcfg) + matching_reader_dispatch_expiry_distinguishes_active_and_passive_heads | PARTIAL (B) |
| 18 HasPollerAfter (1343, ~3) | matching_registry.rs::has_poller_after_reports_outstanding_or_finished_pollers (read; weak) | PORTED |
| 19 RefreshTaskListPartitionConfig (1380, ~5) | refresh_partition_config_from_request_ignores_older_versions | PARTIAL |
| 20 UpdateTaskListPartitionConfig (1498, ~6) | matching_partition.rs::update_partition_config_cases_match_go_task_list_manager_test (B gc-partcfg, CI 40015151528) | PORTED (B) |
| 21, 22 Refresh/Update...Concurrency (1703, 1720) | matching_rpc_native_partition_concurrent_versions_and_fencing | PARTIAL |
| 23 ManagerStart_RootPartition (1739), 24 _NonRootPartition (1777) | shard_policy_accepts_local_assignment_and_rechecks_before_publish; matching_rpc_child_partition_add_falls_back_to_local_persist | PARTIAL |
| 25 DispatchTask (1822, ~9) | matching_reader.rs::matching_reader_dispatches_new_task_to_waiting_poller_then_acks (B batchread; success path only) | PARTIAL (B) |
| 26 GetNumPartitions (1953) | none | CANNOT-PORT |
| 27 DisconnectBlockedPollers (1961, ~3) | release_blocked_pollers_wakes_waiting_decision_poll | PORTED |
| 28 TaskListUsesOverrideRPS (2024, ~7) | matching_rpc_tests.rs::matching_rpc_live_rps_override_matches_go_precedence | COVERED |

### tasklist/matcher_test.go (53)
PORTED: LocalSyncMatch (101) sync_match_local_match_hands_off_to_waiting_poller; LocalSyncMatchTimeout (120) sync_match_local_timeout_reports_no_match_when_poller_finishes_late (read); IsolationLocalSyncMatch (142) sync_match_isolation_group_hands_off_within_its_group; IsolationSyncMatchFailure (278) sync_match_isolation_group_mismatch_does_not_match; QueryLocalSyncMatch (298) matching_query.rs::sync_match_query_hands_off_to_parked_poller; Offer_NoTimeoutSyncMatchedNoError (694) sync_match_zero_local_wait_hands_off_to_waiting_poller; OfferOrTimeout_SyncMatchTimedOut (794) sync_match_offer_deadline_before_poll_withdraws_offer; Test_poll_Task (1143) sync_match_queued_offer_is_returned_to_poll; Test_poll_IsolatedTask (1129) sync_match_isolation_queued_offer_is_returned_to_its_group; Test_poll_QueryTask (1156) matching_query.rs::sync_match_queued_query_is_returned_to_next; Test_pollNonBlocking_Task (1196) sync_match_non_blocking_poll_returns_queued_offer_without_waiting; _IsolatedTask (1181) sync_match_non_blocking_isolated_poll_returns_queued_offer_without_waiting; _QueryTask (1210) matching_query.rs::sync_match_non_blocking_query_poll_returns_queued_query_once. (all in matching_sync.rs unless noted; port2/port3 evidence cites passing runs.)
PARTIAL (11) and CANNOT-PORT (27) and MISSING (2): see the lists above. Rust tests used for PARTIAL: matching_dispatch.rs::matching_remote_sync_match_forwards_to_root_poller, matching_isolation_remote_sync_match_forwards_to_root_poller, matching_remote_sync_match_blocking_waits_for_root_poller, matching_isolation_remote_sync_match_blocking_waits_for_root_poller, matching_sync_match_failure_reports_busy_without_matching (read: asserts `Err(ForwardError::SlowDown)`, not Go's "no error"); matching_sync.rs::sync_match_poll_after_stop_returns_no_task; sync_match_offer_without_waiting_poller_is_not_queued; sync_match_stop_while_parked_releases_poller_without_task; matching_query.rs::withdraw_removes_an_abandoned_offer.

### wrappers/thrift/thrift_handler_test.go
| TestThriftHandler (38, ~10) | thrift_matching.rs::matching_inbound_metrics_count_native_and_thrift_dispatch_once_each, matching_thrift_exceptions_fill_required_fields_like_go_mapper, matching_thrift_uses_each_methods_exception_field_ids | PARTIAL |

## go/common (Matching-relevant; file-level status, subtests not individually mapped)

| Go file (test funcs) | Rust test(s) | Status |
|---|---|---|
| membership/hashring_test.go (17) | cadence-common membership.rs::address_identity_ring_matches_go_hashring_golden_vectors, matching_membership_go_ring_oracle, go_fingerprint32_matches_signed_short_byte_vectors; matching_membership.rs (changed_ring_view_uses_go_gauge_scope_once = TestEmitHashringView, matching_membership_reuses_channels_and_evicts_removed_endpoints, typed_errors_and_atomic_notifications); matching_membership_lifecycle.rs | PARTIAL (subscribe duplicates, unsubscribe-on-empty, HandleUpdatesNeverBlocks, Lookup/Refresh race unmapped) |
| membership/resolver_test.go (5) | matching_shard_routing.rs::lookup_n_and_address_conversion_always_delegate_to_ring, empty_ring_returns_typed_hash_ring_error | PARTIAL |
| membership/sharddistributorresolver_test.go (13) | matching_shard_routing.rs::eligible_route_uses_spectator_and_parses_named_ports_and_ipv6, excluded_..., spectator_errors_and_missing_host_ip_do_not_fall_back, failed_ring_lookup_still_records_go_routing_dimensions, spectator_peer_cache_refreshes_use_and_evicts_at_ttl | PARTIAL (Start/Stop/Subscribe/Members/MemberCount/Refresh unmapped) |
| membership/tasklist_differentiator_test.go (2), percentage_onboarded_test.go (2) | matching_shard_routing.rs::go_farmhash_percentage_and_uuid_exclusion_vectors, policy_is_read_per_lookup_and_uuid_exclusion_precedes_spectator | COVERED (not read) |
| membership/hostinfo_test.go (1), hostinfo_differential_test.go (1), membershipfx (1) | matching_peer_provider.rs::role_filter_and_named_ports_preserve_legacy_tchannel_fallback (nearest) | MISSING (TestBelongs not mapped) |
| peerprovider/ringpopprovider/provider_test.go (3) | ringpop_provider.rs (≈60 tests: join/ping/heal), matching_ringpop_provider.rs::provider_snapshot_retains_other_roles_and_only_known_ports (TestLabelToPort), local_label_update_publishes_an_accepted_membership_change (SubscribeAndNotify) | PARTIAL |
| ringpopprovider/factory_test.go (2), config/config_test.go (4) | service-core ringpop_discovery.rs::dns_provider_matches_go_cache_errors_and_host_port_formatting, dns_srv_provider_*, static_hosts_*, file_reads_each_time..., missing_file_is_io_error_and_config_selects_supported_provider; matches_actual_go_provider_oracle_fixture | COVERED (DNS/SRV/hosts/file); Custom/Invalid mode PARTIAL |
| ringpopprovider/ringpopfx (1) | none | MISSING |
| dynamicconfig/file_based_oracle_test.go (4), file_based_client_test.go (1) | service-core admin.rs::file_based_yaml_matches_go_null_and_map_key_conversion, file_based_yaml_preserves_order_and_constraints; cadence-persistence dynamic_config.rs::task_list_filters_require_every_constraint_and_preserve_entry_order, scoped_duration_and_invalid_values_fall_back_without_skipping_matches | PARTIAL (poll reload unmapped) |
| dynamicconfig/configstore/config_store_client_test.go (3), config_test.go (2) | service-core admin.rs::config_store_snapshot_keeps_entry_value_order; cadence-persistence config_store.rs / dynamic_config.rs (4 filter tests) | PARTIAL |
| dynamicconfig/dynamicproperties, openfeature*, quotas, fx (≈35 tests) | matching_quota.rs (6 tests for rate limiting) only | MISSING for openfeature/unleash/fx (not used by Matching); quotas PARTIAL |
| metrics/defs_test.go (7), scope_test.go (7), histograms_test.go, config_test.go | service-core matching_metrics.rs::go_bucket_contracts_match_defs_go, manager_scaler_and_histogram_dimensions_match_go, ~20 dimension/name tests; matching_metrics_m3.rs (6), matching_metrics_statsd.rs (4), matching_metrics_prometheus.rs (5), matching_metrics_runtime.rs (3) | PARTIAL (matching defs only; TestMetricsAreUnique/OperationIndexesAreUnique not mapped); see also matching-metrics-matrix-20261023.md |
| rpc/params_test.go, factory_test.go, outbounds_test.go, peer_chooser_test.go, direct_peer_chooser_test.go, middleware_test.go, dns_updater_test.go | service-core grpc_tls.rs (rpc_tls_parses_go_yaml_names, mutual_tls_admits_only_clients_signed_by_the_configured_ca, server_tls_requires_a_matching_certificate_and_key, missing_ca_file_fails_tls_load_like_go, disabled_rpc_tls_stays_plaintext); cadence-matching launcher.rs::http_mutual_tls_and_permissive_round_trips, validates_tls_mode_and_certificate_configuration, tls_handshake_timeout_is_injectable; thrift_matching.rs::dedicated_http_listener_enforces_tls_handshake | PARTIAL for TLS; MISSING for outbounds/peer chooser/middleware/dns updater (Matching in Rust uses its own transports) |
| persistence/sql/sql_task_store_test.go (11), nosql/nosql_task_store_test.go (20), task_manager_test.go (13) | cadence-persistence matching_sqlite.rs (+ B persistence-tests: go_suite_get_tasks_paging_is_ascending_and_exclusive, go_suite_task_list_size_counts_above_ack_level, go_suite_complete_tasks_less_than_limits_delete_lowest_first, go_suite_lease_and_update_task_list_range_conditions, go_suite_sticky_and_ephemeral_kinds_lease_and_update, go_suite_complete_task_removes_each_row); matching_mysql.rs / matching_postgres.rs go_sql_pin template tests | PARTIAL (B). TestListTaskList, TestGetOrphanTasks, TestCreateTask have no Rust API |
| nosqlplugin/cassandra/tasks_test.go (10) | matching_cassandra.rs::cassandra_cql_matches_go_tasks_cql (templates), cassandra_task_row_constants_match_go, go_insert_tasks_batch_ttl_variants_and_fence (B) | PARTIAL (templates only, no bound values; TestUpdateTaskListWithTTL/ListTaskList/SelectTaskList unverified) |
| persistence/semaphore_tasks_manager_test.go (12), nosql_semaphore_tasks_store_test.go (8), cassandra/semaphore_tokens_test.go (6) | cadence-persistence semaphores.rs and cassandra.rs semaphore tests (not enumerated here) | PARTIAL (not mapped per test) |
| config/*_test.go (24+), authorization, log | service-core config.rs::parses_go_sql_pool_and_tls_config_for_matching | PARTIAL (only matching-relevant config) |

## Parity gaps recorded by this audit (Rust diverges from Go, from the tests above)

1. Semaphore manager: no params validation, no deadline/ctx on acquire and grant retry, no Unknown grant outcome, no unregister callback on direct `stop`, no page tokens in startup scan, nil/unknown rows not skippable (Cassandra returns Internal) — 7 `#[ignore]` markers in `matching_semaphore_parity_tests.rs`.
2. Matcher: no non-blocking poll-no-tasks (ErrNoTasks), unknown isolation group not rejected on poll (PG-2/PG-3; no test exists).
3. AdaptiveScaler: no Start/Stop handle; production trailing-drain wiring gap (ignored test).
4. TestNewInternalTask never assessed.
5. Query forward path and MustOffer retry loop have no unit seam; only RPC/gate coverage.

## Remaining work in priority order
1. Merge the unmerged test branches (tlm-batchread, tlm-gc-partcfg, tlm-isogroup, handler-tests, engine-integration-tests, persistence-tests) and re-run on a clean target.
2. Write real tests for PG-2/PG-3, TestNewInternalTask, poller manager counts (read the fixture first).
3. Replace weak `has_poller_after` test with Go's before/after times; assert liveness TTL/2 ticker bound.
4. Engine-integration PARTIAL rows (31) need Go-scenario ports; they dominate the gap.
