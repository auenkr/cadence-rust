# Matching CI test count reconciliation (2026-10-30)

Read-only review of the three workspace `rust-ci` runs; run logs retrieved with
`gh run view <id> --repo auenkr/cadence-rust --log`. Counts below are sums of
the single `test result: ok.` summary emitted by each test harness in the test
step. This does not count repeated individual test output or separately
recount doc-test output.

| Run | Head | Conclusion | Harnesses | Passed | Ignored |
| --- | --- | --- | ---: | ---: | ---: |
| [41035815248](https://github.com/auenkr/cadence-rust/actions/runs/41035815248) | `5755021dbaaeeb70b87ec9ba3c69bd06065398e4` | success | 57 | 1829 | 28 |
| [41037584598](https://github.com/auenkr/cadence-rust/actions/runs/41037584598) | `3a958668c040b289b495f9b76b2209969db7e2a0` | success | 57 | 1829 | 28 |
| [41040870450](https://github.com/auenkr/cadence-rust/actions/runs/41040870450) | `a534baae8c921e0ec8109527c6bc3184937b0323` | success | 57 | 1813 | 30 |

Therefore the 1813/30 result is not a miscount. Against 41037584598, the
candidate run has 16 fewer passing cases and two extra ignored cases. The
16 formerly passing case names absent from the candidate log are:

```text
cassandra::semaphores_cassandra_native::tests::caller_io_cancels_pending_and_keeps_ready_result
matching_dispatch_native::tests::backlog_forward_keeps_parent_deadline_after_slow_resolution
matching_dispatch_native::tests::in_process_forwarded_backlog_respects_caller_deadline_and_withdraws
matching_launch_native::tests::configured_grpc_semaphore_slow_start_deadlines_cleanup_and_retry
matching_membership::tests::resolver_member_count_tracks_the_published_ring
matching_persistence_limiter::tests::configured_datastores_receive_independent_limiters
matching_registry::partition_cancellation_tests::query_workflow_dispatch_failure_returns_error_and_cleans_pending_query
matching_registry::partition_cancellation_tests::refresh_rpc_propagates_engine_error_and_recovers_after_fault_clears
matching_registry_native::tests::invalid_liveness_ticker_keeps_partial_manager_for_recovery
matching_runtime_native::tests::manager_config_binder_keeps_constructed_domain_name_and_live_values
matching_semaphore::parity_tests::matching_semaphore_aborted_start_unregisters_releases_waiter_and_rebuilds
matching_semaphore_manager_native::tests::context_aware_grant_cancellation_restores_reservation
matching_semaphore_service_native::tests::independent_startup_timeout_stops_manager_and_allows_fresh_acquire
matching_semaphore_service_native::tests::scan_failure_reports_raw_bucket_and_releases_waiter
matching_semaphore_service_native::tests::typed_and_untyped_startup_errors_preserve_go_wire_semantics
matching_semaphore_service_native::tests::wrapped_typed_startup_error_uses_underlying_grpc_details
```

At `a534baae`, the following two test cases are present but ignored, accounting
for the +2 ignored delta:

```text
query_workflow_failed_to_dispatch_query_task_is_error
refresh_task_list_partition_config_manager_error_is_propagated
```

The run's complete set of 30 ignored test names was extracted from its test
log to `/tmp/ignored-names.txt` during this review; it includes those two,
24 existing parity/integration skips, and the four other Matching behavior
parity skips:
`domain_change_callback_updates_failover_version_and_releases_eligible_pollers`,
`get_task_lists_by_domain_success_reports_poller_identity`,
`list_task_list_partitions_success_owner_hosts_match_go_lookups`, and
`update_task_list_partition_config_tasklist_manager_error`.

Ancestry/source audit: `a534baae` is a 127-line tracing commit whose parent is
`5b0423a87`; the trace commit itself changes only the production-validation
workflow and trace markers. The compared CI heads are on different lines of
development: `3a958668` is not the parent of `a534baae`. The Rust diff between
the two compared heads spans 28 files (229 insertions, 2101 deletions), so the
16-case decrease reflects real source/test scope differences that require
separate review; the trace commit itself did not cause them. The two cited replacement-test commits
`1c1c6e94d` (query dispatch cleanup) and `741dafd86` (refresh error propagation)
are not ancestors of `a534baae`; their replacement tests do not establish
coverage on this candidate. The requested fixture/scanner commits
`f7c81d9d6` and `caa32882f` are also not ancestors. Thus this discrepancy is
an actual test-scope difference between the source heads, not duplicated CI
output. The large cross-line source diff was not fully audited for accidental
reversions by this count-reconciliation review; no production source was
edited. The detailed
ignored-case reasons are in the run log, including the two parity gaps above.

No Cargo, Go, formatter, runtime, CI dispatch, or PR command was run for this
review. Reproduction: retrieve each run log as above, sum integer `passed` and
`ignored` fields once per `test result: ok.` summary, and compare the harness
test names. Clippy/test jobs all concluded success at the exact heads listed.
