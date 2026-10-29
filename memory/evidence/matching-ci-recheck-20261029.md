# Matching CI recheck (2026-10-29)

## Prior run: terminal success

- Run: https://github.com/auenkr/cadence-rust/actions/runs/40841353614
- Workflow `rust-ci`, branch `t3/matching-startup-candidate`, head
  `e84e51a1c5a08cc89df0a04ccf69a77e284bd1c4`.
- `clippy-test` conclusion: success. Clippy, Formatting, and Test steps all
  succeeded; test exit was 0. No failure log was produced.
- Semaphore manager test binary: **14 passed, 0 failed, 0 ignored**.
  Named passing cases include `get_or_create_semaphore_manager_load_runs_outside_creation_lock`
  (two-thread lock contention), `acquire_semaphore_request_during_load_gives_up_at_its_own_deadline`,
  and `get_or_create_semaphore_manager_starts_unstarted_registered_manager`.
- Cold domain metric cases all passed: `empty_name_lookup_is_bad_request`,
  `add_decision_task_captures_domain_resolver_panic_from_blocking_task`,
  `add_decision_task_metric_resolves_domain_created_after_cache_scan`,
  `add_decision_task_swallows_ordinary_domain_resolver_failure`,
  `name_lookup_preserves_domain_cache_error_classification`, and
  `name_based_listing_handlers_read_through_cold_domain_cache`.
- Native semaphore startup cases passed:
  `independent_startup_timeout_stops_manager_and_allows_fresh_acquire`,
  `ownership_error_retains_raw_bucket_detail`,
  `raw_owners_cached_fast_path_disabled_and_cancelled`, and
  `slow_start_keeps_running_after_caller_cancel_and_reuses_manager`.

## Focused recheck dispatched

- Confirmed remote `t3/matching-format-ci` points to documentation HEAD
  `1228a9e3f5bd05f486aae2e3ca72bede0d0e94a2`.
- Pinned source `721e196d31b2cd7876fc09086c8ac4b2a8995ba3` on remote ref
  `t3/matching-format-source-721e196` (the source commit includes the required
  `rust-ci.yml` dispatch inputs).
- Dispatched `rust-ci.yml` with `package=cadence-matching`,
  `clean_target=true`, and `format_matching_parity_files=true`.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40842468415
- Verified after dispatch: run head is exactly
  `721e196d31b2cd7876fc09086c8ac4b2a8995ba3`; status was `in_progress` at
  `clippy-test` checkout. No polling performed; terminal result remains pending.
- No workspace or mixed/dependency gate was dispatched. Frozen baseline
  `matching-native-final-20261029` at
  `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` remains untouched.
