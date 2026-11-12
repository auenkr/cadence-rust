# history-standbytests (2026-11-12)

Branch `history-standbytests` (from master c4312b82d). Test-only change: **no non-test Rust code changed**, so mixed gates were not required.

## Ported tests
- `transfer_standby_task_executor_tests.rs` (new, 33 tests): activity (pending, active-active, push-to-matching, ephemeral tasklist, empty tasklist fallback, success, missing info/workflow, global version mismatch noop, local version mismatch pushes), decision (pending, active-active, push, sticky timeout, ephemeral, success first/non-first, version mismatch), close execution / record workflow closed, cancel/signal/start-child (pending redispatch, fetch history from remote, active-active remote cluster, past discard -> DLQ task + ack level, missing info, version mismatch), RecordWorkflowStarted (+context header, pending status with advanced visibility, version mismatch), UpsertWorkflowSearchAttributes (+context header).
- `timer_standby_task_executor_tests.rs` (new): user timer, activity (create next timer + passive update, heartbeat clear, noop), decision (schedule-to-start ignored, version mismatch global/local), workflow backoff, workflow timeout (version mismatch global/local), pending/fetch/active-active/discard-DLQ matrix across all five timer kinds, not-running noop, load none / busy mapping, standby cluster time.
- `standby_test_support.rs`: shared fakes (domains global/local/active-active, Service with DomainMetricsScopeCache, active cluster manager, DLQ recorder).
- `transfer_active_task_executor_tests.rs` additions: RecordWorkflowStarted (+version mismatch, +context header), Upsert (no version check, +context header), ephemeral activity tasklist, global-domain version mismatch for activity and decision, ResetWorkflow skip paths (no reset point, pending children). Shard fake now has a real Service (get_service no longer unimplemented).
- `timer_active_task_executor_tests.rs` additions: global-domain version mismatch (decision timeout, activity retry timer, workflow timeout); `load_none_is_noop_and_busy_mapped` now really covers ContextDeadlineExceeded -> WorkflowBusy and passthrough of other errors.

## Bugs found
None; all ported tests passed against the existing Rust executors (two failures were test mistakes: Arc::get_mut on a shared Arc, and Go only verifies activity version in the retry timer, not the timeout task).

## Gaps (not ported)
- Active ResetWorkflow success path (needs a fake ResetVersionHistory + auto reset point) and the standalone CopySearchAttributes unit test (only exercised via the empty-attributes record path).
- Active decision ephemeral tasklist kind (decision task list kind comes from DecisionState, not exercised here); standby decision ephemeral is covered.
- Timer standby RetryTimeout/TransferTaskTimeout Go cases are covered only by the executor-level dispatch (activity retry timer drop is not tested).

## CI evidence (rust-ci, package=cadence-history)
- 43166247451 failed (trait item mismatch), 43166777362 failed (Arc::get_mut test bug), 43167737650 success, 43168630246 success, 43169588774 failed (wrong Go semantics in test), final: https://github.com/auenkr/cadence-rust/actions/runs/43170278105 success
- Final head: 6a1b2b949bcfd011ec4597d60d2197e4cb26840e
