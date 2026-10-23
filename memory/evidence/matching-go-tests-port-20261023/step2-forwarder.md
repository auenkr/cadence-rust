# Step 2: go/service/matching/tasklist/forwarder_test.go

Commands (run from rust/ in worktree gotests):

    /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib matching_forward_
      1st run: exit 101 (compile error: ForwardError not in scope in matching_dispatch tests)
      2nd run after adding `use crate::matching_forward::ForwardError;` to the test module: exit 0
      log: forwarder-cargo-test.log, exit: forwarder-cargo-test.exit (0)
      result: 7 passed, 0 failed, 0 ignored

Ported (PORTED):
- TestForwardTaskError -> matching_dispatch.rs::matching_forward_task_error_is_no_parent_or_task_list_kind
- TestForwardTaskRateExceeded -> matching_dispatch.rs::matching_forward_task_rate_exceeded_after_two_forwards
- TestMaxOutstandingConcurrency -> matching_forward.rs::matching_forward_max_outstanding_concurrency
- TestMaxOutstandingConfigUpdate -> matching_forward.rs::matching_forward_config_update_resizes_generation

Still MISSING (8): TestForwardDecisionTask, TestForwardActivityTask (need an in-process mocked parent client),
TestForwardQueryTaskError, TestForwardQueryTask, TestForwardQueryTaskRateNotEnforced (no Rust ForwardQueryTask),
TestForwardPollError, TestForwardPollForDecision, TestForwardPollForActivity (need a parent-less or mocked poll fixture).

Parity gaps: none (no ported test failed).

Step 3: task_gc_test.go does not exist in go/service/matching (only task_gc.go). Nothing to port.
Step 4: liveness_test.go: the 4 non-event-loop tests are COVERED by the existing JSON fixture test (no new test added);
the 2 event-loop tests are MISSING (no Rust daemon). No cargo run is needed because no new test names were added.
