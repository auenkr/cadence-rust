# Group 2: remote-forward cases (matcher_test.go, port pass 2)

Command (rust/, shared target dir, one run per attempt):

    cargo-safe test -p cadence-matching --lib sync_match

Attempts:
- attempt 1: exit 101, compile error (SyncMatcher not in scope in matching_dispatch tests). Log kept as
  remote-cargo-test-attempt1-compile-error.log, exit remote-cargo-test-attempt1.exit.
- attempt 2 (after adding `use crate::matching_sync::SyncMatcher;` to the test module): exit 0.
  Log: remote-cargo-test.log. Result: 16 passed, 0 failed, 0 ignored (includes the 5 new tests).

Ported (5):
- TestRemoteSyncMatch -> matching_dispatch.rs::matching_remote_sync_match_forwards_to_root_poller
- TestIsolationRemoteSyncMatch -> matching_dispatch.rs::matching_isolation_remote_sync_match_forwards_to_root_poller
- TestRemoteSyncMatchBlocking -> matching_dispatch.rs::matching_remote_sync_match_blocking_waits_for_root_poller
- TestIsolationRemoteSyncMatchBlocking -> matching_dispatch.rs::matching_isolation_remote_sync_match_blocking_waits_for_root_poller
- TestSyncMatchFailure -> matching_dispatch.rs::matching_sync_match_failure_reports_busy_without_matching

Scenario mapping (not parity failures, documented in the inventory):
- Rust composes ForwardState::forward_backlog_task with an in-process root (Parent::Dispatcher → accept_forwarded_backlog
  → root SyncMatcher). The child's own local-offer-then-forward step is not a Rust matcher step, so it is not asserted.
- forwardedFrom and task-list name are on the wire in Go; the in-process root only exposes the forwarded signal.
  The wire fields are asserted in the forwarder group (matching_forward_decision_task_fields_match_go_forwarder).
- TestSyncMatchFailure: Go Offer returns no error; Rust forward returns Err(SlowDown), which the caller treats as not matched.
