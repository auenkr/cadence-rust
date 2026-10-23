# matching_rpc_durable_cancel_unregisters_waiting_poller flake

Root cause: TEST race, not a code race. The test waited for `matching.polls` (register_poll) to be
non-empty, then sent CancelOutstandingPoll. In durable mode cancel goes through the queue's poller
manager (`queue.pollers.cancel`), which only knows the poll after `PollLifecycle::begin()` runs, later
than register_poll. A cancel in that window is a no-op (same as Go CancelPoller for an unstarted poll),
so the poll ran to its timeout and the 2s result timeout fired under load.
Fix: wait until `queue.pollers.contains_poll("poller")` before cancelling. Also raised all
`timeout(Duration::from_secs(2), ...)` wait/result guards in matching_rpc_tests.rs to 10s (37 sites;
negative-timing millisecond assertions untouched). Remaining sleeps are short negative checks.

Results (cargo-safe): clippy -p cadence-matching --all-targets -D warnings exit 0 (clippy.exit);
test -p cadence-matching --no-fail-fast exit 0, 398 passed, 0 failed, 2 ignored (test.exit/test.log);
30 runs of the lib test binary filtering the test: 0 failures (loop.txt).
