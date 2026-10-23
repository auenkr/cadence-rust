# Matching test fixes (base origin/master 9c5186e1b)

Commands (from rust/, via cargo-safe):
- test -p cadence-matching --lib <name> -- --nocapture : both failed before fix (exit 101)
- test -p cadence-matching --no-fail-fast : exit 0, 394 passed, 0 failed, 2 ignored (lib)
- clippy -p cadence-matching --all-targets -- -D warnings : exit 0

## matching_reader_local_poller_wins_after_forward_error — TEST wrong (race)
poll() returns when the task is handed over; the dispatcher records
AsyncMatchLocalPollAfterForwardFailed afterwards (Go matcher.go:396-403, metric after send).
Test asserted instantly. Fix: bounded wait for the metric. Code unchanged.

## replacement_can_load_before_old_cleanup_and_keeps_its_lease — TEST wrong
Test expected Owner::finish to return Err for a fenced final ack. Go taskReader.Stop
logs and ignores persistAckLevel failure (task_reader.go:172-176); Rust
tolerate_lost_ownership matches. Test now expects Ok; fencing still asserted on the
store (range_id 2, ack_level 0, only task 101).
