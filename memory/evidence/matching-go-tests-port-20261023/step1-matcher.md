# Step 1: go/service/matching/tasklist/matcher_test.go

Commands (run from rust/ in worktree gotests):

    /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib sync_match_   # exit 0 (matcher-cargo-test.exit)

Log: matcher-cargo-test.log. Result: 10 passed, 0 failed, 0 ignored (the 3 new tests included).

Ported (PORTED):
- TestLocalSyncMatch -> matching_sync.rs::sync_match_local_match_hands_off_to_waiting_poller
- TestLocalSyncMatchTimeout -> matching_sync.rs::sync_match_local_timeout_reports_no_match_when_poller_finishes_late
- TestIsolationLocalSyncMatch -> matching_sync.rs::sync_match_isolation_group_hands_off_within_its_group

Still MISSING: 51 of 54 Go test functions in this file (remote forwarding, MustOffer, private poll helpers, non-blocking poll, AutoConfigHint, rate limiter subtests). See inventory rows.

Parity gaps from this file: none confirmed (no ported test failed).
