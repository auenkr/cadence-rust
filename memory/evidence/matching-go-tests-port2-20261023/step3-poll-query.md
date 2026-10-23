# Group 3: poll and query cases (matcher_test.go, port pass 2)

Command (rust/, shared target dir, one run):

    cargo-safe test -p cadence-matching --lib sync_match

Log: poll-query-cargo-test.log. Exit: poll-query-cargo-test.exit = 0. Result: 23 passed, 0 failed, 0 ignored.
Seven new tests passed: sync_match_queued_offer_is_returned_to_poll, sync_match_isolation_queued_offer_is_returned_to_its_group,
sync_match_non_blocking_poll_returns_queued_offer_without_waiting, sync_match_non_blocking_isolated_poll_returns_queued_offer_without_waiting,
sync_match_isolation_group_mismatch_does_not_match (matching_sync.rs), sync_match_queued_query_is_returned_to_next,
sync_match_query_hands_off_to_parked_poller (matching_query.rs).

Not run: Test_pollNonBlocking_QueryTask (not written this pass).
