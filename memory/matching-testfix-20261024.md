# Matching test health (2026-10-24)
- The two failures in crate-split-20261023/test-matching.log (matching_reader_local_poller_wins_after_forward_error, matching_registry replacement_can_load_before_old_cleanup_and_keeps_its_lease) pass on current master with no code change (source already fixed: reader test waits for the metric after poll() returns, matching Go matcher.go:396-403). Those logs were stale.
- Needed `git submodule update --init go/idls` in a fresh worktree (cadence-proto build script).
- Evidence: memory/evidence/matching-testfix-20261024/ (targeted.exit=0, clippy.exit=0, test.exit=0).
- Ignored parity gap remains: query_workflow_failed_to_dispatch_query_task_is_error.
