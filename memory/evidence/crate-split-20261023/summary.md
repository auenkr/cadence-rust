# Crate split evidence (2026-10-23), base origin/master 3bcf30aca, branch restructure-services-20261023

All via /mnt/storage/service/bin/cargo-safe in rust/ (worktree wt/split). Real exit codes (.exit files alongside):
- check -p cadence-service-core: 0 (check-core.exit)
- check -p cadence-matching --all-targets: 0 (check-matching.exit, after fixing a test import path)
- check -p cadence-server --all-targets: 0 (check-server.exit)
- check -p cadence-history -p cadence-frontend -p cadence-worker: 0 (check-empty.exit)
- clippy -p cadence-service-core -p cadence-matching -p cadence-server --all-targets -- -D warnings: 0 (clippy.exit)
- test -p cadence-service-core: 0 -> lib 91 passed, e2e 27 passed (test-core.log)
- test -p cadence-matching --no-fail-fast: 101 -> lib 392 passed, 2 failed, 2 ignored; integration tests 2 ignored (test-matching.log)
- The 2 failures (matching_reader_local_poller_wins_after_forward_error, matching_registry replacement_can_load_before_old_cleanup_and_keeps_its_lease) are PRE-EXISTING: baseline origin/master `test -p cadence-server --lib` = exit 101, 477 passed, 2 failed (same two), 2 ignored (base-lib.log).
- cadence-history start -> "service not migrated yet", exit 1 (built and run). cadence-matching bin only checked, not run.

## Test counts
before: grep '#[test]|#[tokio::test]' over cadence-server src+tests+examples = 516.
after: core 118 + matching 398 = 516 (history/frontend/worker/server 0). Executed: baseline lib 481 (+6 main.rs) = 487; after core lib 91 + matching lib 396 = 487; e2e 27(+2 ignored) moved to core.

## Timing
Incremental `check -p cadence-matching` after touching matching_limiter.rs: 30s (incr.txt). Baseline monolithic cadence-server test build took >30 min under cargo-safe limits (killed once at the 30 min background limit, re-run completed).
