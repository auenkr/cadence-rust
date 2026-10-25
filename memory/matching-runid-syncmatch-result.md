# Matching raw RunID activity sync-match result (2026-10-25)

- Branch t3/mw-runid-syncmatch (base origin/t3/matching-integration-20261025, no code changes needed).
- Existing test `matching_rpc_activity_sync_match_passes_raw_run_id_to_history_without_persisting` (matching_rpc_tests.rs) covers malformed, empty, uppercase and simple RunIDs through a waiting poller, asserting the exact raw string at History RecordActivityTaskStarted, a non-empty task token and no durable row. The RecordActivityTaskStarted mock records run_id.
- CI: https://github.com/auenkr/cadence-rust/actions/runs/40186820793 head aa268a8d38919dafde0704146539c38c76c9fba9, conclusion success (rust-ci, package=cadence-matching).
- Cassandra review: Go InsertTasks passes RunID string to gocql batch and returns the raw marshal error (cassandra/tasks.go InsertTasks); Rust rejects up front with BackendConfiguration (permanent). Both fail the persist; exact error type differs and is source-only, no behavior test. No production change made.
