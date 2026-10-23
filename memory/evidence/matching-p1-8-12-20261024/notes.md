# P1-8 and P1-12 notes (2026-10-24)

Base: `origin/master` @ `e9b8e9a0e`. Branch `matching-p1-8-12-20261024`. Source of items: `memory/matching-open-gaps-20261024.md` (on `origin/matching-open-gaps-20261024`). P1-4 and P1-6 are owned by another worker and were not touched.

Verification: no local cargo. The only check is GitHub Actions `rust-ci.yml` on this branch (run URL recorded in the final report, not here).

## P1-8: ListTaskListPartitions ring order vs Go. SKIPPED (not a narrow fix)

The suggested fix needs a Go node dump and a mixed gate, run alone. That is beyond this task's ~60-line limit. Source-level comparison, no change made:

- Partition keys match Go. Rust `TaskListName::partition_name` (`cadence-common/src/task_list.rs:50-56`) gives the root for partition 0 and `/__cadence_sys/<root>/<i>` otherwise. Go `getAllPartitions` (`go/service/matching/handler/engine.go:1321-1342`) builds the same keys with `constants.ReservedTaskListPrefix` (`go/common/constants/constants.go:251`).
- Task type values match. Go `TaskListTypeDecision = iota` (`go/common/persistence/data_manager_interfaces.go:217`) is 0, and Rust passes 1 for activity and 0 for decision (`grpc_matching.rs:2844`, `:2847`).
- Owner lookup differs in one place to verify on a mixed gate. Go uses `membershipResolver.Lookup(service.Matching, key)` (`engine.go:1313-1319`). Rust prefers the shard router when one is configured (`grpc_matching.rs:960-966`) and falls back to `membership.owner` (`matching_membership.rs:108`). The ring hash is the same FarmHash32 with 100 replica points (`cadence-common/src/membership.rs:209`, `:229`, `:257` golden vectors; Go `go/common/membership/hashring.go:53`, `:117`). So for non-sharded membership the owner function is the same, and only the sharded path differs.
- Errors from owner lookup are dropped to an empty host string in both (Rust `grpc_matching.rs:987-993`, Go `engine.go:1192` ignores the `getHostInfo` error and keeps the partition).

Still open: a live two-node comparison of `owner_host_name` for the same membership, which needs the mixed gate.

## P1-12: matcher_test port. ONE case added

Go `testRemoteSyncMatch` and `TestSyncMatchFailure` were checked against the Rust tests in `matching_dispatch.rs:2626-2770`.

What was already covered: the four remote-sync forward tests (`matching_dispatch.rs:2670-2689` (unchanged)) and the decision-add wire fields (`matching_dispatch.rs:2291-2320`, which asserts `forwarded_from` and list name).

Gap found: the map says Go's local-offer-then-forward step is not asserted. In Go `taskMatcherImpl.Offer` (`go/service/matching/tasklist/matcher.go:150-250`), a parked local poller takes the task before any forward. Rust does the same in `accept_forwarded_backlog` (`matching_dispatch.rs:466-503`, local `offer` at `:489`, forward at `:498`), but no test checked that the parent is skipped.

Change: added `matching_backlog_offer_prefers_local_poller_before_forwarding` (`matching_dispatch.rs:2691`, after `matching_isolation_remote_sync_match_blocking_waits_for_root_poller`). A child with a parked local poller gets the task locally, and the recording parent receives no add. About 45 lines, test only, no production change.

`TestSyncMatchFailure` (SKIPPED, behavior matches at the caller):
- Go: `Offer` swallows any forward error and returns `(false, nil)` (`matcher.go:222-240`).
- Rust internal: `forward_backlog_task` returns `Err(ForwardError::SlowDown)` (`matching_dispatch.rs:348`), and the existing test pins that (`matching_dispatch.rs:2740`, `matching_sync_match_failure_reports_busy_without_matching`).
- Rust wire caller: maps every forward error to "not matched" with `.ok()` (`grpc_matching.rs:1568`), so the persisted outcome matches Go.
- The divergence is only the internal error type, so no production change was made.

Other PARTIAL rows in the remote family (`matcher_test` 11 PARTIAL in the test map) are not ported here. Next block to port: `TestQueryRemoteSyncMatch` / `TestQueryRemoteSyncMatchError` (`go/service/matching/tasklist/matcher_test.go:317`, `:368`).
