# DescribeTaskList non-durable path parity (2026-10-24)

Branch: `matching-describe-nondurable-20261024` (from origin/master bafd65a77).
Scope: Rust `Matching::describe_task_list` when `self.queues` is `None`
(`rust/crates/cadence-matching/src/grpc_matching.rs` ~3343-3500). Durable path is
covered by `matching_rpc_durable_describe_tracks_activity_poller_and_queue_status`.

## How the non-durable path works (Rust)

1. `payload.domain` resolved from domain UUID; `task_list_type` shifted to proto enum.
2. `queues` is `None`, so the request goes to `Frontend` (`grpc.rs` ~1056) which
   calls `POST /api/v1/tasklists/describe` in-process through the axum router.
3. `cadence-service-core/src/handlers.rs` `describe_task_list_response`:
   - pollers from `state.pollers`, keyed `(domain, list, identity, kind)`, written by
     `note_poller` at the start of each HTTP/yarpc poll handler (so outstanding polls
     are included); filtered to a 5-minute window; sorted; deduped by identity.
   - `rate_per_second` is always `0.0`.
   - `task_list_status` (only when requested) carries only `backlog_count_hint`.
   - `task_list` echoes the request.
4. Matching overlays `partition_config` from `executions.partition_config`.

## Go reference

- `go/service/matching/tasklist/task_list_manager.go` `DescribeTaskList` (~767-799):
  pollers from `GetAllPollerInfo`; `TaskList.Kind` = real kind; partition config;
  when requested, `TaskListStatus` with read/ack levels, backlog, rate limit,
  `TaskIDBlock`, per-group `IsolationGroupMetrics` (every configured group,
  `NewTasksPerSecond` + `PollerCount`), `NewTasksPerSecond`, `Empty` (ack == maxRead).
- `TaskListPartitionConfig` returns nil for non-normal (sticky) kinds.
- `go/service/matching/poller/manager.go` `ListInfo` / `GetCountByIsolationGroup`
  (~196-237): identity-keyed history; `RatePerSecond` from poll metadata; group
  counts only for non-empty isolation group.

## Parity results

| Field | Go | Rust non-durable | Status |
|---|---|---|---|
| pollers per list + task type | yes | yes (filtered, deduped) | matches (passing test) |
| pollers outstanding + 5-min history | yes | yes (note_poller at poll start) | matches |
| poller `rate_per_second` | from TaskListMetadata | always 0.0 | GAP (ignored test) |
| status absent unless requested | yes | yes | matches |
| `backlog_count_hint` | ack-manager backlog | in-memory queue length | matches for untouched list (passing test) |
| `isolation_group_metrics` | every configured group + counts | empty map | GAP (ignored test) |
| `task_id_block`, `empty` | derived from range/ack | unset / false | GAP (ignored test) |
| `read_level`, `ack_level`, `rate_per_second` (limiter), `new_tasks_per_second` | real | zero | GAP (not separately tested; same root cause as ignored tests) |
| `partition_config` for sticky kind | nil | not kind-gated (in practice none stored for sticky) | minor gap, untested |
| `task_list.kind` | real kind | echoes request kind | minor, untested |

## Why gaps were not fixed here

Fixing rate needs `note_poller` and the poller map value to carry
`max_tasks_per_second`, threaded through the HTTP, yarpc and gRPC poll paths.
Isolation metrics need the isolation group recorded per poller. Both exceed the
~50-line bound. `empty`/`task_id_block` need an ack/range model the non-durable
path does not have. These are recorded as `#[ignore = "parity gap: ..."]` tests.

## Tests added (matching_rpc_tests.rs)

- `matching_rpc_non_durable_describe_lists_pollers_per_task_list_and_type` (runs)
- `matching_rpc_non_durable_describe_reports_poller_rate` (ignored)
- `matching_rpc_non_durable_describe_reports_isolation_group_metrics` (ignored)
- `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` (ignored)

Polls are driven through the real `poll_for_*_task` gRPC entrypoints with a 1s
deadline so the handler records the poller exactly as production does.

## Verification

Not run locally (per instructions). Verified on GitHub Actions `rust-ci.yml`,
package `cadence-matching`, commit 295094c63.

- Run: https://github.com/auenkr/cadence-rust/actions/runs/40033300362 (success, first attempt)
- Clippy: pass. Test: `601 passed; 0 failed; 16 ignored` in the main crate suite.
- `matching_rpc_non_durable_describe_lists_pollers_per_task_list_and_type`: ok.
- Three parity-gap tests: ignored with reasons (listed above).
- Note: the new tests appear under the `grpc_matching::durable_tests` module label
  because `matching_rpc_tests.rs` is included there; the name says "non_durable".
