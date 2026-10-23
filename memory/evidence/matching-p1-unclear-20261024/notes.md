# P1 UNCLEAR items: resolution (2026-10-24)

Base: `origin/master` @ `e9b8e9a0e`. Source of items: `memory/matching-open-gaps-20261024.md` (on `origin/matching-open-gaps-20261024`).
Method: read the Go and Rust sources named by each item at `e9b8e9a0e`. Cargo was not run locally. The one code change is verified only by GitHub Actions (run URL below).

| item | verdict | evidence | change | run URL |
|---|---|---|---|---|
| P1-3 | FIXED | See P1-3 section below | none (comparison only) | n/a (no code change) |
| P1-7 | FIXED | See P1-7 section below | none | n/a |
| P1-11 | FIXED (STILL OPEN at base; closed by this branch) | See P1-11 section below | `matching_rpc_tests.rs:3189` `matching_rpc_standby_child_query_poll_receives_parent_query` (+45 lines) | https://github.com/auenkr/cadence-rust/actions/runs/40032293998 (head `e15ab7b2a`, success, 566 passed, new test `... ok`) |
| P1-14 | STILL OPEN | See P1-14 section below | none (needs a PostgreSQL gate run) | n/a |

## P1-3: persistence metric names, tags, buckets vs Go metered wrapper

Go (`go/common/persistence/wrappers/metered/base.go`):
- `updateErrorMetricPerDomain` :96, `updateErrorMetric` :134, `recordLatencyHistogram` :172, `call` :184.
- Per-call: requests counter, `persistence_latency` timer, `persistence_latency_ns` histogram (`defs.go:3333-3334`), manual `persistence_latency_histogram` only when enabled (`defs.go:3335`, `base.go:177`), per-host `persistence_latency_histogram_per_host` (`base.go:174-175`).
- Error counters: `defs.go:3337-3348` (overall) and `:3360-3371` (per-domain, with rollups). Failure counter `persistence_errors` (`defs.go:3332`) increments on Timeout, Busy, DBUnavailable and default.
- Buckets: `PersistenceLatencyBuckets` `defs.go:4250` (1 ms to 60 s), `ResponseRowSizeBuckets` `defs.go:4364` (0, then 1..2^16), `ResponsePayloadSizeBuckets` `defs.go:4383` (0, then 1 KiB * 2^0..2^19).

Rust:
- Class-to-name map `cadence-matching/src/matching_persistence_limiter.rs:35-81` (`PersistenceErrorClass::metric_names`). Matches Go for every class listed: ConditionFailed, NotFound (`entity_not_exists`), Timeout, Busy, DbUnavailable, BadRequest, Internal (`persistence_errors`). The `failure` flag matches Go's `PersistenceFailures` increments (Timeout, Busy, DbUnavailable = true; others false).
- Rollup names `:109-119` match `defs.go:3360-3371` `metricRollupName`.
- Classifier `:250-259` maps the matching task-store errors to those classes. Matching only produces these classes. Go-only classes (DomainAlreadyExists, ExecutionAlreadyStarted, CurrentWorkflowConditionFailed, ShardExists, ShardOwnershipLost, DuplicateRequest) are not reachable from matching, so their absence is not a parity gap for this service.
- Per-call emission `record_call` `:282-405` (requests, latency timer, `persistence_latency_ns` with `GoDefault1ms100s`, per-domain rollups `:368-376`, host latency `:382`).
- Bucket bounds: `cadence-service-core/src/matching_metrics.rs:44-67` (`GO_PERSISTENCE_LATENCY_BUCKETS_MS` ends at 60000 ms, same as `defs.go:4250`; row and payload bounds at `:60`, `:67`).
- Existing tests: `matching_persistence_limiter.rs:598-720` (`metered_task_tests`), and `cadence-service-core/src/matching_metrics.rs:2302` (`persistence_histogram_contract_tests`).

Verdict FIXED. Tags (`task_category=none` when no domain, `is_retry`, `domain` on per-domain) follow `base.go:59-65` and `base.go:184-190`.

## P1-7: per-group poller counts in describe RPC

Go:
- `go/service/matching/tasklist/task_list_manager.go:774-799` (`DescribeTaskList`: iterates `AllIsolationGroups()`, takes `GetCountByIsolationGroup(now - TaskIsolationPollerWindow)` and `qpsTracker.GroupQPS(group)`).
- `go/service/matching/poller/manager.go:196-206` (`GetCountByIsolationGroup` counts non-empty groups), `:225-237` (`forEachPoller`: included if outstanding or `after < created`).
- Window default: `go/common/dynamicconfig/dynamicproperties/constants.go:6197-6202` (`matching.taskIsolationPollerWindow`, 2 s).

Rust:
- `grpc_matching.rs:3378-3448` (durable `describe_task_list`: pollers via `queue.pollers.list`, group universe from `system.allIsolationGroups` `:3408`, window from `matching.taskIsolationPollerWindow` default 2 s `:3412-3417` (`2_000_000_000`), `count_by_group` `:3425`, per-group `IsolationGroupMetrics` `:3426-3440`).
- `cadence-common/src/poller.rs:119` (`list`), `:129` (`count_by_group`, skips empty group, same `outstanding || after < created` filter as `forEachPoller`).

Test: `matching_rpc_tests.rs:746` `matching_rpc_durable_describe_tracks_activity_poller_and_queue_status`, asserts per-group `poller_count` (`zone-a` = 1, `zone-b` = 0) at `:801-803`. Also `:2120-2133` (group QPS and unused group).

Verdict FIXED. Not checked: the non-durable branch (`self.queues` is `None`, `grpc_matching.rs` after `:3457`), which forwards to the frontend. This is a deployment-mode question, not part of the durable path.

## P1-11: standby query-poll forwards to parent and completes

Go:
- `go/service/matching/tasklist/matcher.go:487-505` (`PollForQuery`: local `pollNonBlocking` on `queryTaskC`, then `pollOrForward(..., queryTaskC)` to the parent).

Rust:
- Dispatcher forward: `cadence-matching/src/matching_dispatch.rs:897-920` (`forward_query_poll`).
- RPC branch: `grpc_matching.rs:1748-1800` (standby: `_standby_wait`, `select!` biased on `queue.queries.next()`, else `forward_query_poll`).
- Existing unit test: `matching_dispatch.rs:2375` `matching_standby_query_poll_forwards_to_parent_without_claiming_backlog` (dispatcher to a recording parent, not through the RPC handler). Also `:2478`, `:2511` failover tests.

Gap found: no test drove a standby query poll through the `poll_for_decision_task` RPC handler and the parent query channel. The active-domain analogue is `matching_rpc_tests.rs:3151` (`matching_rpc_child_poll_receives_parent_query_and_preserves_completion_partition`).

Change: `matching_rpc_tests.rs:3189` `matching_rpc_standby_child_query_poll_receives_parent_query` (+45 lines). Standby domain uuid `from_u128(3)` with `active_cluster_name = "remote"`, as in `:2705`. The child's standby poll forwards a query poll to the parent, a `query_workflow` RPC delivers "ping" to the parent's query channel, the poll returns it, the query is answered with "pong".

CI: `rust-ci.yml` `workflow_dispatch` with `package=cadence-matching` on head `e15ab7b2a`, run https://github.com/auenkr/cadence-rust/actions/runs/40032293998, conclusion success. Log line: `grpc_matching::durable_tests::matching_rpc_standby_child_query_poll_receives_parent_query ... ok`, and `566 passed; 0 failed`. The push-triggered run for the same SHA (`40032273137`) was cancelled by the concurrency group, which is expected. A mutation check (removing the standby branch) was not run.

Verdict FIXED. The dispatcher-level test (`:2375`) passes as well. The RPC-level gap is closed by this branch.

## P1-14: PostgreSQL gate after e9b8e9a0e with GC and domain-metadata checks

Searched every `origin/*` branch for `memory/evidence/**` PostgreSQL files (`git ls-tree` over all remote branches; `git diff --diff-filter=AM e9b8e9a0e <branch> -- memory/evidence`). Runs found:

| run | head in source.json | result | task garbage collection | domain metadata | run.exit |
|---|---|---|---|---|---|
| `matching-gate-run-20261023/pg1` | `105ff1378` | `None` | `null` | `null` | 0 (`pg1.exit`) |
| `matching-pg-mixed-20261023/run1-failed-schema-preexisting` | `5337dce19` | `None` | `null` | `null` | 1 (`run1-failed.exit`) |
| `matching-pg-mixed-20261023/run2` | `5337dce19` | `None` | `null` | `null` | 0 (`run2.exit`) |
| `matching-gate-final2-20261023/3-postgres` (on branch `matching-gate-final2-20261023` and others) | `b2f33c6d2` | `None` | `null` | `null` | 0 |

None of these is bound to `e9b8e9a0e`. Their heads `105ff1378` and `5337dce19` are older ancestors of the base (checked with `git merge-base --is-ancestor`), and `b2f33c6d2` is on a branch not merged to master. Each run has GC and domain-metadata checks as `null`, so none proves them.

Verdict STILL OPEN. Narrow fix is not possible from source alone. Needed: build the Rust binary into `cargo-target-final`, record its sha256 and head commit, then run the PostgreSQL lane one at a time with GC and domain checks enabled. Not run here (gate, docker, and no local cargo).
