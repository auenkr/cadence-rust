# Matching zero dispatch recovery reader test (2026-10-27)

## Scope

`matching_reader_zero_dispatch_recovers_same_durable_task_after_ttl` now verifies the
recovery edge at the Rust reader boundary: a task is durably appended, zero
dispatch rate causes a throttle and leaves the ack level unchanged, then an
injected atomic clock advances past `TASK_DISPATCH_RPS_TTL_NANOS`, a higher
limit is reported, and the poller receives the same schedule ID. Successful
handoff advances the ack level. The limiter uses `TaskListLimiter::new` with
the fake clock; the test does not sleep for the 60-second production TTL.

This is scoped reader behavior. It does not reproduce the concurrent RPC
scenario in Go's `matchingEngineSuite.TestConcurrentAddAndPollActivitiesWithZeroDispatch`.
That Go test invokes `ConcurrentAddAndPollTasks(Activity, 20, 100, true,
false)`, switching the dispatch limit to zero on selected poll attempts (at
least 20 times), with task-list isolation disabled. Its companion
`TestConcurrentAddAndPollActivitiesWithZeroDispatchIsolation` enables
isolation. The Rust `fixture` used by this reader test creates no isolation
groups, so the test covers the shared reader channel only; no claim is made
about the isolated channel or concurrent engine/RPC behavior.

The table in `memory/matching-go-test-function-map-20261027.md` was corrected:
`task_list_limiter_test.go` has 12 named rows, and
`matching_partial_upgrade_d.rs::task_list_limiter_matches_go_limit_table`
already asserts all 12. The 11-case matching-rate fixture is separate; no new
limiter table tests were needed.

## Validation

No Cargo, Go, rustfmt, or test command was run locally per repository CI-only
validation requirements. CI evidence is pending the parent/orchestrator's
workflow run.
