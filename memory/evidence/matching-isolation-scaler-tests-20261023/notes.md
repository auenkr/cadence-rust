# Matching isolation balancer and adaptive scaler Go test port (2026-10-23)

Task: audit P1-4. Port Go `go/service/matching/tasklist/isolation_balancer_test.go`
and `adaptive_scaler_test.go` to Rust unit tests with identical expectations.
`matcher_test` and `forwarder_test` are excluded (already ported). No program
logic change beyond the one parity fix below.

Branch: `matching-isolation-scaler-tests-20261023` (from origin/master 749c809df).
Not merged. Branch not deleted.

## CI evidence (final, passing)

- Workflow: `rust-ci.yml`, `package=cadence-matching`, `workflow_dispatch`
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40010958106
- Head sha tested: `81cbcbff0db96b129169716af73c937fd5a8f241`
- Conclusion: **success** (`gh run watch --exit-status` exit 0)
- `cadence-matching` lib tests: 536 passed, 0 failed, 11 ignored

Attempt 1 (run https://github.com/auenkr/cadence-rust/actions/runs/40010334295,
head `ff90c6e19`): 5 balancer scale-down cases failed. Cause was in the harness,
not the port: `ScalerConfig::default()` uses a 120s downscale sustain for
isolation groups, while Go `isolationConfig` (isolation_balancer_test.go:1309-1325)
sets every group sustain to `cycleInterval*4` = 60s. Fixed in `81cbcbff0`.

## Ported (46 tests, all green)

Source: `rust/crates/cadence-matching/src/matching_scaler_go_tests.rs`, included
from `matching_scaler.rs` as `go_tests`.

| Go test | Go lines | Rust tests | Result |
|---|---|---|---|
| `TestAdjustWritePartitions` (29 subtests) | isolation_balancer_test.go:56-1307 | `matching_isolation_balancer_*` (29) | 29/29 ok |
| `TestAdaptiveScalerRun` (17 subtests) | adaptive_scaler_test.go:100-662 | `matching_adaptive_scaler_*` (17) | 17/17 ok |

Harness notes (why these are faithful ports, not approximations):
- Balancer cases call `IsolationBalancer::adjust_write_partitions` with the same
  per-cycle metrics, partitions, and groups-per-partition changes as the Go
  `testCycle` table. Unset fields carry forward as in Go. Clock advances
  15s/cycle as `mockTime.Advance(cycleInterval)`.
- Adaptive cases replay each gomock script as one `ScalerCycle` per `run()`:
  the scripted `TaskListPartitionConfig` return, each `DescribeTaskList` response
  (or error), and the expected `UpdateTaskListPartitionConfig` (or none).
  `run_scaler_cycle` mirrors Go `run()` metric collection: isolation path
  describes every read partition and fails the tick on any error (reset,
  adaptive_scaler.go:156-161); non-isolation path describes only partition 0
  (`assumeEvenQPS`, adaptive_scaler.go:320-329).
- Sustain semantics were checked against `go/common/clock/sustain.go`:
  `check` does not reset on success, and `check_and_reset` re-arms to `now`.
  Rust matches.

## Not portable as written (ignored, with reason)

1. `matching_adaptive_scaler_lifecycle_start_stop_is_idempotent_gap`
   (`#[ignore]`). Go `TestAdaptiveScalerLifecycle` (adaptive_scaler_test.go:87)
   checks that `Start` and `Stop` are idempotent on a daemon object. Rust has no
   `Start`/`Stop` on `Scaler`. The scaler loop is `scaler_loop` in
   `matching_registry.rs`, and it ends when the owner stops. A faithful port
   needs a lifecycle harness around the registry (async, out of scope here).

2. `matching_adaptive_scaler_production_drain_describes_trailing_partitions`
   (`#[ignore]`). **Production wiring gap.** Go describes trailing read
   partitions over RPC during drain when metrics were not collected per partition
   (adaptive_scaler.go:289-306, `describePartition`). In Rust,
   `scaler_update` passes `|_| None` to `Scaler::tick`
   (`matching_registry.rs:1819`), so for non-isolated roots a drain never happens.
   Go `TestAdaptiveScalerRun` "underload sustained then drain"
   (adaptive_scaler_test.go:251) and "require empty" (adaptive_scaler_test.go:193)
   pass in the harness, which serves the scripted describes. They do not cover
   production. A fix needs the trailing describes prefetched or an async drain
   step, which changes RPC call pattern. That is a separate, reviewable change.

## Fixed (Rust now equals Go)

- `matching_registry.rs` `scaler_update`: a disabled scaler (or cache disabled)
  no longer resets the sustain clocks. Go `run()` returns early before touching
  them (adaptive_scaler.go:147-150). Covered by no unit test: the registry loop is
  async. Verified only by CI build.

## Observed, not covered by these tests

- `ensureMinimumGroupsPerPartition`: Go uses a single two-pointer pass over a
  list sorted once (isolation_balancer.go:290-344). Rust
  `ensure_minimum_groups` re-sorts and recomputes after each move. Outcomes match
  on every ported case (all green), but the algorithms are not identical in
  general. Left unchanged.
- Tie-breaking: Go iterates maps (random order) when sizes tie in
  `movePartitions` and in the add/remove sorts. Rust breaks ties by sorted id or
  by stable sort over `HashMap` iteration. Neither is pinned by Go, so no test
  depends on it.
- Go `Sustain` uses `IsZero` for "not started". Rust uses `Option`. Equivalent
  for all reachable times (mock clock starts after zero).

## Counts

- Go subtests in scope: 29 (balancer) + 17 (adaptive run) + 2 lifecycle/run
  containers.
- Ported and green: 46. Of those, 0 needed a Rust logic change.
- Ignored parity gaps: 2 (lifecycle not portable; production trailing drain).
- Production logic change: 1 (disabled scaler no longer resets sustains).

## Reproduce

```
cd /mnt/storage/service/github/wt/isobal
git submodule update --init go/idls
gh workflow run rust-ci.yml --ref matching-isolation-scaler-tests-20261023 -f package=cadence-matching
gh run watch <id> --exit-status
```

Local `cargo` was not run (host loaded, per task instruction). All evidence is
from CI.
