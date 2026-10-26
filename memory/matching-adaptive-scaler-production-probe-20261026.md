# Matching production adaptive-scaler probe — 2026-10-26

Implemented a production mixed-cluster probe for the final Matching coverage gap
in `matching-final-acceptance-audit-20261026.md`. The opt-in lane reuses
`.github/workflows/matching-production-validation.yml` and
`tests/run-go-service-baseline.py`; it runs standalone Rust Matching with the
original Go Frontend, History, and Worker. No Go Matching is started.

The harness scopes `matching.enableAdaptiveScaler=true`,
`matching.enableGetNumberOfPartitionsFromCache=true`, a one-second scaler/QPS
window, a 1 RPS upscale threshold, and zero sustained upscale time to the named
root decision queue `production-adaptive-scaler-probe`. The Node scenario
ensures its root manager exists, schedules 24 real workflows through Go
Frontend/History, then requires the direct Matching `DescribeTaskList` config
to show both a higher write partition count and a higher config version. It
also checks the Go Frontend `ListTaskListPartitions` exposes the new decision
partitions. Finally it invokes the existing Go SDK worker/client against that
same queue and requires its workflow and activity to complete after the update.
The SDK task-list override is optional; existing lane behavior is unchanged
without it.

Source rationale: Go starts the adaptive scaler only on root normal task-list
managers and `adaptiveScalerImpl.run` requires both scaler enablement and
cache-backed partition counts (`go/service/matching/tasklist/task_list_manager.go`
and `adaptive_scaler.go`). The scaler derives target partition count from
observed task QPS and persists a changed config through the manager. Rust
`matching_registry.rs` wires the production scaler on the root normal owner,
reads live config through the scaler provider, and updates partition config.
This probe drives those actual paths; it introduces no production test hook.

## Run and evidence

Exact dispatch after parent serial authorization:

```sh
gh workflow run matching-production-validation.yml --ref t3/matching-adaptive-scaler-probe -f lane=adaptive-scaler
```

The workflow builds a fresh `cadence-matching` binary, records its SHA256, and
stores the source SHA, command, gate logs, scenario JSON, and SDK output in the
three-day artifact. Expected scenario outcome is `adaptiveScaler.result=passed`
with `initial.writePartitions < updated.writePartitions`, an advanced config
version, matching Frontend partition listing, and completed Go SDK workflow and
activity. Mixed execution has not yet been dispatched, per the requested serial
authorization boundary. Rust and Go compilation, formatting, and mixed gates
were not run locally. Local lightweight checks passed: Node syntax checks,
Python byte compilation, and `git diff --check`.

Final source commit SHA and production binary SHA256 remain to be filled from
the pushed commit and authorized Actions run before this evidence can be marked
executed.
