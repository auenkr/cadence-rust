# Matching adaptive scaler probe parity correction (2026-10-26)

## Scope and branch

This work starts from `origin/master` at `4a66ba20c5b3a45e58ab643742ef982ae5dbc497`. It incorporates the SDK completion probe change from `db5c471c7e1613390c4b21a030bcfc79a6c1a122`, then corrects its partition-listing expectation. It does not include the production Rust change from `ca64a5a934c6c04daf0f9385c00adbad09e788ea`.

## Go behavior traced

`go/service/matching/handler/engine.go`, `matchingEngineImpl.getAllPartitions` (around line 1321), obtains `n` using `e.config.NumTasklistWritePartitions(request.GetDomain(), rootPartition, taskListType)`. It returns the root and child partition names for indices below that configured static count. It does not read a task list's persisted adaptive `PartitionConfig`.

Adaptive scaling is a separate path: `go/service/matching/tasklist/adaptive_scaler.go` updates the task list partition config through `UpdateTaskListPartitionConfig`. A scaled persisted write count and version therefore do not redefine the static `matching.numTasklistWritePartitions` result used by `ListTaskListPartitions`.

The prior claim that Rust had a production gap because `ListTaskListPartitions` omitted persisted adaptive partitions was incorrect. The lookup added by `ca64a5a` changes Go behavior and can make a normal listing fail `Unavailable` on a storage lookup failure that Go never performs. That production change is intentionally excluded from this branch.

## Probe expectations

The adaptive probe keeps genuine Go Frontend/History workflow-start traffic, requires a persisted adaptive write-partition increase and version increase, then starts the existing Go SDK worker/client against the changed queue (the helper now honors `CADENCE_MIXED_MATCHING_TASK_LIST` rather than silently choosing a random queue). The SDK must complete all 24 load workflows, and the probe verifies exactly one workflow-completed event per execution.

The partition-listing assertion is now independent: static `matching.numTasklistWritePartitions` is left at its default of one for the adaptive queue, and each Rust Matching node plus Go Frontend must list exactly the root key and agree. The persisted adaptive count/version remain separately asserted from `DescribeTaskList`. No production Rust listing implementation is changed.

The runner exposes `--probe-adaptive-scaler`, applies scaler tuning only to the probe decision queue, and the production validation workflow exposes an `adaptive` lane. The existing harness owns its normal service/domain teardown and the scenario retains its SDK result/history completion checks.

## CI and mixed-gate evidence

- Obsolete mixed gate `40369759414` (head `ca64a5a934c6c04daf0f9385c00adbad09e788ea`) was cancelled; final status/conclusion: `completed` / `cancelled` ([run](https://github.com/auenkr/cadence-rust/actions/runs/40369759414)).
- Focused run `40370761303` for `cadence-matching` at `fb5f0ccac3450d549aad16d0c6fdc785302c9b12` was cancelled as stale after source inspection found the SDK worker did not honor the scaled queue ([run](https://github.com/auenkr/cadence-rust/actions/runs/40370761303), conclusion `cancelled`).
- Replacement focused `rust-ci.yml` run `40371005274` targets corrected source SHA `ee5b080f8306bd9dd65030aa7ddfe513d54e8b23`; it was `in_progress` when last inspected ([run](https://github.com/auenkr/cadence-rust/actions/runs/40371005274)). The current branch head will be a documentation-only descendant recording this run.
- Corrected adaptive mixed gate: pending dispatch until replacement focused CI completes, so the gates remain serial.
- Fresh runner binary SHA256 and final run URLs/conclusions: pending.
- Local checks are syntax-only (`node --check`, `python3 -m py_compile`, `git diff --check`). No Cargo, Go, formatting, or runtime validation was run locally.

No Matching service completion or parity claim is made by this probe correction.
