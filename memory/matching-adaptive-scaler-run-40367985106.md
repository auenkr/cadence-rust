# Adaptive scaler mixed run 40367985106

Run: https://github.com/auenkr/cadence-rust/actions/runs/40367985106  
Head: `db5c471c7e1613390c4b21a030bcfc79a6c1a122`  
Conclusion: failure in the adaptive-scaler mixed lane.  
Downloaded artifact: `/mnt/storage/service/ci-evidence/40367985106`.

## Evidence and diagnosis

The build exit was 0 (`matching-production-adaptive-scaler/build.exit`). The gate, lane, and probe exits were 2, 2, and 1. The binary SHA256 was `07ab839c980e4c9356bffdc5eeb53d2cca75a575b0a5197fcea5ea2f0f19416b`. `results.json` records Go Frontend/History/Worker with Rust Matching, `goMatchingStarted: false`, a fresh Rust Matching binary, and `acceptanceEligible: false` because the probe failed.

The probe traceback is in `matching-production-adaptive-scaler/gate/run/probe.log`. It reached the partition listing assertion at `tests/matching-probe-scenarios.cjs:246`: Rust `DescribeTaskList` had already shown both a larger write-partition set and an advanced config version, but Go Frontend's `ListTaskListPartitions` response had length 1 while the scaled write count was 24. The assertion ran before the Go SDK workflow, so this run contains no evidence that the SDK completed or failed, and no all-24 workflow completion evidence.

This is a Rust production implementation gap, not a bad scale-up fixture. The Go Frontend handler delegates the listing RPC to its Matching client. Rust Matching's `task_list_partition_metadata` used only the static `matching.numTasklistWritePartitions` provider (default 1), although adaptive scaling had persisted the live partition config on the root task list. Thus listing hid the successfully persisted scale-up.

## Repair

Rust Matching now reads the root task-list's persisted adaptive config when building partition metadata and uses its write count, falling back to the static configured count when no adaptive config exists. A regression test stores a four-partition adaptive config and verifies `ListTaskListPartitions` returns all four keys. The acceptance probe's scale-up, config-version, frontend listing, Go SDK execution, and 24 exactly-once workflow completion assertions remain unchanged.

Validation must run on GitHub Actions. The Rust formatting/test workflow and one adaptive-scaler mixed retry are pending; record each URL, head SHA, conclusion, probe exit, and binary SHA when artifacts are available. No claim is made here about root-child delivery beyond scoped same scaled queue delivery.
