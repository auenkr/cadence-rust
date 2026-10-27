# Original-Go adaptive control review (2026-10-27)

Reviewed `fed7fd7fa41e9b444439d9439686ff4628d20368` read-only; no production edits and no local build, test, or runtime gate.

## Findings

No concrete wiring or execution blocker found in the requested path.

- `.github/workflows/matching-production-validation.yml` routes `adaptive-go-control` through `run-mysql-lane.sh` with `CADENCE_MATCHING_GO_CONTROL=1`, optional retry-only propagation, and the ordinary outer probe timeout. It creates `out` before recording evidence, records the source commit and Go server SHA256, and uploads the gate artifacts.
- `tests/gates/run-mysql-lane.sh` creates the isolated MySQL databases and records container cleanup in its EXIT handler. The control runs `run-go-service-baseline.py --adaptive-go-control --mysql` with the same MySQL persistence config as the mixed lane.
- `tests/run-go-service-baseline.py` accepts that all-Go mode, applies the identical task-list-scoped adaptive scaler/cache/QPS/update dynamic config, waits for Go Frontend and Worker readiness, and dispatches the control script. The control uses original Go Matching and all four Go services.
- `tests/matching-adaptive-go-control.cjs` calls the shared `runProbeScenarios` adaptive scenario. `tests/matching-probe-scenarios.cjs` retains the 24-workflow completion checks, unchanged Go SDK suite (60-second context in `tests/go-sdk/mixed_matching.go`), and explicit scaled-read-partition routing checks. The optional fresh-process retry probe is additive; the full suite still runs.
- Control output marks `acceptanceEligible: false` and records its diagnostic topology. It is a one-Go-Matching control, while the Rust adaptive trace exercises two Rust Matching nodes. It cannot establish two-node Rust parity or service acceptance; it provides a baseline for the shared behavioral scenario.

No code fix recommended from this bounded source review. Runtime execution and its outcome remain for the parent gate; this note is not runtime evidence or a service acceptance claim.

## Subsequent runtime correction

Run40472035315 on the reviewed source failed before adaptive execution: Go
Matching had not yet refreshed the newly provisioned canonical domain. Parent
fixed that specific provisioning race in9a3673d0d with the existing compatibility
probe's bounded15s retry of the exact DefaultDomainCache error. No adaptive
assertion or SDK budget was changed. Runtime comparison run40472428123 is now
active at that corrected source. The no-blocker source review above did not
establish runtime readiness.
