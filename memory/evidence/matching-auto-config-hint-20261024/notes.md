# AutoConfigHint gate probe (`--probe-auto-config-hint`)

Status: **not executed.** Only `node --check` and a Python AST parse were run. No gate, docker, or cargo run.

Covers row 41 of `memory/matching-cannot-port-review-20261024.md`
(`tasklist/matcher_test.go:Test_LocalSyncMatch_AutoConfigHint`).

## What the probe asserts

Scenario `autoConfigHint` in `tests/matching-probe-scenarios.cjs`, enabled by `CADENCE_PROBE_AUTO_CONFIG_HINT=1`
(set by `--probe-auto-config-hint`). Every call goes through Go Frontend (`ctx.call`):

1. `StartWorkflowExecution` on a decision list, then `PollForDecisionTask`. The poll must return the
   workflow's decision task.
   - `autoConfigHint` must be present (not null).
   - `autoConfigHint.enableAutoConfig === false`.
2. Respond with one `scheduleActivityTaskDecisionAttributes` on an activity list.
3. `PollForActivityTask` on that list. The poll must return the scheduled activity.
   - Same two checks on `autoConfigHint`.
4. `RespondActivityTaskCompleted`.

The presence check is on the message, not the bool. proto-loader runs with `defaults: true`, so an unset bool
reads as `false`. Without the presence check, a missing hint would pass.

Result is written to `results.json` under `autoConfigHint` as `{ decisionHint, activityHint }`.

## Why the harness overrides the config

`go/config/dynamicconfig/development.yaml` sets `matching.enableClientAutoConfig` to **`true`** (lines 83-84).
`tests/run-go-service-baseline.py` builds `dynamic_text` from that file, and both Go and Rust Matching read it.
Without an override, the Rust poll would return `enableAutoConfig: true` and the probe would fail.

The earlier review said the effective value is `false` by default. That is only the Rust code default
(`rust/crates/cadence-matching/src/matching_dyncfg_defaults_tests.rs:67`). The gate's dynamic config does not use it.
Corrections to that memory note are needed.

With `--probe-auto-config-hint`, the harness replaces the `true` entry with `false` in `dynamic_text` before
writing the Go and Rust dynamic-config files. It exits with an error if the shipped text no longer matches.

`--matching-configstore` seeds `matching.enableClientAutoConfig: 'true'` into MySQL. The harness rejects the
combination.

## How to run (when a gate window is open)

Follow `tests/gates/README.md`: build the Rust binary first under `flock /tmp/cargo.lock` with
`CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared`, then start the gate. Docker limits and
one-gate-at-a-time apply.

```
python3 tests/run-go-service-baseline.py --rust-services matching --rust-production-binary $BIN \
  --probe-auto-config-hint --artifacts-dir /mnt/storage/service/gate-runs/auto-config-hint \
  --evidence-dir memory/evidence/matching-auto-config-hint-20261024
```

Add `--mysql` or `--postgres` if the lane needs a database. The flag is independent of the other `--probe-*`
flags and can be combined with them. Do not combine it with `--matching-configstore`.

Expected evidence: `results.json` contains `probeScenarios.autoConfigHint.result === "passed"` with both hints
showing `enableAutoConfig: false`, and `autoConfigHint.json` is written to the artifacts directory.

## Not yet verified

- No run has happened, so the probe's first real run may still expose a problem (poll shape, activity routing, timing).
- Under `--matching-configstore`, the probe is not applicable.
- The probe covers the poll response through Go Frontend only. The direct-to-Rust RPC unit check suggested in
  the review (`auto_config_hint.is_some()` on a sync-match poll) is not written.
