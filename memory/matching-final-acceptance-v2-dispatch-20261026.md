# Matching final acceptance v2 dispatch (2026-10-26)

## Immutable source

- Tag: `t3/final-acceptance-v2` (annotated, do not move)
- Commit: `eba0b437df9096d44b82be91af542d164c5c5123`
- Source branch at pin time: `master`
- This source includes the missing fixture flag `matching.enableGetNumberOfPartitionsFromCache` in the queue-filtered test and the approved 106 routing/shutdown tests.
- Acceptance context reported before dispatch: Python AST/diff check confirmed; prior fresh-workspace run `40373705020` succeeded on the 106-test source. This is context, not a substitute for the fresh run below.

## Runs dispatched against the immutable tag

| Workflow | Run | URL | Source SHA | Initial state |
|---|---:|---|---|---|
| `rust-ci.yml`, whole workspace, `clean_target=true` | `40375453720` | https://github.com/auenkr/cadence-rust/actions/runs/40375453720 | `eba0b437df9096d44b82be91af542d164c5c5123` | in progress at dispatch |
| `matching-production-validation.yml`, `lane=adaptive` | `40375459588` | https://github.com/auenkr/cadence-rust/actions/runs/40375459588 | `eba0b437df9096d44b82be91af542d164c5c5123` | in progress at dispatch |

## Mixed gate concurrency check

Immediately before dispatch, GitHub reported no queued or in-progress runs. The previous corrected-candidate mixed validation, `40373145351`, was terminal with conclusion `failure`; therefore the adaptive retry was not concurrent with another mixed gate.

## Result

Pending. Do not poll or start another mixed gate as part of this dispatch record. On continuation, inspect run artifacts and final status, including clean-target clippy/test exits, adaptive scenario evidence, and the Rust binary SHA256. Update this file with the run conclusions and artifact evidence.
