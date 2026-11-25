# Recovery timing: cold start and abrupt service loss, Go vs Rust (2026-11-26)

Harness: `tests/frontend-gate/recovery/` (`main.go` SDK driver, `recovery-run.py`), workflow `.github/workflows/recovery-timing.yml` (branch `perf-recovery-timing`, push-triggered on that branch; **must be merged to master before it can be dispatched**).

## Run
- Run [45693252501](https://github.com/auenkr/cadence-rust/actions/runs/45693252501), conclusion **success**, head `1dd2980dccd1f1f45dcd9c6408c5bd4d90faa0ea`, artifact `recovery-timing` (3 day retention).
- Runner: ubuntu-latest, AMD EPYC 9V45 96-Core (4 vCPU, 15 GiB), kernel 7.0.0-1020-azure. MySQL 8.0, one container per service (1 CPU / 1400 MiB each, MySQL 1.5 CPU / 1800 MiB), Go Worker in both arms.
- Binary SHA256: Go `cadence-server` a876056171b1f4adf1548f0bd860e779b821a5dd59724a5b4eea0784ba573607; Rust `cadence-server` f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74, `cadence-history` 1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5; driver 49cc01cce48df962182fc26a689b83530eb7ab9e3826a6db8fb00b0bfeeb6a82.
- Rust arm: `rust_services=frontend,matching,history`. 5 repetitions per arm, alternating order, fresh DB per arm/rep.

## Method
One SDK driver (Go SDK, TChannel, own worker with a single echo-activity workflow) starts a workflow every 100 ms for the whole arm and retries start (50 ms backoff, 5 s per attempt, 150 s deadline) so a workflow started during an outage is not abandoned. Wall-clock ns are recorded per workflow. Scenarios run in sequence on one cluster per arm:
- `cold_fresh`: t0 = just before `docker compose up -d` of the whole stack on an empty DB (includes MySQL init, schema auto-setup and image start).
- `kill_<svc>`: t0 = just before `docker compose kill -s KILL <svc>`; the container is restarted immediately (so recovery includes docker start, not only the process). Frontend, matching and history one at a time.
- `cold_restart`: SIGKILL of all four Cadence services, 2 s later restart of all four (DB stays up).
- "first completion" = first workflow completion after t0 (may be an in-flight workflow that finished just after t0, so it can be very small). "first NEW" = first completion of a workflow started at/after t0. "stable" = end of the first run of 10 consecutive new workflows that all succeed with latency < 2 s. **Stable is the robust number**; it has a floor of about 1 s (10 starts at 100 ms).

## Results (ms, median / p95 of 5)
| scenario | arm | first completion | first NEW | stable (10 consecutive) median | stable p95 | never stable |
|---|---|---|---|---|---|---|
| cold_fresh | go | 20287 / 32183 | 20287 | 20531 | 32693 | 0 |
| cold_fresh | rust | 18220 / 18541 | 18220 | 18573 | 18993 | 0 |
| kill_frontend | go | 37 / 2501 | 39 | 2789 | 2863 | 0 |
| kill_frontend | rust | 2433 / 2506 | 2433 | 2600 | 2744 | 0 |
| kill_matching | go | 2256 / 2283 | 2256 | 2406 | 2456 | 0 |
| kill_matching | rust | 2160 / 2170 | 2160 | 2277 | 2311 | 0 |
| kill_history | go | 16 / 1197 | 1197 | 1345 | 1347 | 0 |
| kill_history | rust | 1158 / 1216 | 1210 | 1240 | 1261 | 0 |
| cold_restart (all 4 killed) | go | 10208 / 11624 | 10248 | 11879 | 13075 | 0 |
| cold_restart (all 4 killed) | rust | 9605 / 10622 | 9605 | 10755 | 11826 | 0 |

Correctness: every started workflow completed in both arms: Go 2015/2015, Rust 1809/1809 (0 lost, no failures, all 50 events reached stable). Rust handles fewer workflows only because its cold start finished earlier relative to a fixed-duration scenario sequence, not because of failures.

Reading: Rust is on par with or slightly faster than Go in every scenario (cold start ~2 s faster median and far less variable: p95 18.5 s vs 32.2 s; history loss ~100 ms faster; all-services restart ~1 s faster). Sub-second recovery was NOT reached in any scenario: frontend/matching loss costs ~2.2-2.6 s in both arms (dominated by client/ring/membership reconnect and docker restart, not by the service language); history loss ~1.2 s. The "first completion" 16-39 ms medians for Go are in-flight workflows finishing just after the kill, not recovery. Frontend kill is bimodal (about 40 ms vs 2.4 s time to first new completion depending on whether the SDK's connection reached the dying process) in both arms.

## Finding (Rust image): restart of the history container fails
First attempt, run [45691181739](https://github.com/auenkr/cadence-rust/actions/runs/45691181739) (failed, not used for results): `docker kill` + `docker start` of the **Rust** history container exits 1: `cadence-auto-setup.sh` retries `t create-database --db cadence` 60x ("Can't create database 'cadence'; database exists") and exits, because auto-setup (env `CADENCE_AUTO_SETUP=true` in the split compose) is not idempotent in the Rust image. The Go image restarts fine with the same env. In this real-world path (container restart policy, orchestrator restart) the Rust history would crash-loop until auto-setup is disabled. No rust/ or go/ code was changed here; the harness works around it for both arms by recreating history with `CADENCE_AUTO_SETUP=false` on restart (timings above include the compose recreate, about 0.3-0.5 s, in both arms identically). This needs a fix in the Rust image's auto-setup script (treat "database exists" as success) before declaring History restart parity.

## Reproduce
`gh workflow run recovery-timing.yml --ref master` (after merge) with optional `repetitions`, `rust_services`, `scenarios`. Raw per-workflow samples, per-event JSON, service logs and image identities are in the artifact (`recovery/`).
