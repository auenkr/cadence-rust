# Lane 9 configstore workflow-task-retry — controls (2026-10-24)

Branch: `matching-gate-controls-20261024`, base `origin/master` 8d31b6f61 (Rust code under test).
Question: is the lane-9 failure `workflow-task-retry: timed out` (tests/go-sdk/mixed_matching.go:84, seen in
`matching-gate-final3-20261024/9-configstore`, binary from 1c45b6997) caused by the configstore path, the
`matching.longPollExpirationInterval=2s` seed, or the Rust binary?

## Verdict table

| Control | Scenario | Rust binary (master 8d31b6f61) | Config source | 2s long-poll seed | run.exit | SDK result | Retry panic → end (SDK log) |
|---|---|---|---|---|---|---|---|
| C0 | lane 9 minus `--matching-configstore` | same | file (`development.yaml` + harness overlay) | no | **0** | passed (`workflow-task retry ... passed`, cancel `Canceled`) | 04:56:43.7 → 04:56:57.9 (~19 s total) |
| C1 | lane 9 with `CADENCE_CONTROL_NO_LONGPOLL_SEED=1` | same | configstore | **removed** (5 entries seeded) | **0** | passed | 05:00:11.6 → 05:00:26.6 |
| C2 | exact lane 9 command | same | configstore | **yes** (6 entries) | **1** | **FAILED**: `panic: workflow-task-retry: timed out ... context deadline exceeded` at `mixed_matching.go:84` | 05:03:31.6 → ~05:04:11 (60 s SDK context expired) |

Reading: the same master binary and the same SDK scenarios pass with the 2s seed removed (C1) and fail with it (C2).
The configstore path alone (C1) does not fail, and the file path (C0) does not fail. The lane-9 failure reproduces
on master, so it is not specific to binary `1c45b6997`.

Caveats:
- Each control is a single run. No repeats, so intermittency is not excluded.
- The seed changes the Rust Matching long-poll expiry from the 60s default to 2s. Nothing in these runs shows the
  mechanism. The SDK sees the second decision retry never delivered within 60s.
- There is no Go-Matching control with 2s, because the lane runs Rust Matching only. So it is not yet known whether
  the stall is a Rust bug (a 2s expiry should not strand the retry task) or a harness scenario that is not valid at 2s.
- Timing: the retry panic lands ~16 s after SDK start in C2, vs ~5–6 s in C0/C1. Lane 9 is already slower before the
  stall. The 60s SDK budget (`tests/go-sdk/main.go:147`) is the limit that turns "slow" into "fail".
- C2 (SDK context 120s) was NOT run. It would separate "slow" from "stalled" and was optional. Not run.

## Single most likely cause

The seeded `matching.longPollExpirationInterval = 2s`. It is the only Rust-visible difference between the failing
C2 and the passing C1, and it is the only difference between C1 and C2. With it, the workflow-task retry is not
dispatched within the SDK's 60s budget. The exact mechanism is not established.

## Commands (verbatim, run from the gatectl worktree, `flock /tmp/cargo.lock work-safe ...`)

Common: `--binary /mnt/storage/service/gate-runs/gatefinal3/build/cadence-server` (Go, sha below),
`--history-transport grpc`, `--evidence-dir memory/evidence/matching-gate-controls-20261024/<C>/evidence`,
artifacts `/mnt/storage/service/gate-runs/gatectl/<c>/`, MySQL lane wrapper `tests/gates/run-mysql-lane.sh`
(mysql:8.0.36, `--memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048`, one container at a time).

- C0: `tests/gates/run-mysql-lane.sh .../c0 .../bin/cadence-matching --binary <go> --history-transport grpc --evidence-dir .../C0/evidence`
- C1: `CADENCE_CONTROL_NO_LONGPOLL_SEED=1 tests/gates/run-mysql-lane.sh .../c1 .../bin/cadence-matching --binary <go> --matching-configstore --configstore-seed-binary .../configstore-seed --history-transport grpc --evidence-dir .../C1/evidence`
- C2: `tests/gates/run-mysql-lane.sh .../c2 .../bin/cadence-matching --binary <go> --matching-configstore --configstore-seed-binary .../configstore-seed --history-transport grpc --evidence-dir .../C2/evidence`

Each control's `command.txt` records its exact line.

## Binaries (sha256)

```
8363698d7f71e0b01c0cd56cc19ff1900b91dd4c15b66ad4180cc0301d4770cb  gate-runs/gatectl/bin/cadence-matching   (Rust, built from origin/master 8d31b6f61)
86e67bb94809d87bc81bcba6a61f35f77e1f79b9fd34f55add55e44938af818e  gate-runs/gatefinal3/build/cadence-server (Go)
1e9259cd49bd916870e4834150a1c126354b5980cce4dea30a205584a252e0d5  gate-runs/gatefinal3/bin/configstore-seed
```

The Go server and seed are reused from final3. `go/` and `tests/` are identical between that base (bafd65a77) and
origin/master (checked with `git diff --quiet bafd65a77 origin/master -- go tests`), so they are valid for master.
The Rust binary was built with `cargo-safe build -p cadence-matching` (CARGO_TARGET_DIR=cargo-target-final,
debug profile). Build exit 0, log in `gate-runs/gatectl/build/rust-build.log`.

## Setup notes

- Worktree: `git worktree add .../wt/gatectl -b matching-gate-controls-20261024 origin/master`.
  `git submodule update --init go/idls cadence-web`.
- npm: the probe needs `@grpc/grpc-js` from `cadence-web/node_modules`. This was NOT in the final3 notes. Without it
  the first C0 attempt failed at the probe with `Cannot find module '@grpc/grpc-js'`. Fix:
  `cd cadence-web && npm ci --omit=dev --ignore-scripts` (445 packages, exit 0).
- Harness edit (CONTROL ONLY, not for merge): `tests/run-go-service-baseline.py`, env
  `CADENCE_CONTROL_NO_LONGPOLL_SEED=1` removes `matching.longPollExpirationInterval` from the seed and skips
  `CADENCE_MATCHING_CONFIGSTORE_POLL_MS` (the probe's 2s assertion, `tests/matching-service-compat.cjs:1169`).
- Build lock: `cargo-safe` already takes `/tmp/cargo.lock` internally. Wrapping it in an outer `flock` on the same file
  would deadlock, so the build ran `cargo-safe` alone.

## Incidents during this run (honest record)

1. A first C0 launch in the wrong cwd did not start (no run output). The stray empty dir created in the main checkout
   was removed.
2. A second C0 launch queued behind the first on `/tmp/cargo.lock`. The first run ran and failed on the missing npm
   module. The second ran the same way. I stopped the second (TERM to its lane script; its MySQL container was removed).
   Invalid attempts were moved to `gate-runs/gatectl/invalid-attempts/` (not in the repo).
3. The C0 run with the npm module missing (attempt 1) is invalid. C0 was then rerun cleanly; the verdict table uses only the
   rerun.

## Evidence layout

Per control `C0/`, `C1/`, `C2/`: `command.txt`, `run.log`, `run.exit`, and `evidence/` (`results.json`, `probe.log`,
`go-sdk.log`, `server.log`, `configstore-seed.log` for C1/C2 where present, `rust-logs/` and `rust-server-logs/`,
config-redacted, binaries.json, EVIDENCE.md).
