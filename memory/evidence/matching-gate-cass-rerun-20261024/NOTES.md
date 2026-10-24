# Lane 4 (Cassandra) rerun with probe fixes — matching-gate-cass-rerun-20261024

Branch `matching-gate-cass-rerun-20261024` from `origin/master` e7285da4f, with `origin/matching-cass-probe-ownercheck-20261024`
(paced-list owner check, `7458a1e9b`) and `origin/matching-auto-config-hint-20261024` (`--probe-auto-config-hint`, `4e9328211`)
merged in (octopus, clean, no conflicts). No Rust production code changed. Go sources identical to the final3 base
(`git diff --stat bafd65a77 origin/master -- go` is empty).

## Result: FAILED BEFORE THE PROBE (run.exit = 1, lane.exit = 1)

No assertion ran. The harness aborted during Go baseline startup:

    RuntimeError: Go baseline startup timed out      (tests/run-go-service-baseline.py:924, 90 s deadline at :918)

- Go `server.log`: schema apply for `cadence` took ~22 s and `cadence_visibility` ~28 s, then `Connecting to store shard`
  at ~71 s after server start. The frontend gRPC port (`FRONTEND_PORT_GRPC`) was still refusing connections at the 90 s deadline.
- Load average was 5.3 at 11:05 and 12.2 at 11:09 (during Cassandra schema apply and Go startup), on 4 cores.
- No Rust `cadence-matching` node was started, so there are no `rust-server-*.log` files and no `paced-owner.json`.
- The owner-changed message (`paced list owner changed between poll and describe`) did NOT fire, because the probe never ran.
- The DescribeTaskList rate assertion and the auto-config-hint probe were NOT reached in this run.

So this run gives no verdict on the probe fixes. The startup timeout is an environment/timing failure in the harness precondition,
not a product or probe result.

## Command (verbatim)

    cd /mnt/storage/service/github/wt/gatecass
    EVD=$PWD/memory/evidence/matching-gate-cass-rerun-20261024/evidence
    G=/mnt/storage/service/gate-runs/gatecass
    flock /tmp/cargo.lock /mnt/storage/service/bin/work-safe tests/gates/run-cassandra-lane.sh $G/l4 \
      /mnt/storage/service/cargo-target-final/debug/cadence-matching \
      --binary /mnt/storage/service/gate-runs/gatefinal3/build/cadence-server \
      --history-transport grpc --evidence-dir $EVD --probe-auto-config-hint > $G/run.log 2>&1; echo $? > $G/run.exit

Same flags as final3 lane 4 (`memory/evidence/matching-gate-final3-20261024/4-cassandra/command.txt`), plus `--probe-auto-config-hint`.
The whole lane script runs under `flock /tmp/cargo.lock` + `work-safe`, so the python harness and the Rust server it starts
are inside `cadence-work.slice`. The lane script's own `python3` call is therefore covered (the harness was not invoked bare).
Cassandra image: `cassandra:4.1.7` (pulled fresh, digest `sha256:17850cb6e5f9…`, `gate-runs/gatecass/docker-pull.log`).
Container: `cadence-rust-gate-cassandra-lane-20261024T053553-1797744`, limits `--memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048`,
`MAX_HEAP_SIZE=1G`. The wrapper trap removed it; `docker ps -a` afterwards shows no gate container.

## Binaries (sha256)

| Binary | Path | sha256 |
|---|---|---|
| Rust `cadence-matching` (merged branch, clean target) | `/mnt/storage/service/cargo-target-final/debug/cadence-matching` | `59d8eb8f97bdf63d627baac24293a12ab5030d63773c77811fef1b1723b5ddfa` |
| Go `cadence-server` (reused from final3, go/ unchanged) | `/mnt/storage/service/gate-runs/gatefinal3/build/cadence-server` | `86e67bb94809d87bc81bcba6a61f35f77e1f79b9fd34f55add55e44938af818e` |

Rust build: `cargo-safe build -p cadence-matching` in `rust/` (CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-final),
exit 0, log `gate-runs/gatecass/build/rust-build.log`, `Compiling cadence-matching v0.1.0 (…/wt/gatecass/rust/crates/cadence-matching)`
and `Finished dev profile`. `.last-workspace` = `…/wt/gatecass/rust`.

Build mistakes recorded honestly:
1. First build ran from the repo root, not `rust/`. Exit 101 (`could not find Cargo.toml`). The `sha256sum` that followed printed
   the OLD gatectl binary (`8363698d…`), which is NOT this build and was not used. The rebuild from `rust/` replaced it.
2. Rebuild exit 0 (above) produced the sha `59d8eb8f…` used in the table.

## Setup

- `git worktree add /mnt/storage/service/github/wt/gatecass -b matching-gate-cass-rerun-20261024 origin/master`
- `git merge --no-ff origin/matching-cass-probe-ownercheck-20261024 origin/matching-auto-config-hint-20261024`: clean.
- `git submodule update --init go/idls cadence-web`.
- `cadence-web`: `npm ci --omit=dev --ignore-scripts` under `work-safe`, exit 0 (log `gate-runs/gatecass-npm.log`).
  `@grpc/grpc-js` is present in `cadence-web/node_modules`.
- Host check before the build: load 6.77 (above 6). Waited for the 5-minute recheck; load then 3.6, MemAvailable 3.1 GB.

## Gate-run artifacts (outside the repo)

- `gate-runs/gatecass/run.log`, `run.exit` (=1), `l4/lane.exit` (=1)
- `gate-runs/gatecass/l4/cassandra-container.log` (314 KB), `cassandra-container-cleanup.txt`
- `gate-runs/gatecass/l4/run/server.log`, `ports.json`, `results.json`, `command.txt`, `rust-dynamic-config.yaml`
- Copied into this directory: `evidence/` (results.json, server.log, run.exit, command.txt, binaries.json, config-hashes.json,
  config-redacted/, EVIDENCE.md, source.json). `EVIDENCE.md` shows `probe exit code: None` and `result: None` as expected.
- `paced-owner.json`: NOT produced (probe not reached). Rust server logs: NOT produced (Rust not started).

## Environment notes (not changed)

- A stray puppeteer Chrome from another project (`/home/appointy/git/whatsapp-agent/data/sessions/session-archiver`, ~1.2 GB,
  12 h old) is still running. It is not part of this task, so it was left alone. Not killed.
- Docker: `local-postgres` and `observ_prometheus_vps` were not touched. Exited `hermes-*` containers belong to someone else and were left.

## Next step (decision for the operator)

The 90 s Go-startup deadline in `tests/run-go-service-baseline.py:918` is the same as in final3. Under this run's load (12 during
startup) Cassandra schema apply plus Go startup was about 71 s before the frontend could listen. Options:
1. Rerun the same command once more when load is under ~4 (the 90 s budget is tight but not exceeded by much). This was not done
   because the instruction was ONE gate.
2. Raise the startup deadline in the harness (a harness change; would need its own commit).
Either way, the probe fixes have not been tested in a gate yet.
