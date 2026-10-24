Goal of this project is to rewrite the cadence project into rust version.
Everthing present in go version should be ported to rust version.
ie history, matching, frontend, peristence, thrift/grpc transport, all workflow logic. everything.

In end i should be able to run rust version without require go verion.

go -> Move all the current codebase here (a fix commit i guess)

rust -> new cadence implementation of in rust

memory -> all finding, progress goes into here. So that future agents can reference it

tests -> all compatibility test including existing sdk working

There should no api breaking changes, make sure rust version is compatible with cadence sdk.

No program logic change.

The existing sdk should work for this. 

## SERVICE-BY-SERVICE MIGRATION

Complete one service before starting migration work on the next service.
Current order: Matching -> History -> Frontend -> Worker. Matching is the
current service and remains incomplete.

For each service, port all of its Go behavior and required dependencies,
including persistence, transports, configuration, discovery, lifecycle and
workflow interactions. Shared infrastructure work must serve the current
service's completion; do not spread implementation across future services.

Test the Rust service with the other services still running their original
Go implementations, with no Go instance of the service being replaced.
Verify existing SDK workflows and service-to-service compatibility, including
failure, restart and recovery behavior. Complete the service's parity audit
and record reproducible test evidence in memory before moving to the next.
Passing a few RPCs or an adapter test does not establish service completion.

Subagents may work independently on parts of the current service. Keep
working through its remaining gaps and mixed Go/Rust validation, then repeat
for the next service until the entire cluster runs in Rust without Go.

Use tmux session: cadence-rust for running long process

After each compaction(let compaction be at 230k token, do not execed this), save the progress in memory and commit and push changes to origin

## AGENT ORCHESTRACTION
The orchestrator model will be divinding the work into subagents so things can speed up and task were done indepedently.

For doing actual work use cheap dump model(But give them enugh instruction so they can work independtly)

Why? So that i improve token efficiency. Try to keep minimal token for the orchestrator model as it will be costly.

Worker AGENT:
Like gpt 6 luna, haiku 5.5, or opencode free model(Medium thinking mode). Normal speed mode(Not fast mode)

## NOTE
This machine has 4 CPUs (i5-7300HQ), 7.7GB RAM and ~117GB free on /mnt/storage, so clean up the things which are not needed or saved already.

CI builds (user requirement 2026-10-24): run ALL Cargo commands (build, clippy, test, check and formatting) on GitHub Actions CI/CD runners, never on this slow dev host. This supersedes earlier local quick-iteration and local final-target instructions. Use `gh workflow run rust-ci.yml --ref <branch> [-f package=cadence-matching]`. Final validation must use a fresh, uncached dedicated target directory on the CI runner, with clippy followed by workspace tests; mixed gates must record the Rust binary SHA256. Record run URL, head SHA and conclusion in memory/evidence. Prefer existing GitHub Actions gate workflows for mixed-cluster validation too.

The local Cargo lock, shared-cache and `cargo-safe` guidance below is historical resource guidance, not permission to run Cargo locally. Recovering a failed worker or fixing formatting does not create an exception; dispatch CI for those commands too.

GitHub Actions free-tier rules (user requirement 2026-10-23): stay inside the free tier. Only the repo auenkr/cadence-rust (PUBLIC) may run Actions; public repos get unlimited minutes on the standard hosted runners. Therefore: use only standard runners (`ubuntu-latest`; never larger/GPU/paid runners, never macOS/Windows which are not worth the risk), no paid features (no Codespaces, no paid artifact/cache storage beyond the free quota, keep artifacts small, `retention-days: 3`), keep every job under `timeout-minutes: 90`, keep `concurrency` with cancel-in-progress so superseded pushes are cancelled, and never make the repo private (private repos bill minutes). Workflow files: `.github/workflows/rust-ci.yml` (clippy + test, `workflow_dispatch` with optional `package` input; also runs on pushes to master and `matching-*` branches that touch rust/**). Run it for one crate when possible. The imported upstream Go workflows in `.github/workflows/` (docker_publish, codecov, simulations, ...) are not used by this project: do not trigger them and do not add secrets; if one starts on push, disable it with `gh workflow disable <name>`. Check usage with `gh run list` and cancel obsolete runs with `gh run cancel <id>`.

Worker model rule (user requirement 2026-10-24): use gpt-6-luna workers with medium reasoning and normal speed (`reasoningEffort: medium`, `serviceTier: default`). Use up to 5 subagents, subject to resource limits; this overrides all earlier Haiku-only and fixed-pool requirements. Give each worker a narrow independent task, exact source pointers, CI validation instructions and a concrete done condition.

Branch cleanup (user requirement 2026-10-24): once a branch is merged into master and pushed, delete it everywhere: `git worktree remove --force <wt-path>` (also delete its rust/target), `git branch -d <branch>` (local) and `git push origin --delete <branch>` (remote). Verify first with `git branch -r --merged origin/master` and that the worktree has no uncommitted work and no worker still using it. Never delete master or a branch with unmerged commits; evidence stays in memory/ on master so nothing is lost. Workers must not delete their own branch before the orchestrator has merged it.

Build rules (RAM is the limit, a cadence-server rustc can use 2-3GB):
- Use the shared target dir: `export CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared`.
- At most ONE cargo job host-wide (observed: 2-3 concurrent workspace builds drove load to 12 on 4 cores and filled swap). Every cargo/rustc invocation must be wrapped in the single lock: `flock /tmp/cargo.lock cargo <cmd> -j2` (RAM-bound: -j3 caused swap thrash, load 16). Never start cargo without the lock; never use another lock file.
- Do not run `cargo check --workspace` separately: `cargo clippy --workspace --all-targets -- -D warnings` already type-checks. Run clippy, then `cargo test --workspace`, never both in parallel. For small changes use `-p <crate>` first, full workspace only for the final gate.
- Do not run cargo at all while a mixed-cluster gate (MySQL/PostgreSQL + Go services) is running, and run only one gate at a time. Build the gate binaries first, then start the gate.
- Check load before starting heavy work: `uptime` load above 6 or `free` available below 2GB means wait.
- Do not poll in tight loops (sleep >= 60s).
- Always record the exit code to a file: `cmd > log 2>&1; echo $? > file.exit`.
Overload guard (observed 2026-10-23: load 12, swap full, 150MB RAM free with 5 agents + cargo + a stray puppeteer Chrome using 1.8GB):
- Before starting a build, gate or new worker, run `uptime; free -h`. If load > 6 or available RAM < 2GB, wait or pause the least important cargo-using worker.
- Kill stray processes that are not part of the current task (old puppeteer/chrome, orphan gate processes, stopped gate containers). Always stop gate containers when a gate ends.
- Workers must commit and push their branch at least every 30 minutes. The orchestrator checks `git log origin/<branch>` and `ps` for each worker every ~30 minutes; a worker with no new commit or process activity for 60 minutes is cancelled and its worktree WIP is committed as unverified.
- Steering a worker with a message restarts its turn; avoid steering unless needed.


Subagent count (behaviour, no fixed number in this file):
- The number of subagents is told by the user. If the user has not said how many, ask before starting any.
- If the user says N, keep N subagents running in parallel. When one finishes, immediately schedule a new subagent on the next task so N keep running.
- If the user later reduces the number (e.g. to 2), do not cancel the running ones: let them finish their work, and start new subagents only once fewer than the new number are running.
- Keep following the parallel-limit rules above (one cargo job host-wide, load/RAM checks, commit every 30 minutes).

Do not keep running subagents forever. Sometimes agents get stuck and in status it shows working since 16+hr.
So after assigning Keep checking on subagents weather they are working correctly or not.


Shared build cache caveat (found 2026-10-23): worktrees share one target dir, and cargo can reuse another worktree's workspace-crate artifacts, giving bogus errors or a STALE binary. `/mnt/storage/service/bin/cargo-safe` now touches all workspace sources whenever the workspace changes since the last build (inside the lock), forcing a rebuild of the workspace crates from the current worktree. Evidence for a final pass/fail (gates, final verification) must come from a clean dedicated target dir: `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-final`, one worktree only, and the gate must record the binary's sha256.

Resource protection (user requirement 2026-10-23: workers must not starve the machine's other processes):
- All heavy work runs inside the capped user slice `cadence-work.slice` (MemoryHigh 4G, MemoryMax 4.5G, swap 1G, CPU 300% = 3 of 4 cores; the rest of the machine keeps at least 1 core and ~3GB). `cargo-safe` does this automatically. Anything else heavy (go build/test, gate harness, python/node probes, rust server binaries started by a gate) must be started with `/mnt/storage/service/bin/work-safe <cmd>`, e.g. `flock /tmp/cargo.lock /mnt/storage/service/bin/work-safe python3 tests/run-go-service-baseline.py ...`.
- Docker gate containers must always be started with limits: `--memory <=1800m --memory-swap <same> --cpus <=1.5 --pids-limit 2048` (Cassandra also MAX_HEAP_SIZE=1G). One database container at a time.
- Never run the agents' own tools with high priority; keep `nice -n 10 ionice -c3` for builds. If `uptime` load > 6 or MemAvailable < 2GB, pause new heavy work (cargo-safe already waits).
