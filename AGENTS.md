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

Build rules (RAM is the limit, a cadence-server rustc can use 2-3GB):
- Use the shared target dir: `export CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared`.
- At most ONE cargo job host-wide (observed: 2-3 concurrent workspace builds drove load to 12 on 4 cores and filled swap). Every cargo/rustc invocation must be wrapped in the single lock: `flock /tmp/cargo.lock cargo <cmd> -j3`. Never start cargo without the lock; never use another lock file.
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

