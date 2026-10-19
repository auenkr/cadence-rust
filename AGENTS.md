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

Worker AGENT:
Like gpt 6 luna, or opencode free model. Normal speed mode(Not fast mode)

## NOTE
This machine have only 100GB storage, so clean up the things which are not needed or saved already.
Do not keep running subagents forever. Sometimes agents get stuck and in status it shows working since 16+hr.
So after assigning Keep checking on subagents weather they are working correctly or not.
