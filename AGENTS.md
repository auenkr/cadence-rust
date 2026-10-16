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

Use tmux session: cadence-rust for running long process

After each compaction(let compaction be at 230k token, do not execed this), save the progress in memory and commit and push changes to origin


