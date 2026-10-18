# Mixed Matching process restart regression — 2026-10-18

## Source and topology

Rust source under test was parent `a1ee92c58a999370e20e3b6e431607c351fdef1d`,
on top of frozen checkpoint `3c8c5f98a8a261eba46ef4a795ce576dd3ac3a76`.
That parent includes the Ringpop JOIN-group implementation in
`rust/crates/cadence-server/src/ringpop_provider.rs`; production startup in
`rust/crates/cadence-server/src/main.rs` does not call the JOIN-group path.
This regression concerns durable Matching RPC startup and makes no JOIN-group
claim.

The existing mixed runner was extended rather than replaced. Its Go test
overlay is [`tests/matching-peer-go_test.go`](../../tests/matching-peer-go_test.go):
it decorates Go discovery for Matching and calls `BuildCLI` with only
`frontend,history,worker`. There is no Go Matching service in either run. The
reused Go test binary is from the completed frozen regression cache at
`matching-frozen-3072698e/full-final/mixed-matching/cadence-server`; Go service
sources and the overlay file are unchanged between that build and this source
parent (the only intervening Go source change is the unrelated TChannel JSON
test server).

The Rust server is the `matching_delivery_go_history` integration executable
from the current source checkout. It owns Matching's SQLite task store and
connects to original Go History. On restart, the probe starts a second process
from that same executable, same fixture/configuration, same SQLite path and
same Matching gRPC address. The existing Go SDK worker communicates with Go
Frontend over its TChannel API.

## Results

Evidence and complete logs are retained outside Git under
`~/.cache/cadence-rust-test-artifacts/matching-process-restart63/`.

* `restart-pass.exit` is **0**. `restart-pass/results.json` is **0** and
  `restart-pass/probe.log` records the mixed process restart pass. Before
  SIGKILL, SQLite contained **1** pending decision-task row for the workflow.
  Rust process PID **867271** was killed; process PID **867277** reopened the
  same database and listener. The existing Go SDK resumed the pending
  `main.EchoWorkflow`, completed its Echo activity, and received
  `restart-proof`. History contained exactly **1** visible workflow completion.
  The original Go services PID **867188** remained alive after the probe.
* `mixed-regression.exit` is **0**. `mixed-regression/results.json` is **0**
  and `mixed-regression/probe.log` records the existing mixed regression pass.
  `mixed-regression/rust-matching/go-sdk.log` confirms existing SDK activity
  retry, child workflow, workflow retry, workflow-task retry, activity timeout,
  and signal scenarios passed. The Rust queue probe also passed its owner idle
  unload/reload, query dispatch, isolation dispatch, and partition config
  checks. Go services PID **868254** remained alive after the probe.

The new restart check allows at-least-once redelivery and does not require a
specific retry count or assert that intermediate delivery cannot repeat. Its
exactly-once assertion is only the final History completion event. SDK
timeout/retry behavior is provided by the adjacent existing mixed regression;
the restart case verifies task replay and completion after process loss.

## Reproduction

With the documented `cadence-web` dependencies installed, run:

```sh
NODE_PATH=/path/to/cadence-web/node_modules \
CARGO_TARGET_DIR=/path/to/completed/rust/target CARGO_BUILD_JOBS=1 \
python3 tests/run-go-service-baseline.py --matching-restart \
  --artifacts-dir /tmp/matching-restart

NODE_PATH=/path/to/cadence-web/node_modules \
CARGO_TARGET_DIR=/path/to/completed/rust/target CARGO_BUILD_JOBS=1 \
python3 tests/run-go-service-baseline.py --rust-matching \
  --artifacts-dir /tmp/mixed-matching
```

Both commands should run in tmux session `cadence-rust`. For this run,
`NODE_PATH` reused the existing dependency tree at
`t3-28143470/cadence-web/node_modules`; no new Node dependency tree was
installed. Cargo used the completed cache at `t3-ce1e72cb/rust/target`.

The first prerequisite attempt failed before starting the probe because the
current checkout had no Node modules. Subsequent attempts exposed and fixed
test-driver setup issues (missing Go `os` import and the SDK's registered
workflow type name); those logs are preserved beside the passing run. No
production Rust code was changed.

## Scope

This closes the specific live Rust Matching process-restart validation gap for
this mixed SQLite/gRPC configuration. It does not complete Matching's parity
audit or establish whole-service replacement. Production discovery/SWIM
startup and the other gaps in the frozen checkpoint remain open. No History
migration work was started.
