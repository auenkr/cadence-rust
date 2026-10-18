# Matching frozen-source regression — 2026-10-18

Source was frozen at parent `3072698e18c8fa3f5e67d566a4ce19ae47f37b9c` for
these checks. The worktree started clean. No test harness or implementation
source was edited. Long commands ran in `tmux` session `cadence-rust`, window
`final-validation`, with `CARGO_BUILD_JOBS=1`. Evidence is retained at:

`~/.cache/cadence-rust-test-artifacts/matching-frozen-3072698e/`

## Results

- `full-final/results.json`: all **34/34** accumulated compatibility runner
  checks exited 0. This includes workspace tests and Clippy, Matching Go
  differentials (including memberlist), scheduler/history/config differentials,
  server build/config/listener checks, native Matching Thrift plus TLS, public
  and internal gRPC, the existing Go SDK TChannel flow, Go-service Matching
  delivery, and the mixed deployment probe.
- `full-final/mixed-matching/results.json`: exit 0, topology **Go
  Frontend/History/Worker with Rust Matching RPC**. No Go Matching process is
  run in this scenario. The probe exercises the existing SDK through Go
  Frontend and Matching delivery/workflow activity compatibility.
- `full-final/matching-native-thrift/`: native Go-generated Thrift clients
  passed all ten methods. Plaintext and enforced TLS were exercised. The
  runner's permissive mode also exercised both plaintext and trusted TLS,
  including a stalled TLS client alongside a healthy request. See its
  `results.json` and server/client logs for per-mode evidence.
- `workspace-locked.exit`: 0 for
  `CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/Cargo.toml --workspace --locked`;
  **524 passed**, 0 failed across 21 test binaries (the test log reports one
  ignored test).
- `clippy-locked.exit`: 0 for
  `CARGO_BUILD_JOBS=1 cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked -- -D warnings`.
- `ringpop-tchannel.exit`: 0 for `tests/run-ringpop-tchannel-compat.py`; the
  pinned Go TChannel JSON client passed join, ping, ping-req, application error,
  malformed JSON and unknown method/service cases. Per-run logs are under
  `ringpop-tchannel/`.
- `ringpop-wire.exit`: 0 for `tests/run-ringpop-wire-compat.py`, which verified
  fixture parity with original Go JSON wire models.

The initial full-run attempt exited 1 because `cadence-web/node_modules` was
missing (`@grpc/grpc-js`); the next attempt exposed missing generated local IDL
files. Both were environment prerequisites documented in `tests/README.md`.
After `cd cadence-web && npm ci && npm run install-idl && npm run generate:idl`,
the final runner passed. Initial logs and exit markers are retained beside the
passing run for traceability. `cadence-web` generated/install outputs are local
ignored files, not source edits.

## Scope and remaining gaps

This establishes the requested regression coverage for the integrated frozen
source. It does **not** establish whole Matching service completion. Production
discovery/Ringpop SWIM integration and startup wiring, domain/persistence
integration gaps, configuration gaps, shard-distributor behavior, and other
items in the Matching parity audit remain open. The Ringpop JSON endpoint/core
tests do not prove a production membership provider. Continue Matching work
before beginning History migration.

## Checkpoint `3c8c5f98a` regression — 2026-10-18

Validated the requested frozen source commit
`3c8c5f98a8a261eba46ef4a795ce576dd3ac3a76` (parent
`ddf7ef09792eec27a2fcb0a4d2d5b5bd413c5a95`). The source worktree was clean
before validation. No production or test source was edited. The complete
combined runner ran in `tmux` session `cadence-rust`, window
`frozen3c8c5f98a-complete-run`, with `CARGO_BUILD_JOBS=1` and a warmed external
Cargo target cache.

Final evidence is under
`memory/evidence/matching-frozen-regression-2026-10-18/checkpoint3c8c5f98a/`.
`complete-run/results.json` records **34/34** compatibility stages at exit 0,
including workspace tests, strict workspace all-target Clippy, Matching Go
differentials, server/config/listener checks, native Thrift/TLS, public and
internal gRPC, existing Go SDK TChannel, Go-service Matching delivery, and the
mixed deployment. The locked workspace test log records **568 passed, 0
failed, 2 ignored** across 22 test binaries. The workspace Clippy stage passed
with `-D warnings`.

The mixed stage ran Go Frontend/History/Worker with Rust Matching RPC and no Go
Matching process. Its `mixed-matching/results.json` records a passing probe,
including `queryDispatch`, `isolationDispatch`, and `idleUnloadAndRecovery`.
The captured workflow history contains workflow and activity events. The mixed
Go SDK driver covers activity retry, child workflow, workflow retry,
workflow-task retry, activity timeout, and signal. The separate SDK TChannel
stage also passed. Workspace tests additionally cover Matching poller restart
after History failure, durable task-queue recovery, and idle-owner recovery.
The live mixed probe exercises owner unload/reload recovery; it does not restart
the Rust Matching process while the Go services remain running, so a mixed
process-restart scenario remains unverified.

Supplementary checks outside the 34-stage runner all passed:

* `writer-go-oracle.exit` — exit 0; original Go `taskWriter` produced and
  verified all **9** event traces.
* `selector-go-oracle.exit` — exit 0; the original Go selector oracle verified
  **16** cases. `ringpop-final/selector-rust.log` passed all 4 Rust fixture
  tests, including the 16-case fixture consumer.
* `inbound-ringpop-go-oracle.exit` — exit 0 for the original pinned Go inbound
  JOIN/PING handler oracle. Its generated fixture hash remained
  `73cbefe51f9fe9e171c853e659a74b41d2121fbadf5e502835ccbcbf2f15ee7f`.
* `ringpop-final/outbound-join.exit` and `outbound-ping.exit` — both exit 0;
  Rust outbound JOIN and PING passed against the real pinned Go Ringpop node.
* `ringpop-final/tchannel-boundary.exit` — exit 0; the Go TChannel JSON client
  passed the Rust listener's JOIN, PING, PING-REQ, malformed/unknown method,
  and application-error cases.

Validation environment fixes were limited to reproducible local outputs. The
runner's native Thrift script expects `rust/target/debug/cadence-server` even
when `CARGO_TARGET_DIR` points at a shared cache, so a temporary workspace-local
target symlink was used and removed after validation. The missing documented
`cadence-web` dependency/IDL outputs were installed/generated with the README
commands; `node_modules` was removed after the final run to restore disk space.
The initial failed attempts and their logs remain in this checkpoint directory
for traceability; only `complete-run/results.json` is the final combined-run
result.

This regression validates this checkpoint; it is not a claim that Matching is
complete. Production discovery/SWIM startup and other parity-audit gaps remain
open. No History migration work was started.
