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
