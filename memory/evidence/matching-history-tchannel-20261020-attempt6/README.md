# Matching TChannel generated-Go peer gate

Reproduce from the repository root with:

```sh
CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target \
CARGO_BUILD_JOBS=1 scripts/verify-history-tchannel-go-peer.sh
```

The script builds `go/tools/cadence-rust-history-peer` with `GOTOOLCHAIN=go1.25.14`, starts it as a generated `HistoryService` TChannel inbound, and runs the ignored Rust integration test against it. The Rust test sends all five Matching History calls with nonempty request fields. The Go server validates the request mappings and writes YARPC caller, service, transport, procedure, and encoding metadata as JSON. The test asserts nonempty response values, typed shard ownership redirection data, and typed `EntityNotExistsError` / `EventAlreadyStartedError` mappings. The peer subprocess is killed and waited on by the Rust test guard on all exits.

Final run after the source cleanup: exit 0, `1 passed`, log at `rust-go-peer-sourcefinal-pass.log`. The earlier request fixture failure is preserved at `describe-request-validation-failure.log`; its panic correctly identified that the redirect-mode test request was empty. The subsequent final run used populated request data. Exact all-target Clippy passed with `-D warnings`; its output is at `clippy-all-targets-pass.log`. The exact Clippy attempt before moving production-unused response encoders and reverse request decoders under `cfg(test)` is preserved as candidate-error evidence at `clippy-all-targets-baseline-failure.log`.

SHA-256 provenance for that pass:

```text
aafc863ca4287681ff59df1399efa6428597268991d187ac596886a748012d9b  /tmp/cadence-rust-history-peer-final
1db46160fd20f79d6f649878fc8bef21c977d3e687a20fc066aa48ced03c8eb5  cadence-server test executable
a38eb8bacb0046834647b21295bc5d443da802beb6b4c8f12e53ab87c566f665  go/tools/cadence-rust-history-peer/main.go
4dab7f2c4ae844cab1146808ebaf3a76ade8b2974acb15649b74d6279be67610  rust/crates/cadence-server/src/matching_history_tchannel.rs
436b8ebe5e0ec9cb87378861c918c1538bb1b2aed19acb06235ee42db63de910  rust/crates/cadence-server/src/tchannel_json_client.rs
f3d6f2470f6a31c6f585c85168c079873bbbc1758589e32dfeccf5ae2d182c81  scripts/verify-history-tchannel-go-peer.sh
506fb2a4530c56e4be85cee2beb49d409a399e805abe4e42ea429170a7ae2a43  go/go.mod
```
