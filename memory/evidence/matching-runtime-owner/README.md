# Matching runtime owner validation

Branch: `t3/matching-runtime-owner`, based on `87e3f1352`.

Run the Cargo commands from the repository root using a subshell rooted at `rust`; the recorded runs used the absolute target directory `/home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/rust/target`. The commands below reproduce that layout without depending on this worktree's absolute path. Run the Python compatibility commands from the repository root. All recorded final exit markers are `0`.

- `(cd rust && CARGO_TARGET_DIR="$PWD/target" CARGO_BUILD_JOBS=1 cargo check --locked -p cadence-server --bin cadence-server)` — `server-check-run.log`.
- `(cd rust && CARGO_TARGET_DIR="$PWD/target" CARGO_BUILD_JOBS=1 cargo build --locked -p cadence-server --bin cadence-server)` — `server-build-run2.log`; used by Go compatibility runner.
- `(cd rust && CARGO_TARGET_DIR="$PWD/target" CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server --lib matching_runtime::tests)` — `focused-run.log` (3 passed).
- `python3 tests/run-matching-thrift-compat.py --server-bin rust/target/debug/cadence-server --artifacts-dir memory/evidence/matching-runtime-owner/thrift-artifacts-final` — `thrift-run3.log`. Verifies generated Go Thrift calls on TChannel and HTTP modes, configured five-second shutdown drain with a generated-Go health RPC after repeated SIGTERM, occupied-listener startup failure cleanup, and TLS/plaintext HTTP behavior.
- `python3 tests/run-combined-matching-ringpop-tchannel.py` — `combined-run2.log`. Go generated Thrift Matching and Ringpop JSON both call the same Rust TChannel port.
- `(cd rust && CARGO_TARGET_DIR="$PWD/target" CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-server --all-targets -- -D warnings)` — `clippy-run.log`.

Failed harness attempts are retained as logs (`thrift-run*.log`, `combined-run.log`) to document the resolved overlay-path and Go test API issues. Generated Go test binaries, Rust binaries, TLS keys/certs, config copies, and local DB files are excluded; the scripts regenerate them.

Source hashes at validation freeze:

```
52153358a6af3709e1789d09d3823c7eeb24e1356febc0b5a711445cce2301fb  rust/crates/cadence-server/src/matching_runtime.rs
bd7c3d69a03e8eea8996535ecb582cf12b62378a156101e6b503ddc63ef81e88  rust/crates/cadence-server/src/main.rs
657be0d86c50103685dd5d2948bfc2068fbac1538e01e2e4257a72f32b3e9699  rust/crates/cadence-server/src/tchannel.rs
fb87d619945eed171f65b783428dd65b91f018842ce87cb0e6b72d4005ed64cd  tests/matching-thrift-go_test.go
53159014c9b55442ad35181f806bc5139c610738d5e30993b31b8aa8d207aab1  tests/combined-matching-ringpop-go_test.go
```

This validates the bounded runtime and transport slice, not full Matching parity. Production membership/Ringpop provider and eviction lifecycle, shared production domain-cache integration, and configured History discovery remain absent from the production assembly. No mixed Go/Rust cluster workflow, restart/recovery, or failure parity claim is made here.
