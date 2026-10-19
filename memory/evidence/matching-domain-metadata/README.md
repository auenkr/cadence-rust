# Matching domain metadata evidence

Source commit: `67ea2980b0d8c32c6bd6dd58ace4dcb5cb3d509c`.

The focused persistence tests and shared SQLite oracle were run sequentially
in one `cadence-rust` tmux command. The focused suite passed 8 tests, with the
ignored Go-created database test run by the next script phase. The interop
phase passed Go seed → Rust read-only open/query → Go reopen/verify against one
SQLite file. `focused.exit` and `interop.exit` are copies of the release
wrapper's aggregate exit marker; because the command chains the two phases with
`&&`, exit 0 means both succeeded. Raw logs are retained as `focused.log` and
`interop.log`.

Commands:

```sh
CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/Cargo.toml -p cadence-persistence matching_domain_metadata
CARGO_BUILD_JOBS=1 python3 tests/run-matching-domain-metadata-compat.py
CARGO_BUILD_JOBS=1 cargo clippy --manifest-path rust/Cargo.toml -p cadence-persistence --all-targets -- -D warnings
```

Strict all-target Clippy has an individual exit marker in `clippy.exit` and
raw output in `clippy.log`; it exited 0.
