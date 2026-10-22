# Verify integration branch (base head f17c25d95)
Worktree base: f17c25d95ccefc74b0c24b3d90bcdbc7e7801f88; plus one fix (removed no-op drop(put) in cadence-persistence/src/matching_domain_metadata.rs, clippy drop_non_drop).
Setup: git submodule update --init go/idls (needed by cadence-proto build.rs).
- Before fix: flock /tmp/cargo.lock cargo clippy -j2 --workspace --all-targets -- -D warnings -> exit 101 (drop_non_drop)
- After fix: same clippy command -> exit 0
- flock /tmp/cargo.lock cargo test -j2 --workspace -> exit 0 (24 "test result: ok" lines, none with failures)
