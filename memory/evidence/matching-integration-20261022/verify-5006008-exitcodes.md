# Integration verify, head 5006008193b08e3d5e51b6f37a28126dcdaf954e
Worktree wt/verify2, CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared, flock /tmp/cargo.lock, -j2.
- cargo clippy -j2 --workspace --all-targets -- -D warnings > /tmp/v2-clippy.log ; exit file /tmp/v2-clippy.exit = 0
- cargo test -j2 --workspace > /tmp/v2-test.log ; exit file /tmp/v2-test.exit = 0 (no FAILED/panicked lines)
Result: clippy and workspace tests green on this exact head. Not a mixed Go/Rust result.
