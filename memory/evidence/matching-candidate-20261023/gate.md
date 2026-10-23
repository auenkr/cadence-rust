# Matching candidate gate 2026-10-23
Merged head (pre-evidence): d8bfbec53b8ca060f8736360dec25c5b98113595
Merges: parity-fixes-rebased (8c6940f87), partition-config-v2 (18039b966), grpc-tls-v2 (3de4f1339); no conflicts; single drop(put) removal retained.
Commands (CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared, from rust/):
- flock /tmp/cargo.lock cargo clippy -j2 --workspace --all-targets -- -D warnings -> exit 0
- flock /tmp/cargo.lock cargo test -j2 --workspace -> exit 0
Test totals: 876 passed, 0 failed, 17 ignored
