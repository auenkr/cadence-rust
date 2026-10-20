#!/usr/bin/env bash
set +e
cd /home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/rust
CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server --lib --no-run > /home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/memory/evidence/matching-history-tchannel-20261020-attempt1/cargo-no-run.raw.log 2>&1
result=$?
printf '%s\n' "$result" > /home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/memory/evidence/matching-history-tchannel-20261020-attempt1/cargo-no-run.exit
exit 0
