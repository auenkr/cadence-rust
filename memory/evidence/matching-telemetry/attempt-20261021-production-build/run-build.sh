#!/usr/bin/env bash
set -o pipefail
cd /home/ubuntu/.t3/worktrees/cadence-rust/matching-telemetry/rust
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target cargo build --locked --package cadence-server --bin cadence-server 2>&1 | tee /home/ubuntu/.t3/worktrees/cadence-rust/matching-telemetry/memory/evidence/matching-telemetry/attempt-20261021-production-build/build.log
result=${PIPESTATUS[0]}
printf '%s\n' "$result" > /home/ubuntu/.t3/worktrees/cadence-rust/matching-telemetry/memory/evidence/matching-telemetry/attempt-20261021-production-build/build.exit
if [ "$result" -eq 0 ]; then
  sha256sum /home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target/debug/cadence-server > /home/ubuntu/.t3/worktrees/cadence-rust/matching-telemetry/memory/evidence/matching-telemetry/attempt-20261021-production-build/build-binary.sha256
fi
exit "$result"
