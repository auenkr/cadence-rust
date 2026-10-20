#!/usr/bin/env bash
set -euo pipefail

# Prepared source-review gate. Run only after the serialized Cargo cache lane
# is released; this script intentionally is not invoked by the review.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${repo_root}/rust/target}"

cd "${repo_root}/rust"
cargo test --locked -p cadence-server --lib matching_shard_distributor::tests -- --nocapture
cargo test --locked -p cadence-server --lib config::tests -- --nocapture
cargo clippy --locked -p cadence-server --all-targets -- -D warnings

cd "${repo_root}/go"
go test ./common/membership -run 'TestShardDistributorResolver_' -count=1 -v
