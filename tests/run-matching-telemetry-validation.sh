#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go_root="$repo_root/go"
target_dir="/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target"

run_cargo() {
  CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR="$target_dir" cargo "$@"
}

run_cargo test --locked --manifest-path "$repo_root/rust/Cargo.toml" \
  -p cadence-common --lib selection_preserves_go_leak_causes
run_cargo test --locked --manifest-path "$repo_root/rust/Cargo.toml" \
  -p cadence-server --lib report_limit_returns_go_rate_gauge_updates
run_cargo test --locked --manifest-path "$repo_root/rust/Cargo.toml" \
  -p cadence-server --lib matching_metrics
run_cargo test --locked --manifest-path "$repo_root/rust/Cargo.toml" \
  -p cadence-server --lib matching_rpc_standby_completes_started_tasks_without_worker_poll

(
  cd "$go_root"
  go test ./service/matching/handler -run '^TestMatchingPollMetricOracle$' -count=1
  go test ./service/matching/handler \
    -run '^TestMatchingEngineSuite$/^TestAddTaskTrafficMetricOracle$' -count=1
  go test ./common/config -run '^TestMetricsSuite$' -count=1
)

CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR="$target_dir" \
  "$repo_root/scripts/verify-matching-prometheus-oracle.sh"
CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR="$target_dir" \
  "$repo_root/tests/verify-matching-metrics-udp-oracles.sh"

run_cargo clippy --locked --manifest-path "$repo_root/rust/Cargo.toml" \
  --workspace --all-targets -- -D warnings
run_cargo build --locked --manifest-path "$repo_root/rust/Cargo.toml" \
  -p cadence-server --bin cadence-server

echo "Matching telemetry tests, Go reporter comparisons, strict Clippy, and build passed."
