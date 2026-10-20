#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

for scenario in "summary default" "histogram default" "summary custom" "histogram custom"; do
  read -r mode config <<<"$scenario"
  go_output="$tmp_dir/go-$mode-$config.txt"
  rust_output="$tmp_dir/rust-$mode-$config.txt"
  (
    cd "$repo_root/go"
    go run ./tools/matching-metrics-prometheus-oracle "$mode" "$config"
  ) >"$go_output"
  cargo run --quiet --manifest-path "$repo_root/rust/Cargo.toml" \
    -p cadence-server --example matching_metrics_prometheus_oracle -- "$mode" "$config" \
    >"$rust_output"
  if ! diff -u "$go_output" "$rust_output"; then
    echo "Prometheus exposition mismatch: timerType=$mode config=$config" >&2
    exit 1
  fi
  echo "matched timerType=$mode config=$config"
done
