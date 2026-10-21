#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
cleanup() {
  if [[ -n "${MATCHING_ORACLE_OUTPUT_DIR:-}" ]]; then
    mkdir -p "$MATCHING_ORACLE_OUTPUT_DIR"
    cp -a "$tmp_dir/." "$MATCHING_ORACLE_OUTPUT_DIR/"
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

normalize_labels() {
  python3 - "$1" "$2" <<'PY'
import re
import sys

source, destination = sys.argv[1:]
with open(source, encoding="utf-8") as input_file, open(destination, "w", encoding="utf-8") as output_file:
    for line in input_file:
        match = re.fullmatch(r"([^{}]+)\{([^{}]*)\}(.*)", line.rstrip("\n"))
        if match:
            labels = ",".join(sorted(match.group(2).split(",")))
            line = f"{match.group(1)}{{{labels}}}{match.group(3)}\n"
        output_file.write(line)
PY
}

for scenario in "summary default" "histogram default" "summary custom" "histogram custom"; do
  read -r mode config <<<"$scenario"
  go_output="$tmp_dir/go-$mode-$config.txt"
  rust_output="$tmp_dir/rust-$mode-$config.txt"
  (
    cd "$repo_root/go"
    go run ./tools/matching-metrics-prometheus-oracle "$mode" "$config"
  ) >"$go_output"
  CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:?set CARGO_BUILD_JOBS=1}" \
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:?set the shared CARGO_TARGET_DIR}" \
  cargo run --locked --quiet --manifest-path "$repo_root/rust/Cargo.toml" \
    -p cadence-server --example matching_metrics_prometheus_oracle -- "$mode" "$config" \
    >"$rust_output"
  normalize_labels "$go_output" "$tmp_dir/go-$mode-$config.normalized.txt"
  normalize_labels "$rust_output" "$tmp_dir/rust-$mode-$config.normalized.txt"
  if ! diff -u "$tmp_dir/go-$mode-$config.normalized.txt" "$tmp_dir/rust-$mode-$config.normalized.txt"; then
    echo "Prometheus exposition mismatch: timerType=$mode config=$config" >&2
    exit 1
  fi
  echo "matched timerType=$mode config=$config"
done
