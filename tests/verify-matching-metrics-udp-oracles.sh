#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

run_cargo() {
  CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR="$target_dir" cargo "$@"
}

for scenario in default custom; do
  suffix=()
  if [[ "$scenario" == custom ]]; then
    suffix=(custom)
  fi
  run_cargo run --locked --quiet --manifest-path "$repo_root/rust/Cargo.toml" \
    -p cadence-server --example matching_metrics_statsd_oracle -- "${suffix[@]}" \
    | sort >"$tmp_dir/rust-statsd-$scenario.txt"
  (
    cd "$repo_root/go"
    go run ./tools/matching-metrics-statsd-oracle "${suffix[@]}"
  ) | sort >"$tmp_dir/go-statsd-$scenario.txt"
  diff -u "$tmp_dir/go-statsd-$scenario.txt" "$tmp_dir/rust-statsd-$scenario.txt"
  echo "StatsD UDP semantics matched ($scenario)"
done

run_cargo run --locked --quiet --manifest-path "$repo_root/rust/Cargo.toml" \
  -p cadence-server --example matching_metrics_m3_oracle \
  >"$tmp_dir/rust-m3-packets.hex"
(
  cd "$repo_root/go"
  go run ./tools/matching-metrics-m3-oracle
) >"$tmp_dir/go-m3-batches.jsonl"
(
  cd "$repo_root/go"
  go run ./tools/matching-metrics-m3-oracle decode-hex
) <"$tmp_dir/rust-m3-packets.hex" >"$tmp_dir/rust-m3-batches.jsonl"
python3 "$repo_root/tests/normalize-matching-m3-oracle.py" \
  "$tmp_dir/go-m3-batches.jsonl" "$tmp_dir/go-m3-canonical.jsonl"
python3 "$repo_root/tests/normalize-matching-m3-oracle.py" \
  "$tmp_dir/rust-m3-batches.jsonl" "$tmp_dir/rust-m3-canonical.jsonl"
diff -u "$tmp_dir/go-m3-canonical.jsonl" "$tmp_dir/rust-m3-canonical.jsonl"
echo "M3 v2 UDP semantics matched (two-host fan-out; timestamps and reporter internals normalized)"
