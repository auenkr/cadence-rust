#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/cadence-history-go-peer.XXXXXX")"
trap 'rm -rf "$build_dir"' EXIT

go_bin="${GO:-go}"
cargo_bin="${CARGO:-cargo}"
target_dir="${CARGO_TARGET_DIR:-/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target}"

(
  cd "$repo_root/go"
  GOTOOLCHAIN=go1.25.14 "$go_bin" build -o "$build_dir/cadence-rust-history-peer" ./tools/cadence-rust-history-peer
)
(
  cd "$repo_root/rust"
  CADENCE_HISTORY_PEER_BIN="$build_dir/cadence-rust-history-peer" \
    CARGO_TARGET_DIR="$target_dir" CARGO_BUILD_JOBS=1 \
    "$cargo_bin" test --locked -p cadence-server \
      --lib \
      matching_history_tchannel::typed_codec_tests::generated_go_history_server_accepts_native_tchannel_calls \
      -- --ignored --exact --nocapture
)
