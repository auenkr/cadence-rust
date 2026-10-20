#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
expected="$repo_root/rust/crates/cadence-server/tests/fixtures/history_tchannel_wire_oracle.json"
export GOTOOLCHAIN=go1.25.14
go_version="$(cd "$repo_root/go" && go version)"
case "$go_version" in
  *go1.25.14*) ;;
  *)
    echo "expected pinned Go toolchain go1.25.14 (go.mod toolchain directive); got: $go_version" >&2
    exit 2
    ;;
esac

observed="$(mktemp "${TMPDIR:-/tmp}/history-tchannel-wire-oracle.XXXXXX.json")"
trap 'rm -f "$observed"' EXIT
(
  cd "$repo_root/go"
  CADENCE_THRIFT_WIRE_FIXTURE_OUT="$observed" \
    go test ./common/types/mapper/thrift -run '^TestGenerateHistoryTChannelWireOracle$' -count=1
)
if [[ ! -f "$expected" ]]; then
  echo "Go oracle generated a fresh observed file at: $observed" >&2
  echo "No reviewed expected fixture exists yet: $expected" >&2
  echo "Copy the observed output, review it, and check it in before rerunning this comparison." >&2
  trap - EXIT
  exit 2
fi
diff -u "$expected" "$observed"
echo "Pinned Go History TChannel oracle matches the reviewed fixture."
