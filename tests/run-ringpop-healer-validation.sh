#!/usr/bin/env bash
# Frozen, serialized validation lane for the Matching Ringpop healer candidate.
# Prepare now; run only after the build/cache lane is explicitly released.
set -u -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE="${RINGPOP_VALIDATION_BASE:-8b414dd19}"
TARGET="$ROOT/rust/target"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${RINGPOP_VALIDATION_OUT:-$ROOT/memory/evidence/ringpop-healer-validation-$STAMP}"

mkdir -p "$OUT"
STATUS="$OUT/commands.tsv"
printf 'status\texit\tcommand\tlog\n' > "$STATUS"

if ! git -C "$ROOT" diff --quiet || ! git -C "$ROOT" diff --cached --quiet; then
  echo "Refusing to validate a dirty worktree; commit the candidate first." >&2
  exit 2
fi
if ! tmux has-session -t cadence-rust 2>/dev/null; then
  echo "Required tmux session cadence-rust is not running." >&2
  exit 2
fi
if [[ ! -d "$TARGET" ]]; then
  echo "The sole warmed target directory is missing: $TARGET" >&2
  exit 2
fi
if ! git -C "$ROOT" cat-file -e "$BASE^{commit}" 2>/dev/null; then
  echo "Validation base commit is unavailable: $BASE" >&2
  exit 2
fi

export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR="$TARGET"

record_source_hash() {
  local phase="$1"
  local hash
  hash="$(git -C "$ROOT" diff --no-ext-diff --binary "$BASE..HEAD" | sha256sum | awk '{print $1}')"
  printf '%s\t%s\t%s\n' "$phase" "$(git -C "$ROOT" rev-parse HEAD)" "$hash" >> "$OUT/source-hashes.tsv"
}

record_source_hash before

run_command() {
  local name="$1"
  shift
  local log="$OUT/$name.log"
  local command
  printf -v command '%q ' "$@"
  echo "[$name] $command"
  (cd "$ROOT" && "$@") > "$log" 2>&1
  local result=$?
  cat "$log"
  printf 'run\t%s\t%s\t%s\n' "$result" "$command" "$log" >> "$STATUS"
  return 0
}

# The Go runner is comparison-only. Never pass --update in this lane.
run_command go_healer_oracle python3 tests/run-ringpop-healer-go-oracle.py
run_command rust_healer_fixture cargo test --manifest-path rust/Cargo.toml -p cadence-server --lib go_healer_expected_trace
run_command rust_partition_heal cargo test --manifest-path rust/Cargo.toml -p cadence-server --lib partition_heal_joins_two_live_protocol_peers_and_converges_membership
run_command rust_reverse_full_sync cargo test --manifest-path rust/Cargo.toml -p cadence-server --lib receiver_full_sync_starts_reverse_join_and_converges_two_live_peers
run_command rust_shared_tchannel_provider cargo test --manifest-path rust/Cargo.toml -p cadence-server --lib two_shared_tchannel_providers_discover_roles_and_self_evict
run_command rust_memberlist_refutation cargo test --manifest-path rust/Cargo.toml -p cadence-common --lib remote_acceptance_refutation_tombstone_and_checksum
run_command rust_memberlist_go_trace cargo test --manifest-path rust/Cargo.toml -p cadence-common --lib single_remote_iterator_round_matches_pinned_go_trace
run_command rust_discovery_provider_regressions cargo test --manifest-path rust/Cargo.toml -p cadence-server --lib dns_provider_matches_go_cache_errors_and_host_port_formatting

# The current Rust source has no blocked-JOIN/healer-stop test. Do not count the
# Go oracle's `stopWaitsForActiveHeal` trace field as Rust implementation proof.
printf 'missing\tNA\tRust blocked-JOIN healer stop test is absent; add it before claiming this lane complete\tNA\n' >> "$STATUS"
echo "MISSING: Rust blocked-JOIN healer-stop regression test (Go oracle coverage is not Rust evidence)." >&2

run_command strict_common_server_clippy cargo clippy --manifest-path rust/Cargo.toml -p cadence-common -p cadence-server --all-targets -- -D warnings

record_source_hash after

if awk -F '\t' 'NR > 1 && ($1 == "run" && $2 != 0 || $1 == "missing") { failed = 1 } END { exit !failed }' "$STATUS"; then
  echo "Validation has failures or missing required coverage; see $OUT/commands.tsv" >&2
  exit 1
fi
echo "All serialized validation commands passed. Evidence: $OUT"
