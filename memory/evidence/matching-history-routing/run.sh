#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
cd "$root"
evidence="$root/memory/evidence/matching-history-routing"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
attempt=$(mktemp -d "$evidence/attempt-$stamp-XXXXXX")
# Reuse the domain-cache dependency target after its owner releases the slot.
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-production-domain-cache/target}"

sources=(
  rust/Cargo.lock
  rust/crates/cadence-server/Cargo.toml
  rust/crates/cadence-server/src/matching_history.rs
  rust/crates/cadence-server/src/matching_history_peer_resolver.rs
  rust/crates/cadence-server/src/matching_history_membership.rs
  rust/crates/cadence-server/src/matching_dispatch.rs
  rust/crates/cadence-server/src/grpc_matching.rs
  rust/crates/cadence-common/src/membership.rs
  rust/crates/cadence-common/src/ringpop_member.rs
  tests/matching-membership-go_test.go
  tests/run-matching-membership-compat.py
  tests/matching-history-peer-oracle/run.py
  tests/matching-history-peer-oracle/oracle_test.go
  tests/matching-history-peer-oracle/evidence/matching_history_peer_oracle.json
  tests/matching-history-peer-oracle/evidence/source-sha256.json
)

git rev-parse HEAD > "$attempt/source-head.txt"
git status --short -- "${sources[@]}" > "$attempt/source-status.before.txt"
if [[ -s "$attempt/source-status.before.txt" ]]; then
  echo "Relevant source is dirty; refusing non-reproducible routing validation." >&2
  cat "$attempt/source-status.before.txt" >&2
  exit 2
fi
sha256sum "${sources[@]}" > "$attempt/source-sha256.before.txt"
cp tests/matching-history-peer-oracle/evidence/matching_history_peer_oracle.json "$attempt/oracle.expected.json"
cp tests/matching-history-peer-oracle/evidence/source-sha256.json "$attempt/oracle.expected-source-sha256.json"

stage() {
  local name=$1
  shift
  printf '$'
  printf ' %q' "$@"
  printf '\n'
  "$@" > "$attempt/$name.log" 2>&1 || {
    local status=$?
    printf '%s\n' "$status" > "$attempt/$name.exit"
    cat "$attempt/$name.log"
    exit "$status"
  }
  printf '0\n' > "$attempt/$name.exit"
}

stage diff-check git diff --check
stage common-farmhash-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-common fingerprint_tests -- --nocapture
stage matching-membership-go-oracle python3 tests/run-matching-membership-compat.py
stage ringpop-member-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-common ringpop_member::tests -- --nocapture
stage strict-common-clippy cargo clippy --locked --manifest-path rust/Cargo.toml -p cadence-common --all-targets -- -D warnings
stage history-client-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-server matching_history::tests -- --nocapture
stage history-peer-resolver-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-server matching_history_peer_resolver::tests -- --nocapture
stage history-membership-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-server matching_history_membership::tests -- --nocapture
stage dispatch-regression-test cargo test --locked --manifest-path rust/Cargo.toml -p cadence-server matching_dispatch::tests::matching_dispatch_poller_wait_ends_before_history_and_restarts_after_failure -- --exact --nocapture
stage strict-clippy cargo clippy --locked --manifest-path rust/Cargo.toml -p cadence-server --all-targets -- -D warnings
stage go-history-peer-oracle python3 tests/matching-history-peer-oracle/run.py
cmp "$attempt/oracle.expected.json" tests/matching-history-peer-oracle/evidence/matching_history_peer_oracle.json
cmp "$attempt/oracle.expected-source-sha256.json" tests/matching-history-peer-oracle/evidence/source-sha256.json
printf '0\n' > "$attempt/go-history-peer-oracle-equivalence.exit"

sha256sum "${sources[@]}" > "$attempt/source-sha256.after.txt"
if ! cmp -s "$attempt/source-sha256.before.txt" "$attempt/source-sha256.after.txt"; then
  echo "Relevant source changed during validation." >&2
  diff -u "$attempt/source-sha256.before.txt" "$attempt/source-sha256.after.txt" >&2 || true
  exit 3
fi
git status --short -- "${sources[@]}" > "$attempt/source-status.after.txt"
if [[ -s "$attempt/source-status.after.txt" ]]; then
  echo "Relevant source became dirty during validation." >&2
  cat "$attempt/source-status.after.txt" >&2
  exit 3
fi
echo "$attempt"
