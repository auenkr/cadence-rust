#!/usr/bin/env bash
set -u
root=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6
attempt="$root/memory/evidence/matching-swim-runtime/attempt-20261019T205810Z"
export CARGO_TARGET_DIR=/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-production-domain-cache/target
export CARGO_BUILD_JOBS=1

run_gate() {
    name=$1
    shift
    printf '%s\t%s\tSTART\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$name" >> "$attempt/gates.tsv"
    "$@" > "$attempt/$name.log" 2>&1
    result=$?
    printf '%s\t%s\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$name" "$result" >> "$attempt/gates.tsv"
    printf '%s\n' "$result" > "$attempt/$name.exit"
    if [ "$result" -ne 0 ]; then
        tail -n 80 "$attempt/$name.log"
        exit "$result"
    fi
}

run_gate common-fingerprint cargo test --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-common --lib membership::fingerprint_tests -- --nocapture
run_gate common-ringpop-member cargo test --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-common --lib ringpop_member -- --nocapture
run_gate common-memberlist cargo test --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-common --lib ringpop_memberlist -- --nocapture
run_gate common-memberlist-oracle cargo test --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-common --test ringpop_memberlist_update_oracle -- --nocapture
run_gate server-lifecycle cargo test --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-server --lib matching_membership_lifecycle -- --nocapture
run_gate server-matching-provider cargo test --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-server --lib matching_ringpop_provider::tests -- --nocapture
run_gate server-ringpop cargo test --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-server --lib ringpop_provider::tests -- --nocapture
run_gate strict-clippy cargo clippy --locked --manifest-path "$root/rust/Cargo.toml" -p cadence-common -p cadence-server --all-targets -- -D warnings
run_gate mixed-go-peer python3 "$root/tests/run-matching-ringpop-go-peer.py"

printf 'Completed gates at %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$attempt/finished.txt"
