#!/usr/bin/env bash
set -u
cd /home/ubuntu/.t3/worktrees/cadence-rust/t3-a91aebfa
EVIDENCE="$PWD/memory/evidence/ringpop-bootstrap-selection/selector57"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target
run_step() {
  local name="$1"; shift
  set +e
  "$@" >"$EVIDENCE/$name.log" 2>&1
  local rc=$?
  set -e
  printf '%s\n' "$rc" >"$EVIDENCE/$name.exit"
  echo "$name exit=$rc"
  if [ "$rc" -ne 0 ]; then cat "$EVIDENCE/$name.log"; exit "$rc"; fi
}
set -e
run_step go-generate python3 tests/run-ringpop-bootstrap-selection-oracle.py --generate
run_step go-run-1 python3 tests/run-ringpop-bootstrap-selection-oracle.py
run_step go-run-2 python3 tests/run-ringpop-bootstrap-selection-oracle.py
run_step rust-focused bash -c 'cd rust && cargo test --locked -p cadence-common --test ringpop_bootstrap_selection_oracle'
run_step common-clippy bash -c 'cd rust && cargo clippy --locked -p cadence-common --all-targets -- -D warnings'
