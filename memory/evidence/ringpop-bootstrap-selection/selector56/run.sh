#!/usr/bin/env bash
set -u
root=$(git rev-parse --show-toplevel)
evidence="$root/memory/evidence/ringpop-bootstrap-selection/selector56"
log="$evidence/validation.log"
marker="$evidence/validation.exit"
rm -f "$marker"
cd "$root"
python3 tests/run-ringpop-bootstrap-selection-oracle.py > "$log" 2>&1
status=$?
printf 'go_verify_1=%s\n' "$status" >> "$log"
if [ "$status" -eq 0 ]; then
  python3 tests/run-ringpop-bootstrap-selection-oracle.py >> "$log" 2>&1
  status=$?
  printf 'go_verify_2=%s\n' "$status" >> "$log"
fi
if [ "$status" -eq 0 ]; then
  cd "$root/rust"
  export CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target
  export CARGO_BUILD_JOBS=1
  cargo test --locked -p cadence-common --test ringpop_bootstrap_selection_oracle >> "$log" 2>&1
  status=$?
  printf 'rust_focused_test=%s\n' "$status" >> "$log"
fi
if [ "$status" -eq 0 ]; then
  cargo clippy --locked -p cadence-common --all-targets -- -D warnings >> "$log" 2>&1
  status=$?
  printf 'strict_clippy=%s\n' "$status" >> "$log"
fi
printf '%s\n' "$status" > "$marker"
printf 'FINAL_EXIT=%s\n' "$status" >> "$log"
tail -100 "$log"
