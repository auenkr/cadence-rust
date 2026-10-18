#!/usr/bin/env bash
set -u
root=$(pwd)
mkdir -p "$root/memory/evidence/ringpop-bootstrap-selection"
log="$root/memory/evidence/ringpop-bootstrap-selection/selector52.log"
marker="$root/memory/evidence/ringpop-bootstrap-selection/selector52.exit"
rm -f "$marker"
python3 tests/run-ringpop-bootstrap-selection-oracle.py > "$log" 2>&1
status=$?
printf 'verify1=%s\n' "$status" >> "$log"
if [ "$status" -eq 0 ]; then
  python3 tests/run-ringpop-bootstrap-selection-oracle.py >> "$log" 2>&1
  status=$?
  printf 'verify2=%s\n' "$status" >> "$log"
fi
if [ "$status" -eq 0 ]; then
  cd "$root/rust"
  CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-common --test ringpop_bootstrap_selection_oracle >> "$log" 2>&1
  status=$?
  printf 'rust_test=%s\n' "$status" >> "$log"
fi
if [ "$status" -eq 0 ]; then
  CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-common --all-targets -- -D warnings >> "$log" 2>&1
  status=$?
  printf 'clippy=%s\n' "$status" >> "$log"
fi
printf '%s\n' "$status" > "$marker"
tail -80 "$log"
