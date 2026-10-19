#!/usr/bin/env bash
set -u
cd /home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider
target=/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-production-domain-cache/target
attempt=$(date -u +%Y%m%dT%H%M%SZ)-$$
evidence_root=$PWD/memory/evidence/matching-production-domain-cache/attempts
evidence=$evidence_root/$attempt
live_artifacts=/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-production-domain-cache/attempts/$attempt/live
mkdir -p "$evidence" "$(dirname "$target")" "$live_artifacts"
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=$target
git rev-parse HEAD >"$evidence/head.txt"
git diff --binary | sha256sum >"$evidence/diff-sha256.txt"
while IFS= read -r path; do
    [ -f "$path" ] && sha256sum "$path"
done < <(git diff --name-only) >"$evidence/source-sha256.txt"
printf 'gate\tstatus\n' >"$evidence/gates.tsv"
for gate in persistence cache domain-failover server-check server-build clippy go-production-probe persistence-go-interop diff-check; do
    printf '%s\tpending\n' "$gate" >>"$evidence/gates.tsv"
done

stage() {
    name=$1
    shift
    sed -i "s/^$name\tpending$/$name\trunning/" "$evidence/gates.tsv"
    "$@" >"$evidence/$name.log" 2>&1
    result=$?
    printf '%s\n' "$result" >"$evidence/$name.exit"
    if [ "$result" -eq 0 ]; then status=passed; else status=failed; fi
    sed -i "s/^$name\trunning$/$name\t$status/" "$evidence/gates.tsv"
    if [ "$result" -ne 0 ]; then
        exit "$result"
    fi
}

stage persistence env CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/Cargo.toml -p cadence-persistence matching_domain_metadata
stage cache env CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/Cargo.toml -p cadence-server --lib matching_domain_cache
stage domain-failover env CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/Cargo.toml -p cadence-persistence matching_metadata_refresh_updates_full_domain_state_and_publishes_failover
stage server-check env CARGO_BUILD_JOBS=1 cargo check --manifest-path rust/Cargo.toml -p cadence-server --bin cadence-server
stage server-build env CARGO_BUILD_JOBS=1 cargo build --message-format=json-render-diagnostics --manifest-path rust/Cargo.toml -p cadence-server --bin cadence-server
python3 - "$evidence/server-build.log" "$evidence/server-bin.txt" <<'PY'
import json
import sys
from pathlib import Path

build_log, output = map(Path, sys.argv[1:])
for line in build_log.read_text().splitlines():
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        continue
    if event.get("reason") == "compiler-artifact" and event.get("target", {}).get("name") == "cadence-server" and "bin" in event.get("target", {}).get("kind", []):
        executable = event.get("executable")
        if executable:
            output.write_text(executable + "\n")
            break
else:
    raise SystemExit("cargo JSON output did not report cadence-server executable")
PY
stage clippy env CARGO_BUILD_JOBS=1 cargo clippy --manifest-path rust/Cargo.toml -p cadence-persistence -p cadence-server --all-targets -- -D warnings
server_bin=$(cat "$evidence/server-bin.txt")
stage go-production-probe python3 tests/run-matching-thrift-compat.py --server-bin "$server_bin" --artifacts-dir "$live_artifacts"
stage persistence-go-interop env CADENCE_MATCHING_SQLITE_DB="$live_artifacts/go-oracle.sqlite" CARGO_BUILD_JOBS=1 cargo test --manifest-path rust/Cargo.toml -p cadence-persistence go_domain_store_database_interop -- --ignored
stage diff-check git diff --check
