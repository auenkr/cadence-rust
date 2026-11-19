#!/usr/bin/env bash
# Enforced rustfmt checks for rust-ci (single source: the fast `fmt` job and the clippy-test job both call this).
# Usage (from rust/): scripts/ci/rust-fmt-check.sh matching-runtime|frontend|all
# Writes <name>.log/<name>.exit like the former inline steps and exits with rustfmt's status.
set -uo pipefail
matching_runtime=(crates/cadence-matching/src/launcher.rs crates/cadence-matching/src/matching_config.rs crates/cadence-matching/src/matching_runtime.rs crates/cadence-matching/src/matching_registry.rs crates/cadence-matching/src/matching_writer.rs crates/cadence-persistence/src/dynamic_config.rs crates/cadence-service-core/src/admin.rs crates/cadence-service-core/src/openfeature/runtime.rs)
frontend=(crates/cadence-service-core/src/openfeature/unleash_engine.rs crates/cadence-service-core/src/openfeature/unleash.rs crates/cadence-frontend/src/lib.rs crates/cadence-frontend/src/main.rs crates/cadence-frontend/src/config.rs crates/cadence-frontend/src/handler_admin.rs crates/cadence-frontend/src/handler_domain.rs crates/cadence-frontend/src/handler_schedule.rs crates/cadence-frontend/src/handler_workflow.rs crates/cadence-frontend/src/handler_workflow_async.rs crates/cadence-frontend/src/history_client.rs crates/cadence-frontend/src/lifecycle.rs crates/cadence-frontend/src/matching_client.rs crates/cadence-frontend/src/request_validator.rs crates/cadence-frontend/src/wrappers/mod.rs crates/cadence-frontend/src/wrappers/accesscontrolled.rs crates/cadence-frontend/src/wrappers/clusterredirection.rs crates/cadence-frontend/src/wrappers/metered.rs crates/cadence-frontend/src/wrappers/ratelimited.rs crates/cadence-frontend/src/wrappers/thrift.rs crates/cadence-frontend/src/wrappers/versioncheck.rs)
run() { # $1=log stem, rest=files
  local stem=$1; shift
  rustfmt --edition 2021 --check "$@" > "$stem.log" 2>&1; local r=$?
  echo "$r" > "$stem.exit"; cat "$stem.log"; return "$r"
}
case "${1:-all}" in
  matching-runtime) run matching-runtime-format "${matching_runtime[@]}" ;;
  frontend) run fmt "${frontend[@]}" ;;
  all) rc=0; run matching-runtime-format "${matching_runtime[@]}" || rc=$?; run fmt "${frontend[@]}" || rc=$?; exit "$rc" ;;
  *) echo "unknown set: $1" >&2; exit 2 ;;
esac
