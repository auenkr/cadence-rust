#!/usr/bin/env bash
# Sequential Matching Cassandra validation. Prepare/run in tmux session cadence-rust.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ARTIFACTS=${CADENCE_CASSANDRA_VALIDATION_ARTIFACTS:-"$ROOT/memory/evidence/matching-cassandra-validation-$(date -u +%Y%m%dT%H%M%SZ)"}
CONTAINER=${CADENCE_CASSANDRA_CONTAINER:-cadence-rust-matching-cassandra}
IMAGE=${CADENCE_CASSANDRA_IMAGE:-cassandra:4.1.7}
PORT=${CADENCE_CASSANDRA_PORT:-9042}
KEYSPACE=${CADENCE_CASSANDRA_KEYSPACE:-cadence}
VISIBILITY_KEYSPACE=${CADENCE_CASSANDRA_VISIBILITY_KEYSPACE:-cadence_visibility}
MIN_FREE_KB=${CADENCE_CASSANDRA_MIN_FREE_KB:-18000000}
MIN_MEM_KB=${CADENCE_CASSANDRA_MIN_MEM_KB:-4000000}
mkdir -p "$ARTIFACTS"
[[ "$KEYSPACE" =~ ^[A-Za-z][A-Za-z0-9_]*$ ]] || { echo "Invalid Cassandra keyspace: $KEYSPACE" >&2; exit 2; }
[[ "$VISIBILITY_KEYSPACE" =~ ^[A-Za-z][A-Za-z0-9_]*$ ]] || { echo "Invalid Cassandra visibility keyspace: $VISIBILITY_KEYSPACE" >&2; exit 2; }

phase() {
  local name=$1
  shift
  local log="$ARTIFACTS/$name.log"
  printf '[%s] %q' "$name" "$@"
  printf '\n' >> "$ARTIFACTS/commands.txt"
  printf '%q ' "$@" >> "$ARTIFACTS/commands.txt"
  printf '\n' >> "$ARTIFACTS/commands.txt"
  set +e
  (cd "$ROOT" && "$@") 2>&1 | tee "$log"
  local status=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "$status" > "$ARTIFACTS/$name.exit"
  if (( status != 0 )); then
    printf '[%s] FAILED (%s); see %s\n' "$name" "$status" "$log" >&2
    exit "$status"
  fi
}

# Perform all resource and tool checks before pulling/starting a Cassandra image.
free_kb=$(df -Pk "$ROOT" | awk 'NR == 2 {print $4}')
mem_kb=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
printf 'utc=%s\nroot=%s\nfree_disk_kb=%s\nmem_available_kb=%s\nimage=%s\n' \
  "$(date -u +%FT%TZ)" "$ROOT" "$free_kb" "$mem_kb" "$IMAGE" | tee "$ARTIFACTS/preflight.txt"
if (( free_kb < MIN_FREE_KB )); then
  echo "Need at least ${MIN_FREE_KB} KB free disk before Cassandra image/container setup." >&2
  exit 2
fi
if (( mem_kb < MIN_MEM_KB )); then
  echo "Need at least ${MIN_MEM_KB} KB MemAvailable before Cassandra image/container setup." >&2
  exit 2
fi
for command in cargo rustc go python3 docker; do
  command -v "$command" >/dev/null || { echo "Missing required tool: $command" >&2; exit 2; }
done
cargo --version | tee -a "$ARTIFACTS/preflight.txt"
rustc --version | tee -a "$ARTIFACTS/preflight.txt"
go version | tee -a "$ARTIFACTS/preflight.txt"
docker info >/dev/null
if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "Container $CONTAINER already exists; preserve it and choose CADENCE_CASSANDRA_CONTAINER." >&2
  exit 2
fi

# Resolve the workspace lock separately so the local Scylla patch and all
# newly introduced transitive packages can be reviewed before locked builds.
phase lockfile-generate cargo generate-lockfile --manifest-path rust/Cargo.toml
if ! awk '
  /^name = "scylla"$/ { found=1; in_scylla=1; next }
  /^\[\[package\]\]/ { in_scylla=0 }
  in_scylla && /^version = "1\.9\.0"$/ { version=1 }
  in_scylla && /^source = / { source=1 }
  END { exit !(found && version && !source) }
' rust/Cargo.lock; then
  echo 'Expected locally patched scylla 1.9.0 package without registry source in rust/Cargo.lock.' >&2
  exit 2
fi
if ! git -C "$ROOT" diff HEAD --quiet -- rust/Cargo.lock; then
  git -C "$ROOT" diff HEAD -- rust/Cargo.lock | tee "$ARTIFACTS/Cargo.lock.diff"
  echo 'Review and commit rust/Cargo.lock regeneration, then rerun this script.' >&2
  exit 3
fi

export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
phase focused-cassandra-config-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-persistence --lib cassandra::session_config_tests -- --nocapture
phase focused-cassandra-runtime-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-persistence --lib cassandra::blocking_session_tests -- --nocapture
phase focused-matching-cassandra-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-persistence --lib matching_cassandra::tests -- --nocapture
phase shared-semaphore-backend-tests cargo test --locked --manifest-path rust/Cargo.toml -p cadence-persistence --lib semaphores::tests -- --nocapture
phase semaphore-rpc-test cargo test --locked --manifest-path rust/Cargo.toml -p cadence-server --lib matching_rpc_tests::matching_rpc_semaphore_acquire_grants_and_reports_no_slot -- --exact --nocapture
phase persistence-all-targets-check cargo check --locked --manifest-path rust/Cargo.toml -p cadence-persistence --all-targets
phase strict-persistence-clippy cargo clippy --locked --manifest-path rust/Cargo.toml -p cadence-persistence --all-targets -- -D warnings
phase matching-server-build cargo build --locked --manifest-path rust/Cargo.toml -p cadence-server --bin cadence-server

# Only now provision a pinned single-node test cluster and apply the existing
# Go schema to a dedicated keyspace. This container is disposable test state.
docker run --detach --name "$CONTAINER" --publish "127.0.0.1:${PORT}:9042" \
  --env CASSANDRA_CLUSTER_NAME=cadence-rust-matching-validation "$IMAGE" \
  | tee "$ARTIFACTS/cassandra-container-id.txt"
for attempt in $(seq 1 120); do
  if docker exec "$CONTAINER" cqlsh -e 'DESCRIBE CLUSTER' >/dev/null 2>&1; then break; fi
  if (( attempt == 120 )); then
    docker logs "$CONTAINER" > "$ARTIFACTS/cassandra-startup.log" 2>&1 || true
    echo 'Cassandra did not become ready within 10 minutes.' >&2
    exit 4
  fi
  sleep 5
done
printf "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':1};\nCREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':1};\n" "$KEYSPACE" "$VISIBILITY_KEYSPACE" \
  | docker exec -i "$CONTAINER" cqlsh

docker exec -i "$CONTAINER" cqlsh -k "$KEYSPACE" \
  < "$ROOT/go/schema/cassandra/cadence/schema.cql" 2>&1 | tee "$ARTIFACTS/apply-go-schema.log"
docker exec -i "$CONTAINER" cqlsh -k "$VISIBILITY_KEYSPACE" \
  < "$ROOT/go/schema/cassandra/visibility/schema.cql" 2>&1 | tee "$ARTIFACTS/apply-go-visibility-schema.log"
export CADENCE_CASSANDRA_CONTACT_POINTS=127.0.0.1
export CADENCE_CASSANDRA_KEYSPACE=$KEYSPACE
export CADENCE_CASSANDRA_VISIBILITY_KEYSPACE=$VISIBILITY_KEYSPACE
export CADENCE_CASSANDRA_PORT=$PORT
export CADENCE_CASSANDRA_ARTIFACTS="$ARTIFACTS/go-rust-go"
phase go-seed-rust-restart-go-verify python3 tests/run-matching-cassandra-interop.py

# Mixed production gate: original Go Frontend/History/Worker and two Rust
# Matching nodes share the Go Cassandra schema/keyspaces. No Go Matching starts.
phase mixed-go-services-rust-matching-restart python3 tests/run-matching-production-failure-recovery.py \
  --rust-production-binary "$ROOT/rust/target/debug/cadence-server" \
  --cassandra \
  --artifacts-dir "$ARTIFACTS/mixed-production-recovery"

printf 'All scripted gates passed. Evidence: %s\n' "$ARTIFACTS"
printf '%s\n' 'The mixed service gate uses original Go Frontend/History/Worker and no Go Matching, with Rust Matching on the same Cassandra schema/keyspaces.'
