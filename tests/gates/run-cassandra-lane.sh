#!/usr/bin/env bash
# Cassandra lane: Rust Matching + Go Frontend/History/Worker on Go's Cassandra schema.
# Usage: tests/gates/run-cassandra-lane.sh ARTIFACTS_DIR RUST_MATCHING_BINARY [extra harness args]
# Starts a uniquely named, labelled cassandra container, applies Go's schema.cql for
# both keyspaces, runs the harness with --cassandra, then removes ONLY that container.
# It never touches any other container (e.g. local-postgres, cadence-mysql-gate).
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ARTIFACTS=${1:?artifacts dir}; BINARY=${2:?cadence-matching binary}; shift 2
IMAGE=${CADENCE_CASSANDRA_IMAGE:-cassandra:4.1.7}
PORT=${CADENCE_CASSANDRA_PORT:-9042}
KEYSPACE=${CADENCE_CASSANDRA_KEYSPACE:-cadence}
VISIBILITY_KEYSPACE=${CADENCE_CASSANDRA_VISIBILITY_KEYSPACE:-cadence_visibility}
RUN_ID="cassandra-lane-$(date -u +%Y%m%dT%H%M%S)-$$"
CONTAINER="cadence-rust-gate-$RUN_ID"
mkdir -p "$ARTIFACTS"
[[ "$KEYSPACE" =~ ^[A-Za-z][A-Za-z0-9_]*$ && "$VISIBILITY_KEYSPACE" =~ ^[A-Za-z][A-Za-z0-9_]*$ ]] || { echo "bad keyspace name" >&2; exit 2; }
STARTED=0
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if (( STARTED )); then
    docker logs "$CONTAINER" > "$ARTIFACTS/cassandra-container.log" 2>&1 || true
    docker rm --force "$CONTAINER" > "$ARTIFACTS/cassandra-container-cleanup.txt" 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM
uptime; free -h
docker run --detach --name "$CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
  --publish "127.0.0.1:${PORT}:9042" --env MAX_HEAP_SIZE=1G --env HEAP_NEWSIZE=256M \
  "$IMAGE" > "$ARTIFACTS/cassandra-container-id.txt" || exit 3
STARTED=1
for attempt in $(seq 1 120); do
  docker exec "$CONTAINER" cqlsh -e 'DESCRIBE CLUSTER' >/dev/null 2>&1 && break
  (( attempt == 120 )) && { echo "Cassandra not ready after 10 minutes" >&2; exit 4; }
  sleep 5
done
for ks in "$KEYSPACE" "$VISIBILITY_KEYSPACE"; do
  echo "CREATE KEYSPACE IF NOT EXISTS $ks WITH replication = {'class':'SimpleStrategy','replication_factor':1};" | docker exec -i "$CONTAINER" cqlsh || exit 5
done
docker exec -i "$CONTAINER" cqlsh -k "$KEYSPACE" < "$ROOT/go/schema/cassandra/cadence/schema.cql" > "$ARTIFACTS/apply-go-schema.log" 2>&1 || exit 5
docker exec -i "$CONTAINER" cqlsh -k "$VISIBILITY_KEYSPACE" < "$ROOT/go/schema/cassandra/visibility/schema.cql" > "$ARTIFACTS/apply-go-visibility-schema.log" 2>&1 || exit 5
python3 "$ROOT/tests/run-go-service-baseline.py" --rust-services matching --cassandra \
  --cassandra-port "$PORT" --cassandra-keyspace "$KEYSPACE" --cassandra-visibility-keyspace "$VISIBILITY_KEYSPACE" \
  --rust-production-binary "$BINARY" --artifacts-dir "$ARTIFACTS/run" "$@"
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
