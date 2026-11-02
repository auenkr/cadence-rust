#!/usr/bin/env bash
# Rust Frontend + Rust Matching against Go History/Worker on Go's Cassandra schema.
# Usage: run-frontend-cassandra-lane.sh ARTIFACTS_DIR [frontend harness args]
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ARTIFACTS=${1:?artifacts dir}; shift
IMAGE=${CADENCE_CASSANDRA_IMAGE:-cassandra:4.1.7}
PORT=${CADENCE_CASSANDRA_PORT:-9042}
KEYSPACE=${CADENCE_CASSANDRA_KEYSPACE:-cadence}
VISIBILITY_KEYSPACE=${CADENCE_CASSANDRA_VISIBILITY_KEYSPACE:-cadence_visibility}
RUN_ID="frontend-cassandra-lane-$(date -u +%Y%m%dT%H%M%S)-$$"
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
docker run --detach --name "$CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
  --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
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
export CADENCE_CASSANDRA_CONTACT_POINTS=127.0.0.1 CADENCE_CASSANDRA_PORT="$PORT"
export CADENCE_CASSANDRA_KEYSPACE="$KEYSPACE" CADENCE_CASSANDRA_VISIBILITY_KEYSPACE="$VISIBILITY_KEYSPACE"
python3 "$ROOT/tests/gates/run-frontend-gate.py" --database cassandra --out "$ARTIFACTS/run" "$@"
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
