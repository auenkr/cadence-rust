#!/usr/bin/env bash
# Two-cluster redirection lane for postgres|cassandra: one DB container hosting both clusters
# (cadence_a*/cadence_b* databases or keyspaces). MySQL keeps run-frontend-two-cluster-lane.sh.
# Usage: run-frontend-two-cluster-lane-db.sh postgres|cassandra ARTIFACTS_DIR [harness args]
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DB=${1:?postgres|cassandra}; ARTIFACTS=${2:?artifacts dir}; shift 2
RUN_ID="two-cluster-$DB-$(date -u +%Y%m%dT%H%M%S)-$$"
CONTAINER="cadence-rust-gate-$RUN_ID"
mkdir -p "$ARTIFACTS"
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  docker logs "$CONTAINER" > "$ARTIFACTS/$DB-container.log" 2>&1 || true
  docker rm --force "$CONTAINER" > "$ARTIFACTS/$DB-container-cleanup.txt" 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM
case "$DB" in
postgres)
  PORT=${CADENCE_POSTGRES_PORT:-5432}
  docker run --detach --name "$CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
    --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
    --publish "127.0.0.1:${PORT}:5432" --env POSTGRES_PASSWORD=cadence \
    "${CADENCE_POSTGRES_IMAGE:-postgres:13}" > "$ARTIFACTS/$DB-container-id.txt" || exit 3
  for attempt in $(seq 1 60); do
    docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
    (( attempt == 60 )) && { echo "PostgreSQL not ready" >&2; exit 4; }
    sleep 5
  done
  for d in cadence_a cadence_a_visibility cadence_b cadence_b_visibility; do
    docker exec "$CONTAINER" createdb -U postgres "$d" || exit 5
  done
  export CADENCE_POSTGRES_ADDR="127.0.0.1:${PORT}" CADENCE_POSTGRES_USER=postgres CADENCE_POSTGRES_PASSWORD=cadence
  ;;
cassandra)
  PORT=${CADENCE_CASSANDRA_PORT:-9042}
  docker run --detach --name "$CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
    --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
    --publish "127.0.0.1:${PORT}:9042" --env MAX_HEAP_SIZE=1G --env HEAP_NEWSIZE=256M \
    "${CADENCE_CASSANDRA_IMAGE:-cassandra:4.1.7}" > "$ARTIFACTS/$DB-container-id.txt" || exit 3
  for attempt in $(seq 1 120); do
    docker exec "$CONTAINER" cqlsh -e 'DESCRIBE CLUSTER' >/dev/null 2>&1 && break
    (( attempt == 120 )) && { echo "Cassandra not ready" >&2; exit 4; }
    sleep 5
  done
  for ks in cadence_a cadence_a_visibility cadence_b cadence_b_visibility; do
    echo "CREATE KEYSPACE IF NOT EXISTS $ks WITH replication = {'class':'SimpleStrategy','replication_factor':1};" | docker exec -i "$CONTAINER" cqlsh || exit 5
  done
  export CADENCE_CASSANDRA_CONTACT_POINTS=127.0.0.1 CADENCE_CASSANDRA_PORT="$PORT"
  ;;
*) echo "unsupported db $DB" >&2; exit 2;;
esac
python3 "$ROOT/tests/gates/run-frontend-two-cluster-gate.py" --database "$DB" --out "$ARTIFACTS/run" "$@"
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
