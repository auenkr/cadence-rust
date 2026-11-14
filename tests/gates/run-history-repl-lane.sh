#!/usr/bin/env bash
# History replication lane (Go<->Rust History across two clusters) on a disposable DB container.
# Usage: run-history-repl-lane.sh mysql|postgres|cassandra ARTIFACTS_DIR [harness args]
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DBK=${1:?database}; ARTIFACTS=${2:?artifacts dir}; shift 2
RUN_ID="repl-$DBK-$(date -u +%Y%m%dT%H%M%S)-$$"
CONTAINER="cadence-rust-gate-$RUN_ID"
mkdir -p "$ARTIFACTS"
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  docker logs "$CONTAINER" > "$ARTIFACTS/db-container.log" 2>&1 || true
  docker rm --force "$CONTAINER" > "$ARTIFACTS/db-container-cleanup.txt" 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM
LIM=(--memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 --label "cadence-rust-gate-run=$RUN_ID")
DBS=(cadence_a cadence_a_visibility cadence_b cadence_b_visibility)
case "$DBK" in
mysql)
  docker run --detach --name "$CONTAINER" "${LIM[@]}" --publish 127.0.0.1:3306:3306 --env MYSQL_ROOT_PASSWORD=cadence mysql:8.0.36 > "$ARTIFACTS/db-container-id.txt" || exit 3
  for i in $(seq 1 60); do docker exec "$CONTAINER" mysql -uroot -pcadence -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1 && break; (( i == 60 )) && exit 4; sleep 5; done
  for d in "${DBS[@]}"; do docker exec "$CONTAINER" mysql -uroot -pcadence -h127.0.0.1 -e "CREATE DATABASE IF NOT EXISTS $d" || exit 5; done
  export CADENCE_MYSQL_ADDR=127.0.0.1:3306 CADENCE_MYSQL_PASSWORD=cadence ;;
postgres)
  docker run --detach --name "$CONTAINER" "${LIM[@]}" --publish 127.0.0.1:5432:5432 --env POSTGRES_PASSWORD=cadence postgres:13 > "$ARTIFACTS/db-container-id.txt" || exit 3
  for i in $(seq 1 60); do docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break; (( i == 60 )) && exit 4; sleep 5; done
  for d in "${DBS[@]}"; do docker exec "$CONTAINER" createdb -U postgres "$d" || exit 5; done
  export CADENCE_POSTGRES_ADDR=127.0.0.1:5432 CADENCE_POSTGRES_USER=postgres CADENCE_POSTGRES_PASSWORD=cadence ;;
cassandra)
  docker run --detach --name "$CONTAINER" "${LIM[@]}" --publish 127.0.0.1:9042:9042 --env MAX_HEAP_SIZE=1G --env HEAP_NEWSIZE=256M cassandra:4.1.7 > "$ARTIFACTS/db-container-id.txt" || exit 3
  for i in $(seq 1 120); do docker exec "$CONTAINER" cqlsh -e 'DESCRIBE CLUSTER' >/dev/null 2>&1 && break; (( i == 120 )) && exit 4; sleep 5; done
  for d in "${DBS[@]}"; do echo "CREATE KEYSPACE IF NOT EXISTS $d WITH replication = {'class':'SimpleStrategy','replication_factor':1};" | docker exec -i "$CONTAINER" cqlsh || exit 5; done
  export CADENCE_CASSANDRA_CONTACT_POINTS=127.0.0.1 CADENCE_CASSANDRA_PORT=9042 ;;
*) echo "bad database $DBK" >&2; exit 2 ;;
esac
python3 "$ROOT/tests/gates/run-history-repl-gate.py" --database "$DBK" --out "$ARTIFACTS/run" "$@"
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
