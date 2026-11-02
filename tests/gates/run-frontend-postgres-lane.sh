#!/usr/bin/env bash
# Disposable PostgreSQL lane on the original Go schema; removes only its own container.
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ARTIFACTS=${1:?artifacts dir}; shift
PORT=${CADENCE_POSTGRES_PORT:-5432}
RUN_ID="postgres-lane-$(date -u +%Y%m%dT%H%M%S)-$$"
CONTAINER="cadence-rust-gate-$RUN_ID"
mkdir -p "$ARTIFACTS"
STARTED=0
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if (( STARTED )); then
    docker exec "$CONTAINER" psql -U postgres -d cadence_visibility -c 'SELECT workflow_id, close_status FROM executions_visibility LIMIT 20' > "$ARTIFACTS/postgres-visibility.txt" 2>&1 || true
    docker logs "$CONTAINER" > "$ARTIFACTS/postgres-container.log" 2>&1 || true
    docker rm --force "$CONTAINER" > "$ARTIFACTS/postgres-container-cleanup.txt" 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM
docker run --detach --name "$CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
  --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
  --publish "127.0.0.1:${PORT}:5432" --env POSTGRES_PASSWORD=cadence \
  "${CADENCE_POSTGRES_IMAGE:-postgres:13}" > "$ARTIFACTS/postgres-container-id.txt" || exit 3
STARTED=1
for attempt in $(seq 1 60); do
  docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
  (( attempt == 60 )) && { echo 'PostgreSQL not ready after five minutes' >&2; exit 4; }
  sleep 5
done
for database in cadence cadence_visibility; do
  docker exec "$CONTAINER" createdb -U postgres "$database" || exit 5
done
export CADENCE_POSTGRES_DOCKER_CONTAINER="$CONTAINER"
export CADENCE_POSTGRES_ADDR="127.0.0.1:${PORT}" CADENCE_POSTGRES_USER=postgres
export CADENCE_POSTGRES_PASSWORD=cadence CADENCE_POSTGRES_DATABASE=cadence
export CADENCE_POSTGRES_VISIBILITY_DATABASE=cadence_visibility CADENCE_POSTGRES_SSLMODE=disable
python3 "$ROOT/tests/gates/run-frontend-gate.py" --database postgres --out "$ARTIFACTS/run" "$@"
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
