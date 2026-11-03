#!/usr/bin/env bash
# Two-cluster redirection lane: one MySQL container, schemas cadence_a*/cadence_b*.
# Usage: tests/gates/run-frontend-two-cluster-lane.sh ARTIFACTS_DIR [harness args]
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ARTIFACTS=${1:?artifacts dir}; shift
IMAGE=${CADENCE_MYSQL_IMAGE:-mysql:8.0.36}
PORT=${CADENCE_MYSQL_PORT:-3306}
PASSWORD=${CADENCE_MYSQL_PASSWORD:-cadence}
RUN_ID="two-cluster-$(date -u +%Y%m%dT%H%M%S)-$$"
CONTAINER="cadence-rust-gate-$RUN_ID"
mkdir -p "$ARTIFACTS"
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  docker logs "$CONTAINER" > "$ARTIFACTS/mysql-container.log" 2>&1 || true
  docker rm --force "$CONTAINER" > "$ARTIFACTS/mysql-container-cleanup.txt" 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM
docker run --detach --name "$CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
  --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
  --publish "127.0.0.1:${PORT}:3306" --env MYSQL_ROOT_PASSWORD="$PASSWORD" \
  "$IMAGE" > "$ARTIFACTS/mysql-container-id.txt" || exit 3
for attempt in $(seq 1 60); do
  docker exec "$CONTAINER" mysql -uroot -p"$PASSWORD" -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1 && break
  (( attempt == 60 )) && { echo "MySQL not ready" >&2; exit 4; }
  sleep 5
done
docker exec "$CONTAINER" mysql -uroot -p"$PASSWORD" -h127.0.0.1 \
  -e 'CREATE DATABASE IF NOT EXISTS cadence_a; CREATE DATABASE IF NOT EXISTS cadence_a_visibility; CREATE DATABASE IF NOT EXISTS cadence_b; CREATE DATABASE IF NOT EXISTS cadence_b_visibility;' || exit 5
export CADENCE_MYSQL_ADDR="127.0.0.1:${PORT}" CADENCE_MYSQL_PASSWORD="$PASSWORD"
python3 "$ROOT/tests/gates/run-frontend-two-cluster-gate.py" --out "$ARTIFACTS/run" "$@"
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
