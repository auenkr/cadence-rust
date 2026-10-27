#!/usr/bin/env bash
# MySQL lane: Rust Matching + Go Frontend/History/Worker on Go's MySQL schema.
# Usage: tests/gates/run-mysql-lane.sh ARTIFACTS_DIR RUST_MATCHING_BINARY [extra harness args]
# Starts a uniquely named, labelled mysql container (limits per AGENTS.md), creates the two
# empty databases (Go --auto-setup applies the schema), runs the harness with --mysql, then
# removes ONLY that container. It never touches any other container.
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ARTIFACTS=${1:?artifacts dir}; BINARY=${2:?cadence-matching binary}; shift 2
IMAGE=${CADENCE_MYSQL_IMAGE:-mysql:8.0.36}
PORT=${CADENCE_MYSQL_PORT:-3306}
PASSWORD=${CADENCE_MYSQL_PASSWORD:-cadence}
RUN_ID="mysql-lane-$(date -u +%Y%m%dT%H%M%S)-$$"
CONTAINER="cadence-rust-gate-$RUN_ID"
mkdir -p "$ARTIFACTS"
STARTED=0
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if (( STARTED )); then
    docker logs "$CONTAINER" > "$ARTIFACTS/mysql-container.log" 2>&1 || true
    docker rm --force "$CONTAINER" > "$ARTIFACTS/mysql-container-cleanup.txt" 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM
uptime; free -h
docker run --detach --name "$CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
  --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
  --publish "127.0.0.1:${PORT}:3306" --env MYSQL_ROOT_PASSWORD="$PASSWORD" \
  "$IMAGE" > "$ARTIFACTS/mysql-container-id.txt" || exit 3
STARTED=1
for attempt in $(seq 1 60); do
  docker exec "$CONTAINER" mysql -uroot -p"$PASSWORD" -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1 && break
  (( attempt == 60 )) && { echo "MySQL not ready after 5 minutes" >&2; exit 4; }
  sleep 5
done
docker exec "$CONTAINER" mysql -uroot -p"$PASSWORD" -h127.0.0.1 \
  -e 'CREATE DATABASE IF NOT EXISTS cadence; CREATE DATABASE IF NOT EXISTS cadence_visibility;' || exit 5
export CADENCE_MYSQL_ADDR="127.0.0.1:${PORT}" CADENCE_MYSQL_PASSWORD="$PASSWORD" CADENCE_MYSQL_DOCKER_CONTAINER="$CONTAINER"
if [[ "${CADENCE_MATCHING_GO_CONTROL:-}" == "1" ]]; then
  python3 "$ROOT/tests/run-go-service-baseline.py" --adaptive-go-control --mysql \
    --artifacts-dir "$ARTIFACTS/run" "$@"
else
  python3 "$ROOT/tests/run-go-service-baseline.py" --rust-services matching --mysql \
    --rust-production-binary "$BINARY" --artifacts-dir "$ARTIFACTS/run" "$@"
fi
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
