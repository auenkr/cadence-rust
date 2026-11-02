#!/usr/bin/env bash
# Frontend MySQL lane: Rust Matching + Rust Frontend + Go History/Worker on Go's MySQL schema.
# Usage: tests/gates/run-frontend-mysql-lane.sh ARTIFACTS_DIR [run-frontend-gate.py args]
# Starts a uniquely named, labelled mysql container (limits per AGENTS.md), creates the two
# empty databases (Go --auto-setup applies the schema), runs the harness with --mysql, then
# Includes SDK child/backfill/API assertions and a real IPv6 gRPC probe.
# removes ONLY that container. It never touches any other container.
set -uo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ARTIFACTS=${1:?artifacts dir}; shift
IMAGE=${CADENCE_MYSQL_IMAGE:-mysql:8.0.36}
PORT=${CADENCE_MYSQL_PORT:-3306}
PASSWORD=${CADENCE_MYSQL_PASSWORD:-cadence}
RUN_ID="mysql-lane-$(date -u +%Y%m%dT%H%M%S)-$$"
CONTAINER="cadence-rust-gate-$RUN_ID"
mkdir -p "$ARTIFACTS"
STARTED=0
KAFKA_CONTAINER=""
KAFKA_PORT=${CADENCE_KAFKA_PORT:-19092}
KAFKA_IMAGE=${CADENCE_KAFKA_IMAGE:-apache/kafka:3.8.0}
KAFKA_TOPIC=${CADENCE_KAFKA_TOPIC:-cadence-async-gate}
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$KAFKA_CONTAINER" ]]; then
    docker logs "$KAFKA_CONTAINER" > "$ARTIFACTS/kafka-container.log" 2>&1 || true
    docker rm --force "$KAFKA_CONTAINER" > "$ARTIFACTS/kafka-container-cleanup.txt" 2>&1 || true
  fi
  if (( STARTED )); then
    docker exec "$CONTAINER" mysql -uroot -p"$PASSWORD" -h127.0.0.1 --force -e \
      "SELECT shard_id, HEX(id) id_hex, name, is_global, data_encoding, LENGTH(data) data_len, HEX(LEFT(data,24)) data_head FROM cadence.domains; SELECT * FROM cadence.domain_metadata; SELECT HEX(domain_id) d, name, task_type, range_id FROM cadence.task_lists; SELECT HEX(domain_id) d, task_list_name, task_type, task_id FROM cadence.tasks LIMIT 50; SELECT shard_id, HEX(domain_id) d, workflow_id, HEX(run_id) r FROM cadence.executions LIMIT 20; SELECT shard_id, task_id, LENGTH(data) FROM cadence.transfer_tasks LIMIT 30; SELECT shard_id, HEX(tree_id) t, HEX(branch_id) b, node_id, txn_id FROM cadence.history_node LIMIT 40; SELECT workflow_id, close_status FROM cadence_visibility.executions_visibility LIMIT 20;" \
      > "$ARTIFACTS/mysql-domains-dump.txt" 2>&1 || true
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
KAFKA_ARGS=()
if [[ "${CADENCE_GATE_KAFKA:-}" == "true" || "${CADENCE_GATE_KAFKA:-}" == "1" ]]; then
  # Single-node KRaft broker, plaintext, advertised on the published host port (async workflow queue).
  KAFKA_CONTAINER="cadence-rust-gate-kafka-$RUN_ID"
  docker run --detach --name "$KAFKA_CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
    --memory 1g --memory-swap 1g --cpus 1 --pids-limit 2048 \
    --publish "127.0.0.1:${KAFKA_PORT}:9092" \
    --env KAFKA_NODE_ID=1 --env KAFKA_PROCESS_ROLES=broker,controller \
    --env KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
    --env KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://127.0.0.1:${KAFKA_PORT}" \
    --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    --env KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    --env KAFKA_AUTO_CREATE_TOPICS_ENABLE=false --env KAFKA_HEAP_OPTS="-Xms512m -Xmx512m" \
    "$KAFKA_IMAGE" > "$ARTIFACTS/kafka-container-id.txt" || exit 6
  for attempt in $(seq 1 40); do
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 3 --replication-factor 1 \
      > "$ARTIFACTS/kafka-topic-create.txt" 2>&1 && break
    (( attempt == 40 )) && { echo "Kafka not ready / topic create failed" >&2; cat "$ARTIFACTS/kafka-topic-create.txt" >&2; exit 7; }
    sleep 5
  done
  KAFKA_ARGS=(--kafka-broker "127.0.0.1:${KAFKA_PORT}" --kafka-container "$KAFKA_CONTAINER" --kafka-topic "$KAFKA_TOPIC")
fi
export CADENCE_MYSQL_ADDR="127.0.0.1:${PORT}" CADENCE_MYSQL_PASSWORD="$PASSWORD" CADENCE_MYSQL_DOCKER_CONTAINER="$CONTAINER"
python3 "$ROOT/tests/gates/run-frontend-gate.py" --out "$ARTIFACTS/run" "${KAFKA_ARGS[@]}" "$@"
status=$?
echo "$status" > "$ARTIFACTS/lane.exit"
exit "$status"
