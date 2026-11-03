#!/usr/bin/env bash
# Shared Kafka helper for the Frontend lanes. Source it, then:
#   kafka_start   -> sets KAFKA_ARGS (harness flags) and KAFKA_CONTAINER when CADENCE_GATE_KAFKA is true/1
#   kafka_cleanup -> collects logs and removes ONLY this run's Kafka container
# Needs ARTIFACTS and RUN_ID to be set by the caller.
KAFKA_CONTAINER=""
KAFKA_ARGS=()
KAFKA_PORT=${CADENCE_KAFKA_PORT:-19092}
KAFKA_IMAGE=${CADENCE_KAFKA_IMAGE:-apache/kafka:3.8.0}
KAFKA_TOPIC=${CADENCE_KAFKA_TOPIC:-cadence-async-gate}

kafka_cleanup() {
  if [[ -n "$KAFKA_CONTAINER" ]]; then
    docker logs "$KAFKA_CONTAINER" > "$ARTIFACTS/kafka-container.log" 2>&1 || true
    docker rm --force "$KAFKA_CONTAINER" > "$ARTIFACTS/kafka-container-cleanup.txt" 2>&1 || true
  fi
}

kafka_start() {
  if [[ "${CADENCE_GATE_KAFKA:-}" == "true" || "${CADENCE_GATE_KAFKA:-}" == "1" ]]; then
    # Single-node KRaft broker, plaintext, advertised on the published host port (async workflow queue).
    KAFKA_CONTAINER="cadence-rust-gate-kafka-$RUN_ID"
    docker run --detach --name "$KAFKA_CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
      --memory 1g --memory-swap 1g --cpus 1 --pids-limit 2048 \
      --publish "127.0.0.1:${KAFKA_PORT}:9092" \
      --env KAFKA_NODE_ID=1 --env KAFKA_PROCESS_ROLES=broker,controller \
      --env KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9094 \
      --env KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://127.0.0.1:${KAFKA_PORT},INTERNAL://localhost:9094" \
      --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT \
      --env KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
      --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
      --env KAFKA_AUTO_CREATE_TOPICS_ENABLE=false --env KAFKA_HEAP_OPTS="-Xms512m -Xmx512m" \
      "$KAFKA_IMAGE" > "$ARTIFACTS/kafka-container-id.txt" || exit 6
    for attempt in $(seq 1 40); do
      docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 \
        --create --if-not-exists --topic "$KAFKA_TOPIC" --partitions 3 --replication-factor 1 \
        > "$ARTIFACTS/kafka-topic-create.txt" 2>&1 && break
      (( attempt == 40 )) && { echo "Kafka not ready / topic create failed" >&2; cat "$ARTIFACTS/kafka-topic-create.txt" >&2; exit 7; }
      sleep 5
    done
    KAFKA_ARGS=(--kafka-broker "127.0.0.1:${KAFKA_PORT}" --kafka-container "$KAFKA_CONTAINER" --kafka-topic "$KAFKA_TOPIC")
  fi
  if [[ "${CADENCE_GATE_TWO_HISTORY:-}" == "true" ]]; then KAFKA_ARGS+=(--two-history); fi
}
