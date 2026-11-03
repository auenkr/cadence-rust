#!/usr/bin/env bash
# Elasticsearch helper for the Frontend lanes (advanced visibility). Source after lib-kafka.sh, then:
#   es_start   -> when CADENCE_GATE_ES is true/1: starts a limited single-node ES, loads Go's index template,
#                 creates the visibility index and the Kafka visibility topics; appends --es-url/--es-index to ES_ARGS
#   es_cleanup -> collects logs and removes ONLY this run's ES container
# Needs ARTIFACTS, RUN_ID, KAFKA_CONTAINER (Kafka must be running) and ROOT.
ES_CONTAINER=""
ES_ARGS=()
ES_PORT=${CADENCE_ES_PORT:-19200}
ES_IMAGE=${CADENCE_ES_IMAGE:-docker.elastic.co/elasticsearch/elasticsearch:7.17.10}
ES_INDEX=${CADENCE_ES_INDEX:-cadence-visibility-dev}

es_cleanup() {
  if [[ -n "$ES_CONTAINER" ]]; then
    curl -s "http://127.0.0.1:${ES_PORT}/${ES_INDEX}/_mapping" > "$ARTIFACTS/es-final-mapping.json" 2>&1 || true
    curl -s "http://127.0.0.1:${ES_PORT}/${ES_INDEX}/_search?size=20" > "$ARTIFACTS/es-final-docs.json" 2>&1 || true
    docker logs "$ES_CONTAINER" > "$ARTIFACTS/es-container.log" 2>&1 || true
    docker rm --force "$ES_CONTAINER" > "$ARTIFACTS/es-container-cleanup.txt" 2>&1 || true
  fi
}

es_start() {
  if [[ "${CADENCE_GATE_ES:-}" == "true" || "${CADENCE_GATE_ES:-}" == "1" ]]; then
    [[ -n "$KAFKA_CONTAINER" ]] || { echo "CADENCE_GATE_ES requires CADENCE_GATE_KAFKA" >&2; exit 8; }
    ES_CONTAINER="cadence-rust-gate-es-$RUN_ID"
    docker run --detach --name "$ES_CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
      --memory 1800m --memory-swap 1800m --cpus 1.5 --pids-limit 2048 \
      --publish "127.0.0.1:${ES_PORT}:9200" \
      --env discovery.type=single-node --env xpack.security.enabled=false \
      --env ES_JAVA_OPTS="-Xms512m -Xmx512m" \
      "$ES_IMAGE" > "$ARTIFACTS/es-container-id.txt" || exit 9
    for attempt in $(seq 1 60); do
      curl -sf "http://127.0.0.1:${ES_PORT}/_cluster/health?wait_for_status=yellow&timeout=5s" > "$ARTIFACTS/es-health.json" 2>&1 && break
      (( attempt == 60 )) && { echo "Elasticsearch not ready after 5 minutes" >&2; exit 10; }
      sleep 5
    done
    # Go's own template + index (go/schema/elasticsearch/v7/visibility).
    curl -sf -XPUT "http://127.0.0.1:${ES_PORT}/_template/cadence-visibility-template" -H 'Content-Type: application/json' \
      --data-binary "@$ROOT/go/schema/elasticsearch/v7/visibility/index_template.json" > "$ARTIFACTS/es-template.json" 2>&1 \
      || { cat "$ARTIFACTS/es-template.json" >&2; exit 11; }
    curl -sf -XPUT "http://127.0.0.1:${ES_PORT}/${ES_INDEX}" > "$ARTIFACTS/es-index.json" 2>&1 \
      || { cat "$ARTIFACTS/es-index.json" >&2; exit 12; }
    for topic in "$ES_INDEX" "${ES_INDEX}-dlq"; do
      docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 \
        --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 \
        >> "$ARTIFACTS/kafka-es-topics.txt" 2>&1 || exit 13
    done
    ES_ARGS=(--es-url "http://127.0.0.1:${ES_PORT}" --es-index "$ES_INDEX")
  fi
}
