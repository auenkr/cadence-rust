#!/usr/bin/env bash
# MinIO helper for the Frontend lanes (S3 archival provider). Source it, then:
#   s3_start   -> when CADENCE_GATE_S3 is true/1: starts a limited single-node MinIO; appends
#                 --s3-endpoint/--s3-bucket to S3_ARGS (the gate creates the bucket itself)
#   s3_cleanup -> collects logs and removes ONLY this run's MinIO container
# Needs ARTIFACTS and RUN_ID.
S3_CONTAINER=""
S3_ARGS=()
S3_PORT=${CADENCE_S3_PORT:-19000}
S3_IMAGE=${CADENCE_S3_IMAGE:-quay.io/minio/minio:RELEASE.2024-10-13T13-34-11Z}
S3_BUCKET=${CADENCE_S3_BUCKET:-cadence-archival-gate}

s3_cleanup() {
  if [[ -n "$S3_CONTAINER" ]]; then
    docker logs "$S3_CONTAINER" > "$ARTIFACTS/minio-container.log" 2>&1 || true
    docker rm --force "$S3_CONTAINER" > "$ARTIFACTS/minio-container-cleanup.txt" 2>&1 || true
  fi
}

s3_start() {
  if [[ "${CADENCE_GATE_S3:-}" == "true" || "${CADENCE_GATE_S3:-}" == "1" ]]; then
    S3_CONTAINER="cadence-rust-gate-minio-$RUN_ID"
    local img chosen=""
    for img in "$S3_IMAGE" quay.io/minio/minio:latest minio/minio:latest docker.io/bitnami/minio:latest; do
      if docker pull "$img" >> "$ARTIFACTS/minio-pull.log" 2>&1; then chosen="$img"; break; fi
    done
    [[ -n "$chosen" ]] || { echo "no MinIO image could be pulled" >&2; exit 14; }
    echo "$chosen" > "$ARTIFACTS/minio-image.txt"
    docker run --detach --name "$S3_CONTAINER" --label "cadence-rust-gate-run=$RUN_ID" \
      --memory 1g --memory-swap 1g --cpus 1 --pids-limit 2048 \
      --publish "127.0.0.1:${S3_PORT}:9000" \
      --env MINIO_ROOT_USER=cadencegate --env MINIO_ROOT_PASSWORD=cadencegate-secret \
      "$chosen" server /data > "$ARTIFACTS/minio-container-id.txt" 2> "$ARTIFACTS/minio-docker-run.err" || exit 14
    for attempt in $(seq 1 30); do
      curl -sf "http://127.0.0.1:${S3_PORT}/minio/health/ready" > /dev/null 2>&1 && break
      (( attempt == 30 )) && { echo "MinIO not ready" >&2; exit 15; }
      sleep 2
    done
    S3_ARGS=(--s3-endpoint "http://127.0.0.1:${S3_PORT}" --s3-bucket "$S3_BUCKET")
  fi
}
