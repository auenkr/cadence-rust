#!/usr/bin/env bash
# S3-compatible server helper for the Frontend lanes (S3 archival provider). Source it, then:
#   s3_start   -> when CADENCE_GATE_S3 is true/1: starts a single-node MinIO; appends
#                 --s3-endpoint/--s3-bucket to S3_ARGS (the gate creates the bucket itself)
#   s3_cleanup -> collects logs and stops ONLY this run's server
# MinIO no longer publishes binaries or reachable registry images, so it is built from source
# with the runner's Go toolchain (MinIO verifies SigV4); moto_server is the fallback.
# Needs ARTIFACTS and RUN_ID.
S3_PID=""
S3_CONTAINER=""
S3_IMAGE=${CADENCE_S3_IMAGE:-minio/minio:RELEASE.2024-10-13T13-34-11Z}
S3_ARGS=()
S3_PORT=${CADENCE_S3_PORT:-19000}
S3_VERSION=${CADENCE_S3_VERSION:-RELEASE.2024-10-13T13-34-11Z}
S3_BUCKET=${CADENCE_S3_BUCKET:-cadence-archival-gate}

s3_cleanup() {
  if [[ -n "$S3_PID" ]]; then
    kill "$S3_PID" > /dev/null 2>&1 || true
  fi
  if [[ -n "$S3_CONTAINER" ]]; then
    docker logs "$S3_CONTAINER" > "$ARTIFACTS/minio-server.log" 2>&1 || true
    docker rm -f "$S3_CONTAINER" > /dev/null 2>&1 || true
  fi
}

s3_start() {
  if [[ "${CADENCE_GATE_S3:-}" == "true" || "${CADENCE_GATE_S3:-}" == "1" ]]; then
    local gobin kind=""
    gobin="$(mktemp -d)"
    if [[ "${CADENCE_S3_DOCKER:-}" == "true" ]]; then
      # MinIO container with resource limits (history-s3-archival workflow); falls through to the source build on failure.
      S3_CONTAINER="cadence-gate-minio-$$"
      if docker run -d --name "$S3_CONTAINER" --memory 512m --memory-swap 512m --cpus 0.5 --pids-limit 512 \
          -p "127.0.0.1:${S3_PORT}:9000" -e MINIO_ROOT_USER=cadencegate -e MINIO_ROOT_PASSWORD=cadencegate-secret \
          "$S3_IMAGE" server /data --address ":9000" > "$ARTIFACTS/minio-container.txt" 2>&1; then
        kind=minio-docker
      else
        cat "$ARTIFACTS/minio-container.txt" >&2
        S3_CONTAINER=""
        echo "minio docker start failed; falling back to source build" >&2
      fi
    fi
    if [[ -n "$kind" ]]; then
      :
    elif GOBIN="$gobin" GOFLAGS=-mod=mod go install "github.com/minio/minio@${S3_VERSION}" \
        > "$ARTIFACTS/minio-build.log" 2>&1; then
      kind=minio
    elif pip install --quiet 'moto[server]' > "$ARTIFACTS/moto-install.log" 2>&1; then
      kind=moto
    else
      echo "no S3 server could be installed" >&2
      exit 14
    fi
    echo "$kind" > "$ARTIFACTS/s3-server-kind.txt"
    if [[ "$kind" == minio-docker ]]; then
      :
    elif [[ "$kind" == minio ]]; then
      sha256sum "$gobin/minio" > "$ARTIFACTS/minio-binary.sha256" || true
      MINIO_ROOT_USER=cadencegate MINIO_ROOT_PASSWORD=cadencegate-secret \
        nohup "$gobin/minio" server "$(mktemp -d)" --address "127.0.0.1:${S3_PORT}" --console-address "127.0.0.1:19001" \
        > "$ARTIFACTS/minio-server.log" 2>&1 &
    else
      nohup moto_server -H 127.0.0.1 -p "${S3_PORT}" > "$ARTIFACTS/minio-server.log" 2>&1 &
    fi
    S3_PID=$!
    [[ "$kind" == minio-docker ]] && S3_PID=""
    for attempt in $(seq 1 30); do
      curl -s -o /dev/null "http://127.0.0.1:${S3_PORT}/minio/health/ready" && break
      (( attempt == 30 )) && { echo "S3 server not ready" >&2; exit 15; }
      sleep 2
    done
    if [[ "${CADENCE_S3_READER:-}" == "true" ]]; then
      # s3-reader reads archives with the Go s3store archivers; it must live in the Go module to build.
      local abs_art
      abs_art="$(cd "$ARTIFACTS" && pwd)"
      mkdir -p "$ROOT/go/cmd/tools/s3-reader"
      cp "$ROOT/tests/gates/s3-reader/main.go" "$ROOT/go/cmd/tools/s3-reader/main.go"
      (cd "$ROOT/go" && go build -o "$abs_art/s3-reader" ./cmd/tools/s3-reader) || { echo "s3-reader build failed" >&2; exit 18; }
      rm -rf "$ROOT/go/cmd/tools/s3-reader"
      S3_ARGS+=(--s3-reader "$abs_art/s3-reader")
    fi
    S3_ARGS+=(--s3-endpoint "http://127.0.0.1:${S3_PORT}" --s3-bucket "$S3_BUCKET")
  fi
}
