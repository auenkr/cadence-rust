#!/usr/bin/env bash
# fake-gcs-server helper for the Frontend lanes (GCS archival provider). Source it, then:
#   gcs_start   -> when CADENCE_GATE_GCS is true/1: starts fake-gcs-server (docker, resource limited), builds the
#                  gcs-seed Go helper, exports STORAGE_EMULATOR_HOST and appends --gcs-* to GCS_ARGS
#   gcs_cleanup -> collects logs and stops ONLY this run's container
# Needs ARTIFACTS and ROOT.
GCS_ARGS=()
GCS_CONTAINER=""
GCS_PORT=${CADENCE_GCS_PORT:-14443}

gcs_cleanup() {
  if [[ -n "$GCS_CONTAINER" ]]; then
    docker logs "$GCS_CONTAINER" > "$ARTIFACTS/fake-gcs-server.log" 2>&1 || true
    docker rm -f "$GCS_CONTAINER" > /dev/null 2>&1 || true
  fi
}

gcs_start() {
  if [[ "${CADENCE_GATE_GCS:-}" == "true" || "${CADENCE_GATE_GCS:-}" == "1" ]]; then
    ARTIFACTS="$(cd "$ARTIFACTS" && pwd)"
    GCS_CONTAINER="cadence-gate-gcs-$$"
    docker run -d --name "$GCS_CONTAINER" --memory 512m --memory-swap 512m --cpus 0.5 --pids-limit 512 \
      -p "127.0.0.1:${GCS_PORT}:4443" fsouza/fake-gcs-server:1.50.2 \
      -scheme http -port 4443 -backend memory -public-host "127.0.0.1:${GCS_PORT}" -external-url "http://127.0.0.1:${GCS_PORT}" \
      > "$ARTIFACTS/gcs-container.txt" 2>&1 || { cat "$ARTIFACTS/gcs-container.txt" >&2; exit 16; }
    docker image inspect fsouza/fake-gcs-server:1.50.2 --format '{{.Id}}' > "$ARTIFACTS/fake-gcs-server.image" || true
    # gcs-seed computes Go gstorage history object names (farmhash fingerprints); it must live in the Go module.
    mkdir -p "$ROOT/go/cmd/tools/gcs-seed"
    cp "$ROOT/tests/gates/gcs-seed/main.go" "$ROOT/go/cmd/tools/gcs-seed/main.go"
    (cd "$ROOT/go" && go build -o "$ARTIFACTS/gcs-seed" ./cmd/tools/gcs-seed) || { echo "gcs-seed build failed" >&2; exit 17; }
    rm -rf "$ROOT/go/cmd/tools/gcs-seed"
    for attempt in $(seq 1 30); do
      curl -s -o /dev/null "http://127.0.0.1:${GCS_PORT}/storage/v1/b?project=p" && break
      (( attempt == 30 )) && { echo "fake-gcs-server not ready" >&2; exit 18; }
      sleep 2
    done
    export STORAGE_EMULATOR_HOST="127.0.0.1:${GCS_PORT}"
    GCS_ARGS=(--gcs-endpoint "http://127.0.0.1:${GCS_PORT}" --gcs-seed "$(cd "$ARTIFACTS" && pwd)/gcs-seed")
  fi
}
