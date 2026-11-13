#!/usr/bin/env bash
set -euo pipefail
if [[ "${GITHUB_ACTIONS:-}" != true || "${GITHUB_REPOSITORY:-}" != auenkr/cadence-rust ]]; then
  echo "Builds and mixed gates must run on auenkr/cadence-rust GitHub Actions." >&2
  exit 2
fi
cd "$(dirname "$0")/.."
mkdir -p Benchmark/evidence/run Benchmark/bin
BENCHMARK_SOURCE_DIR="${BENCHMARK_SOURCE_DIR:-$PWD}"
export BENCHMARK_SOURCE_DIR
service="${SERVICE:-matching}"
case "$service" in
  matching|frontend) rust_package=cadence-server ;;
  history) rust_package=cadence-history ;;
  *) echo "Unsupported service: $service" >&2; exit 2 ;;
esac
exec > >(tee Benchmark/evidence/run/ci.log) 2>&1
trap 'echo "$?" > Benchmark/evidence/run/ci.exit' EXIT
python3 -m unittest discover -s Benchmark -p 'test_*.py'
(cd Benchmark && test -z "$(gofmt -l driver)" && go vet ./driver && go build -trimpath -o bin/driver ./driver)
mkdir -p /tmp/cadence-benchmark-go
rsync -a --exclude .git "$BENCHMARK_SOURCE_DIR/go/" /tmp/cadence-benchmark-go/
git -C /tmp/cadence-benchmark-go init -q
git -C /tmp/cadence-benchmark-go add -A
git -C /tmp/cadence-benchmark-go -c user.email=ci@example.com -c user.name=ci commit -qm ci
docker build -f /tmp/cadence-benchmark-go/Dockerfile --target cadence-auto-setup -t cadence-go:ci /tmp/cadence-benchmark-go > Benchmark/evidence/run/go-build.log 2>&1
docker build --no-cache -f Benchmark/Dockerfile --build-arg GO_IMAGE=cadence-go:ci --build-arg RUST_PACKAGE="$rust_package" -t cadence-rust:ci "$BENCHMARK_SOURCE_DIR" > Benchmark/evidence/run/rust-build.log 2>&1
(cd "$BENCHMARK_SOURCE_DIR/go" && go build -p 2 -ldflags='-s -w' -o "$GITHUB_WORKSPACE/Benchmark/bin/cli" ./cmd/tools/cli)
git rev-parse HEAD > Benchmark/evidence/run/head.txt
sha256sum Benchmark/bin/* > Benchmark/evidence/run/drivers.sha256
python3 Benchmark/run.py --service "$service" --rate "${RATE:-5}" --duration "${DURATION:-30}" --warmup "${WARMUP:-10}" --repetitions "${REPETITIONS:-1}" --repetition-offset "${REPETITION_OFFSET:-0}"
