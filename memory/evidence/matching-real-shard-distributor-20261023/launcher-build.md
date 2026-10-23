# Real shard-distributor launcher: build evidence (2026-10-23)

Host rules: all go commands under `flock /tmp/cargo.lock nice -n 10 ionice -c3`, `GOFLAGS=-p=2 GOMAXPROCS=2`.

| Command (cwd go/) | Exit |
| --- | --- |
| `go get go.etcd.io/etcd/client/v3@v3.5.5 go.etcd.io/etcd/api/v3@v3.5.5` (needed network; etcd not in module cache) | 0 |
| `go build -o /tmp/sdreal-launcher ./tests/shard-distributor-real` | 0 |
| `go vet ./tests/shard-distributor-real` | 0 |
| `python3 -m py_compile tests/run-go-service-baseline.py` | 0 |
| `node --check tests/matching-service-compat.cjs` | 0 |
| `run-go-service-baseline.py --rust-services matching --real-shard-distributor --dry-run` | 0 |

No gate was run; no etcd container was started. Not yet validated: end-to-end behaviour.
