# All-Rust cluster validation — 2026-11-14

Branch: `all-rust-cluster`, based on `origin/master` at `9ec37416a145a13bdfc1cfa031e886a428783cf9`.
Gate source: `d24e746bf52aa139b602d7249d888261a41ee856`.

The existing `history-mixed-gate.yml` already starts Rust Frontend + Rust Matching (`cadence-server`) + Rust History (`cadence-history`) and Go Worker only. `run-frontend-gate.py` starts the Go server with `--services worker` and records `service-stack.json` and process command files. The `rust_frontend_active` input is specific to the separate two-cluster replication harness, which includes Go services and is unsuitable for this single-cluster proof. Worker remains Go per `worker-skip-use-go-worker-20261101.md`.

Added `all_rust_recovery` input to the existing History gate, included in concurrency identity. It adds Matching and Frontend SIGKILL/restart rounds to the existing in-flight SDK recovery suite, which already crashes all History hosts, restarts persistence, and crashes the primary History host. Each added round uses the existing recovery driver (timers, activities, timeouts, signals, raw decision timeout and completion), waits for service readiness and requires workflows to finish. The harness now checks that all requested recovery rounds executed. The ordinary SDK suite also crashes Frontend and a second History host. The workflow requires the exact `GATE SUMMARY failed: []` line after a zero harness exit.

All builds and mixed gates run on GitHub Actions standard `ubuntu-latest` runners, timeout 85 minutes, fresh uncached target directory, artifact retention 3 days. Existing lane Docker limits apply. No local Cargo, Go or Docker commands were run.

Reproduce (for each database mysql/postgres/cassandra):

```sh
gh workflow run history-mixed-gate.yml --ref all-rust-cluster \
  -f database=mysql -f all_rust_recovery=true -f two_history=true
```

Inputs otherwise default: outbound=grpc, queue_v2=false, s3_archival=false, gcs=false. Optional Kafka/ES/archival extensions are not enabled by this History gate.

| Database | Run | Conclusion | Summary |
|---|---|---|---|
| mysql | https://github.com/auenkr/cadence-rust/actions/runs/43583398117 | pending | pending |
| postgres | https://github.com/auenkr/cadence-rust/actions/runs/43583401011 | pending | pending |
| cassandra | https://github.com/auenkr/cadence-rust/actions/runs/43583403860 | pending | pending |

Binary hashes and final results will be recorded after the runs conclude. Frontend and Matching share the cadence-server executable; History uses cadence-history.
