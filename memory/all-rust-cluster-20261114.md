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
| mysql | https://github.com/auenkr/cadence-rust/actions/runs/43583398117 | success | `GATE SUMMARY failed: []` |
| postgres | https://github.com/auenkr/cadence-rust/actions/runs/43583401011 | success | `GATE SUMMARY failed: []` |
| cassandra | https://github.com/auenkr/cadence-rust/actions/runs/43583403860 | success | `GATE SUMMARY failed: []` |

All three runs passed on their first attempt. Frontend and Matching share the cadence-server executable; History uses cadence-history. No Rust bugs were found, so no Rust implementation changes or new Rust unit tests were needed.

| Database | cadence-server SHA256 (Frontend + Matching) | cadence-history SHA256 |
|---|---|---|

| mysql | `abedec8f83b56064aaefa5c4a41e9dab2ff3c3d521f7b91d07cd1899e2e2d6e6` | `0f0469c7e76e7725ac17ce4eec180a1c2ecaa870c912d3ea7db98517a4cf812b` |
| postgres | `f9786f670236b91e67997ece80265de65267b6e6417743bbe19bda700fb3a2b2` | `d24450a8230995bb596b4cab51da783ee06065cec4574369d39c8ce36a87e997` |
| cassandra | `03ce3779e7a84b02f19d6cbc0bf132a49a6f88a8d9867d0b31167df80f834d7e` | `95619eba424873e727bbb6bc42b0939c0f6ad6d3883746aad7c9b9718dd232db` |

Persistent compact evidence: `memory/evidence/all-rust-cluster-20261114/{mysql,postgres,cassandra}.txt` includes hashes, exact Go Worker command, service topology and every GATE assertion. Full artifacts remain attached to the linked runs for 3 days. Each run exercised the ordinary SDK suite, History churn, all-History crash/restart, database restart/reconnect, primary-History crash/restart, Matching crash/restart and Frontend crash/restart with in-flight workflow completion.

Artifact extraction initially hit the local `/tmp` quota; partial downloads created by this task were removed and only selected evidence files were extracted under `/mnt/storage/service/all-rust-evidence-20261114`. This did not affect CI or test results. No merge or branch deletion was performed.
