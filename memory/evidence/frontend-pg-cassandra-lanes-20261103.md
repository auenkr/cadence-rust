# Frontend mixed gate: Postgres + Cassandra lanes (2026-11-03)
Branch frontend-lanes-pg-cass, head 97ae88587d1b05b5bb6725b2417b8c22575d913e (master c2e37c42e + gate fix).
Inputs: kafka=true, two_history=true (all phases: kafka, two_history, oauth, metrics, archival, dynconfig, multi-History churn).

| Lane | Run | Result | GATE ok steps | Rust binary SHA256 |
|---|---|---|---|---|
| Postgres | https://github.com/auenkr/cadence-rust/actions/runs/41622697487 | success | 155 / 0 fail | 455d7dc52a8de0137735f028976940097ae43a9fb1562cefc42b73e207f914ea |
| Cassandra | https://github.com/auenkr/cadence-rust/actions/runs/41622702539 | success | 155 / 0 fail | e0c0ae314993e46383ce35a4afe8ca9fc468e3b980c1a7eff8a79e5f7de0bda2 |
| Postgres + ES + Kafka + Go indexer (es=true) | https://github.com/auenkr/cadence-rust/actions/runs/41624742792 | success | 145 / 0 fail | 55e8150db9097442e48ec7818fcd99cb6a84007beda2bb2ff2d3db001bf06999 |
| rust-ci (clippy + workspace tests) | https://github.com/auenkr/cadence-rust/actions/runs/41624747694 | success | n/a | n/a |

Cassandra ES lane: not run; workflow/lib-es.sh support ES on mysql/postgres only (Go advanced visibility on Cassandra is not covered by the gate).

## Failure found and root cause (no Rust bug)
First runs (41615599718 pg, 41615604414 cass, plus control 41618067347 mysql and 41620438450 pg two_history=false) all failed only
sdk-sched-backfill-completed (+ sdk-driver-exit): "only 1 completed backfill runs, want 2". Reproduced on MySQL, so not lane-specific.
Go scheduler logs "schedule fire skipped" for the 2nd backfill fire 10ms after the 1st: default overlap policy SkipNew and
isWorkflowRunning (DescribeWorkflowExecution) correctly reports the just-started run as open. Earlier green runs only passed because the
2nd fire was delayed ~1s (activity retry), a timing accident. "Error writing to visibility: Operation is not supported" on queue task type 8
(UpsertSearchAttributes) from the Go scheduler also appears in the green runs (Go SQL visibility behaviour), so it is unrelated.
Fix (gate only): tests/frontend-gate/main.go runs backfill with --overlap_policy concurrent. Re-run green on all lanes above.
