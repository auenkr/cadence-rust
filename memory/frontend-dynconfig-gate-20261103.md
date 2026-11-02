# Frontend durable dynamic config mixed-gate evidence (2026-11-03)

Branch `t3/frontend-dynamic-config-gates` (from master 4254b4963). Gate phase added to `tests/gates/run-frontend-gate.py` (all three lanes; lane scripts export the DB container name).

Phase (Rust Frontend restarted with `dynamicconfig.client: configstore`, Go CLI `admin config`): update `frontend.visibilityMaxPageSize`=777 -> get/list show it -> cluster_config row exists -> restore (get returns NotFound like Go, list no longer shows it, row version advances) -> update 888 -> seed a copy of the Rust-written blob as a next-version row (Go row format; negated version on SQL, positive on Cassandra) -> restart Frontend -> value 888 survives -> update 999 lands after the seeded version -> AddSearchAttribute persists (new row) and survives restart -> restart back to filebased.

## Rust bug found and fixed
- Postgres: `fetch_latest_config`/`insert_config` called the sync `postgres` driver on a tokio worker ("Cannot start a runtime from within a runtime"), poisoning the admin config lock; every dynamic config call then failed. Fixed by `outside_runtime` (ea7af13). MySQL and Cassandra were unaffected.
- Gate assertion fix: Get after Restore returns EntityNotExists in Go ("unable to find key"), so the CLI must fail.

## Runs (all `frontend-mixed-gate.yml`, rust_ref = the branch; Go History/Worker + Rust Matching + Rust Frontend)
| Lane | Run | Head | Conclusion | Rust cadence-server SHA256 |
|---|---|---|---|---|
| MySQL | https://github.com/auenkr/cadence-rust/actions/runs/41538270759 | f0aafcc3971ee2ccc06347f51e5e2a6ee3a6b54e | success (failed: []) | ef02a0c9fc76bb8703449335a11bf36d67ce3ed7823b73bfcd8afeeaeb20e84b |
| PostgreSQL | https://github.com/auenkr/cadence-rust/actions/runs/41540868425 | ea7af13d29a48acd393d976207fc13f1fa698b4f | success (failed: []) | 02b33efb34648d6ac1fbe0a0d98813d35f84a7a66aa46d79e11813e2334a4b4b |
| Cassandra | https://github.com/auenkr/cadence-rust/actions/runs/41541744231 | ea7af13d29a48acd393d976207fc13f1fa698b4f | success (failed: []) | 115d59830999a5c786096c8eb67c6565a82f92534ef688e688453210443b7523 |
Earlier failed runs: MySQL 41536838736 (gate assertion only), Postgres 41539636022 (the runtime bug above).

## Limits
- "Go-written" rows are copies of the Rust blob, not rows produced by a Go process (Go Frontend is not in the cluster; Go History runs filebased). Byte-identity with Go is covered by unit tests.
- Elasticsearch mapping in AddSearchAttribute remains absent, so that row stays `partial`.
