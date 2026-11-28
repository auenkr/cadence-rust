# Confirm cold_restart loss fix on current master (2026-11-28)

Verdict: **PASS.** Rust lost 0 workflows (and 0 first-decision-task losses) on Cassandra, MySQL and PostgreSQL; Go also 0. The earlier Rust gap (9/8629 on Cassandra cold_restart, fixed in 8e46e8f12) does not reproduce. No Rust code change needed.

## Setup
- Branch `confirm-cold-restart-loss` = master 1f3477295 + harness commit adding a `postgres` option to `recovery-timing.yml` / `recovery-run.py` (uses `docker-compose.split.postgres.yml`). Head SHA **6dec132fdf6450b14189e28f88756564dab77d25** (runs built this head).
- Workflow: recovery-timing.yml, 10 repetitions per run, 3 runs per database = 30 reps x (Go, Rust) per database. Scenarios: Cassandra `cold_restart,kill_history`; MySQL and PostgreSQL `cold_restart`. Rust services: frontend,matching,history; Go Worker in both arms.
- Runner: ubuntu-latest, 4 vCPU, 15 GiB, kernel 7.0.0-1020-azure.
- Rust binary SHA256 (all runs, identical): cadence-server 1c53abfb7ca133fb3bc68bf967a102bea524f5e90b897367257080dc13aadab9, cadence-history 8fec69bf5d9c4b2a94192cd968804fb0c464dff88a607bb45f0fa87a814cdec5. The Go binary differs per run (go image rebuilt each run; see each artifact's go-binary.sha256).

## Loss table (workflows lost / started)
| database | run | Go | Rust | verdict |
|---|---|---|---|---|
| cassandra | [46108612193](https://github.com/auenkr/cadence-rust/actions/runs/46108612193) | 0/3701 | 0/3276 | PASS |
| cassandra | [46108614721](https://github.com/auenkr/cadence-rust/actions/runs/46108614721) | 0/3146 | 0/2927 | PASS |
| cassandra | [46108617454](https://github.com/auenkr/cadence-rust/actions/runs/46108617454) | 0/3118 | 0/2992 | PASS |
| mysql | [46108620125](https://github.com/auenkr/cadence-rust/actions/runs/46108620125) | 0/3238 | 0/2195 | PASS |
| mysql | [46108623797](https://github.com/auenkr/cadence-rust/actions/runs/46108623797) | 0/2930 | 0/2226 | PASS |
| mysql | [46108627117](https://github.com/auenkr/cadence-rust/actions/runs/46108627117) | 0/3102 | 0/2420 | PASS |
| postgres | [46108631815](https://github.com/auenkr/cadence-rust/actions/runs/46108631815) | 0/2517 | 0/2137 | PASS |
| postgres | [46108635045](https://github.com/auenkr/cadence-rust/actions/runs/46108635045) | 0/2612 | 0/2177 | PASS |
| postgres | [46108638355](https://github.com/auenkr/cadence-rust/actions/runs/46108638355) | 0/2551 | 0/2182 | PASS |
| **total** | | **0/26915** | **0/22532** | |

Per database: Cassandra Go 0/9965, Rust 0/9195 (earlier before-fix: Rust 9/8629); MySQL Go 0/9270, Rust 0/6841; PostgreSQL Go 0/7680, Rust 0/6496. All 9 runs conclusion success, `[verdict] ... rust_first_decision_lost=0 -> PASS`. "Never stable" count 0 in every scenario/arm.

## Timing (ms, cold_restart stable median, per run; Rust is faster than or equal to Go everywhere)
Cassandra Go 12162/11405/11540 vs Rust 9503/10268/9943; MySQL Go 12189/12410/11450 vs Rust 9334/10193/9870; PostgreSQL Go 10901/12125/10946 vs Rust 9652/9776/9862. Cassandra kill_history: Go 1983/1467/1461 vs Rust 1700/1276/1257 (stable median).

Artifacts (3-day retention): recovery-timing-manual per run. Reproduce: `scripts/ci/gh-run.sh dispatch recovery-timing.yml <ref> database=<cassandra|mysql|postgres> scenarios=cold_restart repetitions=10`.
