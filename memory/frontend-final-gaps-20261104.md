# Frontend final gaps (2026-11-04)

Branch `t3/frontend-final-gaps` (from t3/merge-audit-residuals). Code head: 8fe78d4ce (evidence commit follows).

## Done
1. Graceful failover watcher (`cadence-service-core/src/domain_failover_watcher.rs`), wired in launcher:
   `system.enableGracefulFailover` (startup), `frontend.domainFailoverRefreshInterval` and
   `frontend.domainFailoverRefreshTimerJitterCoefficient` (dynamic), counter `graceful_failover_failures`
   (operation=DomainFailover, domain tag). Deviation: Go's history `GetFailoverInfo` call is logging-only and
   Rust history's is a stub, so it is skipped.
2. `system.enableDomainAuditLogging` (default false) gates domain audit log persistence.
3. Metrics pass: Low1ms100s histograms, Go per-API scope operation names (table test of 56 Go scopes vs Rust),
   schedule, domain and semaphore APIs metered like Go's innermost metered wrapper (`observe_local`).
4. Global ratelimiter gate steps ran 3 times (mysql, cassandra, postgres): all `global-ratelimiter-*` green in each run; no flake observed.

## CI evidence (head 8fe78d4ce)
| Run | Result | Rust binary sha256 |
|---|---|---|
| rust-ci https://github.com/auenkr/cadence-rust/actions/runs/41738200029 | success | n/a |
| mysql kafka two_history https://github.com/auenkr/cadence-rust/actions/runs/41739480477 | success | b88eaabab393ea6e82c2f470fe2aa58496f0dc1d6981a3469fa76157fe55f30a |
| postgres two_cluster https://github.com/auenkr/cadence-rust/actions/runs/41741066438 | success | 72c9cf876cc5acbb1a838dc03f21d91fad4c07cf350706d86386b6433fe290a3 |
| cassandra kafka https://github.com/auenkr/cadence-rust/actions/runs/41742269510 | success | 4208166d1f469adbcdd2d0633f6e3a89243df1c716dc4b5609461482c8c56d41 |

| postgres kafka (3rd ratelimiter sample) https://github.com/auenkr/cadence-rust/actions/runs/41744052449 | success | dc2669e1f5a2ff5f2981d91d7e0371e8a07dd5729d7e427875185b1e0bc16356 |

Global-ratelimiter steps (6 each) passed in mysql, cassandra and postgres runs: 3 of 3, flake not reproduced.
