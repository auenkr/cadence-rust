# Matching/History operational config polling parity

Branch `parity-matching-cfg`, head `a774752a83cfb0cda49d8c0d7eac3f9cd1a3d369` (code commit `7eb2bf60`, then rustfmt of launcher.rs).

## Go behavior
`dynamicconfigfx` builds `configstore.NewConfigStoreClient` for row type 2 (operational config) in every service. The constructor starts one poller (default 2s) and `resource.Start` starts a second. Row type 1 (global isolation groups, 30s) likewise gets a constructor poller plus a second one from `Start`.

## Gap
Rust Frontend did this (perf-fe-soak-start). Rust Matching only polled row type 1 once (second poller only for Frontend) and never row type 2. Rust History polled row type 1 once, no row type 2. Result: Matching lane cluster_config SELECTs Rust/Go 0.76.

## Fix (reuse of existing code)
- `cadence-matching/src/launcher.rs`: operational store (`start_operational_config_store`, already used by Frontend) now also runs for Matching (`run_frontend || run_matching`); the isolation store does its second fetch and second poller for Matching too. `operational_config_settings` made `pub`.
- `cadence-history/src/wiring/assembly/env.rs`: History creates two row-type-2 `ConfigStoreClient`s (settings via `cadence_matching::launcher::operational_config_settings`) each with a poll thread. `cadence-history/src/launcher.rs`: second isolation fetch + second poller like Go `resource.Start`.

## Validation (all success)
- rust-ci cadence-history (pre-format-fix head): format failure 44776021055 (cancelled), then 44776397648 success https://github.com/auenkr/cadence-rust/actions/runs/44776397648
- rust-ci cadence-matching: https://github.com/auenkr/cadence-rust/actions/runs/44777662188 success
- rust-ci workspace (clean_target=true): https://github.com/auenkr/cadence-rust/actions/runs/44777669032 success
- Matching mixed gate: https://github.com/auenkr/cadence-rust/actions/runs/44777674607 success, head a774752a, cadence-matching sha256 `aa6c9b30d5643eca0a40f253fe5ba9a1e24611a4303915de6b6da4c13b93316f`
- History mixed gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44777679519 success, `GATE SUMMARY failed: []`, cadence-server sha256 `2f6f06f6f988ffd56204be830f518fb9bd16819214f6bbadf695eaf44e2314ec`, cadence-history `2ebc2b850c011098ba12953cb848ab96db6507b14c02d26c05145b52f2aecd4d`

## Benchmarks (source a774752a, rate 25, 60s, warmup 10, 3 reps, mysql_tmpfs, sql_log_secs 60, fresh_build)
- Matching lane https://github.com/auenkr/cadence-rust/actions/runs/44780097770 success, EPYC 7763: **cluster_config 256 vs 256 = 1.00** (was 0.76). domains/domain_metadata 0.99. 0 errors. Go p99 5s (7763 noise, excluded).
- History lane https://github.com/auenkr/cadence-rust/actions/runs/44780103077 success, EPYC 9V45: **cluster_config 256 vs 256 = 1.00**. Other big tables 1.00. 0 errors.

## Observation (not addressed)
Matching lane task_lists/tasks SELECTs are Rust/Go 0.36/0.35 (439 vs 1222; Go arm runs slow on 7763 so more retries); not a config issue.
