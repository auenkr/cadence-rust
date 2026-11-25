# perf-r4-fixes (2026-11-25)
Branch perf-r4-fixes. Changes: History matching_resolver evicts channels for hosts missing from the membership snapshot (on generation change; unit test channel_cache_tests); notify-order regression tests (real_engine_tests::notify_order create+update paths; ndc_context_hooks::notify_order_tests); blocking pool = max(64, CADENCE_HISTORY_MAX_BLOCKING_THREADS) (config is not available before runtime build; set to SQL maxConns+16 when above 48); jemalloc narenas:1 -> 2 (static malloc_conf).
CI: rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/45530210663 success (earlier runs failed clippy type_complexity, fixed).
Mixed gate: history-mixed-gate https://github.com/auenkr/cadence-rust/actions/runs/45531173880 success.
Head at gate dispatch: 1dd2acd0a87746de915ec33c6a49d67792be1ac8 (evidence commit follows).
