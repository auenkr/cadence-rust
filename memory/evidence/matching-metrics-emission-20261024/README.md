# Matching emitted metric comparison captures

Run: https://github.com/auenkr/cadence-rust/actions/runs/40106065466
Source: e9fa52d4362475a6c5f05d2d6b5c4e3a3bb43ea4. Conclusion: success.
Downloaded artifact: matching-metrics-emission-evidence. Preserved original
Go/Rust JSON captures, comparisons, source identity and comparison binary hash.

Default/both/name-on/name-off comparisons matched names, kinds and full tags
for20/24/21/23 series. Go metered.TaskManager and Rust limit_task_store shared
GetTasks outcomes matched nine series across empty success, one-row success
and BadRequest failure. Production rollups were corrected to retain operation.

These fixtures cover representative emitted paths, not every active metric,
sampled values, every persistence operation/error, or a live cluster exporter.
See memory/matching-metrics-emission-comparison-20261024.md for scope/limits.
Implementation merged as20b17defc; combined-source validation is separate.
