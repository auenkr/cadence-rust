# History owned NDC state slice — CI evidence

Implementation branch: `t3/history-owned-mutstate`

## CI

- Command: `gh workflow run rust-ci.yml --ref t3/history-owned-mutstate -f package=cadence-history`
- Passing head: `f6d2bb833756cf1be65455253ea4cf10a01e8bb7`
- Run: https://github.com/auenkr/cadence-rust/actions/runs/42583560592
- Conclusion: success. The `clippy-test` job ran cadence-history Clippy with `-D warnings`, followed by the cadence-history tests.
- Initial implementation head `2baea21866a49a32fc4ee27387dfc8ea352a0946` failed Clippy because the unsupported-operation helper returned the large `NdcError`; adding a narrow `clippy::result_large_err` allowance fixed it.
- No Cargo command or Go build/test ran on the development host.

## Scope and limits

The passing tests cover Go-shaped initial domain/run identity and state/version/CAS sentinels, owned version-history edits observed through borrowed accessors, condition independence from next-event ID, and explicit unsupported reads for unavailable event-reapplied/activity heartbeat data.

This is only the owned-state structural unblock. It has no lossless persistence envelope, typed replay dispatcher, shared engine-cache checkout, transaction commit/persist behavior, rebuild, or NDC event-reapplied/heartbeat backing. History replication is still non-operational and service completion is not claimed. Next work follows the dependency plan: codec brief 2a, typed replay 3a/3b, then shared cache checkout 4b (with transaction dependencies preserved).
