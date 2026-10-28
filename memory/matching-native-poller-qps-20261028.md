# Matching native poller/QPS byte-exact ports (2026-10-28) — UNVERIFIED

New files (not yet registered in `cadence-common/src/lib.rs`; parent must add
`pub mod poller_bytes; pub mod qps_bytes;`). No cargo/CI run yet.

## `cadence_common::poller_bytes`
Mirrors `poller::Manager` exactly (same TTL 300s, capacity 5000, LRU order, callbacks).
- `Info { identity: GoString, rate_per_second: f64, isolation_group: GoString }`
- `HistoricalInfo { identity: GoString, rate_per_second: f64, last_access_time: i64 }`
- `Manager` (Default): `set_history_callback(Arc<dyn Fn()+Send+Sync>)`,
  `start(&[u8], Info, now)`, `set_cancel(&[u8], cb)`, `cancel(&[u8]) -> bool`,
  `end(&[u8], now)`, `contains_poll(&[u8])`, `has_after(i64)`, `has_group_after(&[u8], i64)`,
  `count()`, `count_at(now)`, `list(now) -> Vec<HistoricalInfo>`,
  `count_by_group(now, after) -> HashMap<GoString, usize>`.
- No serde derives (GoString has none).

## `cadence_common::qps_bytes`
- `EmaQpsBytesTracker::new(exp, Duration, qps::Clock)` (reuses `qps::Clock`);
  `report_counter(i64)`, `report_group(&[u8], i64)`, `qps()`, `group_qps(&[u8])`, `report()`.
  Math/folding/mutex identical to `EmaQpsTracker`; group map keyed by `GoString`.

Tests (in-file) cover ff/fe/U+FFFD non-aliasing for poll IDs, identities, groups, QPS, TTL, callbacks.
