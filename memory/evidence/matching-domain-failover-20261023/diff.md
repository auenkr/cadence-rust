# Matching domain failover callback: Go vs Rust (base ddd0d59b6)

Result: every Go effect is already ported and tested. No code change.

| Effect | Go | Rust | Tests |
|---|---|---|---|
| Register callback "matching"; catch-up sets baseline to max FailoverNotificationVersion, never releases | engine.go:1745-1759 | rust/crates/cadence-matching/src/matching_runtime.rs:455-535 (catch_up) | matching_runtime.rs:720 selection/baseline; domains.rs:~961-1046 initial sync does not notify |
| Eligibility: global, replication cfg present, !IsActiveActive, version > current | engine.go:1813-1818; types/shared.go:2627 | matching_runtime.rs:698 notified_failover_domains; domains.rs:213 notify_failover (scope with non-empty attributes = active-active, same as Go) | matching_runtime.rs:720; domains.rs:~990-1046 |
| Baseline = max over nextDomains, eligibility judged against the OLD baseline | engine.go:1707-1741 | matching_runtime.rs:470-505 | matching_runtime.rs:720 |
| Normal decision + activity task lists, sticky decision only (sticky activity excluded) | engine.go:1719-1737 | matching_registry.rs:583 release_domain_failover_polls (kind/type filter) | matching_rpc_tests.rs:3411, 3438 |
| Task-list set comes from loaded managers (ManagersByDomainID), not persistence | engine.go:1202-1220 | matching_registry.rs:583 (owners map; roots via TaskListName::parse) | matching_rpc_tests.rs:3411 |
| getOrCreate manager, ReleaseBlockedPollers (disconnect + refresh cancel ctx), manager stays loaded; load error is logged only | engine.go:1764-1792; task_list_manager.go:807 | registry.get + dispatcher.release_blocked_pollers (Notify::notify_waiters, later polls unaffected); load error -> eprintln | matching_rpc_tests.rs:2755, 3411 |
| Other domains untouched | engine.go:1715 | registry filters by domain id | matching_rpc_tests.rs:3438 |
| Sticky queue cleanup | none in the callback | none | n/a |

Not a gap: Go's errShutdown for a stopped manager is only logged; Rust's registry.get reloads a stopped owner instead, which is an equivalent outcome (poll set empty).
