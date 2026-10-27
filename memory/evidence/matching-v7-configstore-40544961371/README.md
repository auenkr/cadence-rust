# V7 Configstore mixed lane — reviewed pass

Run: https://github.com/auenkr/cadence-rust/actions/runs/40544961371

Head/artifact commit f5963951befb12dc64b72bb366c44a464939b87a.
Rust binary SHA256 d1d6ba8c1f0a0a172701241091e2de4dc1f8b41814042f6d83e298cd19d15e41.
Gate collection verifies successful build/runtime/probe exits and Go F/H/W
with Rust Matching and no Go Matching. The actual Configstore probe reports
seededLongPollExpiryMs2000 and emptyPollElapsedMs2009 at the selected owner.
SDK cancellation, echo, retries, child, timeout and signal report success.
Restart keeps one durable task and completes once (9012 to9433); graceful
drain removes one queued row and completes on replacement9653. Crash9031 is
taken over by survivor9012, range1 to2/ack0 to1, completing once with zero rows.
Idle eviction/recovery completes once. Cleanup output identifies the removed
lane-owned MySQL container. This proves the exercised seeded long-poll setting,
not every Configstore key or failure mode, and does not establish service
completion. Raw-byte string integration will require a new acceptance pin.
