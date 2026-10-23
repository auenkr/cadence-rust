# Lane 4 (Cassandra) FAILED twice (exit 1, attempts 1 and 2; max reruns used)

Probe: `tests/matching-service-compat.cjs:542` "Cassandra semaphore capacity one must grant exactly one concurrent owner".
Attempt 2 (with grants printed): `[{"outcome":"SEMAPHORE_ACQUIRE_OUTCOME_NO_SLOT","tokenId":0},{...NO_SLOT,"tokenId":0}]`
(`../4-cassandra-attempt2-failed/probe.log`). Deterministic, same in attempt 1.

Cause (read from source, not a flake): commit 3076d7837 (matching-semaphore-tests, "restore Go slot handling")
changed `Manager::load` (rust/crates/cadence-matching/src/matching_semaphore.rs:196-198) to build the free set only
from scanned rows, as Go `loadTokenOwnership` does (go/service/matching/semaphore/semaphore_manager.go:426-480).
Go never seeds token rows when creating a semaphore (`nosql_semaphore_metadata_store.go:57-76` only inserts metadata;
no seeding anywhere in go/ — grep "seed" finds only the comment at semaphore_manager.go:403). So against the pinned Go
code a freshly created semaphore has an empty free set and Acquire returns NoSlot. The earlier gate (run3) passed
because the old Rust load seeded 1..=budget from metadata (a deviation from Go).

Classification: the Rust change matches Go; the probe's expectation (exactly one ACQUIRED) predates it and does not
hold against Go-faithful behavior. NOT fixed here (harness-only fixes allowed; faking token rows in the probe would hide
the Go gap). Decision needed: either accept Go's behaviour and make the probe seed token rows via the gocql helper, or
treat "Go can never grant a fresh semaphore at this pin" as an upstream gap. Lane result: FAIL (exit 1).
