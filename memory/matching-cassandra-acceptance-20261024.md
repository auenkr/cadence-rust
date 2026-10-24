# Matching Cassandra CI acceptance — 2026-10-24

The Cassandra mixed Go/Rust production acceptance lane now passes on a standard GitHub Actions runner. The Rust binary was built on CI into a fresh unique target directory with no Cargo cache. The gate ran Rust Matching only, with original Go Frontend, History and Worker, on Cassandra 4.1.7. It exercised the SDK probe, idle eviction, restart and failure recovery, Cassandra task/semaphore persistence and domain page tokens; acceptance is marked eligible.

Evidence, exact reproduction command, run URL, head SHA, conclusion and Rust binary SHA-256: [matching-cassandra-acceptance-20261024](evidence/matching-cassandra-acceptance-20261024/README.md).

The earlier semaphore gate assertion was corrected to match Go's owner-ID wire format (`<uuid>:<numeric hold id>`); no assertion was weakened. The fresh-semaphore NO_SLOT expectation and seeded capacity-one concurrency check remain intact. One intervening run returned an activity with null input during the paced-dispatch scenario; the next run retained the payload assertions and added owner observation around the two-node paced task list, then passed with both Rust nodes agreeing on the owner. See the saved probe log and recovery checkpoints for the successful run.

Matching's complete service parity audit remains outstanding; this lane is reproducible Cassandra mixed-service acceptance evidence, not the full service completion decision.
