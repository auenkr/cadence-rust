# PostgreSQL gate result

https://github.com/auenkr/cadence-rust/actions/runs/41108824183 is terminal, conclusion failure, exact head `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`.

Matching binary SHA256: `bc45dd2dc600599fdd2f1135a58a918c878bb4f9ca93eaac5692f6a5eece778d`. Build exit 0; probe, run, lane and gate exits 1. The result records Go Frontend/History/Worker, Rust Matching, no Go Matching, and acceptanceEligible=false. Readiness failed before SDK and recovery scenarios; none is credited. Rust received SIGTERM, exceeded the 20-second harness grace deadline, then exited by SIGKILL. This does not alone prove a shutdown deadlock. The cleanup artifact names the PostgreSQL container; it is not by itself an independent post-cleanup inspection.

Retained compact raw evidence: `memory/evidence/matching-postgres-41108824183/`. Full downloaded artifact: `/mnt/storage/service/ci-evidence/matching-postgres-41108824183.AY5Gf8/`. Configurations, private keys and executable binaries are not copied into memory.

The first captured direct RPC (18:58:11.740Z–18:58:21.739Z) reports grpc-js READY throughout but deadline code 4. Its concurrent HTTP/2 probe connected yet received no SETTINGS within 2 seconds. Final readiness reports Waiting for LB pick. The process snapshot and corresponding direct RPC diagnostic are both retained; workers are independently examining process, native trace and shutdown evidence. No unique source cause or fix is established by this checkpoint. Do not dispatch the next mixed lane until this failed lane is resolved. Fresh workspace CI at this SHA passed previously (41107169881); that remains unit/workspace evidence, not mixed acceptance.
