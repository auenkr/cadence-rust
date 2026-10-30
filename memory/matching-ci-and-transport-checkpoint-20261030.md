# Matching fresh CI and transport checkpoint

Current candidate: `a47e4032d94134b250ce0670cbd5f791d32b285b`, branch `t3/matching-pg-range-shard-filter`. Source review passed for process-capture qualification, artifact collection and singleton PostgreSQL shard filtering.

Fresh workspace CI dispatched with package omitted and `clean_target=true`: https://github.com/auenkr/cadence-rust/actions/runs/41103249766 . Direct inspection confirmed exact candidate SHA and queued status; no pass claimed. Do not start its mixed gate until fresh Clippy and workspace tests pass and their artifacts are inspected.

Earlier transport diagnostic gate https://github.com/auenkr/cadence-rust/actions/runs/41100032910 is terminal failure on `4a5da9fb0b9709e40d1117244c1faf82bc60a27f`. Downloaded artifact `matching-production-postgres` to `/mnt/storage/service/ci-evidence/matching-postgres-41100032910/`. Build exit 0; gate, lane and nested run exits each 1. Server SHA256: `3cbfebef37ec88b34d3655be8f0b97af7b14b34db9fb00dee8dc2747ea8d25eb`.

Retained readiness JSON shows direct target `127.0.0.1:29438`, gRPC deadline exceeded waiting for LB pick, connectivity state 1, direct TCP connected, and separate HTTP/2 probe with socket connected but no remote settings before its two-second timeout. No proxy variables were present; credentials insecure. These observations narrow the retained failure below RPC handler work but do not establish a unique runtime defect or timing of its onset. Frontend readiness also timed out. This run does not establish SDK or recovery success.

Three Luna workers are reviewing terminal transport evidence, client/HTTP2 lifecycle and diagnostic source-line reconciliation. The historical CI owner remains separate; no duplicate mixed gate was dispatched. No local Cargo, Go, Node, formatter, build, test or runtime validation was run. Matching remains incomplete; History migration must wait.
