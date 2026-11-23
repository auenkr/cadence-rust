# FE review fixes 1-2 (2026-11-23)

Branch `fe-review-fix-1-2`, head af491d9582ea465bd2240b4da1ed40677589ad9d (+ this evidence commit).

1. Domain replication queue/DLQ: writes prefix 0x59 (Go ThriftRW preambleVersion0); reads via
   `cadence_persistence::domains::decode_thriftrw_payload` (strips 0x59; read-side-only fallback for
   legacy unprefixed Rust rows; other versions/empty rejected). All 4 admin.rs read sites use it.
   Tests: `thriftrw_prefix_tests` (Go-prefixed vs legacy decode, bad version), updated encode test.
2. History poll: continuation token's version-history item forwarded as PollMutableState field 50.
   Tests: `poll_version_history_item_tests`.

CI (head af491d9):
- rust-ci cadence-service-core: https://github.com/auenkr/cadence-rust/actions/runs/45171658593 success
- rust-ci cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/45172586196 success
- rust-ci cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/45172592070 success
- frontend-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/45172597774 success, `GATE SUMMARY failed: []`
- replication-simulation.yml is a Go-only simulation; not run.
Earlier runs on prior heads were cancelled/failed on fmt and unused imports (fixed).
