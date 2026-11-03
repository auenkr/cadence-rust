# Frontend small parity (2026-11-03), branch t3/frontend-small-parity

Head f82622479d691c2d100bf7d17b27f490bed8f942.

1. Authorizer RequestBody: Rust accesscontrolled.rs already fills `request_body` with the full request (payload fields input/signalInput stripped like Go json:"-"), with unit tests; audit text was stale. Go's oauth authorizer does not read RequestBody. No code change.
2. ES visibility query validator (Go xwb1989/sqlparser behavior) in es_query.rs. Fixed: bare where operand -> "invalid where clause"; `... UNION SELECT` -> "Invalid select query."; qualified column `A.B` validated by name `B`; arithmetic left operand (`Custom-Field = 'v'`) -> "invalid comparison expression". `not like` already worked. Added table test `go_validate_query_table` copying all of Go's TestValidateQuery cases. DB (SQL/Cassandra) visibility uses a separate sqlparser path in cadence-persistence/visibility.rs that I did NOT change (its error texts are not Go's); remaining gap.
3. No new gate steps added.

Evidence:
- cadence-frontend CI success: https://github.com/auenkr/cadence-rust/actions/runs/41673182130
- Full rust-ci success on f82622479: https://github.com/auenkr/cadence-rust/actions/runs/41674298136
- MySQL mixed gate (kafka, two_history) FAILED: https://github.com/auenkr/cadence-rust/actions/runs/41674305965 — only the known master failure `EntityNotExistsError Domain <id> does not exist` on SDK start steps; Rust binary sha256 3d33ac25d21c6bd1c2d58db8f7cdb5d39190b06f7a3416e245c4a12ca057863b. Not green.
