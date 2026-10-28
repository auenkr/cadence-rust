# Matching native spectator raw-byte transport regression (2026-10-28)

## Scope

Extended the raw scripted tonic/HTTP2 peer test in `rust/crates/cadence-matching/src/matching_shard_distributor.rs`. The peer decodes `RawOwnerRequest` and `RawSnapshot` wire structs, so invalid UTF-8 keys are accepted at the actual protobuf transport boundary. Its initial snapshot now assigns distinct owners to the one-byte keys `ff`, `fe`, and UTF-8 encoding of U+FFFD. The test checks all three as cache hits (no `GetShardOwner` calls), then requests a cache miss containing `fd 00 62` and checks exact bytes, YARPC metadata (`shard-manager`, `cadence-matching`, `proto`), and a nonempty RPC deadline at the scripted peer. This catches accidental replacement-character normalization and cache key collisions across the snapshot/fallback path.

## Remaining gaps

- This is a bounded, focused native transport regression, not a complete Matching parity audit or mixed Go/Rust SDK/service compatibility validation.
- It covers invalid raw keys on the Rust spectator client boundary. It does not establish arbitrary-byte behavior for every server, persistence, or transport path.
- No production implementation was changed. No small production defect was established by this test-only change.

## Validation

- No local Cargo, rustfmt, Go build, test, or check command was run, per the task constraint and repository CI-only Cargo rule.
- CI validation is pending the parent agent, who owns CI dispatch/registration. Record the CI run URL, head SHA, and conclusion here after that run; until then this regression is unverified by execution.
