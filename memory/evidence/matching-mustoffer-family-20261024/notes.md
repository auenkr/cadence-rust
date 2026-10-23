# MustOffer timeout / forward-error family: Go → Rust port evidence (2026-10-24)

Branch: `matching-mustoffer-family-20261024` (from `origin/master`). Not merged.
Code head: `18bd88049c3ec5714df4218cb262e564867c1ae0` (test commit). CI: `gh workflow run rust-ci.yml -f package=cadence-matching`, run `40035986861`, **success**.
Log extract: `test result: ok. 619 passed; 0 failed; 11 ignored` for the `cadence-matching` lib tests (the four new tests are among the 619 passed).

Seam: `BacklogForward { tokens, forward, local_wait }` through `TaskReader::set_backlog_forwarder` (`rust/crates/cadence-matching/src/matching_reader.rs`). Tests are private-access reader tests in the existing `mod tests`. No production code changed.

## Table

| Go test (`go/service/matching/tasklist/matcher_test.go`) | Rust test (`matching_reader.rs` `mod tests`) | Status | Run URL |
|---|---|---|---|
| `TestMustOffer_ContextExpiredFirstAttempt` (:882). No token/forward; deadline → `DeadlineExceeded`. | `matching_reader_dispatch_timeout_without_poller_keeps_row_unacked` | PASS (behaviour). Go returns an error; Rust keeps the row and retries. The error return is API shape, not observable state. | https://github.com/auenkr/cadence-rust/actions/runs/40035986861 |
| `TestMustOffer_ContextExpiredAfterFirstAttempt` (:893). Mock `AddReqTokenC().Times(1)` returns a nil channel: the token is requested once and never granted, so no `ForwardTask`; deadline → `DeadlineExceeded`. | `matching_reader_dispatch_timeout_without_token_never_forwards_or_acks` | PASS. Asserts zero forwards, zero acks, dispatch-timeout metric, and a later poller receives the same row. | https://github.com/auenkr/cadence-rust/actions/runs/40035986861 |
| `TestMustOffer_ContextExpiredAfterForwardError` (:971). One token, `ForwardTask` → `ErrNoParent`, then cancel → `context.Canceled` wrapping `failed to offer task:`. | `matching_reader_forward_error_then_stop_releases_token_without_ack` | PASS. Asserts exactly one forward, token released after the error (before the local wait), and no ack after stop. The `failed to offer task:` text is Go-only and not asserted. | https://github.com/auenkr/cadence-rust/actions/runs/40035986861 |
| Go has no direct analogue. Covers the "one forward per token" property: one granted token admits one forward, and the token is held while that forward is in flight. | `matching_reader_one_forward_per_token_while_forward_is_in_flight` | PASS (new). | https://github.com/auenkr/cadence-rust/actions/runs/40035986861 |
| `TestMustOffer_LocalMatchAfterChildCtxExpired` (:909), `TestMustOffer_LocalMatchAfterForwardError` (:937) | existing `matching_reader_local_head_start_does_not_forward`, `matching_reader_local_poller_wins_after_forward_error` | Already covered (not re-run here, pre-existing). | — |

Existing test used as base, not modified: `matching_reader_stop_cancels_inflight_forward_without_ack` (in-flight cancel, token released). The new "after forward error" test covers the window the in-flight test does not.

## Divergence from the memo proposal

`memory/matching-cannot-port-review-20261024.md` rows 23 and 24 proposed the `AfterFirstAttempt` case as "one token, forward never completes, assert one forward". The Go test does not do that. Its mock returns a nil token channel, so no forward is ever sent. The Rust port therefore pins zero tokens → zero forwards for that case. The one-token, in-flight case is the separate new test above.

## Parity gaps

None found for this family. No `#[ignore = "parity gap"]` was added because every assertion reproduced against production code passed.

## Counts

- Go tests in the family reproduced: 3 of 3 named in the memo (rows 23, 24, 27).
- New Rust tests: 4 (3 mapped to Go, 1 for the one-forward-per-token property).
- Passed: 4. Ignored: 0. Failed: 0.
- Local cargo: not run (per instruction). Verified only by GitHub Actions run `40035986861`.
