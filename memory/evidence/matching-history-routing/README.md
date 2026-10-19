# Matching History routing validation handoff

Run this runner from the rebased routing worktree after the build slot is
transferred. It records an attempt directory with source hashes and per-gate
logs.

Run it from the integrated repository tree with the slot granted:

```sh
memory/evidence/matching-history-routing/run.sh
```

It uses one Cargo job, defaults to the warmed Matching domain-cache target,
creates a unique UTC `attempt-*` directory, records the HEAD and source hashes,
and refuses to run if any routing source is dirty. It checks the History client
retry/deadline tests, Go placement resolver oracle tests, live membership
adapter tests, the dispatch regression test and strict Clippy. It snapshots the
accepted Go placement fixture and source hashes before running the oracle, then
requires generated evidence to remain byte-identical. Per-stage logs and exit
codes are stored in the attempt directory.

## Frozen candidate

The routing source under review is rebased onto cache commit `e1b78e5f1` and
includes the History peer resolver, live membership adapter, caller-deadline
propagation, and duplicate test-attribute correction. This is not yet a
production integration acceptance: `main.rs` still
creates `MatchingHistoryClient` from one fixed History channel and does not
install the peer resolver/live membership adapter. TChannel remains outside
this source slice.

The cache changes currently under review touch `matching_dispatch.rs`'s
blocked-poller wait/release path and `grpc_matching.rs`'s failover branches.
History deadline forwarding is in separate methods and the query History call
hunk; they are expected to compose, but run the tests on the integrated cache
base after transfer rather than treating candidate-branch test evidence as
integrated evidence.

## Gate ledger

| Gate | State before slot transfer | Evidence required |
|---|---|---|
| Source freeze and clean diff | Prepared, not executed | Attempt HEAD, clean relevant-source status and SHA-256 before/after |
| Go-compatible short FarmHash and ring owner | Not run on final candidate | Go byte vectors and U+FFFD Ring oracle key pass |
| Ringpop checksum/member regressions and common Clippy | Not run on final candidate | Existing fixture tests and strict common lint pass |
| `matching_history::tests` | Not run on final candidate | Exit 0 and saved log |
| `matching_history_peer_resolver::tests` | Not run on final candidate | Exit 0 and saved log |
| `matching_history_membership::tests` | Not run after duplicate-attribute correction | Exit 0 and saved log |
| Matching dispatcher regression | Not run on integrated cache base | Exact filtered test exit 0 and saved log |
| Strict server Clippy | Not run on final candidate | Exit 0 and saved log |
| Go peer placement oracle | Existing prior evidence only | Re-run, fixture/source manifest byte-identical to accepted snapshot |
| Actual mixed Go/Rust History route | Not covered by this runner | Requires production provider injection and a later live binary probe |

## Deadline/error regression coverage review

The current History client module contains focused checks for cancellation of a
redirect RPC, caller deadlines bounding resource retries while preserving the
prior transient error, factory `DeadlineExceeded` being terminal to the resource
retry classifier, live Record*Started timeout selection, and Record*Started
retries stopping at the caller deadline. The retry test uses distinct attempt
messages and asserts that cancellation during a later backoff returns the first
operation error. These tests are included by the module-filter stage.

The boundary test in `matching_history::tests` passes GetMutableState,
RecordDecisionTaskStarted, and RecordActivityTaskStarted request shapes through
the shared RPC request builder and checks that a shorter caller deadline
constrains the outgoing `grpc-timeout`. It does not exercise the dispatch or
query handlers end to end, and it does not prove cancellation or queue/ack
behavior. The runner's dispatch regression is separate; do not claim full
end-to-end deadline parity from these gates.

## Attempt log

Attempt `attempt-20261019T201218Z-ucRiFc` stopped at the first History client
test compile with exit 101. The candidate had two generic closure inference
errors in the deadline-aware refresh/describe calls, and the peer resolver
used `farmhash` without declaring it as a cadence-server dependency. Those
compile failures were corrected before rerunning the complete gate set; no
test assertion ran in this attempt.

Attempt `attempt-20261019T201435Z-ncZKcM` was interrupted after Cargo exposed
that the newly declared direct `farmhash` dependency needed its normal
`Cargo.lock` package-list update. It reached cadence-server compilation and
reported only two dead-code warnings for helpers used by tests; those helpers
are now test-gated. No test result was recorded, and the final runner now uses
`--locked` for every Cargo gate.

Attempt `attempt-20261019T201548Z-211EUS` compiled the library but stopped
while compiling the test target (exit 101). It found a test fixture shadowing
bug and an assertion comparing `Result<_, tonic::Status>` directly; both have
been corrected. No test assertion ran in that attempt.

Attempt `attempt-20261019T201754Z-8fQ3XM` compiled the server test binary and
ran the History client tests (12 passed, 2 failed). The caller-deadline case
used a 25ms budget and was starved before entering its RPC closure; its budget
is now widened to 250ms to retain the deterministic bounded-cancellation
assertion. The timeout test incorrectly expected the factory's 30s request
helper to encode the separate 1s Record*Started operation budget; it now
constructs the RPC request with that operation budget explicitly.

Attempt `attempt-20261019T202250Z-szOBXX` passed all 14 History client tests,
then found one Go placement mismatch for the replacement rune (`U+FFFD`). The
Rust FarmHash crate treats bytes as unsigned in its 0–4 byte path, while
Go's `go-farm` sign-extends each byte as `int8`. A shared Go-compatible
`fingerprint32` now handles that path and is used by Ring placement, History
workflow/domain placement, and Ringpop member checksums/labels. The oracle
fixture remains unchanged.
The resolver gate stopped before later stages; its failure is recorded in
that attempt directory.

The final runner now also executes real Go-generated short-byte FarmHash
vectors, the existing Go Ringpop membership oracle with U+FFFD added as a key,
Ringpop member primitive tests, and strict cadence-common Clippy. This audits
both the History ownership route and other callers of the shared hash helper.

Attempt `attempt-20261019T203141Z-JSv0Tg` passed the new short-byte tests,
Go membership oracle, Ringpop member tests, common Clippy, History client tests,
and the History peer resolver tests (5 passed). The membership snapshot test
failed because its isolated Ringpop node had not been marked ready before the
test injected a PING; the fixture now enables readiness as the node lifecycle
does before handling PINGs.

Attempt `attempt-20261019T203855Z-GYy546` passed all hash, membership, History
client, resolver, and dispatch regression gates. Strict cadence-server Clippy
reported that the new resolver module needed the same `result_large_err`
exception already used for other tonic Status service modules, plus one
redundant closure in the adapter. Both have been corrected before the final
lint run.
