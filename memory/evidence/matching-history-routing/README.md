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
