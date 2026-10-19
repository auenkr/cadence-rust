# Matching outbound History routing — 2026-10-19

Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b` (source-only
follow-up to r6; based on root `f6e6363d7`).

`MatchingHistoryPeerResolver` routes workflow IDs with the pinned Go
`WorkflowIDToHistoryShard` fingerprint and rune shard key, then resolves the
current History member's selected named transport address. The same resolver
maps typed `ShardOwnershipLostError` owner addresses to current named ports.
All five Matching outbound calls use this path and the shared redirect loop:
mutable state, refresh tasks, standby describe, decision started, and activity
started. Redirects accept only gRPC Aborted with the first typed detail named
`uber.cadence.shared.v1.ShardOwnershipLostError`; they share the operation's
deadline and stop on any other error.

The wrapper stack follows the Go factory and resource wiring. Each History RPC
gets a fresh `HistoryDefaultTimeout` (30 seconds by default), and resource
retries use 50ms initial / 10s maximum / 30s expiration with the default 1s /
10s ServiceBusy throttle. `DeadlineExceeded` from the factory timeout is
terminal because this retry wrapper does not configure `WithOperationTimeout`.
The retry loop preserves Go `ThrottleRetry.Do`'s prior-error result when a
later attempt is followed by a terminal error, retry exhaustion, or caller
cancellation during backoff. RecordDecisionTaskStarted and
RecordActivityTaskStarted add their separate live domain timeout (1 second by
default), bounded across History retries and owner redirects; their outer
250ms / 1s / five-attempt retry stays distinct. `with_history_rpc_timeout`
allows callers to configure the factory timeout; the legacy constructor's
timeout argument remains the Record*Started fallback used by current runtime
callers. Matching poll deadlines now reach both Record*Started retry layers,
the inactive-domain refresh call, and query mutable-state retry. A configured
zero Record*Started timeout disables that added operation deadline, matching
Go `WithOperationTimeout(0)`, while the caller deadline still applies.

## Reproducible placement evidence

`python3 tests/matching-history-peer-oracle/run.py` runs the Go production
`history.NewPeerResolver` against the real Go membership ring and writes
`tests/matching-history-peer-oracle/evidence/`. The checked-in Go output and
source hash manifest are also copied to
`memory/evidence/matching-history-peer-oracle/`. The Rust placement test reads
the Go fixture and compares all recorded workflow/domain/shard routes, both
named transport choices, address resolution, and post-removal routing. Separate
Rust source tests start two tonic peers and verify that typed owner loss on A
redirects an actual RPC to B.

Existing recorded Go oracle: exit 0, Go 1.26.8 linux/arm64; exact command and
output are in `memory/evidence/matching-history-peer-oracle/go-oracle.log`.
The fixture and manifest are checked in beside it. The initial source commit is
`fad59909a`; caller-deadline and zero-timeout changes are uncommitted pending
source review and the parent's serialized build slot. No Rust build or tests
were run for this follow-up yet.
After that grant, run the focused peer resolver / matching History tests and
strict lint, then rerun the Go oracle and compare its generated fixture and
source manifest with the recorded copies. Production discovery injection and
the mixed Go/Rust service probe remain separate Matching runtime acceptance
gaps; this note does not claim service completion.
