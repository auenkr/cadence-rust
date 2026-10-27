# Matching delivery delay audit: run 40476007015

This bounded source audit compared Go Matching's async backlog dispatch and
sync offer/poll paths with Rust's `matching_reader.rs` and
`matching_sync.rs`. It made no code changes and ran no local Cargo, Go, build,
test, format, or runtime commands.

## Evidence reviewed

- Run evidence: `memory/evidence/matching-adaptive-single-40476007015/`.
- `gate/run/results.json` identifies Rust Matching with Go Frontend, History,
  and Worker, with `goMatchingStarted: false`. The gate's `acceptanceEligible`
  is false and the probe exited 1; use it as a diagnostic observation only.
- `memory/matching-acceptance-status-20261027.md:25` records the measured SDK
  delays: initial 24 completed quickly; activity-retry 18.486s, child 11.784s,
  workflow-run-retry 20.564s, then workflow-task-retry exhausted the remaining
  7.344s. The Go one-node control is a separate run and passed.
- These workflow and activity tasks are async backlog deliveries. They do not
  traverse Rust `matching_sync.rs`, so the sync matcher is not a direct cause
  candidate for these delays.

## Source comparison

- Go reads tasks into per-isolation buffers in `go/service/matching/tasklist/task_reader.go:200-223`
  and dispatches each head with retry in `:470-535`. Deadline and throttling
  outcomes retry that same head; throttling yields (`:540-555`).
- Go `MustOffer` first waits for `LocalTaskWaitTime`, then concurrently selects
  local poll handoff and a forwarding token. A failed forward waits up to its
  two-second child context for a local poll before retrying
  (`go/service/matching/tasklist/matcher.go:330-408`).
- Rust's reader retains one dispatch loop per buffer, uses the same local wait
  before obtaining a forwarding token, then reoffers locally for the forward
  deadline after a failed forward (`rust/crates/cadence-matching/src/matching_reader.rs:680-745`).
  Throttled and timed-out attempts yield before retrying the head (`:838-842`).
- Rust's rendezvous queues offers FIFO and removes them on future drop; poll
  cancellation does not strand an item (`rust/crates/cadence-matching/src/matching_reader.rs:150-275`).
  Token acquisition races a local acceptance under the same lock before
  forwarding. This differs in scheduler arbitration details from Go channel
  `select`, but inspection found no lost handoff or starvation defect: local
  accept wins if it claimed the item, otherwise the token path removes the
  unclaimed offer atomically before forwarding.
- Rust sync offers are separately cancellation-safe and wait for History's
  completion response (`rust/crates/cadence-matching/src/matching_sync.rs:90-215`);
  that path does not explain the async workflow/activity retry observations.

No concrete source divergence explains the delivery delays. There is no
regression or implementation change justified by this evidence, and no timeout
increase is proposed.

## Discriminator for a follow-up trace

For one delayed workflow/activity task, correlate its task-list and task ID
across these stages, with monotonic timestamps on the Rust Matching process:

1. Persistence allocation/append and task ID assignment.
2. Reader scan sees the row and advances the read level; record buffer enqueue
   and buffer-head dequeue separately.
3. Dispatch attempt start, selected isolation group, limiter wait completion,
   local-wait end, each forward-token acquisition/result, and final local
   handoff or forward success.
4. Matching poll request start/cancel and task receipt; for sync tasks also
   record History start-result response.

The first stage gap discriminates the remaining hypotheses: a long gap before
scan/head dequeue points to persistence/read-range/partition scheduling; a gap
after dequeue before a poll receipt points to limiter, local offer/token
arbitration, or absent/misdirected pollers; a prompt poll receipt followed by a
long SDK wait points downstream of Matching delivery. Preserve the existing
SDK assertion and deadline; do not loosen the scenario to hide the delay.

## Limits

Run 40476007015 has no Matching exporter (`matchingExporter: null`), does not
qualify as acceptance, and its probe failed. Its result timings establish the
symptom under the recorded topology but cannot locate the delay inside
Matching. The currently live traced run 40476881898 was not inspected or
polled, per task instruction. No Actions validation was needed because this is
documentation only.
