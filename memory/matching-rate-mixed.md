# Matching rate-limiter mixed validation (2026-10-16)

## Full runner: 30/30 passed with the limiter wiring
`tests/run-compatibility.py --artifacts-dir .../matching-limiter-full
--go-mixed-server-bin .../matching-mixed-20261016/cadence-server` passed every
stage, including `go-services-matching-delivery` (146s) and
`go-services-rust-matching` (18s), i.e. original Go Frontend/History/Worker
routed to Rust Matching. Results:
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-limiter-full/results.json`.
This validates that wiring the dispatch limiter did not regress any existing
scenario, but it does NOT by itself exercise rate limiting.

## New mixed rate scenario (uncommitted at save time)
`tests/matching-service-compat.cjs` now schedules three activities on a fresh
task list, then polls each with `taskListMetadata.maxTasksPerSecond = { value: 1 }`
via the public Go Frontend WorkerAPI (the field is forwarded verbatim to Rust
Matching). One-second dispatch pacing means the third (backlog) poll must wait
for its token. Assertions:
- delivered activity inputs are the set {paced-0,paced-1,paced-2} (dispatch is
  NOT identity-FIFO);
- last poll latency >= 500 ms (proves paced backlog dispatch);
- `DescribeTaskList(ACTIVITY).taskListStatus.ratePerSecond === 1` (proves the
  Describe wiring reads the reported backing rate).

### Evidence / status
- First standalone run (`.../matching-paced-mixed`) failed ONLY on an incorrect
  assumption: the probe asserted ordered identity and received `paced-2` where it
  expected `paced-1`. The Rust Matching server, Go services and durable delivery
  all behaved; only the probe's ordering assert was wrong.
- Fix applied (assert the delivered set, keep the >=500 ms last-poll bound).
  Re-run (`.../matching-paced-mixed2`) was launched in tmux `cadence-rust` but
  the machine restarted before it completed. MUST re-run and confirm before
  claiming this gate.

### Reproduce
```
python3 tests/run-go-service-baseline.py --rust-matching \
  --artifacts-dir /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-paced-mixed2 \
  --binary /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-20261016/cadence-server
```
Inspect `probe.log` in that dir for the node assertions.

## Still open for the Matching rate gate
- Confirm the mixed rate scenario passes.
- Real partition count provider (limiter divides by a hardcoded 1).
- Do not assert the first two deliveries after a rate transition are spaced
  (an old-rate token/head may still be in flight).
