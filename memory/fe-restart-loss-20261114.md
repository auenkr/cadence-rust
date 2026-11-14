# Frontend restart loss investigation (2026-11-14)

Branch fe-restart-loss, based on 1004338ef1258b282d3940ce26a21f0bb21b993b.
Original soak: https://github.com/auenkr/cadence-rust/actions/runs/43605415284,
41999/42000. Activity START_TO_CLOSE timeout near Frontend restart.

## Proven transport defect, incident attribution pending

The soak sends SIGKILL, not SIGTERM, so graceful shutdown cannot prevent its
restart window. Rust TChannel connection selects read_frame against completed
RPC tasks and fragment expiration. read_frame used read_exact into local buffers;
Tokio read_exact is not cancellation safe. A completed sibling RPC can drop a
partly consumed header/payload; the next read starts in the middle of the frame,
corrupting the multiplexed connection and aborting its RPC JoinSet. A dispatched
activity poll can then lose its response after History marks it started.
Go tchannel-go connection readLoop retains a frame through dispatch; it does not
race/discard partial frame reads against handler completion.

Fix: keep frame header/payload and byte offsets in per-connection FrameReader,
using cancellation-safe read calls and updating offsets immediately. Regression
forces cancellation at every header/payload byte boundary and checks two successive
frames. This repairs a proven loss mechanism but does not by itself prove the
specific original workflow hit it. Frontend latency tuning is untouched.

Validation completed successfully (see final verdict below). No local Cargo, Go or Docker commands run.

## Original artifact follow-up and shutdown audit

Downloaded soak-rust artifact to /mnt/storage/service/tmp/fe-restart-loss/original
(TMPDIR relocated after /tmp download hit quota; no build/test executed).
SDK worker has one DecisionWorker EntityNotExists at 15:20:16, no activity report
failure. Go History logs ActivityTaskTimedOut at 15:21:13.336 for the lost workflow,
placing its start at 15:20:13.336, approximately the restart instant. Thus a lost
started-poll response is consistent with the evidence; the original logs lack
frame-level diagnostics and cannot prove the byte-framing race occurred.

Go frontend Service.Stop (service.go:319) marks unhealthy, waits discovery, stops
admission/limiters, then drains for min(1 second, configured duration). Rust launcher
uses the same sequence (matching/src/launcher.rs:2489), and WorkflowHandler returns
InternalServiceError("Shutting down") during admission shutdown, which Go SDK's
service transient retry handles. SDK reportActivityComplete also retries transport
failures. Neither sequence executes for the soak's SIGKILL. No change to admission,
retry policy, workflow activity options, or latency tuning is justified here.

Targeted CI: https://github.com/auenkr/cadence-rust/actions/runs/43613537757,
head 286ef1e3a (SUCCESS).

Targeted CI 43613537757 **SUCCESS**, including
`tchannel::tests::interrupted_frame_reads_preserve_header_payload_and_next_frame`.
Compact build/test evidence saved in memory/evidence/fe-restart-loss-20261114/.
Full fresh-target workspace CI dispatched at documentation head 12e0f53d5:
https://github.com/auenkr/cadence-rust/actions/runs/43614446974 (SUCCESS).

Full fresh-target CI 43614446974 **SUCCESS** at
12e0f53d504e177041088b4ef3ff5efae5cf9d2c: formatting, workspace Clippy then workspace
tests, PostgreSQL wakeup and live Go/Rust metric parity. Source identical to
286ef1e3a; subsequent commits only record evidence.

Soak attempt 1 dispatched with defaults (70 minutes, 10 workflows/s,
lanes go-baseline,rust):
https://github.com/auenkr/cadence-rust/actions/runs/43615529134,
head 4e014ea994ffdc0e5ec957dcd2a1df5d13845ce9. Pending. No second soak started.

## Resume checkpoint, 16:53 UTC

Resumed after an interrupted agent turn/rate limit. Soak 43615529134 remains
in progress in both lanes (jobs started 16:33 UTC). No replacement run or second
soak dispatched. Code unchanged; targeted and full fresh-target CI remain green.

## Binary identity, post-midpoint checkpoint

Completed build job logs obtained directly from the GitHub job-log API.
Soak 43615529134 built head 4e014ea994ffdc0e5ec957dcd2a1df5d13845ce9;
Rust binary SHA256:
`e29f01c446d61b25620cb466fc2d6bb6b3c31abaf0e0cf90d8d242bc5770f98b`.
Both lanes remain in progress after the scheduled midpoint; final restart and
completion evidence still pending. No second soak dispatched.

## Final verdict: PASS

https://github.com/auenkr/cadence-rust/actions/runs/43615529134 — workflow
**SUCCESS**, analyzer **RUST OVERALL: PASS**. Default 70 minutes, rate 10,
go-baseline + rust (Rust Frontend/Matching, Go History/Worker).

- Both lanes: **42000/42000 completed**, failed=0, inflight=0. No restart loss
  observed. Restart records and final SDK counts are saved with the summaries.
- Rust lane: no panics/crashes, no failed gate steps. Every analyzer criterion
  passes, including latency and memory/file-descriptor slopes.
- Rust e2e p95/p99 4052/4064 ms vs Go 4095/4118; start p95/p99 6/7 ms vs Go
  14/19; query p99 21 ms vs Go 46. FE RSS slope 0.15 MB/h; Matching 4.19 MB/h.
- Tested head 4e014ea994ffdc0e5ec957dcd2a1df5d13845ce9; Rust binary SHA256
  e29f01c446d61b25620cb466fc2d6bb6b3c31abaf0e0cf90d8d242bc5770f98b.
  Later commits are evidence only. Source fix is commit 286ef1e3a.
- Exactly **one** measured soak dispatched for this task. No second needed.

Reproduce (all commands dispatch GitHub Actions, no local builds/tests):
```
gh workflow run rust-ci.yml --ref fe-restart-loss -f package=cadence-matching
gh workflow run rust-ci.yml --ref fe-restart-loss -f clean_target=true
gh workflow run frontend-soak.yml --ref fe-restart-loss
```

Root cause fixed: cancellation of a partially consumed TChannel frame by the
multiplexed server select loop. Persistent FrameReader state restores Go's frame
retention behavior; the regression forces interruptions at every byte boundary.
The passing mixed soak satisfies the completion/restart acceptance criterion.
It does not retroactively prove which transport interleaving the original lost
workflow hit: that run lacked frame-level diagnostics. Hard-kill delivery has an
inherent response-loss window in Go as well; no workflow retry policy or service
semantics were changed to hide that window. Frontend latency tuning untouched.
