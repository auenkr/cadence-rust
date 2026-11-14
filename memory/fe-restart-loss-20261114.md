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

Validation pending. No local Cargo, Go or Docker commands run.

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
head 286ef1e3a (pending).

Targeted CI 43613537757 **SUCCESS**, including
`tchannel::tests::interrupted_frame_reads_preserve_header_payload_and_next_frame`.
Compact build/test evidence saved in memory/evidence/fe-restart-loss-20261114/.
Full fresh-target workspace CI dispatched at documentation head 12e0f53d5:
https://github.com/auenkr/cadence-rust/actions/runs/43614446974 (pending).

Full fresh-target CI 43614446974 **SUCCESS** at
12e0f53d504e177041088b4ef3ff5efae5cf9d2c: formatting, workspace Clippy then workspace
tests, PostgreSQL wakeup and live Go/Rust metric parity. Source identical to
286ef1e3a; subsequent commits only record evidence.

Soak attempt 1 dispatched with defaults (70 minutes, 10 workflows/s,
lanes go-baseline,rust):
https://github.com/auenkr/cadence-rust/actions/runs/43615529134,
head 4e014ea994ffdc0e5ec957dcd2a1df5d13845ce9. Pending. No second soak started.
