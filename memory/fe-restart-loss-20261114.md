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
