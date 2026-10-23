# Host resource-rule change

The old metrics gate process group (566034; queued clippy processes 567005/567006)
was terminated with SIGTERM after the user's single-Cargo-lock instruction.
It was blocked on the shared target lock; no metrics rustc child was running.
The initial check had exited 0. The queued clippy did not write an exit-code
file, and tests had not started. No completion code is fabricated for it.

The replacement uses only `/tmp/cargo.lock`, `-j3`, clippy then tests sequentially,
no separate cargo check. It waits in 60-second intervals for mixed gates to stop,
load <= 6, and MemAvailable >= 2 GiB. Final-source outputs use `final-*` names.

The first single-lock queue was canceled before Cargo started because its load
check could go stale while flock waited. The replacement uses nonblocking
`flock -n -E 75 /tmp/cargo.lock cargo ... -j3`, and retries only lock conflicts
(exit 75), rechecking resource/gate guards every 60 seconds. Real Cargo exit
codes, including failures, are recorded normally. It never starts Cargo from
a stale queued resource check.
