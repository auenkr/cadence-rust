# Adaptive scaler original-Go control, run 40473213335

Run: https://github.com/auenkr/cadence-rust/actions/runs/40473213335  
Conclusion: success; source/head `fc359c05e16b2abff94b757c7d301986e6b93572`.  
Downloaded evidence: `/mnt/storage/service/ci-evidence/go-control-40473213335/matching-production-adaptive-go-control/`.

## What completed

This was the unmodified original-Go adaptive control (`adaptive-go-control.json`
and `adaptiveScaler.json`), not Rust acceptance. It ran the Go Frontend,
History, Matching and Worker services; `results.json` records
`acceptanceEligible:false`, `rustServices:[]`, `goMatchingStarted:true`, and
all four Go services. The lane and probe both exited 0. Worker startup
readiness was observed before probe execution.

The adaptive scenario passed with the existing Go SDK. It enabled adaptive
scaling and cache-backed partition counts for
`production-adaptive-scaler-probe` (workflow task type 0), submitted 24 load
workflows and completed all 24. The partition description changed from version
0 / one write partition to version 1 / 12 write and 12 read partitions. The
existing SDK worker/client then completed a workflow and activity after the
scaler update. It also completed the 576 routing assertions (read IDs 0–575);
each route's requested partition delivered and its workflow completed. The
probe's serving-proof caveat remains: the RPC response does not expose which
internal partition physically served after forwarding.

Timing controls in `dynamic-config.yaml` were: idle task-list check 3s,
adaptive scaler update interval 1s, QPS tracker interval 1s, upscale RPS 1,
and sustained upscale duration 1s, all constrained to the probe list and type
0. Scaler update/version evidence is in `adaptiveScaler.json`; it does not
record the exact time at which partition count changed. The captured Go server
log has task-list lifecycle events but no useful scaler transition timestamps.
Do not infer a scaler latency from this run beyond the configured 1s polling /
sustained thresholds.

The route evidence shows one distinct candidate Matching endpoint,
`127.0.0.1:14110`, across all 576 requests. This is a one-Go-Matching-instance
control. It is not topologically equivalent to Rust diagnostic run
40469125507, whose probe started Rust Matching in a two-instance setup
(documented in that run's artifact README / production Ringpop topology).

## Comparison with Rust diagnostic 40469125507

Rust run 40469125507 failed at the retained SDK assertion: only one scaled-list
SDK operation completed (`Go SDK did not complete work ...: 1`). Before that,
retry-only completed in 37.769s with 22.185s budget left, and injected workflow
failure recovery succeeded. The activity-timeout workflow subsequently timed
out; its history showed the activity timeout and a scheduled next decision,
but no completed decision before the SDK deadline. The full 24-terminal
history assertion and final explicit routing phase were not reached. Its
instrumentation showed repeated forward attempts and no successful token
return for the affected delivery. See
`memory/evidence/matching-adaptive-trace-40469125507/README.md` and the
retained logs under that evidence directory.

These outcomes do not prove a Rust scaler or queue defect. The Go control
demonstrates that this scenario and its assertions can pass on original Go
under one Matching instance; Rust ran with two. Different instance count,
ownership and forwarding paths are material confounders. Also, Go's control
proves outcome and final partition counts, not matching scaler-transition
timing at the same fine-grained boundary as the Rust diagnostic. No concrete
source defect is established by these artifacts.

## Next adaptive experiment

Parent should run one serial, matched-topology pair before changing production
logic: preserve the probe assertions, SDK version, 60s SDK budget, task-list
name/type, load (24), dynamic-config values above, database and Go
Frontend/History/Worker versions; use two Matching instances for both original
Go control and Rust candidate. Capture per-instance endpoint/owner membership,
partition count/version snapshots with timestamps, task enqueue and poll/token
return boundaries, SDK scenario start/end and history terminal events. The Go
control should first show whether two-instance Go reproduces or clears the
timed delivery behavior. Then run the Rust candidate under the same topology.
Keep the 24 completions and 576 route-delivery/completion assertions unchanged.
If possible, also retain the one-instance runs as a topology sensitivity pair.
This isolates instance count and yields the missing scaler/queue timing without
relaxing acceptance assertions. Parent owns serial gate dispatch; no gate was
dispatched for this research note.

## Provenance

- Run: 40473213335, successful original-Go control, head
  `fc359c05e16b2abff94b757c7d301986e6b93572`.
- Go binary SHA256 is in `go-binary.sha256` in the downloaded evidence tree.
- Probe command, dynamic config, ports, full SDK logs, `results.json`,
  `adaptiveScaler.json`, server log and exit files are retained beside this
  note at the evidence path above.
- Rust comparator: run 40469125507; source `7d072c25422c165c92e6912d4210de7b8bb741a4`,
  binary SHA256 `c347383fda712d9c1da14c96a63a61116fc2240cf42e2064e207dd1b3f9e42e0`.
