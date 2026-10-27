# Matching adaptive diagnostic comparison: run 40480825761

## Result and interpretation

This is useful single-node diagnostic evidence, not acceptance. The adaptive
SDK probe passed (24/24 load workflows, routing evidence, and SDK scenarios),
and `acceptanceEligible=false`; the recorded gate exit is 0, as are build,
run and probe exits. The retained pending GC task is intentional probe residue;
`acksCoverRemovedRows=false` does not by itself establish a failed assertion.
The lane intentionally omitted two-instance failure recovery.

No concrete adaptive QPS, durable-reader, or child-delivery defect is
established by this comparison. In the prior failing run 40476881898, the SDK
spent its remaining 38.084 s suite budget waiting for a child; it timed out at
38.0347 s with 50 ms left. Parent history showed child start but no terminal
child event. Current run 40480825761 completed that child scenario in 199.7 ms
with 54.278 s remaining. The prior evidence still lacks the child's own
history and its poll/decision result, so the changed outcome is diagnostic
variation, not proof of a fix.

## Timelines and counts

All UTC timestamps below are from run 40480825761's Rust Matching trace.

| UTC | Observation |
| --- | --- |
| 09:30:09.485715 | First scaler tick: `qps=24`, read/write `1/1`, next write `1`. |
| 09:30:10.486744 | Tick: `qps=12`, `1/1`, next write `12`. |
| 09:30:11.507193 | Tick: `qps=102`, `12/12`, next write `102`. |
| 09:30:12.780876 | Tick: `qps=433.5`, `102/102`, next write `434`. |
| 09:30:08.509228 | Durable reader dispatch began for load workflow `-0`, schedule 2, task 1. |
| 09:30:10.790920 | First observed worker poll return boundary (`poll_received`); worker readiness was separately observed. |
| 09:30:10.922357 | First logged child-partition durable completion, acknowledged. |
| 09:30:10.789–10.996 | SDK retry scenario took 4.366 s; first child scenario then took 199.732 ms. |
| 09:30:47.269 | Go SDK worker stopped after all SDK scenarios; 31.951 s of the 60 s suite budget remained. |

The current trace contains 63 scaler ticks, 79 `reader_head_dispatch_begin`,
48 `reader_durable_completion`, and 1,459 `poll_received` records. The trace's
poll event is request arrival; returned workflow/task identities are in
`poll_result`. The examples above establish early successful reader dispatch
and acknowledgement, not a one-to-one join between all pollers and tasks.
The adaptive SDK transcript records all 24 load workflows completed. Its
scenario-line durations are SDK call durations, not workflow execution
latencies; those short values must not be interpreted as task completion
times.

Both 40480825761 and 40476881898 show the same scaler progression
`1 -> 12 -> 102 -> 434`, with sampled QPS `24, 12, 102, 433.5`; subsequent
ticks report zero after the load. Thus the previously observed high terminal
partition count is not by itself new evidence of an anomalous QPS counter.
Current durable reader events show successful child-head acknowledgements
during the adaptive run; prior trace 40476881898 did not contain the durable
reader/poll trace boundary. This makes instrumentation coverage different,
not evidence that the old child was or was not polled.

## Next action and limits

Do not add instrumentation based on this comparison. Continue with the
existing two-instance adaptive acceptance lane (including failure recovery)
and collect child-specific history / poll identity only if the child wait
fails again. If it does, the first discriminating lookup remains the child's
own history, followed by its first decision's Matching poll identity if no
decision-started event exists. No production or harness change is justified
by this single passing diagnostic.

The single-node diagnostic ran Go Frontend/History/Worker and Rust Matching;
it did not validate two-node failure recovery. The gate passed at its recorded
pin, but does not establish Matching
service completion or parity.

## Provenance

- CI run: [40480825761](https://github.com/auenkr/cadence-rust/actions/runs/40480825761), source `d8759bd09c4b74908d77818a14e5a9f15a3aab4a`.
- Rust binary SHA256: `5076795be1078e189066c1be5d3148026638f933e5c6a8000b7a877140059ff8`.
- SDK log SHA256: `81c36612b780f12fb62937515550720d40fe9ab278871b86665602cb307ded35`.
- Rust server trace SHA256: `2cbb30711a4dd855d8643e4d80220d4e4070cd20428571f43c28b25bbae9fcf3`.
- Adaptive result JSON SHA256: `433294dd98e2a7821c178bd2a23c34d0001d95e9b19d528c997572b4757ed77a`.
- Prior failed run 40476881898 SDK log SHA256:
  `dd3bb95df7903b0741e0abec4256f2243f31c1c3a228ab69de4212606bceb9a8`.
- Prior retained trace SHA256:
  `f8af86d776a7ef9fbdd865b71c11a33300c48cb611e805f2bc616b7247ea54aa`.
- Prior run narrative: `memory/evidence/matching-adaptive-child-boundary-40476881898.md`.
- The trace extractor `tests/extract-matching-adaptive-trace.py` preserves IDs
  and does not join records by timestamp. The current downloaded files are
  under `/mnt/storage/service/ci-evidence/40480825761/`; no large raw log was
  added to the repository.
