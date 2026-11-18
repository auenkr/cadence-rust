# History lane dispatch latency (2026-11-19)

Branch `perf-history-dispatch`. Fix commit `91b767573de107c1cb43dd0252d6f3e076ddefaa` (base master f8eccb140).

## Findings
- Re-measured on current master (after 451146825, the burst DynamicRateLimiter): the decision schedule-to-start (STS) gap is gone.
  At 15/s, Rust decision STS p99 is 12 ms vs Go 19 ms, and Rust completion p50/p99 is 44/56 ms vs Go 51/75 ms. No queue/notify change needed.
- The activity STS of "~1 ms vs 0.08" was a timestamp artifact, not a delay. Rust History stamped engine events with
  `now_millis() * 1_000_000` (truncated to ms); Go uses `timeSource.Now().UnixNano()`. Since ActivityTaskScheduled was truncated
  and ActivityTaskStarted was not, the measured STS was 0–1 ms (p50 0.49, p99 1.00) on every run.
- Fix: the 7 engine files in `wiring/assembly/` (decision_ops, decision, events, events_transfer, timer, continue, semaphore)
  now use the nanosecond clock, and `now_millis` was removed. New unit test `scheduled_activity_keeps_nanosecond_timestamp`.
  No API/logic change.
- Go-arm generator exit 1 at 25/s: did not recur in either 25/s run (all arms 0 errors). Not reproducible; not investigated further.

## Runs
- rust-ci cadence-history on 91b767573: https://github.com/auenkr/cadence-rust/actions/runs/44378993373 (success)
- before 15/s (f8eccb140): https://github.com/auenkr/cadence-rust/actions/runs/44378647976
- before 25/s (f8eccb140): https://github.com/auenkr/cadence-rust/actions/runs/44378654432
- after 15/s (91b767573): https://github.com/auenkr/cadence-rust/actions/runs/44388394348
- after 25/s (91b767573): https://github.com/auenkr/cadence-rust/actions/runs/44388399444
All runs: History lane, 30 s warmup, 120 s, 3 alternating pairs, 0 errors.

## Table (p50/p99 ms: completion | decision STS | activity STS)
| rate | run | before Go | before Rust | after Go | after Rust |
|---|---|---|---|---|---|
| 15 | 00 | 51.4/74.8 \| 9.4/19.2 \| 0.02/0.05 | 44.4/56.1 \| 7.4/12.7 \| 0.48/1.00 | 41.0/120.6 \| 7.1/19.7 \| 0.02/0.04 | 35.0/49.3 \| 5.1/10.0 \| 0.01/0.05 |
| 15 | 01 | 52.3/75.8 \| 9.7/20.3 \| 0.02/0.05 | 44.1/56.4 \| 7.3/12.9 \| 0.49/1.00 | 41.0/65.6 \| 7.0/16.3 \| 0.02/0.05 | 35.0/154.2 \| 5.1/32.6 \| 0.01/0.04 |
| 15 | 02 | 51.0/72.0 \| 9.4/19.0 \| 0.02/0.05 | 43.7/56.1 \| 7.3/12.1 \| 0.49/1.01 | 41.1/114.4 \| 7.1/21.0 \| 0.02/0.04 | 35.2/111.4 \| 5.2/23.4 \| 0.01/0.05 |
| 25 | 00 | 17099/50740 \| 2146/30226 \| 0.03/151 | 75.5/264 \| 15.3/104 \| 0.56/1.01 | 52.3/1501 \| 9.7/695 \| 0.02/22.9 | 36.5/211 \| 5.5/93.4 \| 0.01/0.12 |
| 25 | 01 | 11663/47094 \| 1607/27669 \| 0.03/99 | 314/10280 \| 96.9/4094 \| 0.57/3235 | 54.2/1734 \| 9.7/868 \| 0.02/21.0 | 37.8/203 \| 5.7/88.7 \| 0.01/0.10 |
| 25 | 02 | 221/35225 \| 44.4/18931 \| 0.03/86 | 101/2721 \| 21.7/1203 \| 0.57/792 | 58.4/2718 \| 11.1/1487 \| 0.02/40.8 | 39.0/203 \| 6.0/89.7 \| 0.01/0.11 |

Notes: the activity STS fix is deterministic (Rust p50 0.49 -> 0.01 ms, now equal to Go). Other shifts between the before and after
batches also move the Go arm (runner variance; the before 25/s batch saturated both arms on that runner). In every run, Rust p50
completion is lower than Go's. At 15/s, Rust p99 had outliers in pairs 01/02 of the after batch, and Go p99 did too.
