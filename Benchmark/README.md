# Repeatable SDK benchmark

The initial gate compares **All Go** against **Rust Matching + Go Frontend/History**.
Everything new lives here except GitHub's required workflow entry points.
This benchmark is independent of the older closed-loop Frontend+Matching harness.
Do not combine its numbers with those historical results.

## Initial pipeline

`driver/` uses the pinned existing Cadence Go SDK over TChannel. One workflow
runs one deterministic echo activity with a controlled 10 ms delay and a 1024-byte
payload. Both arms run the same executable, workflow, SDK settings, worker
capacity (128 activity and decision executions), task polling settings, and
non-sticky execution. Change these defaults only as an explicit scenario change.
Production workload distributions were not supplied; these are synthetic inputs.

The generator schedules each arrival against an absolute monotonic deadline.
It never waits for a workflow to finish before scheduling another. Each scheduled
ID gets a JSONL ledger entry, including start success, run ID, result verification,
expected/observed result SHA256, client timeout/error and generator scheduling delay. A 10,000-inflight safety bound
**records a dropped arrival and fails**, rather than throttling starts. Warmup
uses the same arrival process and drains before measurement. Measurement drains
for at most the per-execution two-minute client budget after arrivals end.

The worker runs in a separate process pinned to one CPU, the generator and
collector on another, with Cadence and MySQL restricted to two other CPUs. This
requires a four-CPU standard public Actions runner. Cadence services each have
identical 1 CPU/1400 MiB limits and MySQL has 1.5 CPU/1800 MiB. CPU sets intentionally
oversubscribe the service pool equally in both arms; this is a small-runner
benchmark, not a production sizing recommendation. The Go system Worker remains
present in both arms and is distinct from the SDK activity/decision worker.

Each arm destroys and recreates MySQL, including schema setup using the same Go
image and schema, and registers the same fresh domain. Record the resolved Compose
file, generated service configs, dynamic configs, image IDs, process inventories,
DB container identity, runner CPU details, source commit, SDK lock hash, driver
hash and **actual Rust executable SHA256**. Binary hash comes from the actual
container image, not a source or image tag. Runtime images build uncached on CI.

After every successful completion the driver pages its history and checks exactly
one activity start/completion, at least two decisions, and no failed/timed-out
tasks. Schedule-to-start samples use History event timestamps; activity duration
uses started/completed events. History audit RPCs add identical diagnostic load
to both arms; the client in-flight bound includes them. Completion latency ends
before the audit and includes generator delay. Start latency includes SDK retries.

`report.py` rejects missing/duplicate ledger entries, wrong results, warmup errors,
missing task samples, invalid latency, and excessive generator lag. Reports include
p50/p95/p99/p999, starts/completions per arrival-window second, History-timestamp
activity throughput, and outstanding executions at the arrival-window end.
The complete raw ledger remains available, including drained completions. Low
sample-count p999 values are not statistically strong tail estimates.

`collect_resources.py` uses a warmup-excluding baseline handshake and end marker.
It measures cumulative container CPU, sampled cgroup memory, summed process RSS
(shared pages may count twice), network bytes and cgroup v2 block I/O bytes for
all services **and MySQL**. Collection includes the completion drain and history
audits; it must not be described as precisely the arrival window. CPU averages
use the actual collection interval. Missing v1 disk/RSS values remain null.

## Run on Actions

Repository must remain public; only standard `ubuntu-latest`, 90-minute job limit,
three-day artifacts and cancellation of superseded runs are used. No local Cargo,
Go builds/tests or mixed gates. From the repository root:

```sh
# Existing registered entry point, initial fixed 5/s, 10s warmup, 30s measurement:
gh workflow run frontend-matching-benchmark.yml --ref t3/matching-benchmark-harness -f sdk_openloop=true
# New dedicated entry point (available after GitHub registers it):
gh workflow run sdk-matching-benchmark.yml --ref t3/matching-benchmark-harness \
  -f rate=5 -f warmup=10 -f duration=30 -f repetitions=1 -f repetition_offset=0
```

Artifacts contain `Benchmark/evidence/run/` contents. Retain compact real evidence
here before the artifacts expire, with run URL, tested head and conclusion.
`ci.sh` records its final exit even on error. `run.py` records subprocess commands,
exits and logs and always tears down database/service containers. It alternates
Go/Rust order by repetition index. Use odd offsets to start with Rust.

A full pair needs **80 minutes of traffic alone** at 600s warmup / 1800s measurement
per arm. Image builds and readiness can exceed the 90-minute CI limit. The short
workflow defaults deliberately validate the pipeline only. Do not reduce warmup
and then describe it as the full protocol. A build/runtime artifact handoff or
separate per-arm jobs with verified equivalent runner hardware is needed before
running three full pairs. Large image artifacts must fit the free storage quota;
this implementation does not publish images or allocate paid storage.

## Completion criteria and expansion

No speedup claim is enabled by this initial report. Before performance conclusions:

1. Complete the initial Go/Rust Matching gate and retain actual passing evidence.
2. Add persistence query latency/ops/connection instrumentation, SDK/server retry
   counters, task queue backlog size/age and resource time series for growth.
   MySQL CPU/memory/I/O already share the resource collector. Outstanding workflow
   count is **not** server queue backlog. Missing metrics are named in the manifest.
3. Add scenarios in `scenarios.json`: sequential activities, fan-out, timers,
   signals/queries, large histories/payloads, failures/retries/timeouts. These are
   specified, **not implemented**, and must pass correctness before timing.
4. Add Frontend-only, History-only and all-Rust arms, preserving the Go system
   Worker until its own replacement is explicitly benchmarked. The initial runner
   intentionally allows only the two Matching arms.
5. Implement stepped open-loop capacity against explicit p99/error/backlog targets
   and recovery fault injection with traffic continuing. A failed fixed-load run
   is not a capacity estimate or recovery measurement.
6. Run >=3 alternating pairs with 10-minute warmup and 30-minute measurements.
   Report resources at equal verified completion throughput and capacity at equal
   latency/error targets, including all failed runs and repeat variation.
7. Run a longer low-CPU soak against prebuilt binaries locally in tmux
   `cadence-rust`, under `work-safe`, nice/ionice and host load/RAM guards.

There are no production-derived sizes, latency SLOs, error budgets or soak duration
in the request. Choose and record those before enabling capacity comparisons.
