# Review of Matching readiness trace candidate `a534baae8c921e0ec8109527c6bc3184937b0323`

## Activation and rerun input

The opt-in trace is activated end to end for the PostgreSQL production lane:

1. `.github/workflows/matching-production-validation.yml` sets job-level
   `CADENCE_MATCHING_LIST_PARTITIONS_TRACE: "1"`.
2. `run-postgres-lane.sh` starts `run-go-service-baseline.py` without replacing
   its environment.
3. The Python probe builds `probe_environment` from `os.environ` for production
   Matching, then passes it to `subprocess.run` for Node.
4. `matching-service-compat.cjs` starts primary and secondary Rust processes
   with `process.env` (or its spread), preserving the toggle.
5. The Rust helper checks for presence of the variable and writes stage lines
   to stderr; the probe and Rust stderr are retained in the gate logs.

The parent can dispatch this exact diagnostic rerun (no local or review-side
dispatch was made):

```sh
gh workflow run matching-production-validation.yml \
  --repo auenkr/cadence-rust \
  --ref t3/diagnose-matching-gate-failure \
  -f lane=postgres
```

No additional trace input is needed. For equivalent manual launches, export
`CADENCE_MATCHING_LIST_PARTITIONS_TRACE=1` in the environment of
`run-postgres-lane.sh`; do not set it only on the later Node command.

## Interpreting the last trace line

The current sequence is emitted separately for each request but has no request
correlation ID. A single primary readiness request is expected before teardown
in this lane; if multiple requests overlap, stage lines cannot be assigned
reliably to one RPC. Domain and task-list identifiers are not logged.

- No `handler-enter`: request did not enter this Rust handler, or stderr was
  not captured.
- `handler-enter` without `admission-complete`: stuck/failing
  `check_started` or admission; errors at this boundary currently have no
  trace error marker.
- `admission-complete` without `engine-begin`: handler path exited unexpectedly
  between admission and engine invocation.
- `engine-begin` then `domain-resolution-begin` without completion: context
  domain ID resolution failed or stalled. The current source emits the
  completion marker only on success.
- `postgres-domain-read-begin` without `postgres-domain-read-complete`: cold
  domain lookup in persistence stalled or failed. This error path currently
  has no distinct error marker.
- `domain-cache-refresh-begin` without completion: cache refresh stalled or
  failed after the initial domain read.
- `domain-resolution-complete` without either partition-count marker: stalled
  or failed while constructing the task-list key or fetching partition
  configuration/count.
- `*-owner-lookup-begin` without matching completion: owner lookup stalled;
  note that owner lookup errors are currently swallowed into an empty owner,
  so a completion marker means only that the await returned.
- `engine-error`: engine returned an error, but the trace does not identify
  which operation failed or its safe error class. The RPC response/probe log is
  needed to correlate the failure.
- `engine-complete`: the Rust engine completed. If the client still times out,
  investigate response delivery or a different request/process.

The process-wide cap emits at most 32 lines, including primary and secondary
Matching processes. The counter is static per process; each process gets its
own allowance. The trace does not acquire Matching locks or call logging
callbacks, so there is no lock-held logging recursion.

## Review findings and separate parity item

Activation is correct. The trace has markers on both sides of the important
admission, persistence/cache, partition-count, and owner-lookup boundaries and
does not log domain/task-list values. Its main diagnostic limitation is that
errors do not carry a correlation ID or bounded safe error category. For this
single-node readiness request, the ordered stage sequence can still narrow the
last blocking stage, but concurrent retries would be ambiguous. The trace is
observational and does not change request behavior.

The prior gate facts remain: Matching startup completed; the first actual
`ListTaskListPartitions` readiness call timed out before SDK workflows or
failure/restart recovery started. Later Ringpop removals happened during
teardown and are not evidence of an earlier spontaneous membership failure.
This rerun is intended to locate the handler/cache/persistence/owner stage; it
does not retroactively provide SDK or recovery evidence.

Separately, `ListTaskListPartitions` does not forward the gRPC request deadline
into its cold domain-cache lookup: `NativeMatchingService::list_task_list_partitions`
discards request metadata and `NativeMatchingEngine::list_task_list_partitions`
calls `context.domain_id` without a deadline. The cache uses its fixed
`DOMAIN_CACHE_PERSISTENCE_TIMEOUT`. Track this as an independent deadline parity
issue; it is not established as the cause of run 41039044942 and should not be
folded into the trace diagnosis.

No runtime, build, formatter, or CI validation was performed for this review.
