# Matching metric emission comparison (2026-10-24)

## Final CI evidence

- Branch: `t3/matching-metrics-emission-parity`
- Source SHA: `765fbe11d8cb1a646125164c9edbd24bd7a38761`
- CI: https://github.com/auenkr/cadence-rust/actions/runs/40104751037
- Conclusion: success. The runner selected a fresh target at
  `/home/runner/work/_temp/cargo-target-final`; workspace clippy passed before
  workspace tests. Formatting, definition audit, Go fixture capture, four live
  comparisons, and artifact upload also passed.
- Rust comparison binary SHA256:
  `04bcf82de27e16b84a856c3acf5d791ff39bae2804bbe509fecb02b3b80c5e88`
- Generated evidence artifact: `matching-metrics-emission-evidence` in the CI
  run above (3-day retention). It includes raw Go and Rust snapshots, per-policy
  summaries, source SHA, and binary hash.

The Go fixture captures `tally.NewTestScope` output produced by the actual
`metrics.NewClient` and `Client.Scope` paths. Rust exercises the production
`MatchingMetrics` with `MigrationMetricSink`. The compared set is series
identity (metric name, kind, and complete tags), including root rollups and
histogram bucket tags. The policy matrix is:

| Migration policy | Compared series | Result |
| --- | ---: | --- |
| Default | 20 | Match |
| `both` | 24 | Match |
| Name override enables `syncmatch_latency_per_tl_ns` | 21 | Match |
| `both` with name override disabling `syncmatch_latency_per_tl_ns` | 23 | Match |

The representative Matching calls cover add-task request, poll success and
timeout, poller gauge, local sync match, handler sync match, forwarded task,
sync-forward throttle, condition-failed, and sync-match latency timer and
histogram. These exercise the shared manager, RPC handler, and client scope
paths. Defaults, `both`, name-on, and name-off filtering are compared against
the actual Go metrics client. The CI source audit also checked the entire
Matching metric catalog and all 74 Go-to-Rust rollup mappings.

## Persistence wrapper comparison

The Go persistence capture invokes `metered.NewTaskManager` backed by a small
deterministic `TaskManager` implementation. It performs one successful empty
`GetTasks` and one BadRequest failure. The Rust test calls the production
`limit_task_store` wrapper backed by a deterministic `MatchingTaskStore`; the
wrapper records request, latency, response, error-class, and root rollup
emissions through the configured `MigrationMetricSink`. The two generated
snapshots each contain eight series and match on names, kinds, and complete
tags. The snapshots are `go-persistence-wrapper.json` and
`rust-persistence-wrapper.json` in the artifact.

The first live wrapper comparison exposed a real mismatch: Go persistence root
rollups retain `operation=GetTasks`, while Rust emitted empty tag maps. Rust
now preserves the operation tag on counter, timer, error, and histogram root
rollup candidates. Migration filtering still suppresses the histogram by
default. The final CI run passes the wrapper comparison with that correction.

## Matrix coverage and intentionally uncalled definitions

The existing `matching-metrics-matrix-20261023.md` has row-level Go definition,
production callsite, Rust emitter/caller, kind, and tags. The rows group into
the shared handler (`H`), task-list manager (`M`), and adaptive scaler (`S`)
scope paths. This comparison inspects the matrix's current production callsite
inventory and source-reviewed group mappings, then compares representative
calls through those shared paths; it does not invoke all 84 active metric
definitions one by one. The CI audit checks all catalog names, active Go
production references, Rust name inventory, and the full rollup map. The
matrix's older note that tag parity is not exhaustively re-verified per row
still applies outside the live representative paths documented above.

Nine Go catalog definitions have no production Matching callsite: three
buffer-isolation redirect counters and six ForwardPoll/ForwardTask/ForwardQuery
call/error counters. `rust/tools/audit-matching-metrics.py` scans every
non-test Go source file below `go/service/matching` for each definition
identifier. None of these nine identifiers has a production reference; they
are catalog-only and cannot be emitted by the current Go Matching code.

## Limits

The live comparison checks series identity, not sampled numeric values, timer
durations, histogram observations, or rates. The persistence wrapper scenario
uses a fake backend and covers successful empty response plus BadRequest; it
does not exercise every persistence operation or backend error class. The
normal Matching fixture covers representative shared emit paths rather than
executing all 84 active metric definitions. The matrix/audit provides the
per-definition source review, but this evidence is not a mixed-cluster gate or
a claim that every runtime branch of Matching has been exercised.

Earlier attempts on this branch failed before the final successful run:
`40090929433` failed the persistence scope fixture, `40091471559` found the
rollup tag mismatch and fixture scope regressions, `40091983545` found a test
fixture Arc type error, and `40104173563` found one remaining unit-test scope
expectation. All were corrected before the successful run recorded above.
