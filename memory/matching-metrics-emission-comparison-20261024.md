# Matching metric emission comparison (2026-10-24)

Source: `35d815323c2999765d10c8e30410b7ec73fd95cf` on
`t3/matching-metrics-emission-parity`.

## Reproduction and result

CI run: https://github.com/auenkr/cadence-rust/actions/runs/40089465425

The `rust-ci.yml` runner used a fresh `/home/runner/work/_temp/cargo-target-final`
directory, passed workspace clippy followed by workspace tests, and ran the
Go/Rust emitter comparison. The comparison captured `tally.NewTestScope` output
from `metrics.NewClient` in Go and `RecordingMetricSink` output from Rust's
`MatchingMetrics` wrapped with `MigrationMetricSink`. The scenario emits add
task request, poll success/timeout, poller gauge, local sync match, sync
forward throttle, forwarded request, condition-failed, and add-decision sync
match timer/histogram series. Persistence response empty/row/payload emitters
are also captured.

| Migration policy | Compared series | Result |
| --- | ---: | --- |
| Default | 20 | Match |
| `both` | 24 | Match |
| Name override enables `syncmatch_latency_per_tl_ns` | 21 | Match |
| `both` with name override disabling `syncmatch_latency_per_tl_ns` | 23 | Match |

The comparison checks name, kind, and full tag map, including Go root rollups
and exponential histogram tags. Raw Go and Rust captures and per-policy
comparison summaries are in the CI artifact `matching-metrics-emission-evidence`
for run 40089465425 (3-day retention). Rust binary SHA256:
`04bcf82de27e16b84a856c3acf5d791ff39bae2804bbe509fecb02b3b80c5e88`.

The audit in the same run compared every `MetricDefs[Matching]` rollup mapping
against the Rust static map (74/74) and the existing Go/Rust migration
allowlists. It also reported the nine catalog-only definitions: the three
buffer-isolation redirect counters and six ForwardPoll/ForwardTask/ForwardQuery
call/error counters. They have no non-test identifier reference anywhere under
`go/service/matching`, so Go has no production emission to mirror for them.

## Correction made

Go `metricsScope` emits root rollups from the scope created by `Client.Scope`.
That root retains the static `operation` tag while dropping per-task-list,
domain, caller, and transport tags. Exponential histogram rollups retain the
Go histogram start/end/scale tags. Rust's Matching sink previously emitted
only the scoped series. It now emits the corresponding root candidate and
lets `MigrationMetricSink` filter it by the rollup's own name. The live
comparison found and fixed the operation-tag and exponential-histogram capture
differences before passing.

## Remaining evidence limit

The persistence comparison currently exercises the production response
emitters used by the Rust persistence limiter (`record_persistence_empty_response`,
`record_persistence_response_rows`, and `record_persistence_response_payload`)
and equivalent `Client.Scope` calls in the Go fixture. It does not yet run a
Go `metered.TaskManager` call against a mock persistence store alongside a
Rust `limit_task_store` call for successful and failed backend outcomes.
Consequently this evidence does not close parity for persistence request,
latency, error-class, retry, or full wrapper outcome emissions. Those remain
open for the Matching metrics audit; the passing RPC/response-series comparison
must not be read as full persistence-wrapper parity.
