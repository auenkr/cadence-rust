# V7 two-node MySQL dispatch review (2026-10-27)

## Dispatch arguments

The `matching-mixed-gate.yml` workflow input `extra_args` defaults to
`--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3`.
When a nonempty `extra_args` value is supplied, the Gate step uses it in place
of `DEFAULT_ARGS`; it does not append the defaults. Preserve all three default
flags and add the two opt-in Prometheus switches:

```sh
gh workflow run matching-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" \
  -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3 --matching-exporter prometheus --matching-cold-domain-metric-probe'
```

Set `FINAL_REF` to the immutable v7 candidate ref/SHA intended for the same-pin
rerun. The workflow itself adds `--rust-services matching`, `--mysql`, the
prebuilt Rust production binary and artifacts path through
`tests/gates/run-mysql-lane.sh`; do not repeat those in `extra_args`.

## Source-verified behavior and assertions

- The two Prometheus options are opt-in. `--matching-exporter prometheus`
  enables capture and semantic validation. `--matching-cold-domain-metric-probe`
  has no default and is rejected unless production Rust Matching and the
  Prometheus exporter are selected.
- Combining exporter Prometheus and failure recovery configures a distinct
  secondary-node metrics listener. The scraper polls both addresses, and the
  validator receives both as expected endpoints. It fails if either endpoint
  has no nonempty samples, if required metric samples or the labeled query
  request sample are absent, or if the cold-domain evidence is missing/wrong.
- The cold-domain probe registers late domains, queries by name before UUID
  warming, checks the domain-name and partition lookups, sends concurrent
  AddDecisionTask requests to exercise the exact-domain one-token quota, and
  asserts RESOURCE_EXHAUSTED. Exporter validation requires its positive
  canonical AddDecisionTask metric sample with expected labels.
- The retained default flags still request the normal process restart, the
  two-instance failure/recovery path, and the 3-second idle check. The probe
  source includes idle eviction/recovery and SDK completion, owner SIGKILL and
  survivor/restart recovery checks, plus SIGTERM drain/recovery. Confirm their
  actual outcomes in artifacts; requested args alone are not runtime evidence.

## Evidence to inspect after a future run

Workflow artifacts are named `matching-mixed-gate`. Review at minimum:

- `out/commit.txt`, `out/binary.sha256`, `out/command.txt`,
  `out/cargo-build.exit`, `out/gate.exit`, and `out/gate.log` for pin, binary,
  exact invocation, and exits.
- `out/gate/run/results.json` for the Go F/H/W + Rust Matching topology,
  exporter semantic results and SDK outcome.
- `out/gate/run/probe.log`, `out/gate/run/prometheus-scrape-samples.jsonl.gz`,
  `out/gate/run/rust-matching/matching-cold-domain-metric-probe.json`,
  `out/gate/run/production-failure-recovery-checkpoints.json`,
  `out/gate/run/matching-shutdown.json`, restart/SDK logs, and both Rust server
  logs for scenario-level results.
- `out/gate/mysql-container-cleanup.txt` (written by the lane cleanup trap as
  `out/gate/mysql-container-cleanup.txt`) and `out/gate/mysql-container.log`
  for owned-container cleanup evidence.

Check for late name lookup and partition lookup results; quota rejection code
8 plus the corresponding positive canonical metric; successful scrapes and
nonempty samples from both listener endpoints; required positive metrics and
query labels; both Rust instances and their shared ownership map; idle release
and subsequent SDK workflow completion; crash survivor/takeover and restarted
member rejoin; restart task durability/completion; graceful drain; and clean
MySQL teardown. Record run URL, head SHA and conclusion only after reviewing
the run/artifacts. This source review makes no runtime claim.

## Confirmed blocker

The currently reviewed workflow default omits both Prometheus flags. A dispatch
that supplies only the prior default-equivalent args will therefore skip the
late-domain/quota probe and Prometheus checks. Because supplied `extra_args`
replaces defaults, the command above must explicitly carry restart, failure
recovery and idle flags as well as both opt-ins. No other blocker to forming
the requested dispatch was confirmed by these source reads; runtime behavior
remains unverified until a same-pin run is dispatched and its artifacts are
reviewed.
