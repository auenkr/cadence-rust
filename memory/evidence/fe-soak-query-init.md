# FE soak query "init" FAIL line — investigation (2026-11-19)

## Finding
The single `FAIL ... query query state "init"` line in the FE/MT soak run
[44366018885](https://github.com/auenkr/cadence-rust/actions/runs/44366018885)
(master d1dba9370) is in the **go-baseline** lane (all-Go cluster), not the Rust lane:

- go-baseline: started 42000, completed 41999, failed 1 (`fail_stage_counts: {query: 1}`),
  example `FAIL fe-soak-e75374cd-...-0020994 query query state "init"`;
  query_init attempts 28536 / workflows 20984.
- rust: started 42000, completed 42000, failed 0, `fail_examples: []`;
  query_init attempts 30239 / workflows 21087 (within the +10% compare criterion → PASS).

## Cause
`tests/frontend-soak/main.go` signals `wake` then queries `state`, retrying for 90 s
(`retry()`), and fails if the workflow still answers `"init"`. The query itself
succeeded (no RPC error); the workflow simply had not yet processed the `wake`
signal for 90 s in the Go cluster (soak includes a service restart at 2100 s).
Both clusters hit "init" retries on ~50% of workflows: a query is answered from
the last completed decision, before the signal's decision task runs — that is the
normal Cadence query semantics (Go workflowHandler.QueryWorkflow → history
engine QueryWorkflow → matching QueryWorkflow), identical in Rust.

## Verdict
Parity, Go behaviour; no Rust difference, so no code change. Earlier
memory notes calling this a "Rust deferred soak issue" were wrong for the
2026-11-18 run (the old Rust-specific cause was fixed in a143c88bc / 1f59dcd34,
see memory/frontend-soakfix-20261113.md).

## Confirmation runs (branch fe-soak-query-init, head = origin/master 56b58c6c1 + this note)
- rust-ci cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/44426988643 — success
- frontend-soak default: https://github.com/auenkr/cadence-rust/actions/runs/44426979723 — success but **`RUST OVERALL: FAIL`** — sole failing criterion start call p99 rust=123ms vs go=35ms (known tail-latency issue, memory/fe-soak-fix-deferred, unrelated to query). Query criteria all PASS: failed=0 in both lanes, query "init" rust 21346 wf/30313 attempts vs go 20929/28253; query p99 rust 33ms vs go 47ms.
- frontend-mixed-gate default: https://github.com/auenkr/cadence-rust/actions/runs/44426984365 — success; `GATE SUMMARY failed: []`
- frontend-soak rerun: https://github.com/auenkr/cadence-rust/actions/runs/44439992947 — in progress at pause (unverified); result not yet recorded

## Status at pause (2026-11-19)
- Done: cause found ("init" FAIL was Go-baseline lane only, Rust lane 0 failures); parity, no code change. rust-ci and frontend-mixed-gate green.
- Open: soak RUST OVERALL is FAIL only on start call p99 (tail latency, deferred issue, not the query path).
- Next: read result of rerun 44439992947. If start p99 passes → done. If it fails again → note it as the known tail-latency issue (memory/fe-soak-fix-deferred) and hand it back; it's outside this task's query scope.
