# Matching late-domain name probe (2026-10-27)

Extended the existing `--matching-cold-domain-metric-probe` fixture. After
Rust Matching passes readiness (and its startup scan), the fixture registers
separate uniquely named domains and makes the first RPCs for those domain keys
directly by name: `GetTaskListsByDomain` and `ListTaskListPartitions`. It sends
no UUID RPC to warm either domain. The partition result must contain the exact
root task-list key; the domain-list response must have both expected maps.
The existing quota rejection and Prometheus metric assertions remain in place,
and the existing per-RPC 20-second SDK deadlines are unchanged.

The JSON evidence `matching-cold-domain-metric-probe.json` records each
operation, response shape, actual task-list/partition keys, the existing quota
outcomes, and an explicit limit: these domains have distinct keys but share
Matching's process cache. A background refresh can observe a registration
before its RPC, so this exercises late-domain name resolution without claiming
an isolated cache or proving that the first RPC itself encountered a cache
miss. Runtime outcomes are populated by the existing gate and were not run in
this source-only change.

Validation performed locally: Python AST parse, Node syntax check, and diff
inspection only. No Cargo, Go, formatter, runtime, or mixed gate was run.
