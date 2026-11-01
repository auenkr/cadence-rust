# Worker core — 2026-11-01

## Diagnostics pure logic
Ported in `diagnostics.rs` (Go-derived tests): runbook links, root-cause strings, `getIssueType`,
`capNumberOfIssues`, starter/workflow input wire names, usage-data Pinot payload (sorted keys).
Not ported: workflow bodies, activities needing frontend/messaging clients, invariants (timeout, failure, retry, timeoutrisk, antipatterns).
CI: https://github.com/auenkr/cadence-rust/actions/runs/41286336454 — success at `c34015c62`.

## Diagnostics invariants (pure logic)
`diagnostics/invariants.rs`: retry (`checkRetryPolicy`, heartbeat rule), timeoutrisk rules, failure
classification and root cause map, timeout poller/heartbeat root causes and type strings, antipattern burst
detection (sliding window, merge of overlapping clusters) and strings. Not ported: history traversal and
metadata JSON (needs HistoryEvent bindings), cron continue-as-new check, DescribeTaskList call, workflow bodies.
CI: https://github.com/auenkr/cadence-rust/actions/runs/41286623128 — success at `f8aac499a` (before burst detection; rerun below).
