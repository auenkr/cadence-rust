# Matching LB Go-parity gaps (2026-11-19)

Branch `matching-lb-parity`, validated head `6ae56e3c07b40460a217909653dc2958ddb73ee3`. CI only, no local Cargo.

## Changes (vs go/client/matching)
1. History LB dynamic config filters by domain + task list + **task type** (Go `...FilteredByTaskListInfo`): partition counts, cache flag, strategy. `Filters.task_type` added and honoured by file / config-store / persistent / OpenFeature clients (`taskType` filter).
2. Frontend `system.tasklistLoadBalancerStrategy` read via new `dynamic_config::task_list_string` (domain+tasklist+tasktype), was domain-only.
3. History add-activity/add-decision: on `ReadOnlyPartitionError` detail, `provider().invalidate(...)` (Go client.go:79/116; Go does not do this for polls).
4. Matching forwarder audit vs forwarder.go: no gaps (token generations, outstanding polls/tasks, 5s dynamic rate, ServiceBusy->SlowDown, parent fields; MaxChildrenPerNode clamped >=1 so degree-0 NoParent path is unreachable, same as Go TLM).

Unit tests: dynconfig task_type constraint, HistoryLbConfig filters, task_list_string, read-only detail detection.

## Evidence (head 6ae56e3c)
| Run | Result |
|---|---|
| rust-ci cadence-history [44387516096](https://github.com/auenkr/cadence-rust/actions/runs/44387516096) | success |
| rust-ci cadence-frontend [44387523102](https://github.com/auenkr/cadence-rust/actions/runs/44387523102) | success |
| rust-ci cadence-persistence [44387529440](https://github.com/auenkr/cadence-rust/actions/runs/44387529440) | success |
| matching-mixed-gate MySQL restart+recovery [44389828042](https://github.com/auenkr/cadence-rust/actions/runs/44389828042) | success (no summary line, as on master) |
| matching-cassandra-mixed-gate [44389835107](https://github.com/auenkr/cadence-rust/actions/runs/44389835107) | success |
| matching-production-validation postgres [44389841199](https://github.com/auenkr/cadence-rust/actions/runs/44389841199) | success |
| history-mixed-gate mysql grpc two_history [44389846411](https://github.com/auenkr/cadence-rust/actions/runs/44389846411) | success, GATE SUMMARY failed: [] |
| history-mixed-gate postgres tchannel two_history [44389853789](https://github.com/auenkr/cadence-rust/actions/runs/44389853789) | success, GATE SUMMARY failed: [] |
| frontend-mixed-gate mysql two_cluster rust_frontend_active [44389859744](https://github.com/auenkr/cadence-rust/actions/runs/44389859744) | success, GATE SUMMARY failed: [] |

Earlier head 755483f0c: same 6 gates green; rust-ci failed on rustfmt only (fixed in 6ae56e3c).
