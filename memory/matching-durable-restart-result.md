# Matching durableRestart probe failure (run 40189574487, mysql-probes)

Failure: poll of partition /2 after SIGKILL restart returned durable-0 (root backlog) instead of durable-2.

Root cause: scenario race, not a Rust persistence bug. The restart-load path keys backlog by partition name correctly (the first child poll returned its own task). After restart a child task list is cold; its reader loads asynchronously. A poll with no local task waits only the local poll wait (default 10ms, Go LocalPollWaitTime parity) then forwards to the parent, and the root, which still held durable-0, answered. Go has the same behaviour. The scenario polled root last, so it was timing-dependent.

Fix: tests/matching-probe-scenarios.cjs polls root first (draining its backlog), then children, still asserting each child returns its own task. No Rust change.

Validation: see commit history / CI run notes appended below.

Validation: mysql-probes lane run https://github.com/auenkr/cadence-rust/actions/runs/40192762057 head ead9d304db760028999d329f4406f0c2dc575383 conclusion success.
