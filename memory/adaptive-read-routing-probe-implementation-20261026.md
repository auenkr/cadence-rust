# Adaptive decision read-routing probe implementation (2026-10-26)

Added an opt-in direct decision-poll phase to `adaptiveScaler` in
`tests/matching-probe-scenarios.cjs`. It runs after the production adaptive
config update is observed and before the existing Go SDK worker starts.

The phase reads eligible IDs from the live `DescribeTaskList.partitionConfig`
response, requires root ID 0 and at least one child, then creates a unique
routing task list. It copies the live read/write partition maps to that list,
starts one dedicated workflow on the root and one on a live child ID, and polls
each exact partition directly through Rust Matching. These tasks cannot be
confused with or consumed from the 24 existing adaptive-load workflow IDs.
Each response must contain that dedicated workflow ID and run ID. The probe
completes every returned task using Go Frontend `RespondDecisionTaskCompleted`
with `CompleteWorkflowExecution` and a payload, then requires exactly one
workflow completion event. The JSON result records target task-list name,
partition ID, workflow/run IDs and completion API.

The existing 24 load workflows and their SDK completion assertions are
unchanged. No production Rust code was changed. The routing task list is an
isolated controlled copy of the live partition config: this proves root and
eligible-child direct decision-task service and Go Frontend completion, but
does not prove which adaptive partition the production History client selects
for ordinary workflow tasks. The existing queue’s load tasks remain untouched
until its SDK worker starts.

Local checks: `node --check tests/matching-probe-scenarios.cjs` and `git diff
--check` passed. No Cargo, Go, formatter, runtime, or CI/gate command was run as
instructed. Mixed-cluster execution and served-task evidence remain pending
until the parent pins an immutable candidate SHA and authorizes a gate.
