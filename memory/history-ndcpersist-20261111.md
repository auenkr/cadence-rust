# History NDC context persistence — 2026-11-11

Branch: `history-ndcpersist`
Base: `df60d4dfcabf08645b62f05eff175a949304816b` (`origin/master`)

## Done

- `EngineNdcWorkflow::{persist_start_workflow_batch_events,persist_non_start_workflow_batch_events}` now append through its lock-owning engine context and `SqlHistoryManager`.
- Appends preserve incoming event task IDs and use a shard-generated transaction ID. Start batches use new-branch history and Go's cleanup metadata; empty non-start batches return an empty persisted blob.
- `git diff --check` passed. No Cargo, rustfmt, or Go command was run locally.

## Gaps

- The Go pointer `go/service/history/ndc/workflow.go` is absent from this checkout; NDC transaction managers call `execution.Context` directly. `go/service/history/execution/context.go` is present and was used to verify append semantics.
- No concrete `WorkflowContextHooks` implementation is wired to `SqlWorkflowContextStore`, and `EngineNdcWorkflow` update/create/passive/conflict methods remain explicit errors. Therefore update/current-run CAS, CAN atomic persistence, local generated task ID allocation, post-commit queue notifications, duplicate/recovery handling, and required persistence failure tests are not complete.
- This is not service parity evidence. Do not proceed to the next history migration item on this change.

## CI

- Run URL: pending
- Code head SHA: pending
- Conclusion: pending
