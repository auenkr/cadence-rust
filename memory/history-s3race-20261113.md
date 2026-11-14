# History S3 gate: retention-0 visibility-archive race (2026-11-13)

Branch `history-s3race` (no code change). Head SHA = commit containing this file.

## Finding: Go has the same race; Rust matches Go. No code change, 2-of-3 visibility check stays.
Go ordering (all under go/service/history/):
- execution/mutable_state_task_generator.go:161-172 GenerateWorkflowCloseTasks adds the CloseExecution transfer task.
- same file :197-209 adds the DeleteHistoryEvent timer with VisibilityTimestamp = closeTimestamp + retentionDays*24h (+jitter) (:185-205).
  Retention 0 => timer due immediately. The timer queue and transfer queue are independent; nothing orders the timer after the transfer task.
- task/transfer_active_task_executor.go:397-402 (processCloseExecutionTaskHelper): `mutableState == nil || running` => `return nil`.
  recordWorkflowClosed (:466+) is the ONLY place that archives visibility (transfer_task_executor_base.go ~:405-470, archiverClient.Archive with VisibilityURI).
- task/timer_task_executor_base.go archiveWorkflow (:185+) archives HISTORY only (Targets: ArchiveTargetHistory) and deletes the workflow execution (deleteWorkflowExecution, :~270).
  If the timer wins, the close task finds nil mutable state and visibility is never archived.
Rust: rust/crates/cadence-history/src/execution/mutable_state_task_generator.rs:79-89 is a line-for-line port (same timestamps/tasks); the transfer and timer executors follow the same Go checks.
Therefore not a Rust divergence; strict 3-of-3 is not valid for retention 0 against Go semantics.

## Evidence
Source citation only (rule: cite exact lines). No CI runs needed since no code changed; a Go-History lane was not added.
Prior observed flake: run 43456601022 (see memory/history-s3gate-20261113.md).
