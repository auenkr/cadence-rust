# Adaptive decision read routing probe scope (2026-10-26)

## Result

The existing `adaptiveScaler` mixed probe does not directly prove decision polling on both the root and a child read partition. It verifies adaptive config persistence (version and read/write partition counts), static `ListTaskListPartitions` parity, and completion of all 24 workflows by the existing Go SDK worker. Those results should not be described as direct root/child routing proof.

## Go behavior reviewed

- `go/service/matching/tasklist/task_list_manager.go:385-393` exposes the loaded persisted `ReadPartitions` map through `PartitionReadConfig`, indexed by the manager's partition ID. This is the actual read-partition membership check.
- `go/service/matching/tasklist/adaptive_scaler.go:176-196` computes read partitions from the updated write partition map and persists changes using `UpdateTaskListPartitionConfig`.
- `go/service/matching/handler/engine.go:1321-1344` implements public `getAllPartitions` from the configured static `NumTasklistWritePartitions`. It does not consult the persisted adaptive config. Thus ListTaskListPartitions intentionally remains a static listing and cannot be used as adaptive read-routing evidence.
- Go's forwarding path in `go/service/matching/tasklist/forwarder.go:233-246` issues a decision poll to the selected partition. A direct routing probe needs to identify which poll received which decision task, and complete that task with a valid response.

## Why this probe does not add a direct poll assertion

The 24 SDK workflows are actively polled and completed by the Go SDK worker. Injecting direct root/child polls on that same task list would race the worker for tasks; a poll can consume a decision task and the probe must then complete it with the corresponding token. The SDK completion evidence does not identify the partition a task was read from. Public static partition listing also cannot supply the adaptive child IDs. A safe direct proof therefore needs isolated, purpose-created workflows and explicit poll/complete handling per target partition, plus evidence from the returned task identity/token that the target was served. That is a separate bounded probe design, not something implied by the existing 24 SDK completions.

No code change was made to production behavior or to static listing semantics. No Cargo/Go/format/runtime validation or CI gate was run. This note records the narrow source-review outcome; it is not a parity completion claim.
