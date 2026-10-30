# PostgreSQL range observer shard filter

Based on consolidated process-evidence candidate `c22fdcf0a5a913ab67473e96cb63e1d15cd1d320`.

The range observer now includes `shard_id = 0` alongside domain, name and task type. Previously MAX could select a duplicate task-list row on a different shard. Go's PostgreSQL task-list lookup includes the shard key (`go/common/persistence/sql/sqlplugin/postgres/task.go`). The baseline PostgreSQL configs omit NumShards; Go normalizes it to one (`go/common/config/persistence.go`), so the task-list shard hash selects zero. This observer is scoped to that singleton gate setup.

Retains reviewed recovery assertions, attempt-time channel diagnostics, shutdown tracing, bounded process capture after 20 seconds or at final readiness failure, and CI preflight checks. No runtime behavior change. No local Cargo, Go, Node, formatting, build, test or runtime commands were run. Only source inspection and git diff whitespace checks; candidate remains unvalidated. Main CI owner retains run 41100032910; do not duplicate dispatch until its evidence is reconciled. Matching remains incomplete.
