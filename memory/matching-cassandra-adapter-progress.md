# Matching Cassandra adapter progress (unvalidated)

Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/matching-semaphore-durable`

Source commits `a9e406a8b` and `d1bbb2be9` add a shared `matching_blocking::run` boundary and route synchronous Matching persistence work in the task reader, task writer, dispatcher, queue registry, maintenance/ack paths, and partition updates through it. The added source regression uses a blocking fake operation to check Tokio timer responsiveness and that the operation completes before its caller advances to shutdown. The registry's owner startup, owner finish, lease, partition-cache load, ack persistence, and GC paths are inside `owner_work`, which runs on Tokio's blocking pool; `queue.status` and partition cache reads not in that worker are in-memory only.

The Cassandra task adapter is still missing. Authoritative pinned Go sources inspected:

- `go/common/persistence/nosql/nosqlplugin/cassandra/tasks_cql.go`
- `go/common/persistence/nosql/nosqlplugin/cassandra/tasks.go`
- `go/common/persistence/nosql/nosql_task_store.go`
- `go/schema/cassandra/cadence/schema.cql`
- `go/common/persistence/nosql/nosqlplugin/cassandra/domain_cql.go`
- `go/common/persistence/nosql/nosqlplugin/cassandra/domain.go`

Cassandra task rows multiplex task and task-list rows in the `tasks` table, partitioned by `(domain_id, task_list_name, task_list_type)`. Row `type=0` is a task; `type=1, task_id=-12345` is the task-list lease. Go task UDT fields are `domain_id`, `workflow_id`, `run_id`, `schedule_id`, `created_time`, and `partition_config`. The task-list UDT fields are `domain_id`, `name`, `type`, `ack_level`, `kind`, `last_updated`, and `adaptive_partition_config`; the latter includes nested read/write partition maps in current schema. Go batches task inserts with a conditional range-id update, so any Rust implementation must preserve the logged conditional batch atomicity and TTL behavior.

Go domain lookup reads `domains_by_name_v2` (partition 0) for the nested `domain`, `config`, and `replication_config` UDTs and notification/version columns; it also relies on `domains` for the UUID-to-domain mapping. The domain config UDT includes retention, emit metrics, archival settings, bad binaries, isolation groups, and async workflow config blobs/encodings. Replication includes active cluster, clusters, and active-cluster config blobs.

An uncommitted `CassandraBlockingSession` source addition owns a dedicated multi-thread Tokio runtime for Scylla and schedules runtime destruction on a plain helper thread so its final drop can safely originate from a Tokio worker. Its drop/timer regression is source-only and has not been built or run. It is not yet wired into the Matching factory, and Cassandra task/domain adapters and factory selection remain incomplete. No Cargo build, lockfile update, or service run was performed; all changes need validation in the coordinated build lane.
