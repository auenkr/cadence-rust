# History queue factory (2026-11-07, branch history-gap-queuefactory)

Done: `wiring/assembly/queue_factory.rs` adds `ExecQueueFactory` (impl `QueueFactory`) over a `QueueExecProvider`; launcher now uses it instead of `UnwiredQueueFactory`. When a provider returns a `QueueExec`, it builds real transfer/timer processors via existing `queues::build_queue_processors` / `queue_exec` builders.

NOT done (blocker): the provider used at startup (`MissingExecutorsProvider`) returns None, so shards still run with no queue processors (logged at error). Real executors need implementations of the dependency traits in task/: TransferDependencies, TransferActiveDependencies, TransferStandbyDependencies, CleanupDependencies, TimerActiveDependencies, TimerStandbyDependencies (plus ActiveMutableState/ActiveTransferContext adapters over real execution::MutableState/Context, visibility manager, archiver, history/matching/parent-close clients), and a QueueShardContext/task ShardContext/TaskAllocator per shard. None exist; this is several thousand lines and needs its own units.

CI: https://github.com/auenkr/cadence-rust/actions/runs/42337455828 head d7e30d44877769895dd3d5969d22328cfc00cd55 conclusion success (clippy+tests, cadence-history).
