# history-gap-timerq2 checklist (2026-11-05)

Branch: history-gap-timerq2. Head SHA: d65c4c21bfbf3d160bc5580b20c4546a52c08a15
CI run (unverified at time of writing): https://github.com/auenkr/cadence-rust/actions/runs/41928309437

## Done (pushed, CI pending)
- [x] CI clippy doc fix in timer_queue_processor_base.rs
- [x] queue/timer_queue_active_processor.rs
- [x] queue/timer_queue_standby_processor.rs
- [x] queue/timer_queue_failover_processor.rs
- [x] queue/timer_queue_processor.rs
- [x] queue/factory.rs
- [x] common.rs (common/errors.go, type.go, lookup.go)
- [x] task/rescheduler.rs audited: all Go functions present

## Remaining
- [ ] Audit queue/constants.rs, interface.rs, processing_queue_state.rs vs Go
- [ ] task/ executors (timer/transfer active+standby, bases), task_util, constants
- [ ] Replace TODO(link)/stub/unimplemented! in queue/deps.rs, task/deps.rs, task/deps_exec.rs, queue/task_allocator.rs (needs execution/shard/cache ports)
