# History queue port (go/service/history/queue -> rust/crates/cadence-history/src/queue)

Branch: history-port-queue. Last green CI: (none yet)

| Go file | Rust | Status |
|---|---|---|
| constants.go | constants.rs | ported |
| action.go | action.rs | ported |
| domain_filter.go | domain_filter.rs | ported |
| processing_queue_state.go | processing_queue_state.rs (interface collapsed to struct) | ported |
| processing_queue.go | processing_queue.rs (interface -> struct; pointer identity -> id) | ported |
| processing_queue_collection.go | processing_queue_collection.rs (activeQueue tracked by id) | ported |
| split_policy.go | split_policy.rs | ported |
| interface.go | split across modules; Processor trait todo | todo |
| processor_options.go | todo | todo |
| queue_processor_util.go | todo | todo |
| processor_base.go | todo | todo |
| task_allocator.go | todo | todo |
| transfer_queue_validator.go | todo | todo |
| transfer_queue_processor_base.go | todo | todo |
| transfer_queue_processor.go | todo | todo |
| timer_queue_processor_base.go | todo | todo |
| timer_queue_processor.go | todo | todo |
| timer_queue_{active,standby,failover}_processor.go | todo | todo |
| factory.go | todo | todo |
| factory_mock.go, interface_mock.go | generated mocks, not ported | n/a |

Stand-ins live in queue/deps.rs (TODO(link) markers).
