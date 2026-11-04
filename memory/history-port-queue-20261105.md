# History queue port (go/service/history/queue -> rust/crates/cadence-history/src/queue)

Branch: history-port-queue. Last green CI: https://github.com/auenkr/cadence-rust/actions/runs/41897617384 (head dc363b91afa2e44cb084a932dba0ae52c8ee8ad7, clippy+tests, package cadence-history)

| Go file | Rust | Status |
|---|---|---|
| constants.go | constants.rs | ported |
| action.go | action.rs | ported |
| domain_filter.go | domain_filter.rs | ported |
| processing_queue_state.go | processing_queue_state.rs (interface collapsed to struct) | ported |
| processing_queue.go | processing_queue.rs (interface -> struct; pointer identity -> id) | ported |
| processing_queue_collection.go | processing_queue_collection.rs (activeQueue tracked by id) | ported |
| split_policy.go | split_policy.rs | ported |
| interface.go | interface.rs (Processor trait) | ported |
| processor_options.go | processor_options.rs + processor_options_factory.rs + config.rs | ported |
| queue_processor_util.go | util.rs (coverage to re-check) | ported |
| processor_base.go | processor_base.rs | ported |
| task_allocator.go | task_allocator.rs | ported |
| transfer_queue_validator.go | transfer_queue_validator.rs | ported |
| transfer_queue_processor_base.go | transfer_queue_processor_base.rs | ported |
| transfer_queue_processor.go | transfer_queue_processor.rs | ported |
| timer_queue_processor_base.go | todo | todo |
| timer_queue_processor.go | todo | todo |
| timer_queue_{active,standby,failover}_processor.go | todo | todo |
| factory.go | todo | todo |
| factory_mock.go, interface_mock.go | generated mocks, not ported | n/a |

Stand-ins live in queue/deps.rs (TODO(link) markers).
