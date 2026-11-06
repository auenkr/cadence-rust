# history-gap-timerq3 (2026-11-06)
Branch history-gap-timerq3, head (code) 080f29450288fc2260eebb0361b1bc7ec9861cb9. CI https://github.com/auenkr/cadence-rust/actions/runs/42218452225 (cadence-history) = success.
- Merged timerq2 onto master. Master already had timer_gate/timer_queue_processor_base/timer_queue_processor with active/standby/failover inlined and all Go funcs ported (handleAckLevelUpdate inlined in processor_pump), so master versions kept; timerq2's duplicate separate active/standby/failover files dropped.
- Added from timerq2: queue/factory.rs, common.rs.
- Remaining gaps: queue/deps.rs, task/deps*.rs TODO(link) traits; task executors (timer active/standby/base) still partial per audit.
