# Matching adaptive scaler parity (Go vs Rust)

Rust: rust/crates/cadence-matching/src/matching_scaler.rs (R). Go: go/service/matching/tasklist/adaptive_scaler.go (G).

| Behavior | Go | Rust | Result |
|---|---|---|---|
| run gate: enableAdaptiveScaler && enableGetNumberOfPartitionsFromCache | G:148 | R:1-8, grpc_matching.rs | match |
| default partition config {0:{}} when nil | G:217-229 | matching_registry.rs caller | match |
| upscale threshold = n*upscaleRPS; downscale = (n-1)*upscaleRPS*factor | G:232-237 | R:109-110 (n-1 clamped at 0; same effect) | match |
| min write partitions = max(1,cfg); result = max(n,min) | G:243-244 | R:111-112 | match |
| overload/underload Sustain CheckAndReset; ceil(qps/upscale) floored by min | G:246-257,430-441 | R:90-120 | match |
| adjustWritePartitions dense 0..target, keep entries | G:260-274 | R:123-135 | match |
| adjustReadPartitions: union old read + new write; drain trailing readOnly&&empty; break on describe failure | G:276-311 | R:140-172 | DIVERGED, fixed: Go sets changed only when id absent from old read map (G:283-285); Rust also set it when groups differed. Now `!contains_key`. Test matching_scaler_read_changed_only_for_new_partition_ids |
| metrics error resets sustains+isolation, no update | G:155-161 | matching_registry.rs caller | not re-audited line by line |
| assumeEvenQPS = root NewTasksPerSecond * writeCount | G:320-329 | matching_scaler_go_tests.rs:1040 helper | match |
| empty check: EnablePartitionEmptyCheck ? Empty : BacklogCountHint==0 | G:405-410 | partition_metrics(id, empty_check) | match |
| Dyn config keys/defaults (upscaleRPS 200, downscaleFactor 0.75, sustains 60s/120s, interval 15s, enable flags false) | constants.go:4066,4896,5537,5774,5780,5810 | matching_dyncfg_defaults_tests.rs:35-62 | match |
| Isolation balancer | isolation_balancer.go | R:244-560 | not re-audited in this pass |
| UpdateTaskListPartitionConfig persistence | tasklist manager | matching_registry.rs:1887 | not re-audited in this pass |

Gate gap remains: no lane enables enableAdaptiveScaler (gate workflows intentionally not added).
