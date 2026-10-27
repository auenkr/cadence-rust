# Original-Go control provisioning failure

https://github.com/auenkr/cadence-rust/actions/runs/40472035315 failed on
fed7fd7fa41e9b444439d9439686ff4628d20368. The Go binary built and all four Go
services started; Worker readiness was observed and the services stayed alive.
The first direct Matching DescribeTaskList failed with Go's
`DefaultDomainCache encounter case where domain exists but cannot be loaded`.
No SDK adaptive scenario ran. This is not Rust Matching evidence or an adaptive
comparison. Actual topology, source, Go binary hash, exit, and cleanup remain.

The independent Go roles refresh newly registered domains separately. Existing
`tests/service-workflow-compat.cjs` already retries this exact provisioning error
for15s before testing partition configuration. The new control now uses the same
bounded error-specific provisioning wait before its unchanged shared adaptive
assertions. Other errors surface immediately; no scenario deadline was changed.
