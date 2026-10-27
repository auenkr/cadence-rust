# Repeated adaptive diagnostic failure

Run https://github.com/auenkr/cadence-rust/actions/runs/40474255482 failed on
source `5808e205a1b58b2169084b2e46eaea219f8b7cf2`.
Rust binary SHA256:
`468f9306660ffd1d1693be8d43995187e52fabbfd682059a488ddc35cc05ae9c`.
Build succeeded; probe and gate failed. The collector returned1, preserving
the failed result, topology, SDK logs, recovery checkpoints and DB cleanup.
`serviceAcceptance:false` remains explicit.

The full SDK reached activity-retry with38.482s of its60s suite budget left.
Activity-retry, child and workflow-run-retry completed. Workflow-task-retry
completed in19.894s, leaving14.823s for activity-timeout. That workflow failed
after14.778s; the independently fetched history contains only execution start
and the initial scheduled decision. It does not establish whether that
decision Add reached Matching, persisted, forwarded, or reached a poller.
The final24-terminal-history and explicit routing phases were not reached.

The successful original-Go control40473213335 reached activity-retry with59.814s
remaining and completed workflow-task-retry in10.367s and activity-timeout in
1.254s. These are timing observations, not proof of a source defect: that
control used one Go Matching instance, while this run used two Rust instances.
Different scaler trajectories, queue ownership and delivery paths remain
confounders. Do not extend deadlines or infer a retry scheduling delay.

The next comparison40476007015 uses one Rust Matching instance at a time,
retains adaptive SDK/load/routing assertions and restart, and omits two-node
failure recovery. It is diagnostic, not a substitute for the required
two-node adaptive acceptance lane. Delivery instrumentation is separately
being extended before a further traced two-node run.
