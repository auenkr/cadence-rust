# Traced single-node adaptive failure

Run https://github.com/auenkr/cadence-rust/actions/runs/40476881898 failed
at source `a5276f33659c6a1cea47f157d909983c2ce0885e`.
Rust binary SHA256:
`5430dfd1287952c18e5e2d8d362a2017fc750f8f765dcb0d4a0ded0bd2c6a531`.
One Rust Matching instance at a time, original Go peers, no Go Matching;
process restart and unchanged adaptive assertions. Diagnostic only.

Initial load completed quickly. Activity-retry took20.280s; the child workflow
then exhausted38.035s. Its history contains child-start and two completed
parent decisions but no terminal child result. This is a different stalled
workflow from earlier runs, not a new retry-delay diagnosis.

The retained matching-adaptive-trace.log has570 events. Scaler samples report
write counts1->12->102->434 with root-QPS-times-write-count estimates24,
12,102,433.5 and then0. Counts alone do not identify defective scaling.
Child parent decisions reached Matching, were enqueued, and generated poll
responses through the forwarding chain. That does not prove its child
execution completed or locate every remaining delivery delay.

The current trace covers synchronous forwarding, not durable-reader dispatch
and acknowledgment. Reader instrumentation is being added separately. The
extractor preserves missing fields and distinct schedule IDs; do not join
polls to a schedule using a workflow ID alone. Raw downloaded logs remain in
/mnt/storage/service/ci-evidence/40476881898/. Preserved artifacts keep
serviceAcceptance:false and original runtime evidence limits.
