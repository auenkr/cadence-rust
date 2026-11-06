# history-gap-apitypes evidence (2026-11-06)
Branch history-gap-apitypes. Added Semaphore*/RequestCancelActivityTaskFailed attrs + HistoryEvent fields, started/continued-as-new fields (Thrift/proto converters), history_builder adders, state_builder semaphore replication (SemaphoreState on MutableStateBuilder).
Code head before this note: e5880ca5d5f4ad1385cf450cebf1352e0b68b031. CI (rust-ci.yml, package input, all conclusion=success):
- cadence-api https://github.com/auenkr/cadence-rust/actions/runs/42226344444
- cadence-history .../runs/42234515077
- cadence-proto .../runs/42235304899
- cadence-thrift .../runs/42235885877
- cadence-persistence .../runs/42224415917 (run on earlier head 2 commits back; proto-only fixes since, persistence doesn't depend on proto)
Remaining gaps: ParentWorkflowDomainID and jitter are Go-only (no Thrift field; Thrift decode yields None); state_builder still uses parent_workflow_domain not parent_workflow_domain_id; no new round-trip unit tests; ASCP/jitter on persistence continue-as-new event set to None (decision attrs lack them).
