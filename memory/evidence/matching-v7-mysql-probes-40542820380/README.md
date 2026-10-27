# V7 MySQL probes — reviewed pass

Run: https://github.com/auenkr/cadence-rust/actions/runs/40542820380

Head and artifact commit: f5963951befb12dc64b72bb366c44a464939b87a.
Rust binary SHA256: 192df464638f6750bfbd2f0804e52e2659431ab2ace50c20a9f5105c731f81c8.
Build, gate, lane and runtime exits are zero. Commands/results identify the
fresh binary, Go Frontend/History/Worker and two Rust Matching hosts via
original Go Ringpop, with no Go Matching. Transport is gRPC.

Actual artifact outcomes reviewed:
- SDK log reports cancellation ended Canceled and echo, activity retry, child,
  workflow retry, workflow-task retry, timeout and signal passed.
- Present tables equal Go schema plus Go setup tables; missingGoTables is empty.
- All six observed decision/activity queues have zero remaining rows, while
  the pending control retains one task. Domain uses thriftrw metadata493 bytes.
- Child-to-root and parent-to-child backlog forwarding each starts with a
  durable row and reaches a poller on the other partition.
- Standby query answers on the child and six times via Frontend. Three ordered
  partitions agree across both nodes and Frontend; direct Describe reaches
  each reported owner. Auto-config hints pass for decision and activity.
- Three durable partition tasks drain after restart (PID9412 to9876).
  Ordinary restart preserves work and completes once; graceful drain removes
  one queued row and completes once on a replacement host.
- Idle recovery completes once. Crash recovery kills9018; survivor9001
  advances range1 to2 and ack0 to1, completes once, and leaves zero task rows.
- Final SIGTERM exits0; cleanup output names the uniquely owned MySQL
  container successfully removed by docker-rm.

The legacy preliminary-passed/acceptanceEligible:false labels do not replace
manual review or whole-service acceptance. This credits only this lane.
collection.json and the retained probe, SDK and recovery records substantiate
the bounded findings; other same-pin requirements remain in the ledger.
