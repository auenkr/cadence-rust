# V7 PostgreSQL mixed gate — reviewed pass

Run: https://github.com/auenkr/cadence-rust/actions/runs/40541390213

Head and artifact commit: f5963951befb12dc64b72bb366c44a464939b87a.
Rust Matching SHA256: 3607288508d933f28eace17ec92f92e55a7435007d0419409f4560441b1294f6.
Build, gate, lane and runtime exits are zero. The command and results identify
this fresh binary, Go Frontend/History/Worker and two Rust Matching hosts,
with no Go Matching. Transport is gRPC and discovery is original Go Ringpop.

Reviewed actual probe data and SDK logs:
- SDK log reports echo, activity retry, child, workflow retry, workflow-task
  retry, activity timeout and signal passed. Injected panic/cancellation log
  lines belong to those successful retry/cancellation scenarios.
- PostgreSQL GC reaches ack1 and zero decision rows while its pending control
  retains one row. Domain metadata is Go-compatible thriftrw (493 bytes).
- Standby query answers directly through the child and six times via Frontend.
- Partition listing returns three ordered keys, agrees across both Rust hosts
  and Go Frontend, and each direct Describe is served by its reported owner.
- Durable restart starts with one activity in each of three partitions and
  drains all three after restart (PID9211 to PID9437).
- Ordinary restart retains a durable task and completes once; graceful drain
  reduces one queued row to zero and completes once via a replacement host.
- Crash recovery kills PID9007; survivor8995 completes once, advances range1
  to2 and ack0 to1, and leaves zero queued rows. Idle recovery completes once.
- Final SIGTERM exits0. Cleanup output names the uniquely owned PostgreSQL
  container removed by the successful docker-rm operation.

The probe's legacy preliminary-passed/acceptanceEligible:false labels do not
establish or reject whole-service acceptance. This credits only the reviewed
PostgreSQL lane; all other exact-pin requirements remain in the ledger.
collection.json is an inventory, supplemented by this manual result review.
