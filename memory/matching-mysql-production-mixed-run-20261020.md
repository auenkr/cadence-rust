# Matching MySQL production mixed run (2026-10-20)

Frozen binary: /home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction/artifacts/frozen-production/cadence-server-task-store-abstraction
SHA256: 35e25467dfb6961d41d30565260c6e4afd9ed80b54857a403445cac57131817d
Source HEAD: df3466479

Command:
NODE_PATH=/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules CADENCE_MYSQL_DOCKER_CONTAINER=cadence-mysql-test python3 tests/run-go-service-baseline.py --rust-matching --matching-restart --rust-production-binary <frozen> --binary /tmp/cadence-go-baseline-1r0mq_xe/cadence-server --mysql --history-transport grpc --artifacts-dir <disk>

Result: EXIT=0, results.json result=preliminary-passed, acceptanceEligible=false.
Covered: SDK workflow/query/isolation/native partition, SIGKILL restart with 1 durable row and 1 completion, graceful drain recovery 1 queued row, process restart same Go pid.
GC verification: not covered by this harness results; GC gap remains unverified.
