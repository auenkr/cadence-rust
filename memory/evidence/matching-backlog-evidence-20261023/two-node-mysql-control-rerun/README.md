Control: two-node MySQL lane WITHOUT --matching-backlog-forward (`--matching-failure-recovery` only),
harness timeout fixed to 900s (commit f35d83cb2). gate.exit = 1. probe.log line 1:
"paced activity dispatch was not rate limited: [83,1387,473]" (tests/matching-service-compat.cjs:1147).
This step runs before the backlog scenario-free GC section and does not touch the backlog code.
So the two-node lane has a pre-existing failure on this branch that is independent of the new scenario.
