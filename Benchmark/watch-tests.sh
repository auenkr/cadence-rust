#!/usr/bin/env bash
cd /home/ubuntu/.t3/worktrees/cadence-rust/t3-de576e9f-729bfd59a9c94d4b || exit 1
nice -n 10 ionice -c3 python3 Benchmark/watch-tests.py > Benchmark/evidence/watch-tests.log 2>&1
status=$?
printf '%s\n' "$status" > Benchmark/evidence/watch-tests.exit
exit "$status"
