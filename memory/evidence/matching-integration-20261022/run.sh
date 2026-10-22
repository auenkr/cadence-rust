#!/bin/bash
# usage: run.sh name cmd...
E=/home/ubuntu/.t3/worktrees/cadence-rust/matching-integration-20261021/memory/evidence/matching-integration-20261022
cd /home/ubuntu/.t3/worktrees/cadence-rust/matching-integration-20261021/rust
n=$1; shift
"$@" > $E/$n.log 2>&1
echo $? > $E/$n.exit
