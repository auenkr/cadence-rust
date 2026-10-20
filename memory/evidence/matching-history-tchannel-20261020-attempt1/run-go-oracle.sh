#!/usr/bin/env bash
set +e
cd /home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/go
GOTOOLCHAIN=go1.25.14 CADENCE_THRIFT_WIRE_FIXTURE_OUT=/home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/memory/evidence/matching-history-tchannel-20261020-attempt1/history-go-oracle.observed.json go test ./common/types/mapper/thrift -run '^TestGenerateHistoryTChannelWireOracle$' -count=1 > /home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/memory/evidence/matching-history-tchannel-20261020-attempt1/go-oracle.raw.log 2>&1
result=$?
printf '%s\n' "$result" > /home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b/memory/evidence/matching-history-tchannel-20261020-attempt1/go-oracle.exit
exit 0
