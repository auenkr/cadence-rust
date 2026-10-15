#!/usr/bin/env bash
set -euo pipefail
cadence_cli=${CADENCE_CLI_BIN:-/home/ubuntu/.local/bin/cadence}
cli_domain="rust-cli-$(date +%s)-$RANDOM"
cli_grpc=${CADENCE_GRPC_PEER:-127.0.0.1:7833}
cli_tchannel=${CADENCE_TCHANNEL_PEER:-127.0.0.1:7933}
for endpoint in "$cli_grpc" "$cli_tchannel"; do
  peer_host=${endpoint%:*}
  peer_port=${endpoint##*:}
  ready=false
  for ((attempt=0; attempt<30; attempt++)); do
    if timeout 1 bash -c 'exec 3<>"/dev/tcp/$1/$2"' _ "$peer_host" "$peer_port" 2>/dev/null; then ready=true; break; fi
    sleep 1
  done
  [[ "$ready" == true ]] || { echo "Rust endpoint not ready: $endpoint"; exit 1; }
done
cli_call() { "$cadence_cli" --address "$cli_grpc" --transport grpc --domain "$cli_domain" "$@"; }
# The upstream CLI can exit zero on errors, so assert its actual output.
registration=$(cli_call domain register --global_domain false --retention 3)
[[ "$registration" == *"successfully registered"* ]] || { echo "$registration"; exit 1; }
description=$("$cadence_cli" --address "$cli_tchannel" --transport tchannel --domain "$cli_domain" domain describe)
[[ "$description" == *"Name: $cli_domain"* ]] || { echo "$description"; exit 1; }
start=$(cli_call workflow start --workflow_id retry-timeout --workflow_type CliRetry --tasklist cli-check --execution_timeout 1 --decision_timeout 1 --retry_attempts 2 --retry_interval 1 --retry_backoff 2)
original_run=$(sed -n 's/.*run Id: \([a-f0-9-]*\).*/\1/p' <<< "$start")
[[ -n "$original_run" ]] || { echo "$start"; exit 1; }
for ((attempt=0; attempt<30; attempt++)); do
  history=$(cli_call workflow show --workflow_id retry-timeout --run_id "$original_run")
  successor=$(sed -n 's/.*NewExecutionRunID:\([a-f0-9-]*\).*/\1/p' <<< "$history")
  [[ -n "$successor" ]] && break
  sleep 1
done
[[ -n "$successor" && "$history" == *"Initiator:RetryPolicy"* ]] || { echo "$history"; exit 1; }
for ((attempt=0; attempt<30; attempt++)); do
  next_history=$("$cadence_cli" --address "$cli_tchannel" --transport tchannel --domain "$cli_domain" workflow show --workflow_id retry-timeout --run_id "$successor")
  [[ "$next_history" == *"WorkflowExecutionTimedOut"* ]] && break
  sleep 1
done
[[ "$next_history" == *"WorkflowExecutionTimedOut"* && "$next_history" == *"ContinuedExecutionRunID:$original_run"* && "$next_history" == *"FirstExecutionRunID:$original_run"* && "$next_history" == *"Attempt:1"* ]] || { echo "$next_history"; exit 1; }
visibility=$(cli_call workflow list --query 'WorkflowID = "retry-timeout"')
[[ "$visibility" == *"CONTINUED_AS_NEW"* && "$visibility" == *"TIMED_OUT"* ]] || { echo "$visibility"; exit 1; }
printf 'CLI compatibility passed: domain=%s original=%s successor=%s\n' "$cli_domain" "$original_run" "$successor"
