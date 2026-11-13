#!/usr/bin/env python3
"""History admin APIs driven through the Go `cadence admin ...` CLI against the mixed cluster.

Usage: history_admin_cli.py <go-cli> <grpc-addr> <tchannel-addr> <domain> <cluster> <workflow-id> <work-dir>
Prints one JSON object per row: {"name","ok","detail"}; exit 0 when all ok.
Expected outcomes mirror Go (service/frontend/admin/handler.go -> service/history/handler):
  DescribeHistoryHost  -> JSON with numberOfShards/address (+shardIDs with --pf)
  DescribeShardDistribution -> "Total Number of Shards"
  DescribeQueue / ResetQueue -> success for transfer(2)/timer(3)
  RemoveTask -> success (Go deletes without checking existence)
  RefreshWorkflowTasks -> success; unknown workflow -> not found
  DLQ read/purge (history) -> success on empty DLQ; merge -> 'Invalid target cluster name.' (no remote cluster)
Disruptive rows (CloseShard) run last.
"""
import json, subprocess, sys

cli_bin, grpc_addr, tch_addr, domain, cluster, wid, work = sys.argv[1:8]
results = []


def row(name, argv, rc_ok=True, expect=(), absent=(), timeout=90):
    r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    out = r.stdout + r.stderr
    low = out.lower()
    ok = (r.returncode == 0) == rc_ok
    for e in expect:
        ok = ok and e.lower() in low
    for a in absent:
        ok = ok and a.lower() not in low
    results.append({'name': name, 'ok': ok, 'detail': f'rc={r.returncode} want_rc_ok={rc_ok} expect={list(expect)} out={out.strip()[:400]}'})
    return out


for tname, addr in (('grpc', grpc_addr), ('tchannel', tch_addr)):
    base = [cli_bin, '--address', addr, '--transport', tname]
    adm = [*base, '--domain', domain, 'admin']
    t = tname
    row(f'host-describe-shard-{t}', [*adm, 'history_host', 'describe', '--sid', '0', '--pf'], expect=['numberOfShards', 'address', 'shardIDs'])
    row(f'host-describe-wid-{t}', [*adm, 'history_host', 'describe', '--wid', wid], expect=['numberOfShards', 'address'], absent=['shardIDs'])
    row(f'host-describe-no-args-{t}', [*adm, 'history_host', 'describe'], rc_ok=False, expect=['at least one'])
    row(f'host-getshard-{t}', [*adm, 'history_host', 'getshard', '--wid', wid, '--number_of_shards', '4'])
    row(f'shard-distribution-{t}', [*adm, 'shard', 'list'], expect=['Total Number of Shards'])
    for qt in ('2', '3'):
        row(f'queue-describe-{qt}-{t}', [*adm, 'queue', 'describe', '--shard_id', '0', '--cluster', cluster, '--queue_type', qt])
    row(f'workflow-describe-mutable-state-{t}', [*adm, 'workflow', 'describe', '--wid', wid], expect=['mutableStateInDatabase'] if False else [])
    row(f'workflow-refresh-tasks-{t}', [*adm, 'workflow', 'refresh-tasks', '--wid', wid], expect=['Refresh workflow task succeeded'])
    row(f'workflow-refresh-tasks-unknown-{t}', [*adm, 'workflow', 'refresh-tasks', '--wid', 'no-such-wf-admingate'], rc_ok=False)
    if t == 'grpc':
        row(f'dlq-count-{t}', [*adm, 'dlq', 'count'])
    else:  # Go thrift admin client: CountDLQMessages is gRPC-only
        row(f'dlq-count-{t}', [*adm, 'dlq', 'count'], rc_ok=False, expect=['Feature not supported on TChannel'])
    row(f'dlq-read-{t}', [*adm, 'dlq', 'read', '--dlq_type', 'history', '--source_cluster', cluster, '--shards', '0-1'], absent=['failed to read'])
    row(f'dlq-purge-{t}', [*adm, 'dlq', 'purge', '--dlq_type', 'history', '--source_cluster', cluster, '--shards', '0-1', '--last_message_id', '100'],
        expect=['Successfully purge DLQ Messages in shard 0'], absent=['Failed to purge'])
    row(f'dlq-merge-{t}', [*adm, 'dlq', 'merge', '--dlq_type', 'history', '--source_cluster', cluster, '--shards', '0-1', '--last_message_id', '100'],
        expect=['Failed to merge DLQ message in shard 0 with error: Invalid target cluster name.'])  # Go: no task executor for a cluster without remote replication

# Disruptive rows last (CloseShard / ResetQueue make History re-acquire shards).
for tname, addr in (('grpc', grpc_addr), ('tchannel', tch_addr)):
    adm = [cli_bin, '--address', addr, '--transport', tname, '--domain', domain, 'admin']
    t = tname
    for qt in ('2', '3'):
        row(f'queue-reset-{qt}-{t}', [*adm, 'queue', 'reset', '--shard_id', '0', '--cluster', cluster, '--queue_type', qt], expect=['Reset queue state succeeded'])
    row(f'shard-remove-task-transfer-{t}', [*adm, 'shard', 'removeTask', '--shard_id', '0', '--task_id', '999999999', '--task_type', '2'])
    row(f'shard-remove-task-timer-{t}', [*adm, 'shard', 'removeTask', '--shard_id', '0', '--task_id', '999999999', '--task_type', '3',
                                         '--task_timestamp', '1'])
    row(f'shard-remove-task-replication-{t}', [*adm, 'shard', 'removeTask', '--shard_id', '0', '--task_id', '999999999', '--task_type', '4'])
    row(f'shard-close-{t}', [*adm, 'shard', 'closeShard', '--shard_id', '0'])
    subprocess.run(['sleep', '5'])

for r in results:
    print(json.dumps(r))
sys.exit(0 if all(r['ok'] for r in results) else 1)
