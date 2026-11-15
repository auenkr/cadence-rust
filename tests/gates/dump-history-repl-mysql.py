#!/usr/bin/env python3
"""Write a bounded, failure-only MySQL snapshot for workflows in failure-workflows.json."""
import json
import subprocess
import sys
from pathlib import Path

container, run_dir, out_path = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
failures_file = run_dir / 'failure-workflows.json'
if not failures_file.exists():
    raise SystemExit(0)
failures = json.loads(failures_file.read_text())
out = out_path

def sql(db, query):
    result = subprocess.run([
        'docker', 'exec', container, 'mysql', '-uroot', '-pcadence', '--batch', '--raw', '--skip-column-names',
        db, '-e', query,
    ], capture_output=True, text=True, timeout=45)
    return result.stdout + (('\nERROR: ' + result.stderr) if result.returncode else '')

def q(value):
    return "'" + value.replace("'", "''") + "'"

with out.open('w') as f:
    for cluster in ('A', 'B'):
        db = f'cadence_{cluster.lower()}'
        print(f'### cluster {cluster} database {db}', file=f)
        for failure in failures:
            wid, rid = q(failure['workflow_id']), q(failure['run_id'])
            print(f"## workflow {failure['workflow_id']} run {failure['run_id']} step {failure['step']}", file=f)
            rows = sql(db, f"SELECT shard_id, HEX(domain_id), workflow_id, HEX(run_id), next_event_id, last_write_version, data_encoding, OCTET_LENGTH(data), HEX(LEFT(data,4096)) FROM executions WHERE workflow_id={wid} AND run_id=UNHEX(REPLACE({rid},'-',''))")
            print('[executions shard/domain/workflow/run/next_event/write_version/encoding/data_len/data_prefix_hex]\n' + rows, file=f)
            cur = sql(db, f"SELECT shard_id, HEX(domain_id), workflow_id, HEX(run_id), state, close_status, last_write_version FROM current_executions WHERE workflow_id={wid}")
            print('[current_executions]\n' + cur, file=f)
            shard_rows = sql(db, f"SELECT DISTINCT shard_id FROM executions WHERE workflow_id={wid}")
            shards = [x.strip() for x in shard_rows.splitlines() if x.strip().isdigit()]
            if shards:
                shard_list = ','.join(shards)
                for table, query in (
                    ('history_node', f"SELECT shard_id, HEX(tree_id), HEX(branch_id), node_id, txn_id, data_encoding, OCTET_LENGTH(data), HEX(LEFT(data,4096)) FROM history_node WHERE shard_id IN ({shard_list}) ORDER BY shard_id,tree_id,branch_id,node_id,txn_id"),
                    ('history_tree', f"SELECT shard_id, HEX(tree_id), HEX(branch_id), data_encoding, OCTET_LENGTH(data), HEX(LEFT(data,4096)) FROM history_tree WHERE shard_id IN ({shard_list}) ORDER BY shard_id,tree_id,branch_id"),
                ):
                    print(f'[{table} shard(s) {shard_list}; blob prefix capped at 4096 bytes]\n' + sql(db, query), file=f)
        for table, query in (
            ('replication_tasks', 'SELECT shard_id, task_id, data_encoding, OCTET_LENGTH(data), HEX(LEFT(data,4096)) FROM replication_tasks ORDER BY shard_id,task_id'),
            ('replication_tasks_dlq', 'SELECT source_cluster_name, shard_id, task_id, data_encoding, OCTET_LENGTH(data), HEX(LEFT(data,4096)) FROM replication_tasks_dlq ORDER BY source_cluster_name,shard_id,task_id'),
        ):
            print(f'[{table}; all rows; blob prefix capped at 4096 bytes]\n' + sql(db, query), file=f)
print(f'wrote {out}', flush=True)
