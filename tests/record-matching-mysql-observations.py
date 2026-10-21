#!/usr/bin/env python3
"""Record the raw MySQL observations of a Matching production gate run.

The gate reads Go's `tasks` and `task_lists` tables. This helper records the
exact statements and their raw output, so a query defect can be separated
from a persistence defect of the service.

    python3 tests/record-matching-mysql-observations.py <output.json>
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

CONTAINER = os.environ.get('CADENCE_MYSQL_DOCKER_CONTAINER', 'cadence-mysql-test')
PASSWORD = os.environ.get('CADENCE_MYSQL_PASSWORD', 'cadence')
DATABASE = os.environ.get('CADENCE_MYSQL_DATABASE', 'cadence')
TASK_LIST_SHARD_ID = 0
DOMAIN_SHARD_ID = 54321


def query(statement):
    """Run one statement and return its raw output."""
    command = ['docker', 'exec', CONTAINER, 'mysql', '--batch', '--skip-column-names',
               '-uroot', f'-p{PASSWORD}', DATABASE, '-e', statement]
    result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    return {
        'statement': statement,
        'exitCode': result.returncode,
        'stdout': result.stdout,
        'stderr': result.stderr.replace(f'-p{PASSWORD}', '-p***'),
    }


def main():
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    domains = query("SELECT name, HEX(id) FROM domains WHERE name LIKE 'service-compat-%' ORDER BY name;")
    observations = {
        'capturedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'container': CONTAINER,
        'database': DATABASE,
        'domainShardId': DOMAIN_SHARD_ID,
        'taskListShardId': TASK_LIST_SHARD_ID,
        'statements': [],
    }
    for line in domains['stdout'].splitlines():
        if not line.strip():
            continue
        name, domain_hex = line.split('\t')
        observations['statements'].append(domains)
        for statement in (
            f"SELECT shard_id, COUNT(*) FROM task_lists GROUP BY shard_id;",
            f"SELECT task_list_name, task_type, COUNT(*), MAX(task_id) FROM tasks WHERE domain_id=UNHEX('{domain_hex.lower()}') GROUP BY task_list_name, task_type ORDER BY task_list_name, task_type;",
            f"SELECT name, task_type, range_id, HEX(data), data_encoding FROM task_lists WHERE shard_id={TASK_LIST_SHARD_ID} AND domain_id=UNHEX('{domain_hex.lower()}') ORDER BY name, task_type;",
            f"SELECT name, task_type, range_id FROM task_lists WHERE shard_id={DOMAIN_SHARD_ID} AND domain_id=UNHEX('{domain_hex.lower()}') ORDER BY name, task_type;",
            f"SELECT name, HEX(id), is_global, data_encoding, LENGTH(data) FROM domains WHERE name='{name}';",
        ):
            observations['statements'].append(query(statement))
        break
    observations['statements'].append(query('SELECT notification_version FROM domain_metadata;'))
    text = json.dumps(observations, indent=2) + '\n'
    if output:
        output.write_text(text)
        print(f'wrote {output}')
    else:
        print(text)


if __name__ == '__main__':
    main()
