#!/usr/bin/env python3
"""Go seed -> Rust persistence/restart -> Go verify for Matching Cassandra."""
import datetime as dt
import os
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = Path(os.environ.get("CADENCE_CASSANDRA_ARTIFACTS", "artifacts/matching-cassandra-interop"))
ARTIFACTS = (ROOT / ARTIFACTS).resolve()
ARTIFACTS.mkdir(parents=True, exist_ok=True)

required = [
    "CADENCE_CASSANDRA_CONTACT_POINTS",
    "CADENCE_CASSANDRA_KEYSPACE",
]
missing = [name for name in required if not os.environ.get(name)]
if missing:
    sys.exit("required environment variables: " + ", ".join(missing))

os.environ.setdefault("CADENCE_CASSANDRA_TEST_DOMAIN_ID", "00000000-0000-0000-0000-000000000701")
os.environ.setdefault("CADENCE_CASSANDRA_TEST_TASK_LIST", "rust-cassandra-interop")
metadata = [
    f"timestamp_utc={dt.datetime.now(dt.timezone.utc).isoformat()}",
    f"contact_points={os.environ['CADENCE_CASSANDRA_CONTACT_POINTS']}",
    f"port={os.environ.get('CADENCE_CASSANDRA_PORT', '9042')}",
    f"keyspace={os.environ['CADENCE_CASSANDRA_KEYSPACE']}",
    f"domain_id={os.environ['CADENCE_CASSANDRA_TEST_DOMAIN_ID']}",
    f"task_list={os.environ['CADENCE_CASSANDRA_TEST_TASK_LIST']}",
]
(ARTIFACTS / "run.txt").write_text("\n".join(metadata) + "\n")

def run(label, command, cwd):
    rendered = shlex.join(command)
    print(f"[{label}] {rendered}", flush=True)
    (ARTIFACTS / f"{label}.command").write_text(rendered + "\n")
    result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, env=os.environ.copy())
    (ARTIFACTS / f"{label}.log").write_text(result.stdout)
    print(result.stdout, end="", flush=True)
    (ARTIFACTS / f"{label}.exit").write_text(f"{result.returncode}\n")
    if result.returncode:
        sys.exit(result.returncode)

run("go-seed", ["go", "run", "./tests/matching-cassandra-interop", "seed"], ROOT / "go")
run("rust-restart-probe", [
    "cargo", "test", "--manifest-path", "rust/Cargo.toml", "-p", "cadence-persistence",
    "matching_cassandra::tests::go_seed_rust_write_restart_go_verify", "--", "--ignored", "--exact", "--nocapture",
], ROOT)
run("go-verify", ["go", "run", "./tests/matching-cassandra-interop", "verify"], ROOT / "go")
print(f"Cassandra interop passed; evidence: {ARTIFACTS}")
