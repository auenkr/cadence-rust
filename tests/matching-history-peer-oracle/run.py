#!/usr/bin/env python3
"""Run real Go Matching -> History placement through production resolver/ring."""
import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parents[2]
go_root = root / "go"
oracle_test = Path(__file__).resolve().parent / "oracle_test.go"
package_test = go_root / "client/history/zz_cadence_matching_history_peer_oracle_test.go"
output_dir = Path(__file__).resolve().parent / "evidence"
output_dir.mkdir(exist_ok=True)
memory_dir = root / "memory/evidence/matching-history-peer-oracle"
memory_dir.mkdir(parents=True, exist_ok=True)
fixture = output_dir / "matching_history_peer_oracle.json"
log = output_dir / "go-oracle.log"
hashes = output_dir / "source-sha256.json"

with tempfile.TemporaryDirectory(prefix="matching-history-peer-oracle-") as tmp:
    overlay = Path(tmp) / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(package_test): str(oracle_test)}}))
    env = {
        **os.environ,
        "CADENCE_MATCHING_HISTORY_PEER_ORACLE_OUTPUT": str(fixture),
    }
    version = subprocess.run(["go", "version"], text=True, capture_output=True, check=True).stdout.strip()
    result = subprocess.run(
        ["go", "test", "-overlay", str(overlay), "./client/history", "-run", "^TestCadenceMatchingHistoryPeerPlacementOracle$", "-count=1", "-v"],
        cwd=go_root,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=300,
    )
    log.write_text(f"$ {version}\n$ go test -overlay <temporary-overlay> ./client/history -run ^TestCadenceMatchingHistoryPeerPlacementOracle$ -count=1 -v\n{result.stdout}")
    print(result.stdout, end="")
    if result.returncode:
        raise RuntimeError(f"Go peer-placement oracle exited {result.returncode}; see {log}")

files = [
    go_root / "client/history/peer_resolver.go",
    go_root / "common/membership/hashring.go",
    go_root / "common/membership/resolver.go",
    go_root / "common/membership/hostinfo.go",
    go_root / "service/history/lookup/lookup.go",
    go_root / "common/util.go",
    oracle_test,
]
hashes.write_text(json.dumps({str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}, indent=2) + "\n")
for name in ("matching_history_peer_oracle.json", "go-oracle.log", "source-sha256.json"):
    (memory_dir / name).write_bytes((output_dir / name).read_bytes())
print(f"Wrote Go peer-placement fixture: {fixture}")
print(f"Wrote source hashes: {hashes}")
print(f"Copied reproducible evidence to: {memory_dir}")
