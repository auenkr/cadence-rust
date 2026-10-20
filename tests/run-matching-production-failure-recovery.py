#!/usr/bin/env python3
"""Run the two-Rust-Matching production failure and idle recovery probe."""
import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--rust-production-binary', type=Path, required=True)
parser.add_argument('--artifacts-dir', type=Path, required=True)
parser.add_argument('--history-transport', choices=('grpc', 'tchannel'), default='grpc')
parser.add_argument('--idle-check-seconds', type=int, default=3)
args = parser.parse_args()
if args.idle_check_seconds < 1:
    parser.error('--idle-check-seconds must be positive')
args.artifacts_dir.mkdir(parents=True, exist_ok=True)
command = [
    sys.executable, str(ROOT / 'tests/run-go-service-baseline.py'),
    '--rust-production-binary', str(args.rust_production_binary.resolve()),
    '--history-transport', args.history_transport,
    '--matching-failure-recovery',
    '--matching-idle-check-seconds', str(args.idle_check_seconds),
    '--artifacts-dir', str(args.artifacts_dir.resolve()),
]
raise SystemExit(subprocess.run(command, cwd=ROOT, check=False).returncode)
