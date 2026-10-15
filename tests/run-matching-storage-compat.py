#!/usr/bin/env python3
"""Compare Matching SQL blob formats with the original Go serializers."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import sqlite3
root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--regenerate', action='store_true')
args = parser.parse_args()
fixture = root / 'tests/fixtures/matching-blobs.json'
with tempfile.TemporaryDirectory(prefix='cadence-matching-storage-') as directory:
    overlay = Path(directory) / 'overlay.json'
    output = Path(directory) / 'output.json'
    overlay.write_text(json.dumps({'Replace': {
        str(root / 'go/common/persistence/serialization/rust_matching_blobs_test.go'): str(root / 'tests/matching-blob-go_test.go')
    }}))
    subprocess.run(['go','test','-overlay',str(overlay),'./common/persistence/serialization',
                    '-run','^TestRustMatchingBlobFixtures$','-count=1'], cwd=root/'go',
                   env={**os.environ,'CADENCE_MATCHING_BLOB_OUTPUT':str(output)},check=True)
    actual=json.loads(output.read_text())
    if args.regenerate:
        fixture.write_text(json.dumps(actual,indent=2)+'\n')
    elif actual != json.loads(fixture.read_text()):
        raise RuntimeError('original Go Matching blob fixtures drifted')
if not args.regenerate:
    subprocess.run(['cargo','test','--manifest-path',str(root/'rust/Cargo.toml'),'-p','cadence-persistence','matching_'],check=True)
print(f'Verified {len(actual)} original Go SQL matching blob fixtures')

if not args.regenerate:
    with tempfile.TemporaryDirectory(prefix='cadence-matching-sqlite-') as directory:
        database=Path(directory)/'tasks.db'
        with sqlite3.connect(database) as connection:
            connection.executescript((root/'rust/sql/matching-sqlite.sql').read_text())
        overlay=Path(directory)/'overlay.json'
        overlay.write_text(json.dumps({'Replace': {
            str(root/'go/common/persistence/sql/rust_matching_sqlite_test.go'): str(root/'tests/matching-sqlite-go_test.go')
        }}))
        def go(phase):
            subprocess.run(['go','test','-overlay',str(overlay),'./common/persistence/sql',
                '-run','^TestRustMatchingSQLiteInterop$','-count=1'],cwd=root/'go',
                env={**os.environ,'CADENCE_MATCHING_SQLITE_DB':str(database),'CADENCE_MATCHING_SQLITE_PHASE':phase},check=True)
        def rust(phase):
            subprocess.run(['cargo','test','--manifest-path',str(root/'rust/Cargo.toml'),'-p','cadence-persistence',
                'matching_sqlite::interop_tests::matching_sqlite_go_database_interop','--','--ignored','--exact'],
                env={**os.environ,'CADENCE_MATCHING_SQLITE_DB':str(database),'CADENCE_MATCHING_SQLITE_PHASE':phase},check=True)
        go('seed')
        rust('write')
        go('verify')
        rust('verify')
    print('Verified bidirectional original-Go/Rust SQLite matching persistence')
