#!/usr/bin/env python3
"""Compare durable Matching queue rules with the actual Go task reader."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
root=Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--regenerate',action='store_true')
args=parser.parse_args()
for name,test in [('reader','Reader'),('expiry','Expiry')]:
    fixture=root/f'tests/fixtures/matching-queue-{name}.json'
    with tempfile.TemporaryDirectory(prefix='cadence-queue-') as directory:
        overlay=Path(directory)/'overlay.json'
        output=Path(directory)/'output.json'
        overlay.write_text(json.dumps({'Replace':{str(root/'go/service/matching/tasklist/rust_queue_test.go'):str(root/'tests/matching-queue-go_test.go')}}))
        subprocess.run(['go','test','-overlay',str(overlay),'./service/matching/tasklist','-run',f'^TestRustMatchingQueue{test}$','-count=1'],cwd=root/'go',env={**os.environ,'CADENCE_MATCHING_QUEUE_INPUT':str(fixture),'CADENCE_MATCHING_QUEUE_OUTPUT':str(output)},check=True)
        actual=json.loads(output.read_text())
        if args.regenerate: fixture.write_text(json.dumps(actual,indent=2)+'\n')
        elif actual!=json.loads(fixture.read_text()): raise RuntimeError(f'Go queue {name} fixtures drifted')
if not args.regenerate:
    subprocess.run(['cargo','test','--manifest-path',str(root/'rust/Cargo.toml'),'-p','cadence-persistence','matching_queue'],check=True)
print('Verified original Go Matching reader and expiry rules')
