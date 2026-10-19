#!/usr/bin/env python3
"""Run deterministic probe and timer traces against pinned Ringpop Go."""
import json
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
module = Path('/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0')

with tempfile.TemporaryDirectory(prefix='ringpop-swim-runtime-oracle-') as directory:
    temp = Path(directory)
    copied = temp / 'ringpop-go'
    shutil.copytree(module, copied, ignore=shutil.ignore_patterns('.git'))
    for current, _dirs, files in os.walk(copied):
        os.chmod(current, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
        for filename in files:
            os.chmod(Path(current) / filename, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    shutil.copy2(root / 'go/go.mod', copied / 'go.mod')
    shutil.copy2(root / 'go/go.sum', copied / 'go.sum')
    mod = copied / 'go.mod'
    mod.write_text(mod.read_text().replace('module github.com/uber/cadence', 'module github.com/uber/ringpop-go', 1))
    virtual = copied / 'swim/zz_cadence_ringpop_swim_runtime_test.go'
    overlay = temp / 'overlay.json'
    overlay.write_text(json.dumps({'Replace': {str(virtual): str(root / 'tests/ringpop-swim-runtime-go_test.go')}}))
    result = subprocess.run([
        'go', 'test', '-overlay', str(overlay), './swim',
        '-run', '^TestGoSwimProbeAndTransitionOracle$', '-count=1', '-v',
    ], cwd=copied, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False, timeout=180)
    print(result.stdout, end='')
    if result.returncode:
        raise RuntimeError(f'pinned Go oracle exited {result.returncode}')
    marker = 'CADENCE_SWIM_RUNTIME_ORACLE='
    raw = next((line[len(marker):] for line in result.stdout.splitlines() if line.startswith(marker)), None)
    if raw is None:
        raise RuntimeError('Go test did not emit the runtime trace')
    oracle = json.loads(raw)

fixture = root / 'tests/fixtures/ringpop_swim_runtime_oracle.json'
fixture.write_text(json.dumps(oracle, indent=2) + '\n')
print(f'Wrote Go runtime oracle fixture: {fixture}')
