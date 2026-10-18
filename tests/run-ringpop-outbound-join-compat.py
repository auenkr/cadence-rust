#!/usr/bin/env python3
"""Run Rust's one-seed JOIN sender against a bootstrapped Ringpop v0.10.0 node."""
import json, os, shutil, stat, subprocess, tempfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
module = Path('/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0')
example = Path(os.environ.get('RINGPOP_JOIN_PROBE', '/tmp/cadence-ringpop-join-probe'))
target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'rust/target'))
if not example.exists():
    subprocess.run(['cargo', 'build', '--locked', '--manifest-path', str(root/'rust/Cargo.toml'), '-p', 'cadence-server', '--example', 'ringpop_join_probe'], cwd=root/'rust', check=True)
    built = target/'debug/examples/ringpop_join_probe'
    if not built.exists():
        built = root/'rust/target/debug/examples/ringpop_join_probe'
    shutil.copy2(built, example)
with tempfile.TemporaryDirectory(prefix='ringpop-outbound-join-') as directory:
    temp = Path(directory); copied = temp/'ringpop-go'
    shutil.copytree(module, copied, ignore=shutil.ignore_patterns('.git'))
    for current, _dirs, _files in os.walk(copied):
        os.chmod(current, stat.S_IRWXU|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
    shutil.copy2(root/'go/go.mod', copied/'go.mod'); shutil.copy2(root/'go/go.sum', copied/'go.sum')
    mod = copied/'go.mod'; mod.write_text(mod.read_text().replace('module github.com/uber/cadence', 'module github.com/uber/ringpop-go', 1))
    virtual = copied/'swim/zz_cadence_ringpop_outbound_join_test.go'
    overlay = temp/'overlay.json'; overlay.write_text(json.dumps({'Replace': {str(virtual): str(root/'tests/ringpop-outbound-join-go_test.go')}}))
    env = {**os.environ, 'RINGPOP_JOIN_PROBE': str(example.resolve())}
    subprocess.run(['go', 'test', '-overlay', str(overlay), './swim', '-run', '^TestRustOutboundJoin(Group)?AgainstGo$', '-count=1', '-v'], cwd=copied, env=env, check=True, timeout=180)
print('Rust outbound JOIN and JoinGroup passed against real pinned Go Ringpop nodes')
