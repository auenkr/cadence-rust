#!/usr/bin/env python3
"""Run Rust's one-seed JOIN sender against a bootstrapped Ringpop v0.10.0 node."""
import json, os, shutil, stat, subprocess, tempfile
from pathlib import Path

root = Path(__file__).resolve().parent.parent
module = Path('/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0')
probe_override = os.environ.get('RINGPOP_JOIN_PROBE')
target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'rust/target'))

if probe_override:
    # Explicit overrides are caller-owned binaries; validate them, but do not
    # replace or rebuild them. The default path is always freshly built below.
    example = Path(probe_override).expanduser().resolve()
    if not example.is_file() or not os.access(example, os.X_OK):
        raise SystemExit(f'RINGPOP_JOIN_PROBE must name an executable file: {example}')
    probe_temp = None
else:
    subprocess.run(['cargo', 'build', '--locked', '--manifest-path', str(root/'rust/Cargo.toml'), '-p', 'cadence-server', '--example', 'ringpop_join_probe'], cwd=root/'rust', check=True)
    built = target/'debug/examples/ringpop_join_probe'
    if not built.exists():
        built = root/'rust/target/debug/examples/ringpop_join_probe'
    if not built.is_file() or not os.access(built, os.X_OK):
        raise SystemExit(f'cargo build did not produce executable probe: {built}')
    probe_temp = tempfile.TemporaryDirectory(prefix='ringpop-join-probe-')
    example = Path(probe_temp.name)/'ringpop_join_probe'
    shutil.copy2(built, example)
    example.chmod(example.stat().st_mode | stat.S_IXUSR)

try:
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
        test_regex = '^(TestRustOutboundJoin(Group)?AgainstGo|TestOriginalJoinCluster(ChecksDeadlineAfterGroup|DestroyedDiscardsPartialSuccess)|TestOriginalJoinDelayerWindowsAndOptionEdges|TestOriginalGoDurationStringFractionalFixtures)$'
        subprocess.run(['go', 'test', '-overlay', str(overlay), './swim', '-run', test_regex, '-count=1', '-v'], cwd=copied, env=env, check=True, timeout=180)
    print('JOIN, JoinGroup, JoinCluster, delayer, and duration compatibility tests passed against pinned Go Ringpop')
finally:
    if probe_temp is not None:
        probe_temp.cleanup()
