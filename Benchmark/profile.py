#!/usr/bin/env python3
"""Opt-in CPU profile (PROFILE=1): perf record Rust service PIDs during measurement; emit folded stacks, SVG, top-30."""
import argparse, collections, subprocess, sys, time
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--start', required=True); p.add_argument('--end', required=True)
p.add_argument('--output-prefix', required=True); p.add_argument('--flamegraph-dir', required=True)
p.add_argument('--pids', nargs='+', required=True); p.add_argument('--max-seconds', type=int, default=600)
a = p.parse_args()
deadline = time.time() + a.max_seconds
while not Path(a.start).exists() and time.time() < deadline: time.sleep(0.2)
# Include all threads of each container init process tree.
pids, todo = set(), list(a.pids)
while todo:
    pid = todo.pop(); pids.add(pid)
    todo += subprocess.run(['pgrep', '-P', pid], capture_output=True, text=True).stdout.split()
data = '/tmp/' + Path(a.output_prefix).name + '.perf.data'  # outside artifact dir (root-owned)
rec = subprocess.Popen(['sudo', 'perf', 'record', '-F', '99', '-g', '-o', data, '-p', ','.join(sorted(pids))],
                       stdout=subprocess.DEVNULL, stderr=open(a.output_prefix + '-perf.log', 'w'))
while not Path(a.end).exists() and time.time() < deadline: time.sleep(0.5)
subprocess.run(['sudo', 'kill', '-INT', str(rec.pid)]); subprocess.run(['sudo', 'pkill', '-INT', '-f', data])
rc = rec.wait(timeout=120)
print('perf record exit', rc, flush=True)
# perf record exits 0 or 130/-2 on SIGINT; anything else is a recording failure.
if rc not in (0, 130, -2):
    sys.exit(f'perf record failed with exit {rc}; see {a.output_prefix}-perf.log')
def step(name, cmd, inp=None):
    r = subprocess.run(cmd, input=inp, capture_output=True, text=True, errors='replace')
    if r.returncode != 0: sys.exit(f'{name} failed with exit {r.returncode}: {r.stderr[-1500:]}')
    return r.stdout
fg = Path(a.flamegraph_dir)
script = step('perf script', ['sudo', 'perf', 'script', '--no-inline', '-i', data])
print('perf script bytes', len(script), flush=True)
folded = step('stackcollapse', ['perl', str(fg/'stackcollapse-perf.pl')], script)
Path(a.output_prefix + '.folded').write_text(folded)
svg = step('flamegraph', ['perl', str(fg/'flamegraph.pl'), '--width', '1600'], folded)
Path(a.output_prefix + '.svg').write_text(svg)
self_c, incl = collections.Counter(), collections.Counter(); total = 0
for line in folded.splitlines():
    stack, _, n = line.rpartition(' ')
    n = int(n); total += n; frames = stack.split(';')
    self_c[frames[-1]] += n
    for f in set(frames): incl[f] += n
with open(a.output_prefix + '-top30.txt', 'w') as out:
    out.write(f'total samples {total}\n== self ==\n')
    for f, n in self_c.most_common(30): out.write(f'{100*n/max(total,1):6.2f}% {f}\n')
    out.write('== inclusive ==\n')
    for f, n in incl.most_common(60): out.write(f'{100*n/max(total,1):6.2f}% {f}\n')
subprocess.run(['sudo', 'rm', '-f', data])
if total == 0 or not svg.strip(): sys.exit('profile has zero samples')
print('total samples', total, flush=True)
