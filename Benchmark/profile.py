#!/usr/bin/env python3
"""Opt-in CPU profile (PROFILE=1): perf record Rust service PIDs during measurement; emit folded stacks, SVG, top-30."""
import argparse, collections, subprocess, time
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
rec.wait(timeout=120)
fg = Path(a.flamegraph_dir)
script = subprocess.run(['sudo', 'perf', 'script', '--no-inline', '-i', data], capture_output=True, text=True, errors='replace').stdout
print('perf script bytes', len(script), flush=True)
folded = subprocess.run(['perl', str(fg/'stackcollapse-perf.pl')], input=script, capture_output=True, text=True).stdout
Path(a.output_prefix + '.folded').write_text(folded)
svg = subprocess.run(['perl', str(fg/'flamegraph.pl'), '--width', '1600'], input=folded, capture_output=True, text=True).stdout
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
