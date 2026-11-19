import json,sys,glob,os
from datetime import datetime
def ts(s): return datetime.fromisoformat(s.rstrip('Z')[:26]+'+00:00') if s else None
for d in sys.argv[1:]:
  for f in sorted(glob.glob(d+'/*-samples.jsonl')):
    arm=os.path.basename(f)[:-14]
    rows=[json.loads(l) for l in open(f)]
    rows=[r for r in rows if r.get('phase')!='warmup']
    if not rows: continue
    keys=list(rows[0].keys())
    t0=ts(rows[0]['scheduled']); b={}
    for r in rows:
      s=ts(r['scheduled']); e=ts(r.get('completed') or r.get('finished') or '')
      if not e: continue
      b.setdefault(int((s-t0).total_seconds()//10),[]).append((e-s).total_seconds()*1000)
    errs=sum(1 for r in rows if r.get('error'))
    line=' '.join(f"{sorted(v)[len(v)//2]:.0f}/{max(v):.0f}" for k,v in sorted(b.items()))
    res=json.load(open(f.replace('samples.jsonl','resources.json')))['containers']
    cpu=' '.join(f"{k.split('-')[-2]}={v['average_cpu_cores']:.2f}" for k,v in res.items())
    print(arm,len(rows),'err',errs,'| p50/max per10s:',line,'|',cpu)
print('-- p50/p99 completion, commit_ms, host iowait%, io-psi%, steal%')
for d in sys.argv[1:]:
  for f in sorted(glob.glob(d+'/*-samples.jsonl')):
    arm=os.path.basename(f)[:-14]
    v=sorted(json.loads(l)['completion_ms'] for l in open(f) if '"warmup"' not in l)
    R=json.load(open(f.replace('samples.jsonl','resources.json')))
    m=R['mysql_samples']; a,b=m[0]['counters'],m[-1]['counters']
    cm=(b['commit_wait_ps']-a['commit_wait_ps'])/max(1,b['commit_count']-a['commit_count'])/1e9
    t=R['timeline']; h0,h1=t[0].get('host',{}),t[-1].get('host',{}); dt=t[-1]['elapsed_seconds']
    hs=''
    if 'total_jiffies' in h0:
      tj=h1['total_jiffies']-h0['total_jiffies']
      hs=f"iow={100*(h1['iowait_jiffies']-h0['iowait_jiffies'])/tj:.1f}% steal={100*(h1['steal_jiffies']-h0['steal_jiffies'])/tj:.1f}% iopsi={(h1['io_pressure_some_total_us']-h0['io_pressure_some_total_us'])/dt/1e4:.1f}% load={h1['loadavg1']}"
    print(arm,f"{v[len(v)//2]:.0f}/{v[int(.99*(len(v)-1))]:.0f}",f"commit={cm:.1f}ms",hs)
